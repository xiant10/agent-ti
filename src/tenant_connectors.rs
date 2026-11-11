use crate::control_copy::ControlCopies;
use crate::grants::GrantBroker;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const FRESH_DURATION: Duration = Duration::from_secs(10);
const TIMEOUT_DURATION: Duration = Duration::from_secs(2);
const MAX_DESCRIPTION: usize = 1000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantConnectorTool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub input_schema: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantConnectorRecord {
    pub id: String,
    pub name: String,
    pub title: String,
    pub version: u64,
    pub tools: Vec<TenantConnectorTool>,
}

#[derive(Clone)]
pub struct TenantToolEntry {
    pub name: String,
    pub server: String,
    pub connector_id: String,
    pub mcp_name: String,
    pub description: String,
    pub input_schema: Value,
    pub validator: Arc<jsonschema::Validator>,
}

#[derive(Clone, Default)]
pub struct TenantTools {
    pub tools: Arc<Vec<TenantToolEntry>>,
    pub connectors: Arc<HashMap<String, String>>,
    pub digest: String,
}

#[derive(Clone)]
struct TenantConnectorsEntry {
    etag: Option<String>,
    tools: TenantTools,
    /// When control last answered, or `None` if it never has for this tenant.
    ok_at: Option<Instant>,
    checked_at: Instant,
}

pub struct TenantConnectors {
    url: String,
    gateway_url: String,
    broker: Arc<GrantBroker>,
    client: reqwest::Client,
    entries: RwLock<HashMap<String, TenantConnectorsEntry>>,
    changes: RwLock<HashMap<String, u64>>,
    in_flight: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
    fresh_duration: Duration,
    timeout_duration: Duration,
    copies: ControlCopies,
}

impl TenantConnectors {
    pub fn new(
        url: String,
        gateway_url: String,
        broker: Arc<GrantBroker>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            gateway_url: gateway_url.trim_end_matches('/').to_string(),
            broker,
            client,
            entries: RwLock::new(HashMap::new()),
            changes: RwLock::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            fresh_duration: FRESH_DURATION,
            timeout_duration: TIMEOUT_DURATION,
            copies: ControlCopies::new(None, "connectors"),
        }
    }

    /// Keeps each tenant's last good answer on disk, so a restart while control is down starts
    /// from it.
    pub fn with_copies(mut self, copies: ControlCopies) -> Self {
        self.copies = copies;
        self
    }

    /// The copy on disk, the first time this process needs a tenant. Its ETag makes the first
    /// check a 304 if nothing changed.
    async fn load_saved(&self, tenant: &str, platform_tool_names: &[String]) {
        if self.entries.read().await.contains_key(tenant) {
            return;
        }
        let Some(saved) = self.copies.load(tenant) else {
            return;
        };
        let tools = build_tenant_tools(&records_of(&saved.body), platform_tool_names);
        self.entries
            .write()
            .await
            .entry(tenant.to_string())
            .or_insert(TenantConnectorsEntry {
                etag: saved.etag,
                tools,
                ok_at: Some(saved.at),
                checked_at: Instant::now() - self.fresh_duration * 2,
            });
    }

    pub fn gateway_url(&self) -> &str {
        &self.gateway_url
    }

    /// The tools last read for a tenant, without asking control.
    pub fn known(&self, tenant_id: Option<&str>) -> TenantTools {
        if let Ok(entries) = self.entries.try_read() {
            tenant_id
                .and_then(|t| entries.get(t).map(|e| e.tools.clone()))
                .unwrap_or_default()
        } else {
            TenantTools::default()
        }
    }

    /// Marks a tenant's connectors stale (called when skills change feed notifies change).
    pub async fn changed(&self, tenant: &str) {
        {
            let mut changes = self.changes.write().await;
            let counter = changes.entry(tenant.to_string()).or_insert(0);
            *counter += 1;
        }
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(tenant) {
                entry.checked_at = Instant::now() - self.fresh_duration * 2;
            }
        }
    }

    /// Returns the tools offered by a tenant's connectors.
    pub async fn for_tenant(
        &self,
        tenant_id: Option<&str>,
        platform_tool_names: &[String],
    ) -> TenantTools {
        let Some(tenant) = tenant_id.filter(|t| !t.is_empty()) else {
            return TenantTools::default();
        };
        self.load_saved(tenant, platform_tool_names).await;

        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(tenant)
                && entry.checked_at.elapsed() < self.fresh_duration
            {
                return entry.tools.clone();
            }
        }

        // Deduplicate concurrent checks
        let notify = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(existing) = in_flight.get(tenant) {
                Some(existing.clone())
            } else {
                let n = Arc::new(tokio::sync::Notify::new());
                in_flight.insert(tenant.to_string(), n);
                None
            }
        };

        if let Some(n) = notify {
            n.notified().await;
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(tenant) {
                return entry.tools.clone();
            }
            return TenantTools::default();
        }

        let existing_entry = self.entries.read().await.get(tenant).cloned();
        let result = self
            .check_tenant(tenant, platform_tool_names, existing_entry)
            .await;

        {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(n) = in_flight.remove(tenant) {
                n.notify_waiters();
            }
        }

        result
    }

    async fn check_tenant(
        &self,
        tenant: &str,
        platform_tool_names: &[String],
        existing: Option<TenantConnectorsEntry>,
    ) -> TenantTools {
        let now = Instant::now();
        let generation = self.changes.read().await.get(tenant).copied().unwrap_or(0);
        let checked_at = {
            let changes = self.changes.read().await;
            if changes.get(tenant).copied().unwrap_or(0) == generation {
                now
            } else {
                now - self.fresh_duration * 2
            }
        };

        let token = match self.broker.service_token().await {
            Ok(tok) => tok,
            Err(e) => {
                return self
                    .fallback_or_empty(tenant, existing, now, &format!("service token: {e}"))
                    .await;
            }
        };

        let mut req = self
            .client
            .get(format!("{}/v1/runtime/connectors", self.url))
            .query(&[("tenant", tenant)])
            .header("authorization", format!("Bearer {token}"))
            .timeout(self.timeout_duration);

        if let Some(ref entry) = existing
            && let Some(ref etag) = entry.etag
        {
            req = req.header("if-none-match", etag);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                return self
                    .fallback_or_empty(tenant, existing, now, &e.to_string())
                    .await;
            }
        };

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_MODIFIED
            && let Some(mut entry) = existing
        {
            entry.ok_at = Some(now);
            entry.checked_at = checked_at;
            self.entries
                .write()
                .await
                .insert(tenant.to_string(), entry.clone());
            return entry.tools;
        }

        if !status.is_success() {
            return self
                .fallback_or_empty(tenant, existing, now, &format!("status {status}"))
                .await;
        }

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let body = match resp.json::<Value>().await {
            Ok(v) => v,
            Err(e) => {
                return self
                    .fallback_or_empty(tenant, existing, now, &format!("invalid JSON: {e}"))
                    .await;
            }
        };

        let tools = build_tenant_tools(&records_of(&body), platform_tool_names);
        self.copies.save(tenant, etag.as_deref(), &body);
        let entry = TenantConnectorsEntry {
            etag,
            tools: tools.clone(),
            ok_at: Some(now),
            checked_at,
        };
        self.entries.write().await.insert(tenant.to_string(), entry);
        tools
    }

    /// The last good answer stands in with no time limit; with none, no connector tools. Either
    /// way control is asked again only after `fresh_duration`.
    async fn fallback_or_empty(
        &self,
        tenant: &str,
        existing: Option<TenantConnectorsEntry>,
        now: Instant,
        error: &str,
    ) -> TenantTools {
        let entry = match existing {
            Some(entry) if entry.ok_at.is_some() => {
                self.copies
                    .failed(tenant, error, entry.ok_at, "last answer");
                TenantConnectorsEntry {
                    checked_at: now,
                    ..entry
                }
            }
            _ => {
                self.copies.failed(tenant, error, None, "none");
                TenantConnectorsEntry {
                    etag: None,
                    tools: TenantTools::default(),
                    ok_at: None,
                    checked_at: now,
                }
            }
        };
        let tools = entry.tools.clone();
        self.entries.write().await.insert(tenant.to_string(), entry);
        tools
    }
}

/// The records in an answer from `GET /v1/runtime/connectors`.
fn records_of(body: &Value) -> Vec<TenantConnectorRecord> {
    body.get("connectors")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| serde_json::from_value::<TenantConnectorRecord>(v.clone()).ok())
                .collect()
        })
        .unwrap_or_default()
}

pub fn build_tenant_tools(
    records: &[TenantConnectorRecord],
    platform_tool_names: &[String],
) -> TenantTools {
    let mut entries = Vec::new();
    let mut connectors = HashMap::new();
    let mut seen_names = std::collections::HashSet::new();

    for record in records {
        connectors.insert(record.name.clone(), record.id.clone());
        for tool in &record.tools {
            let offered_name = format!("{}_{}", record.name, tool.name);
            // Never override a platform tool
            if platform_tool_names.contains(&offered_name) {
                continue;
            }
            if !seen_names.insert(offered_name.clone()) {
                continue;
            }
            let desc_raw = tool.description.as_deref().unwrap_or(&tool.name).trim();
            let description = if desc_raw.chars().count() > MAX_DESCRIPTION {
                let truncated: String = desc_raw.chars().take(MAX_DESCRIPTION - 3).collect();
                format!("{truncated}...")
            } else {
                desc_raw.to_string()
            };
            let schema = tool
                .input_schema
                .clone()
                .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
            let validator = Arc::new(jsonschema::validator_for(&schema).unwrap_or_else(|_| {
                jsonschema::validator_for(&json!({"type": "object"})).unwrap()
            }));
            entries.push(TenantToolEntry {
                name: offered_name,
                server: record.name.clone(),
                connector_id: record.id.clone(),
                mcp_name: tool.name.clone(),
                description,
                input_schema: schema,
                validator,
            });
        }
    }

    let digest = serde_json::to_string(
        &records
            .iter()
            .map(|r| (&r.id, r.version))
            .collect::<Vec<_>>(),
    )
    .unwrap_or_default();

    TenantTools {
        tools: Arc::new(entries),
        connectors: Arc::new(connectors),
        digest,
    }
}
