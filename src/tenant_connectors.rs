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
const STALE_DURATION: Duration = Duration::from_secs(300);
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
    ok_at: Instant,
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
    stale_duration: Duration,
    timeout_duration: Duration,
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
            stale_duration: STALE_DURATION,
            timeout_duration: TIMEOUT_DURATION,
        }
    }

    #[cfg(test)]
    pub fn with_durations(mut self, fresh: Duration, stale: Duration, timeout: Duration) -> Self {
        self.fresh_duration = fresh;
        self.stale_duration = stale;
        self.timeout_duration = timeout;
        self
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
                tracing::warn!(tenant = %tenant, error = %e, "connectors.service_token_failed");
                return self.fallback_or_empty(tenant, existing);
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
                tracing::warn!(tenant = %tenant, error = %e, "connectors.tenant_unavailable");
                return self.fallback_or_empty(tenant, existing);
            }
        };

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_MODIFIED
            && let Some(mut entry) = existing
        {
            entry.ok_at = now;
            entry.checked_at = checked_at;
            self.entries
                .write()
                .await
                .insert(tenant.to_string(), entry.clone());
            return entry.tools;
        }

        if !status.is_success() {
            tracing::warn!(tenant = %tenant, status = %status, "connectors.tenant_unavailable");
            return self.fallback_or_empty(tenant, existing);
        }

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let body = match resp.json::<Value>().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(tenant = %tenant, error = %e, "connectors.invalid_json");
                return self.fallback_or_empty(tenant, existing);
            }
        };

        let records: Vec<TenantConnectorRecord> = body
            .get("connectors")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<TenantConnectorRecord>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();

        let tools = build_tenant_tools(&records, platform_tool_names);
        let entry = TenantConnectorsEntry {
            etag,
            tools: tools.clone(),
            ok_at: now,
            checked_at,
        };
        self.entries.write().await.insert(tenant.to_string(), entry);
        tools
    }

    fn fallback_or_empty(
        &self,
        tenant: &str,
        existing: Option<TenantConnectorsEntry>,
    ) -> TenantTools {
        if let Some(entry) = existing
            && entry.ok_at.elapsed() < self.stale_duration
        {
            tracing::info!(tenant = %tenant, "using cached connectors during control outage");
            return entry.tools;
        }
        TenantTools::default()
    }
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
