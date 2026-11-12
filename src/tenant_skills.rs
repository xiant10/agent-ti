use crate::flights::Flights;
use crate::control_copy::ControlCopies;
use crate::grants::GrantBroker;
use agent_ti_config::{SkillConfig, TenantSkillRecord, apply_tenant_skills};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const FRESH_DURATION: Duration = Duration::from_secs(10);
const TIMEOUT_DURATION: Duration = Duration::from_secs(2);
const RETRY_PAUSE: Duration = Duration::from_secs(5);
const MAX_TURN_PINS: usize = 10_000;

#[derive(Clone)]
struct TenantSkillEntry {
    etag: Option<String>,
    skills: Arc<Vec<SkillConfig>>,
    records: Vec<TenantSkillRecord>,
    /// When control last answered, or `None` if it never has for this tenant.
    ok_at: Option<Instant>,
    checked_at: Instant,
}

/// The records in an answer from `GET /v1/runtime/skills`.
fn records_of(body: &Value) -> Vec<TenantSkillRecord> {
    body.get("skills")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| serde_json::from_value::<TenantSkillRecord>(v.clone()).ok())
                .collect()
        })
        .unwrap_or_default()
}

pub type TenantChangeListener = Box<dyn Fn(&str) + Send + Sync>;

pub struct TenantSkills {
    base: Arc<Vec<SkillConfig>>,
    url: String,
    broker: Arc<GrantBroker>,
    client: reqwest::Client,
    entries: RwLock<HashMap<String, TenantSkillEntry>>,
    changes: RwLock<HashMap<String, u64>>,
    in_flight: Flights<String>,
    listeners: RwLock<Vec<TenantChangeListener>>,
    fresh_duration: Duration,
    timeout_duration: Duration,
    copies: ControlCopies,
}

impl TenantSkills {
    pub fn new(
        base: Arc<Vec<SkillConfig>>,
        url: String,
        broker: Arc<GrantBroker>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            base,
            url: url.trim_end_matches('/').to_string(),
            broker,
            client,
            entries: RwLock::new(HashMap::new()),
            changes: RwLock::new(HashMap::new()),
            in_flight: Flights::default(),
            listeners: RwLock::new(Vec::new()),
            fresh_duration: FRESH_DURATION,
            timeout_duration: TIMEOUT_DURATION,
            copies: ControlCopies::new(None, "skills"),
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
    async fn load_saved(&self, tenant: &str) {
        if self.entries.read().await.contains_key(tenant) {
            return;
        }
        let Some(saved) = self.copies.load(tenant) else {
            return;
        };
        let records = records_of(&saved.body);
        let skills = self.view(tenant, &records);
        self.entries
            .write()
            .await
            .entry(tenant.to_string())
            .or_insert(TenantSkillEntry {
                etag: saved.etag,
                skills,
                records,
                ok_at: Some(saved.at),
                checked_at: Instant::now() - self.fresh_duration * 2,
            });
    }

    fn view(&self, tenant: &str, records: &[TenantSkillRecord]) -> Arc<Vec<SkillConfig>> {
        if records.is_empty() {
            return self.base.clone();
        }
        Arc::new(apply_tenant_skills(&self.base, records, |rec, reason| {
            tracing::warn!(tenant = %tenant, skill = %rec.name, version = rec.version, reason = %reason, "skills.tenant_skipped");
        }))
    }

    pub async fn add_listener(&self, listener: TenantChangeListener) {
        self.listeners.write().await.push(listener);
    }

    /// Marks a tenant stale so that its next read asks control again.
    pub async fn changed(&self, tenant: &str) {
        {
            let mut changes = self.changes.write().await;
            let counter = changes.entry(tenant.to_string()).or_insert(0);
            *counter += 1;
        }
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(tenant) {
                // Set checked_at far in the past to force revalidation
                entry.checked_at = Instant::now() - self.fresh_duration * 2;
            }
        }
        let listeners = self.listeners.read().await;
        for listener in listeners.iter() {
            listener(tenant);
        }
    }

    /// The skills one tenant sees. A session without a tenant sees the built-in base skills.
    pub async fn for_tenant(&self, tenant_id: Option<&str>) -> Arc<Vec<SkillConfig>> {
        let Some(tenant) = tenant_id.filter(|t| !t.is_empty()) else {
            return self.base.clone();
        };
        self.load_saved(tenant).await;

        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(tenant)
                && entry.checked_at.elapsed() < self.fresh_duration
            {
                return entry.skills.clone();
            }
        }

        // Deduplicate concurrent checks for the same tenant
        let Some(_flight) = self.in_flight.start(&tenant.to_string()).await else {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(tenant) {
                return entry.skills.clone();
            }
            return self.base.clone();
        };

        let existing_entry = self.entries.read().await.get(tenant).cloned();
        self.check_tenant(tenant, existing_entry).await
    }

    async fn check_tenant(
        &self,
        tenant: &str,
        existing: Option<TenantSkillEntry>,
    ) -> Arc<Vec<SkillConfig>> {
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
                    .fallback_or_base(tenant, existing, now, &format!("service token: {e}"))
                    .await;
            }
        };

        let mut req = self
            .client
            .get(format!("{}/v1/runtime/skills", self.url))
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
                    .fallback_or_base(tenant, existing, now, &e.to_string())
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
            return entry.skills;
        }

        if !status.is_success() {
            return self
                .fallback_or_base(tenant, existing, now, &format!("status {status}"))
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
                    .fallback_or_base(tenant, existing, now, &format!("invalid JSON: {e}"))
                    .await;
            }
        };

        let records = records_of(&body);
        let skills = self.view(tenant, &records);
        self.copies.save(tenant, etag.as_deref(), &body);

        let entry = TenantSkillEntry {
            etag,
            skills: skills.clone(),
            records,
            ok_at: Some(now),
            checked_at,
        };
        self.entries.write().await.insert(tenant.to_string(), entry);
        skills
    }

    /// The last good answer stands in with no time limit; with none, the built-in skills. Either
    /// way control is asked again only after `fresh_duration`, so an outage costs one slow turn
    /// per tenant, not every one.
    async fn fallback_or_base(
        &self,
        tenant: &str,
        existing: Option<TenantSkillEntry>,
        now: Instant,
        error: &str,
    ) -> Arc<Vec<SkillConfig>> {
        let entry = match existing {
            Some(entry) if entry.ok_at.is_some() => {
                self.copies
                    .failed(tenant, error, entry.ok_at, "last answer");
                TenantSkillEntry {
                    checked_at: now,
                    ..entry
                }
            }
            _ => {
                self.copies.failed(tenant, error, None, "built-in skills");
                TenantSkillEntry {
                    etag: None,
                    skills: self.base.clone(),
                    records: Vec::new(),
                    ok_at: None,
                    checked_at: now,
                }
            }
        };
        let skills = entry.skills.clone();
        self.entries.write().await.insert(tenant.to_string(), entry);
        skills
    }

    /// A tenant's skills with one author's draft of one skill in place of its published version.
    pub async fn with_draft(
        &self,
        tenant_id: Option<&str>,
        author: &str,
        name: &str,
    ) -> Arc<Vec<SkillConfig>> {
        let published = self.for_tenant(tenant_id).await;
        let Some(tenant) = tenant_id.filter(|t| !t.is_empty()) else {
            return published;
        };

        let token = match self.broker.service_token().await {
            Ok(tok) => tok,
            Err(_) => return published,
        };

        let resp = match self
            .client
            .get(format!("{}/v1/runtime/skills/draft", self.url))
            .query(&[("tenant", tenant), ("name", name), ("author", author)])
            .header("authorization", format!("Bearer {token}"))
            .timeout(self.timeout_duration)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(tenant = %tenant, skill = %name, error = %e, "skills.draft_unavailable");
                return published;
            }
        };

        if resp.status() == reqwest::StatusCode::NOT_FOUND || !resp.status().is_success() {
            return published;
        }

        let draft: TenantSkillRecord = match resp.json().await {
            Ok(d) => d,
            Err(_) => return published,
        };

        let records = {
            let entries = self.entries.read().await;
            entries
                .get(tenant)
                .map(|e| e.records.clone())
                .unwrap_or_default()
        };

        let mut filtered: Vec<_> = records.into_iter().filter(|r| r.name != name).collect();
        filtered.push(draft);

        let view = apply_tenant_skills(&self.base, &filtered, |rec, reason| {
            tracing::warn!(tenant = %tenant, skill = %rec.name, reason = %reason, "skills.draft_skipped");
        });
        Arc::new(view)
    }

    /// Follows control's change feed until shutdown signal.
    pub async fn watch(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut cursor: Option<u64> = None;
        let mut failing = false;

        while !*shutdown.borrow() {
            let started = Instant::now();
            let token = match self.broker.service_token().await {
                Ok(tok) => tok,
                Err(e) => {
                    if !failing {
                        tracing::warn!(error = %e, "skills.changes_unavailable: token error");
                        failing = true;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(RETRY_PAUSE) => continue,
                        _ = shutdown.changed() => break,
                    }
                }
            };

            let mut req = self
                .client
                .get(format!("{}/v1/runtime/skills/changes", self.url))
                .header("authorization", format!("Bearer {token}"))
                .timeout(Duration::from_secs(30));

            if let Some(c) = cursor {
                req = req.query(&[("after", c)]);
            }

            let resp = tokio::select! {
                r = req.send() => r,
                _ = shutdown.changed() => break,
            };

            let resp = match resp {
                Ok(r) => r,
                Err(e) => {
                    if !failing {
                        tracing::warn!(error = %e, "skills.changes_unavailable");
                        failing = true;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(RETRY_PAUSE) => continue,
                        _ = shutdown.changed() => break,
                    }
                }
            };

            if !resp.status().is_success() {
                if !failing {
                    tracing::warn!(status = %resp.status(), "skills.changes_status_error");
                    failing = true;
                }
                tokio::select! {
                    _ = tokio::time::sleep(RETRY_PAUSE) => continue,
                    _ = shutdown.changed() => break,
                }
            }

            failing = false;
            let body = match resp.json::<Value>().await {
                Ok(b) => b,
                Err(_) => {
                    tokio::select! {
                        _ = tokio::time::sleep(RETRY_PAUSE) => continue,
                        _ = shutdown.changed() => break,
                    }
                }
            };

            if let Some(tenants) = body.get("tenants").and_then(Value::as_array) {
                for t in tenants {
                    if let Some(name) = t.as_str() {
                        self.changed(name).await;
                    }
                }
            }

            let new_cursor = body.get("cursor").and_then(Value::as_u64);
            let moved = new_cursor.is_some() && new_cursor != cursor;
            if let Some(c) = new_cursor {
                cursor = Some(c);
            }

            if !moved && started.elapsed() < Duration::from_millis(1000) {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1000)) => {},
                    _ = shutdown.changed() => break,
                }
            }
        }
    }
}

type PinnedSkills = (
    Instant,
    Arc<Vec<SkillConfig>>,
    Option<Arc<crate::tenant_config::TenantConfig>>,
);

/// Pins the skills, and the tenant's customization, each conversation's turn started with.
#[derive(Default)]
pub struct TurnPins {
    pins: Mutex<HashMap<String, PinnedSkills>>,
}

impl TurnPins {
    pub async fn pin(
        &self,
        session_id: &str,
        skills: Arc<Vec<SkillConfig>>,
        config: Option<Arc<crate::tenant_config::TenantConfig>>,
    ) {
        let mut pins = self.pins.lock().await;
        if pins.len() >= MAX_TURN_PINS {
            let oldest = pins
                .iter()
                .min_by_key(|(_, (at, _, _))| *at)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                pins.remove(&k);
            }
        }
        pins.insert(session_id.to_string(), (Instant::now(), skills, config));
    }

    pub async fn get(&self, session_id: &str) -> Option<Arc<Vec<SkillConfig>>> {
        let pins = self.pins.lock().await;
        pins.get(session_id).map(|(_, s, _)| s.clone())
    }

    /// The customization a session's turn started with, if it was pinned.
    pub async fn config(
        &self,
        session_id: &str,
    ) -> Option<Arc<crate::tenant_config::TenantConfig>> {
        let pins = self.pins.lock().await;
        pins.get(session_id).and_then(|(_, _, c)| c.clone())
    }

    pub async fn remove(&self, session_id: &str) {
        let mut pins = self.pins.lock().await;
        pins.remove(session_id);
    }
}
