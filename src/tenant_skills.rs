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
const STALE_DURATION: Duration = Duration::from_secs(300);
const TIMEOUT_DURATION: Duration = Duration::from_secs(2);
const RETRY_PAUSE: Duration = Duration::from_secs(5);
const MAX_TURN_PINS: usize = 10_000;

#[derive(Clone)]
struct TenantSkillEntry {
    etag: Option<String>,
    skills: Arc<Vec<SkillConfig>>,
    records: Vec<TenantSkillRecord>,
    ok_at: Instant,
    checked_at: Instant,
}

pub type TenantChangeListener = Box<dyn Fn(&str) + Send + Sync>;

pub struct TenantSkills {
    base: Arc<Vec<SkillConfig>>,
    url: String,
    broker: Arc<GrantBroker>,
    client: reqwest::Client,
    entries: RwLock<HashMap<String, TenantSkillEntry>>,
    changes: RwLock<HashMap<String, u64>>,
    in_flight: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
    listeners: RwLock<Vec<TenantChangeListener>>,
    fresh_duration: Duration,
    stale_duration: Duration,
    timeout_duration: Duration,
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
            in_flight: Mutex::new(HashMap::new()),
            listeners: RwLock::new(Vec::new()),
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

        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(tenant)
                && entry.checked_at.elapsed() < self.fresh_duration
            {
                return entry.skills.clone();
            }
        }

        // Deduplicate concurrent checks for the same tenant
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
                return entry.skills.clone();
            }
            return self.base.clone();
        }

        let existing_entry = self.entries.read().await.get(tenant).cloned();
        let result = self.check_tenant(tenant, existing_entry).await;

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
                tracing::warn!(tenant = %tenant, error = %e, "skills.service_token_failed");
                return self.fallback_or_base(tenant, existing, now);
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
                tracing::warn!(tenant = %tenant, error = %e, "skills.tenant_unavailable");
                return self.fallback_or_base(tenant, existing, now);
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
            return entry.skills;
        }

        if !status.is_success() {
            tracing::warn!(tenant = %tenant, status = %status, "skills.tenant_unavailable");
            return self.fallback_or_base(tenant, existing, now);
        }

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let body = match resp.json::<Value>().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(tenant = %tenant, error = %e, "skills.invalid_json");
                return self.fallback_or_base(tenant, existing, now);
            }
        };

        let records: Vec<TenantSkillRecord> = body
            .get("skills")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value::<TenantSkillRecord>(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();

        let skills = if records.is_empty() {
            self.base.clone()
        } else {
            let view = apply_tenant_skills(&self.base, &records, |rec, reason| {
                tracing::warn!(tenant = %tenant, skill = %rec.name, version = rec.version, reason = %reason, "skills.tenant_skipped");
            });
            Arc::new(view)
        };

        let entry = TenantSkillEntry {
            etag,
            skills: skills.clone(),
            records,
            ok_at: now,
            checked_at,
        };
        self.entries.write().await.insert(tenant.to_string(), entry);
        skills
    }

    fn fallback_or_base(
        &self,
        tenant: &str,
        existing: Option<TenantSkillEntry>,
        _now: Instant,
    ) -> Arc<Vec<SkillConfig>> {
        if let Some(entry) = existing
            && entry.ok_at.elapsed() < self.stale_duration
        {
            tracing::info!(tenant = %tenant, "using cached skills during control outage");
            return entry.skills;
        }
        self.base.clone()
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

type PinnedSkills = (Instant, Arc<Vec<SkillConfig>>);

/// Pins the skills each conversation's turn started with.
#[derive(Default)]
pub struct TurnPins {
    pins: Mutex<HashMap<String, PinnedSkills>>,
}

impl TurnPins {
    pub async fn pin(&self, session_id: &str, skills: Arc<Vec<SkillConfig>>) {
        let mut pins = self.pins.lock().await;
        if pins.len() >= MAX_TURN_PINS {
            let oldest = pins
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                pins.remove(&k);
            }
        }
        pins.insert(session_id.to_string(), (Instant::now(), skills));
    }

    pub async fn get(&self, session_id: &str) -> Option<Arc<Vec<SkillConfig>>> {
        let pins = self.pins.lock().await;
        pins.get(session_id).map(|(_, s)| s.clone())
    }

    pub async fn remove(&self, session_id: &str) {
        let mut pins = self.pins.lock().await;
        pins.remove(session_id);
    }
}
