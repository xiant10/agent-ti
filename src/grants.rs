//! Grant broker for `auth: "grant"` MCP servers and delegated callers.
//!
//! A user's request opens or extends a grant at the auth service. Tool calls get short-lived tokens
//! minted from that grant, allowing long-running turns to survive restarts and progress while the user is offline.
use crate::auth::Principal;
use crate::flights::Flights;
use agent_ti_config::GrantsConfig;
use agent_ti_store::{AgentStore, StoredGrant, UserScope};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const RENEW_BEFORE: Duration = Duration::from_secs(60);
const CHAIN_CACHE_DURATION: Duration = Duration::from_secs(60);
const DELEGATED_RENEW_WITHIN: Duration = Duration::from_secs(3600);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const ATTEMPTS: usize = 3;

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{:02X}", b);
        }
    }
    out
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum GrantError {
    #[error("The agent's access has expired; ask the user to send a message to renew it.")]
    Expired,
    #[error("The agent's access was stopped; end this task and tell the user.")]
    Stopped,
    #[error("the auth service is unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[derive(Clone)]
pub struct GrantBroker {
    config: GrantsConfig,
    client: reqwest::Client,
    by_conversation: Arc<RwLock<HashMap<String, StoredGrant>>>,
    tokens: Arc<RwLock<HashMap<String, CachedToken>>>,
    service_token: Arc<RwLock<Option<CachedToken>>>,
    confirmed: Arc<RwLock<HashMap<String, Instant>>>,
    in_flight_attach: Flights<String>,
    in_flight_mint: Flights<String>,
    in_flight_service: Flights<()>,
    retry_delay: Duration,
}

impl GrantBroker {
    pub fn new(config: GrantsConfig, client: reqwest::Client) -> Self {
        let base_url = config.url.trim_end_matches('/').to_string();
        Self {
            config: GrantsConfig {
                url: base_url,
                ..config
            },
            client,
            by_conversation: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            service_token: Arc::new(RwLock::new(None)),
            confirmed: Arc::new(RwLock::new(HashMap::new())),
            in_flight_attach: Flights::default(),
            in_flight_mint: Flights::default(),
            in_flight_service: Flights::default(),
            retry_delay: Duration::from_millis(200),
        }
    }

    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    pub fn config(&self) -> &GrantsConfig {
        &self.config
    }

    /// Attaches or extends a grant for the session, persisting it to `store`.
    pub async fn attach(
        &self,
        store: &AgentStore,
        scope: &UserScope,
        session_id: &str,
        principal: &Principal,
        traceparent: Option<&str>,
    ) -> Result<StoredGrant, GrantError> {
        let parent = principal.claims.get("grant").and_then(Value::as_str);

        // Deduplicate concurrent attach calls for the same session
        let _flight = match self.in_flight_attach.start(&session_id.to_string()).await {
            Some(flight) => Some(flight),
            None => match self.grant_of(store, session_id).await {
                Some(g) => return Ok(g),
                None => None,
            },
        };
        self.attach_internal(store, scope, session_id, principal, parent, traceparent)
            .await
    }

    async fn attach_internal(
        &self,
        store: &AgentStore,
        scope: &UserScope,
        session_id: &str,
        principal: &Principal,
        parent: Option<&str>,
        traceparent: Option<&str>,
    ) -> Result<StoredGrant, GrantError> {
        let current = self.grant_of(store, session_id).await;

        let mut grant: Option<StoredGrant> = None;
        if let Some(ref curr) = current
            && curr.parent.as_deref() == parent
        {
            grant = if parent.is_none() {
                self.extended(curr, &principal.token, traceparent).await?
            } else {
                self.lasting(curr).await?
            };
        }

        let grant = match grant {
            Some(g) => g,
            None => self.open(principal, parent, traceparent).await?,
        };

        self.by_conversation
            .write()
            .await
            .insert(session_id.to_string(), grant.clone());
        let _ = store.set_grant(scope, session_id, &grant).await;
        Ok(grant)
    }

    async fn extended(
        &self,
        current: &StoredGrant,
        subject_token: &str,
        traceparent: Option<&str>,
    ) -> Result<Option<StoredGrant>, GrantError> {
        let path = format!("/v1/grants/{}/extend", url_encode(&current.grant_id));
        let body = json!({ "subject_token": subject_token });
        match self.service_call(&path, Some(&body), traceparent).await {
            Ok(_) => Ok(Some(current.clone())),
            Err(GrantError::Unavailable(_)) => {
                // If auth service is temporarily unavailable, keep the current grant (it lasts hours)
                Ok(Some(current.clone()))
            }
            Err(_) => Ok(None),
        }
    }

    /// A delegated session can't extend its own grant, since only a person's token can. When
    /// little of it is left, the next turn opens a new one under the same parent, and the old
    /// one runs out by itself so that work it started isn't cut off.
    async fn lasting(&self, current: &StoredGrant) -> Result<Option<StoredGrant>, GrantError> {
        let service = self.service_token().await?;
        let url = format!(
            "{}/v1/grants/{}/chain",
            self.config.url,
            url_encode(&current.grant_id)
        );
        let response = self
            .client
            .get(&url)
            .header("authorization", format!("Bearer {service}"))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;
        // If auth service is temporarily unavailable, keep the current grant.
        let Ok(response) = response else {
            return Ok(Some(current.clone()));
        };
        let status = response.status();
        if status.as_u16() >= 500 {
            return Ok(Some(current.clone()));
        }
        let body: Value = response.json().await.unwrap_or(json!({}));
        if !status.is_success() {
            return if body["error"] == "grant_expired" {
                Ok(None)
            } else {
                Err(GrantError::Stopped)
            };
        }
        let ends = body["chain"]
            .as_array()
            .and_then(|chain| chain.last())
            .and_then(|leaf| leaf["expires_at"].as_str())
            .and_then(|at| at.parse::<jiff::Timestamp>().ok());
        let left = ends.map(|at| at.as_second() - jiff::Timestamp::now().as_second());
        Ok(match left {
            Some(left) if left < DELEGATED_RENEW_WITHIN.as_secs() as i64 => None,
            _ => Some(current.clone()),
        })
    }

    async fn open(
        &self,
        principal: &Principal,
        parent: Option<&str>,
        traceparent: Option<&str>,
    ) -> Result<StoredGrant, GrantError> {
        let (actions, audiences) = self.narrowed(principal);
        let mut body = json!({
            "subject_token": principal.token,
            "actions": actions,
            "audiences": audiences,
        });
        if let Some(p) = parent {
            body["parent"] = json!(p);
        }

        let resp = self
            .service_call("/v1/grants", Some(&body), traceparent)
            .await?;
        let grant_id = resp
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| GrantError::Unavailable("the auth service returned no grant id".into()))?
            .to_string();

        Ok(StoredGrant {
            grant_id,
            parent: parent.map(str::to_string),
        })
    }

    fn narrowed(&self, principal: &Principal) -> (Vec<String>, Vec<String>) {
        let has_grant_claim = principal.claims.get("grant").is_some();
        if !has_grant_claim {
            return (self.config.actions.clone(), self.config.audiences.clone());
        }

        let held_aud: Vec<String> = match &principal.claims["aud"] {
            Value::String(s) => vec![s.clone()],
            Value::Array(items) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            _ => vec![],
        };

        let actions = self
            .config
            .actions
            .iter()
            .filter(|a| principal.scopes.contains(a))
            .cloned()
            .collect();
        let audiences = self
            .config
            .audiences
            .iter()
            .filter(|a| held_aud.contains(a))
            .cloned()
            .collect();
        (actions, audiences)
    }

    /// Returns the stored grant for a session from memory or persistent store.
    pub async fn grant_of(&self, store: &AgentStore, session_id: &str) -> Option<StoredGrant> {
        if let Some(g) = self.by_conversation.read().await.get(session_id).cloned() {
            return Some(g);
        }
        if let Ok(Some(g)) = store.find_grant(session_id).await {
            self.by_conversation
                .write()
                .await
                .insert(session_id.to_string(), g.clone());
            return Some(g);
        }
        None
    }

    /// Mints or returns a cached token for the session's grant.
    pub async fn token(&self, store: &AgentStore, session_id: &str) -> Result<String, GrantError> {
        let grant = self
            .grant_of(store, session_id)
            .await
            .ok_or(GrantError::Stopped)?;
        self.mint(&grant.grant_id).await
    }

    /// The session's token for a model call, when its audience names the model gateway, so
    /// `ai-gateway-llm` reads the tenant from it. None without a grant, for a grant that doesn't
    /// list the gateway, or in an outage: the gateway then trusts the tenant header, as before.
    pub async fn model_token(&self, store: &AgentStore, session_id: &str) -> Option<String> {
        let token = self.token(store, session_id).await.ok()?;
        for_model_gateway(&token).then_some(token)
    }

    /// Mints a token for a given `grant_id`.
    pub async fn mint(&self, grant_id: &str) -> Result<String, GrantError> {
        {
            let tokens = self.tokens.read().await;
            if let Some(cached) = tokens.get(grant_id)
                && cached.expires_at > Instant::now() + RENEW_BEFORE
            {
                return Ok(cached.token.clone());
            }
        }

        // Deduplicate concurrent minting for the same grant_id
        let flight = self.in_flight_mint.start(&grant_id.to_string()).await;
        if flight.is_none() {
            let tokens = self.tokens.read().await;
            if let Some(cached) = tokens.get(grant_id) {
                return Ok(cached.token.clone());
            }
        }
        self.mint_internal(grant_id).await
    }

    async fn mint_internal(&self, grant_id: &str) -> Result<String, GrantError> {
        let request = json!({
            "grant_type": "agent_grant",
            "grant_id": grant_id,
        });

        match self.token_request(&request).await {
            Ok(cached) => {
                let tok = cached.token.clone();
                self.tokens
                    .write()
                    .await
                    .insert(grant_id.to_string(), cached);
                Ok(tok)
            }
            Err(e) => {
                self.tokens.write().await.remove(grant_id);
                Err(e)
            }
        }
    }

    /// Confirms that a delegated token's grant and ancestors are still live.
    pub async fn confirm_chain(&self, grant_id: &str) -> Result<(), GrantError> {
        {
            let conf = self.confirmed.read().await;
            if let Some(at) = conf.get(grant_id)
                && at.elapsed() < CHAIN_CACHE_DURATION
            {
                return Ok(());
            }
        }

        let service = self.service_token().await?;
        let url = format!(
            "{}/v1/grants/{}/chain",
            self.config.url,
            url_encode(grant_id)
        );
        let response = self
            .client
            .get(&url)
            .header("authorization", format!("Bearer {service}"))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| GrantError::Unavailable(e.to_string()))?;

        if response.status().as_u16() >= 500 {
            return Err(GrantError::Unavailable(format!(
                "the auth service answered {}",
                response.status()
            )));
        }
        if !response.status().is_success() {
            return Err(GrantError::Stopped);
        }

        let mut conf = self.confirmed.write().await;
        conf.retain(|_, at| at.elapsed() < CHAIN_CACHE_DURATION);
        conf.insert(grant_id.to_string(), Instant::now());
        Ok(())
    }

    /// Obtains this runtime's service token.
    pub async fn service_token(&self) -> Result<String, GrantError> {
        {
            let service = self.service_token.read().await;
            if let Some(cached) = service.as_ref()
                && cached.expires_at > Instant::now() + RENEW_BEFORE
            {
                return Ok(cached.token.clone());
            }
        }

        let flight = self.in_flight_service.start(&()).await;
        if flight.is_none() {
            let service = self.service_token.read().await;
            if let Some(cached) = service.as_ref() {
                return Ok(cached.token.clone());
            }
        }

        let req = json!({ "grant_type": "client_credentials" });
        let cached = self.token_request(&req).await?;
        let tok = cached.token.clone();
        *self.service_token.write().await = Some(cached);
        Ok(tok)
    }

    async fn token_request(&self, params: &Value) -> Result<CachedToken, GrantError> {
        let mut body = params.clone();
        body["client_id"] = json!(self.config.client_id);
        body["client_secret"] = json!(self.config.client_secret);

        let resp = self.post("/v1/token", None, Some(&body), None).await?;
        let access_token = resp
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| GrantError::Unavailable("the auth service returned no token".into()))?
            .to_string();
        let expires_in = resp
            .get("expires_in")
            .and_then(Value::as_u64)
            .unwrap_or(300);

        Ok(CachedToken {
            token: access_token,
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        })
    }

    async fn service_call(
        &self,
        path: &str,
        body: Option<&Value>,
        traceparent: Option<&str>,
    ) -> Result<Value, GrantError> {
        let token = self.service_token().await?;
        self.post(path, Some(&token), body, traceparent).await
    }

    async fn post(
        &self,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
        traceparent: Option<&str>,
    ) -> Result<Value, GrantError> {
        let mut last_error = "no attempt".to_string();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                let delay = self.retry_delay * (1 << (attempt - 1));
                tokio::time::sleep(delay).await;
            }

            let mut req = self
                .client
                .post(format!("{}{}", self.config.url, path))
                .header("content-type", "application/json")
                .timeout(REQUEST_TIMEOUT);

            if let Some(b) = bearer {
                req = req.header("authorization", format!("Bearer {b}"));
            }
            if let Some(tp) = traceparent {
                req = req.header("traceparent", tp);
            }
            if let Some(b) = body {
                req = req.json(b);
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = e.to_string();
                    continue;
                }
            };

            let status = response.status();
            if status.as_u16() >= 500 {
                last_error = format!("status {status}");
                continue;
            }

            let text = response.text().await.unwrap_or_default();
            let parsed: Value = if text.is_empty() {
                json!({})
            } else {
                serde_json::from_str(&text).unwrap_or(json!({}))
            };

            if status.is_success() {
                return Ok(parsed);
            }

            let error_code = parsed.get("error").and_then(Value::as_str).unwrap_or("");
            if error_code == "grant_expired" {
                return Err(GrantError::Expired);
            }
            if error_code == "invalid_grant" {
                return Err(GrantError::Stopped);
            }
            return Err(GrantError::Unavailable(format!(
                "the auth service refused the request: {}",
                if !error_code.is_empty() {
                    error_code
                } else {
                    status.as_str()
                }
            )));
        }

        Err(GrantError::Unavailable(format!(
            "the auth service is unavailable: {last_error}"
        )))
    }

    /// Gives a child session its own grant under its parent's, unless it has one, as on a rerun
    /// after a restart (agent-pi's `spawn`). It holds no secret: its tokens come through this broker.
    pub async fn spawn(
        &self,
        store: &AgentStore,
        scope: &UserScope,
        child: &str,
        parent: &str,
    ) -> Result<StoredGrant, GrantError> {
        if let Some(known) = self.grant_of(store, child).await {
            return Ok(known);
        }
        let Some(grant) = self.grant_of(store, parent).await else {
            return Err(GrantError::Stopped);
        };
        let token = self.mint(&grant.grant_id).await?;
        let claims: Value = token
            .split('.')
            .nth(1)
            .and_then(|p| {
                use base64::Engine;
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .ok()
            })
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let actions: Vec<&str> = claims["scope"]
            .as_str()
            .unwrap_or_default()
            .split(' ')
            .filter(|a| !a.is_empty())
            .collect();
        let audiences = match &claims["aud"] {
            Value::String(a) => json!([a]),
            Value::Array(a) => json!(a),
            _ => json!([]),
        };
        let body = json!({"subject_token": token, "actions": actions, "audiences": audiences});
        let resp = self.service_call("/v1/grants", Some(&body), None).await?;
        let grant_id = resp
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| GrantError::Unavailable("the auth service returned no grant id".into()))?
            .to_string();
        let spawned = StoredGrant {
            grant_id,
            parent: None,
        };
        self.by_conversation
            .write()
            .await
            .insert(child.to_string(), spawned.clone());
        let _ = store.set_grant(scope, child, &spawned).await;
        Ok(spawned)
    }

    /// Releases a session's grant.
    pub async fn release(&self, session_id: &str) {
        let grant = self.by_conversation.write().await.remove(session_id);
        if let Some(g) = grant {
            self.tokens.write().await.remove(&g.grant_id);
            let path = format!("/v1/grants/{}/revoke", url_encode(&g.grant_id));
            let _ = self.service_call(&path, None, None).await;
        }
    }

    /// Closes a session's grant and cleans up cached state.
    pub async fn close(&self, session_id: &str) {
        self.release(session_id).await;
    }
}

/// The model gateway's audience. A run's token goes with a model call only when it names this.
pub const MODEL_AUDIENCE: &str = "ai-gateway-llm";

/// Whether a token is for the model gateway. It reads the claims without checking them: the
/// gateway does that.
pub fn for_model_gateway(token: &str) -> bool {
    use base64::Engine;
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload) else {
        return false;
    };
    match serde_json::from_slice::<Value>(&bytes).ok().and_then(|c| c.get("aud").cloned()) {
        Some(Value::String(aud)) => aud == MODEL_AUDIENCE,
        Some(Value::Array(aud)) => aud.iter().any(|a| a == MODEL_AUDIENCE),
        _ => false,
    }
}
