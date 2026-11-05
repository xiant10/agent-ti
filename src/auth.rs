//! JWT verification before any agent work, with agent-pi's auth settings from the host file.
use crate::adapters::bounded;
use agent_ti_config::AuthConfig;
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct Principal {
    pub user_id: String,
    pub tenant_id: String,
    pub scopes: Vec<String>,
    pub expires_at: u64,
    pub claims: Value,
    pub token: String,
}
impl Principal {
    pub fn has(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
    pub fn origin(&self) -> Value {
        let mut v = json!({"trigger":self.claims["trg"].as_str().unwrap_or("user")});
        if let Some(a) = self.claims["automation"].as_str() {
            v["automation"] = json!(a);
        }
        v
    }
}
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
#[derive(Debug)]
pub struct AuthError;
pub struct Auth {
    config: AuthConfig,
    algorithms: Vec<Algorithm>,
    client: reqwest::Client,
    cache: Mutex<Option<(Instant, Arc<JwkSet>)>>,
}
impl Auth {
    pub fn new(config: AuthConfig, client: reqwest::Client) -> Self {
        let algorithms = config
            .algorithms
            .iter()
            .filter_map(|a| a.parse().ok())
            .collect();
        Self {
            config,
            algorithms,
            client,
            cache: Mutex::new(None),
        }
    }
    pub async fn verify(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let token = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .filter(|t| t.len() <= 16384)
            .ok_or(AuthError)?;
        let header = decode_header(token).map_err(|_| AuthError)?;
        // `none` and HMAC are never in the list, so an HMAC key can't verify an asymmetric token.
        if !self.algorithms.contains(&header.alg) {
            return Err(AuthError);
        }
        let kid = header.kid.ok_or(AuthError)?;
        let mut cache = self.cache.lock().await;
        let refresh = match &*cache {
            None => true,
            Some((time, keys)) => {
                time.elapsed() > Duration::from_secs(300)
                    || (keys.find(&kid).is_none() && time.elapsed() > Duration::from_secs(5))
            }
        };
        if refresh {
            let response = self
                .client
                .get(&self.config.jwks_url)
                .send()
                .await
                .map_err(|_| AuthError)?;
            let bytes = bounded(response, 128 * 1024).await.map_err(|_| AuthError)?;
            let keys: JwkSet = serde_json::from_slice(&bytes).map_err(|_| AuthError)?;
            if keys.keys.len() > 64 {
                return Err(AuthError);
            }
            *cache = Some((Instant::now(), Arc::new(keys)));
        }
        let keys = cache.as_ref().ok_or(AuthError)?.1.clone();
        drop(cache);
        let jwk = keys.find(&kid).ok_or(AuthError)?;
        if jwk
            .common
            .key_algorithm
            .is_some_and(|a| a.to_string() != format!("{:?}", header.alg))
        {
            return Err(AuthError);
        }
        let key = DecodingKey::from_jwk(jwk).map_err(|_| AuthError)?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&self.config.issuer);
        validation.set_audience(&self.config.audience);
        validation.leeway = self.config.clock_tolerance_sec;
        validation.validate_nbf = true;
        let claims: Value = decode::<Value>(token, &key, &validation)
            .map_err(|_| AuthError)?
            .claims;
        let text = |name: &str| claims[name].as_str().map(str::to_owned);
        let user_id = text(&self.config.user_claim).ok_or(AuthError)?;
        let tenant_id = match &self.config.tenant_claim {
            Some(claim) => text(claim).ok_or(AuthError)?,
            None => "default".into(),
        };
        let scopes = match (&claims["scope"], &claims["scp"]) {
            (Value::String(s), _) => s.split_whitespace().map(str::to_owned).collect(),
            (_, Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            _ => vec![],
        };
        let expires_at = claims["exp"].as_u64().ok_or(AuthError)?;
        if user_id.is_empty()
            || tenant_id.is_empty()
            || user_id.len() > 256
            || tenant_id.len() > 128
        {
            return Err(AuthError);
        }
        Ok(Principal {
            user_id,
            tenant_id,
            scopes,
            expires_at,
            claims,
            token: token.to_owned(),
        })
    }
    /// The required scopes the principal lacks, if any.
    pub fn missing_scope(&self, p: &Principal) -> Option<&str> {
        self.config
            .required_scopes
            .iter()
            .find(|s| !p.has(s))
            .map(String::as_str)
    }
}
