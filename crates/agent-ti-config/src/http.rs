//! A remote config API: `GET <url>/<name>` returns the agent's directory as one JSON document.
//! The last good snapshot is kept on the volume, so a restart doesn't depend on the API being up.
use crate::{
    BoxFuture, ConfigError, ConfigSource, MAX_SNAPSHOT_BYTES, Result, Snapshot, SourceConfig, fail,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Document {
    version: String,
    root: String,
    #[serde(default)]
    files: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    files_base64: BTreeMap<String, String>,
}
impl Document {
    fn snapshot(self) -> Result<Snapshot> {
        let mut files: BTreeMap<String, Vec<u8>> = self
            .files
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect();
        for (path, data) in self.files_base64 {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|_| ConfigError(format!("{path}: invalid base64")))?;
            if files.insert(path.clone(), bytes).is_some() {
                return fail(format!("{path}: in both files and filesBase64"));
            }
        }
        Snapshot::from_files(self.version, self.root, files)
    }
}

pub struct HttpSource {
    name: String,
    config: SourceConfig,
    cache: PathBuf,
    client: reqwest::Client,
    env: fn(&str) -> Option<String>,
}
impl HttpSource {
    /// `cache` is the host's `storage.dir/config`; each agent's snapshot is kept as `<agent>.json` there.
    pub fn new(
        name: &str,
        config: SourceConfig,
        cache: &Path,
        env: fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ConfigError(e.to_string()))?;
        Ok(Self {
            name: name.into(),
            config,
            cache: cache.to_path_buf(),
            client,
            env,
        })
    }
    async fn fetch(&self, agent: &str) -> Result<Document> {
        let url = format!("{}/{agent}", self.config.url.trim_end_matches('/'));
        let mut request = self.client.get(&url).header("accept", "application/json");
        if let Some(env) = &self.config.token_env {
            let token = (self.env)(env)
                .filter(|t| !t.is_empty())
                .ok_or_else(|| ConfigError(format!("source {}: {env} is not set", self.name)))?;
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ConfigError(format!("{url}: {e}")))?;
        if !response.status().is_success() {
            return fail(format!("{url}: HTTP {}", response.status()));
        }
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"').to_owned());
        if response
            .content_length()
            .is_some_and(|n| n > 2 * MAX_SNAPSHOT_BYTES as u64)
        {
            return fail(format!("{url}: config document too large"));
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| ConfigError(format!("{url}: {e}")))?;
        if body.len() > 2 * MAX_SNAPSHOT_BYTES {
            return fail(format!("{url}: config document too large"));
        }
        let document: Document =
            serde_json::from_slice(&body).map_err(|e| ConfigError(format!("{url}: {e}")))?;
        if etag.is_some_and(|e| e != document.version) {
            return fail(format!("{url}: ETag differs from the document's version"));
        }
        Ok(document)
    }
}
impl ConfigSource for HttpSource {
    fn load<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Snapshot>> {
        Box::pin(async move {
            let cached = self.cache.join(format!("{agent}.json"));
            match self.fetch(agent).await {
                Ok(document) => {
                    let text =
                        serde_json::to_vec(&document).map_err(|e| ConfigError(e.to_string()))?;
                    let snapshot = document.snapshot()?;
                    std::fs::create_dir_all(&self.cache)
                        .map_err(|e| ConfigError(format!("{}: {e}", self.cache.display())))?;
                    let partial = cached.with_extension("json.partial");
                    std::fs::write(&partial, text)
                        .and_then(|_| std::fs::rename(&partial, &cached))
                        .map_err(|e| ConfigError(format!("{}: {e}", cached.display())))?;
                    Ok(snapshot)
                }
                Err(error) => {
                    let Ok(text) = std::fs::read(&cached) else {
                        return Err(error);
                    };
                    let document: Document = serde_json::from_slice(&text)
                        .map_err(|e| ConfigError(format!("{}: {e}", cached.display())))?;
                    let mut snapshot = document.snapshot()?;
                    snapshot.notice = Some(format!(
                        "config source {} is unavailable ({error}); using the cached version",
                        self.name
                    ));
                    Ok(snapshot)
                }
            }
        })
    }
}
