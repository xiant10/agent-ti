//! The host file: settings the agents in one process share, and where to load the agents from.
use crate::{ConfigError, Reader, Result, fail, substitute};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct HostConfig {
    pub port: u16,
    pub host: String,
    /// Unset: each agent's store keeps an ID of its own, as agent-pi does.
    pub instance_id: Option<String>,
    pub log_level: String,
    /// Holds one `<agent>.sqlite` per agent, and remote sources' config cache.
    pub storage_dir: PathBuf,
    pub auth: AuthConfig,
    pub limits: HostLimits,
    pub retention: RetentionConfig,
    pub backup: Option<BackupConfig>,
    pub allowed_origins: Vec<String>,
    pub sources: BTreeMap<String, SourceConfig>,
    pub agents: Vec<AgentLocation>,
    pub default_agent: Option<String>,
}
#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub algorithms: Vec<String>,
    pub jwks_url: String,
    pub issuer: Vec<String>,
    pub audience: Vec<String>,
    pub required_scopes: Vec<String>,
    pub user_claim: String,
    /// Unset: every caller is in tenant `default`.
    pub tenant_claim: Option<String>,
    pub name_claim: Option<String>,
    pub clock_tolerance_sec: u64,
}
#[derive(Clone, Debug)]
pub struct HostLimits {
    pub max_body_bytes: usize,
    pub max_concurrent_runs: usize,
    pub max_concurrent_runs_per_tenant: usize,
    pub max_queued_per_tenant: u64,
    pub max_queued_per_session: u64,
    pub max_sessions_per_tenant: u64,
    pub max_streams: usize,
    pub max_parallel_tools: usize,
    /// How long a plugin's hook may take. A security hook that takes longer refuses; an observer's
    /// is logged and ignored.
    pub hook_timeout_ms: u64,
}
/// What to remove, by age in days since a session was last active. Unset, nothing is removed.
#[derive(Clone, Debug, Default)]
pub struct RetentionConfig {
    /// A session idle this long loses its events and finished steps; its transcript stays.
    pub events_days: Option<u64>,
    /// A session closed and idle this long is removed whole.
    pub closed_sessions_days: Option<u64>,
}
/// Where to write online backups of each agent's store, and how often.
#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub dir: PathBuf,
    pub interval_minutes: u64,
    pub keep: usize,
}
#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub url: String,
    /// The bearer token is read from this variable at each load, never written to the cache.
    pub token_env: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentLocation {
    File(PathBuf),
    Remote { source: String, name: String },
}
impl std::fmt::Display for AgentLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "{}", path.display()),
            Self::Remote { source, name } => write!(f, "{source}:{name}"),
        }
    }
}

const TOP: &[&str] = &[
    "port",
    "host",
    "instanceId",
    "logLevel",
    "storage",
    "auth",
    "limits",
    "retention",
    "backup",
    "http",
    "sources",
    "agents",
    "defaultAgent",
];
const AUTH: &[&str] = &[
    "kind",
    "algorithms",
    "jwksUrl",
    "issuer",
    "audience",
    "requiredScopes",
    "userClaim",
    "tenantClaim",
    "nameClaim",
    "clockToleranceSec",
];
const LIMITS: &[&str] = &[
    "maxBodyBytes",
    "maxConcurrentRuns",
    "maxConcurrentRunsPerTenant",
    "maxQueuedPerTenant",
    "maxQueuedPerSession",
    "maxSessionsPerTenant",
    "maxStreams",
    "maxParallelTools",
    "hookTimeoutMs",
];
pub(crate) fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}
pub(crate) fn cluster_url(url: &str, source: &str, at: &str) -> Result<()> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return fail(format!("{source}: {at} must be an http(s) URL"));
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return fail(format!("{source}: {at} must not hold credentials"));
    }
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    let local = host == "localhost"
        || host == "127.0.0.1"
        || host == "[::1]"
        || !host.contains('.')
        || host.ends_with(".svc")
        || host.contains(".svc.");
    match scheme {
        "https" => Ok(()),
        "http" if local => Ok(()),
        _ => fail(format!(
            "{source}: {at} must be https, or http only for a cluster-local host"
        )),
    }
}

/// The `agent.json` of each subfolder of `dir` that has one, in name order.
fn agents_in(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let file = entry?.path().join("agent.json");
        if file.is_file() {
            found.push(file);
        }
    }
    found.sort();
    Ok(found)
}

/// Reads the host file. Relative paths in it are relative to its directory.
pub fn load_host(path: &Path, env: &dyn Fn(&str) -> Option<String>) -> Result<HostConfig> {
    let source = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError(format!("{source}: {e}")))?;
    let raw: Value =
        serde_json::from_str(&text).map_err(|e| ConfigError(format!("{source}: {e}")))?;
    let raw = substitute(raw, env, &source)?;
    let dir = path.parent().unwrap_or(Path::new("."));
    let r = Reader { source: &source };
    let top = r.object(&raw, "host", TOP)?;
    let port = r.count(top.get("port"), "port", 18120)?;
    if port == 0 || port > 65535 {
        return r.fail("port", "must be a TCP port");
    }
    let instance_id = r.optional_string(top.get("instanceId"), "instanceId")?;
    if instance_id.as_deref().is_some_and(|i| !identifier(i)) {
        return r.fail("instanceId", "must match [A-Za-z0-9_-]{1,64}");
    }
    let storage = r.optional_object(top.get("storage"), "storage", &["kind", "dir"])?;
    r.one_of(
        storage.get("kind"),
        "storage.kind",
        &["sqlite"],
        Some("sqlite"),
    )?;
    let storage_dir = dir.join(
        r.optional_string(storage.get("dir"), "storage.dir")?
            .unwrap_or_else(|| "data".into()),
    );

    let a = r.object(top.get("auth").unwrap_or(&Value::Null), "auth", AUTH)?;
    r.one_of(a.get("kind"), "auth.kind", &["jwt"], Some("jwt"))?;
    let algorithms = r.strings(a.get("algorithms"), "auth.algorithms")?;
    if algorithms.is_empty()
        || algorithms
            .iter()
            .any(|alg| !["RS256", "ES256"].contains(&alg.as_str()))
    {
        return r.fail(
            "auth.algorithms",
            "must list RS256 and/or ES256 (other algorithms are not ported yet)",
        );
    }
    let jwks_url = r.string(a.get("jwksUrl"), "auth.jwksUrl")?;
    cluster_url(&jwks_url, &source, "auth.jwksUrl")?;
    let auth = AuthConfig {
        algorithms,
        jwks_url,
        issuer: r.strings(a.get("issuer"), "auth.issuer")?,
        audience: r.strings(a.get("audience"), "auth.audience")?,
        required_scopes: r.strings(a.get("requiredScopes"), "auth.requiredScopes")?,
        user_claim: r
            .optional_string(a.get("userClaim"), "auth.userClaim")?
            .unwrap_or_else(|| "sub".into()),
        tenant_claim: r.optional_string(a.get("tenantClaim"), "auth.tenantClaim")?,
        name_claim: r.optional_string(a.get("nameClaim"), "auth.nameClaim")?,
        clock_tolerance_sec: r.count(a.get("clockToleranceSec"), "auth.clockToleranceSec", 60)?,
    };
    if auth.issuer.is_empty() || auth.audience.is_empty() {
        return r.fail("auth", "needs issuer and audience");
    }

    let l = r.optional_object(top.get("limits"), "limits", LIMITS)?;
    let positive = |key: &str, fallback: u64| -> Result<u64> {
        let n = r.count(l.get(key), &format!("limits.{key}"), fallback)?;
        if n == 0 {
            return r.fail(&format!("limits.{key}"), "must be at least 1");
        }
        Ok(n)
    };
    let limits = HostLimits {
        max_body_bytes: positive("maxBodyBytes", 1_000_000)? as usize,
        max_concurrent_runs: positive("maxConcurrentRuns", 200)? as usize,
        max_concurrent_runs_per_tenant: positive("maxConcurrentRunsPerTenant", 100)? as usize,
        max_queued_per_tenant: positive("maxQueuedPerTenant", 1024)?,
        max_queued_per_session: positive("maxQueuedPerSession", 16)?,
        max_sessions_per_tenant: positive("maxSessionsPerTenant", 10_000)?,
        max_streams: positive("maxStreams", 2000)? as usize,
        max_parallel_tools: positive("maxParallelTools", 4)? as usize,
        hook_timeout_ms: positive("hookTimeoutMs", 10_000)?,
    };
    let retained = r.optional_object(
        top.get("retention"),
        "retention",
        &["eventsDays", "closedSessionsDays"],
    )?;
    let days = |field: &str| -> Result<Option<u64>> {
        if !retained.contains_key(field) {
            return Ok(None);
        }
        let at = format!("retention.{field}");
        let days = r.count(retained.get(field), &at, 0)?;
        if days == 0 {
            return r.fail(&at, "must be at least 1");
        }
        Ok(Some(days))
    };
    let retention = RetentionConfig {
        events_days: days("eventsDays")?,
        closed_sessions_days: days("closedSessionsDays")?,
    };
    let backup = match top.get("backup") {
        None => None,
        Some(_) => {
            let raw = r.optional_object(
                top.get("backup"),
                "backup",
                &["dir", "intervalMinutes", "keep"],
            )?;
            let interval = r.count(raw.get("intervalMinutes"), "backup.intervalMinutes", 60)?;
            let keep = r.count(raw.get("keep"), "backup.keep", 24)?;
            if interval == 0 || keep == 0 {
                return r.fail("backup", "intervalMinutes and keep must be at least 1");
            }
            Some(BackupConfig {
                dir: dir.join(r.string(raw.get("dir"), "backup.dir")?),
                interval_minutes: interval,
                keep: keep as usize,
            })
        }
    };
    let http = r.optional_object(top.get("http"), "http", &["allowedOrigins"])?;

    let mut sources = BTreeMap::new();
    if let Some(value) = top.get("sources") {
        let Some(map) = value.as_object() else {
            return r.fail("sources", "must be an object");
        };
        for (name, value) in map {
            let at = format!("sources.{name}");
            if !identifier(name) || name == "file" {
                return r.fail(&at, "must be named [A-Za-z0-9_-]{1,64}, not \"file\"");
            }
            let s = r.object(value, &at, &["kind", "url", "tokenEnv"])?;
            r.one_of(s.get("kind"), &format!("{at}.kind"), &["http"], None)?;
            // An empty URL turns the source off, so one host file serves with and without it.
            let Some(url) = r.optional_string(s.get("url"), &format!("{at}.url"))? else {
                continue;
            };
            cluster_url(&url, &source, &format!("{at}.url"))?;
            sources.insert(
                name.clone(),
                SourceConfig {
                    url,
                    token_env: r.optional_string(s.get("tokenEnv"), &format!("{at}.tokenEnv"))?,
                },
            );
        }
    }
    let mut agents = Vec::new();
    for entry in r.strings(top.get("agents"), "agents")? {
        let location = match entry.split_once(':') {
            Some((name, rest)) if top.get("sources").and_then(|s| s.get(name)).is_some() => {
                if !sources.contains_key(name) {
                    return r.fail(
                        "agents",
                        &format!("reads {entry:?} from source {name:?}, which has no url"),
                    );
                }
                if !identifier(rest) {
                    return r.fail(
                        "agents",
                        &format!("{entry:?} must name an agent [A-Za-z0-9_-]{{1,64}}"),
                    );
                }
                AgentLocation::Remote {
                    source: name.into(),
                    name: rest.into(),
                }
            }
            _ => {
                let path = dir.join(entry.strip_prefix("file://").unwrap_or(&entry));
                if path.is_dir() {
                    // A folder of agents: each subfolder with an agent.json, by name.
                    let found = agents_in(&path).map_err(|e| {
                        ConfigError(format!("{source}: agents {}: {e}", path.display()))
                    })?;
                    if found.is_empty() {
                        return r.fail(
                            "agents",
                            &format!("{} has no <agent>/agent.json", path.display()),
                        );
                    }
                    agents.extend(found.into_iter().map(AgentLocation::File));
                    continue;
                }
                AgentLocation::File(path)
            }
        };
        agents.push(location);
    }
    if agents.is_empty() {
        return r.fail("agents", "must name at least one agent");
    }
    Ok(HostConfig {
        port: port as u16,
        host: r
            .optional_string(top.get("host"), "host")?
            .unwrap_or_else(|| "0.0.0.0".into()),
        instance_id,
        log_level: r.one_of(
            top.get("logLevel"),
            "logLevel",
            &["debug", "info", "warn", "error"],
            Some("info"),
        )?,
        storage_dir,
        auth,
        limits,
        retention,
        backup,
        allowed_origins: r.strings(http.get("allowedOrigins"), "http.allowedOrigins")?,
        sources,
        agents,
        default_agent: r.optional_string(top.get("defaultAgent"), "defaultAgent")?,
    })
}
