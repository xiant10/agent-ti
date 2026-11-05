//! agent-pi's `agent.json`, read unmodified (agent-pi's `config.ts` is the reference).
use crate::{ConfigError, Raw, Reader, Result, Snapshot, host::cluster_url, substitute};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub agent: String,
    pub bundle: Option<String>,
    pub models: BTreeMap<String, ModelConfig>,
    pub profiles: BTreeMap<String, ProfileConfig>,
    pub default_profile: String,
    pub instructions: Vec<Section>,
    pub plugins: Vec<PluginRef>,
    pub skills: Vec<SkillConfig>,
    pub tenant_skills: Option<TenantSkillsConfig>,
    pub tenant_connectors: Option<TenantConnectorsConfig>,
    pub mcp_servers: Vec<McpServerConfig>,
    pub grants: Option<GrantsConfig>,
    pub limits: AgentLimits,
    pub durable: Durable,
    /// Fields accepted for agent-pi compatibility whose feature isn't ported, for startup warnings.
    pub unported: Vec<String>,
    /// Host settings present in the file; the host file's values apply.
    pub host_fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantSkillsConfig {
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantConnectorsConfig {
    pub gateway_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSource {
    pub kind: String,
    pub version: u64,
    pub digest: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SkillConfig {
    pub name: String,
    pub description: String,
    pub tools: Vec<String>,
    pub keywords: Vec<String>,
    pub body: String,
    pub files: BTreeMap<String, String>,
    #[serde(default)]
    pub flow: Option<Value>,
    #[serde(default)]
    pub source: Option<SkillSource>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TenantSkillRecord {
    pub name: String,
    pub kind: String,
    pub version: u64,
    pub digest: String,
    pub content: TenantSkillContent,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TenantSkillContent {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub tools: Option<Vec<String>>,
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
    #[serde(default)]
    pub flow: Option<Value>,
    #[serde(default)]
    pub files: Option<BTreeMap<String, String>>,
}
#[derive(Clone, Debug)]
pub struct ModelConfig {
    pub model_id: String,
    pub base_url: String,
    pub api_key_env: Option<String>,
    pub reasoning: bool,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    /// Identity field (`agent`, `sessionId`, `turnId`, `userId`, `userName`, `tenantId`) to header name.
    pub identity_headers: BTreeMap<String, String>,
}
#[derive(Clone, Debug)]
pub struct ProfileConfig {
    pub model: String,
    pub thinking_level: String,
    pub instructions: Option<String>,
    pub always_on_tools: Vec<String>,
    pub max_active_tools: u64,
    pub preload_max_tools: u64,
    pub preload_min_score: f64,
    pub speech: bool,
    pub thinking_events: bool,
}
#[derive(Clone, Debug)]
pub struct Section {
    pub name: String,
    pub content: String,
    pub tag: bool,
}
#[derive(Clone, Debug)]
pub struct PluginRef {
    pub name: String,
    pub options: Value,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantsConfig {
    pub url: String,
    pub client_id: String,
    pub client_secret: String,
    pub actions: Vec<String>,
    pub audiences: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum McpAuth {
    /// Operator credentials in `headers`, or none.
    Static,
    Bearer {
        token: String,
        tenants: Vec<String>,
    },
    Grant {
        token_header: Option<String>,
    },
}
#[derive(Clone, Debug)]
pub struct McpServerConfig {
    pub name: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub auth: McpAuth,
    pub prefix: String,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub call_timeout_ms: u64,
    pub max_description_chars: usize,
    pub agent_context: bool,
}
#[derive(Clone, Debug)]
pub struct AgentLimits {
    pub max_sessions_per_user: u64,
    pub max_concurrent_runs_per_user: usize,
    pub max_input_chars: usize,
    pub max_tool_rounds: u32,
}
#[derive(Clone, Debug)]
pub struct Durable {
    pub stream_timeout_ms: u64,
    pub cache_retention: Option<String>,
    pub restart_limit: u32,
}

const TOP: &[&str] = &[
    "agent",
    "bundle",
    "port",
    "host",
    "instanceId",
    "storage",
    "auth",
    "grants",
    "models",
    "profiles",
    "progressModel",
    "defaultProfile",
    "subagents",
    "instructions",
    "plugins",
    "skillsDir",
    "tenantSkills",
    "tenantConnectors",
    "mcpServers",
    "limits",
    "mcpEndpoint",
    "http",
    "durable",
    "logLevel",
];
const HOST_FIELDS: &[&str] = &[
    "port",
    "host",
    "instanceId",
    "logLevel",
    "storage",
    "auth",
    "http",
];
const AUTH: &[&str] = &[
    "kind",
    "tokens",
    "algorithms",
    "jwksUrl",
    "hmacSecret",
    "publicKeyFiles",
    "issuer",
    "audience",
    "requiredScopes",
    "userClaim",
    "tenantClaim",
    "nameClaim",
    "clockToleranceSec",
    "requireExp",
    "maxTokenAgeSec",
];
const MODEL: &[&str] = &[
    "provider",
    "providerId",
    "modelId",
    "baseUrl",
    "apiKeyEnv",
    "api",
    "reasoning",
    "contextWindow",
    "maxOutputTokens",
    "identityHeaders",
];
const PROFILE: &[&str] = &[
    "model",
    "thinkingLevel",
    "instructionsFile",
    "alwaysOnTools",
    "maxActiveTools",
    "preload",
    "speech",
    "thinkingEvents",
    "taskReports",
    "expert",
];
const MCP: &[&str] = &[
    "name",
    "transport",
    "url",
    "headers",
    "auth",
    "prefix",
    "include",
    "exclude",
    "callTimeoutMs",
    "maxDescriptionChars",
    "tokenHeader",
    "agentContext",
    "token",
    "tokenFile",
    "tenants",
    "command",
    "args",
    "env",
    "cwd",
];
const LIMITS: &[&str] = &[
    "maxSessionsPerUser",
    "maxConcurrentRunsPerUser",
    "maxConcurrentRuns",
    "maxInputChars",
    "maxBodyBytes",
    "maxSubagentsPerSession",
    "maxBackgroundRunsPerUser",
    "maxTasksPerSession",
    "maxUnattendedRuns",
    "maxUnattendedTokens",
    "maxToolRounds",
];
const THINKING: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];
const IDENTITY: &[&str] = &[
    "agent",
    "sessionId",
    "turnId",
    "userId",
    "userName",
    "tenantId",
];
/// agent-pi's `GATEWAY_IDENTITY_HEADERS`.
const GATEWAY: &[(&str, &str)] = &[
    ("agent", "X-Agent"),
    ("sessionId", "X-Session-ID"),
    ("turnId", "X-Turn-ID"),
    ("userId", "X-User-Id"),
    ("userName", "X-User-Name"),
    ("tenantId", "X-Tenant-ID"),
];

fn agent_name(s: &str) -> bool {
    s.bytes().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.len() <= 64
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_-".contains(&c))
}
fn section_name(s: &str) -> bool {
    agent_name(s) && !["instructions", "skills", "profile"].contains(&s)
}

/// Reads the agent file of `snapshot`, and the files it names, through the snapshot.
pub fn load_agent(
    snapshot: &Snapshot,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<AgentConfig> {
    let source = snapshot.root().to_owned();
    let raw: Value = serde_json::from_str(&snapshot.read_root()?)
        .map_err(|e| ConfigError(format!("{source}: {e}")))?;
    let raw = substitute(raw, env, &source)?;
    let r = Reader { source: &source };
    let top = r.object(&raw, "config", TOP)?;
    let text = |relative: &str, at: &str| -> Result<String> {
        snapshot
            .read_text(relative)
            .map_err(|e| ConfigError(format!("{source}: {at} cannot be read: {e}")))
    };
    let mut unported = Vec::new();
    let mut note = |field: &str, feature: &str| unported.push(format!("{field} ({feature})"));

    let agent = r.string(top.get("agent"), "agent")?;
    if !agent_name(&agent) {
        return r.fail("agent", "must match ^[a-z][a-z0-9_-]*$");
    }
    // Host settings, checked for shape only: the host file's values apply.
    let host_fields: Vec<String> = HOST_FIELDS
        .iter()
        .filter(|f| top.contains_key(**f))
        .map(|f| f.to_string())
        .collect();
    r.optional_object(top.get("storage"), "storage", &["kind", "path"])?;
    r.optional_object(top.get("auth"), "auth", AUTH)?;
    r.optional_object(
        top.get("http"),
        "http",
        &["path", "allowedOrigins", "keepAliveMs"],
    )?;

    let grants_obj = r.optional_object(
        top.get("grants"),
        "grants",
        &["url", "clientId", "clientSecret", "actions", "audiences"],
    )?;
    let grants_url = r.optional_string(grants_obj.get("url"), "grants.url")?;
    let grants_config = if let Some(url) = grants_url.filter(|u| !u.is_empty()) {
        Some(GrantsConfig {
            url,
            client_id: r.string(grants_obj.get("clientId"), "grants.clientId")?,
            client_secret: r.string(grants_obj.get("clientSecret"), "grants.clientSecret")?,
            actions: r.strings(grants_obj.get("actions"), "grants.actions")?,
            audiences: r.strings(grants_obj.get("audiences"), "grants.audiences")?,
        })
    } else {
        None
    };
    let grants_on = grants_config.is_some();
    if grants_on
        && top
            .get("auth")
            .and_then(|a| a.get("kind"))
            .and_then(Value::as_str)
            .is_some_and(|k| k != "jwt")
    {
        return r.fail("grants", "needs auth.kind jwt");
    }

    let mut models = BTreeMap::new();
    for (name, value) in r.map(top.get("models"), "models")? {
        models.insert(name.clone(), model(&r, value, &format!("models.{name}"))?);
    }
    if models.is_empty() {
        return r.fail("models", "must name at least one model");
    }
    let model_name = |value: Option<&Value>, at: &str| -> Result<String> {
        let name = r.string(value, at)?;
        if !models.contains_key(&name) {
            return r.fail(at, &format!("names unknown model {name:?}"));
        }
        Ok(name)
    };

    let mut profiles = BTreeMap::new();
    for (name, value) in r.map(top.get("profiles"), "profiles")? {
        let at = format!("profiles.{name}");
        let p = r.object(value, &at, PROFILE)?;
        let preload = r.optional_object(
            p.get("preload"),
            &format!("{at}.preload"),
            &["maxTools", "minScore"],
        )?;
        if let Some(expert) = p.get("expert") {
            let e = r.object(expert, &format!("{at}.expert"), &["model", "thinkingLevel"])?;
            model_name(e.get("model"), &format!("{at}.expert.model"))?;
            r.one_of(
                e.get("thinkingLevel"),
                &format!("{at}.expert.thinkingLevel"),
                THINKING,
                Some("high"),
            )?;
            note(&format!("{at}.expert"), "consult_expert, step 8");
        }
        r.one_of(
            p.get("taskReports"),
            &format!("{at}.taskReports"),
            &["wake", "note"],
            Some("wake"),
        )?;
        let instructions = match r
            .optional_string(p.get("instructionsFile"), &format!("{at}.instructionsFile"))?
        {
            Some(file) => Some(text(&file, &format!("{at}.instructionsFile"))?),
            None => None,
        };
        let profile = ProfileConfig {
            model: model_name(p.get("model"), &format!("{at}.model"))?,
            thinking_level: r.one_of(
                p.get("thinkingLevel"),
                &format!("{at}.thinkingLevel"),
                THINKING,
                Some("off"),
            )?,
            instructions,
            always_on_tools: r.strings(p.get("alwaysOnTools"), &format!("{at}.alwaysOnTools"))?,
            max_active_tools: r.count(
                p.get("maxActiveTools"),
                &format!("{at}.maxActiveTools"),
                32,
            )?,
            preload_max_tools: r.count(
                preload.get("maxTools"),
                &format!("{at}.preload.maxTools"),
                0,
            )?,
            preload_min_score: r.number(
                preload.get("minScore"),
                &format!("{at}.preload.minScore"),
                4.0,
            )?,
            speech: r.boolean(p.get("speech"), &format!("{at}.speech"), false)?,
            thinking_events: r.boolean(
                p.get("thinkingEvents"),
                &format!("{at}.thinkingEvents"),
                false,
            )?,
        };
        if profile.speech {
            note(&format!("{at}.speech"), "speech events, step 8");
        }
        profiles.insert(name.clone(), profile);
    }
    if let Some(progress) = r.optional_string(top.get("progressModel"), "progressModel")? {
        model_name(Some(&Value::String(progress)), "progressModel")?;
        note("progressModel", "progress events");
    }
    let default_profile = r
        .optional_string(top.get("defaultProfile"), "defaultProfile")?
        .or_else(|| profiles.keys().next().cloned())
        .unwrap_or_default();
    if !profiles.contains_key(&default_profile) {
        return r.fail("defaultProfile", "must name a configured profile");
    }
    if top
        .get("subagents")
        .and_then(Value::as_object)
        .is_some_and(|s| !s.is_empty())
    {
        note("subagents", "step 10");
    }
    if top.contains_key("mcpEndpoint") {
        note("mcpEndpoint", "the agent's own MCP endpoint");
    }

    let mut instructions = Vec::new();
    for (index, value) in top
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let at = format!("instructions[{index}]");
        let entry = r.object(value, &at, &["name", "file", "tag"])?;
        let name = r.string(entry.get("name"), &format!("{at}.name"))?;
        if !section_name(&name) {
            return r.fail(
                &format!("{at}.name"),
                "must match ^[a-z][a-z0-9_-]*$ and not be instructions, skills or profile",
            );
        }
        let file = r.string(entry.get("file"), &format!("{at}.file"))?;
        instructions.push(Section {
            name,
            content: text(&file, &format!("{at}.file"))?,
            tag: r.boolean(entry.get("tag"), &format!("{at}.tag"), true)?,
        });
    }

    let mut plugins = Vec::new();
    for (index, value) in top
        .get("plugins")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let at = format!("plugins[{index}]");
        let entry = r.object(value, &at, &["use", "options"])?;
        let options = r
            .map(entry.get("options"), &format!("{at}.options"))?
            .clone();
        plugins.push(PluginRef {
            name: r.string(entry.get("use"), &format!("{at}.use"))?,
            options: Value::Object(options),
        });
    }

    let mut skills = Vec::new();
    if let Some(dir) = r.optional_string(top.get("skillsDir"), "skillsDir")? {
        let list = snapshot.list(&dir)?;
        let mut skill_dirs = std::collections::BTreeSet::new();
        for item in &list {
            if let Some((subdir, rest)) = item.split_once('/')
                && rest == "SKILL.md"
            {
                skill_dirs.insert(subdir.to_string());
            }
        }
        for item in &list {
            if !item.contains('/') && item.ends_with(".md") {
                let file_path = format!("{dir}/{item}");
                let content = snapshot.read_text(&file_path)?;
                let skill = parse_skill(&file_path, &content)?;
                skills.push(skill);
            }
        }
        for subdir in &skill_dirs {
            let skill_file = format!("{dir}/{subdir}/SKILL.md");
            let content = snapshot.read_text(&skill_file)?;
            let mut skill = parse_skill(&skill_file, &content)?;
            let prefix = format!("{subdir}/");
            let flow_file = format!("{dir}/{subdir}/flow.json");
            if let Ok(flow_content) = snapshot.read_text(&flow_file)
                && let Ok(flow_val) = serde_json::from_str::<Value>(&flow_content)
            {
                skill.flow = Some(flow_val);
            }
            for item in &list {
                if let Some(sub_path) = item.strip_prefix(&prefix)
                    && sub_path != "SKILL.md"
                    && sub_path != "flow.json"
                {
                    let file_name = sub_path.split('/').next_back().unwrap_or(sub_path);
                    if !file_name.starts_with('.') {
                        let file_path = format!("{dir}/{item}");
                        let file_content = snapshot.read_text(&file_path)?;
                        skill.files.insert(sub_path.to_string(), file_content);
                    }
                }
            }
            skills.push(skill);
        }
        skills.sort_by(|a, b| a.name.cmp(&b.name));
        for i in 1..skills.len() {
            if skills[i].name == skills[i - 1].name {
                return r.fail(
                    "skillsDir",
                    &format!("{dir}: two skills named {}", skills[i].name),
                );
            }
        }
    }
    let tenant_skills = r.optional_object(top.get("tenantSkills"), "tenantSkills", &["url"])?;
    let tenant_skills_url = r.optional_string(tenant_skills.get("url"), "tenantSkills.url")?;
    let tenant_skills_cfg = if let Some(url) = tenant_skills_url {
        if !url.is_empty() {
            if !grants_on {
                return r.fail(
                    "tenantSkills",
                    "needs a grants section, for the runtime's service account",
                );
            }
            cluster_url(&url, &source, "tenantSkills.url")?;
            Some(TenantSkillsConfig { url })
        } else {
            None
        }
    } else {
        None
    };
    let tenant_connectors = r.optional_object(
        top.get("tenantConnectors"),
        "tenantConnectors",
        &["gatewayUrl"],
    )?;
    let gateway_url = r.optional_string(
        tenant_connectors.get("gatewayUrl"),
        "tenantConnectors.gatewayUrl",
    )?;
    let tenant_connectors_cfg = if let Some(gw) = gateway_url {
        if !gw.is_empty() {
            if tenant_skills_cfg.is_none() {
                return r.fail(
                    "tenantConnectors",
                    "needs tenantSkills.url, where connectors are published",
                );
            }
            cluster_url(&gw, &source, "tenantConnectors.gatewayUrl")?;
            Some(TenantConnectorsConfig { gateway_url: gw })
        } else {
            None
        }
    } else {
        None
    };

    let mut mcp_servers = Vec::new();
    for (index, value) in top
        .get("mcpServers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let at = format!("mcpServers[{index}]");
        let s = r.object(value, &at, MCP)?;
        // An empty url turns a server off, so one config file serves both modes from the environment.
        if s.get("url").is_some_and(|u| u == "") {
            continue;
        }
        let name = r.string(s.get("name"), &format!("{at}.name"))?;
        if r.one_of(
            s.get("transport"),
            &format!("{at}.transport"),
            &["http", "stdio"],
            None,
        )? == "stdio"
        {
            note(&format!("{at} ({name})"), "stdio MCP servers");
            continue;
        }
        let auth = r.one_of(
            s.get("auth"),
            &format!("{at}.auth"),
            &["static", "grant", "bearer"],
            Some("static"),
        )?;
        if auth == "grant" && !grants_on {
            return r.fail(
                &format!("{at} ({name})"),
                "has auth: \"grant\", which needs a grants section",
            );
        }
        if auth != "bearer" && s.contains_key("tenants") {
            return r.fail(&format!("{at}.tenants"), "is only for auth \"bearer\"");
        }
        let auth = if auth == "bearer" {
            let token = match (
                r.optional_string(s.get("token"), &format!("{at}.token"))?,
                r.optional_string(s.get("tokenFile"), &format!("{at}.tokenFile"))?,
            ) {
                (Some(token), None) => token.trim().to_owned(),
                (None, Some(file)) => text(&file, &format!("{at}.tokenFile"))?.trim().to_owned(),
                _ => {
                    return r.fail(
                        &at,
                        "auth \"bearer\" needs exactly one of token and tokenFile",
                    );
                }
            };
            McpAuth::Bearer {
                token,
                tenants: r.strings(s.get("tenants"), &format!("{at}.tenants"))?,
            }
        } else if auth == "grant" {
            McpAuth::Grant {
                token_header: r
                    .optional_string(s.get("tokenHeader"), &format!("{at}.tokenHeader"))?,
            }
        } else {
            McpAuth::Static
        };
        let agent_context =
            r.boolean(s.get("agentContext"), &format!("{at}.agentContext"), false)?;
        let url = r.string(s.get("url"), &format!("{at}.url"))?;
        cluster_url(&url, &source, &format!("{at}.url"))?;
        let headers = r
            .map(s.get("headers"), &format!("{at}.headers"))?
            .iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|v| (k.clone(), v.to_owned()))
                    .ok_or(())
                    .or_else(|_| r.fail(&format!("{at}.headers.{k}"), "must be a string"))
            })
            .collect::<Result<_>>()?;
        mcp_servers.push(McpServerConfig {
            prefix: s
                .get("prefix")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{name}_")),
            name,
            url,
            headers,
            auth,
            include: r.strings(s.get("include"), &format!("{at}.include"))?,
            exclude: r.strings(s.get("exclude"), &format!("{at}.exclude"))?,
            call_timeout_ms: r.count(
                s.get("callTimeoutMs"),
                &format!("{at}.callTimeoutMs"),
                30_000,
            )?,
            max_description_chars: r.count(
                s.get("maxDescriptionChars"),
                &format!("{at}.maxDescriptionChars"),
                1024,
            )? as usize,
            agent_context,
        });
    }

    let l = r.optional_object(top.get("limits"), "limits", LIMITS)?;
    for field in ["maxConcurrentRuns", "maxBodyBytes"] {
        if l.contains_key(field) {
            r.number(l.get(field), &format!("limits.{field}"), 0.0)?;
        }
    }
    let limits = AgentLimits {
        max_sessions_per_user: r.count(
            l.get("maxSessionsPerUser"),
            "limits.maxSessionsPerUser",
            20,
        )?,
        max_concurrent_runs_per_user: r.count(
            l.get("maxConcurrentRunsPerUser"),
            "limits.maxConcurrentRunsPerUser",
            3,
        )? as usize,
        max_input_chars: r.count(l.get("maxInputChars"), "limits.maxInputChars", 32_000)? as usize,
        max_tool_rounds: r.count(l.get("maxToolRounds"), "limits.maxToolRounds", 12)? as u32,
    };
    if limits.max_tool_rounds == 0 || limits.max_tool_rounds > 128 {
        return r.fail("limits.maxToolRounds", "must be between 1 and 128");
    }
    let mut host_fields = host_fields;
    host_fields.extend(
        ["maxConcurrentRuns", "maxBodyBytes"]
            .iter()
            .filter(|f| l.contains_key(**f))
            .map(|f| format!("limits.{f}")),
    );

    let d = r.optional_object(
        top.get("durable"),
        "durable",
        &["stream", "compaction", "retry", "restartLimit"],
    )?;
    let stream = r.optional_object(
        d.get("stream"),
        "durable.stream",
        &["timeoutMs", "cacheRetention"],
    )?;
    let compaction = r.optional_object(
        d.get("compaction"),
        "durable.compaction",
        &["enabled", "reserveTokens", "keepRecentTokens"],
    )?;
    if r.boolean(
        compaction.get("enabled"),
        "durable.compaction.enabled",
        false,
    )? {
        note(
            "durable.compaction",
            "compaction, step 9; the last eight turns are sent",
        );
    }
    let retry = r.optional_object(
        d.get("retry"),
        "durable.retry",
        &["maxRetries", "baseDelayMs"],
    )?;
    if !retry.is_empty() {
        note("durable.retry", "model request retries");
    }
    let cache_retention = match stream.get("cacheRetention") {
        None => None,
        value => Some(r.one_of(
            value,
            "durable.stream.cacheRetention",
            &["none", "short", "long"],
            None,
        )?),
    };
    let durable = Durable {
        stream_timeout_ms: r
            .count(stream.get("timeoutMs"), "durable.stream.timeoutMs", 120_000)?
            .max(1),
        cache_retention,
        restart_limit: r
            .count(d.get("restartLimit"), "durable.restartLimit", 5)?
            .max(1) as u32,
    };
    let bundle = r.optional_string(top.get("bundle"), "bundle")?;
    Ok(AgentConfig {
        agent,
        bundle,
        models,
        profiles,
        default_profile,
        instructions,
        plugins,
        skills,
        tenant_skills: tenant_skills_cfg,
        tenant_connectors: tenant_connectors_cfg,
        mcp_servers,
        grants: grants_config,
        limits,
        durable,
        unported,
        host_fields,
    })
}

fn model(r: &Reader, value: &Value, at: &str) -> Result<ModelConfig> {
    let m: &Raw = r.object(value, at, MODEL)?;
    let provider = r.one_of(
        m.get("provider"),
        &format!("{at}.provider"),
        &["anthropic", "openai", "openai-compatible"],
        None,
    )?;
    if provider != "openai-compatible" {
        return r.fail(
            &format!("{at}.provider"),
            "is not ported yet: agent-ti calls openai-compatible endpoints only",
        );
    }
    if r.one_of(
        m.get("api"),
        &format!("{at}.api"),
        &["responses", "chat-completions"],
        Some("chat-completions"),
    )? != "chat-completions"
    {
        return r.fail(
            &format!("{at}.api"),
            "is not ported yet: agent-ti speaks chat-completions only",
        );
    }
    r.optional_string(m.get("providerId"), &format!("{at}.providerId"))?;
    let base_url = r.string(m.get("baseUrl"), &format!("{at}.baseUrl"))?;
    cluster_url(&base_url, r.source, &format!("{at}.baseUrl"))?;
    let identity_headers = match m.get("identityHeaders") {
        None => BTreeMap::new(),
        Some(Value::String(s)) if s == "none" => BTreeMap::new(),
        Some(Value::String(s)) if s == "gateway" => GATEWAY
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        Some(value) => {
            let fields = r.object(value, &format!("{at}.identityHeaders"), IDENTITY)?;
            fields
                .iter()
                .map(|(k, v)| {
                    Ok((
                        k.clone(),
                        r.string(Some(v), &format!("{at}.identityHeaders.{k}"))?,
                    ))
                })
                .collect::<Result<_>>()?
        }
    };
    let optional = |key: &str| -> Result<Option<u64>> {
        if m.contains_key(key) {
            r.count(m.get(key), &format!("{at}.{key}"), 0).map(Some)
        } else {
            Ok(None)
        }
    };
    Ok(ModelConfig {
        model_id: r.string(m.get("modelId"), &format!("{at}.modelId"))?,
        base_url,
        api_key_env: r.optional_string(m.get("apiKeyEnv"), &format!("{at}.apiKeyEnv"))?,
        reasoning: r.boolean(m.get("reasoning"), &format!("{at}.reasoning"), false)?,
        context_window: optional("contextWindow")?,
        max_output_tokens: optional("maxOutputTokens")?,
        identity_headers,
    })
}

fn skill_name(s: &str) -> bool {
    s.bytes().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_-".contains(&c))
}

fn unquote(value: &str) -> String {
    let s = value.trim();
    if s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FrontmatterVal {
    Single(String),
    List(Vec<String>),
}

impl FrontmatterVal {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Single(s) => Some(s.as_str()),
            Self::List(l) => (l.len() == 1).then_some(l[0].as_str()),
        }
    }
    pub fn to_list(&self) -> Vec<String> {
        match self {
            Self::Single(s) => {
                let s = s.trim();
                if s.is_empty() {
                    Vec::new()
                } else if s.contains(',') {
                    s.split(',')
                        .map(|p| unquote(p.trim()))
                        .filter(|p| !p.is_empty())
                        .collect()
                } else {
                    vec![unquote(s)]
                }
            }
            Self::List(l) => l.clone(),
        }
    }
}

pub fn parse_frontmatter(text: &str) -> BTreeMap<String, FrontmatterVal> {
    let mut fields: BTreeMap<String, FrontmatterVal> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indented = line.starts_with(' ') || line.starts_with('\t');
        let item = trimmed.strip_prefix("- ");
        if let Some(ref cur) = current {
            if let Some(item_part) = item {
                let val = unquote(item_part.trim());
                match fields.get_mut(cur) {
                    Some(FrontmatterVal::List(list)) => list.push(val),
                    Some(FrontmatterVal::Single(s)) => {
                        let prev = std::mem::take(s);
                        let list = if prev.is_empty() {
                            vec![val]
                        } else {
                            vec![prev, val]
                        };
                        fields.insert(cur.clone(), FrontmatterVal::List(list));
                    }
                    None => {
                        fields.insert(cur.clone(), FrontmatterVal::List(vec![val]));
                    }
                }
                continue;
            }
            if indented {
                if let Some(FrontmatterVal::Single(existing)) = fields.get_mut(cur) {
                    if existing.is_empty() {
                        *existing = trimmed.to_string();
                    } else {
                        existing.push(' ');
                        existing.push_str(trimmed);
                    }
                }
                continue;
            }
        }
        if let Some((key, val)) = line.split_once(':') {
            let key = key.trim().to_string();
            current = Some(key.clone());
            let value = val.trim();
            if value == ">" || value == "|" || value == ">-" || value == "|-" {
                fields.insert(key, FrontmatterVal::Single(String::new()));
                continue;
            }
            if value.starts_with('[') && value.ends_with(']') {
                let inner = &value[1..value.len() - 1];
                let list = inner
                    .split(',')
                    .map(|part| unquote(part.trim()))
                    .filter(|part| !part.is_empty())
                    .collect();
                fields.insert(key, FrontmatterVal::List(list));
            } else {
                fields.insert(key, FrontmatterVal::Single(unquote(value)));
            }
        }
    }
    fields
}

pub fn parse_skill(file: &str, content: &str) -> Result<SkillConfig> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return Err(ConfigError(format!(
            "{file}: must start with a --- frontmatter block holding name and description"
        )));
    }
    let rest = &trimmed[3..];
    let rest = rest
        .strip_prefix("\r\n")
        .or_else(|| rest.strip_prefix('\n'))
        .ok_or_else(|| {
            ConfigError(format!(
                "{file}: must start with a --- frontmatter block holding name and description"
            ))
        })?;
    let (fm_text, body_text) = if let Some(idx) = rest.find("\n---\n") {
        (&rest[..idx], &rest[idx + 5..])
    } else if let Some(idx) = rest.find("\r\n---\r\n") {
        (&rest[..idx], &rest[idx + 7..])
    } else if let Some(idx) = rest.find("\n---") {
        let b = &rest[idx + 4..];
        let b = b
            .strip_prefix("\r\n")
            .or_else(|| b.strip_prefix('\n'))
            .unwrap_or(b);
        (&rest[..idx], b)
    } else if let Some(idx) = rest.find("\r\n---") {
        let b = &rest[idx + 5..];
        let b = b
            .strip_prefix("\r\n")
            .or_else(|| b.strip_prefix('\n'))
            .unwrap_or(b);
        (&rest[..idx], b)
    } else {
        return Err(ConfigError(format!(
            "{file}: must start with a --- frontmatter block holding name and description"
        )));
    };

    let fields = parse_frontmatter(fm_text);
    let name = fields.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
        ConfigError(format!(
            "{file}: frontmatter \"name\" must match ^[a-z][a-z0-9_-]*$"
        ))
    })?;
    if !skill_name(name) {
        return Err(ConfigError(format!(
            "{file}: frontmatter \"name\" must match ^[a-z][a-z0-9_-]*$"
        )));
    }
    let description = fields
        .get("description")
        .and_then(|v| v.as_str())
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| {
            ConfigError(format!(
                "{file}: frontmatter needs a non-empty \"description\""
            ))
        })?;

    let tools = fields.get("tools").map(|v| v.to_list()).unwrap_or_default();
    let keywords = fields
        .get("keywords")
        .map(|v| v.to_list())
        .unwrap_or_default();
    let body = body_text.trim().to_string();

    Ok(SkillConfig {
        name: name.to_string(),
        description: description.to_string(),
        tools,
        keywords,
        body,
        files: BTreeMap::new(),
        flow: None,
        source: None,
    })
}

pub fn pattern_matches_glob(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }
    let mut remainder = text;
    if !parts[0].is_empty() {
        if !remainder.starts_with(parts[0]) {
            return false;
        }
        remainder = &remainder[parts[0].len()..];
    }
    for (i, part) in parts.iter().enumerate().skip(1) {
        if i == parts.len() - 1 && !part.is_empty() {
            return remainder.ends_with(part) && remainder.len() >= part.len();
        }
        if !part.is_empty() {
            if let Some(pos) = remainder.find(part) {
                remainder = &remainder[pos + part.len()..];
            } else {
                return false;
            }
        }
    }
    true
}

pub fn tool_allowed_by_skill(tool: &str, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return false;
    }
    patterns.iter().any(|pattern| {
        let bare = if let Some(idx) = pattern.find(':') {
            &pattern[idx + 1..]
        } else {
            pattern.as_str()
        };
        let underscored = pattern.replace(':', "_");
        if bare == "*" || bare == tool || pattern == tool || underscored == tool {
            return true;
        }
        for candidate in [bare, &underscored] {
            if candidate.contains('*') && pattern_matches_glob(candidate, tool) {
                return true;
            }
        }
        false
    })
}

pub fn is_valid_reference_file_path(path: &str) -> bool {
    if path.is_empty() || path.starts_with('/') || path.starts_with('.') || path.contains("..") {
        return false;
    }
    for segment in path.split('/') {
        if segment.is_empty() || segment.starts_with('.') {
            return false;
        }
        for (i, c) in segment.chars().enumerate() {
            if i == 0 {
                if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
                    return false;
                }
            } else if !c.is_ascii_alphanumeric() && c != '.' && c != '_' && c != '-' {
                return false;
            }
        }
    }
    true
}

fn extract_flow_tools(flow: &Value) -> Vec<String> {
    let mut tools = Vec::new();
    if let Some(nodes) = flow.get("nodes").and_then(Value::as_array) {
        for node in nodes {
            if let Some(call) = node.get("call").and_then(Value::as_object)
                && let Some(tool) = call.get("tool").and_then(Value::as_str)
            {
                tools.push(tool.to_string());
            }
        }
    }
    tools
}

fn validate_flow_patterns(flow: &Value) -> std::result::Result<(), String> {
    if let Some(nodes) = flow.get("nodes").and_then(Value::as_array) {
        for node in nodes {
            let node_id = node.get("id").and_then(Value::as_str).unwrap_or("step");
            if let Some(extract) = node.get("extract").and_then(Value::as_object) {
                for (field, spec) in extract {
                    if let Some(pat) = spec.get("pattern").and_then(Value::as_str)
                        && pat.chars().count() > 200
                    {
                        return Err(format!(
                            "step {node_id}: the pattern for {field} is longer than 200 characters"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn apply_tenant_skills(
    base: &[SkillConfig],
    records: &[TenantSkillRecord],
    mut skipped: impl FnMut(&TenantSkillRecord, &str),
) -> Vec<SkillConfig> {
    let mut by_name: BTreeMap<String, SkillConfig> =
        base.iter().map(|s| (s.name.clone(), s.clone())).collect();

    for record in records {
        let source = SkillSource {
            kind: record.kind.clone(),
            version: record.version,
            digest: record.digest.clone(),
        };
        if record.kind == "override" {
            let Some(builtin) = by_name.get(&record.name) else {
                skipped(record, "there's no built-in skill of that name");
                continue;
            };
            let Some(ref flow) = record.content.flow else {
                skipped(record, "an override needs a workflow");
                continue;
            };
            let tools = extract_flow_tools(flow);
            let mut tool_err = None;
            for tool in &tools {
                if !tool_allowed_by_skill(tool, &builtin.tools) {
                    tool_err = Some(format!(
                        "tool \"{tool}\" isn't one of the skill's tools ({})",
                        if builtin.tools.is_empty() {
                            "none".to_string()
                        } else {
                            builtin.tools.join(", ")
                        }
                    ));
                    break;
                }
            }
            if let Some(err) = tool_err {
                skipped(record, &format!("skill {}: {err}", record.name));
                continue;
            }
            if let Err(e) = validate_flow_patterns(flow) {
                skipped(record, &e);
                continue;
            }
            let mut overridden = builtin.clone();
            overridden.flow = Some(flow.clone());
            overridden.source = Some(source);
            by_name.insert(record.name.clone(), overridden);
        } else if record.kind == "custom" {
            if base.iter().any(|b| b.name == record.name) {
                skipped(record, "a built-in skill has that name");
                continue;
            }
            let description = record.content.description.as_deref().unwrap_or("").trim();
            if description.is_empty() {
                skipped(
                    record,
                    &format!("skill {}: needs a description", record.name),
                );
                continue;
            }
            let tools = record.content.tools.clone().unwrap_or_default();
            if let Some(ref flow) = record.content.flow {
                let flow_tools = extract_flow_tools(flow);
                let mut tool_err = None;
                for tool in &flow_tools {
                    if !tool_allowed_by_skill(tool, &tools) {
                        tool_err = Some(format!(
                            "tool \"{tool}\" isn't in the skill's tools ({})",
                            if tools.is_empty() {
                                "none".to_string()
                            } else {
                                tools.join(", ")
                            }
                        ));
                        break;
                    }
                }
                if let Some(err) = tool_err {
                    skipped(record, &format!("skill {}: {err}", record.name));
                    continue;
                }
                if let Err(e) = validate_flow_patterns(flow) {
                    skipped(record, &e);
                    continue;
                }
            }
            let mut files = BTreeMap::new();
            let mut bad_file = false;
            if let Some(ref rec_files) = record.content.files {
                for (p, text) in rec_files {
                    if !is_valid_reference_file_path(p) {
                        skipped(
                            record,
                            &format!("skill {}: {p:?} isn't a reference file path", record.name),
                        );
                        bad_file = true;
                        break;
                    }
                    files.insert(p.clone(), text.clone());
                }
            }
            if bad_file {
                continue;
            }
            let custom = SkillConfig {
                name: record.name.clone(),
                description: description.to_string(),
                tools,
                keywords: record.content.keywords.clone().unwrap_or_default(),
                body: record.content.body.clone().unwrap_or_default(),
                files,
                flow: record.content.flow.clone(),
                source: Some(source),
            };
            by_name.insert(record.name.clone(), custom);
        } else {
            skipped(record, &format!("unknown skill kind: {}", record.kind));
        }
    }

    let mut result: Vec<_> = by_name.into_values().collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));
    result
}

pub fn read_skill_file<'a>(
    skill: &'a SkillConfig,
    path: &str,
) -> std::result::Result<&'a str, String> {
    let clean = path.strip_prefix("./").unwrap_or(path);
    if clean.starts_with("..")
        || clean.contains("/..")
        || clean.starts_with('/')
        || clean.is_empty()
    {
        return Err(format!("{path:?} is outside skill {}", skill.name));
    }
    skill.files.get(clean).map(|s| s.as_str()).ok_or_else(|| {
        let available = skill.files.keys().cloned().collect::<Vec<_>>().join(", ");
        let list = if available.is_empty() {
            "none"
        } else {
            &available
        };
        format!("{path:?} isn't one of skill {}'s files: {list}", skill.name)
    })
}

pub fn render_skill_index(skills: &[SkillConfig]) -> String {
    let mut lines = Vec::new();
    lines.push(
        "Skills hold instructions and tools for specific tasks. When a request matches one, call load_skill with its exact".to_string(),
    );
    lines.push(
        "name before acting; it returns the instructions and enables the skill's tools. Do not guess a skill's contents.".to_string(),
    );
    for skill in skills {
        lines.push(format!("- {}: {}", skill.name, skill.description));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_parsing() {
        let text = "name: a\ndescription: Use when: the user asks. 注意：中文\ntools: [crm:list_*, \"cal:*\"]\nkeywords:\n  - visit\n  - 拜访";
        let fm = parse_frontmatter(text);
        assert_eq!(fm.get("name").unwrap().as_str(), Some("a"));
        assert_eq!(
            fm.get("description").unwrap().as_str(),
            Some("Use when: the user asks. 注意：中文")
        );
        assert_eq!(
            fm.get("tools").unwrap().to_list(),
            vec!["crm:list_*", "cal:*"]
        );
        assert_eq!(fm.get("keywords").unwrap().to_list(), vec!["visit", "拜访"]);
    }

    #[test]
    fn skill_parsing_and_files() {
        let content = "---\nname: visit\ndescription: Prepare visits.\ntools: [\"crm:*\"]\nkeywords: [visit]\n---\nBody two.";
        let mut skill = parse_skill("SKILL.md", content).unwrap();
        assert_eq!(skill.name, "visit");
        assert_eq!(skill.description, "Prepare visits.");
        assert_eq!(skill.tools, vec!["crm:*"]);
        assert_eq!(skill.keywords, vec!["visit"]);
        assert_eq!(skill.body, "Body two.");

        skill
            .files
            .insert("references/gates.md".into(), "gate rules".into());
        assert_eq!(
            read_skill_file(&skill, "references/gates.md").unwrap(),
            "gate rules"
        );
        assert!(
            read_skill_file(&skill, "../single.md")
                .unwrap_err()
                .contains("outside")
        );
    }

    #[test]
    fn test_apply_tenant_skills() {
        let weather = SkillConfig {
            name: "weather".into(),
            description: "Weather forecasts".into(),
            tools: vec!["crm:get_forecast".into()],
            keywords: vec![],
            body: "Built-in weather instructions.".into(),
            files: BTreeMap::new(),
            flow: Some(serde_json::json!({
                "nodes": [
                    { "id": "start", "kind": "start" },
                    { "id": "reply", "kind": "llm_reply" }
                ]
            })),
            source: None,
        };

        let override_rec = TenantSkillRecord {
            name: "weather".into(),
            kind: "override".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                description: None,
                body: None,
                tools: None,
                keywords: None,
                flow: Some(serde_json::json!({
                    "title": "Acme weather",
                    "nodes": [
                        { "id": "start", "kind": "start" },
                        { "id": "call", "kind": "tool", "call": { "tool": "crm_get_forecast" } },
                        { "id": "reply", "kind": "llm_reply" }
                    ]
                })),
                files: None,
            },
        };

        let mut files = BTreeMap::new();
        files.insert("refs/tiers.md".into(), "Tier A: over 1M ARR.".into());
        let vip = TenantSkillRecord {
            name: "vip-intake".into(),
            kind: "custom".into(),
            version: 1,
            digest: "sha256:v".into(),
            content: TenantSkillContent {
                description: Some("VIP intake".into()),
                body: Some("Ask for the VIP account number first.".into()),
                tools: Some(vec!["crm:get_forecast".into()]),
                keywords: Some(vec!["vip".into()]),
                flow: None,
                files: Some(files),
            },
        };

        let mut skipped = Vec::new();
        let view = apply_tenant_skills(
            std::slice::from_ref(&weather),
            &[override_rec, vip],
            |rec, reason| skipped.push(format!("{}: {reason}", rec.name)),
        );
        assert!(skipped.is_empty(), "unexpected skips: {skipped:?}");
        assert_eq!(view.len(), 2);
        assert_eq!(view[0].name, "vip-intake");
        assert_eq!(view[1].name, "weather");

        let weather_view = &view[1];
        assert_eq!(weather_view.body, weather.body);
        assert_eq!(weather_view.flow.as_ref().unwrap()["title"], "Acme weather");
        assert_eq!(weather_view.source.as_ref().unwrap().digest, "sha256:1");

        let vip_view = &view[0];
        assert_eq!(
            read_skill_file(vip_view, "refs/tiers.md").unwrap(),
            "Tier A: over 1M ARR."
        );
        assert_eq!(
            read_skill_file(vip_view, "./refs/tiers.md").unwrap(),
            "Tier A: over 1M ARR."
        );
        assert!(read_skill_file(vip_view, "../weather/SKILL.md").is_err());
        assert!(read_skill_file(vip_view, "/etc/passwd").is_err());
        assert!(read_skill_file(vip_view, "refs/other.md").is_err());
    }

    #[test]
    fn test_apply_tenant_skills_validation_skips() {
        let weather = SkillConfig {
            name: "weather".into(),
            description: "Weather forecasts".into(),
            tools: vec!["crm:get_forecast".into()],
            keywords: vec![],
            body: "Built-in weather instructions.".into(),
            files: BTreeMap::new(),
            flow: None,
            source: None,
        };

        let bad_tool_override = TenantSkillRecord {
            name: "weather".into(),
            kind: "override".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                flow: Some(serde_json::json!({
                    "nodes": [
                        { "id": "call", "kind": "tool", "call": { "tool": "delete_everything" } }
                    ]
                })),
                ..Default::default()
            },
        };

        let missing_override = TenantSkillRecord {
            name: "missing".into(),
            kind: "override".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                flow: Some(serde_json::json!({ "nodes": [] })),
                ..Default::default()
            },
        };

        let colliding_custom = TenantSkillRecord {
            name: "weather".into(),
            kind: "custom".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                description: Some("colliding".into()),
                ..Default::default()
            },
        };

        let bad_regex = TenantSkillRecord {
            name: "bad-regex".into(),
            kind: "custom".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                description: Some("bad regex".into()),
                flow: Some(serde_json::json!({
                    "nodes": [
                        { "id": "step1", "extract": { "q": { "pattern": "a".repeat(201) } } }
                    ]
                })),
                ..Default::default()
            },
        };

        let mut bad_files = BTreeMap::new();
        bad_files.insert("../x.md".into(), "bad".into());
        let bad_path = TenantSkillRecord {
            name: "bad-path".into(),
            kind: "custom".into(),
            version: 1,
            digest: "sha256:1".into(),
            content: TenantSkillContent {
                description: Some("bad path".into()),
                files: Some(bad_files),
                ..Default::default()
            },
        };

        let mut skipped = Vec::new();
        let view = apply_tenant_skills(
            std::slice::from_ref(&weather),
            &[
                bad_tool_override,
                missing_override,
                colliding_custom,
                bad_regex,
                bad_path,
            ],
            |rec, reason| skipped.push(format!("{}: {reason}", rec.name)),
        );
        assert_eq!(view, vec![weather]);
        assert_eq!(skipped.len(), 5);
    }
}
