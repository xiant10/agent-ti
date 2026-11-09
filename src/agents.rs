//! The agents one process hosts, each loaded from a config source snapshot.
use crate::{adapters, plugins};
use agent_ti_config::{
    AgentConfig, AgentLocation, ConfigSource, FileSource, HostConfig, HttpSource, McpAuth,
    McpServerConfig, ProfileConfig, Snapshot, load_agent, process_env,
};
use agent_ti_plugin_sdk::Plugin;
use agent_ti_store::{AgentStore, StoreError, StoreLimits};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, atomic::AtomicBool},
    time::{Duration, Instant},
};

/// Information about a tool provided by a tenant connector.
#[derive(Clone)]
pub struct TenantConnectorTarget {
    pub tenant_id: String,
    pub connector_id: String,
    pub connector_name: String,
}

/// A tool offered to the model: an MCP server's tool under the server's prefix.
pub struct ToolEntry {
    pub name: String,
    pub server: usize,
    pub mcp_name: String,
    pub description: String,
    pub schema: Value,
    pub validator: jsonschema::Validator,
    pub handler: Option<agent_ti_plugin_sdk::ToolHandler>,
    pub connector: Option<TenantConnectorTarget>,
}
/// A server's listed tools, `None` until a listing succeeds, and when it was last tried.
type Listing = (Option<Arc<Vec<ToolEntry>>>, Option<Instant>);
/// Each server's listing; a failed one is retried at most every 30 s.
struct Catalog {
    meta_tools: Arc<Vec<ToolEntry>>,
    servers: Vec<Listing>,
    plugin_tools: Arc<Vec<ToolEntry>>,
}
pub struct Agent {
    pub name: String,
    pub config: AgentConfig,
    /// The config version sessions record: the agent's `bundle`, or else its snapshot's version.
    pub version: String,
    pub instance_id: String,
    pub store: Arc<AgentStore>,
    /// Live events: committed ones after their commit, and deltas from memory.
    pub hub: Arc<crate::events::Hub>,
    pub plugins: Vec<Arc<dyn Plugin>>,
    /// The system prompt sections of each profile.
    pub instructions: BTreeMap<String, Vec<String>>,
    /// False after a storage failure: this agent stops admitting work, the others carry on.
    pub accepting: AtomicBool,
    pub grants: Option<Arc<crate::grants::GrantBroker>>,
    pub tenant_skills: Option<Arc<crate::tenant_skills::TenantSkills>>,
    pub tenant_connectors: Option<Arc<crate::tenant_connectors::TenantConnectors>>,
    pub turn_pins: Arc<crate::tenant_skills::TurnPins>,
    /// Wakes the background-task supervisor (`tasks::supervise`).
    pub task_wake: tokio::sync::Notify,
    /// Children running per session (`children::Slot`).
    pub children: std::sync::Mutex<std::collections::HashMap<String, usize>>,
    /// Sessions with a background compaction running.
    pub compacting: std::sync::Mutex<std::collections::HashSet<String>>,
    catalog: RwLock<Catalog>,
}
fn pattern(glob: &str, text: &str) -> bool {
    crate::catalog::glob_matches(glob, text)
}

pub const EXPERT_TOOL: &str = "consult_expert";
pub const SUBAGENT_PREFIX: &str = "agent_";
/// A tool that runs a child session, with its one string parameter.
fn child_tool(name: &str, description: &str, param: &str, about: &str) -> ToolEntry {
    let schema = serde_json::json!({
        "type": "object",
        "properties": { param: { "type": "string", "description": about } },
        "required": [param]
    });
    ToolEntry {
        name: name.into(),
        server: usize::MAX - 4,
        mcp_name: name.into(),
        description: description.into(),
        validator: jsonschema::validator_for(&schema).unwrap(),
        schema,
        handler: None,
        connector: None,
    }
}
/// A background-task tool with string parameters, and optionally `urgent`.
fn task_tool(name: &str, description: &str, params: &[(&str, &str)], urgent: bool) -> ToolEntry {
    let mut properties = serde_json::Map::new();
    for (param, about) in params {
        properties.insert(
            (*param).into(),
            serde_json::json!({"type":"string","description":about}),
        );
    }
    if urgent {
        properties.insert("urgent".into(), serde_json::json!({"type":"boolean","description":"Join the work it is doing now, after its current tool round. Otherwise this waits for its turn to end."}));
    }
    let required: Vec<&str> = params.iter().map(|(p, _)| *p).collect();
    let schema = serde_json::json!({"type":"object","properties":properties,"required":required});
    ToolEntry {
        name: name.into(),
        server: usize::MAX - 5,
        mcp_name: name.into(),
        description: description.into(),
        validator: jsonschema::validator_for(&schema).unwrap(),
        schema,
        handler: None,
        connector: None,
    }
}
fn meta_tools() -> Vec<ToolEntry> {
    let load_skill_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Exact skill name." },
            "file": { "type": "string", "description": "Relative path of a file inside the skill." },
            "query": { "type": "string", "description": "Optional user query or entity name to run the skill's deterministic flow immediately." }
        },
        "required": ["name"]
    });
    let load_skill_validator = jsonschema::validator_for(&load_skill_schema).unwrap();

    let search_tools_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "query": { "type": "string", "description": "What the tool should do." },
            "limit": { "type": "integer", "minimum": 1, "maximum": 20, "description": "At most 20." }
        },
        "required": ["query"]
    });
    let search_tools_validator = jsonschema::validator_for(&search_tools_schema).unwrap();

    vec![
        ToolEntry {
            name: "load_skill".into(),
            server: usize::MAX - 1,
            mcp_name: "load_skill".into(),
            description: "Load a skill by its exact name from the skills list: returns its instructions and enables its tools for the next step. Pass `file` to read a file the skill's instructions reference. Pass `query` when the skill has a deterministic flow to execute its lookup steps immediately.".into(),
            schema: load_skill_schema,
            validator: load_skill_validator,
            handler: None,
            connector: None,
        },
        ToolEntry {
            name: "search_tools".into(),
            server: usize::MAX - 2,
            mcp_name: "search_tools".into(),
            description: "Find tools for a task when no offered tool or skill fits. Describe the action in a few words, for example \"list open invoices for a customer\". The best matches are enabled for the next step.".into(),
            schema: search_tools_schema,
            validator: search_tools_validator,
            handler: None,
            connector: None,
        },
    ]
}
impl Agent {
    pub async fn load(host: &HostConfig, snapshot: Snapshot) -> Result<Self, String> {
        let config = load_agent(&snapshot, &process_env).map_err(|e| e.to_string())?;
        let name = config.agent.clone();
        if let Some(notice) = &snapshot.notice {
            tracing::warn!(agent = %name, "{notice}");
        }
        for field in &config.unported {
            tracing::warn!(agent = %name, "agent.json sets {field}, which agent-ti doesn't support yet; it is ignored");
        }
        if !config.host_fields.is_empty() {
            tracing::info!(agent = %name, fields = %config.host_fields.join(", "), "agent.json host settings are ignored; the host file's apply");
        }
        let mut loaded: Vec<Arc<dyn Plugin>> = Vec::new();
        for (index, p) in config.plugins.iter().enumerate() {
            let factory = plugins::catalog(&p.name).ok_or_else(|| {
                format!("{name}: plugins[{index}] names unknown plugin {:?}", p.name)
            })?;
            let plugin = factory(&p.options, &snapshot)
                .map_err(|e| format!("{name}: plugins[{index}] ({}): {e}", p.name))?;
            tracing::info!(agent = %name, plugin = plugin.name(), version = plugin.version(), "plugin loaded");
            loaded.push(plugin);
        }
        let mut plugin_names = std::collections::HashSet::new();
        for plugin in &loaded {
            if !plugin_names.insert(plugin.name().to_string()) {
                return Err(format!("{name}: two plugins named {}", plugin.name()));
            }
        }
        let core_methods = [
            "session.create",
            "session.list",
            "session.get",
            "session.messages",
            "session.tool",
            "session.update",
            "session.close",
            "session.prompt",
            "session.cancel",
            "agents.list",
            "skills.list",
        ];
        let mut registered_rpc = std::collections::HashSet::new();
        for plugin in &loaded {
            for method in plugin.rpc_methods() {
                if core_methods.contains(&method.as_str()) {
                    return Err(format!(
                        "{name}: plugin method {method} already defined by core"
                    ));
                }
                if !registered_rpc.insert(method.clone()) {
                    return Err(format!(
                        "{name}: two plugins registered RPC method {method}"
                    ));
                }
            }
        }
        let mut instructions = BTreeMap::new();
        for (profile_name, profile) in &config.profiles {
            let mut sections: Vec<(String, String, bool)> = config
                .instructions
                .iter()
                .map(|s| (s.name.clone(), s.content.clone(), s.tag))
                .collect();
            if !config.skills.is_empty() {
                sections.push((
                    "skills".into(),
                    agent_ti_config::render_skill_index(&config.skills),
                    true,
                ));
            }
            if let Some(text) = &profile.instructions {
                sections.push(("profile".into(), text.clone(), true));
            }
            for plugin in &loaded {
                for sec in plugin.instructions(profile_name) {
                    sections.push((sec.name, sec.content, sec.tag));
                }
            }
            let rendered = sections
                .into_iter()
                .filter(|(_, text, _)| !text.trim().is_empty())
                .map(|(key, text, tag)| {
                    if tag {
                        format!("<{key}>\n{}\n</{key}>", text.trim_end())
                    } else {
                        text
                    }
                })
                .collect();
            instructions.insert(profile_name.clone(), rendered);
        }
        let mut plugin_tools = Vec::new();
        for plugin in &loaded {
            for tool in plugin.tools() {
                let validator = jsonschema::validator_for(&tool.parameters)
                    .map_err(|e| format!("invalid schema for plugin tool {}: {e}", tool.name))?;
                plugin_tools.push(ToolEntry {
                    name: tool.name.clone(),
                    server: usize::MAX,
                    mcp_name: tool.name.clone(),
                    description: tool.description,
                    schema: tool.parameters,
                    validator,
                    handler: Some(tool.handler),
                    connector: None,
                });
            }
        }
        std::fs::create_dir_all(&host.storage_dir)
            .map_err(|e| format!("{}: {e}", host.storage_dir.display()))?;
        let path = host.storage_dir.join(format!("{name}.sqlite"));
        let limits = StoreLimits {
            max_sessions_per_tenant: host.limits.max_sessions_per_tenant,
            max_sessions_per_user: config.limits.max_sessions_per_user,
            max_queued_per_tenant: host.limits.max_queued_per_tenant,
            max_queued_per_session: host.limits.max_queued_per_session,
            max_input_bytes: config.limits.max_input_chars * 4,
            restart_limit: config.durable.restart_limit,
        };
        let store = AgentStore::open(&path, &name, limits)
            .await
            .map_err(|e| match e {
                StoreError::Conflict => {
                    format!("{}: another process owns this store", path.display())
                }
                StoreError::WrongAgent(owner) => format!(
                    "{}: this is agent {owner:?}'s store, not {name:?}'s",
                    path.display()
                ),
                e => format!("{}: {e:?}", path.display()),
            })?;
        let hub = Arc::new(crate::events::Hub::default());
        let feed = hub.clone();
        store.on_commit(Box::new(move |events| feed.committed(events)));
        let instance_id = store
            .instance_id(host.instance_id.as_deref())
            .await
            .map_err(|e| format!("{name}: instance ID: {e:?}"))?;
        let version = config.bundle.clone().unwrap_or_else(|| snapshot.version());
        let servers = config.mcp_servers.iter().map(|_| (None, None)).collect();
        let mut meta = meta_tools();
        if config.profiles.values().any(|p| p.expert.is_some()) {
            meta.push(child_tool(
                EXPERT_TOOL,
                "Ask a slower, stronger reasoning model a hard question: multi-step analysis, planning, or a decision with trade-offs. Include the facts it needs. Returns its conclusion. Do not use it for simple lookups.",
                "question",
                "The question, with every fact needed to answer it.",
            ));
        }
        // Each sub-agent is a catalog tool, so skills, search, preload and `alwaysOnTools` reach
        // it as any tool; its code is in the worker.
        for sub in &config.subagents {
            plugin_tools.push(child_tool(
                &format!("{SUBAGENT_PREFIX}{}", sub.name),
                &sub.description,
                "task",
                "The task, with everything the agent needs: it cannot see this conversation.",
            ));
        }
        if config.subagents.iter().any(|s| s.background) {
            let task_id = ("taskId", "The task, such as t1.");
            meta.push(task_tool(crate::tasks::SEND_TOOL, "Send a message to a background task: the answer to its question, or more work. Its reply arrives as a report.", &[task_id, ("message", "The whole message. The task cannot see this conversation.")], true));
            meta.push(task_tool(
                crate::tasks::LIST_TOOL,
                "List this session's background tasks, with each one's status and last report.",
                &[],
                false,
            ));
            meta.push(task_tool(
                crate::tasks::STOP_TOOL,
                "End a background task and its work. It cannot be resumed.",
                &[task_id],
                false,
            ));
            let mut note = task_tool(
                crate::tasks::NOTE_TOOL,
                "Tell the agent that started you something while you keep working. It reads this at its next step, unless urgent.",
                &[(
                    "message",
                    "What it should know. It cannot see this conversation.",
                )],
                true,
            );
            note.schema["properties"]["urgent"]["description"] =
                serde_json::json!("Interrupt it now. Only for something that changes its plan.");
            note.validator = jsonschema::validator_for(&note.schema).unwrap();
            meta.push(note);
        }
        let meta = Arc::new(meta);
        let grants = config.grants.as_ref().map(|g| {
            Arc::new(crate::grants::GrantBroker::new(
                g.clone(),
                reqwest::Client::new(),
            ))
        });
        let tenant_skills = if let (Some(ts_cfg), Some(broker)) = (&config.tenant_skills, &grants) {
            Some(Arc::new(crate::tenant_skills::TenantSkills::new(
                Arc::new(config.skills.clone()),
                ts_cfg.url.clone(),
                broker.clone(),
                reqwest::Client::new(),
            )))
        } else {
            None
        };
        let tenant_connectors = if let (Some(tc_cfg), Some(ts_cfg), Some(broker)) =
            (&config.tenant_connectors, &config.tenant_skills, &grants)
        {
            Some(Arc::new(crate::tenant_connectors::TenantConnectors::new(
                ts_cfg.url.clone(),
                tc_cfg.gateway_url.clone(),
                broker.clone(),
                reqwest::Client::new(),
            )))
        } else {
            None
        };
        if let (Some(ts), Some(tc)) = (&tenant_skills, &tenant_connectors) {
            let tc_clone = tc.clone();
            ts.add_listener(Box::new(move |tenant: &str| {
                let tc = tc_clone.clone();
                let tenant = tenant.to_string();
                tokio::spawn(async move {
                    tc.changed(&tenant).await;
                });
            }))
            .await;
        }
        let turn_pins = Arc::new(crate::tenant_skills::TurnPins::default());
        Ok(Self {
            name,
            version,
            instance_id,
            store: Arc::new(store),
            hub,
            plugins: loaded,
            instructions,
            accepting: AtomicBool::new(true),
            catalog: RwLock::new(Catalog {
                meta_tools: meta,
                servers,
                plugin_tools: Arc::new(plugin_tools),
            }),
            grants,
            tenant_skills,
            tenant_connectors,
            turn_pins,
            compacting: Default::default(),
            children: Default::default(),
            task_wake: Default::default(),
            config,
        })
    }
    pub fn filter_and_validate_tools(
        &self,
        index: usize,
        server: &McpServerConfig,
        tools: Vec<adapters::McpTool>,
    ) -> Vec<ToolEntry> {
        let mut entries = Vec::new();
        for tool in tools {
            let name = format!("{}{}", server.prefix, tool.name);
            let qualified = format!("{}:{}", server.name, tool.name);
            if (!server.include.is_empty()
                && !server
                    .include
                    .iter()
                    .any(|p| pattern(p, &name) || pattern(p, &qualified)))
                || server
                    .exclude
                    .iter()
                    .any(|p| pattern(p, &name) || pattern(p, &qualified))
            {
                continue;
            }
            let Ok(validator) = jsonschema::validator_for(&tool.input_schema) else {
                tracing::warn!(agent = %self.name, server = %server.name, tool = %tool.name, "tool schema is invalid; tool skipped");
                continue;
            };
            let mut description = tool.description;
            if description.chars().count() > server.max_description_chars {
                description = description
                    .chars()
                    .take(server.max_description_chars.saturating_sub(3))
                    .collect::<String>()
                    + "...";
            }
            entries.push(ToolEntry {
                name,
                server: index,
                mcp_name: tool.name,
                description,
                schema: tool.input_schema,
                validator,
                handler: None,
                connector: None,
            });
        }
        entries
    }

    pub async fn relist_server(&self, index: usize, client: &reqwest::Client) {
        if index >= self.config.mcp_servers.len() {
            return;
        }
        let server = &self.config.mcp_servers[index];
        match adapters::mcp_list(client, server).await {
            Ok(tools) => {
                let entries = self.filter_and_validate_tools(index, server, tools);
                tracing::info!(agent = %self.name, server = %server.name, tools = entries.len(), "re-listed MCP server on list_changed");
                self.catalog.write().unwrap().servers[index].0 = Some(Arc::new(entries));
            }
            Err(e) => {
                tracing::warn!(agent = %self.name, server = %server.name, error = %e, "failed to re-list MCP server on list_changed");
            }
        }
    }

    /// Starts background Streamable HTTP SSE notification listeners for all configured MCP servers.
    pub fn start_mcp_listeners(self: &Arc<Self>, client: &reqwest::Client) {
        for (index, server) in self.config.mcp_servers.iter().enumerate() {
            let agent = Arc::downgrade(self);
            let srv = server.clone();
            let http_client = client.clone();
            tokio::spawn(async move {
                Self::mcp_listener_loop(agent, index, srv, http_client).await;
            });
        }
    }

    async fn mcp_listener_loop(
        agent: std::sync::Weak<Self>,
        index: usize,
        server: McpServerConfig,
        client: reqwest::Client,
    ) {
        let mut backoff = Duration::from_secs(1);
        loop {
            // Check if agent is still alive
            if agent.upgrade().is_none() {
                break;
            }

            // 1. Initialize MCP session
            let init_req = {
                let mut r = client
                    .post(&server.url)
                    .timeout(Duration::from_secs(10))
                    .header("accept", "application/json, text/event-stream")
                    .header("mcp-protocol-version", "2025-03-26")
                    .json(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-03-26",
                            "capabilities": {},
                            "clientInfo": {
                                "name": "agent-ti",
                                "version": env!("CARGO_PKG_VERSION")
                            }
                        }
                    }));
                for (name, value) in &server.headers {
                    r = r.header(name, value);
                }
                if let McpAuth::Bearer { token, .. } = &server.auth {
                    r = r.bearer_auth(token);
                }
                r
            };

            let Ok(init_resp) = init_req.send().await else {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            };

            let session_id = init_resp
                .headers()
                .get("mcp-session-id")
                .and_then(|h| h.to_str().ok())
                .map(str::to_string);

            let Some(session_id) = session_id else {
                // If server doesn't provide mcp-session-id, SSE notification streaming is not active
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            };

            // 2. Send notifications/initialized
            let mut init_notif = client
                .post(&server.url)
                .timeout(Duration::from_secs(10))
                .header("mcp-session-id", &session_id)
                .header("mcp-protocol-version", "2025-03-26")
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized"
                }));
            for (name, value) in &server.headers {
                init_notif = init_notif.header(name, value);
            }
            if let McpAuth::Bearer { token, .. } = &server.auth {
                init_notif = init_notif.bearer_auth(token);
            }
            let _ = init_notif.send().await;

            // 3. Open GET SSE stream
            let mut sse_req = client
                .get(&server.url)
                .header("accept", "text/event-stream")
                .header("mcp-session-id", &session_id)
                .header("mcp-protocol-version", "2025-03-26");
            for (name, value) in &server.headers {
                sse_req = sse_req.header(name, value);
            }
            if let McpAuth::Bearer { token, .. } = &server.auth {
                sse_req = sse_req.bearer_auth(token);
            }

            let Ok(resp) = sse_req.send().await else {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            };

            if !resp.status().is_success() {
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }

            backoff = Duration::from_secs(1);
            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();
            use futures_util::StreamExt;
            while let Some(chunk_res) = stream.next().await {
                let Ok(chunk) = chunk_res else { break };
                let Ok(text) = std::str::from_utf8(&chunk) else {
                    continue;
                };
                buffer.push_str(text);
                while let Some(pos) = buffer.find("\n\n") {
                    let event = buffer[..pos].to_string();
                    buffer = buffer[pos + 2..].to_string();
                    if event.contains("tools/list_changed") {
                        if let Some(ag) = agent.upgrade() {
                            ag.relist_server(index, &client).await;
                        } else {
                            return;
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Lists the tools of servers not listed yet. A server that is down is retried on a later run.
    pub async fn refresh_tools(&self, client: &reqwest::Client) {
        for (index, server) in self.config.mcp_servers.iter().enumerate() {
            let due = {
                let catalog = self.catalog.read().unwrap();
                let (tools, tried) = &catalog.servers[index];
                tools.is_none() && tried.is_none_or(|t| t.elapsed() > Duration::from_secs(30))
            };
            if !due {
                continue;
            }
            self.catalog.write().unwrap().servers[index].1 = Some(Instant::now());
            match adapters::mcp_list(client, server).await {
                Ok(tools) => {
                    let entries = self.filter_and_validate_tools(index, server, tools);
                    tracing::info!(agent = %self.name, server = %server.name, tools = entries.len(), "MCP server listed");
                    self.catalog.write().unwrap().servers[index].0 = Some(Arc::new(entries));
                }
                Err(e) => {
                    tracing::warn!(agent = %self.name, server = %server.name, error = %e, "MCP server unavailable; retrying later");
                }
            }
        }
    }
    pub fn platform_tool_names(&self) -> Vec<String> {
        let catalog = self.catalog.read().unwrap();
        let mut names = Vec::new();
        for t in catalog.meta_tools.iter() {
            names.push(t.name.clone());
        }
        for t in catalog.plugin_tools.iter() {
            names.push(t.name.clone());
        }
        for (tools, _) in &catalog.servers {
            if let Some(list) = tools {
                for t in list.iter() {
                    names.push(t.name.clone());
                }
            }
        }
        names
    }

    pub fn match_tools_sync(&self, patterns: &[String], tenant: &str) -> Vec<String> {
        if patterns.is_empty() {
            return Vec::new();
        }
        let catalog = self.catalog.read().unwrap();
        let servers = &self.config.mcp_servers;
        let mut matched = Vec::new();

        for (index, (listing, _)) in catalog.servers.iter().enumerate() {
            let server = &servers[index];
            let tenant_allowed = match &server.auth {
                McpAuth::Bearer { tenants, .. } => tenants.iter().any(|t| t == "*" || t == tenant),
                McpAuth::Static | McpAuth::Grant { .. } => true,
            };
            if !tenant_allowed {
                continue;
            }
            if let Some(tools) = listing {
                for tool in tools.as_ref() {
                    let is_match = patterns.iter().any(|p| {
                        crate::catalog::pattern_matches(p, &server.name, &tool.mcp_name, &tool.name)
                    });
                    if is_match && !matched.contains(&tool.name) {
                        matched.push(tool.name.clone());
                    }
                }
            }
        }
        for tool in catalog.plugin_tools.as_ref() {
            let is_match = patterns
                .iter()
                .any(|p| crate::catalog::pattern_matches(p, "", &tool.mcp_name, &tool.name));
            if is_match && !matched.contains(&tool.name) {
                matched.push(tool.name.clone());
            }
        }
        if let Some(ref tc) = self.tenant_connectors {
            let known = tc.known(Some(tenant));
            for tool in known.tools.iter() {
                let is_match = patterns.iter().any(|p| {
                    crate::catalog::pattern_matches(p, &tool.server, &tool.mcp_name, &tool.name)
                });
                if is_match && !matched.contains(&tool.name) {
                    matched.push(tool.name.clone());
                }
            }
        }
        matched
    }

    pub async fn match_tools(&self, patterns: &[String], tenant: &str) -> Vec<String> {
        if patterns.is_empty() {
            return Vec::new();
        }
        let mut matched = self.match_tools_sync(patterns, tenant);
        if let Some(ref tc) = self.tenant_connectors {
            let platform_names = self.platform_tool_names();
            let tenant_tools = tc.for_tenant(Some(tenant), &platform_names).await;
            for tool in tenant_tools.tools.iter() {
                let is_match = patterns.iter().any(|p| {
                    crate::catalog::pattern_matches(p, &tool.server, &tool.mcp_name, &tool.name)
                });
                if is_match && !matched.contains(&tool.name) {
                    matched.push(tool.name.clone());
                }
            }
        }
        matched
    }

    pub async fn pinned_tools(&self, tenant: &str, profile: &ProfileConfig) -> Vec<String> {
        let mut pinned = vec!["load_skill".to_string(), "search_tools".to_string()];
        if profile.expert.is_some() {
            pinned.push(EXPERT_TOOL.to_string());
        }
        // agent-pi offers these from a session's first task on; here, from the start, so that
        // nothing needs to change a session's pinned set.
        if self.has_background_tasks() {
            pinned.extend(crate::tasks::PARENT_TOOLS.iter().map(|t| t.to_string()));
        }
        let matched = self.match_tools(&profile.always_on_tools, tenant).await;
        for name in matched {
            if !pinned.contains(&name) {
                pinned.push(name);
            }
        }
        pinned
    }

    pub async fn catalog_tools(&self, tenant: &str) -> Vec<crate::catalog::CatalogTool> {
        let mut tools = Vec::new();
        {
            let catalog = self.catalog.read().unwrap();
            let servers = &self.config.mcp_servers;
            for (index, (listing, _)) in catalog.servers.iter().enumerate() {
                let server = &servers[index];
                let tenant_allowed = match &server.auth {
                    McpAuth::Bearer { tenants, .. } => {
                        tenants.iter().any(|t| t == "*" || t == tenant)
                    }
                    McpAuth::Static | McpAuth::Grant { .. } => true,
                };
                if !tenant_allowed {
                    continue;
                }
                if let Some(list) = listing {
                    for t in list.as_ref() {
                        tools.push(crate::catalog::CatalogTool {
                            name: t.name.clone(),
                            server: server.name.clone(),
                            mcp_name: t.mcp_name.clone(),
                            description: t.description.clone(),
                        });
                    }
                }
            }
            for t in catalog.plugin_tools.as_ref() {
                tools.push(crate::catalog::CatalogTool {
                    name: t.name.clone(),
                    server: String::new(),
                    mcp_name: t.mcp_name.clone(),
                    description: t.description.clone(),
                });
            }
        }
        if let Some(ref tc) = self.tenant_connectors {
            let platform_names = self.platform_tool_names();
            let tenant_tools = tc.for_tenant(Some(tenant), &platform_names).await;
            for t in tenant_tools.tools.iter() {
                tools.push(crate::catalog::CatalogTool {
                    name: t.name.clone(),
                    server: t.server.clone(),
                    mcp_name: t.mcp_name.clone(),
                    description: t.description.clone(),
                });
            }
        }
        tools
    }

    /// The catalog's tools this profile may be offered.
    async fn profile_tools(
        &self,
        tenant: &str,
        profile: &ProfileConfig,
    ) -> Vec<crate::catalog::CatalogTool> {
        let mut tools = self.catalog_tools(tenant).await;
        tools.retain(|t| allowed(profile, &t.server, &t.mcp_name, &t.name));
        tools
    }

    pub async fn search_tools(
        &self,
        query: &str,
        limit: usize,
        tenant: &str,
        profile: &ProfileConfig,
    ) -> Vec<crate::catalog::SearchHit> {
        let tools = self.profile_tools(tenant, profile).await;
        let index = crate::catalog::SearchIndex::new(&tools);
        index.search(query, limit)
    }

    pub async fn preload_tools(
        &self,
        input: &str,
        profile: &ProfileConfig,
        tenant: &str,
    ) -> Vec<String> {
        if profile.preload_max_tools == 0 {
            return Vec::new();
        }
        let tools = self.profile_tools(tenant, profile).await;
        let index = crate::catalog::SearchIndex::new(&tools);
        let lowered = input.to_lowercase();
        let mut chosen = Vec::new();

        let skills = if let Some(ref ts) = self.tenant_skills {
            ts.for_tenant(Some(tenant)).await
        } else {
            Arc::new(self.config.skills.clone())
        };

        for skill in skills.iter() {
            if skill
                .keywords
                .iter()
                .any(|kw| lowered.contains(&kw.to_lowercase()))
            {
                for pattern in &skill.tools {
                    for tool in &tools {
                        if crate::catalog::pattern_matches(
                            pattern,
                            &tool.server,
                            &tool.mcp_name,
                            &tool.name,
                        ) && !chosen.contains(&tool.name)
                        {
                            chosen.push(tool.name.clone());
                        }
                    }
                }
            }
        }

        let hits = index.search(input, profile.preload_max_tools as usize);
        for hit in hits {
            if hit.score >= profile.preload_min_score {
                if !chosen.contains(&hit.tool.name) {
                    chosen.push(hit.tool.name.clone());
                }
                for skill in skills.iter() {
                    let matches_skill = skill.tools.iter().any(|pattern| {
                        crate::catalog::pattern_matches(
                            pattern,
                            &hit.tool.server,
                            &hit.tool.mcp_name,
                            &hit.tool.name,
                        )
                    });
                    if matches_skill {
                        for pattern in &skill.tools {
                            for tool in &tools {
                                if crate::catalog::pattern_matches(
                                    pattern,
                                    &tool.server,
                                    &tool.mcp_name,
                                    &tool.name,
                                ) && !chosen.contains(&tool.name)
                                {
                                    chosen.push(tool.name.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        chosen.truncate(profile.preload_max_tools as usize);
        chosen
    }

    /// The tools a run of this tenant's session is offered: the session's active tools.
    pub async fn offered(
        &self,
        tenant: &str,
        profile: &ProfileConfig,
        active_names: &[String],
    ) -> Vec<Arc<OfferedTool>> {
        let mut sets: Vec<Arc<Vec<ToolEntry>>> = Vec::new();
        {
            let catalog = self.catalog.read().unwrap();
            let servers = &self.config.mcp_servers;
            sets.push(catalog.meta_tools.clone());
            if !catalog.plugin_tools.is_empty() {
                sets.push(catalog.plugin_tools.clone());
            }
            for (index, (listing, _)) in catalog.servers.iter().enumerate() {
                let server = &servers[index];
                let tenant_allowed = match &server.auth {
                    McpAuth::Bearer { tenants, .. } => {
                        tenants.iter().any(|t| t == "*" || t == tenant)
                    }
                    McpAuth::Static | McpAuth::Grant { .. } => true,
                };
                if !tenant_allowed {
                    continue;
                }
                if let Some(tools) = listing {
                    sets.push(tools.clone());
                }
            }
        }

        if let Some(ref tc) = self.tenant_connectors {
            let platform_names = self.platform_tool_names();
            let tenant_tools = tc.for_tenant(Some(tenant), &platform_names).await;
            if !tenant_tools.tools.is_empty() {
                let mut entries = Vec::new();
                for t in tenant_tools.tools.iter() {
                    let validator =
                        jsonschema::validator_for(&t.input_schema).unwrap_or_else(|_| {
                            jsonschema::validator_for(&serde_json::json!({"type":"object"}))
                                .unwrap()
                        });
                    entries.push(ToolEntry {
                        name: t.name.clone(),
                        server: usize::MAX - 3,
                        mcp_name: t.mcp_name.clone(),
                        description: t.description.clone(),
                        schema: t.input_schema.clone(),
                        validator,
                        handler: None,
                        connector: Some(TenantConnectorTarget {
                            tenant_id: tenant.to_string(),
                            connector_id: t.connector_id.clone(),
                            connector_name: t.server.clone(),
                        }),
                    });
                }
                sets.push(Arc::new(entries));
            }
        }

        let mut result = Vec::new();
        for name in active_names {
            for set in &sets {
                if let Some(i) = set.iter().position(|t| &t.name == name) {
                    let t = &set[i];
                    let server = match (self.config.mcp_servers.get(t.server), &t.connector) {
                        (Some(server), _) => server.name.as_str(),
                        (None, Some(connector)) => connector.connector_name.as_str(),
                        (None, None) => "",
                    };
                    if !allowed(profile, server, &t.mcp_name, &t.name) {
                        break;
                    }
                    result.push(Arc::new(OfferedTool {
                        set: set.clone(),
                        index: i,
                    }));
                    break;
                }
            }
        }
        result
    }

    pub fn has_background_tasks(&self) -> bool {
        self.config.subagents.iter().any(|s| s.background)
    }
    /// A child's instructions: the agent's sections without the `profile` one, so a consulted
    /// expert doesn't get "short spoken sentences", then the child's own (agent-pi's delegation).
    pub fn render_child_instructions(
        &self,
        profile_name: &str,
        skills: &[agent_ti_config::SkillConfig],
        own: Option<&str>,
        task: bool,
    ) -> Vec<String> {
        let mut rendered = self.render_sections(profile_name, skills, false);
        if task {
            rendered.push(format!(
                "<tasks>\n{}\n</tasks>",
                crate::tasks::CHILD_SECTION
            ));
        }
        if let Some(own) = own.filter(|t| !t.trim().is_empty()) {
            rendered.push(own.to_owned());
        }
        rendered
    }
    pub fn render_instructions(
        &self,
        profile_name: &str,
        skills: &[agent_ti_config::SkillConfig],
    ) -> Vec<String> {
        self.render_sections(profile_name, skills, true)
    }
    fn render_sections(
        &self,
        profile_name: &str,
        skills: &[agent_ti_config::SkillConfig],
        with_profile: bool,
    ) -> Vec<String> {
        let profile = match self.config.profiles.get(profile_name) {
            Some(p) => p,
            None => return Vec::new(),
        };
        let mut sections: Vec<(String, String, bool)> = self
            .config
            .instructions
            .iter()
            .map(|s| (s.name.clone(), s.content.clone(), s.tag))
            .collect();
        if !skills.is_empty() {
            sections.push((
                "skills".into(),
                agent_ti_config::render_skill_index(skills),
                true,
            ));
        }
        if let Some(text) = profile.instructions.as_ref().filter(|_| with_profile) {
            sections.push(("profile".into(), text.clone(), true));
        }
        if with_profile && self.has_background_tasks() {
            sections.push(("tasks".into(), crate::tasks::PARENT_SECTION.into(), true));
        }
        // Plugin sections carry policy, so they render in a child too.
        for plugin in &self.plugins {
            for sec in plugin.instructions(profile_name) {
                sections.push((sec.name, sec.content, sec.tag));
            }
        }
        sections
            .into_iter()
            .filter(|(_, text, _)| !text.trim().is_empty())
            .map(|(key, text, tag)| {
                if tag {
                    format!("<{key}>\n{}\n</{key}>", text.trim_end())
                } else {
                    text
                }
            })
            .collect()
    }

    pub fn start_tenant_skills_watcher(
        self: &Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        if let Some(ref ts) = self.tenant_skills {
            let ts = ts.clone();
            tokio::spawn(async move {
                ts.watch(shutdown).await;
            });
        }
    }
}
/// Whether a profile may be offered a tool: none of its `excludeTools` match it.
fn allowed(profile: &ProfileConfig, server: &str, mcp_name: &str, name: &str) -> bool {
    !profile
        .exclude_tools
        .iter()
        .any(|p| crate::catalog::pattern_matches(p, server, mcp_name, name))
}
/// One offered tool, keeping its server's listing alive.
pub struct OfferedTool {
    set: Arc<Vec<ToolEntry>>,
    index: usize,
}
impl std::ops::Deref for OfferedTool {
    type Target = ToolEntry;
    fn deref(&self) -> &ToolEntry {
        &self.set[self.index]
    }
}

/// Loads every agent the host file names, through its config source.
pub async fn load_all(host: &HostConfig, client: &reqwest::Client) -> Result<Vec<Agent>, String> {
    let mut sources: BTreeMap<String, Box<dyn ConfigSource>> = BTreeMap::new();
    sources.insert("file".into(), Box::new(FileSource));
    for (name, config) in &host.sources {
        let source = HttpSource::new(
            name,
            config.clone(),
            &host.storage_dir.join("config").join(name),
            process_env,
        )
        .map_err(|e| e.to_string())?;
        sources.insert(name.clone(), Box::new(source));
    }
    let mut agents: Vec<Agent> = Vec::new();
    for location in &host.agents {
        let snapshot = match location {
            AgentLocation::File(path) => sources["file"].load(&path.to_string_lossy()).await,
            AgentLocation::Remote { source, name } => sources[source].load(name).await,
        }
        .map_err(|e| format!("{location}: {e}"))?;
        let agent = Agent::load(host, snapshot)
            .await
            .map_err(|e| format!("{location}: {e}"))?;
        if agents.iter().any(|a| a.name == agent.name) {
            return Err(format!(
                "{location}: agent {:?} is loaded twice",
                agent.name
            ));
        }
        tracing::info!(agent = %agent.name, %location, version = %agent.version, instance = %agent.instance_id, "agent loaded");
        agents.push(agent);
    }
    for agent in &agents {
        agent.refresh_tools(client).await;
    }
    Ok(agents)
}
