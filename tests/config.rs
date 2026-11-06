use agent_ti::agents::Agent;
use agent_ti_config::*;
use agent_ti_plugin_sdk::{PromptDecision, RequestContext, ToolCall, ToolOutcome, Turn};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| map.get(name).cloned()
}
fn write(dir: &Path, files: &[(&str, &str)]) {
    for (path, text) in files {
        let file = dir.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, text).unwrap();
    }
}
fn host_file(dir: &Path, agents: &str, extra: Value) -> HostConfig {
    let mut host = json!({
        "storage": {"dir": dir.join("data")},
        "auth": {"algorithms": "ES256", "jwksUrl": "http://auth-server:18000/jwks", "issuer": "t10", "audience": "t10", "tenantClaim": "tid"},
        "agents": agents,
    });
    for (k, v) in extra.as_object().unwrap() {
        host[k] = v.clone();
    }
    std::fs::write(dir.join("agent-ti.json"), host.to_string()).unwrap();
    load_host(&dir.join("agent-ti.json"), &env(&[])).unwrap()
}
const MINIMAL: &str = r#"{
  "agent": "ti",
  "models": {"chat": {"provider": "openai-compatible", "modelId": "${MODEL:-m}", "baseUrl": "http://ai-gateway-llm:7200/v1"}},
  "profiles": {"chat": {"model": "chat", "maxActiveTools": "${TOOLS:-8}"}},
  "instructions": [{"name": "base", "file": "base.md", "tag": false}]
}"#;

#[test]
fn substitution_follows_agent_pi() {
    let e = env(&[("SET", "value")]);
    assert_eq!(
        substitute(json!("${SET}/${UNSET:-fallback}/${EMPTY:-}"), &e, "t").unwrap(),
        json!("value/fallback/")
    );
    assert!(
        substitute(json!({"a": ["${UNSET}"]}), &e, "t")
            .unwrap_err()
            .to_string()
            .contains("UNSET")
    );
}
#[test]
fn agent_files_are_read_through_their_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        &[
            ("ti/agent.json", MINIMAL),
            ("ti/base.md", "You are Ti."),
            ("secret.txt", "no"),
        ],
    );
    let snapshot = FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap();
    let config = load_agent(&snapshot, &env(&[("TOOLS", "3")])).unwrap();
    assert_eq!(config.profiles["chat"].max_active_tools, 3);
    assert_eq!(config.instructions[0].content, "You are Ti.");
    assert!(
        snapshot
            .read("../secret.txt")
            .unwrap_err()
            .to_string()
            .contains("leaves")
    );
    assert!(snapshot.read("/etc/hosts").is_err());
    // The version is a hash of the files read, so it changes when one does.
    let before = snapshot.version();
    write(dir.path(), &[("ti/base.md", "You are Ti, revised.")]);
    let again = FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap();
    load_agent(&again, &env(&[])).unwrap();
    assert_ne!(again.version(), before);
}
#[test]
fn agent_pi_app_layout_reaches_its_sibling_directories() {
    let dir = tempfile::tempdir().unwrap();
    let config = MINIMAL.replace("\"file\": \"base.md\"", "\"file\": \"../prompts/base.md\"");
    write(
        dir.path(),
        &[
            ("app/config/agent.json", &config),
            ("app/prompts/base.md", "base"),
        ],
    );
    let snapshot = FileSource::snapshot(&dir.path().join("app/config/agent.json")).unwrap();
    assert_eq!(snapshot.root(), "config/agent.json");
    assert_eq!(
        load_agent(&snapshot, &env(&[])).unwrap().instructions[0].content,
        "base"
    );
}
#[test]
fn unknown_fields_stop_startup_and_unported_ones_warn() {
    let dir = tempfile::tempdir().unwrap();
    for (bad, field) in [
        (
            MINIMAL.replace("\"agent\": \"ti\",", "\"agent\": \"ti\", \"agnet\": 1,"),
            "agnet",
        ),
        (
            MINIMAL.replace("\"maxActiveTools\"", "\"maxActivTools\""),
            "maxActivTools",
        ),
        (
            MINIMAL.replace(
                "\"provider\": \"openai-compatible\"",
                "\"provider\": \"anthropic\"",
            ),
            "not ported",
        ),
    ] {
        write(dir.path(), &[("ti/agent.json", &bad), ("ti/base.md", "x")]);
        let snapshot = FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap();
        let error = load_agent(&snapshot, &env(&[])).unwrap_err().to_string();
        assert!(error.contains(field), "{error}");
    }
    let with_unported = MINIMAL.replace(
        "\"agent\": \"ti\",",
        "\"agent\": \"ti\", \"mcpEndpoint\": {}, \"port\": 1,",
    );
    write(dir.path(), &[("ti/agent.json", &with_unported)]);
    let config = load_agent(
        &FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap(),
        &env(&[]),
    )
    .unwrap();
    assert!(config.unported.iter().any(|u| u.starts_with("mcpEndpoint")));
    assert_eq!(config.host_fields, ["port"]);
}
#[test]
fn the_assistants_agent_json_loads_unmodified() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../agent-pi/apps/assistant/config/agent.json");
    if !path.exists() {
        eprintln!("skipped: {} is not checked out", path.display());
        return;
    }
    let config = load_agent(&FileSource::snapshot(&path).unwrap(), &env(&[])).unwrap();
    assert_eq!(config.agent, "assistant");
    assert_eq!(
        config
            .plugins
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["guardrails", "shortcuts", "clock", "audit"]
    );
    assert_eq!(
        config.models["fast"].identity_headers["sessionId"],
        "X-Session-ID"
    );
    // Both MCP servers are off without their URLs, as in agent-pi.
    assert!(config.mcp_servers.is_empty());
}
#[test]
fn the_host_file_names_agents_and_sources() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_file(
        dir.path(),
        "agents/ti/agent.json, control:sales",
        json!({"sources": {"control": {"kind": "http", "url": "http://t10-agent-control:8090/v1/agents"}}}),
    );
    assert_eq!(
        host.agents[0],
        AgentLocation::File(dir.path().join("agents/ti/agent.json"))
    );
    assert_eq!(
        host.agents[1],
        AgentLocation::Remote {
            source: "control".into(),
            name: "sales".into()
        }
    );
    std::fs::write(dir.path().join("bad.json"), json!({"auth": {"algorithms": "ES256", "jwksUrl": "http://example.com/jwks", "issuer": "i", "audience": "a"}, "agents": "x"}).to_string()).unwrap();
    assert!(
        load_host(&dir.path().join("bad.json"), &env(&[]))
            .unwrap_err()
            .to_string()
            .contains("https")
    );
}

async fn serve(documents: Value) -> (String, tokio::task::JoinHandle<()>) {
    use axum::{Router, extract::Path, http::StatusCode, response::IntoResponse, routing::get};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/v1/agents",
        listener.local_addr().unwrap().port()
    );
    let app = Router::new().route(
        "/v1/agents/{name}",
        get(
            move |Path(name): Path<String>, headers: axum::http::HeaderMap| {
                let documents = documents.clone();
                async move {
                    if headers.get("authorization").and_then(|v| v.to_str().ok())
                        != Some("Bearer secret")
                    {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    match documents.get(&name) {
                        Some(d) => (
                            [("etag", format!("\"{}\"", d["version"].as_str().unwrap()))],
                            axum::Json(d.clone()),
                        )
                            .into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            },
        ),
    );
    (
        url,
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    )
}
#[tokio::test]
async fn an_http_source_serves_snapshots_and_restarts_from_its_cache() {
    let dir = tempfile::tempdir().unwrap();
    let config = MINIMAL.replace("\"file\": \"base.md\"", "\"file\": \"../prompts/base.md\"");
    let documents = json!({
        "ti": {"version": "rev-42", "root": "config/agent.json", "files": {"config/agent.json": config, "prompts/base.md": "remote base"}},
        "escape": {"version": "rev-1", "root": "agent.json", "files": {"../agent.json": "{}"}},
    });
    let (url, server) = serve(documents).await;
    let source = SourceConfig {
        url,
        token_env: Some("CONFIG_TOKEN".into()),
    };
    let cache = dir.path().join("config/control");
    let http = HttpSource::new("control", source.clone(), &cache, |n| {
        (n == "CONFIG_TOKEN").then(|| "secret".into())
    })
    .unwrap();
    let snapshot = http.load("ti").await.unwrap();
    assert_eq!(snapshot.version(), "rev-42");
    assert!(snapshot.notice.is_none());
    assert_eq!(
        load_agent(&snapshot, &env(&[])).unwrap().instructions[0].content,
        "remote base"
    );
    assert!(
        http.load("escape")
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("leaves")
    );
    let unauthorized =
        HttpSource::new("control", source.clone(), &cache, |_| Some("wrong".into())).unwrap();
    assert!(
        unauthorized
            .load("missing")
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("401")
    );
    server.abort();
    let _ = server.await;
    // The API is down: the last good snapshot is used, with a warning.
    let cached = http.load("ti").await.unwrap();
    assert_eq!(cached.version(), "rev-42");
    assert!(cached.notice.as_deref().unwrap().contains("cached"));
    assert!(
        !std::fs::read_to_string(cache.join("ti.json"))
            .unwrap()
            .contains("secret")
    );
}

struct NoMemo;
impl RequestContext for NoMemo {
    fn memo<'a>(
        &'a self,
        _: &'a str,
        compute: Box<dyn FnOnce() -> Value + Send + 'a>,
    ) -> agent_ti_core::BoxFuture<'a, std::result::Result<Value, String>> {
        Box::pin(async move { Ok(compute()) })
    }
}
fn turn(text: &str) -> Turn<'_> {
    Turn {
        text,
        note: None,
        agent: "ti",
        session_id: "s",
        tenant_id: "t",
        user_id: "u",
    }
}
#[tokio::test]
async fn the_plugin_library_matches_agent_pi() {
    let dir = tempfile::tempdir().unwrap();
    let config = MINIMAL.replace(
        "\"instructions\"",
        r#""plugins": [{"use": "guardrails"}, {"use": "shortcuts", "options": {"faqFile": "faq.json", "assistantName": "Ti"}}, {"use": "clock", "options": {"timeZone": "Asia/Shanghai"}}, {"use": "audit"}],
  "instructions""#,
    );
    write(
        dir.path(),
        &[
            ("ti/agent.json", &config),
            ("ti/base.md", "base"),
            (
                "ti/faq.json",
                r#"[{"patterns": ["opening hours"], "answer": "Nine to six."}]"#,
            ),
        ],
    );
    let host = host_file(dir.path(), "ti/agent.json", json!({}));
    let agent = Agent::load(
        &host,
        FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap(),
    )
    .await
    .unwrap();
    let names: Vec<_> = agent.plugins.iter().map(|p| p.name()).collect();
    assert_eq!(names, ["guardrails", "shortcuts", "clock", "audit"]);
    let (guardrails, shortcuts, clock) = (&agent.plugins[0], &agent.plugins[1], &agent.plugins[2]);
    assert_eq!(
        guardrails
            .before_prompt(&turn("my card is 4111 1111 1111 1111"))
            .await
            .unwrap(),
        Some(PromptDecision::Rewrite {
            text: "my card is [card number removed]".into(),
            note: None,
        })
    );
    // A card number in the turn's note is removed as well, as agent-pi's guardrails do.
    let mut noted = turn("pay it");
    noted.note = Some("heard: 4111 1111 1111 1111");
    assert_eq!(
        guardrails.before_prompt(&noted).await.unwrap(),
        Some(PromptDecision::Rewrite {
            text: "pay it".into(),
            note: Some("heard: [card number removed]".into()),
        })
    );
    assert_eq!(
        guardrails
            .before_prompt(&turn("call me at 5551234"))
            .await
            .unwrap(),
        None
    );
    let call = ToolCall {
        id: "c".into(),
        name: "t".into(),
        arguments: json!({}),
    };
    assert_eq!(
        guardrails
            .after_tool_call(&call, ToolOutcome::ok("4111-1111-1111-1111"))
            .content,
        "[card number removed]"
    );
    assert_eq!(
        shortcuts.before_prompt(&turn("Hello!")).await.unwrap(),
        Some(PromptDecision::Reply(
            "Hi, I'm Ti. What can I do for you?".into()
        ))
    );
    assert_eq!(
        shortcuts
            .before_prompt(&turn("What are your OPENING HOURS?"))
            .await
            .unwrap(),
        Some(PromptDecision::Reply("Nine to six.".into()))
    );
    assert_eq!(
        shortcuts
            .before_prompt(&turn("Hello, can you help?"))
            .await
            .unwrap(),
        None
    );
    let messages = vec![
        agent_ti_core::Message::text("system", "s"),
        agent_ti_core::Message::text("user", "earlier"),
        agent_ti_core::Message::text("assistant", "a"),
        agent_ti_core::Message::text("user", "latest"),
    ];
    let out = clock.before_model_request(messages, &NoMemo).await.unwrap();
    assert!(
        out[3].content.starts_with("<now>") && out[3].content.ends_with("(Asia/Shanghai)</now>")
    );
    assert_eq!(out[4].content, "latest");
    drop(agent);
    // An unknown plugin or a bad option stops startup.
    for (plugins, error) in [
        (r#"[{"use": "nope"}]"#, "unknown plugin"),
        (
            r#"[{"use": "clock", "options": {"timeZone": "Mars/Base"}}]"#,
            "Mars/Base",
        ),
    ] {
        write(
            dir.path(),
            &[(
                "ti/agent.json",
                &MINIMAL.replace(
                    "\"instructions\"",
                    &format!("\"plugins\": {plugins}, \"instructions\""),
                ),
            )],
        );
        let result = Agent::load(
            &host,
            FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap(),
        )
        .await;
        assert!(
            result.as_ref().err().is_some_and(|e| e.contains(error)),
            "{:?}",
            result.err()
        );
    }
}
#[test]
fn sub_agents_and_the_expert_follow_agent_pi() {
    let dir = tempfile::tempdir().unwrap();
    let config = MINIMAL
        .replace(
            "\"maxActiveTools\": \"${TOOLS:-8}\"}",
            "\"maxActiveTools\": 8, \"expert\": {\"model\": \"chat\"}}",
        )
        .replace(
            "\"agent\": \"ti\",",
            "\"agent\": \"ti\", \"subagents\": {\"research\": {\"description\": \"Research.\", \"model\": \"chat\", \"instructionsFile\": \"research.md\", \"tools\": [\"crm_*\", \"search_tools\"]}},",
        );
    write(
        dir.path(),
        &[
            ("ti/agent.json", &config),
            ("ti/base.md", "x"),
            ("ti/research.md", "Dig deep."),
        ],
    );
    let config = load_agent(
        &FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap(),
        &env(&[]),
    )
    .unwrap();
    let research = &config.subagents[0];
    assert_eq!(research.name, "research");
    assert_eq!(research.thinking_level, "off");
    assert_eq!(research.timeout_ms, 300_000);
    assert_eq!(research.instructions.as_deref(), Some("Dig deep."));
    assert_eq!(research.tools, ["crm_*", "search_tools"]);
    let expert = config.profiles["chat"].expert.as_ref().unwrap();
    assert_eq!(
        (expert.model.as_str(), expert.thinking_level.as_str()),
        ("chat", "high")
    );
    assert_eq!(config.limits.max_subagents_per_session, 3);
    assert!(config.unported.is_empty(), "{:?}", config.unported);
    // A sub-agent naming an unknown model stops startup.
    let bad = MINIMAL.replace(
        "\"agent\": \"ti\",",
        "\"agent\": \"ti\", \"subagents\": {\"x\": {\"description\": \"X.\", \"model\": \"nope\"}},",
    );
    write(dir.path(), &[("ti/agent.json", &bad)]);
    let error = load_agent(
        &FileSource::snapshot(&dir.path().join("ti/agent.json")).unwrap(),
        &env(&[]),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("subagents.x.model"), "{error}");
}
