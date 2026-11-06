//! agent-pi's `flow.test.ts`: validation, synthesis and the deterministic engine. The end-to-end
//! part (`load_skill`, `flow.updated`, `skills.checkFlow`) runs against the real process in
//! `scripts/smoke.mjs`.
use agent_ti::flow::*;
use agent_ti_config::flow::*;
use agent_ti_config::{SkillConfig, TenantSkillContent, custom_skill, override_flow};
use agent_ti_core::BoxFuture;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

fn visit_flow() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/customer-visit-preparation.flow.json"
    ))
    .unwrap()
}

#[derive(Default)]
struct Host {
    results: HashMap<&'static str, String>,
    calls: Mutex<Vec<(String, Value)>>,
    updates: Mutex<Vec<FlowRunTrace>>,
}
impl Host {
    fn with(results: &[(&'static str, Value)]) -> Self {
        Self {
            results: results.iter().map(|(k, v)| (*k, v.to_string())).collect(),
            ..Default::default()
        }
    }
    fn called(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.0.clone())
            .collect()
    }
}
impl FlowHost for Host {
    fn call_tool(
        &self,
        _step: String,
        name: String,
        args: Value,
    ) -> BoxFuture<'_, ToolCallOutcome> {
        self.calls.lock().unwrap().push((name.clone(), args));
        let output = self
            .results
            .get(name.as_str())
            .cloned()
            .unwrap_or("[]".into());
        Box::pin(async move {
            ToolCallOutcome {
                output,
                is_error: false,
            }
        })
    }
    fn update(&self, trace: FlowRunTrace) -> BoxFuture<'_, ()> {
        self.updates.lock().unwrap().push(trace);
        Box::pin(async {})
    }
}

async fn run(flow: &Value, input: &str, host: &Host) -> FlowExecution {
    let parsed: SkillFlow = serde_json::from_value(flow.clone()).unwrap();
    execute_flow(&parsed, input, language_of(input), host)
        .await
        .unwrap()
}
fn status<'a>(result: &'a FlowExecution, step: &str) -> &'a str {
    &result
        .trace
        .steps
        .iter()
        .find(|s| s.step_id == step)
        .unwrap()
        .status
}

#[test]
fn validates_a_dag_and_collects_every_referenced_tool() {
    let (tools, _) = validate_skill_flow(&visit_flow()).unwrap();
    let mut sorted = tools.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        [
            "find_product_documents",
            "find_success_cases",
            "get_company",
            "list_activities",
            "list_companies",
            "list_contacts",
            "list_deals",
            "search_quote_catalog"
        ]
    );
}

#[test]
fn rejects_cycles_and_unknown_targets() {
    let cyclic = json!({"id":"cyclic","title":"Cyclic","mode":"deterministic",
        "nodes":[{"id":"a","kind":"start","label":"A"},{"id":"b","kind":"tool","label":"B"}],
        "edges":[{"id":"e1","source":"a","target":"b"},{"id":"e2","source":"b","target":"a"}]});
    assert!(
        validate_skill_flow(&cyclic)
            .unwrap_err()
            .contains("cycle detected")
    );

    let unknown = json!({"id":"x","nodes":[
        {"id":"a","kind":"switch","branches":[{"id":"b","label":"B","when":"vars.x","target":"nowhere"}]}
    ],"edges":[]});
    assert!(
        validate_skill_flow(&unknown)
            .unwrap_err()
            .contains("targets unknown node nowhere")
    );

    let duplicate =
        json!({"id":"x","nodes":[{"id":"a","kind":"start"},{"id":"a","kind":"start"}],"edges":[]});
    assert!(
        validate_skill_flow(&duplicate)
            .unwrap_err()
            .contains("duplicate node id a")
    );
    assert!(validate_skill_flow(&json!({"id":"x","nodes":[],"edges":[]})).is_err());
}

#[test]
fn a_pattern_the_regex_crate_cant_run_fails_when_checked() {
    let lookahead = json!({"id":"x","nodes":[
        {"id":"a","kind":"extract","extract":{"f":{"pattern":"(?=x)y"}}}
    ],"edges":[]});
    assert!(
        validate_skill_flow(&lookahead)
            .unwrap_err()
            .contains("the pattern for f")
    );
}

#[test]
fn fills_in_a_nodes_tools_and_keeps_the_rest_of_the_json() {
    let flow = json!({"id":"x","custom":"kept","nodes":[
        {"id":"a","kind":"tool","call":{"tool":"one"}},
        {"id":"b","kind":"parallel","parallel":[{"id":"p","tool":"two"}]}
    ],"edges":[]});
    let (tools, normalized) = validate_skill_flow(&flow).unwrap();
    assert_eq!(tools, ["one", "two"]);
    assert_eq!(normalized["custom"], "kept");
    assert_eq!(normalized["nodes"][0]["tools"], json!(["one"]));
    assert_eq!(normalized["nodes"][1]["tools"], json!(["two"]));
}

#[test]
fn synthesizes_a_guided_flow_from_headings_and_tools() {
    let flow = synthesize_skill_flow(
        "crm-lookup",
        "Look up CRM companies and contacts.",
        &["list_companies".into(), "get_company".into()],
        "### 1. Search Company\nUse `list_companies`.\n### 2. Inspect Details\nUse `get_company`.",
    );
    assert_eq!(flow["mode"], "guided");
    let kinds: Vec<_> = flow["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["start", "tool", "tool", "llm_reply"]);
    assert_eq!(
        validate_skill_flow(&flow).unwrap().0,
        ["list_companies", "get_company"]
    );
}

#[test]
fn synthesizes_from_tools_alone_and_numbered_lines() {
    let flow = synthesize_skill_flow("s", "d", &["crm:a".into(), "crm:b".into()], "no headings");
    assert_eq!(flow["nodes"][1]["kind"], "parallel");
    assert_eq!(flow["nodes"][1]["tools"], json!(["a", "b"]));
    let flow = synthesize_skill_flow(
        "s",
        "d",
        &["weather:*".into()],
        "1. Resolve the place.\n2. Call it.",
    );
    assert_eq!(flow["nodes"][1]["label"], "Resolve the place.");
    // A wildcard names no tool of a section; the first step is given the skill's tools anyway.
    assert_eq!(flow["nodes"][1]["tools"], json!(["*"]));
}

#[tokio::test]
async fn runs_parallel_lookups_when_one_company_matches() {
    let host = Host::with(&[
        (
            "list_companies",
            json!([{"id":"comp_1","name":"字节跳动","industry":"互联网"}]),
        ),
        (
            "get_company",
            json!({"id":"comp_1","name":"字节跳动","size":"10000+"}),
        ),
        (
            "list_deals",
            json!([{"id":"deal_1","title":"企业AI平台升级"}]),
        ),
        (
            "list_activities",
            json!([{"id":"act_1","note":"上周技术交流"}]),
        ),
        (
            "find_product_documents",
            json!([{"id":"doc_1","title":"智能工作台白皮书"}]),
        ),
        (
            "find_success_cases",
            json!([{"id":"case_1","title":"头部互联网客户案例"}]),
        ),
        (
            "search_quote_catalog",
            json!([{"id":"q_1","sku":"ENT-PRO","price":"¥198,000/yr"}]),
        ),
    ]);
    let result = run(
        &visit_flow(),
        "帮我准备字节跳动的客户拜访简报，客户很关心报价",
        &host,
    )
    .await;

    assert_eq!(result.status, "completed");
    assert_eq!(result.vars["company"], "字节跳动");
    assert_eq!(result.vars["relationshipOnly"], false);
    assert_eq!(result.vars["includeQuote"], true);
    assert_eq!(result.trace.llm_calls_saved, Some(3));
    assert_eq!(result.trace.parallel_tools_run, Some(6));
    assert_eq!(status(&result, "ask_disambiguate"), "skipped");
    assert_eq!(status(&result, "crm_facts"), "completed");
    assert_eq!(status(&result, "product_cases"), "completed");
    assert_eq!(status(&result, "reply"), "completed");
    let evidence = result.evidence.unwrap();
    assert!(
        evidence.contains("<step id=\"company_detail\" status=\"completed\">"),
        "{evidence}"
    );
    assert!(evidence.contains("ENT-PRO"));
    assert!(host.updates.lock().unwrap().len() > 4);
}

#[tokio::test]
async fn halts_at_ask_user_when_several_companies_match() {
    let host = Host::with(&[(
        "list_companies",
        json!([{"id":"c1","name":"华为技术有限公司","city":"深圳"},{"id":"c2","name":"华为云计算技术有限公司","city":"贵阳"}]),
    )]);
    let result = run(&visit_flow(), "准备华为的拜访简报", &host).await;

    assert_eq!(result.status, "needs_input");
    let reply = result.immediate_reply.as_deref().unwrap();
    assert!(reply.contains("1. 华为技术有限公司 (深圳)"), "{reply}");
    assert!(
        reply.contains("2. 华为云计算技术有限公司 (贵阳)"),
        "{reply}"
    );
    assert_eq!(status(&result, "ask_disambiguate"), "needs_input");
    assert_eq!(status(&result, "crm_facts"), "skipped");
    assert_eq!(status(&result, "product_cases"), "skipped");
    assert_eq!(status(&result, "reply"), "skipped");
    assert_eq!(host.called(), ["list_companies"]);
}

#[tokio::test]
async fn skips_crm_facts_when_nothing_matches_and_joins_at_product_cases() {
    let host = Host::default();
    let result = run(&visit_flow(), "准备星辰大海科技的拜访简报", &host).await;

    assert_eq!(result.status, "completed");
    assert_eq!(
        host.called(),
        [
            "list_companies",
            "find_product_documents",
            "find_success_cases"
        ]
    );
    assert_eq!(status(&result, "crm_facts"), "skipped");
    assert_eq!(status(&result, "product_cases"), "completed");
}

#[tokio::test]
async fn weather_extracts_the_place_and_asks_when_ambiguous() {
    let weather: Value = serde_json::from_str(include_str!("fixtures/weather.flow.json")).unwrap();
    let host = Host::with(&[("get_forecast", json!({"place":"Tokyo","temp":"22°C"}))]);
    let result = run(&weather, "What is the weather in Tokyo?", &host).await;
    assert_eq!(result.status, "completed");
    assert_eq!(
        host.calls.lock().unwrap()[0],
        ("get_forecast".to_string(), json!({"place":"Tokyo"}))
    );
    assert!(result.evidence.unwrap().contains("22°C"));

    let host = Host::default();
    let result = run(&weather, "Weather in Springfield", &host).await;
    assert_eq!(result.status, "needs_input");
    assert_eq!(
        result.immediate_reply.as_deref(),
        Some(
            "Which Springfield do you mean? Please specify the state or country so I can fetch the right forecast."
        )
    );
    assert!(host.called().is_empty());
}

fn context<'a>(vars: &'a Map<String, Value>, steps: &'a Map<String, Value>) -> FlowContext<'a> {
    FlowContext {
        vars,
        steps,
        input: "hello",
    }
}

#[test]
fn conditions_follow_javascripts_comparisons() {
    let vars = json!({"period":"weekly","on":true,"off":false,"zero":0,"nothing":null});
    let steps = json!({"lookup":{"count":2,"items":[{"id":"a"}]}});
    let (vars, steps) = (vars.as_object().unwrap(), steps.as_object().unwrap());
    let c = context(vars, steps);
    for (expr, want) in [
        ("steps.lookup.count > 1", true),
        ("steps.lookup.count >= 2", true),
        ("steps.lookup.count < 2", false),
        ("steps.lookup.count == 2", true),
        ("vars.period == 'weekly'", true),
        ("vars.period != \"weekly\"", false),
        ("vars.on == true", true),
        ("vars.off != true", true),
        ("vars.on", true),
        ("vars.off", false),
        ("vars.zero", false),
        ("vars.missing", false),
        ("vars.missing == null", true),
        // A stored null isn't undefined, as with `===`.
        ("vars.nothing == null", false),
        ("vars.period > 1", false),
        ("vars.off || vars.on", true),
        ("vars.on && vars.off", false),
        ("vars.off || vars.on && vars.period == 'weekly'", true),
        ("steps.lookup.items.0.id == 'a'", true),
        ("steps.lookup.items.length == 1", true),
        ("input == 'hello'", true),
    ] {
        assert_eq!(evaluate_condition(expr, &c), want, "{expr}");
    }
}

#[test]
fn query_cleaning_leaves_the_entity() {
    for (input, want) in [
        ("What is the weather in Tokyo?", "Tokyo"),
        ("weather for Paris", "Paris"),
        ("帮我准备字节跳动的客户拜访简报，客户很关心报价", "字节跳动"),
        ("查一下北京天气", "北京"),
        ("  plain words.  ", "plain words"),
        ("？", "？"),
    ] {
        assert_eq!(clean_query_input(input), want, "{input}");
    }
}

#[tokio::test]
async fn the_first_keyword_in_file_order_wins() {
    // serde_json keeps object order only with `preserve_order`, which this relies on.
    let flow = json!({"id":"k","mode":"deterministic","nodes":[
        {"id":"e","kind":"extract","extract":{"kind":{"keywords":{"zebra":"z","apple":"a"}}}}
    ],"edges":[]});
    let result = run(&flow, "apple and zebra", &Host::default()).await;
    assert_eq!(result.vars["kind"], "z");
}

#[tokio::test]
async fn templates_fill_strings_and_drop_empty_arguments() {
    let flow = json!({"id":"t","mode":"deterministic","nodes":[
        {"id":"e","kind":"extract","extract":{"who":{"pattern":"name is (\\w+)"}}},
        {"id":"c","kind":"tool","call":{"tool":"look","args":{
            "exact":"$vars.who","text":"hi {{vars.who}}","gone":"$vars.nope","blank":"{{vars.nope}}","n":3,"list":["$vars.who"]
        }}}
    ],"edges":[{"id":"1","source":"e","target":"c"}]});
    let host = Host::default();
    run(&flow, "My NAME IS Ada", &host).await;
    assert_eq!(
        host.calls.lock().unwrap()[0].1,
        json!({"exact":"Ada","text":"hi Ada","n":3,"list":["Ada"]})
    );
}

#[tokio::test]
async fn a_failed_tool_is_evidence_and_the_flow_goes_on() {
    struct Failing;
    impl FlowHost for Failing {
        fn call_tool(&self, _: String, _: String, _: Value) -> BoxFuture<'_, ToolCallOutcome> {
            Box::pin(async {
                ToolCallOutcome {
                    output: "no such thing".into(),
                    is_error: true,
                }
            })
        }
        fn update(&self, _: FlowRunTrace) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }
    let flow: SkillFlow = serde_json::from_value(json!({"id":"f","mode":"deterministic","nodes":[
        {"id":"c","kind":"tool","call":{"tool":"look"}},{"id":"r","kind":"llm_reply"}
    ],"edges":[{"id":"1","source":"c","target":"r"}]}))
    .unwrap();
    let result = execute_flow(&flow, "x", "en", &Failing).await.unwrap();
    assert_eq!(result.status, "completed");
    assert_eq!(result.trace.steps[0].status, "failed");
    assert_eq!(result.trace.steps[1].status, "completed");
    assert!(
        result
            .evidence
            .unwrap()
            .contains("<step id=\"c\" status=\"failed\">\nno such thing")
    );
}

#[test]
fn a_guided_trace_moves_with_the_models_tool_calls() {
    let flow: SkillFlow = serde_json::from_value(json!({"id":"g","title":"G","mode":"guided","nodes":[
        {"id":"start","kind":"start"},{"id":"one","kind":"tool"},{"id":"two","kind":"tool"},{"id":"reply","kind":"llm_reply"}
    ],"edges":[]})).unwrap();
    let mut trace = guided_trace(&flow, "g");
    let statuses = |t: &FlowRunTrace| t.steps.iter().map(|s| s.status.clone()).collect::<Vec<_>>();
    assert_eq!(
        statuses(&trace),
        ["completed", "running", "pending", "pending"]
    );
    advance_after_tool(&mut trace, "crm_look", false);
    assert_eq!(
        statuses(&trace),
        ["completed", "completed", "running", "pending"]
    );
    assert_eq!(
        trace.steps[1].tool_calls.as_ref().unwrap()[0].name,
        "crm_look"
    );
    advance_after_tool(&mut trace, "crm_fail", true);
    assert_eq!(
        statuses(&trace),
        ["completed", "completed", "failed", "running"]
    );
    complete_after_answer(&mut trace);
    assert_eq!(trace.status, "completed");
    assert_eq!(
        statuses(&trace),
        ["completed", "completed", "failed", "completed"]
    );
}

fn weather_skill() -> SkillConfig {
    SkillConfig {
        name: "weather".into(),
        description: "d".into(),
        tools: vec!["crm:get_forecast".into()],
        keywords: vec![],
        body: "b".into(),
        files: BTreeMap::new(),
        flow: None,
        source: None,
    }
}

#[test]
fn an_override_may_only_call_the_built_in_skills_tools() {
    let base = weather_skill();
    let good = json!({"id":"ignored","nodes":[{"id":"c","kind":"tool","call":{"tool":"crm_get_forecast"}}],"edges":[]});
    let checked = override_flow(&base, &good).unwrap();
    assert_eq!(checked["id"], "weather");

    for bad in [
        json!({"id":"x","nodes":[{"id":"c","kind":"tool","tools":["delete_everything"]}],"edges":[]}),
        json!({"id":"x","nodes":[{"id":"c","kind":"tool","call":{"tool":"delete_everything"}}],"edges":[]}),
        // Parallel calls count as well.
        json!({"id":"x","nodes":[{"id":"c","kind":"parallel","parallel":[{"id":"p","tool":"delete_everything"}]}],"edges":[]}),
    ] {
        assert!(
            override_flow(&base, &bad)
                .unwrap_err()
                .contains("delete_everything"),
            "{bad}"
        );
    }
}

#[test]
fn a_custom_skills_flow_may_only_call_the_tools_it_lists() {
    let flow = json!({"id":"contract_risk_check","title":"Contract Risk Check","mode":"deterministic","nodes":[
        {"id":"start","kind":"start"},{"id":"lookup","kind":"tool","call":{"tool":"crm_get_forecast"}},{"id":"reply","kind":"llm_reply"}
    ],"edges":[{"id":"e1","source":"start","target":"lookup"},{"id":"e2","source":"lookup","target":"reply"}]});
    let content = |tools: Option<Vec<String>>| TenantSkillContent {
        description: Some("d".into()),
        tools,
        flow: Some(flow.clone()),
        ..Default::default()
    };
    assert!(custom_skill("contract_risk_check", &content(None), None).is_err());
    let skill = custom_skill(
        "contract_risk_check",
        &content(Some(vec!["crm:get_forecast".into()])),
        None,
    )
    .unwrap();
    assert_eq!(skill.flow.unwrap()["id"], "contract_risk_check");

    // Without a flow, one is drawn from the skill.
    let plain = TenantSkillContent {
        description: Some("d".into()),
        ..Default::default()
    };
    assert_eq!(
        custom_skill("plain", &plain, None).unwrap().flow.unwrap()["mode"],
        "guided"
    );
}

#[test]
fn a_tenants_pattern_is_bounded() {
    let long = "a".repeat(201);
    let flow = json!({"id":"x","nodes":[{"id":"e","kind":"extract","extract":{"f":{"pattern":long}}}],"edges":[]});
    assert!(
        override_flow(&weather_skill(), &flow)
            .unwrap_err()
            .contains("longer than 200")
    );
}

/// A switch skips a branch that joins a step reached another way. The join has to run whichever
/// order the file lists the edges in.
fn join_flow(edges: Value) -> Value {
    json!({"id":"j","mode":"deterministic","nodes":[
        {"id":"gate","kind":"switch","branches":[],"defaultTarget":"after"},
        {"id":"skipped","kind":"tool","call":{"tool":"not_called"}},
        {"id":"after","kind":"tool","call":{"tool":"called"}}
    ],"edges":edges})
}

#[tokio::test]
async fn a_join_runs_whichever_order_its_edges_are_listed() {
    let to_skipped = json!({"id":"a","source":"gate","target":"skipped"});
    let to_join = json!({"id":"b","source":"gate","target":"after"});
    let skipped_to_join = json!({"id":"c","source":"skipped","target":"after"});
    for edges in [
        json!([&to_skipped, &to_join, &skipped_to_join]),
        json!([&to_join, &to_skipped, &skipped_to_join]),
        json!([&skipped_to_join, &to_join, &to_skipped]),
    ] {
        let host = Host::default();
        let result = run(&join_flow(edges.clone()), "x", &host).await;
        assert_eq!(host.called(), ["called"], "{edges}");
        assert_eq!(status(&result, "after"), "completed", "{edges}");
        assert_eq!(status(&result, "skipped"), "skipped", "{edges}");
    }
}

#[tokio::test]
async fn a_chain_of_skipped_steps_still_reaches_the_join() {
    let flow = json!({"id":"j","mode":"deterministic","nodes":[
        {"id":"gate","kind":"switch","branches":[],"defaultTarget":"after"},
        {"id":"one","kind":"tool","call":{"tool":"not_called"}},
        {"id":"two","kind":"tool","call":{"tool":"not_called"}},
        {"id":"after","kind":"tool","call":{"tool":"called"}}
    ],"edges":[
        {"id":"b","source":"gate","target":"after"},
        {"id":"a","source":"gate","target":"one"},
        {"id":"c","source":"one","target":"two"},
        {"id":"d","source":"two","target":"after"}
    ]});
    let host = Host::default();
    let result = run(&flow, "x", &host).await;
    assert_eq!(host.called(), ["called"]);
    assert_eq!(status(&result, "two"), "skipped");
}

#[tokio::test]
async fn a_stored_result_appears_once_in_the_evidence() {
    let flow = json!({"id":"e","mode":"deterministic","nodes":[
        {"id":"look","kind":"tool","call":{"tool":"look","storeAs":"found"}},
        {"id":"plain","kind":"tool","call":{"tool":"plain"}},
        {"id":"then","kind":"switch","branches":[{"id":"b","label":"Both","when":"steps.look.count == 1 && steps.found.count == 1","target":"yes"}]},
        {"id":"yes","kind":"llm_reply"}
    ],"edges":[
        {"id":"1","source":"look","target":"plain"},
        {"id":"2","source":"plain","target":"then"},
        {"id":"3","source":"then","target":"yes"}
    ]});
    let host = Host::with(&[("look", json!([{"id":"a","name":"marker"}]))]);
    let result = run(&flow, "x", &host).await;
    let evidence = result.evidence.clone().unwrap();
    assert_eq!(evidence.matches("marker").count(), 1, "{evidence}");
    assert!(evidence.contains("<step id=\"found\""), "{evidence}");
    assert!(evidence.contains("<step id=\"plain\""), "{evidence}");
    // Later steps can still read a stored result by either name.
    assert_eq!(status(&result, "yes"), "completed");
}
