use agent_ti_core::*;
use serde_json::json;
use std::{sync::Mutex, time::Duration};
struct Script(Mutex<Vec<ModelReply>>);
impl ModelPort for Script {
    fn generate<'a>(
        &'a self,
        _: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>> {
        Box::pin(async move { Ok(self.0.lock().unwrap().remove(0)) })
    }
}
struct Echo;
impl ToolPort for Echo {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        Box::pin(async move {
            if call.name != "echo" {
                return Ok(ToolOutcome::error(format!("Unknown tool {:?}.", call.name)));
            }
            // Later calls finish first, to show results keep the calls' order.
            let delay = 30 - call.arguments["v"].as_u64().unwrap_or(0).min(3) * 10;
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if call.arguments["fail"] == true {
                return Ok(ToolOutcome::error("server said no"));
            }
            Ok(ToolOutcome::ok(call.arguments.to_string()))
        })
    }
}
fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}
fn calls(list: Vec<ToolCall>) -> Message {
    Message {
        tool_calls: list,
        ..Message::text("assistant", "")
    }
}
fn echo() -> Vec<ToolDescriptor> {
    vec![ToolDescriptor {
        name: "echo".into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }]
}
fn reply(message: Message) -> ModelReply {
    ModelReply {
        message,
        input_tokens: 5,
        output_tokens: 2,
    }
}
fn spec() -> ExecutionSpec {
    ExecutionSpec {
        instructions: vec!["system".into()],
        model: "test".into(),
        max_output_tokens: 32,
        max_tool_rounds: 2,
        max_parallel_tools: 4,
        end_turn_tools: vec![],
        first_tool: None,
    }
}
#[tokio::test]
async fn zero_plugin_model_and_complete_tool_round() {
    let script = Script(Mutex::new(vec![
        reply(calls(vec![call("c", "echo", json!({"v":1}))])),
        reply(Message::text("assistant", "done")),
    ]));
    let result = execute(
        &spec(),
        vec![Message::text("user", "tool")],
        echo(),
        &script,
        &Echo,
        4096,
    )
    .await
    .unwrap();
    assert_eq!(
        result
            .messages
            .iter()
            .map(|m| m.role.as_str())
            .collect::<Vec<_>>(),
        ["assistant", "tool", "assistant"]
    );
    assert_eq!(result.input_tokens, 10);
    let script = Script(Mutex::new(vec![reply(Message::text("assistant", "hello"))]));
    assert_eq!(
        execute(
            &spec(),
            vec![Message::text("user", "hi")],
            vec![],
            &script,
            &Echo,
            4096
        )
        .await
        .unwrap()
        .messages[0]
            .content,
        "hello"
    );
}
#[test]
fn context_keeps_whole_tool_rounds_and_rejects_invalid_or_oversized_mandatory_input() {
    let history = vec![
        Message::text("user", "a".repeat(4096)),
        Message::text("assistant", "old"),
        Message::text("user", "new"),
    ];
    assert_eq!(context(&["system".into()], &history, 256).unwrap().len(), 2);
    assert_eq!(
        context(&["x".repeat(1024)], &history, 256).unwrap_err(),
        ExecutionError::ContextLimit
    );
    let bad = vec![
        Message::text("user", "hi"),
        Message::tool("missing", ToolOutcome::ok("dangling")),
    ];
    assert_eq!(
        context(&[], &bad, 4096).unwrap_err(),
        ExecutionError::InvalidToolRound
    );
}
#[test]
fn gateway_stream_normalization_handles_fragmented_tool_arguments_and_unicode() {
    let stream = "data: {\"choices\":[{\"delta\":{\"content\":\"😀é\",\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"v\\\":\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";
    let parsed = agent_ti::adapters::parse_model(stream.as_bytes()).unwrap();
    assert_eq!(parsed.message.tool_calls[0].arguments, json!({"v":1}));
    assert_eq!(parsed.message.content.encode_utf16().count(), 4);
    assert!(agent_ti::adapters::parse_model(b"data: {}\n\n").is_err());
}
#[tokio::test]
async fn tool_errors_go_back_to_the_model_and_parallel_results_keep_their_order() {
    let script = Script(Mutex::new(vec![
        reply(calls(vec![
            call("a", "echo", json!({"v":1})),
            call("b", "echo", json!({"v":2,"fail":true})),
            call("c", "missing_tool", json!({})),
            call("d", "echo", json!({"v":3})),
        ])),
        reply(Message::text("assistant", "corrected")),
    ]));
    let result = execute(
        &spec(),
        vec![Message::text("user", "tools")],
        echo(),
        &script,
        &Echo,
        8192,
    )
    .await
    .unwrap();
    let tools: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.role == "tool")
        .collect();
    assert_eq!(
        tools
            .iter()
            .map(|m| m.tool_call_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(
        tools.iter().map(|m| m.is_error).collect::<Vec<_>>(),
        [false, true, true, false]
    );
    assert!(tools[2].content.contains("Unknown tool"));
    assert_eq!(result.messages.last().unwrap().content, "corrected");
}
struct Recorder(Mutex<Vec<ModelReply>>, Mutex<Vec<ModelRequest>>);
impl ModelPort for Recorder {
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>> {
        Box::pin(async move {
            self.1.lock().unwrap().push(request);
            Ok(self.0.lock().unwrap().remove(0))
        })
    }
}
#[tokio::test]
async fn the_last_round_answers_with_tools_off() {
    let model = Recorder(
        Mutex::new(vec![
            reply(calls(vec![call("a", "echo", json!({"v":1}))])),
            reply(calls(vec![call("b", "echo", json!({"v":2}))])),
            reply(Message::text("assistant", "what I found")),
        ]),
        Mutex::new(vec![]),
    );
    let result = execute(
        &spec(),
        vec![Message::text("user", "go")],
        echo(),
        &model,
        &Echo,
        4096,
    )
    .await
    .unwrap();
    assert_eq!(result.messages.last().unwrap().content, "what I found");
    {
        let requests = model.1.lock().unwrap();
        assert_eq!(
            requests.iter().map(|r| r.answer_only).collect::<Vec<_>>(),
            [false, false, true]
        );
        assert!(requests[2].messages[0].content.ends_with(LAST_ROUND));
        assert!(!requests[1].messages[0].content.contains(LAST_ROUND));
    }

    // A model that calls a tool anyway still ends the turn.
    let model = Recorder(
        Mutex::new(vec![
            reply(calls(vec![call("a", "echo", json!({}))])),
            reply(calls(vec![call("b", "echo", json!({}))])),
            reply(calls(vec![call("c", "echo", json!({}))])),
        ]),
        Mutex::new(vec![]),
    );
    let error = execute(
        &spec(),
        vec![Message::text("user", "go")],
        echo(),
        &model,
        &Echo,
        4096,
    )
    .await
    .unwrap_err();
    assert_eq!(error, ExecutionError::ToolRoundLimit);
}

#[tokio::test]
async fn a_round_of_only_handover_tools_ends_the_turn() {
    let tools = vec![
        ToolDescriptor {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        ToolDescriptor {
            name: "other".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
    ];
    let mut handover = spec();
    handover.end_turn_tools = vec!["echo".into()];
    let said = Message {
        tool_calls: vec![
            call("a", "echo", json!({"v":1})),
            call("b", "echo", json!({"v":2})),
        ],
        ..Message::text("assistant", "handing it over")
    };
    // One model call, then the turn ends with the text written alongside the calls.
    let model = Recorder(Mutex::new(vec![reply(said)]), Mutex::new(vec![]));
    let result = execute(
        &handover,
        vec![Message::text("user", "go")],
        tools.clone(),
        &model,
        &Echo,
        4096,
    )
    .await
    .unwrap();
    assert_eq!(model.1.lock().unwrap().len(), 1);
    assert_eq!(
        result
            .messages
            .iter()
            .map(|m| m.role.as_str())
            .collect::<Vec<_>>(),
        ["assistant", "tool", "tool", "assistant"]
    );
    assert_eq!(result.messages.last().unwrap().content, "handing it over");

    // A failed handover, or a round with another tool in it, goes back to the model.
    for calls_made in [
        vec![call("a", "echo", json!({"fail":true}))],
        vec![call("a", "echo", json!({})), call("b", "other", json!({}))],
    ] {
        let model = Recorder(
            Mutex::new(vec![
                reply(calls(calls_made)),
                reply(Message::text("assistant", "done")),
            ]),
            Mutex::new(vec![]),
        );
        let result = execute(
            &handover,
            vec![Message::text("user", "go")],
            tools.clone(),
            &model,
            &Echo,
            4096,
        )
        .await
        .unwrap();
        assert_eq!(model.1.lock().unwrap().len(), 2);
        assert_eq!(result.messages.last().unwrap().content, "done");
    }
}

#[tokio::test]
async fn the_first_tool_is_forced_on_the_first_call_only() {
    let mut forced = spec();
    forced.first_tool = Some("echo".into());
    let model = Recorder(
        Mutex::new(vec![
            reply(calls(vec![call("a", "echo", json!({"v":1}))])),
            reply(Message::text("assistant", "found")),
        ]),
        Mutex::new(vec![]),
    );
    execute(
        &forced,
        vec![Message::text("user", "look it up")],
        echo(),
        &model,
        &Echo,
        4096,
    )
    .await
    .unwrap();
    let requests = model.1.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.force_tool.clone())
            .collect::<Vec<_>>(),
        [Some("echo".to_string()), None]
    );
}
