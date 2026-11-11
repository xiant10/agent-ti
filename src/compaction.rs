//! pi-durable's compaction (`harness/compaction.js`): when a session's context nears the model's
//! window, older messages are summarized and model requests start from the summary. Messages stay
//! in storage; the transcript is unchanged.
//!
//! Above `contextWindow - reserveTokens` a run waits for the summary before its first request
//! (`threshold`); `backgroundTokens` below that, a summary is made in the background and the next
//! run starts from it (`background`). A provider's context-overflow error compacts and retries
//! the run once (`overflow`). Unlike pi-durable, the check is made before each run rather than
//! before each request: a run's own tool rounds stay within the byte budget of `agent-ti-core`.
use crate::{adapters, agents::Agent};
use agent_ti_config::Compaction;
use agent_ti_core::{ExecutionError, Message, ModelRequest};
use agent_ti_store::{Summary, Work};
use serde_json::json;
use std::{sync::Arc, time::Duration};

/// pi-ai's estimate: 3.5 characters a token.
const CHARS_PER_TOKEN: f64 = 3.5;
/// Longest tool result text a summary's source keeps.
const TOOL_RESULT_MAX_CHARS: usize = 2000;
const SUMMARIZATION_SYSTEM_PROMPT: &str = "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.";
const SUMMARIZATION_PROMPT: &str = "The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or \"(none)\" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or \"(none)\" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages.";

pub fn estimate_text(text: &str) -> u64 {
    (text.encode_utf16().count() as f64 / CHARS_PER_TOKEN).ceil() as u64
}
/// pi-ai's `estimateMessageTokens`: text, and a tool call's name and arguments.
pub fn estimate(message: &Message) -> u64 {
    let calls: usize = message
        .tool_calls
        .iter()
        .map(|c| c.name.len() + c.arguments.to_string().len())
        .sum();
    estimate_text(&message.content) + (calls as f64 / CHARS_PER_TOKEN).ceil() as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    Blocking,
    Background,
}
/// Which threshold a context of `tokens` is over, as pi-durable's `thresholdCompaction`.
pub fn threshold(tokens: u64, context_window: u64, policy: &Compaction) -> Option<Need> {
    if !policy.enabled || context_window == 0 {
        return None;
    }
    let blocking = context_window.saturating_sub(policy.reserve_tokens);
    let background = blocking.saturating_sub(policy.background_tokens);
    if tokens > blocking {
        Some(Need::Blocking)
    } else if policy.background_tokens > 0 && tokens > background {
        Some(Need::Background)
    } else {
        None
    }
}

/// Index of the first message a summary keeps, or `None` when there is nothing to compact, as
/// pi-durable's `selectCut`: walk back from the end until `keep` tokens are kept, then cut at the
/// first candidate at or after that message. A candidate is a user or assistant message, never a
/// tool result, and never a user message that a result of the preceding assistant's calls still
/// follows. Records, which models never see, are left out by the caller.
pub fn select_cut(messages: &[&Message], keep: u64) -> Option<usize> {
    let candidates: Vec<usize> = (0..messages.len())
        .filter(|i| candidate(messages, *i))
        .collect();
    let mut kept = 0;
    for index in (0..messages.len()).rev() {
        kept += estimate(messages[index]);
        if kept < keep {
            continue;
        }
        let cut = candidates
            .iter()
            .copied()
            .find(|c| *c >= index)
            .or(candidates.last().copied())?;
        return (cut > 0).then_some(cut);
    }
    None
}
fn candidate(messages: &[&Message], index: usize) -> bool {
    match messages[index].role.as_str() {
        "assistant" => true,
        "user" => {
            let Some(before) = messages[..index]
                .iter()
                .rev()
                .find(|m| m.role == "assistant")
            else {
                return true;
            };
            if before.tool_calls.is_empty() {
                return true;
            }
            for after in &messages[index..] {
                if after.role == "assistant" {
                    return true;
                }
                if after.role == "tool"
                    && after
                        .tool_call_id
                        .as_ref()
                        .is_some_and(|id| before.tool_calls.iter().any(|c| &c.id == id))
                {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

/// Messages as plain text, so the summarizer reads a transcript instead of continuing it.
pub fn serialize(messages: &[&Message]) -> String {
    let mut parts = Vec::new();
    for m in messages {
        match m.role.as_str() {
            "user" if !m.content.is_empty() => parts.push(format!("[User]: {}", m.content)),
            "assistant" => {
                if !m.content.is_empty() {
                    parts.push(format!("[Assistant]: {}", m.content));
                }
                if !m.tool_calls.is_empty() {
                    let calls: Vec<String> = m
                        .tool_calls
                        .iter()
                        .map(|c| {
                            let args: Vec<String> = c
                                .arguments
                                .as_object()
                                .map(|o| o.iter().map(|(k, v)| format!("{k}={v}")).collect())
                                .unwrap_or_default();
                            format!("{}({})", c.name, args.join(", "))
                        })
                        .collect();
                    parts.push(format!("[Assistant tool calls]: {}", calls.join("; ")));
                }
            }
            "tool" if !m.content.is_empty() => {
                parts.push(format!("[Tool result]: {}", truncate(&m.content)))
            }
            _ => {}
        }
    }
    parts.join("\n\n")
}
fn truncate(text: &str) -> String {
    let count = text.chars().count();
    if count <= TOOL_RESULT_MAX_CHARS {
        return text.to_owned();
    }
    let kept: String = text.chars().take(TOOL_RESULT_MAX_CHARS).collect();
    format!(
        "{kept}\n\n[... {} more characters truncated]",
        count - TOOL_RESULT_MAX_CHARS
    )
}

/// The estimated size of a run's first request: instructions and the history it starts from.
pub fn context_tokens(instructions: &[String], history: &[Message]) -> u64 {
    instructions.iter().map(|i| estimate_text(i)).sum::<u64>()
        + history
            .iter()
            .filter(|m| m.role != "record")
            .map(estimate)
            .sum::<u64>()
}

/// Summarizes the session's older messages and places the summary. `Ok(false)` when there was
/// nothing to compact, or a further cut was placed first. The run's identity headers go with the
/// summarizer's request, as pi-durable's compaction shares its conversation's.
pub async fn compact(
    client: &reqwest::Client,
    agent: &Agent,
    work: &Work,
    reason: &str,
) -> Result<bool, ExecutionError> {
    let store_error = |_| ExecutionError::Unavailable;
    let (summary, messages) = agent
        .store
        .compaction_source(&work.scope, &work.session_id)
        .await
        .map_err(store_error)?;
    let visible: Vec<(i64, &Message)> = messages
        .iter()
        .filter(|(_, m)| m.role != "record")
        .map(|(seq, m)| (*seq, m))
        .collect();
    let policy = &agent.config.durable.compaction;
    let only: Vec<&Message> = visible.iter().map(|(_, m)| *m).collect();
    let Some(cut) = select_cut(&only, policy.keep_recent_tokens) else {
        return Ok(false);
    };
    let head = visible[cut].0;
    agent
        .store
        .session_event(
            &work.scope,
            &work.session_id,
            json!({"type":"compaction","phase":"start","reason":reason}),
        )
        .await
        .map_err(store_error)?;
    let result = summarize(client, agent, work, summary.as_ref(), &only[..cut]).await;
    let (text, end) = match &result {
        Ok(text) => (
            Some(text.as_str()),
            json!({"type":"compaction","phase":"end"}),
        ),
        Err(e) => {
            tracing::warn!(agent = %agent.name, session_id = %work.session_id, %reason, error = %e, "compaction failed");
            (
                None,
                json!({"type":"compaction","phase":"end","error":format!("Summarization failed: {e}")}),
            )
        }
    };
    match text {
        Some(text) => {
            let placed = agent
                .store
                .place_compaction(&work.scope, &work.session_id, head, text, reason, end)
                .await
                .map_err(store_error)?;
            tracing::info!(agent = %agent.name, session_id = %work.session_id, %reason, head, placed, "compaction placed");
            Ok(placed)
        }
        None => {
            agent
                .store
                .session_event(&work.scope, &work.session_id, end)
                .await
                .map_err(store_error)?;
            Err(ExecutionError::Unavailable)
        }
    }
}

async fn summarize(
    client: &reqwest::Client,
    agent: &Agent,
    work: &Work,
    earlier: Option<&Summary>,
    messages: &[&Message],
) -> Result<String, String> {
    let profile = &agent.config.profiles[&work.profile];
    let model = &agent.config.models[&profile.model];
    let earlier = earlier.map(|s| Message::text("user", s.message()));
    let source: Vec<&Message> = earlier.iter().chain(messages.iter().copied()).collect();
    let prompt = format!(
        "<conversation>\n{}\n</conversation>\n\n{SUMMARIZATION_PROMPT}",
        serialize(&source)
    );
    let policy = &agent.config.durable.compaction;
    let cap = (0.8 * policy.reserve_tokens as f64).floor() as u64;
    let max_output = model.max_output_tokens.map_or(cap, |m| m.min(cap)).max(256);
    let request = ModelRequest {
        model: profile.model.clone(),
        messages: vec![
            Message::text("system", SUMMARIZATION_SYSTEM_PROMPT),
            Message::text("user", prompt),
        ],
        tools: vec![],
        max_output_tokens: max_output.min(u32::MAX as u64) as u32,
        answer_only: false,
        force_tool: None,
    };
    let run_token = crate::worker::model_token(agent, model, work).await;
    let call = adapters::ModelCall {
        config: model,
        agent: &agent.name,
        thinking_level: &profile.thinking_level,
        timeout: Duration::from_millis(agent.config.durable.stream_timeout_ms),
        run_token: run_token.as_deref(),
    };
    let reply = adapters::model(client, call, work, request, &mut Silent)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let text = reply.message.content.trim();
    if !reply.message.tool_calls.is_empty() {
        return Err("Summarization attempted to call a tool".into());
    }
    if text.is_empty() {
        return Err("Summarization produced no text".into());
    }
    Ok(text.to_owned())
}
/// The summarizer's reply isn't streamed to anyone.
struct Silent;
impl adapters::DeltaSink for Silent {
    fn text(&mut self, _: &str) {}
    fn thinking(&mut self, _: &str) {}
}

/// Starts a background compaction for the session unless one is running.
pub fn background(client: reqwest::Client, agent: Arc<Agent>, work: Work) {
    if !agent
        .compacting
        .lock()
        .unwrap()
        .insert(work.session_id.clone())
    {
        return;
    }
    tokio::spawn(async move {
        let _ = compact(&client, &agent, &work, "threshold").await;
        agent.compacting.lock().unwrap().remove(&work.session_id);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_ti_core::ToolCall;

    fn text(role: &str, chars: usize) -> Message {
        Message::text(role, "x".repeat(chars))
    }
    fn policy() -> Compaction {
        Compaction {
            enabled: true,
            reserve_tokens: 1000,
            keep_recent_tokens: 100,
            background_tokens: 2000,
        }
    }

    #[test]
    fn thresholds_follow_pi_durable() {
        // blocking above 10000 - 1000, background above 9000 - 2000.
        assert_eq!(threshold(9001, 10_000, &policy()), Some(Need::Blocking));
        assert_eq!(threshold(7001, 10_000, &policy()), Some(Need::Background));
        assert_eq!(threshold(7000, 10_000, &policy()), None);
        let off = Compaction {
            enabled: false,
            ..policy()
        };
        assert_eq!(threshold(1_000_000, 10_000, &off), None);
    }

    #[test]
    fn a_cut_keeps_recent_context_and_starts_a_turn() {
        // Four turns of 350 characters (100 tokens) a message.
        let messages: Vec<Message> = (0..4)
            .flat_map(|_| [text("user", 350), text("assistant", 350)])
            .collect();
        let refs: Vec<&Message> = messages.iter().collect();
        // Keeping 100 tokens keeps the last message: the cut is at the last assistant reply.
        assert_eq!(select_cut(&refs, 100), Some(7));
        // Keeping 200 reaches the last user message, which is a candidate.
        assert_eq!(select_cut(&refs, 200), Some(6));
        // Keeping 250 walks back to the reply before it.
        assert_eq!(select_cut(&refs, 250), Some(5));
        // Everything fits: nothing to compact.
        assert_eq!(select_cut(&refs, 10_000), None);
    }

    #[test]
    fn a_cut_never_separates_a_tool_call_from_its_result() {
        let call = Message {
            tool_calls: vec![ToolCall {
                id: "c1".into(),
                name: "echo".into(),
                arguments: json!({}),
            }],
            ..text("assistant", 10)
        };
        let result = Message {
            tool_call_id: Some("c1".into()),
            ..text("tool", 700)
        };
        // A steer that joined between the call and its result.
        let messages = [
            text("user", 700),
            call,
            text("user", 10),
            result,
            text("assistant", 10),
        ];
        let refs: Vec<&Message> = messages.iter().collect();
        // Walking back reaches the tool result; the next candidate is the final assistant reply,
        // not the steer (2) or the result (3).
        assert_eq!(select_cut(&refs, 150), Some(4));
    }

    #[test]
    fn the_summarizer_reads_a_transcript() {
        let call = Message {
            tool_calls: vec![ToolCall {
                id: "c1".into(),
                name: "echo".into(),
                arguments: json!({"value":"x"}),
            }],
            ..Message::text("assistant", "")
        };
        let result = Message {
            tool_call_id: Some("c1".into()),
            ..Message::text("tool", "y".repeat(2100))
        };
        let messages = [Message::text("user", "hi"), call, result];
        let refs: Vec<&Message> = messages.iter().collect();
        let text = serialize(&refs);
        assert!(text.starts_with(
            "[User]: hi\n\n[Assistant tool calls]: echo(value=\"x\")\n\n[Tool result]: yyy"
        ));
        assert!(text.ends_with("[... 100 more characters truncated]"));
    }

    #[test]
    fn overflow_errors_are_told_from_rate_limits() {
        assert!(adapters::context_overflow(
            r#"{"error":{"message":"This model's maximum context length is 8192 tokens"}}"#
        ));
        assert!(adapters::context_overflow(
            "Range of input length should be [1, 98304]"
        ));
        assert!(!adapters::context_overflow(
            "rate limit: too many tokens per minute"
        ));
        assert!(!adapters::context_overflow("internal error"));
    }
}
