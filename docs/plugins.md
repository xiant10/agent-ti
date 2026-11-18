---
title: "Plugins"
audience: contributor
kind: design
status: current
summary: "agent-ti's plugin hooks, following agent-pi's hook list."
---

# Plugins

Status: revised 2026-11-05. The hook list is agent-pi's ([`plugins/types.ts`](../../agent-pi/packages/agent-pi/src/plugins/types.ts), [lifecycle](../../agent-pi/docs/lifecycle.md)). What the code has today is in the [plan](records/implementation-plan.md#current-code-against-the-design).

## Shape

A plugin is a Rust type implementing `Plugin`. Every member has a default that does nothing, as every member of an agent-pi plugin is optional. Plugins are compiled into the binary and registered in a catalog by name; `agent.json` chooses them with `{ "use": "<name>", "options": {...} }`, and hooks run in that order.

```rust
/// Builds a plugin from its `options`, reading any file it needs from the agent's snapshot.
/// An error stops startup, so a misconfigured agent never serves.
pub type Factory = fn(&Value, &Snapshot) -> Result<Arc<dyn Plugin>, String>;

pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }
    fn setup(&self, host: &Host) -> BoxFuture<'_, Result<(), String>> { ready(Ok(())) }
    fn instructions(&self, ctx: &SectionContext) -> Vec<Section> { vec![] }
    fn tools(&self) -> Vec<Tool> { vec![] }
    fn before_prompt(&self, turn: &Turn) -> BoxFuture<'_, Result<Option<PromptDecision>, PluginError>> { ready(Ok(None)) }
    // authenticate, session_created, session_closed, before_model_request, after_model_response,
    // on_final_answer, before_tool_call, after_tool_call, after_run, map_event, rpc, dispose: same pattern
}
```

The exact signatures are settled while porting; the members and their meaning aren't. `crates/agent-ti-plugin-sdk` has the members built so far.

## Hooks

| Member | When | Can |
|---|---|---|
| `setup(host)` | After services are built, before listening and resuming runs | Read services and the agent's config snapshot (never the filesystem); keep a session handle for background work |
| `dispose()` | Shutdown, reverse order | Close clients |
| `instructions` | Every request | System prompt sections, after the configured ones and before the skill index |
| `tools` | Like MCP tools | In-process tools under server `<plugin>`; reachable through skills, search, preload and `alwaysOnTools` |
| `skills`, `mcp_servers` | Startup | Added to the configured ones |
| `rpc` | `POST /agent` | Extra JSON-RPC methods, scoped to the caller's own session |
| `authenticate` | Each request, after the credential is verified | Map roles, check entitlement, reject. Can't change tenant or user |
| `session_created` / `session_closed` | Session lifecycle | Greet, seed state |
| `before_prompt` | Each input, before admission | `input` rewrite, `activate_tools`, `reply` without the model, `reject` (422) |
| `before_model_request` | Each model request | Messages for this request only; not stored |
| `after_model_response` | Each model response | Observe |
| `on_final_answer` | A final answer | `continue` with a correction instead of ending, at most a configured number of times |
| `before_tool_call` / `after_tool_call` | Around each tool | Block, rewrite arguments, redact results. Arguments are validated again after a rewrite |
| `after_run` | A submission settled | Audit, analytics; never delays the answer |
| `map_event` | Each client event, before it is stored | Change, split or drop events; can't change event IDs, order or terminal events |

A rewritten `input` is what gets stored and what later turns see.

## History and state

As in agent-pi: `history.say(text)`, `history.exchange(input, answer)` and `history.record(type, data)` write to the transcript, committed at once when the session is idle and at the next turn boundary when busy. `session.state` is the plugin's own JSON per session, stored under the plugin's name.

## Replay

Model and tool hooks run inside journaled steps. After a crash a step's recorded result is reused, so a hook in it may run again for a step whose result was never recorded. These hooks get read-only state and `memo(name, compute)`; `memo` records the value the first time and returns it on replay. A side effect in a hook needs its own idempotency key, because `memo` can't close the gap between doing something and recording it. `before_prompt` decisions are recorded with the command, so a retried `requestId` returns the same decision without running the hook again.

A failing security hook (`authenticate`, `before_tool_call`) fails closed. A failing observer (`after_model_response`, `session_created`, `session_closed`) is logged and ignored. Each asynchronous hook has a timeout, `limits.hookTimeoutMs`. `after_run`, `after_tool_call` and `map_event` are synchronous, so they can't be timed out and must not block.

## Library

agent-pi's `agent-pi/plugins` library is the list to port: `guardrails`, `shortcuts` (FAQ answers without the model, from agent-pi's FAQ file format), `clock`, `audit`.

## Plugin identity

A session's logs record each plugin by name and crate version, not a hash of its source, so editing a comment changes nothing a session depends on.
