# Implementation plan

Status: revised 2026-11-05, when the design was changed to follow agent-pi (see [design.md](design.md)); the fix-now list and steps 1–7 were done the same day, and step 8's speech, voice profile and `note` that evening. This is the one document that tracks build status; the design documents describe only the target.

## Built

- Streamable HTTP: `POST`/`GET`/`DELETE /agent`, per-agent routes `/agents/{name}/…`, `session.create/list/get/messages/update/close/prompt/cancel`, `skills.list` (empty), `agents.list`. The shared agent contract passes (16 required cases) with the contract's own agent-pi `harness.json`, unmodified.
- Configuration (steps 1 and 1a): every config read goes through `ConfigSource` (`agent-ti-config`). The `file` source reads a local agent directory and versions it by a hash of the files read; the `http` source reads `GET <url>/<name>`, authenticates with a bearer token from an env var, and keeps the last good snapshot under `storage.dir/config/<source>/`. A host file (`agent-ti.json`) names the agents; each is an unmodified agent-pi `agent.json`, with unknown fields rejected and unported ones warned about. agent-pi's assistant `agent.json` loads unchanged. `agent-ti check <host.json>` loads every agent without serving.
- JWT (ES256/RS256) with agent-pi's auth fields in the host file: issuers, audiences, required scopes (403), user and tenant claims, clock tolerance. Ownership by tenant, agent and user, 404 for others' sessions.
- One SQLite file per agent (`<storage.dir>/<agent>.sqlite`) with an owner lock; the file records its agent and refuses to open as another, and keeps the instance ID when the host file sets none. Idempotent create and prompt, queue and session limits. An in-memory scheduler spans every agent's store: woken on admission, round-robin across tenants, within the process, tenant and per-user (`maxConcurrentRunsPerUser`) run limits.
- Model/tool loop on the OpenAI-compatible gateway, with agent-pi's identity headers and a per-request timeout (`durable.stream.timeoutMs`). MCP tools from each server's `tools/list` (`static` and `bearer` auth, `prefix`, `include`/`exclude`, `callTimeoutMs`), validators built once per listing, offered up to the profile's `maxActiveTools` with `alwaysOnTools` first. Independent tool calls of a round run in parallel (`limits.maxParallelTools`). Every tool failure, including unknown tools and invalid arguments, goes back to the model as an `isError` result.
- Journaled steps and crash recovery, tested by killing the process (locally and in Kubernetes) during a model call and during an unsafe tool call. An unsafe call cut off by a crash comes back to the model as `interrupted` and is not repeated; a command restarted `durable.restartLimit` times without a new step is parked, and `session.cancel` gives up on it.
- Plugins compiled in and chosen by `use`: agent-pi's `guardrails`, `shortcuts` (FAQ file and greeting), `clock` and `audit`. Hooks so far: `before_prompt` (before admission: a rewrite is what is stored, `reply` answers without the model, `reject` is a 422, a retried `requestId` returns the original outcome without running hooks), `before_model_request` with a journaled `memo`, `after_tool_call`, `session_created`, `after_run`. Plugins are identified by name and crate version.
- Cancellation through an in-process token set when the cancel flag commits.
- Live text (step 2): the model adapter reads the stream incrementally; text and thinking deltas go to a per-agent in-memory hub at most every 100 ms or 16 KiB, never written. Committed events are handed to the hub after their commit, so the event stream no longer polls SQLite. Delta IDs are `<session>:<seq>.<n>`; a per-session ring (1,024 deltas, 1 MiB) lets a reconnect resume exactly, and snapshots carry the reply in progress as `partial`. Each model call is its own `messageId`; `thinking.delta` goes out when the profile sets `thinkingEvents`.
- Voice (step 8, in part): `speech` events from memory for a profile that sets `speech`, flushed at tool start, message end and run end; `note` on `session.prompt`, for the model only; the `voice` capability; Ti has a `voice` profile.
- Compaction (step 9): pi-durable's thresholds, cut and summarizer prompt, on by default as in pi-durable; summaries in `compactions`, placed only when they cut further; overflow errors compact and retry once; the starting summary journaled per run. The eight-turn window is gone.
- Delegation (step 10a): `consult_expert` on a profile with `expert`, and `subagents` as `agent_<name>` catalog tools. A child is a session with a parent (owner, tenant, profile, origin and metadata copied; its own model, instructions without the `profile` section, starting tools, grant under the parent's via auth-server, and event stream). It isn't listed, counts toward no session or run limit, can be read but not driven (404), and shows in the parent's `session.get` as `subagents`. One level deep; `limits.maxSubagentsPerSession` (3); a timeout per sub-agent; a cancel or barge-in of the parent cancels the child; a restart reruns the parent's tool, which finds the same child and submission.
- Background tasks (step 10b): a sub-agent with `background: true` starts a task and returns at once. `task_send`, `task_list`, `task_stop` and `task_note`; reports as wakes or notes, with `taskReports` and `maxUnattendedRuns`; `timeoutMs`, `idleTtlMs` and `maxLifetimeMs`; `maxTasksPerSession` and `maxBackgroundRunsPerUser`; `task.updated`, `session.get` and snapshot `tasks`; `task` lines in `session.messages`; `session.cancel` with `tasks`; close ends tasks. pi-durable's anchor and delivery tasks are `tasks`, `task_messages` and a per-agent supervisor (`src/tasks.rs`).
- Flows (step 10c): every skill has a flow, from its `flow.json` or drawn from its headings. `load_skill` with `query` runs a `deterministic` flow without the model (`extract`, `tool`, `parallel`, `switch`, `ask_user`, `llm_reply`) and returns `<flow_evidence>` or `<flow_needs_input>`; otherwise a `guided` trace follows the model's tool calls. `flow.updated`, `flow` in `session.get` and the snapshot, `skills.checkFlow`. A tenant's flow is checked in full, including its `tools` and `parallel` calls, and a bad one is skipped.
- Replicas (step 11): `AGENT_INSTANCE_ID` from the pod name; `<agent>.<replica>.<n>` IDs; another replica's ID answers 421 with `Agent-Instance` on `POST`, `GET` and `DELETE`, before anything is read; `DELETE` closes a session; JSON-RPC error codes are agent-pi's. Three replicas behind the real `ai-gateway-agent`: `scripts/test-replicas.mjs`, `scripts/test-replicas-k8s.sh`. Ti runs one, as agent-pi's assistant does on the local cluster.
- Operations (step 12): `GET /metrics` (first token, run and tool latency per server, runs, tokens, active runs, open streams, store size); four read connections beside the writer; retention and online backups, off unless the host file asks; hook timeouts, with `before_tool_call` now refusing when it fails; `usage`, `taskUsage` and `limits.maxUnattendedTokens`; `scripts/perf-compare.mjs`.
- Durable event replay and snapshots; Docker, Compose, Kubernetes base and smoke overlay mounting ConfigMaps directly; Ti Assistant in the platform picker as a text-only agent. The runtime keeps no agents of its own; Ti and the property agent are in `t10-agents`.

## Current code against the design

Where today's code falls short of, or departs from, the design. Each is closed by the step named.

| Area | Code today | Closed by |
|---|---|---|
| Live text | A run re-done after a crash streams under a new `generationId` but doesn't first mark the cut-off reply aborted | Open; listed under step 8, not done with it |
| A plugin's reply to a busy session | Queued as a command and placed when the session's earlier work ends, instead of waiting in the inbox for the next turn boundary | Open; listed under step 4, not done with it |
| Auth | ES256/RS256 and the `authenticate` hook; no static-token authenticator or HMAC | Port of agent-pi's `auth/` |
| Tenant connectors | The split agent-pi runs today: published schemas from control's `GET /v1/runtime/connectors`, calls to the gateway's `/mcp/connector/{id}`. The design's `perTenant` server at `/mcp/tenant` waits on the gateway and agent-pi ([order of work](../../t10-agent-control/docs/tenant-connectors.md#order-of-work)) | `perTenant`, after the gateway serves `/mcp/tenant` |
| Voice through `t10-livekit-agent` | agent-ti speaks the protocol, but voice tokens are minted for `agent-pi` and `agent-sales` only, and agent-ti accepts `t10`. Since 2026-11-11 a submission's ID is a number (its admission order), as agent-pi's is, and a streamed `POST` ends with the JSON-RPC result when its run settles. Before that the worker failed on a UUID `Agent-Submission-Id` and a stream that never ended. Tried only with `curl` against the Compose stack, not a call | An `agent-ti` audience on voice grants (auth-server), then re-registration with the `voice` capability, then a real call |
| Flows: the `flow-runner` plugin | Not ported, so an agent that names it stops at startup. From config it does nothing in agent-pi either (it needs `callTool` from code), and no app uses it | When an agent needs one |
| Sub-agents and tasks | A parent's tool polls the child's outcome (20–250 ms) rather than being woken | When it shows up in a measurement |
| Throughput per store | Commits go through one writer, about 100 turns a second on the test machine; agent-ti's own turn time stays flat while runs wait to be claimed | Batching commits, if an agent needs more than a replica gives |
| `cacheRetention`, `progressModel`, `mcpEndpoint`, stdio MCP servers | Accepted in `agent.json` with a startup warning, and not used | When an agent needs one |
| Models | `openai-compatible` with `chat-completions` only; another provider or `api: "responses"` stops startup | When an agent needs one |

Measured with `t10-perf` ([results](validation.md#2026-11-05-step-12-operations)), and host-file settings (`limits`): 200 concurrent runs per process, 100 per tenant, queued work 1,024 per tenant and 16 per session per agent, 2,000 event streams. Still starting values, not measured: stored sessions 10,000 per tenant per agent, and four parallel tool calls per round. Sessions per user and runs per user come from each `agent.json`, as in agent-pi.

Code map: config sources, host file and `agent.json` `crates/agent-ti-config`; live events `src/events.rs`; speech `src/speech.rs`; compaction `src/compaction.rs`; child sessions `src/children.rs`; background tasks `src/tasks.rs` and `crates/agent-ti-store/src/sessions/tasks.rs`; flows `src/flow.rs` (running) and `crates/agent-ti-config/src/flow.rs` (checking); loading agents and the tool catalog `src/agents.rs`; transport and service `src/server.rs`; auth `src/auth.rs`; plugins `src/plugins.rs` and `crates/agent-ti-plugin-sdk`; scheduler and worker `src/worker.rs` and `crates/agent-ti-core`; model and MCP adapters `src/adapters.rs`; store `crates/agent-ti-store`; retention and backups `src/maintenance.rs` and `crates/agent-ti-store/src/sessions/maintenance.rs`; metrics `src/metrics.rs`.

## Fixed

The review's fix-now list, done with step 1 on 2026-11-05:

1. **Tool errors go to the model.** Unknown tool, invalid arguments, MCP error, unreachable server or timeout becomes an `isError` tool result, journaled with its `tool.finished` event.
2. **Plugin identity by version**, not a hash of `src/plugins.rs`.
3. **Store rewritten input.** `before_prompt` runs before admission, so the rewrite is the stored input.
4. **Cancellation token** set when the cancel flag commits; the scheduler is woken on admission, with a one-second scan as a backstop.
5. **Parallel tool calls** within a round, results in call order; each tool's schema validator is built once per listing.
6. **No hidden limits.** A per-model-request timeout replaces the 120 s whole-run timeout, and the process run limit is a host-file setting.
7. **Interrupted, not parked.** An unsafe call with no recorded result becomes an `interrupted` result and the run continues; only a crash loop parks.

## Port from agent-pi, in order

| Step | Work | agent-pi source | Done when |
|---|---|---|---|
| 1 ✓ | The `ConfigSource` abstraction and its `file` source; load an unmodified agent-pi `agent.json` plus the host file, with the field rules in [configuration.md](configuration.md#agent-file); `bundle` as the session's config version; one SQLite file per agent and an in-memory scheduler; retire bundles, bindings, resources, revision publishing and `generate-deploy.py`; tenant access from the platform catalog | `config.ts`, `apps/assistant/config/agent.json` | The assistant's `agent.json`, unchanged, starts on agent-ti; nothing outside the config source opens a config file; deploy configs are plain files |
| 1a ✓ | The `http` source: snapshot API, token auth, last-good cache on the volume | — | An agent loads from a fixture config API, and restarts from the cache with the API down |
| 2 ✓ | Live text: deltas published from memory at most every 100 ms and not committed, a per-session delta ring, partial text in snapshots | `events/hub.ts`, `events/mapper.ts` | Workbench shows text as it arrives; resume mid-reply shows the partial text |
| 2a ✓ | Measure: the same `t10-perf` text scenario against agent-pi and agent-ti, memory and time to first token at rising concurrency | — | Both results are in [validation.md](validation.md); if agent-ti isn't clearly better, revisit the plan before step 3 |
| 3 ✓ | Full plugin hooks; `before_prompt` before admission (422 on reject); history and per-session state; port `guardrails`, `shortcuts`, `clock`, `audit` | `plugins/types.ts`, `runner.ts`, `history.ts`, `plugins/library/` | The plugin tests in `agent-pi/packages/agent-pi/test/plugins.test.ts` have Rust equivalents that pass |
| 4 ✓ | `session.steer`, `session.interrupt` (with `duringTools`), `session.retry`, `whenBusy`, the session inbox | `service.ts` | Barge-in and steer behave as agent-pi's `flow.test.ts` and `agent.test.ts` describe |
| 5 ✓ | Tool catalog, active set, `load_skill`, `search_tools` (BM25), preload, eviction, `tools.active` events; agent-pi skill files | `tools/catalog.ts`, `activation.ts`, `skills/skills.ts` | The assistant's `skills/` work unchanged |
| 6 ✓ | Grants from auth-server for `auth: "grant"` MCP servers; Streamable HTTP MCP with `list_changed`; collab and knowledge connected | `auth/grants.ts`, `mcp/manager.ts` | A collab tool runs as the user, and continues after a restart with the user offline |
| 7 ✓ (cluster-checked) | Tenant skills from `t10-agent-control` (ETag cache, change feed); tenant connectors as a `perTenant` grant MCP server at the gateway's `/mcp/tenant` ([design](../../t10-agent-control/docs/tenant-connectors.md#control-gateway-and-harness)) | `skills/tenant.ts`, agent-pi's `perTenant` server | A skill published in Workbench reaches the next turn; two tenants' connectors never mix |
| 8 (part) | Profiles in full, `speech` events, `note`, the `voice` capability; voice through `t10-livekit-agent` | `events/speech.ts`, `events/mapper.ts`, `agent/profiles.ts` | A voice call works on a `voice` profile. Done: the contract's voice case and a `voice` turn on the cluster; open: the call itself (above) |
| 9 ✓ | Compaction in place of the eight-turn window | pi-durable `harness/compaction.js`, `generation.js` | A long session keeps a stable request size |
| 10a ✓ | `consult_expert` and sub-agents as child sessions | `docs/delegation.md`, `agent/subagents.ts`, `core-extension.ts` | A sub-agent and the expert answer as children; cancel and restart behave as the delegation doc says |
| 10b ✓ | Background tasks: `background: true`, `task.updated`, reports and notes, `task_*` tools | `docs/async-tasks.md`, `agent/tasks.ts` | Start, report, send, note, stop, deadlines, limits, barge-in and restart behave as `async-tasks.md` describes |
| 10c ✓ | Flows: a skill's `flow.json` run by `load_skill` with `query` | `flow/engine.ts`, `flow/plugin.ts` | agent-pi's `flow.test.ts` has Rust equivalents that pass |
| 11 ✓ | Replicas: `<agent>.<replica>.<n>` IDs, 421 with `Agent-Instance`, N StatefulSet replicas | `docs/replicas.md` | The gateway routes to three replicas |
| 12 ✓ | Operations: metrics, retention job, online backup, read pool, load test with `t10-perf` | — | Measured limits replace the current defaults |

## Status

Steps 1–7 and 9–12 are done (2026-11-05); step 7 was also checked on the local cluster against real control, gateway and auth-server (`scripts/step7-cluster.mjs`). Step 8 is done except the call through `t10-livekit-agent`, which waits on voice token audiences. That and the open rows above are what's left.

## Testing

- Port agent-pi's tests as each feature lands. The behavior they describe is the specification; a Rust test that disagrees with agent-pi needs a reason in the design's [differences table](design.md#what-differs-from-agent-pi).
- A scripted model and MCP fixture, as agent-pi's [mock model](../../agent-pi/docs/mock-model.md) (`scripts/fixture.mjs` today).
- Keep the crash tests: kill the real process during a model call, after a tool's prepared record (both in `scripts/test-local.mjs` and `scripts/test-k8s.sh`), and after the final commit (not yet scripted).
- Run `node contracts/agent/conformance.ts agent-ti` from the workspace, and the Workbench browser test (`AGENT_TI_E2E=1`) against a real model.
- Record each run worth keeping in [validation.md](validation.md).

## Rollout

agent-pi keeps serving `assistant`. Ti stays a separate agent in the picker until it covers steps 1–7. Moving an agent from agent-pi to agent-ti means pointing its catalog pool at agent-ti replicas; existing agent-pi conversations stay on agent-pi, since transcripts aren't migrated.
