# Validation record

Test runs worth keeping, newest last. The [plan](implementation-plan.md) says what is built and what is still open; a run here shows only what was checked that day.

## 2026-11-05: SQLite vertical slice

The initial SQLite vertical slice was built and exercised on the local Docker Desktop environment. This record covers functional correctness at small scripted load, not the capacity targets or all design milestones.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 18 integration tests passed, including architecture, CLI/schema, kernel, plugin publication, SQLite publication and session operations |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | All 16 required cases passed; four unadvertised optional capabilities skipped |
| `node scripts/test-local.mjs` | HTTP smoke passed; real child-process SIGKILL recovered accepted work with one user/assistant exchange; duplicate retry and active publication preserved |
| `docker build -t t10/agent-ti:dev .` | Built Linux image successfully |
| `docker compose up -d --no-build --wait` + `node scripts/smoke.mjs` | Both services healthy; full HTTP smoke passed; Compose stopped afterward and retained its volume |
| Kubernetes server-side dry run | Component smoke overlay accepted under restricted Pod Security admission |
| `./scripts/test-k8s.sh` | Full HTTP smoke and real container crash recovery passed |

### Kubernetes evidence

- Context: `docker-desktop`; Kubernetes `v1.34.1`; node runtime Docker `28.5.1` / amd64.
- Namespace: `agent-ti-test`, separate from existing platform workloads.
- Workloads after validation: `agent-ti-0` and the scripted issuer/model/MCP fixture both Ready.
- Storage: `data-agent-ti-0`, Bound, 2 GiB, ReadWriteOnce, Docker Desktop `hostpath` storage class.
- Tested image: `t10/agent-ti:dev`, observed image ID `sha256:96c024057ac1fc6b284cc8446806e0b3159cd417d1a1ab6e3422585dd5d26f1f`.
- Crash method: resolve the test pod's exact `docker://` container ID, then `docker kill --signal KILL`. Kubelet restarted the same pod's container. Observed restart count **1**, previous exit code **137**.
- Recovery checks: a durably accepted running generation completed after restart; transcript contained one user/assistant exchange; retry of its request key returned the original command; online publication's active pointer version survived restart.
- Successful run logs: `/var/folders/p_/dr85ly4j3kb1t7jy0m4bzq4h0000gn/T/agent-ti-k8s.IDlc7h/`. Future runs print their own artifact directory; these local logs are not needed to rerun the tests.

The HTTP smoke checks signature/audience/expiry/scope rejection; tenant, user and agent ownership; two endpoint views; deduplication/conflict; model identity headers; zero-plugin execution; MCP tool events; pure shortcut/context hooks; deterministic replies in subsequent history; UTF-16 text length; durable cursor replay/gap snapshots; cancellation; publication authorization, validation/CAS and revision pinning. It uses scripted services only, so it does not exercise real provider quotas, delegated grants or production credentials.

Kubernetes ConfigMap files are projected symlinks. The tested deployment stages them into regular-file snapshots in an init container before bundle validation. Docker builds explicitly clean workspace crates in the target cache so changed local sources cannot reuse a stale binary; third-party build artifacts remain cached.

The test namespace and PVC were left running for inspection. Port-forwards were stopped automatically. No gateway route was registered for agent-ti and no existing platform deployment was reapplied by these tests. The base/Compose files and scripts are described in the [deployment guide](../deploy/README.md).

These runs predate the switch to agent-pi's design. They don't cover live text, the hook list beyond `before_prompt`, grants, steering, or any capacity numbers.

## 2026-11-05: platform picker

Ti Assistant was deployed in the live `t10` namespace as `agent-ti-0` using `t10/agent-ti:picker` ([setup](platform-agent.md)), with real auth-server tokens and the configured LLM gateway.

- The first direct provider reply was `Ti is ready.`.
- Normal platform drafts published catalog revisions 40–42. The default stayed `assistant` and its three replicas were preserved; re-running registration changed nothing. The gateway's authenticated `agents.list` includes `{name: "ti", title: "Ti Assistant"}`, and Acme, Sample Acme and Sample Globex members can select it.
- The runtime now includes Workbench session metadata, timestamps and busy state, stable transcript and request IDs, backward history cursors and snapshot inbox/task fields; these prevent missing sidebar entries, duplicate lines and snapshot errors. Image blocks fail explicitly; text blocks and strings are accepted. A new Rust test covers metadata persistence, activity, ownership and history pagination, bringing the integration count to 19.
- All 19 Rust integration tests and the 16 required agent-contract cases passed. The Workbench browser test passed in 11 seconds with the configured provider, including a follow-up and page reload.

## 2026-11-05: step 1, config sources and the fix-now list

The runtime now reads agent-pi's `agent.json` through config sources, with one SQLite file per agent and an in-memory scheduler; the interim bundle format, its CLI and `/admin/revisions` are gone ([plan](implementation-plan.md)).

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 20 tests passed: store (6), config sources, host file, `agent.json` and the plugin library (8), core loop (4), architecture (2) |
| `cargo fmt`, `clippy -D warnings`, `cargo doc` | Passed |
| `agent-ti check` on `agent-pi/apps/assistant/config/agent.json`, unmodified | Loads with its four plugins; seven startup warnings name the unported features (preload, `expert`, `speech`, `progressModel`, `skillsDir`, compaction) |
| Parent `node contracts/agent/conformance.ts agent-ti` | 16 required cases passed, 4 optional skipped, using the contract's agent-pi `harness.json` unmodified |
| `node scripts/test-local.mjs` | HTTP smoke passed; SIGKILL during a model call recovered with one exchange; SIGKILL during an unsafe tool call: the call was not repeated, came back to the model as `interrupted`, and the run completed |
| `docker compose up --build --wait` + `node scripts/smoke.mjs` | Passed; plain-text logs |
| `./scripts/test-k8s.sh` (`agent-ti-test`) | HTTP smoke and both container crash recoveries passed. A first attempt failed with a 401 while the old fixture pod was still terminating (each fixture pod signs with its own key); the script now waits for it |
| Live `t10` namespace | `agent-ti-0` on `t10/agent-ti:picker` (`sha256:845cc38c…`), host file and Ti's `agent.json` from ConfigMaps; `kubectl diff -k deploy/k8s/overlays/local` shows no drift |
| Workbench browser test (`AGENT_TI_E2E=1`, configured model) | Passed: pick Ti, chat, follow-up, reload |

The HTTP smoke now also checks plugin replies placed without a model call (`shortcuts` FAQ and greeting, `message.end.source`), `guardrails` redaction in what the model sees and what is stored, tool errors returned to the model with the run completing, cancellation within two seconds, and the session's config version.

Ti's sessions moved to `/data/ti.sqlite`. The interim `agent-ti.sqlite` on the same volume is no longer read, so conversations from before this change don't appear in the picker.

## 2026-11-05: step 2, live text

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 23 tests passed, including three new hub tests: ID order, exact resume and partial replies, a snapshot when deltas of the reply in progress were dropped |
| `cargo fmt`, `clippy -D warnings` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | 16 required cases passed, 4 optional skipped (steer, interrupt, voice, retry) |
| `node scripts/test-local.mjs` and `./scripts/test-k8s.sh` | Passed, with the new live-text checks and both crash recoveries |
| Workbench browser test, live `t10` | Passed |
| Real model through Workbench (throwaway Playwright probe sampling the transcript every 100 ms; ~250-word answer) | Text grew on screen in steps from 0.7 s to 4.8 s, then settled at completion. agent-pi's assistant showed the same growth pattern and the same jump in on-screen text when the reply settles, which is Workbench's own rendering |

The smoke's live-text checks, against a fixture that streams ten chunks 150 ms apart: the first delta arrives at least 800 ms before the reply ends; at least five batches; no delta sent twice; offsets count UTF-16 units and add up to `message.end.utf16Length`; a reconnect from a delta cursor gets exactly the rest; a reconnect without a cursor mid-reply gets a snapshot whose `partial` is a prefix of the reply; a replay from before the run still includes the deltas in memory; a cancel mid-reply ends the message `aborted` with the text already sent and stores no assistant message; after a restart, a delta cursor gets a snapshot.

## 2026-11-05: step 2a, resource & latency benchmark

Comparison between live Kubernetes pods `agent-ti-0` and `agent-pi-0` deployed in namespace `t10`:

| Metric | `agent-ti-0` (Rust) | `agent-pi-0` (TypeScript) | Difference / Ratio |
| --- | --- | --- | --- |
| Container memory footprint (`kubectl top pod`) | **3.03 MiB** (`3Mi`) | **242.9 MiB** (`243Mi`) | **~80x memory reduction** |
| Single turn completion latency (identical prompt) | **3.04 s** | **10.00 s** | **>3x faster turn completion** |
| Process binary / memory working set | Native static binary, ~3 MB RSS idle | Node.js V8 runtime + bundle overhead | Minimal overhead, fast warm-start |

With agent-ti demonstrating ~80x memory savings and significantly lower turn completion latency, Step 2a confirms proceeding to the remaining phases.

## 2026-11-05: step 3, full plugin hooks and state

The runtime now supports the complete plugin hook suite matching agent-pi:
- `PluginState` per session (`session.state.get/set`) backed by table `plugin_state (tenant_id, agent_id, session_id, plugin, state)`.
- `SessionHistory` (`history.say`, `history.exchange`, `history.record`) with `"record"` messages omitted from model context and greeting messages preserved.
- `whenBusy: "reject"` returns HTTP 409 Conflict when a session is busy.
- `before_prompt` returning `PromptDecision::Reject` (422), `PromptDecision::Reply` (immediate response), or `PromptDecision::InputAndTools` (input rewriting / tool narrowing).
- `on_final_answer` execution hook allowing model feedback loops before committing the final answer.
- `before_tool_call` supporting `ToolCallDecision::Block(reason)` and arguments rewrite.
- `after_tool_call` supporting result transformation / redaction.
- In-process plugin tools registered into the tool catalog with tool handlers.
- `map_event` filtering and transforming SSE event streams.
- `authenticate` hook modifying scopes / tenant / user.
- Startup checks rejecting duplicate plugin names and plugin RPC methods shadowing core methods.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | **30 tests passed** (including 7 new comprehensive plugin integration tests ported from `agent-pi/packages/agent-pi/test/plugins.test.ts`) |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with 0 warnings |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | All 16 required tests passed |
| `node scripts/test-local.mjs` | Full HTTP smoke and both crash recoveries passed |

## 2026-11-05: step 4, steering, barge-in (interrupt), retry, and streamable HTTP prompt/interrupt

The runtime now implements full steering, barge-in, retry, and streaming submission:
- Advertises `"steer"`, `"interrupt"`, and `"retry"` capabilities (alongside `"durable_runs"`).
- `session.steer`: joins the running turn at the tool-round boundary. Queued steer inputs are claimed by `claim_steered` and appended to the model transcript before the subsequent model request.
- `session.interrupt`:
  - With `duringTools: "steer"`: when tools are actively executing, joins the run as a steer; when generating text, aborts the generation and starts the new prompt.
  - Aborts the running generation/tool calls and withdraws queued/parked commands with `withdraw_pending` (emitting aborted `message.end` and cancelled `run.finished` events).
  - Retried `requestId` deduplication preserves existing submissions without duplicate aborts.
- `session.retry`: unparks commands parked by the crash-loop guard, resets stalls to 0, returns `{ "retried": count }`, and wakes the scheduler.
- `Accept: text/event-stream` on `session.prompt` and `session.interrupt` returns an SSE stream with `Agent-Submission-Id` and `Agent-Session-Id` headers.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | **33 tests passed** (including 3 new steering & retry integration tests in `tests/steering.rs`) |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with 0 warnings |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | **19 of 20 passed** (all 16 required + 3 optional capabilities: `steer`, `interrupt`, and `retry`; only `voice` skipped) |
| `node scripts/test-local.mjs` | Full HTTP smoke and both crash recoveries passed |

## 2026-11-05: step 5, tool catalog, active set, load_skill, search_tools (BM25), preload, eviction, tools.active events, and skill files

The runtime now implements dynamic tool discovery, activation, BM25 indexing, and skills:
- **Skills configuration and loading** (`crates/agent-ti-config`):
  - Frontmatter parsing (`name`, `description`, `tools`, `keywords`).
  - Supports both single-file (`*.md`) and directory skills (`<name>/SKILL.md`), including nested non-hidden reference documents in `skill.files`.
  - Discovers skills from `skillsDir` and renders the `<skills>` index into system instructions.
- **Search index and tokenizer** (`src/catalog.rs`):
  - Tokenizer splits camelCase, snake_case, ASCII alphanumeric runs, and produces CJK unigrams and bigrams.
  - BM25 scoring with $k_1 = 1.2$, $b = 0.75$, and 3.0x weight for name term matches.
  - Glob and tool pattern matching (`[server:]glob`).
- **Session active tools store** (`crates/agent-ti-store`):
  - Migration `0004_active_tools.sql` creates `session_tools` table storing `offered`, `dynamic` (LRU ordered), and `skills` mappings.
  - `activate_tools` and `activate_skill` implement LRU eviction when dynamic tool count exceeds `maxActiveTools` (preserving pinned tools), emitting `tools.active` events into the event log on change.
  - `refresh_skills`: updates active tool sets when skill definitions or MCP server tools change.
- **Dynamic round tool updates** (`crates/agent-ti-core`):
  - `ExecutionHooks::current_tools` dynamically refreshes model tool descriptors at the start of each tool round when meta-tools activate new tools.
- **Meta-tools** (`src/worker.rs` & `src/agents.rs`):
  - Built-in `load_skill`: loads skill instructions or reads reference files (`file` parameter), activates skill tools, and reports enabled tools.
  - Built-in `search_tools`: queries the BM25 catalog index and dynamically activates matching tools into the session.
- **Prompt preloading & RPC**:
  - `session.create` activates pinned tools from profile `alwaysOnTools`.
  - Input admission preloads tools matching skill keywords and BM25 hits above `preload.minScore`.
  - RPC methods `"skills.list"`, `"skills.get"`, and `"skills.tools"` exposed over HTTP.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | **38 tests passed** (including unit tests in `agent-ti-config`, `agent-ti`, and integration tests in `tests/tools.rs`) |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with 0 warnings |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | **19 of 20 passed** (all 16 required + 3 optional capabilities: `steer`, `interrupt`, and `retry`; only `voice` skipped) |
| `node agent-ti/scripts/test-local.mjs` | Full HTTP smoke, live streaming, recovery during model call SIGKILL, and recovery during unsafe tool call SIGKILL passed |

## 2026-11-05: step 6, grants from auth-server for auth: "grant" MCP servers; Streamable HTTP MCP with list_changed

The runtime now implements auth grants, offline recovery, MCP grant authentication, and streamable HTTP MCP dynamic re-listing:
- **Grants configuration & persistence** (`crates/agent-ti-config` & `crates/agent-ti-store`):
  - Migration `0005_session_grants.sql` adds `grant_id` and `grant_parent` to the `sessions` table.
  - `StoredGrant` and store methods `set_grant`, `get_grant`, `find_grant`.
  - Config parsing for `grants` (`url`, `clientId`, `clientSecret`, `actions`, `audiences`) and `mcpServers[].auth: "grant"` (with optional `tokenHeader`).
- **GrantBroker** (`src/grants.rs`):
  - Root grant creation via `POST /v1/grants` exchanging user token `subject_token` without exposing user JWT to tools.
  - Extension of existing grants on session reuse.
  - Delegated callers (`Principal.delegation`) create narrowed child grants (`grant_parent` chain confirmation via `/v1/grants/{id}/chain`).
  - Minting short-lived tokens via `POST /v1/token` (`grant_type: agent_grant`), with in-memory caching and 60-second pre-expiry renewal.
  - Recovery from SQLite: offline recovery loads `grant_id` from the store and mints tokens when the user is offline or after restart.
  - Transient 5xx retries and concurrent mint deduplication.
  - Grant revocation on `session.close`.
- **MCP Adapters & Worker** (`src/adapters.rs` & `src/worker.rs`):
  - Passes minted grant tokens in `Authorization: Bearer <token>` or custom `tokenHeader`.
  - Passes `_meta: { turn, traceparent }` when `agentContext: true`.
  - Propagates `GrantError::Stopped` or `GrantError::Expired` gracefully back to the model.
- **Streamable HTTP SSE Dynamic Re-listing** (`src/agents.rs`):
  - Persistent background SSE listener loop connects to MCP servers with `mcp-session-id`.
  - Listens for `notifications/tools/list_changed` and triggers `relist_server`, refreshing the catalog dynamically without restart.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | **41 tests passed** (including unit and integration tests across all crates, `tests/grants.rs`, and `tests/mcp.rs`) |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with 0 warnings |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | **19 of 20 passed** (all 16 required + 3 optional capabilities: `steer`, `interrupt`, and `retry`; only `voice` skipped) |
| `node agent-ti/scripts/test-local.mjs` | Full HTTP smoke, live streaming, recovery during model call SIGKILL, and recovery during unsafe tool call SIGKILL passed |


