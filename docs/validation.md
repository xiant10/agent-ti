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

Ti Assistant was deployed in the live `t10` namespace as `agent-ti-0` using `t10/agent-ti:picker` ([setup](../../t10-agents/docs/ti.md), then in agent-ti), with real auth-server tokens and the configured LLM gateway.

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

Review note (2026-11-05, later): this is an idle-memory reading and one turn, not the planned `t10-perf` text scenario at rising concurrency, and the 3 s against 10 s compares one turn each on a real model. It shows the direction but doesn't replace the measurement, which step 12's load test still has to make.

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

## 2026-11-05: step 7, tenant skills from t10-agent-control and tenant connectors

The runtime now implements tenant skills, author drafts, reference files, turn pinning, and tenant connectors:
- **Tenant Skills & Control integration** (`crates/agent-ti-config` & `src/tenant_skills.rs`):
  - Fetches tenant skills and workflow overrides from `t10-agent-control` (`GET /v1/runtime/skills?tenant={tenant}`) with ETag caching (10s fresh, 5m stale fallback, 304 Not Modified).
  - Background change feed watcher follows `/v1/runtime/skills/changes?after={cursor}` and invalidates the tenant cache on publication.
  - Author drafts support: test sessions with `session.metadata.skillDraft` resolve draft skill definitions via `/v1/runtime/skills/draft`.
  - Turn pinning (`TurnPins`): skills available at turn start remain pinned in memory for the turn duration, preventing mid-turn skill changes from affecting running turns.
  - Custom tenant skill reference files: `skill.files` validated and served by `load_skill` via `file` parameter.
  - Built-in skills support `flow.json` alongside `SKILL.md`.
- **Tenant Connectors** (`src/tenant_connectors.rs` & `src/agents.rs`):
  - Fetches published connector tools from `t10-agent-control` (`GET /v1/runtime/connectors?tenant={tenant}`) with ETag caching and change feed invalidation.
  - Connector tool scoping: exposed as `{connector_name}_{tool_name}` to avoid collisions with platform tools.
  - Complete tenant isolation: two tenants' connectors never mix or leak.
  - Gateway dispatch under grant auth: connector tools are dispatched to `{gatewayUrl}/mcp/connector/{connectorId}` using a short-lived token minted by `GrantBroker`.
- **Worker & Server RPC integration** (`src/worker.rs` & `src/server.rs`):
  - Dynamic tool matching, preloading, and active set management incorporates tenant connector tools.
  - System prompt `<skills>` dynamically renders pinned tenant skills and built-in skills.
  - `skills.list`, `skills.get`, and `skills.tools` RPC endpoints return tenant-aware skills and tools.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | **46 tests passed** (including unit tests in `agent-ti-config` and integration tests in `tests/tenant_skills.rs` and `tests/tenant_connectors.rs`) |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed with 0 warnings |
| `cargo doc --workspace --no-deps --locked` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | **19 of 20 passed** (all 16 required + 3 optional capabilities: `steer`, `interrupt`, and `retry`; only `voice` skipped) |
| `node agent-ti/scripts/test-local.mjs` | Full HTTP smoke, live streaming, recovery during model call SIGKILL, and recovery during unsafe tool call SIGKILL passed |

## 2026-11-05: step 8 (part), speech events, the voice profile and `note`

Ported from agent-pi:

- **Speech segmentation** (`src/speech.rs`, from `events/speech.ts`): sentences at `.!?;` plus whitespace, CJK `。！？；`, or line breaks; segments under 12 characters wait; a sentence over 220 is cut at its last clause break, else its last space; markdown is stripped per segment. agent-pi's three unit tests pass in Rust, plus two more.
- **`speech` events** (`src/events.rs`): a session whose profile sets `speech` gets `{type, text, index}` events from memory, like text deltas, in the same ID order and resume ring. Buffered text is flushed before `tool.started`, `message.end` and `run.finished`; an aborted message's buffered text is dropped. A plugin's reply, and a reply recovered after a restart that streamed no deltas in this process, are spoken whole from their committed events.
- **`note`** on `session.prompt`: given to the model ahead of the input as `<turn-note>…</turn-note>`, left out of `session.messages`, part of the request fingerprint, and passed to `before_prompt` (`Turn.note`). `PromptDecision::Rewrite` can replace both; `guardrails` redacts card numbers in the note, as in agent-pi.
- **`voice` capability** in the manifest and on every session, as agent-pi's `capabilitiesOf` lists it.
- **Ti's `voice` profile** (`deploy/k8s/base/agents/ti/voice.md`, agent-pi's voice prompt without `consult_expert`), and an ingress rule for `t10-livekit-agent`.

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 60 passed |
| `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| Parent `node contracts/agent/conformance.ts agent-ti` | **20 of 20 passed**, including `capability voice` |
| `node agent-ti/scripts/test-local.mjs` | HTTP smoke (now with `note`: model input, redaction, transcript, a non-string note answers 400) and both SIGKILL recoveries passed |
| Local cluster, namespace `t10`, `qwen3.5-flash`, `sample-member` token, `voice` profile | First `text.delta` 1.35 s and first `speech` 1.66 s after the prompt; three sentence events matching `message.end`; transcript without the note |

Not done: a call through `t10-livekit-agent`. Its tokens come from grants whose audiences are `agent-pi` and `agent-sales` (`auth-server/clients.yaml`), and agent-ti accepts `t10`. `auth-server/docs/agent-identity.md` asks for per-exchange audience narrowing before a third audience is added.

## 2026-11-05: step 7 on the local cluster

Ti was given agent-pi's step 7 settings (`tenantSkills`, `tenantConnectors`, `grants`, the same environment as the assistant) and its own runtime account: `agent-ti` in `auth-server/clients.yaml` (`optional: true`, audiences `agent-ti`, collab, knowledge and the tool gateway) with `AGENT_TI_SECRET` in the setup secret. Control's runtime directory maps it to the `ti` pool through the pool's `agent-ti` audience. `scripts/step7-cluster.mjs`, run against namespace `t10` with `qwen3.5-flash`:

| Check | Observed result |
| --- | --- |
| A custom skill published for Ti by `sample-acme-admin` | In an Acme member's `skills.list` 62–72 ms after the publish; the model loaded it and answered with its code |
| A second version | Used by the next turn, 3.6 s later |
| Globex (`sample-multi`) | Never saw the skill |
| A connector to `mcp-fixture`'s `/open/mcp`, published with `echo` for agent `ti` | Offered to Acme, activated by a skill naming `<connector>:*`, called through `ai-gateway-tools` on Ti's grant, no error |
| Globex | Not offered the tool; the model said it couldn't call it |

Found on the way: with nothing else in Ti's catalog, BM25 scores a lone connector tool below `preload.minScore`, so it's reached through a skill or `search_tools`, as agent-pi's formula gives too. The script removes what it publishes.

## 2026-11-05: step 9, compaction

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 66 passed, including the cut (keeps recent context, never splits a tool round), thresholds, the summarizer's transcript, overflow patterns, and the store's furthest-cut rule |
| Smoke, `ti-plain` with a 3,000-token window, `reserveTokens` 500, `keepRecentTokens` 300 | Twelve 600-character turns: request sizes 710, 1,996, 2,842, then 2,847–2,851 to the end; the last request starts with the summary; `compaction` start and end events; `session.messages` still has all 24 messages |
| Smoke, an overflow | The fixture's `maximum context length` 400 on a long request: the run compacted and answered on its second request |
| `node agent-ti/scripts/test-local.mjs`, `./scripts/test-k8s.sh` | Both pass, with both SIGKILL recoveries |
| Contract | 20 of 20 |

Not checked with a real model: Ti's 128,000-token window makes a long enough session impractical by hand. The summarizer prompt is pi-durable's, unchanged.

## 2026-11-05: step 10a, `consult_expert` and sub-agents

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --locked` | 67 passed, including the `subagents` and `expert` config fields and their defaults |
| Smoke, `ti-demo` with sub-agent `helper` (no tools, its own instructions) and an `expert` on the chat profile | `agent_helper` answered through a child session: the child's request carried `helper.md` and no tools; `consult_expert`'s child was offered the parent's tools without `consult_expert` or `agent_helper`. The parent's `session.get` lists both children; neither is in `session.list`; a child can be read by its owner, not by another user, and `session.prompt` on it answers 404 |
| Smoke, cancel | Cancelling the parent while its child's model call ran left the child idle within 4 s |
| Crash, `scripts/test-local.mjs` and `./scripts/test-k8s.sh` | SIGKILL while the child's model call was in flight: after the restart the parent answered with the child's reply, one child, and the child's transcript one exchange |
| Contract | 20 of 20 |
| Local cluster, namespace `t10` | Deployed; Ti declares no sub-agents, so this is the step 7 check passing again on the new image |

## 2026-11-05: step 10b, background tasks

Smoke, against `ti-demo` with background sub-agents `scout` and `slowpoke` (500 ms deadline) and a `quiet` profile (`taskReports: "note"`):

| Check | Observed result |
| --- | --- |
| Start and report | The tool returned at once; `task.updated` working then idle; the child's answer woke the parent as `<task_report id="t1" name="scout" status="idle">`; the child had only `task_note`, each side had its `tasks` section; `session.get` shows the task and `session.messages` a `task` line |
| `task_send`, `task_list`, `task_stop` | A send started the child's next run, which reported; the list read the last report; after a stop, a send is an error the model reads |
| `task_note` | Reached the transcript as a `task` line of kind `note` with no run of its own |
| `taskReports: "note"` | No run; the next request carried the report |
| Deadline | The run was stopped; the report said it ran out of time; status `failed` |
| Interrupt | A report queued behind a busy parent, withdrawn by an interrupt, came back as one note; the model answered the interrupting input, and no run started for the report |
| `maxBackgroundRunsPerUser` | With three children working, a fourth waited, and its 500 ms deadline waited with it |
| `maxTasksPerSession`, cancel | A fourth live task was refused; a plain cancel left tasks working; `tasks: true` stopped them |
| Crash, local and Kubernetes | SIGKILL with the parent idle and the child's model call in flight: the supervisor resumed and the parent got one report |
| Store tests | The unattended budget turns reports into notes until a user speaks; a rerun finds the same task; a note waits for a boundary |
| `cargo test --workspace --locked` | 71 passed |
| `node agent-ti/scripts/test-local.mjs` | Passed 4 runs of 4 |
| `./scripts/test-k8s.sh` | Passed 5 runs of 5 after the fix, on one volume |
| Local cluster, namespace `t10` | Deployed; Ti declares no sub-agents, so this is the step 7 check passing again on the new image |
| Contract | 20 of 20 |

Found by repeating the Kubernetes suite: a note placed just after a run was claimed followed the run's input in its history, so the model answered the report instead of the person. It appeared intermittently, only under that timing. Notes now wait for a boundary, in the transaction that writes them, with a store test. Also found: `scout`'s 20 s deadline expired while kubelet's crash backoff held the pod down (the deadline counts downtime, as agent-pi's does), and the suite left too many sessions on its volume for repeated runs; both were the test's, and the smoke agents' limits changed.

Not tested: `maxLifetimeMs` and `idleTtlMs` expiry end to end, an urgent `task_note`, a report past `maxUnattendedRuns` end to end (the store test covers the decision), and background tasks with a real model on the cluster: Ti declares no sub-agents.

## 2026-11-05: step 10c, flows

agent-pi's `flow.test.ts` in Rust (`tests/flow.rs`, using its `customer-visit-preparation` fixture and the assistant's `weather` flow), and the end-to-end part against the real process in `scripts/smoke.mjs`, on `ti-demo`'s `lookup` and `guided` skills:

| Check | Observed result |
| --- | --- |
| Validation and synthesis | Tools collected; cycles, unknown targets and duplicates refused; a `guided` flow drawn from headings and from tools alone |
| Parallel lookups | 1 company matched: `llmCallsSaved` 3, `parallelToolsRun` 6, `ask_disambiguate` skipped, evidence holds `company_detail` and the quote |
| Disambiguation | 2 companies: `needs_input`, the numbered list, one tool called |
| No match | Joins at `product_cases`; three tools called |
| `load_skill` with `query` | `flow.updated` per step, last one `completed`; one tool call, two model calls; `<flow_evidence>` in the tool result; `flow` in `session.get` and the snapshot |
| `ask_user` | `<flow_needs_input>` with the question; nothing called |
| Guided | Trace `running` on load, `completed` when the model answered |
| `skills.checkFlow` | An edit returned and not saved; a tool outside the skill's list 400; a custom flow 400 until its `tools` are given; `skills.list` unchanged |
| Crash, local and Kubernetes | SIGKILL with a flow's first tool finished and its unsafe second in flight: first not repeated, second not repeated, evidence says `interrupted`, the run completed |
| `cargo test --workspace` | 91 passed |
| `node agent-ti/scripts/test-local.mjs` | Passed |
| `./scripts/test-k8s.sh` | Passed. The first run stopped at "container did not restart": a fifth kill of one pod waits 1m20s on kubelet's backoff, past the script's 60 s; the wait is now 180 s |
| Contract | 20 of 20 |
| Local cluster, namespace `t10` | Deployed (`t10/agent-ti:flows`), migration applied; Ti declares no skills, so no flow has run there |

Found while porting: `apply_tenant_skills` checked only `call.tool`, so a tenant's `parallel` calls and `tools` weren't held to the skill's list, which started to matter once flows run. A tool's early errors in `invoke` (no grant, wrong tenant) returned without recording their step; they now record it. `serde_json` sorted object keys, which would have changed which `extract.keywords` entry wins, so the workspace now sets `preserve_order`.

Not tested: a real model through a flow, tenant-published flows on the cluster (the checks run in `tests/flow.rs` and `tests/tenant_skills.rs`), and flows calling a tenant connector.

## 2026-11-05: step 11, replicas

`scripts/replicas.mjs`: three replicas behind a real `ai-gateway-agent`, its token check pointed at the fixture's JWKS. Locally (`scripts/test-replicas.mjs`, the gateway built from the sibling repo, SIGKILL for the loss) and on the cluster (`scripts/test-replicas-k8s.sh`, image `ai-gateway:latest`, a scale for the loss, which keeps the volume):

| Check | Observed result |
| --- | --- |
| Placement | 30 people reached all three replicas; each person's second session stayed on the first's replica |
| Routing | Prompts, streams and transcripts through the gateway reached the replica in the ID on each of the three |
| Wrong replica | 421, `Agent-Instance: ti-demo.agent-ti-0`, code -32021 on `POST`, `GET` and `DELETE`; the owner still answered; a malformed ID 404 |
| Listing | One person's sessions on three replicas came back merged, newest first, and whole across a `limit: 2` cursor |
| `DELETE` | 204 through the gateway; then 404 |
| Losing a replica | Its session answered 5xx; another replica's session answered a prompt; new sessions went elsewhere; the list said `partial` and held the rest |
| Getting it back | Its session answered with its transcript intact and took another prompt; the list was whole |
| `cargo test --workspace`, `scripts/test-local.mjs`, `./scripts/test-k8s.sh`, contract 20 of 20 | Passed |
| Local cluster, namespace `t10` | `t10/agent-ti:replicas`, one replica, `AGENT_INSTANCE_ID` from the pod name; still `agent-ti-0`, so its sessions' IDs are unchanged |

Found while doing it: every replica would have had the ID `agent-ti-0`, since the workload never set `AGENT_INSTANCE_ID`. A stream opened on the wrong replica got 404, because ownership was checked first. `DELETE` wasn't routed at all, though the plan listed it as built. A 421 had no header and the code -32421; error codes now follow agent-pi's table. The plan's `Agent-Instance` claim was also not yet true of `GET`.

Not tested: routes from `t10-agent-control` with more than one Ti replica (the test uses `AGENT_URLS`), `t10-livekit-agent`'s routing by ID, and a replica added to a running pool.

## 2026-11-05: step 12, operations

### Load: agent-ti against agent-pi

`scripts/perf-compare.mjs`: t10-perf's client and mock model, agent-pi's assistant `agent.json` unchanged on both, one process each with its own SQLite file, release build of agent-ti, Node 25 for agent-pi. Limits set high so neither's decide the result. Mock model: 300 ms to first token, 30 tokens at 20 ms, no tools. One machine (16 CPUs) running client, mocks and runtime, on a laptop SSD: compare the two, and don't read these as a server's capacity. Each user takes 3 turns; times in ms.

| Users | agent-ti turns/s | TTFT p50 / p95 | Failed | Peak RSS | agent-pi turns/s | TTFT p50 / p95 | Failed | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 | 10.2 | 335 / 357 | 0 | 26 MiB | 9.5 | 445 / 609 | 0 | 205 MiB |
| 50 | 42.6 | 328 / 332 | 0 | 39 MiB | 41.4 | 445 / 517 | 0 | 307 MiB |
| 100 | 69.5 | 331 / 398 | 0 | 59 MiB | 48.6 | 601 / 1559 | 0 | 401 MiB |
| 200 | 89.3 | 390 / 1013 | 0 | 91 MiB | 46.4 | 662 / 5523 | 0 | 470 MiB |
| 400 | 96.9 | 1648 / 2232 | 0 | 147 MiB | 40.2 | 757 / 13756 | 68 of 1200 | 553 MiB |
| 800 | 81.0 | 5841 / 6923 | 0 | 267 MiB | 31.9 | 1454 / 13249 | 1019 of 2400 | 587 MiB |
| 1600 | 67.8 | 14928 / 19437 | 0 | 459 MiB | 26.3 | 1621 / 15604 | 3347 of 4800 | 672 MiB |

Idle RSS: agent-ti 16.7 MiB, agent-pi 139.9 MiB. agent-pi's p50 at 400 and above counts only the turns that succeeded, so it flatters it.

Step 2a's reading was idle memory and one turn each; this is the scenario it meant to run, and it holds: lower latency and about a seventh of the memory at the sizes that matter, with twice the throughput at its peak. The gap at low concurrency is the runtime's own: 30 ms over the model's 300 ms, against about 145 ms.

**Where agent-ti's limit is.** From `/metrics`, a model call's own wait for its first token stayed at 321 ms, and a run from claim to end at 920–970 ms, at every load up to 800 users. So what a client sees past that is waiting to be claimed. Throughput levels off at about 100 turns a second with about 2.4 CPUs in use. That fits every commit going through the one writer with `synchronous=FULL`, though it wasn't isolated further. It's per store, so it's per agent and per replica.

### Limits

| Limit | Was | Now | Why |
| --- | --- | --- | --- |
| `maxConcurrentRuns` | 200 | 200 | Measured: at 200, TTFT p95 682 ms and 98 turns/s. Past it, runs only wait longer. Capping at 200 kept 400 and 800 users at or above uncapped throughput |
| `maxConcurrentRunsPerTenant` | 16 | 100 | At 16, one tenant got 16.7 turns/s. At 100: 69.5 turns/s with TTFT p95 under 400 ms, leaving half the process for others |
| `maxQueuedPerTenant` | 128 | 1024 | At 128, 212 of 600 turns failed with 200 users of one tenant. 1,024 is about 10 s of work at the measured rate |
| `maxStreams` | 1000 | 2000 | At 1000, 227 of 2400 turns failed with 800 users, each holding a listening stream and a turn's. 1,600 users took 459 MiB, within the workload's 1 GiB |

On those defaults, with no overrides, in one tenant:

| Users | Turns/s | TTFT p50 / p95 | Failed | Peak RSS |
| --- | --- | --- | --- | --- |
| 100 | 69.5 | 332 / 380 | 0 | 51 MiB |
| 200 | 81.0 | 677 / 1415 | 0 | 81 MiB |
| 400 | 77.5 | 2335 / 3880 | 0 | 139 MiB |
| 800 | 65.9 | 6915 / 8747 | 0 | 258 MiB |

Not measured, so still starting values: `maxSessionsPerTenant`, `maxParallelTools`, and anything with tools or a real model. A voice turn wanting a first token within a second has room for about 200 active runs per replica at these numbers.

### The rest

| Check | Observed result |
| --- | --- |
| Metrics | Smoke: runs by status, run and first-token latency, tool latency per server, tokens, active runs, store size, all at `GET /metrics` |
| Read pool | 20 parallel creates each read back on another connection at once; contract 20 of 20 three times |
| Retention (`tests/operations.rs`) | A closed session goes whole only past its cutoff; an idle one loses events and keeps its transcript; a session with queued work keeps everything; a stream with a cursor before the gap can tell |
| Backup and restore (`tests/operations.rs`) | With a command cut off mid-run: the copy opened, `recover` found it, it ran and finished; what came after the backup wasn't in the copy; the live store stayed writable |
| Backup on a real process | Host file with `backup` and `retention`: a copy was written at startup while serving, and that copy served as the store of a new process |
| Usage | Smoke: a turn's tokens in `usage.models`; a foreground child's in `usage.tools.subagents`; a task's in `tasks[].usage` and `taskUsage` |
| `maxUnattendedTokens` (`tests/operations.rs`) | Past the budget a report became a note; after a user spoke, the same spend was behind them and a report woke the parent |
| Hook timeouts | `within` unit-tested |
| `cargo test --workspace` | 98 passed |
| `scripts/test-local.mjs`, `./scripts/test-k8s.sh`, `scripts/test-replicas.mjs`, `./scripts/test-replicas-k8s.sh` | Passed |
| Contract | agent-ti and agent-pi, 20 of 20 |
| Local cluster, namespace `t10` | `t10/agent-ti:ops`, same instance ID, `/metrics` answers. Its host ConfigMap wasn't re-applied, so it keeps 16 runs per tenant and 1,000 streams until the overlay is |

Found while doing it: `before_tool_call` errors were swallowed, so a failing security hook let the call through, against [plugins.md](plugins.md); it now refuses. The contract suite registered `after(rmSync(data))` inside `before`, and `node:test` runs that when the hook ends, so it deleted the harness's storage during the tests. One open connection survives that and a pool can't, so the suite now removes it at the end (agent-pi passes either way). `scripts/test-replicas-k8s.sh`'s first version left port-forwards running.

Not tested: `before_tool_call` refusing end to end (no plugin in the smoke fails), the retention job on its hourly timer (`prune` itself is tested), retention or backups on the cluster, and `/metrics` being scraped: nothing on the platform collects Prometheus metrics yet, as `ARCHITECTURE.md` notes.

## 2026-11-06: flow fixes, in agent-ti and agent-pi

Two bugs in the engine agent-ti copied, fixed in both so they stay the same:

| Bug | Before | After |
| --- | --- | --- |
| A join after a switch depended on edge order | With the edge to the join listed before the edge to the skipped branch, the join stayed `pending` and its tool never ran; the flow still reported `completed` | The skip reports what it leaves ready, so the join runs in any order, and through a chain of skipped steps |
| A result with `storeAs` was in the evidence twice | Under its step's name and its stored name, doubling what the model read | Once, under the stored name; later steps can still read it by either |

New tests in `tests/flow.rs` and agent-pi's `flow.test.ts` were run with each fix disabled, to see them fail, and pass with the fixes in. After the fixes: agent-pi's whole suite 282 of 282, and the contract 20 of 20 on both. agent-pi isn't redeployed. agent-ti is on namespace `t10` as `t10/agent-ti:flowfix`.

## 2026-11-06: usage of runs that end badly

Spend is now added in the transaction that records each model call, not when the run finishes. A store test fails a run after a recorded call and finds its tokens kept, and a reused step adds nothing. The smoke cancels a run during its tool call and finds the first model call's 15 tokens in `usage`. A model call cut off mid-stream still counts nothing, since providers report usage at the end of a reply. Not tested: a crash between calls end to end.
