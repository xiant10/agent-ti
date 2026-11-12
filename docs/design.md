# agent-ti design

Status: revised 2026-11-05. This document describes the target. What the code does today, and how it falls short of this, is in [implementation-plan.md](implementation-plan.md).

agent-ti is agent-pi written in Rust. It copies agent-pi's design: the protocol, configuration format, plugin hooks, skills and tool loading, grants, and voice output. Read this alongside agent-pi's [design](../../agent-pi/docs/design.md) and [lifecycle](../../agent-pi/docs/lifecycle.md). When this document doesn't say otherwise, agent-pi's behavior is the specification.

The first version (written 2026-11-04) planned a microkernel with a separate publication control plane, PostgreSQL leases and Wasm plugins. That plan is withdrawn; its ideas are kept under [later, not now](#later-not-now).

## Why a Rust agent-pi

agent-pi gets durability from `@earendil-works/pi-durable` and runs one agent per Node process. agent-ti aims at:

- **More agents per process.** One process hosts several agent configs, so a small agent doesn't cost a whole process.
- **Lower and steadier memory and latency** under many concurrent sessions, without a garbage-collected event loop.
- **Durability we own.** A small, explicit SQLite journal of commands, steps and results instead of a library's document store.

Everything else, including what clients see, matches agent-pi, so the gateway, Workbench, the voice app and the agent contract treat both the same way.

## What differs from agent-pi

| Topic | agent-pi | agent-ti | Why |
|---|---|---|---|
| Language and durability | TypeScript on pi-durable | Rust; own SQLite journal | Memory and latency under load; durability we can inspect |
| Agents per process | One | Several, at `/agents/{name}/…`, each with its own SQLite file | Small agents shouldn't each cost a process; a store per agent keeps agent-pi's isolation of storage |
| Host settings | In `agent.json` | Port, storage, auth and process limits in a small host file, which overrides the same fields in `agent.json` | Agents in one process share them; the same `agent.json` runs on both |
| Plugins | TypeScript objects | Rust traits compiled in, chosen by name | No dynamic loading; same hook list |
| Event replay | Memory ring; snapshot after restart | Durable events in SQLite, except text and thinking deltas, which come from a memory ring | Resume survives restart for everything but a reply in progress, which is re-run after a crash anyway |
| Tenant limits | Per user and global | Also per tenant, with fair scheduling | Several tenants and agents share a process |
| Reading config | Local files | Every config read goes through a config source: local files, or a remote config API | Agents can be deployed without rebuilding an image or ConfigMap |
| Tool rounds | Bounded by pi-durable | `limits.maxToolRounds` in `agent.json` | The loop is ours, so its bound is a setting |
| Parallel tool calls | pi-durable's | `limits.maxParallelTools` in the host file | Same reason; a process limit, since agents share the process |
| When compaction is checked | Before every model request | Before each run; a run's own tool rounds are bounded by the byte budget, which drops whole old turns of the request if it must | The run's history is built once and its steps are journaled against it; a mid-run cut would change recorded requests |
| How a parent's tool waits for its child | Durable's submission wait | Polls the child's command outcome, every 20–250 ms | No in-process wait on another session's command yet; the poll reads one indexed row |
| Background tasks' engine | pi-durable anchor and delivery tasks | `tasks` and `task_messages` tables and a per-agent supervisor, looking every 200 ms or when woken | There's no durable task engine here; the behavior in `async-tasks.md` is the specification. A deadline can be enforced up to one tick late |
| When a parent is offered `task_send`, `task_list`, `task_stop` | From its first task on | From the start, on an agent with a background sub-agent | Pinned tools come from the profile, not per session; they cost a little in every request, and refuse nothing |
| A flow's tool calls after a crash | `load_skill` is `replay: safe`, so the whole flow runs again | Each of a flow's tool calls is a journaled step: a finished one is reused, and one cut off comes back to the flow as `interrupted` | Every MCP tool is unsafe to repeat (see [durability](#durability)), and a flow can call one that writes |
| A flow's `extract.pattern` | A JavaScript `RegExp`, compiled when it runs | The `regex` crate, checked when the skill is read: no lookaround or backreferences, and a pattern that uses them fails its checks | No backtracking in a process tenants share; a bad pattern shows up at `skills.checkFlow`, not on a user's turn |
| Flow traces kept | The latest and nine before it | The latest | Nothing reads the older ones |
| How `usage` is broken down | By model and tool | Input and output tokens: the session's own under its profile's model, foreground children's under `tools.subagents`. Counted as each model call is journaled, so a failed or cancelled run keeps what its finished calls spent; a call cut off mid-stream counts nothing, since providers report usage at the end | The totals clients show and `maxUnattendedTokens` need these; per-model detail and cost wait until something reads them |
| Compaction in the transcript | A `{role: "compaction"}` line in `session.messages` | None: the transcript lists messages only, and the `compaction` events mark it | Not yet built |
| Where agents live | In the repo (`apps/assistant`), built into the image | Nowhere in this repo but its test agents: the image's host file loads every agent folder a deployment puts in `/etc/agent-ti/agents`; t10's ship as the `t10-agents` image | The runtime is building blocks; an agent's prompts and tools change on their own schedule, and adding one shouldn't touch the runtime |
| Paths in `agent.json` | Any path relative to the file | Within the agent's config snapshot: for a local file, the app directory (`<app>` for `<app>/config/agent.json`) | A remote snapshot can't name local files; local agents behave the same way, and agent-pi's layout still resolves |

Everything not in this table should behave as agent-pi does. A new difference needs a row here with its reason.

## Requirements

agent-pi's requirements, unchanged. Build status for each is in the [plan](implementation-plan.md).

| # | Requirement | agent-pi source |
|---|---|---|
| R1 | Streamable HTTP | `transport/streamable-http.ts` |
| R2 | Steering and barge-in | `session.steer`, `session.interrupt` |
| R3 | Skills that load tool schemas on demand | `skills/`, `tools/activation.ts` |
| R4 | Runs in Docker | `Dockerfile`, probes, SIGTERM |
| R5 | Many users' sessions at once | per-user and global limits |
| R6 | JWT before any agent work, with an extension hook | `auth/`, plugins' `authenticate` |
| R7 | Fast enough for voice, still able to reason | profiles, `speech`, preloading, `consult_expert` |
| R8 | Hundreds of MCP tools | catalog, active set, search, eviction |

## Layers

The same layers as agent-pi. pi-durable's place is taken by agent-ti's own session worker and journal.

```
 HTTP request
   ▼
 transport      Streamable HTTP: JSON-RPC over POST, SSE responses, GET event stream, resumable
   ▼
 auth           Authenticator -> Principal, then plugins' authenticate (runs first, always)
   ▼
 service        create/list/get/close, prompt, steer, interrupt, cancel; ownership; limits
   ▼
 plugins        lifecycle, before_prompt (may answer without the model), after_run, map_event
   ▼
 events         committed events, client mapping, speech segmentation, SSE fan-out
   ▼
 session worker one logical worker per busy session: the model/tool loop, steps journaled
   ├── tools    catalog of every MCP and plugin tool, per-session active set, search, eviction
   ├── skills   built-in skill files + the tenant's skills from t10-agent-control
   └── mcp      HTTP MCP clients, per-grant connections, list_changed refresh
 store          SQLite: sessions, commands, steps, messages, events
```

## Package boundary

agent-pi splits the harness from apps: `packages/agent-pi` holds only technical concerns, and `apps/<product>` holds the config, prompts, skills, content and plugins. agent-ti keeps that split:

- The **runtime** (`agent-ti` binary and crates) holds no prompt text, persona, model choice or integration.
- An **agent** is a directory with `agent.json`, prompts, skills and content, in agent-pi's format, read through a config source: the local filesystem or a remote config API (see [configuration.md](configuration.md#config-sources)). An agent that needs only config runs on the shared binary.
- **Plugins** are Rust code compiled into the binary and chosen by name in `agent.json`, as agent-pi's `pluginCatalog` does. A config can't name a file or library to load. A product with plugins of its own builds its own binary from the runtime crates and its plugin crate, the way an agent-pi app calls `runAgent({ plugins })`.

The hook points are agent-pi's, one for one; [plugins.md](plugins.md) has the Rust shape and replay rules.

```
request ─► authenticator ─► plugin.authenticate × n
session.create ─► plugin.session_created × n
session.prompt ─► limits ─► plugin.before_prompt × n ─┬─ reply ──► write user + assistant messages (no model) ─┐
                                                      ├─ reject ─► 422                                       │
                                                      └─ input', tools ─► preload ─► admit                   │
   generation: before_model_request ─► model ─► after_model_response ─► on_final_answer (may continue)       │
   tool call:  before_tool_call ─► execute ─► after_tool_call                                                │
   settled ─► plugin.after_run × n ◄──────────────────────────────────────────────────────────────────────────┘
events ─► event mapper ─► plugin.map_event × n ─► SSE
```

`before_prompt` runs **before admission**, as in agent-pi, so `reject` is a synchronous 422 and `reply` returns its answer without the prompt ever being queued.

**Answering without the model**, copied from agent-pi: a plugin's reply is stored as a user message and an assistant message with `source` naming the plugin and zero usage, so the next model request sees the exchange. Clients receive it the same way as a model answer (`text.delta`, `speech`, `message.end` with `source`). When the session is idle the pair is committed at once. When it's busy, it waits in the session's inbox and is placed at the next turn boundary, never inside a tool round.

## R6: authentication

As in agent-pi: one `Authenticator` produces a `Principal { user_id, tenant_id, scopes, claims, expires_at }` before routing. JWT verification checks the signature, `exp`/`nbf`, issuer, audience and required scopes, with the user and tenant read from configured claims. `none` is never accepted and an HMAC key never verifies an asymmetric token. Then each plugin's `authenticate` hook runs in order and may change the principal or reject. agent-pi's algorithms, JWKS refresh rules and static-token development authenticator all carry over.

A session belongs to `(tenant, agent, user)`. Another user's session returns 404. The raw bearer token is never stored or forwarded.

## R1: Streamable HTTP transport

The same transport, methods and events as agent-pi's design, so a client can't tell the two apart.

- `POST /agent` takes one JSON-RPC request. `Agent-Session-Id` (or `params.sessionId`) selects the session. With `Accept: text/event-stream` on `session.prompt` and `session.interrupt`, the response is that run's SSE stream ending in the JSON-RPC result. Otherwise it's JSON.
- `GET /agent` with `Agent-Session-Id` is the session's event stream. `Last-Event-ID` resumes from durable events in SQLite, so a resume survives a restart. An unknown or expired cursor starts with a `snapshot`.
- `DELETE /agent` closes the session: a 204, or 421 on the wrong replica.
- Methods: `session.create`, `list`, `get`, `messages`, `update`, `prompt`, `steer`, `interrupt`, `cancel`, `retry`, `close`, `skills.list`, `skills.get`, `skills.tools`, `skills.checkFlow`, plus plugin `rpc` methods, with agent-pi's arguments and results.
- Events: `snapshot`, `run.started`, `run.finished`, `text.delta`, `speech`, `thinking.delta`, `message.end`, `tool.started`, `tool.finished`, `tools.active`, `task.updated`, `flow.updated`, `inbox`, `submission`, `retry`, `compaction`, `error`, with agent-pi's fields. `text.delta.offset` counts UTF-16 code units, as JavaScript clients expect.

Each agent is served at `/agents/{name}/agent` and `/agents/{name}/v1/runtime`, each looking like a single agent-pi endpoint to the gateway. Plain `/agent` serves the host's default agent.

**Live text.** A model reply streams. Text and thinking deltas are published from memory as they arrive, batched at most every 100 ms (agent-pi's floor on delta latency), and are not committed. They take IDs from the session's event sequence (the last committed event's, plus their index after it) and sit in a per-session memory ring, so a reconnect within the ring resumes exactly. A cursor that names a delta no longer in the ring, or any delta cursor after a restart, gets a `snapshot`; its `partial` is the reply in progress, from memory. `message.end` and every state change are committed before they are published.

## R2: steering

Copied from agent-pi:

- `session.steer` queues input that joins the current run at the next tool-round boundary. Without a tool round it waits for the answer, then starts the next run.
- `session.interrupt` stops generation and tool calls, withdraws queued inputs, keeps the aborted partial answer in storage but out of model context, and submits the new input as a fresh prompt. `duringTools: "steer"` lets running tools finish and only aborts text, so the model doesn't report a sent email as cancelled.
- A retried `interrupt` with the same `requestId` doesn't abort again.

Cancellation and interrupt reach the worker through an in-process cancellation token set when the command commits. The durable cancel flag is for recovery after a restart.

## R3 + R8: skills and dynamic tool loading

Copied from agent-pi: register every tool, send few.

1. **Catalog.** Every MCP server's tools, and every plugin's tools, are registered as `<prefix><tool>`. Registering costs nothing per request. Servers are listed at startup all at once; one that is down is retried in the background at most every 30 s, never inside a run, so it never holds up a turn, and its tools are offered from the first run after it answers, as in agent-pi. `notifications/tools/list_changed` re-lists a server. A tool call reuses the MCP session of its server and credential (a grant's token), as agent-pi keeps a client per grant, so a call is one request rather than `initialize`, `initialized` and the call; a session the server ended (404 or 400) is replaced and the call sent once more, since the server didn't run it. Sessions idle 5 minutes, or past 256, are dropped.
2. **Core set.** A new session offers only the meta-tools (`load_skill`, `search_tools`) and the profile's `alwaysOnTools`.
3. **Skills.** Markdown files with `name`, `description`, `tools` patterns and `keywords` frontmatter, in agent-pi's format, so the same skill files work in both. The system prompt lists names and descriptions. `load_skill` returns the instructions and activates the matching tools in one step.
4. **Search.** `search_tools` runs BM25 over names and descriptions and activates the top matches.
5. **Preload.** Before a prompt is admitted, the same index scores the input; matching skills and tools above `preload.minScore` are activated, up to `preload.maxTools`.
6. **Budget.** `maxActiveTools` caps the set; the oldest dynamically activated tools are evicted. Activation and eviction are journaled, so replay and the next request offer the same tools. Additions append, which keeps the provider's cached prompt prefix stable.

**Tenant skills** come from `t10-agent-control`, as in agent-pi: `GET /v1/runtime/skills?tenant=`, cached 10 seconds with ETags and refreshed early by the change feed. **Tenant connectors** come from `ai-gateway-tools`' `/mcp/tenant`, an `auth: "grant"` MCP server with `perTenant: true`: listed at the start of a turn with the turn's grant token, kept per tenant for 10 seconds, its tools offered only to that tenant's sessions, and named by `_meta["t10/server"]` for skill patterns. agent-ti has no connector code beyond that option. Each agent in a host has its own runtime client in its `grants`, so the gateway can tell which agent a token runs. A turn pins the skill version it started with. agent-ti doesn't keep its own copy of tenant settings. See [tenant skills](../../t10-agent-control/docs/tenant-skills.md) and [tenant connectors](../../t10-agent-control/docs/tenant-connectors.md).

**Tool errors go back to the model.** An unknown tool name, arguments that fail the schema, a denied call, an MCP error or a timeout becomes a tool result with `isError: true`, as in agent-pi. The model can correct itself; the turn doesn't fail.

Independent tool calls in one round run in parallel, within a per-session limit, as pi-durable does.

## R7: speed and reasoning

Copied from agent-pi:

- **Profiles** pick the model, thinking level, extra instruction sections, output cap, `alwaysOnTools` and preload settings. A `voice` profile uses a fast model.
- **Few tools per request** and **tool preloading**.
- **Prompt cache:** a stable prefix; plugin context added by `before_model_request` is per-request and not stored.
- **Speech segmentation on the server:** `speech` events carry complete sentences, markdown stripped, flushed at tool start and run end.
- **`consult_expert`:** the voice profile's fast model can hand a hard question to a child run on the expert model, as in agent-pi's [delegation](../../agent-pi/docs/delegation.md). Sub-agents and background tasks follow agent-pi's documents.
- **Flows:** `load_skill` with `query` runs a skill's deterministic `flow.json` without the model, cutting several tool-calling rounds to one, as in agent-pi.

## R5: multi-user and scaling

- **Sessions.** One logical worker per busy session serializes its transcript; many sessions run as Tokio tasks. Idle sessions hold no task or memory.
- **Limits** as in agent-pi: `maxSessionsPerUser`, `maxConcurrentRunsPerUser`, `maxConcurrentRuns`, `maxBodyBytes`, returning 429 with `Retry-After`. agent-ti also caps runs and queued work per tenant, because one process serves several agents and tenants. Runs are picked round-robin across tenants.
- **Idempotency** as in agent-pi: a `requestId` names one input; a retry returns the original submission, or the original plugin reply, without running hooks again.
- **MCP credentials** as in agent-pi: servers with `auth: "grant"` get a short token minted from the session's agent grant via auth-server, never the caller's token. The session stores only the grant ID, so a run continues with the user offline and after a restart. Sub-agents get child grants. Operator credentials stay for servers configured that way.
- **Several agents.** One process hosts several agent configs (agent-pi uses one process per agent). Each agent has its own SQLite file and lock, which records the agent and refuses to open as another, as agent-pi's storage does. So one agent's storage error stops only that agent, agents don't share a writer, and an agent can move to a host of its own by moving its file. The agents share the scheduler, process limits and the HTTP server; each agent's tools, plugins and skills are its own.
- **Horizontal scaling** as in agent-pi's [replicas](../../agent-pi/docs/replicas.md): one owner process per SQLite file, enforced by a lock; N replicas each with their own volume; public session IDs `<agent>.<replica>.<n>`; a request for another replica's session gets 421 with `Agent-Instance` so the gateway routes it.
- **Long sessions.** Compaction summarizes older turns in the background, as pi-durable does.
- **Restart.** Unfinished runs resume from the journal; event streams restart with a `snapshot`.

## Durability

pi-durable gives agent-pi durable conversations. agent-ti has its own journal; [storage.md](storage.md) has the details.

- An accepted command is committed before the client is told it was accepted.
- Each model call, tool call and hook decision is a step: prepared, then its result recorded. After a crash a recorded result is reused, not repeated.
- A tool call is safe or unsafe to repeat, from the tool's `replay` setting as in agent-pi: every MCP tool is unsafe; a plugin tool is unsafe unless it declares `replay: "safe"`. An unsafe call that was prepared but has no result is not repeated. It comes back to the model as `interrupted`, and the run continues.
- A command cut off by a restart `durable.restartLimit` times in a row (default 5) without progress is parked and logged, as in agent-pi, so a crash loop doesn't take the process down with it. The session stays busy and `session.get` shows `parked`; `session.retry` runs it again, and `session.cancel` or `session.interrupt` gives up on it.
- Hooks that run inside a step get read-only state and `memo`, as in agent-pi, so a replay makes the same decisions.

## R4: Docker

One image holds the runtime and the bundled agents. `AGENT_CONFIG` (one location or a comma-separated list, local paths or a remote source's agents) picks the agents a container runs. `GET /healthz` and `GET /readyz` are unauthenticated; an MCP server being down doesn't make the replica unready. SIGTERM stops admission, sends `server.shutdown` on open streams, waits for running steps up to a deadline, and leaves the rest to resume on the next start. SQLite lives on the container's volume.

## Review decisions

The 2026-11-05 review settled these; the sections above carry the result.

1. An unsafe tool cut off by a crash comes back to the model as `interrupted`; only crash loops park, as in agent-pi. Safety comes from each tool's `replay` setting.
2. Text and thinking deltas are published from memory and not committed; everything else is.
3. Each agent has its own SQLite file, even when several agents share a process.
4. agent-ti reads an unmodified agent-pi `agent.json` ([configuration.md](configuration.md#agent-file)).
5. A session records its agent's `bundle` version, as agent-pi does.
6. agent-ti's memory and latency are measured against agent-pi after live text lands, before the larger ports ([plan](implementation-plan.md), step 2a).

Also decided that day: all configuration is read through one config-source abstraction, local files or a remote API ([configuration.md](configuration.md#config-sources)).

## Later, not now

These ideas from the first version are kept as options. None of them shapes the current design:

- **Versioned agent publication:** digest-pinned agent revisions published through an API, with sessions pinned to a revision. Useful if agents are ever edited online; agent-pi's model (files in the image, a rollout to change them, tenant changes through agent-control) is enough today.
- **PostgreSQL with leases**, so any replica can run any session. Needs a gateway and contract change; sticky SQLite replicas work for now.
- **Wasm plugins** for tenant-supplied code.
