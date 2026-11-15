# agent-ti API

The reference for calling agent-ti. It serves the agent contract in the t10 workspace's [`contracts/agent/README.md`](../../contracts/agent/README.md), and its methods, events and params are agent-pi's, so agent-pi's [`docs/api.md`](https://github.com/xiant10/agent-pi/blob/main/docs/api.md) is the reference for them. This page covers what differs: several agents in one process, the greeting methods, metrics and errors. [`design.md`](design.md#what-differs-from-agent-pi) lists every difference with its reason, [`configuration.md`](configuration.md) the host and agent files, and [`greeting.md`](greeting.md) the greeting in full.

## Reaching it

| From | Base URL |
| --- | --- |
| A client in t10 | `ai-gateway-agent`, or workbench's `/api/agents/{agent}/agent` |
| A service on the cluster | `http://agent-ti:18120` |

## Endpoints

One process serves every agent in its host file. The default agent (`defaultAgent`, else the first loaded) also answers at the root.

| Method and path | Does |
| --- | --- |
| `POST`, `GET`, `DELETE /agents/{name}/agent` | One agent's `/agent`: JSON-RPC requests, the session's SSE events, and closing a session |
| `POST`, `GET`, `DELETE /agent` | The same, for the default agent |
| `GET /agents/{name}/v1/runtime`, `GET /v1/runtime` | The manifest, with no token |
| `GET /agents/{name}/readyz`, `GET /readyz` | Readiness, with no token |
| `GET /healthz` | `200` while the process runs |
| `GET /metrics` | Prometheus text: open streams, active runs, store size per agent |

Headers, tokens and SSE resume are as agent-pi's. Session IDs are `<agent>.<instance>.<n>`.

## The manifest

```json
{
  "contract": 0, "harness": "ti", "harness_version": "…", "pool": "…", "agent": "ti", "bundle": "<agent version>",
  "profiles": ["chat", "voice"],
  "capabilities": ["durable_runs", "steer", "interrupt", "voice", "retry", "background_tasks", "greeting", "tenant_config"],
  "models": ["…"], "builtin_tools": [], "methods": []
}
```

`pool` is the agent's name. `models` lists the agent's model entries, and `tenant_config` appears for an agent tenants may customize, whose admins pick among those models. An agent-pi feature not yet ported, such as `mcpEndpoint`, is accepted with a warning and left out of the manifest.

## Methods

Every agent-pi method: `agents.list`, `session.create`, `list`, `get`, `messages`, `tool`, `update`, `prompt`, `steer`, `interrupt`, `cancel`, `retry`, `close`, and `skills.list`, `get`, `checkFlow`, `tools`. Two more, for the `greeting` capability:

| Method | Params | Result |
| --- | --- | --- |
| `session.greet` | `channel?` (`call`), `locale?`, `timeZone?`, `client?` (at most 16 strings), `requestId?`, `turnId?`, `prepare?`, `dryRun?` | `{text, source: "greeting", by: "model" or "text", turnId}`, or `{skipped: "off" | "busy" | "recent" | "empty"}`. With `Accept: text/event-stream`, `greeting.delta` events first. `prepare` answers `{prepared: true}` and keeps the text 60 seconds for the call; `dryRun` adds `instructions`, `variables` and `wouldSkip` and says nothing |
| `variables.catalog` | | `{variables: [{name, description, trust, source, example?}]}`, every variable a greeting can use. Needs no session |

What's different in results:

- `session.messages` has no `compaction` line; `compaction` events mark it instead.
- `usage` counts input and output tokens only (`cacheRead` and `cacheWrite` are `0`), the session's own and foreground sub-agents' under `tools.subagents`.
- A session created with `metadata.configDraft: true` runs on its owner's draft customization from `t10-agent-control`, read every turn.

## Events

agent-pi's events, plus `greeting.delta` on a streamed `session.greet`. Events are kept in SQLite, so `Last-Event-ID` resumes across a restart, except text and thinking deltas, which come from memory; a reply in progress at a crash is run again.

## Errors

A JSON-RPC error `{"jsonrpc": "2.0", "id": …, "error": {"code", "message"}}` with an HTTP status. The codes match agent-pi's.

| Status | JSON-RPC code | Means |
| --- | --- | --- |
| 400 | `-32602` | Invalid params or input, or an `Agent-Name` this path doesn't serve |
| 401 | `-32001` | A missing, invalid or expired token |
| 403 | `-32003` | Not allowed |
| 404 | `-32004` | Unknown, closed or someone else's session, or an unknown method |
| 409 | `-32009` | Busy, or a conflict |
| 421 | `-32021` | Another instance's session. The `agent-instance` header names it |
| 422 | `-32022` | Refused |
| 429 | `-32029` | Over a tenant, user or capacity limit. `Retry-After: 1` |
| 503 | `-32603` | Storage or a dependency unavailable. `Retry-After: 1` |
