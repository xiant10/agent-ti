# Configuration

Status: revised 2026-11-05. This describes the target: agent-pi's `agent.json` format plus a small host file, read through config sources. What is built is in the [plan](implementation-plan.md).

## Two kinds of file

agent-pi has one `agent.json` per process, holding the agent and the process settings. agent-ti runs several agents in one process, so the process settings move to a host file and each agent keeps an agent-pi `agent.json`.

| File | Owner | Holds |
|---|---|---|
| Host file (`agent-ti.json`) | Deployment | Port, instance ID, storage, auth, process limits, HTTP options, the config sources and the agents to load from them |
| `agent.json` per agent | Agent author | Everything else in agent-pi's `agent.json`: models, profiles, instructions, skills, plugins, grants, MCP servers, tenant skills and connectors, per-agent limits |

Both use agent-pi's `${VAR}` and `${VAR:-default}` substitution, applied after a document is read, whatever its source.

## Config sources

Every piece of configuration is read through one abstraction, a **config source**, never by opening a file directly. That covers `agent.json`, the files it points to (instructions, profile instruction files, the skills directory, plugin option files such as `faqFile`) and anything a plugin reads at setup. The host file is the only exception: it is the bootstrap that names the sources, so it is always a local file.

A source turns a location into a **snapshot**: an immutable, versioned set of documents. An agent is loaded from one snapshot, so it never mixes files from two versions.

```rust
pub trait ConfigSource: Send + Sync {
    /// Read the agent at `location` and everything under it, as one consistent version.
    fn load(&self, location: &str) -> BoxFuture<'_, Result<Snapshot, SourceError>>;
}

pub struct Snapshot {
    pub version: String,                     // ETag, content hash or API revision
    pub root: String,                        // the agent file's path within the snapshot
    files: BTreeMap<String, Bytes>,          // relative path -> content
}
// Snapshot::read(path) and Snapshot::list(dir) resolve paths relative to the agent file,
// and refuse a path that leaves the snapshot ("..", absolute paths).
```

Kinds:

| Kind | Location | Reads |
|---|---|---|
| `file` | A path, or `file:///…` | The agent's app directory on the local filesystem (the image, a mounted ConfigMap, a checkout): `<app>` for agent-pi's `<app>/config/agent.json` layout, so `../prompts` and `../skills` resolve as in agent-pi, otherwise the agent file's own directory. Symlinks are followed only within it, which is how ConfigMap keys are projected. The version is a hash of the files read |
| `http` | `<source>:<name>`, a source named in the host file | A remote config API, below. Its URL is `https://…`, or plain `http://` only for cluster-local hosts |

More kinds (an object store, an OCI artifact) implement the same trait. The runtime, the agent loader and plugins see only `Snapshot`.

### Remote config API

An `http` source reads `GET <url>/<name>`, which returns the whole agent directory as one JSON document:

```json
{
  "version": "rev-42",
  "root": "agent.json",
  "files": { "agent.json": "…", "base.md": "…", "skills/crm/SKILL.md": "…" }
}
```

with an `ETag` equal to `version`. File contents are UTF-8 text; binary content is base64 under `"filesBase64"`. The source authenticates with a bearer token from an env var or a client-credentials token from auth-server, as the host file says. No t10 service serves this API yet; `t10-agent-control` is the natural owner if agents are ever edited online.

### Loading and failure

- Sources are read at startup. An agent whose source can't be read, or whose config is invalid, doesn't start, and the process fails startup, as agent-pi does with a bad config.
- An `http` source keeps the last good snapshot under `storage.dir/config/<agent>/`. If the API is down at startup, the cached snapshot is used and a warning logged, so a restart doesn't depend on the config API being up.
- A changed config takes effect at the next restart, as in agent-pi. Reloading a running agent from a newer snapshot belongs with versioned publication under [later, not now](design.md#later-not-now).
- A remote document is trusted as much as a file in the image: whoever controls the source controls the agent, including which `${VAR}` values reach which URLs. Point a source only at a service with the same trust as the deployment.

Tenant skills and connectors are not config sources. Skills are per-tenant data read per turn from `t10-agent-control`, with their own ETag cache and change feed; connectors are the tools of a `perTenant` MCP server at `ai-gateway-tools`, listed per turn ([design](design.md#r3--r8-skills-and-dynamic-tool-loading)).

## Host file

```json
{
  "port": "${PORT:-18120}",
  "instanceId": "${AGENT_INSTANCE_ID:-}",
  "logLevel": "${LOG_LEVEL:-info}",
  "storage": { "kind": "sqlite", "dir": "${AGENT_DATA_DIR:-./data}" },
  "auth": {
    "kind": "${AUTH_KIND:-jwt}",
    "algorithms": "${JWT_ALGORITHMS:-RS256,ES256}",
    "jwksUrl": "${JWT_JWKS_URL:-}",
    "issuer": "${JWT_ISSUER:-}",
    "audience": "${JWT_AUDIENCE:-}",
    "requiredScopes": "${JWT_REQUIRED_SCOPES:-}",
    "userClaim": "${JWT_USER_CLAIM:-sub}",
    "tenantClaim": "${JWT_TENANT_CLAIM:-tid}"
  },
  "limits": {
    "maxBodyBytes": "${MAX_BODY_BYTES:-16000000}",
    "maxConcurrentRuns": "${MAX_RUNS:-200}",
    "maxConcurrentRunsPerTenant": "${MAX_RUNS_PER_TENANT:-100}",
    "maxQueuedPerTenant": 1024,
    "maxQueuedPerSession": 16,
    "maxSessionsPerTenant": 10000,
    "maxStreams": 2000,
    "maxParallelTools": 4
  },
  "http": { "allowedOrigins": "${ALLOWED_ORIGINS:-}" },
  "sources": {
    "control": { "kind": "http", "url": "${AGENT_CONFIG_API:-}", "tokenEnv": "CONFIG_TOKEN" }
  },
  "agents": "${AGENT_CONFIG:-/etc/agent-ti/agents}",
  "defaultAgent": "${DEFAULT_AGENT:-}"
}
```

- `auth` takes agent-pi's JWT fields: `algorithms`, `jwksUrl`, `issuer`, `audience`, `requiredScopes` (a token without one gets 403), `userClaim`, `tenantClaim` (unset: every caller is in tenant `default`), `nameClaim`, `clockToleranceSec` (default 60).
- `limits` are the process's: the body size, runs in the process and per tenant, and the per-tenant and per-session queue, stored sessions per tenant, event streams and parallel tool calls per round. The queue and session limits apply per agent, since each agent has its own store. Sessions and runs per user stay in each `agent.json`, as in agent-pi.
- `storage.dir` holds one `<agent>.sqlite` per agent ([storage](storage.md#store)) and the config cache.
- `sources` names remote sources, their base URL and how they authenticate. The local filesystem needs no entry.
- `agents` is one location or a comma-separated list. A plain path is a local file, or a folder: each of its subfolders with an `agent.json` is an agent, in name order, and other files are ignored. A folder with none fails startup. `<source>:<name>` reads agent `<name>` from a named source, at `GET <url>/<name>`; for example `control:sales`.
- `defaultAgent` is the agent served at plain `/agent` and `/v1/runtime` (default: the first agent); every agent is also served at `/agents/{agent}/…`.
- An unset `instanceId` is generated once and kept in each agent's store, as agent-pi does.

`agent-ti serve <host file>` serves; `agent-ti check <host file>` loads every agent, with its startup warnings, and exits.

## Agent file

agent-ti reads an unmodified agent-pi `agent.json`. The same file runs on both runtimes, apart from plugins that exist in only one.

```json
{
  "agent": "ti",
  "bundle": "${AGENT_BUNDLE:-}",
  "models": {
    "chat": {
      "provider": "openai-compatible",
      "modelId": "${CHAT_MODEL_ID:-qwen3.5-flash}",
      "baseUrl": "${LLM_BASE_URL:-http://ai-gateway-llm:7200/v1}",
      "apiKeyEnv": "LLM_API_KEY",
      "contextWindow": 128000,
      "maxOutputTokens": 2048,
      "identityHeaders": "gateway"
    }
  },
  "defaultProfile": "chat",
  "profiles": {
    "chat": { "model": "chat", "maxActiveTools": 24, "alwaysOnTools": [], "preload": { "maxTools": 4, "minScore": 4 } }
  },
  "instructions": [{ "name": "base", "file": "base.md", "tag": false }],
  "skillsDir": "skills",
  "tenantSkills": { "url": "${AGENT_CONTROL_URL:-}" },
  "plugins": [{ "use": "clock", "options": { "timeZone": "Asia/Shanghai" } }],
  "grants": { "url": "${AUTH_URL:-}", "clientId": "agent-ti", "clientSecret": "${AUTH_CLIENT_SECRET:-}" },
  "mcpServers": [],
  "limits": { "maxSessionsPerUser": 1000, "maxConcurrentRunsPerUser": 20, "maxToolRounds": 12 }
}
```

Field meanings are agent-pi's (see its [developer guide](../../agent-pi/docs/developer-guide.md) and `config.ts`). Paths such as `file`, `skillsDir` and `faqFile` are relative to the agent file within its snapshot. How agent-ti treats each group:

| Fields | agent-ti |
|---|---|
| `port`, `host`, `instanceId`, `logLevel`, `storage`, `auth`, `http`, `limits.maxBodyBytes`, `limits.maxConcurrentRuns` | Host settings. The host file's values apply; a different value in `agent.json` is logged at startup |
| `agent`, `bundle`, `models`, `profiles`, `defaultProfile`, `instructions`, `skillsDir`, `plugins`, `grants`, `mcpServers`, `tenantSkills`, `tenantConnectors`, the other `limits` | agent-pi's meaning |
| `durable.compaction`, `durable.retry`, `durable.restartLimit`, `durable.stream.timeoutMs` | agent-pi's meaning, applied by agent-ti's own journal and loop |
| `durable.stream.cacheRetention` | Passed to the provider as its prompt-cache hint |
| `limits.maxToolRounds` | agent-ti only; agent-pi ignores it |
| An agent-pi field whose feature isn't ported yet (`subagents`, `progressModel`, `mcpEndpoint`, …) | Accepted with a startup warning naming the missing feature; the feature isn't advertised in `/v1/runtime` |
| A field agent-pi doesn't know either | Rejected, so a typo stops startup |

`plugins[].use` names a plugin compiled into the binary; each plugin validates its own `options` at startup and reads any file it needs through the agent's snapshot. An unknown plugin name or invalid options stops startup, so a misconfigured agent never serves.

## Changing an agent

As with agent-pi:

- **Prompts, profiles, built-in skills, plugin options:** change them in the agent's source (the image, a mounted ConfigMap, or the remote config API) and restart.
- **A tenant's skills and connectors:** published in Workbench through `t10-agent-control`; the next turn uses them, with no restart.
- **A new plugin:** Rust code, so a new binary.

Each session records the config version it started under: the agent's `bundle` field, as agent-pi shows in `/v1/runtime`, or else its snapshot's version. A session continues under the config the process has now; agent-pi works the same way.

## Retention and backups

Both are off unless the host file asks, since removing conversations and where copies of them go are the operator's decisions. Every agent in the host follows the same settings.

```json
{
  "retention": { "eventsDays": 30, "closedSessionsDays": 90 },
  "backup": { "dir": "/backups", "intervalMinutes": 60, "keep": 24 }
}
```

| Field | Effect |
| --- | --- |
| `retention.eventsDays` | A session idle this long loses its events and finished steps. Its transcript stays, and a stream resuming from before them starts with a `snapshot` |
| `retention.closedSessionsDays` | A session closed and idle this long is removed whole, its children with it |
| `backup.dir` | Each agent's copies go in `<dir>/<agent>/<millis>.sqlite`. Put it on another volume: a copy on the same volume doesn't survive losing it |
| `backup.intervalMinutes`, `backup.keep` | How often (default 60), and how many to keep (default 24) |

Retention runs at startup and hourly, and never touches a session with queued or running work. A backup runs at startup and then at each interval. To restore, stop the replica, put the copy in place as `<storage.dir>/<agent>.sqlite`, remove any `-wal` and `-shm` beside it, and start: unfinished work in the copy runs again, as after a crash.

`limits.hookTimeoutMs` (default 10,000) bounds each asynchronous plugin hook. `authenticate`, `before_prompt` and `before_tool_call` refuse when one fails or takes too long; `session_created`, `session_closed` and `after_model_response` are logged and ignored.
