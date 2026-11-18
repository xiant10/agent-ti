---
title: "Call greeting and context variables"
audience: consumer
kind: design
status: current
summary: "How an agent speaks first on a call, and the context variables a caller passes for it."
---

# Call greeting and context variables

Status: built 2026-11-12 on `feat/call-greeting` branches, not committed. Tested with unit and end-to-end tests; previews on the local cluster.

When someone starts a voice call, the agent speaks first. What it says is either a fixed text or a line the model writes from an instruction. Both can carry **context variables**, such as `{{user.name}}`, `{{team.name}}` and `{{tasks.overdue}}`, which agent-ti fills in with the caller's real values before speaking or before asking the model.

Variables are a general mechanism with one consumer for now: the greeting. Profile instructions, tenant instructions and skill text can use them later without a new design (see "Later").

## Who does what

```
caller's mic track subscribed (t10-livekit-agent)
  └─ greeting turn in LiveCallController
       ├─ TTS Warm()                                   in parallel
       └─ POST /agent  session.greet  (SSE) ───────────► agent-ti
                                                          1. greeting config: tenant > agent; skip rules
                                                          2. variables in the text it uses, in parallel,
                                                             within variablesTimeoutMs, as the caller
                                                          3. fixed text, or one short model call
                                                          4. history.say(text), source "greeting"
          ◄── greeting.delta … then the JSON-RPC result ─
       └─ the same chunking, TTS and playback as any reply; caller speech interrupts it
```

- **agent-ti owns the greeting.** It knows the persona, the tenant's settings, the caller's language and the transcript, and it writes the greeting into the conversation, so the model sees it next turn and chat shows it.
- **The voice worker asks and speaks.** It never composes the greeting except as a local fallback when agent-ti can't answer (`VOICE_GREETING`). See [`t10-livekit-agent/docs/greeting.md`](../../t10-livekit-agent/docs/greeting.md).
- **There is no general "say" method.** A caller holding a grant could otherwise put assistant messages of their own into a transcript. `session.greet` writes only text the server produced.

## Configuration

In `agent.json`, at the top level, because calls reuse the conversation's own profile (a Workbench call runs in `chat`):

```json
"greeting": {
  "instructions": "The caller just joined a voice call. Greet {{user.name ?? \"them\"}} for the {{time.part}}. ...",
  "text": {
    "zh": ["{{user.name}}，{{time.part}}好，我是Ti，有什么可以帮你？", "您好，我是Ti，有什么可以帮您？"],
    "en": ["Hi {{user.name}}, I'm Ti. What can I do for you?", "Hi, I'm Ti. What can I do for you?"]
  },
  "model": "chat",
  "locale": "zh",
  "timeZone": "Asia/Shanghai",
  "timeoutMs": 3000,
  "variablesTimeoutMs": 800,
  "cooldownMs": 120000
},
"variables": {
  "tasks.overdue": {
    "tool": "collab:work_list", "args": { "view": "owned", "due": "overdue" }, "pick": "count",
    "description": "How many of the caller's tasks are overdue"
  },
  "tasks.doing": {
    "tool": "collab:work_list", "args": { "view": "owned", "status": "doing" }, "pick": "items[].title",
    "description": "Titles of the caller's tasks in progress", "ttlMs": 60000
  }
}
```

| Field | Meaning |
|---|---|
| `instructions` (or `instructionsFile`) | The model writes the greeting from this, with variables filled in. Without it the greeting is fixed. |
| `text` | The fixed greeting, and the fallback when the model fails, is late, or the caller chose a simple greeting. A string, a list of alternatives, or either one per language. |
| `model` | A name in `models`. Default: the default profile's model. Thinking is always off. |
| `locale` | The language when the caller's isn't known. |
| `timeZone` | IANA name for `time.*`, unless the call says otherwise. Default `UTC`. |
| `timeoutMs` | How long the model may take to its first word (1000–10000, default 3000). After that, `text`. |
| `variablesTimeoutMs` | Budget for all variables together (100–3000, default 800). |
| `cooldownMs` | No greeting if this conversation was greeted this recently (default 120000; 0 always greets), as when a dropped call comes back. Other recent messages don't count: in Workbench a call starts after typing. |

**Fixed only:** give `text` and no `instructions`. **Model only:** give `instructions`, and `text` is still worth giving as the fallback; without one, a failed model call means no greeting.

**Alternatives:** the first alternative whose variables all have values is used. Ending a list with one that has no variables makes sure something is always said. If none resolves, nothing is said.

**Language:** the call's `locale`, then the caller's language preference, then `greeting.locale`. A language map is matched exactly, then by its primary tag (`zh-Hans` → `zh`), then its first entry. The model is told which language to use.

## Variables

### Syntax

```
{{user.name}}
{{user.name ?? "the caller"}}
```

The flow template syntax (`{{ }}`) plus a quoted default after `??`. No conditions or loops: the model handles a missing value or a zero, and a fixed text uses alternatives. A name is `namespace.field`, lowercase letters, digits and `_`, with `camelCase` fields allowed. Only variables a text actually names are looked up.

### Sources

| Namespace | Values | From |
|---|---|---|
| `user` | `name`, `role` | auth-server `GET /v1/org/members/{caller}?tenant=` with agent-ti's service token: only the caller's own record |
| `team` | `name`, `manager` | the same record's unit (`/v1/org/units`) and its manager's record |
| `time` | `part` (morning, afternoon …, in the call's language), `weekday`, `local` (`14:05`), `date` | the call's time zone, else `greeting.timeZone` |
| `conversation` | `returning` (true/false), `minutesSince`, `recent` (the last few lines) | the session's transcript |
| `agent` | `name` | the agent's config |
| `call` | `locale`, `channel` | the request |
| `client` | anything the client sent, such as `client.screen` | `session.greet`'s `client`, which the voice worker copies from the caller's `t10.client.*` room attributes |
| any other | as configured | a tool variable in `variables`, or a plugin's namespace |

**Tool variables** call one of the agent's tools as the caller. `args` are the tool's arguments. `pick` takes a field of the result after the flow summary (`count`, `items`, `firstName` …); `[]` maps over a list (`items[].title`). `ttlMs` (default 60000, 0 to 600000) caches the value per tenant and user. A tool variable's namespace can't be a built-in one.

**Plugins** add namespaces with `variable_namespaces()` and answer `resolve_variables(ctx, names)`.

### Lookup rules

- **In parallel, within `variablesTimeoutMs`.** A source that fails or is late leaves its variables empty. One slow source never holds up the greeting.
- **As the caller, read-only.** Tool variables run under a child grant of the session's grant that carries only the parent's `*:read` actions, made once per session grant. Its actions and audiences come from the grant itself (`/v1/grants/{id}/chain`), not from a token, because behind a tool gateway a token's audience is narrowed to the gateways. The services enforce it with their own policy, so a misconfigured variable that names a write tool fails instead of changing anything. Built-in sources read only the caller's own records.
- **Logged by name.** A trace and the logs say which variables were looked up, how long each took and whether it had a value, never the value.
- **Caps.** At most 16 tool variables, 8 looked up per greeting; a value is cut to 1,000 characters, a list to 10 items.

### Trust

A value is either **system** (a number, a boolean, a name or a time the platform holds) or **content** (text other people write: task titles, `conversation.recent`, every `client.*` value, and any tool variable unless it says `"trust": "system"`).

- In `text`, every value is put in as it is: no model reads it.
- In `instructions`, a content value that is a string or a list is never pasted into the instruction. Its place reads `[tasks.doing: see context data]`, and the values go into a `<context_data>` JSON block in the user message, which the system prompt says is data, not instructions. Numbers and booleans are always inline.

A missing value in `instructions` without a default reads `unknown`.

## `session.greet`

Capability `greeting`, listed in the manifest and on sessions.

```json
{"jsonrpc":"2.0","id":1,"method":"session.greet","params":{
  "sessionId":"ti.agent-ti-0.…","channel":"call","locale":"zh-Hans","timeZone":"Asia/Shanghai",
  "client":{"screen":"property/unit/12-1901"},"requestId":"…","turnId":"…"}}
```

| Param | |
|---|---|
| `channel` | `call` (default). Reported as `call.channel`. |
| `locale`, `timeZone` | The caller's, when the client knows them. |
| `client` | At most 16 string values, keys `[a-zA-Z][a-zA-Z0-9_]{0,31}`, each at most 200 characters. |
| `prepare: true` | Look up variables and, with `instructions`, write the greeting, then keep it for this session for 60 s without saying it. Answers `{ "prepared": true }`. A call that starts while it's still being written waits for it, up to `timeoutMs` plus `variablesTimeoutMs`, instead of asking the model again. The voice worker sends this when it joins a room, so the greeting is ready when the microphone arrives. |
| `dryRun: true` | Everything except saying it: answers the text and how it was made (below). Skip rules are reported, not applied. For Workbench's preview. |

**Answer.** With `Accept: text/event-stream` (and neither `prepare` nor `dryRun`), an SSE stream of `{"type":"greeting.delta","text":"…"}` events, then the JSON-RPC response as the last event. Otherwise the response alone:

```json
{"text":"张伟，下午好。你有两个任务已经逾期，要我先说说吗？","source":"greeting","by":"model","turnId":"…"}
{"skipped":"recent"}
```

`by` is `model` or `text`. A `dryRun` adds `instructions` (as the model saw them), `variables` (`{name: {"value", "ms", "trust", "source"}}`, the caller's own data) and `wouldSkip`.

**Skip reasons.** `off`: no greeting configured, the tenant turned it off, or the caller chose no greeting. `busy`: a run is going; the caller is about to hear its answer. `recent`: this conversation was greeted within `cooldownMs`. `empty`: nothing resolved and there is no plain alternative.

**Writing.** The finished text goes in with `history.say` as `source: "greeting"`, so clients get `text.delta` and `message.end` on the session's stream like any answer. A greeting the caller interrupted is still the whole text in the transcript. The voice worker tells the model what was heard on the next turn, as it does for any interrupted reply.

**Cost.** One model call of about 300 instruction tokens plus variables, at most 80 output tokens, counted in the session's usage and sent through the gateway with the usual identity headers.

## `variables.catalog`

Lists every variable this agent and tenant offer, for Workbench's editor: `{ "variables": [{ "name", "description", "trust", "source", "example"? }] }`. `client.*` is listed as one entry. Needs no session.

## The caller's choice

auth-server's preferences gain `greeting`: `""` (personal, the default), `simple` or `off`. agent-ti reads them with its service token (`GET /v1/users/{id}/preferences`).

- `simple`: the model isn't used, and only alternatives with no personal variables are said (`agent`, `time` and `call` only).
- `off`: no greeting.

## Tenants

A tenant's agent config (t10-agent-control `agent_config`) gains:

```json
"greeting": { "instructions": "…", "text": "…", "off": false },
"variables": { "deals.open": { "tool": "crm:deal_list", "args": {…}, "pick": "count" } }
```

Each field the tenant sets replaces the agent's; `off: true` turns the greeting off for the tenant. Tenant variables are tool variables only, at most 10, and can't reuse a built-in or the agent's names. Control checks limits on publish: instructions up to 4 KiB, each text up to 300 characters, at most 10 alternatives per language. A draft applies in the publisher's own test conversation, so Workbench's **Preview as me** runs `session.greet` with `dryRun` there.

## Workbench

- The agent's **Customize** page gets a **Call greeting** section: instructions, fixed text, the tenant's variables, a variable list from `variables.catalog`, and **Preview as me**.
- **Settings** get the greeting preference.
- A call sends `client.locale`, `client.timeZone` and `client.screen` (the page it was started from) in its admission request. auth-server copies them into the room token as `t10.client.*` attributes, which the client can't change after admission.

## Observability

- `agent_ti_greeting_total{agent,outcome}`, where outcome is `model`, `text`, `fallback`, `failed` or `skipped_<reason>`.
- `agent_ti_greeting_seconds{agent,by}`: time to the first word.
- `agent_ti_variable_seconds{agent,source}` and `agent_ti_variable_total{agent,source,outcome}`, where outcome is `ok`, `empty`, `late` or `failed`.

## Later

- **Variables in other instructions.** Profile, tenant and skill instructions could use them, filled once per session (or per call), never per turn: a system prompt that changes every turn loses prompt caching and steadiness. A per-turn value belongs in `before_model_request`.
- **agent-pi.** The same method and config, so agent-ti keeps following agent-pi's protocol. Until then the voice worker falls back to its local text when a harness answers `method not implemented`.
- **Greetings in chat.** `channel: "chat"` is reserved for a new conversation's first screen.
