# Acknowledgements and progress

Status: built 2026-11-12 (`src/progress.rs`, the hub in `src/events.rs`, config in `crates/agent-ti-config`). The voice worker's part is not built yet. Where the build differs from this design, it says so below.

## The problem, from the user's side

On t10 a Ti turn spends about 3–5 s before anything specific appears: admission, history, then the first model call, whose first token takes most of that. Until then, Workbench shows its own "Understanding your request…" and a timer, which say the request arrived but not that it was understood. Then the first tool's name appears, then "Analyzing results…", then the answer.

Three moments hurt:

1. **The first few seconds.** The user can't tell whether the agent got the request right until the answer arrives. If it didn't, they find out late.
2. **Voice calls.** Silence of 3–5 s on a call feels broken. The voice worker reads only `text.delta`, so the caller hears nothing until the answer.
3. **Long quiet stretches.** A turn with several tool rounds, or a model retry, can go 10 s or more with nothing new to show.

A good acknowledgement says, in the user's own words and language, what the agent is about to do: "I'll check your open work items." It arrives in about a second and lets the user correct a misunderstanding early. Generic text ("Working on it…") adds nothing that the client's own indicator doesn't already show.

## Decisions

### What is said

- **One sentence in the user's language** saying what the agent will look at or do. It must not answer, claim results, promise an outcome, or commit to an action that might need the user's confirmation. That means "I'll look up your tasks" and never "I'll delete the task".
- **Nothing for requests that need no work.** Greetings, thanks and small talk get an empty reply from the acknowledgement call, and nothing is shown. Noise is worse than silence when the answer is about to arrive anyway.
- **No canned text from the server.** The client's local indicator is faster and is already localized. The server sends phases (`working`, `waiting`, `finished`); text comes only from the model. This differs from agent-pi's generic mode, whose English sentences show unchanged to Chinese-speaking users.
- **The model sees a little context.** It gets the user's input after `before_prompt` plugins have rewritten it (so guardrail redaction applies), up to 2,000 characters, plus the last assistant reply, up to 600 characters. Without the previous reply, "Yes, go ahead" or "and the other one?" produce empty or wrong acknowledgements. It does not see tools, instructions or history beyond that: a short prompt keeps it fast.

### When it is said

- **For a person who is watching, at the start of their run.** The call starts when the scheduler claims the command, alongside the run, never ahead of it. The run never waits for it.
- **Skipped when it can't help:**
  - a plugin's direct reply (the answer is already there);
  - a run resumed after a restart (`attempt > 0`);
  - a cancelled command;
  - a child session's run, or a background task waking its parent (nobody asked anything);
  - a session nobody is watching: no live stream on it, the prompt didn't ask for one, and it didn't say `progress: true`. Workbench says so on a new conversation's first message, which it sends before opening the conversation's stream. Automations and other JSON-only callers pay nothing.
- **A queued message is acknowledged when it starts running**, not when it is queued. Until then Workbench shows "Queued". Describing queued work as underway would be wrong.
- **A message steered into a running turn gets its own acknowledgement**, which replaces the current one: "Got it, I'll include last week's items too." Steering is exactly when users wonder whether the agent heard them, and today nothing confirms it until the model's next round. This is generated at steer admission, under the same rules.

### When it is dropped

- **Once answer text has started**, a late acknowledgement is discarded and its request aborted. The answer is better feedback than a sentence about it.
- **After a deadline** (`timeoutMs`, 5 s): by then tool status is usually showing.
- **On any error.** It is never retried and never fails the turn.

A tool starting before the acknowledgement arrives does not drop it: "I'll check your work items" still reads well next to "Listing work items…".

### Long quiet stretches

- After `waitingAfterMs` (chat 15 s, voice 7 s) with no visible activity, meaning no text, no tool start or finish and no other progress, the server sends `phase: "waiting"` once. Activity resets the clock.
- It carries the current acknowledgement as its text, so a client keying status by submission keeps showing it.
- The client decides what to do with it. Workbench can add a quiet "Still working…" beside the timer. A voice client says a short localized filler once.
- Model retries already send `retry` events (attempt, time, error), so no separate `retrying` phase is needed.

### How it reaches clients

The events are the same as agent-pi's, so Workbench and other clients work unchanged:

```json
{"type":"progress","submissionIds":[302],"phase":"working","text":"I'll check your open work items."}
{"type":"progress","submissionIds":[302],"phase":"waiting","text":"I'll check your open work items."}
{"type":"progress","submissionIds":[302],"phase":"finished","text":""}
```

- They are hub deltas, published from memory like text deltas and never committed. A restart loses them, which is fine: the client falls back to its indicator.
- `submissionIds` are the command ordinals the `submission` events already use. A steered input joins the run's IDs.
- The hub keeps the session's latest progress and clears it on that run's `run.finished`; a steered message's own `run.finished`, which names its command, leaves it. (Workbench clears its status on any `run.finished`, so it can lose the line when a steered message is taken up mid-turn; the next acknowledgement brings it back.)
- The hub also tracks when something new last reached the user (text, a tool starting or finishing, a retry), which is how "late" and "quiet" are decided without hooks in the run loop. A stream that starts fresh, or resumes past the ring, gets it as `progress` in the snapshot, as agent-pi's `snapshot.progress` does, so reconnecting mid-turn still shows it.
- The acknowledgement never enters model history or the transcript. The answer replaces it.

### Voice

- The voice worker should speak the first `working` text once, only if no answer audio has started, and cut it off when the answer begins. That change belongs in `t10-livekit-agent` (`internal/provider/agentpi/stream.go`) and is the largest win this feature has. It is a follow-up, not part of this change.
- Ti's voice instructions gain one line: no preamble such as "Let me check…". The acknowledgement already said it, and saying it twice sounds robotic.
- Acknowledgements are not cut into `speech` segments. They are status, not part of the reply.

### Cost and accounting

- The call is small: about 200 instruction tokens plus the input, and at most 60 output tokens. Thinking is off (`enable_thinking: false` for Qwen). As built, no temperature is sent: the adapter has no setting for it, and the provider's default has been steady enough.
- **Its tokens count in the session's usage** (`input_tokens`, `output_tokens`), so the admin's "Total ✧" stays honest. agent-pi leaves them out; we don't. The user is present, so `maxUnattendedTokens` is unaffected.
- It goes through the same gateway with the same identity headers, so the gateway's per-tenant audit sees it.

## Configuration

Off unless an agent turns it on. An agent-level block sets the defaults, and each profile can override them or switch them off.

```json
"progress": {
  "enabled": true,
  "model": "chat",
  "timeoutMs": 5000,
  "waitingAfterMs": 15000,
  "steer": true
},
"profiles": {
  "chat": { },
  "voice": { "progress": { "waitingAfterMs": 7000 } },
  "batch": { "progress": false }
}
```

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Generate acknowledgements. A string such as `"${AGENT_PROGRESS:-true}"` is accepted, so operators can switch it off without a new config image. |
| `model` | the profile's model | A name in `models`. A fast model is best; thinking is always off. |
| `timeoutMs` | 5000 | Drop an acknowledgement that hasn't arrived by then. 1,000–15,000. |
| `waitingAfterMs` | 15000 | Quiet time before `waiting`; 0 turns `waiting` off. |
| `steer` | `true` | Acknowledge a message steered into a running turn. |

- **Compatibility:** agent-pi's `"progressModel": "fast"` means `{"enabled": true, "model": "fast"}`. If both are given, `progress` wins, and the startup warning for `progressModel` goes away.
- **Per prompt:** `session.prompt` accepts `"progress": false` for a caller that streams but doesn't want acknowledgements, such as a test harness.
- **Ti** turns it on for chat and voice, with its `chat` model (`qwen3.5-flash`).
- **Per tenant (later):** a tenant switch would belong in tenant config from t10-agent-control, next to the customization it already sends. It isn't needed to ship.

## Where it goes in the code

- `crates/agent-ti-config`: parse `progress` at the agent and profile levels, and `progressModel` as shorthand.
- `src/progress.rs` (new):
  - the prompt and the request (non-streaming, with `max_tokens`, temperature and the thinking switch);
  - cleaning the output to one line of at most 200 characters, with no Markdown or quotes, and empty when the model says nothing;
  - the per-run controller: start, abort, quiet timer and finish.
- `src/worker.rs`:
  - at claim, decide whether to acknowledge and spawn the controller;
  - `LiveText` tells it when answer text starts (drop and abort);
  - tool events reset the quiet timer;
  - the run's end stops it and adds its tokens to the completion's usage.
- `src/events.rs`: keep the latest progress per session, clear it on `run.finished`, and expose it to snapshots.
- `src/server.rs`:
  - put `progress` in the snapshot;
  - mark a prompt that streams as watched;
  - read `progress: false`;
  - acknowledge a steered input.
- `t10-agents/agents/ti`: the `progress` block, and the voice line about preambles.

## Tests

- `tests/config.rs`: the block, the shorthand, profile overrides, the environment switch, and bad values.
- `scripts/smoke.mjs` with the `ti-ack` agent and the fixture's acknowledgement model:
  - shown before the answer, under its submission, and cleared before `run.finished`;
  - not in the transcript, and its tokens in the session's usage (15 + 10);
  - the next turn sends the previous reply;
  - nothing for a greeting, a late reply or a failure;
  - no request at all when nobody watches, for `progress: false`, or for a profile that turns it off;
  - `waiting` once after a quiet second, with the acknowledgement's text, and a fresh stream's snapshot carrying it until the run ends;
  - a steered message acknowledged under both submissions;
  - each outcome in `/metrics`.

## How we'll know it helps

New metrics:

- `agent_ti_progress_total{outcome}`, where `outcome` is `shown`, `empty`, `late`, `timeout`, `failed` or `skipped`;
- `agent_ti_progress_seconds`: from claim to the acknowledgement being shown;
- `agent_ti_progress_lead_seconds`: from the acknowledgement being shown to the first answer text or tool. This is the time the user had something specific instead of a spinner.

If the lead is usually under half a second, or most outcomes are `late`, the feature isn't paying for itself on that model, and it should be off for that agent.

On t10, compare Ti with and without it. Use `scripts/perf-compare.mjs` for first-feedback time, and check the answer's first-token time doesn't get worse from the extra request.

## Not doing

- **Acknowledgements in history or the transcript.** They would cost tokens every turn and tie the model to a sentence it didn't write.
- **Waiting for the acknowledgement before the main call**, so the model could avoid repeating it. That adds latency to every turn to save a few words. The voice instruction line handles repetition where it matters.
- **Tool-derived narration** ("Found 3 items, now checking comments"). It needs a model call per step, and Workbench already names each tool as it runs.
