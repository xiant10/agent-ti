# Storage and recovery

Status: revised 2026-11-05. SQLite only. This is the part of agent-ti that replaces pi-durable, so it has to give the guarantees agent-pi gets from it: accepted input is never lost, a restarted process continues unfinished runs, a recorded model or tool result is reused rather than repeated, and event streams resume. What the code has today is in the [plan](implementation-plan.md#current-code-against-the-design).

## Store

Each agent has its own SQLite file, holding every tenant's sessions of that agent, even when one process serves several agents. One process owns each file, as in agent-pi:

- An exclusive OS lock on `<file>.owner.lock`, released by the OS if the process dies. A second process fails at startup.
- The file records its agent and refuses to open as another.
- Files live in the host file's `storage.dir`, as `<agent>.sqlite`. Adding an agent to a host adds a file; moving an agent to another host moves its file.
- WAL, `synchronous=FULL`, foreign keys, numbered migrations under `crates/agent-ti-store/migrations/sqlite`.
- Short `BEGIN IMMEDIATE` write transactions; no transaction spans a model call, tool call or client stream.
- Each file has one writer. Writes are admitted by count and bytes, so a burst returns 429 instead of growing memory. A storage error stops admission; work isn't continued from memory.
- A small read pool next to the single writer, so SSE catch-up and listings don't queue behind writes.

The runtime calls domain operations (`create`, `admit`, `claim`, `prepare`, `record`, `finish`, `cancel`, `events`, …), not SQL. Every operation takes the tenant, agent and user scope; another user's session returns not found.

## Tables

All keyed by `(tenant_id, agent_id, session_id, …)`, with composite foreign keys so a row can't point into another tenant.

| Table | Holds |
|---|---|
| `sessions` | Owner, profile, origin, name, metadata, status, timestamps, next message/event sequence, grant ID, config identity |
| `create_keys` | Idempotent `session.create`: request key, fingerprint, session |
| `commands` | Each accepted input: request key and fingerprint, turn ID, input, status (`queued`, `running`, `completed`, `failed`, `cancelled`, `parked`), cancel flag, attempt, admission order |
| `steps` | Each model call, tool call or hook decision of a command: operation ID, kind, input digest, whether it is an unsafe effect, recorded result |
| `messages` | The transcript: ordered model-visible messages per command |
| `events` | Client events in order, the source for SSE replay and snapshots |
| `cancel_keys` | Idempotent cancellation |

The matching features add: the session inbox (steers, follow-ups and plugin history writes waiting for a boundary), active tools and loaded skills per session, plugin state per session and plugin, compaction summaries, and child runs for sub-agents and background tasks.

Each file also has `store_meta`, which records its agent and, when the host file sets none, the replica's instance ID. `commands` counts restarts in a row without a new step (`stalls`), which is how a crash loop is told apart from progress.

## Admission

1. Authenticate, resolve the session, check size, run `before_prompt`.
2. In one transaction: check the owner and status, deduplicate by request key and fingerprint (same key with a different payload is a conflict), check queue limits, insert the command, user message and `submission` event.
3. Reply "accepted" only after that commit. Wake the scheduler in-process; a periodic scan catches anything missed.

## Runs and steps

The scheduler is in memory and spans every agent's store. It claims the oldest runnable command of the least recently served tenant, one at a time per session, within the process and tenant run limits. Each store keeps its own queue; the round-robin position isn't stored, so fairness starts afresh after a restart. The worker then runs the model/tool loop, and each external call is a step:

1. Commit `prepared` with the call's input and digest.
2. Make the call outside the transaction.
3. Commit the result with its messages and events.

On restart, `running` commands become runnable again with `attempt + 1`, and the worker replays: a step with a recorded result returns it without calling again, and a changed input digest for the same step is an error.

| Interrupted step | After restart |
|---|---|
| Hook decision | Reused if recorded, otherwise run again (hooks must be replay-safe, see [plugins.md](plugins.md)) |
| Model call | Called again; a partial reply already sent is marked aborted and replaced |
| Tool with `replay: "safe"` | Called again |
| Unsafe tool (every MCP tool, and plugin tools by default) | Not called again. Recorded as an `interrupted` tool result and returned to the model, which carries on, as in agent-pi |

A command restarted `durable.restartLimit` times in a row (default 5) without recording a new step is **parked**: its session stays busy and `session.get` shows it, `session.retry` runs it again, and `session.cancel` or `session.interrupt` gives up on it.

This isn't exactly-once execution of external effects; no crash-recovery scheme gives that. Passing the step ID to an MCP server as an idempotency key, where the server supports it, is the way to narrow the gap.

A tool error that the call itself returned (an MCP error, invalid arguments, a denied call, a timeout without a crash) is not a step failure. It's recorded as the tool's result with `isError` and goes back to the model, as in agent-pi.

## Cancellation and interrupt

`session.cancel` and `session.interrupt` set the durable cancel flag and the cancel key in one transaction, then signal the running worker through an in-process cancellation token. The flag matters only after a restart. An interrupt's replacement input is admitted in the same transaction, so a retried interrupt can't cancel its own replacement.

## Events and streaming

Events are written with the state change that causes them and published only after commit. `GET /agent` with `Last-Event-ID` reads events after that sequence; without one, or with an unknown one, the stream starts with a `snapshot` of current state. Snapshot and replay read the same high-water mark, so nothing falls between them.

Text and thinking deltas are the exception. They are published from memory, batched at most every 100 ms or 16 KiB, and never written; the committed `message.end` carries the whole text. They take IDs from the session's event sequence: `<session>:<seq>.<n>` is the `n`th delta after committed event `seq`, so committed events and deltas share one order without deltas being written. Committed events reach the in-memory hub only after their transaction commits, and the hub publishes both kinds in that order. A subscriber catches up from SQLite up to the hub's high-water mark, merged with the deltas still in the ring, then follows the hub.

Deltas are kept in a per-session memory ring (1,024 deltas or 1 MiB); a session nobody follows is forgotten after a minute without events, as agent-pi's hub is. A cursor inside the ring resumes from it; a cursor that names a delta no longer there, or any delta cursor after a restart, gets a `snapshot` whose `partial` is the reply in progress. A committed cursor resumes from SQLite even when older deltas were dropped, since each finished message's `message.end` carries its whole text; only a dropped delta of the reply still in progress forces a snapshot. Each model call is its own message (`<command>:<n>`), ended by a committed `message.end`, so text before a tool call ends cleanly and a cancelled reply ends with the text already sent. Slow subscribers are disconnected and resume from their cursor rather than blocking the worker. Old committed events are pruned after a retention period.

## Retention and backups

A retention job for closed sessions and old events, and backups with SQLite's online backup API (copying the file alone while WAL is active is not a backup). Restore is tested with pending commands.

## Later

A shared PostgreSQL store with session leases, so any replica can run any session, stays an option (see [design.md](design.md#later-not-now)). It would need the same operations with fencing on every commit, a gateway that stops routing by replica, and the same tests on both stores.
