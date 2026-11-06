-- agent-pi's `app.tasks`: a session's background tasks, and the messages between parent and
-- child, each named by the request key that dedupes its submission.
CREATE TABLE tasks (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 task_id TEXT NOT NULL, call_key TEXT NOT NULL, name TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('working','idle','stopped','failed','expired')),
 child TEXT, pending INTEGER NOT NULL DEFAULT 0, run_at INTEGER NOT NULL, timed_out INTEGER NOT NULL DEFAULT 0,
 notes INTEGER NOT NULL DEFAULT 0, heard_at INTEGER, last_report TEXT,
 started_at INTEGER NOT NULL, active_at INTEGER NOT NULL,
 PRIMARY KEY(tenant_id,agent_id,session_id,task_id),
 UNIQUE(tenant_id,agent_id,session_id,call_key),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
-- kind: input (to the child) or report (to the parent). level: wake or note, for a report.
CREATE TABLE task_messages (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL, task_id TEXT NOT NULL,
 request_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('input','report')),
 content TEXT NOT NULL, urgent INTEGER NOT NULL DEFAULT 0, level TEXT NOT NULL DEFAULT 'wake',
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','submitted','done')),
 created_at INTEGER NOT NULL,
 PRIMARY KEY(tenant_id,agent_id,session_id,request_key),
 FOREIGN KEY(tenant_id,agent_id,session_id,task_id) REFERENCES tasks(tenant_id,agent_id,session_id,task_id)
);
CREATE INDEX task_messages_open ON task_messages(status) WHERE status<>'done';
-- Runs tasks started since a user last spoke (`maxUnattendedRuns`).
ALTER TABLE sessions ADD COLUMN unattended INTEGER NOT NULL DEFAULT 0;
