-- A summary of a session's messages before `head`, the first message it keeps, as pi-durable's
-- `pi.compaction` entry. The newest (highest head) is the one model requests start from.
CREATE TABLE compactions (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 head INTEGER NOT NULL, summary TEXT NOT NULL, reason TEXT NOT NULL, created_at INTEGER NOT NULL,
 PRIMARY KEY(tenant_id,agent_id,session_id,head),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
