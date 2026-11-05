-- One file per agent; store_meta records which agent and the replica's instance ID.
CREATE TABLE store_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE sessions (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 owner_user_id TEXT NOT NULL, config_version TEXT NOT NULL, profile TEXT NOT NULL,
 name TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'open',
 origin TEXT NOT NULL CHECK(json_valid(origin)),
 metadata TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata)),
 version INTEGER NOT NULL DEFAULT 1,
 next_event INTEGER NOT NULL DEFAULT 0, next_message INTEGER NOT NULL DEFAULT 0,
 created_at INTEGER NOT NULL, last_active_at INTEGER NOT NULL,
 PRIMARY KEY (tenant_id, agent_id, session_id), UNIQUE(session_id)
);
CREATE INDEX sessions_owner ON sessions(tenant_id,agent_id,owner_user_id,session_id);
CREATE TABLE create_keys (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, owner_user_id TEXT NOT NULL,
 request_key TEXT NOT NULL, fingerprint TEXT NOT NULL, session_id TEXT NOT NULL,
 PRIMARY KEY(tenant_id,agent_id,owner_user_id,request_key),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
-- reply/source: a plugin's answer, placed without the model. stalls: restarts in a row without a new step.
CREATE TABLE commands (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 command_id TEXT NOT NULL, turn_id TEXT NOT NULL, request_key TEXT NOT NULL,
 fingerprint TEXT NOT NULL, input TEXT NOT NULL, traceparent TEXT,
 reply TEXT, source TEXT,
 status TEXT NOT NULL DEFAULT 'queued', cancel_requested INTEGER NOT NULL DEFAULT 0,
 attempt INTEGER NOT NULL DEFAULT 0, stalls INTEGER NOT NULL DEFAULT 0, error TEXT,
 ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
 UNIQUE(tenant_id,agent_id,session_id,command_id), UNIQUE(tenant_id,agent_id,session_id,request_key),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
CREATE INDEX commands_pending ON commands(status,ordinal);
CREATE INDEX commands_session ON commands(tenant_id,agent_id,session_id,status);
CREATE TABLE messages (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 sequence INTEGER NOT NULL, command_id TEXT NOT NULL, position INTEGER NOT NULL,
 payload TEXT NOT NULL CHECK(json_valid(payload)),
 PRIMARY KEY(tenant_id,agent_id,session_id,sequence),
 UNIQUE(tenant_id,agent_id,session_id,command_id,position),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
CREATE TABLE events (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL,
 sequence INTEGER NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
 PRIMARY KEY(tenant_id,agent_id,session_id,sequence),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
CREATE TABLE cancel_keys (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL, request_key TEXT NOT NULL,
 PRIMARY KEY(tenant_id,agent_id,session_id,request_key),
 FOREIGN KEY(tenant_id,agent_id,session_id) REFERENCES sessions(tenant_id,agent_id,session_id)
);
CREATE TABLE steps (
 tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL, command_id TEXT NOT NULL,
 operation_id TEXT NOT NULL, kind TEXT NOT NULL, input_digest TEXT NOT NULL,
 unsafe_effect INTEGER NOT NULL, result TEXT CHECK(result IS NULL OR json_valid(result)),
 PRIMARY KEY(tenant_id,agent_id,session_id,command_id,operation_id),
 FOREIGN KEY(tenant_id,agent_id,session_id,command_id) REFERENCES commands(tenant_id,agent_id,session_id,command_id)
);
