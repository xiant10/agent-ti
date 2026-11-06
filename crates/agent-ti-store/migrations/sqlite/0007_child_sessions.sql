-- A child session runs a sub-agent or `consult_expert` for a parent's tool call (agent-pi's
-- delegation). `child` is its spec: {"kind":"expert"|"subagent","name":...}.
ALTER TABLE sessions ADD COLUMN parent TEXT;
ALTER TABLE sessions ADD COLUMN child TEXT;
CREATE INDEX sessions_by_parent ON sessions(tenant_id,agent_id,parent,created_at) WHERE parent IS NOT NULL;
