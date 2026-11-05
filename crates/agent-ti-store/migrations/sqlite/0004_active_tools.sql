CREATE TABLE session_tools (
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  offered TEXT NOT NULL CHECK(json_valid(offered)),
  dynamic TEXT NOT NULL CHECK(json_valid(dynamic)),
  skills TEXT NOT NULL CHECK(json_valid(skills)),
  PRIMARY KEY (tenant_id, agent_id, session_id),
  FOREIGN KEY (tenant_id, agent_id, session_id) REFERENCES sessions(tenant_id, agent_id, session_id)
);
