CREATE TABLE plugin_state (
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  plugin TEXT NOT NULL,
  state TEXT NOT NULL CHECK(json_valid(state)),
  PRIMARY KEY (tenant_id, agent_id, session_id, plugin),
  FOREIGN KEY (tenant_id, agent_id, session_id) REFERENCES sessions(tenant_id, agent_id, session_id)
);
