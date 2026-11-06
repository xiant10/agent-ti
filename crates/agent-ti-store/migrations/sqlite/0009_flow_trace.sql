-- agent-pi's `FlowDoc.latest`: the trace of the skill flow a session last ran.
ALTER TABLE sessions ADD COLUMN flow TEXT CHECK(flow IS NULL OR json_valid(flow));
