-- What a session's own model calls spent, and what its whole tree had spent when a user last spoke,
-- so `limits.maxUnattendedTokens` can count what came after. Children have rows of their own.
ALTER TABLE sessions ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN spent_at_input INTEGER NOT NULL DEFAULT 0;
-- Retention finds closed and idle sessions by when they were last active.
CREATE INDEX sessions_idle ON sessions(status,last_active_at);
