-- What each command's own model calls and acknowledgement spent, so a transcript can say what a turn cost.
-- Children's spend stays in their own sessions' rows.
ALTER TABLE commands ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE commands ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0;
