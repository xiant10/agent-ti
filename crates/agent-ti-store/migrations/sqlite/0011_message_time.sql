-- When each message was written, in Unix milliseconds, for clients to show. Older messages have none.
ALTER TABLE messages ADD COLUMN created_at INTEGER;
