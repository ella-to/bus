--
-- EVENTS 
--
CREATE TABLE
    IF NOT EXISTS events (
        id TEXT NOT NULL,
        subject TEXT NOT NULL,
        reply TEXT,
        reply_count INTEGER NOT NULL DEFAULT 0,
        size INTEGER NOT NULL,
        data BLOB,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS events_subject ON events (subject);

CREATE INDEX IF NOT EXISTS events_created_at ON events (created_at);

CREATE INDEX IF NOT EXISTS events_expires_at ON events (expires_at);

--
-- CONSUMERS
--
CREATE TABLE
    IF NOT EXISTS consumers (
        id TEXT NOT NULL,
        subject TEXT NOT NULL,
        type INTEGER NOT NULL,
        ack_strategy INTEGER NOT NULL,
        batch_size INTEGER NOT NULL DEFAULT 1,
        queue_name TEXT, -- <- queue name, can be null
        acked_count INTEGER NOT NULL DEFAULT 0,
        last_event_id TEXT, -- <- last event id consumed, can be null
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS consumers_subject ON consumers (subject);

CREATE INDEX IF NOT EXISTS consumers_queue_name ON consumers (queue_name);

CREATE INDEX IF NOT EXISTS consumers_last_event_id ON consumers (last_event_id);