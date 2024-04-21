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
        expires_at INTEGER NOT NULL DEFAULT 4778065560, -- magic number for representing 2121-05-30T12:26:00-04:00
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS events_subject ON events (subject);

CREATE INDEX IF NOT EXISTS events_created_at ON events (created_at);

CREATE INDEX IF NOT EXISTS events_expires_at ON events (expires_at);

--
-- QUEUES
--
CREATE TABLE
    IF NOT EXISTS queues (
        name TEXT NOT NULL,
        last_event_id TEXT,
        FOREIGN KEY (last_event_id) REFERENCES events (id) ON DELETE SET NULL,
        PRIMARY KEY (name)
    );

CREATE INDEX IF NOT EXISTS queues_last_event_id ON queues (last_event_id);

--
-- CONSUMERS
--
CREATE TABLE
    IF NOT EXISTS consumers (
        id TEXT NOT NULL,
        pattern TEXT NOT NULL,
        queue_name TEXT,
        durable INTEGER NOT NULL DEFAULT 0,
        batch_size INTEGER NOT NULL DEFAULT 1,
        acked_counts INTEGER NOT NULL DEFAULT 0,
        last_event_id TEXT,
        updated_at INTEGER NOT NULL,
        expires_in INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (queue_name) REFERENCES queues (name) ON DELETE CASCADE,
        FOREIGN KEY (last_event_id) REFERENCES events (id) ON DELETE SET NULL,
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS consumers_pattern ON consumers (pattern);

CREATE INDEX IF NOT EXISTS consumers_queue_name ON consumers (queue_name);

CREATE INDEX IF NOT EXISTS consumers_last_event_id ON consumers (last_event_id);

--
-- CONSUMERS_EVENTS
--
CREATE TABLE
    IF NOT EXISTS consumers_events (
        consumer_id TEXT,
        event_id TEXT NOT NULL,
        acked INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (event_id) REFERENCES events (id) ON DELETE CASCADE,
        FOREIGN KEY (consumer_id) REFERENCES consumers (id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS consumers_events_consumer_id ON consumers_events (consumer_id);

CREATE INDEX IF NOT EXISTS consumers_events_event_id ON consumers_events (event_id);