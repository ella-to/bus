CREATE TABLE
    IF NOT EXISTS consumers (
        id TEXT NOT NULL,
        subject TEXT NOT NULL,
        count_messages INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS consumers_subject ON consumers (subject);

CREATE TABLE
    IF NOT EXISTS queues (
        name TEXT NOT NULL,
        last_event_id TEXT,
        FOREIGN KEY (last_event_id) REFERENCES events (id) ON DELETE CASCADE,
        PRIMARY KEY (name)
    );

CREATE INDEX IF NOT EXISTS queues_last_event_id ON queues (last_event_id);

--
-- NOTE: each consumer can be subscribed to only one queue at a time
-- and each queue can have multiple consumers
--
CREATE TABLE
    IF NOT EXISTS queues_consumers (
        queue_name TEXT NOT NULL,
        consumer_id TEXT NOT NULL,
        FOREIGN KEY (queue_name) REFERENCES queues (name) ON DELETE CASCADE,
        FOREIGN KEY (consumer_id) REFERENCES consumers (id) ON DELETE CASCADE,
        PRIMARY KEY (consumer_id)
    );

CREATE INDEX IF NOT EXISTS queues_consumers_queue_name ON queues_consumers (queue_name);

--
-- this table is getting populated by triggered and each record will be updated 
-- once it's been read by set acked to 1
--
CREATE TABLE
    IF NOT EXISTS consumers_events (
        consumer_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        acked INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (consumer_id) REFERENCES consumers (id) ON DELETE CASCADE,
        FOREIGN KEY (event_id) REFERENCES events (id) ON DELETE CASCADE,
        PRIMARY KEY (consumer_id, event_id)
    );