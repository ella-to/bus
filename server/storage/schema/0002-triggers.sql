--
-- Trigger notify function when a new record is inserted into 
-- consumers_events with acked = 0
--
CREATE TRIGGER IF NOT EXISTS trigger_consumers_events_notify
--
AFTER INSERT ON consumers_events
--
FOR EACH ROW WHEN NEW.acked = 0
--
BEGIN
--
SELECT
    notify (
        id,
        subject,
        reply,
        reply_count,
        size,
        data,
        created_at,
        expires_at,
        NEW.consumer_id
    )
FROM
    events
WHERE
    id = NEW.event_id;

END;

--
-- INSERT relevant events based on pattern of consumer
-- and last_event_id and LIMIT it based on batch size
-- if and only if batch_size > count of not events that not acked
--
CREATE TRIGGER IF NOT EXISTS trigger_consumers_events_insert
--
AFTER INSERT ON consumers
--
FOR EACH ROW WHEN NEW.queue_name IS NULL
--
BEGIN
--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    NEW.id as consumer_id,
    id as event_id
FROM
    events
WHERE
    subject LIKE NEW.pattern
    AND id > COALESCE(NEW.last_event_id, '')
ORDER BY
    id
LIMIT
    NEW.batch_size - (
        SELECT
            COUNT(*)
        FROM
            consumers_events
        WHERE
            consumer_id = NEW.id
            AND acked = 0
    );

END;

--
-- This triggers, insert events into consumers_events table
-- based on the pattern of the consumer and last_event_id
-- and batch_size and acked count
--
CREATE TRIGGER IF NOT EXISTS trigger_events_inserted
--
AFTER INSERT ON events
--
FOR EACH ROW
--
BEGIN
--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    consumers.id as consumer_id,
    NEW.id as event_id
FROM
    consumers
WHERE
    consumers.queue_name IS NULL
    AND COALESCE(consumers.last_event_id, '') < NEW.id
    AND NEW.subject LIKE consumers.pattern
    AND (
        SELECT
            COUNT(*)
        FROM
            consumers_events
        WHERE
            consumer_id = consumers.id
            AND acked = 0
    ) < consumers.batch_size;

END;

--
--
--
CREATE TRIGGER IF NOT EXISTS trigger_consumers_events_acked
--
AFTER
UPDATE OF acked ON consumers_events WHEN NEW.acked = 1
AND NEW.event_id = (
    -- Get the last event_id of the consumer that has been acked
    -- since the update can be executed in batch
    SELECT
        event_id
    FROM
        consumers_events
    WHERE
        consumer_id = NEW.consumer_id
        AND acked = 1
    ORDER BY
        event_id DESC
    LIMIT
        1
)
--
BEGIN
--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    NEW.consumer_id as consumer_id,
    events.id as event_id
FROM
    events
WHERE
    events.id > NEW.event_id
    AND events.subject LIKE (
        SELECT
            pattern
        FROM
            consumers
        WHERE
            id = NEW.consumer_id
    )
    AND (
        SELECT
            COUNT(*)
        FROM
            consumers_events
        WHERE
            consumer_id = NEW.consumer_id
            AND acked = 0
    ) < (
        SELECT
            batch_size
        FROM
            consumers
        WHERE
            id = NEW.consumer_id
    )
LIMIT
    (
        SELECT
            batch_size
        FROM
            consumers
        WHERE
            id = NEW.consumer_id
    );

END;

END;