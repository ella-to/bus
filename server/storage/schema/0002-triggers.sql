--
-- INSERT relevant events based on pattern of consumer
-- and last_event_id and LIMIT it based on batch size
-- if and only if batch_size > count of not events that not acked
--
-- NOTE: This trigger only runs for non-queue consumers
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
-- NOTE: This Trigger only runs for queue consumers
--
CREATE TRIGGER IF NOT EXISTS trigger_queue_consumers_events_insert
--
AFTER INSERT ON consumers
--
FOR EACH ROW WHEN NEW.queue_name IS NOT NULL
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
    AND consumer_id IN (
        SELECT
            id
        FROM
            (
                SELECT
                    id,
                    MIN(acked_counts)
                FROM
                    consumers
                WHERE
                    queue_name = NEW.queue_name
                LIMIT
                    1
            )
    )
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
-- NOTE: This trigger only runs for non-queue consumers
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
-- NOTE: This trigger only runs for queue consumers
--
CREATE TRIGGER IF NOT EXISTS trigger_queue_events_inserted
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
    consumers.queue_name IS NOT NULL
    AND COALESCE(consumers.last_event_id, '') < NEW.id
    AND NEW.subject LIKE consumers.pattern
    AND consumers.id IN (
        SELECT
            id
        FROM
            (
                SELECT
                    id,
                    MIN(acked_counts)
                FROM
                    consumers
                WHERE
                    queue_name = consumers.queue_name
                LIMIT
                    1
            )
    )
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

CREATE TRIGGER IF NOT EXISTS trigger_consumers_events_cleanup AFTER
UPDATE OF deleted ON consumers_events WHEN NEW.deleted = 1 BEGIN
DELETE FROM consumers_events
WHERE
    consumer_id = NEW.consumer_id
    AND event_id <= NEW.event_id
    AND deleted = 1;

END;

--
-- NOTE: This Trigger only runs for non-queue consumers
--
CREATE TRIGGER IF NOT EXISTS trigger_consumers_events_acked AFTER
UPDATE OF acked ON consumers_events FOR EACH ROW WHEN NEW.acked = 1
AND NEW.deleted = 0 BEGIN
--
--
--
UPDATE consumers
SET
    last_event_id = NEW.event_id,
    acked_counts = acked_counts + 1
WHERE
    id = NEW.consumer_id;

--
--
--
UPDATE consumers_events
SET
    deleted = 1
WHERE
    consumer_id = NEW.consumer_id
    AND event_id = NEW.event_id;

--
--
--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    NEW.consumer_id as consumer_id,
    events.id as event_id
FROM
    events
WHERE
    events.id > (
        SELECT
            event_id
        FROM
            consumers_events
        WHERE
            consumer_id = NEW.consumer_id
        ORDER BY
            event_id DESC
        LIMIT
            1
    )
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
            AND deleted = 0
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
    ) - (
        SELECT
            COUNT(*)
        FROM
            consumers_events
        WHERE
            consumer_id = NEW.consumer_id
            AND deleted = 0
    );

END;

--
-- NOTE: This Trigger only runs for queue consumers
--
CREATE TRIGGER IF NOT EXISTS trigger_queue_consumers_events_acked
--
AFTER
UPDATE OF acked ON consumers_events WHEN NEW.acked = 1
AND NEW.event_id > (
    -- Get the last event_id of the consumer that has been acked
    -- since the update can be executed in batch
    SELECT
        COALESCE(last_event_id, '') AS last_event_id
    FROM
        consumers
    WHERE
        id = NEW.consumer_id
)
-- We want to make sure that the trigger only runs for queue consumers
AND EXISTS (
    SELECT
        1
    FROM
        consumers
    WHERE
        id = NEW.consumer_id
        AND queue_name IS NOT NULL
)
--
BEGIN
--
UPDATE consumers
SET
    last_event_id = NEW.event_id,
    acked_counts = acked_counts + 1
WHERE
    id = NEW.consumer_id;

--
-- Need to update the last_event_id of the queue
-- So the next consumer can initalize their last_event_id
UPDATE queues
SET
    last_event_id = NEW.event_id
WHERE
    name = (
        SELECT
            queue_name
        FROM
            consumers
        WHERE
            id = NEW.consumer_id
    );

-- Because consumers_events is getting bigger and bigger
-- we need to delete the old events that has been acked
-- and keep the latest one, 
-- However, calling delete will cause the trigger to be called again
-- So at the beginning of the trigger, we need to check if the new event
-- is bigger than the last_event_id of the consumer, if not, it means that tigger
-- should not be called
DELETE FROM consumers_events
WHERE
    consumer_id = NEW.consumer_id
    AND acked = 1
    AND rowid NOT IN (
        SELECT
            MAX(rowid)
        FROM
            consumers_events
        WHERE
            consumer_id = NEW.consumer_id
            AND acked = 1
    );

--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    (
        SELECT
            id
        FROM
            (
                SELECT
                    id,
                    MIN(acked_counts)
                FROM
                    consumers
                WHERE
                    queue_name = (
                        SELECT
                            queue_name
                        FROM
                            consumers
                        WHERE
                            id = NEW.consumer_id
                    )
                LIMIT
                    1
            )
    ) as consumer_id,
    (
        SELECT
            id
        FROM
            events
        WHERE
            subject LIKE (
                SELECT
                    pattern
                FROM
                    consumers
                WHERE
                    id = NEW.consumer_id
            )
            AND id > NEW.event_id
        ORDER BY
            id
        LIMIT
            1
    ) as event_id
WHERE
    consumer_id IS NOT NULL
    AND event_id IS NOT NULL;

END;