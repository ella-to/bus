--
--
--
--
--
CREATE TRIGGER IF NOT EXISTS initial_consumer_insert_trigger
--
AFTER INSERT ON consumers
--
FOR EACH ROW
--
BEGIN
--
INSERT INTO
    consumers_events (consumer_id, event_id)
SELECT
    NEW.id AS consumer_id,
    events.id AS event_id
FROM
    events
    INNER JOIN consumers ON events.subject LIKE consumers.subject
    LEFT JOIN queues_consumers ON queues_consumers.consumer_id = consumers.id
    LEFT JOIN queues ON queues.name = queues_consumers.queue_name
WHERE
    queues.name IS NULL
    AND consumers.id = NEW.id
    AND events.id > COALESCE(NEW.last_event_id, '');

END;

--
--
--
--
--
CREATE TRIGGER IF NOT EXISTS append_consumers_events_trigger
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
    round_robin_consumers.consumer_id AS consumer_id,
    NEW.id AS event_id
FROM
    (
        SELECT
            consumers.id AS consumer_id,
            MIN(consumers.count_messages) AS min_messages
        FROM
            consumers
            LEFT JOIN queues_consumers ON queues_consumers.consumer_id = consumers.id
            LEFT JOIN queues ON queues.name = queues_consumers.queue_name
        WHERE
            queues.name IS NOT NULL
            AND EXISTS (
                SELECT
                    1
                FROM
                    events
                WHERE
                    events.id = NEW.id
                    AND events.subject LIKE consumers.subject
            )
        GROUP BY
            queues.name
    ) AS round_robin_consumers
UNION
SELECT
    other_consumers.consumer_id AS consumer_id,
    NEW.id AS event_id
FROM
    (
        SELECT
            consumers.id AS consumer_id,
            consumers.count_messages AS min_messages
        FROM
            consumers
            LEFT JOIN queues_consumers ON queues_consumers.consumer_id = consumers.id
            LEFT JOIN queues ON queues.name = queues_consumers.queue_name
        WHERE
            (
                queues.name IS NULL
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        events
                    WHERE
                        events.id = NEW.id
                        AND events.subject LIKE consumers.subject
                )
            )
    ) AS other_consumers;

END;

--
--
--
--
--
CREATE TRIGGER IF NOT EXISTS events_notification_trigger
--
AFTER INSERT ON consumers_events
--
FOR EACH ROW
--
BEGIN
--
-- Increment the count_messages of the consumer, this is impoertant for
-- load balancing the consumers if they are using the same queue
--
UPDATE consumers
SET
    count_messages = count_messages + 1
WHERE
    id = NEW.consumer_id;

--
-- Notify the consumer about the event
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