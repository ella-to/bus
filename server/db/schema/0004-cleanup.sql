-- In every startup, we need to clean up the database from the previous run.
--
--
-- Clean up consumers that belongs to a queue
DELETE FROM consumers
WHERE
    id IN (
        SELECT
            consumer_id
        FROM
            queues_consumers
    )
    OR subject LIKE 'inbox.%'
    OR subject LIKE 'confirm.%';

-- Clean up consumers_events that has a consumer that belongs to a queue
DELETE FROM consumers_events
WHERE
    consumer_id IN (
        SELECT
            consumer_id
        FROM
            queues_consumers
    );

-- Clean up all the events that are not needed anymore
DELETE FROM events
WHERE
    subject LIKE 'inbox.%'
    OR subject LIKE 'confirm.%'
    OR reply != '' -- means it's either request or confirm events
    OR reply_count > 0;