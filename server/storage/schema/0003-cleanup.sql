-- In every startup, we need to clean up the database from the previous run.
-- Clean up consumers that belongs to a queue
DELETE FROM consumers
WHERE
    queue_name IS NOT NULL
    OR pattern LIKE 'inbox.%';

-- Clean up all the events that are not needed anymore
DELETE FROM events
WHERE
    subject LIKE 'inbox.%'
    OR reply != '' -- means it's either request or confirm events
    OR reply_count > 0;