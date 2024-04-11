CREATE TABLE
    IF NOT EXISTS events (
        id TEXT NOT NULL,
        subject TEXT NOT NULL,
        reply TEXT,
        size INTEGER NOT NULL,
        data BLOB,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS events_subject ON events (subject);

CREATE INDEX IF NOT EXISTS events_created_at ON events (created_at);