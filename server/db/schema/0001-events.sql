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