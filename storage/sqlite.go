package storage

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"ella.to/bus"
	"ella.to/sqlite"
)

//go:embed schema/*.sql
var migrationFiles embed.FS

type Sqlite struct {
	db  *sqlite.Database
	wdb *sqlite.Worker
}

func (s *Sqlite) SaveEvents(ctx context.Context, events ...*bus.Event) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		defer conn.Save(&err)()

		const numFields = 9

		var stmt *sqlite.Stmt
		var sb strings.Builder

		sb.WriteString(`
			INSERT INTO events 
			(id, subject, type, reply, reply_count, size, data, created_at, expires_at) 
			VALUES `)

		sqlite.GroupPlaceholdersStringBuilder(len(events), numFields, &sb)

		sb.WriteString(";")

		args := make([]any, 0, len(events)*numFields)
		for _, event := range events {
			args = append(
				args,

				event.Id,
				event.Subject,
				int64(event.Type),
				event.Reply,
				event.ReplyCount,
				len(event.Data),
				event.Data,
				event.CreatedAt,
				event.ExpiresAt,
			)
		}

		stmt, err = conn.Prepare(ctx, sb.String(), args...)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		_, err = stmt.Step()
	})
	return
}

func (s *Sqlite) SaveConsumer(ctx context.Context, c *bus.Consumer) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		defer conn.Save(&err)()

		var lastEventId any
		var queueName any
		var stmt *sqlite.Stmt

		if c.LastEventId != "" {
			lastEventId = c.LastEventId
		}

		if c.QueueName != "" {
			queueName = c.QueueName
		}

		stmt, err = conn.Prepare(ctx,
			`INSERT INTO consumers 
				(
					id,
					pattern,
					type,
					ack_strategy,
					batch_size,
					queue_name,
					acked_counts,
					last_event_id,
					updated_at
				) 
			VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET 
				acked_counts = EXCLUDED.acked_counts,
				last_event_id = EXCLUDED.last_event_id,
				updated_at = EXCLUDED.updated_at
			;`,
			c.Id,
			c.Pattern,
			int64(c.Type),
			int64(c.AckStrategy),
			c.BatchSize,
			c.AckedCount,
			queueName,
			lastEventId,
			c.UpdatedAt,
		)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		_, err = stmt.Step()

	})
	return
}

func (s *Sqlite) Close() error {
	return s.db.Close()
}

func NewSqlite(ctx context.Context, workerSize int, opts ...sqlite.OptionFunc) (*Sqlite, error) {
	db, err := sqlite.New(opts...)
	if err != nil {
		return nil, err
	}

	err = sqlite.Migration(ctx, db, migrationFiles, "schema")
	if err != nil {
		return nil, err
	}

	return &Sqlite{
		db:  db,
		wdb: sqlite.NewWorker(db, 1000, int64(workerSize)),
	}, nil
}

func loadConsumer(stmt *sqlite.Stmt) *bus.Consumer {
	c := &bus.Consumer{}

	c.Id = stmt.GetText("id")
	c.Pattern = stmt.GetText("pattern")
	c.Type = bus.ConsumerType(stmt.GetInt64("type"))
	c.AckStrategy = bus.AckStrategy(stmt.GetInt64("ack_strategy"))
	c.BatchSize = stmt.GetInt64("batch_size")
	c.QueueName = stmt.GetText("queue_name")
	c.AckedCount = stmt.GetInt64("acked_counts")
	c.LastEventId = stmt.GetText("last_event_id")
	c.UpdatedAt = sqlite.LoadTime(stmt, "updated_at")

	return c
}

func loadEvent(stmt *sqlite.Stmt) (e *bus.Event, err error) {
	e = &bus.Event{}

	e.Id = stmt.GetText("id")
	e.Subject = stmt.GetText("subject")
	e.Type = bus.EventType(stmt.GetInt64("type"))
	e.Reply = stmt.GetText("reply")
	e.ReplyCount = stmt.GetInt64("reply_count")
	e.Size = stmt.GetInt64("size")
	e.Data = make([]byte, int(e.Size))
	e.CreatedAt = sqlite.LoadTime(stmt, "created_at")
	e.ExpiresAt = sqlite.LoadTime(stmt, "expires_at")

	i := stmt.GetBytes("data", e.Data)
	if i != int(e.Size) {
		return nil, fmt.Errorf("data size mismatch: %d != %d", i, e.Size)
	}

	return
}

func loadEvents(stmt *sqlite.Stmt) (events []*bus.Event, err error) {
	for hasRow, err := stmt.Step(); hasRow; hasRow, err = stmt.Step() {
		if err != nil {
			return nil, err
		}

		event, err := loadEvent(stmt)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return
}

func loadConsumers(stmt *sqlite.Stmt) (consumers []*bus.Consumer, err error) {
	for hasRow, err := stmt.Step(); hasRow; hasRow, err = stmt.Step() {
		if err != nil {
			return nil, err
		}

		consumers = append(consumers, loadConsumer(stmt))
	}

	return
}
