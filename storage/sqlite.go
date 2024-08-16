package storage

import (
	"context"
	"embed"
	"strings"
	"time"

	"ella.to/bus"
	"ella.to/sqlite"
)

//go:embed schema/*.sql
var migrationFiles embed.FS

type Sqlite struct {
	db  *sqlite.Database
	wdb *sqlite.Worker
}

var _ Storage = (*Sqlite)(nil)

func (s *Sqlite) SaveEvent(ctx context.Context, events *bus.Event) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		defer conn.Save(&err)()

		var stmt *sqlite.Stmt

		stmt, err = conn.Prepare(ctx,
			`INSERT INTO events 
			(id, subject, reply, reply_count, size, data, created_at, expires_at) 
			VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?);`,

			events.Id,
			events.Subject,
			events.Reply,
			events.ReplyCount,
			events.Size,
			[]byte(events.Data),
			events.CreatedAt,
			events.ExpiresAt,
		)
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
					subject,
					type,
					ack_strategy,
					batch_size,
					acked_count,
					queue_name,
					last_event_id,
					updated_at
				) 
			VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET 
				acked_count = EXCLUDED.acked_count,
				last_event_id = EXCLUDED.last_event_id,
				updated_at = EXCLUDED.updated_at
			;`,
			c.Id,
			c.Subject,
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

func (s *Sqlite) LoadEventById(ctx context.Context, eventId string) (event *bus.Event, err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt
		var hasRow bool

		stmt, err = conn.Prepare(ctx, `SELECT * FROM events WHERE id = ?;`, eventId)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		hasRow, err = stmt.Step()
		if err != nil {
			return
		}

		if !hasRow {
			err = ErrEventNotFound
			return
		}

		event, err = loadEvent(stmt)
	})

	return
}

func (s *Sqlite) LoadConsumerById(ctx context.Context, consumerId string) (consumer *bus.Consumer, err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt
		var hasRow bool

		stmt, err = conn.Prepare(ctx, `SELECT * FROM consumers WHERE id = ?;`, consumerId)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		hasRow, err = stmt.Step()
		if err != nil {
			return
		}

		if !hasRow {
			err = ErrConsumerNotFound
		}

		consumer = loadConsumer(stmt)
	})

	return
}

func (s *Sqlite) LoadNextQueueConsumerByName(ctx context.Context, queueName string) (consumer *bus.Consumer, err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt
		var hasRow bool

		stmt, err = conn.Prepare(ctx, `SELECT * FROM consumers WHERE queue_name = ? ORDER BY ack_counts ASC LIMIT 1;`, queueName)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		hasRow, err = stmt.Step()
		if err != nil {
			return
		}

		if !hasRow {
			err = ErrConsumerNotFound
			return
		}

		consumer = loadConsumer(stmt)
	})

	return
}

func (s *Sqlite) LoadEventsByConsumerId(ctx context.Context, consumerId string) (events []*bus.Event, err error) {
	consumer, err := s.LoadConsumerById(ctx, consumerId)
	if err != nil {
		return
	}

	pattern := strings.ReplaceAll(consumer.Subject, "*", "%")
	pattern = strings.ReplaceAll(pattern, ">", "%")

	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt

		stmt, err = conn.Prepare(ctx,
			`SELECT * FROM events
			WHERE 
				subject LIKE ? AND
				id > ?
				ORDER BY id
				LIMIT ?;`,
			pattern,
			consumer.LastEventId,
			consumer.BatchSize,
		)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		events, err = loadEvents(stmt)
	})

	return
}

func (s *Sqlite) LoadLastEventId(ctx context.Context) (lastEventId string, err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt
		var hasRow bool

		stmt, err = conn.Prepare(ctx, `SELECT id FROM events ORDER BY id DESC LIMIT 1;`)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		hasRow, err = stmt.Step()
		if err != nil {
			return
		}

		if !hasRow {
			err = ErrEventNotFound
			return
		}

		lastEventId = stmt.GetText("id")
	})

	return
}

func (s *Sqlite) UpdateConsumerAck(ctx context.Context, consumerId string, eventId string) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt

		stmt, err = conn.Prepare(ctx,
			`UPDATE consumers 
			SET 
			 	acked_count = acked_count + 1, 
				last_event_id = ?,
				updated_at = ?
			WHERE 
				id = ?;`,
			eventId,
			time.Now(),
			consumerId,
		)

		if err != nil {
			return
		}

		defer stmt.Finalize()

		_, err = stmt.Step()
	})

	return
}

func (s *Sqlite) DeleteConsumer(ctx context.Context, consumerId string) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt

		stmt, err = conn.Prepare(ctx, `DELETE FROM consumers WHERE id = ?;`, consumerId)
		if err != nil {
			return
		}

		defer stmt.Finalize()

		_, err = stmt.Step()
	})

	return
}

func (s *Sqlite) DeleteExpiredEvents(ctx context.Context) (err error) {
	s.wdb.Submit(func(conn *sqlite.Conn) {
		var stmt *sqlite.Stmt

		stmt, err = conn.Prepare(ctx, `DELETE FROM events WHERE expires_at < ?;`, time.Now())
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