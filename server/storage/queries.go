package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"ella.to/bus"
	"ella.to/sqlite"
)

var (
	ErrEventNotFound    = errors.New("event not found")
	ErrConsumerNotFound = errors.New("consumer not found")
)

func LoadNotAckedEvents(ctx context.Context, conn *sqlite.Conn, consumerId string) (events []*bus.Event, err error) {
	stmt, err := conn.Prepare(ctx, `
		SELECT 
			events.id AS id, 
			events.subject AS subject, 
			events.reply AS reply,
			events.reply_count AS reply_count,
			events.size AS size, 
			events.data AS data, 
			events.created_at AS created_at,
			events.expires_at AS expires_at
		FROM events
		JOIN consumers_events 
			ON events.id = consumers_events.event_id 
		WHERE 
			consumers_events.consumer_id = ? 
			AND consumers_events.acked = 0
		ORDER BY events.id ASC
		LIMIT 1;`, consumerId)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	events = make([]*bus.Event, 0)

	for {
		event, err := loadEvent(stmt)
		if errors.Is(err, ErrEventNotFound) {
			break
		} else if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}

func loadEvent(stmt *sqlite.Stmt) (*bus.Event, error) {
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if !hasRow {
		return nil, ErrEventNotFound
	}

	size := stmt.GetInt64("size")

	expiresAt := sqlite.LoadTime(stmt, "expires_at")

	event := &bus.Event{
		Id:         stmt.GetText("id"),
		Subject:    stmt.GetText("subject"),
		Reply:      stmt.GetText("reply"),
		ReplyCount: stmt.GetInt64("reply_count"),
		Size:       size,
		Data:       make(json.RawMessage, size),
		CreatedAt:  sqlite.LoadTime(stmt, "created_at"),
		ExpiresAt:  &expiresAt,
	}

	n := stmt.GetBytes("data", event.Data)
	if int64(n) != size {
		return nil, fmt.Errorf("data size mismatch: %d != %d", n, size)
	}

	return event, nil
}

func loadConsumer(stmt *sqlite.Stmt) (*bus.Consumer, error) {
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if !hasRow {
		return nil, ErrConsumerNotFound
	}

	c := &bus.Consumer{}

	c.Id = stmt.GetText("id")
	c.Pattern = stmt.GetText("pattern")
	c.QueueName = stmt.GetText("queue_name")
	c.Durable = stmt.GetBool("durable")
	c.BatchSize = stmt.GetInt64("batch_size")
	c.AckedCount = stmt.GetInt64("acked_counts")
	c.LastEventId = stmt.GetText("last_event_id")
	c.UpdatedAt = sqlite.LoadTime(stmt, "updated_at")
	c.ExpiresAt = sqlite.LoadTime(stmt, "expires_in")

	return c, nil
}

//
// CONSUMERS
//

func LoadConsumerById(ctx context.Context, conn *sqlite.Conn, id string) (*bus.Consumer, error) {
	stmt, err := conn.Prepare(ctx, `
		SELECT 
			consumers.id AS id, 
			consumers.pattern AS pattern,
			consumers.queue_name AS queue_name,
			consumers.durable AS durable,
			consumers.batch_size AS batch_size,
			consumers.acked_counts AS acked_counts,
			(
				SELECT 
					event_id 
				FROM consumers_events 
				WHERE 
					consumer_id = ? 
					AND acked = 1 
				ORDER BY event_id DESC 
				LIMIT 1
			) AS last_event_id,
			consumers.updated_at AS updated_at,
			consumers.expires_in AS expires_in
		FROM consumers
		WHERE consumers.id = ?;`, id, id)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	return loadConsumer(stmt)
}

func SaveConsumer(ctx context.Context, conn *sqlite.Conn, c *bus.Consumer) (err error) {
	defer conn.Save(&err)()

	var lastEventId any
	var queueName any

	if c.LastEventId != "" {
		lastEventId = c.LastEventId
	}

	if c.QueueName != "" {
		queueName = c.QueueName
	}

	stmt, err := conn.Prepare(ctx,
		`INSERT INTO consumers 
			(
				id,
				pattern,
				queue_name,
				durable,
				batch_size,
				acked_counts,
				last_event_id,
				updated_at,
				expires_in
			) 
		VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO NOTHING;`,
		c.Id,
		c.Pattern,
		queueName,
		c.Durable,
		c.BatchSize,
		c.AckedCount,
		lastEventId,
		c.UpdatedAt,
		c.ExpiresAt,
	)
	if err != nil {
		return err
	}

	defer stmt.Finalize()

	_, err = stmt.Step()
	return err
}

//
// EVENTS
//

func LoadLastEvent(ctx context.Context, conn *sqlite.Conn) (*bus.Event, error) {
	sql := `
	SELECT 
		id, 
		subject, 
		reply,
		reply_count,
		size, 
		data, 
		created_at,
		expires_at
	FROM events 
	ORDER BY id DESC 
	LIMIT 1
	;`

	stmt, err := conn.Prepare(ctx, sql)
	if err != nil {
		return nil, err
	}

	defer stmt.Finalize()

	return loadEvent(stmt)
}

func AppendEvents(ctx context.Context, conn *sqlite.Conn, events ...*bus.Event) (err error) {
	defer conn.Save(&err)()

	const numFields = 8

	var sb strings.Builder

	sb.WriteString(`
	INSERT INTO events 
		(id, subject, reply, reply_count, size, data, created_at, expires_at) 
	VALUES `)

	sqlite.GroupPlaceholdersStringBuilder(len(events), numFields, &sb)

	args := make([]any, 0, len(events)*numFields)
	for _, event := range events {
		args = append(
			args,

			event.Id,
			event.Subject,
			event.Reply,
			event.ReplyCount,
			len(event.Data),
			event.Data,
			event.CreatedAt,
			*event.ExpiresAt,
		)
	}

	stmt, err := conn.Prepare(ctx, sb.String(), args...)
	if err != nil {
		return err
	}

	defer stmt.Finalize()

	_, err = stmt.Step()
	if err != nil {
		return err
	}

	return nil
}

//
// DELETE CONSUMER
//

func DeleteConsumer(ctx context.Context, conn *sqlite.Conn, consumerId string) (err error) {
	defer conn.Save(&err)()

	stmt, err := conn.Prepare(ctx, `DELETE FROM consumers WHERE id = ?;`, consumerId)
	if err != nil {
		return err
	}

	defer stmt.Finalize()

	_, err = stmt.Step()
	return err
}

//
// Ack
//

func AckEvent(ctx context.Context, conn *sqlite.Conn, consumerId, eventId string) (err error) {
	defer conn.Save(&err)()

	stmt, err := conn.Prepare(ctx, `
	UPDATE 
		consumers_events 
	SET
		acked = 1
	WHERE
		consumer_id = ? AND event_id <= ?;
	`, consumerId, eventId)
	if err != nil {
		return err
	}

	defer stmt.Finalize()

	_, err = stmt.Step()
	return err
}
