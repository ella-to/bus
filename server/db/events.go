package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ella.to/bus"
	"ella.to/sqlite"
)

var (
	ErrEventNotFound = fmt.Errorf("event not found")
)

func loadNextEvent(stmt *sqlite.Stmt) (*bus.Event, error) {
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if !hasRow {
		return nil, ErrEventNotFound
	}

	size := int(stmt.GetInt64("size"))

	expiresAt := sqlite.LoadTime(stmt, "expires_at")

	event := &bus.Event{
		Id:         stmt.GetText("id"),
		Subject:    stmt.GetText("subject"),
		Reply:      stmt.GetText("reply"),
		ReplyCount: stmt.GetInt64("reply_count"),
		Data:       make(json.RawMessage, size),
		CreatedAt:  sqlite.LoadTime(stmt, "created_at"),
		ExpiresAt:  &expiresAt,
	}

	n := stmt.GetBytes("data", event.Data)
	if n != size {
		return nil, fmt.Errorf("data size mismatch: %d != %d", n, size)
	}

	return event, nil
}

//
// ACKED
//

// AckEvent marks the all the events up to and including the event with the given eventId as acked.
// This is designed to be like this to allow for single ack for batching process
func AckEvent(ctx context.Context, conn *sqlite.Conn, consumerId, eventId string) (err error) {
	defer conn.Save(&err)()

	sql := `UPDATE consumers_events SET acked = 1 WHERE consumer_id = ? AND event_id <= ?;`

	stmt, err := conn.Prepare(ctx, sql, consumerId, eventId)
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
// APPEND
//

func AppendEvents(ctx context.Context, conn *sqlite.Conn, events ...*bus.Event) (err error) {
	defer conn.Save(&err)()

	const numFields = 8

	sql := fmt.Sprintf(`
		INSERT INTO events 
			(id, subject, reply, reply_count, size, data, created_at, expires_at) 
		VALUES
		%s;
	`, sqlite.GroupPlaceholders(len(events), numFields))

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

	stmt, err := conn.Prepare(ctx, sql, args...)
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
// LOAD
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

	return loadNextEvent(stmt)
}

// LoadLastConsumerEvent returns the last event that was acked by the consumer.
func LoadLastConsumerEvent(ctx context.Context, conn *sqlite.Conn, consumerId string) (*bus.Event, error) {
	sql := `
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

	INNER JOIN consumers_events ON consumers_events.event_id = events.id

	WHERE 
		consumers_events.consumer_id = ?
		AND consumers_events.acked = 1
	ORDER BY events.id DESC
	LIMIT 1
	;`

	stmt, err := conn.Prepare(ctx, sql, consumerId)
	if err != nil {
		return nil, err
	}

	defer stmt.Finalize()

	event, err := loadNextEvent(stmt)
	if err != nil {
		return nil, err
	}

	return event, nil
}

func LoadNotAckedEvent(ctx context.Context, conn *sqlite.Conn, consumerId string, lastEventId string) (*bus.Event, error) {
	sql := `
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
	
	INNER JOIN consumers_events ON consumers_events.event_id = events.id

	WHERE 
		consumers_events.consumer_id = ?
		AND events.id > ?
		AND consumers_events.acked = 0
	ORDER BY events.id
	;`

	stmt, err := conn.Prepare(ctx, sql, consumerId, lastEventId)
	if err != nil {
		return nil, err
	}

	defer stmt.Finalize()

	event, err := loadNextEvent(stmt)
	if err != nil {
		return nil, err
	}

	return event, nil
}

func DeleteExpiredEventsBeforeDate(ctx context.Context, conn *sqlite.Conn, before time.Time) (err error) {
	defer conn.Save(&err)()

	const sql = `DELETE FROM events WHERE expires_at < ?;`

	stmt, err := conn.Prepare(ctx, sql, before)
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

func DeleteExpiredEvents(ctx context.Context, conn *sqlite.Conn) (err error) {
	return DeleteExpiredEventsBeforeDate(ctx, conn, time.Now())
}
