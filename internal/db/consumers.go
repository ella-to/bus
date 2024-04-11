package db

import (
	"context"
	"errors"

	"ella.to/bus.go"
	"ella.to/bus.go/internal/sqlite"
)

var (
	ErrConsumerNotFound = errors.New("consumer not found")
)

func LoadConsumerById(ctx context.Context, conn *sqlite.Conn, id string) (*bus.Consumer, error) {
	stmt, err := conn.Prepare(ctx, `
		SELECT 
			consumers.id AS id, 
			consumers.subject AS subject,
			queues.name AS queue,
			(
				SELECT 
					event_id 
				FROM consumers_events 
				WHERE 
					consumer_id = ? 
					AND acked = 1 
				ORDER BY event_id DESC 
				LIMIT 1
			) AS last_event_id
		FROM consumers
		INNER JOIN queues_consumers ON queues_consumers.consumer_id = consumers.id
		INNER JOIN queues ON queues.name = queues_consumers.queue_name
		WHERE consumers.id = ?;`, id, id)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if !rowReturned {
		return nil, ErrConsumerNotFound
	}

	c := &bus.Consumer{}

	c.Id = stmt.GetText("id")
	c.Subject = stmt.GetText("subject")
	c.Queue = stmt.GetText("queue")
	c.LastEventId = stmt.GetText("last_event_id")

	return c, nil
}

func SaveConsumer(ctx context.Context, conn *sqlite.Conn, c *bus.Consumer) (err error) {
	defer conn.Save(&err)()

	err = func() error {
		stmt, err := conn.Prepare(ctx,
			`INSERT OR IGNORE INTO consumers 
				(id, subject) 
			VALUES 
				(?, ?)
			;`,
			c.Id, c.Subject,
		)
		if err != nil {
			return err
		}

		defer stmt.Finalize()

		_, err = stmt.Step()
		return err
	}()
	if err != nil {
		return err
	}

	err = func() error {
		if c.Queue == "" {
			return nil
		}

		stmt, err := conn.Prepare(ctx,
			`INSERT OR IGNORE INTO queues_consumers 
				(queue_name, consumer_id)
			VALUES
				(?, ?)
			;`, c.Queue, c.Id,
		)
		if err != nil {
			return err
		}
		defer stmt.Finalize()

		_, err = stmt.Step()
		return err
	}()
	if err != nil {
		return err
	}

	return nil
}

func DeleteConsumerById(ctx context.Context, conn *sqlite.Conn, id string) (err error) {
	defer conn.Save(&err)()

	stmt, err := conn.Prepare(ctx, `DELETE FROM consumers WHERE id = ?;`, id)
	if err != nil {
		return err
	}

	defer stmt.Finalize()

	_, err = stmt.Step()
	return err
}
