package db

import (
	"context"
	"errors"

	"ella.to/bus"
	"ella.to/sqlite"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
)

func LoadQueueByName(ctx context.Context, conn *sqlite.Conn, name string) (*bus.Queue, error) {
	stmt, err := conn.Prepare(ctx, `SELECT * FROM queues WHERE name = ?;`, name)
	if err != nil {
		return nil, err
	}

	defer stmt.Finalize()

	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if !rowReturned {
		return nil, ErrQueueNotFound
	}

	q := &bus.Queue{}

	q.Name = stmt.GetText("name")
	q.LastEventId = stmt.GetText("last_event_id")

	return q, nil
}

func SaveQueue(ctx context.Context, conn *sqlite.Conn, queue *bus.Queue) (err error) {
	defer conn.Save(&err)()

	sql := `
		INSERT INTO queues 
			(name, last_event_id) 
		VALUES
			(?, ?);
	`

	var lastEventId any
	if queue.LastEventId != "" {
		lastEventId = queue.LastEventId
	}

	stmt, err := conn.Prepare(ctx, sql, queue.Name, lastEventId)
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
