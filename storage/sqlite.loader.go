package storage

import (
	"fmt"

	"ella.to/bus"
	"ella.to/sqlite"
)

func loadConsumer(stmt *sqlite.Stmt) *bus.Consumer {
	c := &bus.Consumer{}

	c.Id = stmt.GetText("id")
	c.Subject = stmt.GetText("subject")
	c.Type = bus.ConsumerType(stmt.GetInt64("type"))
	c.AckStrategy = bus.AckStrategy(stmt.GetInt64("ack_strategy"))
	c.BatchSize = stmt.GetInt64("batch_size")
	c.QueueName = stmt.GetText("queue_name")
	c.AckedCount = stmt.GetInt64("acked_count")
	c.LastEventId = stmt.GetText("last_event_id")
	c.UpdatedAt = sqlite.LoadTime(stmt, "updated_at")

	return c
}

func loadEvent(stmt *sqlite.Stmt) (e *bus.Event, err error) {
	e = &bus.Event{}

	e.Id = stmt.GetText("id")
	e.Subject = stmt.GetText("subject")
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
