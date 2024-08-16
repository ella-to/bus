package storage

import (
	"ella.to/bus"
	"ella.to/sqlite"
)

func loadConsumer(stmt *sqlite.Stmt) *bus.Consumer {
	c := &bus.Consumer{}

	c.Id = stmt.GetText("id")
	c.Subject = stmt.GetText("subject")
	c.Type = bus.ConsumerType(stmt.GetInt64("type"))
	c.BatchSize = stmt.GetInt64("batch_size")
	c.QueueName = stmt.GetText("queue_name")
	c.AckedCount = stmt.GetInt64("acked_count")
	c.LastEventId = stmt.GetText("last_event_id")
	c.UpdatedAt = sqlite.LoadTime(stmt, "updated_at")

	return c
}

func loadEvent(stmt *sqlite.Stmt) (e *bus.Event) {
	e = &bus.Event{}

	e.Id = stmt.GetText("id")
	e.Subject = stmt.GetText("subject")
	e.Reply = stmt.GetText("reply")
	e.ReplyCount = stmt.GetInt64("reply_count")
	e.Data = stmt.GetText("data")
	e.CreatedAt = sqlite.LoadTime(stmt, "created_at")
	e.ExpiresAt = sqlite.LoadTime(stmt, "expires_at")

	return
}

func loadEvents(stmt *sqlite.Stmt) (events []*bus.Event, err error) {
	for hasRow, err := stmt.Step(); hasRow; hasRow, err = stmt.Step() {
		if err != nil {
			return nil, err
		}

		events = append(events, loadEvent(stmt))
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
