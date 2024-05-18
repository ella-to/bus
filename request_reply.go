package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type RequestFunc func(ctx context.Context, req any) (json.RawMessage, error)
type ReplyFunc func(ctx context.Context, req json.RawMessage) (any, error)

func Request(stream Stream, subject string) RequestFunc {
	return func(ctx context.Context, req any) (out json.RawMessage, err error) {
		evt, err := NewEvent(
			WithSubject(subject),
			WithReply(),
			WithJsonData(req),
			WithExpiresAt(30*time.Second),
		)
		if err != nil {
			return nil, err
		}

		err = stream.Put(ctx, evt)
		if err != nil {
			return nil, err
		}

		for msgs, err := range stream.Get(
			ctx,
			WithSubject(evt.Reply),
			WithFromOldest(),
		) {
			if err != nil {
				return nil, err
			}

			if len(msgs.Events) != 1 {
				return nil, fmt.Errorf("expected one event but got %d", len(msgs.Events))
			}

			evt := msgs.Events[0]

			replyMsg := struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}{}

			err = json.Unmarshal(evt.Data, &replyMsg)
			if err != nil {
				return nil, err
			}

			if replyMsg.Type == "error" {
				var errMsg string
				err = json.Unmarshal(replyMsg.Payload, &errMsg)
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf(errMsg)
			}

			return replyMsg.Payload, nil
		}

		return
	}
}

func Reply(ctx context.Context, stream Stream, subject string, fn ReplyFunc) {
	queueName := fmt.Sprintf("queue.%s", subject)
	msgs := stream.Get(
		ctx,
		WithSubject(subject),
		WithFromNewest(),
		WithQueue(queueName),
		WithManualAck(),
	)

	go func() {
		for msg, err := range msgs {
			if err != nil {
				return
			}

			if len(msg.Events) != 1 {
				slog.Error("expected one event", "events", len(msg.Events))
				continue
			}

			event := msg.Events[0]

			var replyMsg struct {
				Type    string `json:"type"`
				Payload any    `json:"payload"`
			}

			resp, err := fn(ctx, event.Data)
			if err != nil {
				replyMsg.Type = "error"
				replyMsg.Payload = err.Error()
			} else {
				replyMsg.Type = "result"
				replyMsg.Payload = resp
			}

			replyEvent, err := NewEvent(WithSubject(event.Reply), WithJsonData(replyMsg), WithExpiresAt(30*time.Second))
			if err != nil {
				return
			}

			err = stream.Put(ctx, replyEvent)
			if err != nil {
				return
			}

			err = msg.Ack(ctx)
			if err != nil {
				slog.Error("failed to acked event", "error", err)
			}
		}
	}()
}
