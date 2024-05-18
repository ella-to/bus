package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type RequestFunc func(ctx context.Context, req any, resp any) error
type ReplyFunc[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

func Request(stream Stream, subject string) RequestFunc {
	return func(ctx context.Context, req any, resp any) (err error) {
		evt, err := NewEvent(
			WithSubject(subject),
			WithReply(),
			WithJsonData(req),
			WithExpiresAt(30*time.Second),
		)
		if err != nil {
			return err
		}

		err = stream.Put(ctx, evt)
		if err != nil {
			return err
		}

		for msgs, err := range stream.Get(
			ctx,
			WithSubject(evt.Reply),
			WithFromOldest(),
		) {
			if err != nil {
				return err
			}

			if len(msgs.Events) != 1 {
				return fmt.Errorf("expected one event but got %d", len(msgs.Events))
			}

			evt := msgs.Events[0]

			replyMsg := struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}{}

			err = json.Unmarshal(evt.Data, &replyMsg)
			if err != nil {
				return err
			}

			if replyMsg.Type == "error" {
				var errMsg string
				err = json.Unmarshal(replyMsg.Payload, &errMsg)
				if err != nil {
					return err
				}
				return fmt.Errorf(errMsg)
			}

			err = json.Unmarshal(replyMsg.Payload, resp)
			if err != nil {
				return err
			}

			return nil
		}

		return
	}
}

func Reply[Req, Resp any](ctx context.Context, stream Stream, subject string, fn ReplyFunc[*Req, *Resp]) {
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

			var req Req
			err = json.Unmarshal(event.Data, &req)
			if err != nil {
				return
			}

			var replyMsg struct {
				Type    string `json:"type"`
				Payload any    `json:"payload"`
			}

			resp, err := fn(ctx, &req)
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
