package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"time"
)

type RequestReplyFunc[Req, Resp any] func(context.Context, Req) (Resp, error)

func Request[Req, Resp any](stream Stream, subject string) RequestReplyFunc[Req, Resp] {
	return func(ctx context.Context, req Req) (resp Resp, err error) {
		evt, err := NewEvent(
			WithSubject(subject),
			WithReply(),
			WithJsonData(req),
			WithExpiresAt(30*time.Second),
		)
		if err != nil {
			return resp, err
		}

		err = stream.Put(ctx, evt)
		if err != nil {
			return resp, err
		}

		for evt, err := range stream.Get(
			ctx,
			WithSubject(evt.Reply),
			WithFromOldest(),
		) {
			if err != nil {
				return resp, err
			}

			replyMsg := struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}{}

			err = json.Unmarshal(evt.Data, &replyMsg)
			if err != nil {
				return resp, err
			}

			if replyMsg.Type == "error" {
				var errMsg string
				err = json.Unmarshal(replyMsg.Payload, &errMsg)
				if err != nil {
					return resp, err
				}
				return resp, fmt.Errorf(errMsg)
			}

			resp, err = jsonUnmarshal[Resp](replyMsg.Payload)
			if err != nil {
				return resp, err
			}

			return resp, nil
		}

		return
	}
}

func Reply[Req, Resp any](ctx context.Context, stream Stream, subject string, fn RequestReplyFunc[Req, Resp]) {
	queueName := fmt.Sprintf("queue.%s", subject)
	events := stream.Get(
		ctx,
		WithSubject(subject),
		WithFromNewest(),
		WithQueue(queueName),
		WithManualAck(),
	)

	go func() {
		for evt, err := range events {
			if err != nil {
				return
			}

			req, err := jsonUnmarshal[Req](evt.Data)
			if err != nil {
				return
			}

			var replyMsg struct {
				Type    string `json:"type"`
				Payload any    `json:"payload"`
			}

			resp, err := fn(ctx, req)
			if err != nil {
				replyMsg.Type = "error"
				replyMsg.Payload = err.Error()
			} else {
				replyMsg.Type = "result"
				replyMsg.Payload = resp
			}

			replyEvent, err := NewEvent(WithSubject(evt.Reply), WithJsonData(replyMsg), WithExpiresAt(30*time.Second))
			if err != nil {
				return
			}

			err = stream.Put(ctx, replyEvent)
			if err != nil {
				return
			}

			err = evt.Ack(ctx)
			if err != nil {
				slog.Error("failed to acked event", "error", err)
			}
		}
	}()
}

func isPointer(v any) bool {
	t := reflect.TypeOf(v)
	return t.Kind() == reflect.Ptr
}

func jsonUnmarshal[T any](data json.RawMessage) (v T, err error) {
	if isPointer(v) {
		v = initializePointer(v)
		err = json.Unmarshal(data, v)
		return v, err
	}

	err = json.Unmarshal(data, &v)
	return v, err
}

func initializePointer[T any](v T) T {
	t := reflect.TypeOf(v)
	return reflect.New(t.Elem()).Interface().(T)
}
