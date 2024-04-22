package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

type RequestReplyFunc[Req, Resp any] func(context.Context, Req) (Resp, error)

func Request[Req, Resp any](stream Stream, subject string) RequestReplyFunc[Req, Resp] {
	return func(ctx context.Context, req Req) (resp Resp, err error) {
		evt, err := NewEvent(WithSubject(subject), WithReply(), WithJsonData(req), WithExpiresAt(30*time.Second))
		if err != nil {
			return resp, err
		}

		err = stream.Publish(ctx, evt)
		if err != nil {
			return resp, err
		}

		for evt, err := range stream.Consume(ctx, WithSubject(evt.Reply), WithFromOldest()) {
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
				return resp, fmt.Errorf(string(replyMsg.Payload))
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
	events := stream.Consume(
		ctx,
		WithSubject(subject),
		WithFromNewest(),
		WithQueue(queueName),
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

			err = stream.Publish(ctx, replyEvent)
			if err != nil {
				return
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
