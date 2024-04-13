package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"sync"
	"time"

	"ella.to/bus.go/internal/gen"
)

func GetEventId() string {
	return fmt.Sprint("e_", gen.NewID())
}

func GetConsumerId() string {
	return fmt.Sprint("c_", gen.NewID())
}

type Event struct {
	Id        string          `json:"id,omitempty"`
	Subject   string          `json:"subject"`
	Reply     string          `json:"reply,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

func NewEvent(opts ...EventOpt) (*Event, error) {
	evt := &Event{}

	for _, opt := range opts {
		err := opt.configureEvent(evt)
		if err != nil {
			return nil, err
		}
	}

	return evt, nil
}

type Queue struct {
	Name        string `json:"name"`
	LastEventId string `json:"last_id"`
}

type Consumer struct {
	Id          string `json:"id"`
	Subject     string `json:"subject"`
	Queue       string `json:"queue"`
	LastEventId string `json:"last_id"`
}

func NewConsumer(opts ...ConsumerOpt) (*Consumer, error) {
	c := &Consumer{}

	for _, opt := range opts {
		err := opt.configureConsumer(c)
		if err != nil {
			return nil, err
		}
	}

	if c.Subject == "" {
		return nil, fmt.Errorf("consumer must have a subject")
	}

	if c.Id == "" {
		c.Id = GetConsumerId()
	}

	return c, nil
}

//
// OPTIONS
//

type ConsumerOpt interface {
	configureConsumer(*Consumer) error
}

type consumerOptFn func(opts *Consumer) error

func (opt consumerOptFn) configureConsumer(opts *Consumer) error {
	return opt(opts)
}

type EventOpt interface {
	configureEvent(*Event) error
}

type eventOptFn func(opts *Event) error

func (opt eventOptFn) configureEvent(opts *Event) error {
	return opt(opts)
}

type subjectOpt struct {
	value string
}

func (opt *subjectOpt) configureEvent(evt *Event) error {
	evt.Subject = opt.value
	return nil
}

func (opt *subjectOpt) configureConsumer(c *Consumer) error {
	c.Subject = opt.value
	return nil
}

//
// OPTIONS FUNCTIONS
//

func WithSubject(subject string) *subjectOpt {
	return &subjectOpt{value: subject}
}

func WithQueue(queueName string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.Queue = queueName
		return nil
	})
}

func WithDurable(durable string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.Id = durable
		return nil
	})
}

func WithFromBeginning() ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.LastEventId = "all"
		return nil
	})
}

func WithFromNewest() ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.LastEventId = "normal"
		return nil
	})
}

func WithFromEventId(eventId string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.LastEventId = eventId
		return nil
	})
}

func WithReply() EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Reply = fmt.Sprintf("inbox.%s", gen.NewID())
		return nil
	})
}

func WithConfirm() EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Reply = fmt.Sprintf("confirm.%s", gen.NewID())
		return nil
	})
}

func WithData(data any) EventOpt {
	return eventOptFn(func(evt *Event) error {
		var err error
		evt.Data, err = json.Marshal(data)
		return err
	})
}

//
// Helper types and functions
//

type ConsumersEventMap struct {
	pipes map[string]chan *Event
	size  int
	rw    sync.RWMutex
}

func (m *ConsumersEventMap) Add(consumerId string) <-chan *Event {
	m.rw.Lock()
	defer m.rw.Unlock()

	pipe := make(chan *Event, m.size)
	m.pipes[consumerId] = pipe

	return pipe
}

func (m *ConsumersEventMap) Remove(consumerId string) {
	m.rw.Lock()
	defer m.rw.Unlock()

	delete(m.pipes, consumerId)
}

func (m *ConsumersEventMap) Push(consumerId string, evt *Event) {
	var pipe chan *Event
	var ok bool

	m.rw.RLock()
	pipe, ok = m.pipes[consumerId]
	m.rw.RUnlock()
	if !ok {
		return
	}

	pipe <- evt
}

func NewConsumersEventMap(size int) *ConsumersEventMap {
	return &ConsumersEventMap{
		pipes: make(map[string]chan *Event),
		size:  size,
	}
}

type Stream interface {
	Publish(ctx context.Context, evt *Event) error
	Consume(ctx context.Context, consumerOpts ...ConsumerOpt) iter.Seq2[*Event, error]
}

type RequestReplyFunc[Req, Resp any] func(context.Context, Req) (Resp, error)

func Request[Req, Resp any](stream Stream, subject string) RequestReplyFunc[Req, Resp] {
	return func(ctx context.Context, req Req) (resp Resp, err error) {
		evt, err := NewEvent(WithSubject(subject), WithReply(), WithData(req))
		if err != nil {
			return resp, err
		}

		err = stream.Publish(ctx, evt)
		if err != nil {
			return resp, err
		}

		for evt, err := range stream.Consume(ctx, WithSubject(evt.Reply), WithFromBeginning()) {
			if err != nil {
				return resp, err
			}

			resp, err = jsonUnmarshal[Resp](evt.Data)
			if err != nil {
				return resp, err
			}

			return resp, nil
		}

		return
	}
}

func Reply[Req, Resp any](ctx context.Context, stream Stream, subject string, fn RequestReplyFunc[Req, Resp]) {
	go func() {
		for evt, err := range stream.Consume(ctx, WithSubject(subject), WithFromNewest()) {
			if err != nil {
				return
			}

			req, err := jsonUnmarshal[Req](evt.Data)
			if err != nil {
				return
			}

			resp, err := fn(ctx, req)
			if err != nil {
				return
			}

			replyEvent, err := NewEvent(WithSubject(evt.Reply), WithData(resp))
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
