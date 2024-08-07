package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"ella.to/bus/internal/gen"
)

const DefaultAddr = "http://0.0.0.0:8080"

// magic number for representing 2121-05-30T12:26:00-04:00
const DefaultExpiresAtUnix = 4778065560

func GetDefaultExpiresAt() *time.Time {
	expiresAt := time.Unix(DefaultExpiresAtUnix, 0)
	return &expiresAt
}

func GetEventId() string {
	return fmt.Sprint("e_", gen.NewID())
}

func GetConsumerId() string {
	return fmt.Sprint("c_", gen.NewID())
}

//
// OPTIONS
//

type EventOpt interface {
	configureEvent(*Event) error
}

type eventOptFn func(opts *Event) error

func (opt eventOptFn) configureEvent(opts *Event) error {
	return opt(opts)
}

type MsgOpt interface {
	configureMsg(*Msg) error
}

type msgOptFn func(opts *Msg) error

func (opt msgOptFn) configureMsg(opts *Msg) error {
	return opt(opts)
}

type ConsumerOpt interface {
	configureConsumer(*Consumer) error
}

type consumerOptFn func(opts *Consumer) error

func (opt consumerOptFn) configureConsumer(opts *Consumer) error {
	return opt(opts)
}

type subjectOpt struct {
	value string
}

var _ EventOpt = (*subjectOpt)(nil)
var _ ConsumerOpt = (*subjectOpt)(nil)

func (opt *subjectOpt) configureEvent(evt *Event) error {
	evt.Subject = opt.value
	return nil
}

func (opt *subjectOpt) configureConsumer(c *Consumer) error {
	c.Pattern = strings.ReplaceAll(opt.value, "*", "%")
	return nil
}

func WithSubject(subject string) *subjectOpt {
	return &subjectOpt{value: subject}
}

func WithJsonData(v any) EventOpt {
	return eventOptFn(func(evt *Event) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}

		evt.Data = data
		return nil
	})
}

func WithData(data []byte) EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Data = data
		return nil
	})
}

func WithQueue(queueName string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.QueueName = queueName
		return nil
	})
}

func WithDurable() ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.Durable = true
		return nil
	})
}

func WithManualAck() ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.AckStrategy = "manual"
		return nil
	})
}

func WithFromOldest() ConsumerOpt {
	return WithFromEventId("oldest")
}

func WithFromNewest() ConsumerOpt {
	return WithFromEventId("newest")
}

func WithFromEventId(eventId string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.LastEventId = eventId
		return nil
	})
}

func WithExpiresAt(duration time.Duration) EventOpt {
	return eventOptFn(func(evt *Event) error {
		exporesAt := time.Now().Add(duration)
		evt.ExpiresAt = &exporesAt
		return nil
	})
}

func WithId(id string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.Id = id
		return nil
	})
}

func WithBatchSize(size int64) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.BatchSize = size
		return nil
	})
}

func WithReply() EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Reply = fmt.Sprintf("inbox.%s", gen.NewID())
		return nil
	})
}

func WithConfirm(n int64) EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Reply = fmt.Sprintf("confirm.%s", gen.NewID())
		evt.ReplyCount = n
		return nil
	})
}

// WithInitAck is an option to initialize the acker for the events
// NOTE: This option is only used for the client and should not be used
// directly by the user
func WithInitAck(consumerId string, acker Acker) MsgOpt {
	return msgOptFn(func(m *Msg) error {
		m.consumerId = consumerId
		m.acker = acker
		return nil
	})
}

//
// EVENTS
//

type Event struct {
	Id         string          `json:"id"`
	Subject    string          `json:"subject"`
	Reply      string          `json:"reply_to"`
	ReplyCount int64           `json:"reply_count"`
	Size       int64           `json:"size"`
	Data       json.RawMessage `json:"data"`
	CreatedAt  time.Time       `json:"created_at"`
	ExpiresAt  *time.Time      `json:"expires_at,omitempty"`
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

type Msg struct {
	Events []*Event

	consumerId string
	acker      Acker
}

func (m *Msg) Ack(ctx context.Context) error {
	if m.acker == nil || len(m.Events) == 0 {
		return nil
	}

	return m.acker.Ack(ctx, m.consumerId, m.Events[len(m.Events)-1].Id)
}

func NewMsg(events []*Event, opts ...MsgOpt) (*Msg, error) {
	msg := &Msg{
		Events: events,
	}

	for _, opt := range opts {
		if err := opt.configureMsg(msg); err != nil {
			return nil, err
		}
	}

	return msg, nil
}

//
// CONSUMERS
//

type ConsumerType int

const (
	_ ConsumerType = iota
	EphemeralConsumer
	DurableConsumer
	QueueConsumer
)

type Consumer struct {
	Id          string    `json:"id"`
	Pattern     string    `json:"pattern"`
	QueueName   string    `json:"queue_name"`
	AckStrategy string    `json:"ack_strategy"` // manual, auto
	Durable     bool      `json:"durable"`
	BatchSize   int64     `json:"batch_size"`
	AckedCount  int64     `json:"acked_count"`
	LastEventId string    `json:"last_event_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func NewConsumer(opts ...ConsumerOpt) (*Consumer, error) {
	c := &Consumer{}

	for _, opt := range opts {
		err := opt.configureConsumer(c)
		if err != nil {
			return nil, err
		}
	}

	if c.QueueName != "" && c.Durable {
		return nil, fmt.Errorf("durable and queue consumer can't be used together")
	}

	return c, nil
}

//
// Consumers Event Map
// This is a map of consumerId to a channel of events
//

type ConsumersEventMap struct {
	pipes        map[string]chan *Event
	waitDuration time.Duration
	rw           sync.RWMutex
}

func (m *ConsumersEventMap) Add(consumerId string, size int64) <-chan *Event {
	m.rw.Lock()
	defer m.rw.Unlock()

	pipe := make(chan *Event, size)
	m.pipes[consumerId] = pipe

	return pipe
}

func (m *ConsumersEventMap) Remove(consumerId string) {
	m.rw.Lock()
	defer m.rw.Unlock()

	delete(m.pipes, consumerId)
}

func (m *ConsumersEventMap) Push(consumerId string, evt *Event) error {
	var pipe chan *Event
	var ok bool

	m.rw.RLock()
	pipe, ok = m.pipes[consumerId]
	m.rw.RUnlock()
	if !ok {
		return nil
	}

	select {
	case pipe <- evt:
		return nil
	case <-time.After(m.waitDuration):
		return fmt.Errorf("consumer %s pipe is full", consumerId)
	}
}

func NewConsumersEventMap(size int, waitDuration time.Duration) *ConsumersEventMap {
	return &ConsumersEventMap{
		pipes:        make(map[string]chan *Event),
		waitDuration: waitDuration,
	}
}

//
// QUEUES
//

type Queue struct {
	Name        string `json:"name"`
	Pattern     string `json:"pattern"`
	AckStrategy string `json:"ack_strategy"` // manual, auto
	LastEventId string `json:"last_event_id"`
}

//
// Stream
//

type Putter interface {
	Put(ctx context.Context, evt *Event) error
}

type Getter interface {
	Get(ctx context.Context, opts ...ConsumerOpt) iter.Seq2[*Msg, error]
}

type Acker interface {
	Ack(ctx context.Context, consumerId string, eventId string) error
}

type Closer interface {
	Close(ctx context.Context, consumerId string) error
}

type Stream interface {
	Putter
	Getter
	Acker
	Closer
}
