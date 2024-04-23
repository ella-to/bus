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

// AttachEventMetaData is used to set the consumerId and acker for an event
// NOTE: Don't call this directly, this is used internally by the library
func AttachEventMetaData(evt *Event, consumerId string, acker Acker) {
	evt.consumerId = consumerId
	evt.acker = acker
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

	consumerId string
	acker      Acker
}

func (evt *Event) Ack(ctx context.Context) error {
	if evt.acker == nil {
		return nil
	}

	return evt.acker.Ack(ctx, evt.consumerId, evt.Id)
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
	ExpiresAt   time.Time `json:"expires_at"`
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
	Get(ctx context.Context, opts ...ConsumerOpt) iter.Seq2[*Event, error]
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
