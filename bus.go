package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
)

const DefaultAddr = "http://0.0.0.0:8080"

// magic number for representing 2121-05-30T12:26:00-04:00 which is represent a magical time for me
const DefaultExpiresAtUnix = 4778065560

const Oldest = "oldest"
const Newest = "newest"

func GetDefaultExpiresAt() time.Time {
	return time.Unix(DefaultExpiresAtUnix, 0)
}

func GetEventId() string {
	return newID("e_")
}

func GetConsumerId() string {
	return newID("c_")
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
	c.Subject = opt.value
	return nil
}

func WithSubject(subject string) *subjectOpt {
	return &subjectOpt{value: subject}
}

func WithData(v any) EventOpt {
	return eventOptFn(func(evt *Event) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		evt.Data = string(data)
		return nil
	})
}

func WithQueue(queueName string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.QueueName = queueName
		return WithType(Queue).configureConsumer(c)
	})
}

func WithType(t ConsumerType) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		if c.Type != 0 {
			return fmt.Errorf("consumer type is already set to %s", c.Type)
		}
		c.Type = t
		return nil
	})
}

func WithFromOldest() ConsumerOpt {
	return WithFromEventId(Oldest)
}

func WithFromNewest() ConsumerOpt {
	return WithFromEventId(Newest)
}

func WithFromEventId(eventId string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.LastEventId = eventId
		return nil
	})
}

func WithExpiresAt(duration time.Duration) EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.ExpiresAt = time.Now().Add(duration)
		return nil
	})
}

func WithId(id string) ConsumerOpt {
	return consumerOptFn(func(c *Consumer) error {
		c.Id = id
		return WithType(Durable).configureConsumer(c)
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
		evt.Reply = fmt.Sprintf("inbox.%s", newID(""))
		return nil
	})
}

func WithConfirm(n int64) EventOpt {
	return eventOptFn(func(evt *Event) error {
		evt.Reply = fmt.Sprintf("confirm.%s", newID(""))
		evt.ReplyCount = n
		return nil
	})
}

// WithInitAck is an option to initialize the acker for the events
// NOTE: This option is only used for the client and should not be used
// directly by the user
func WithInitAck(consumerId string, replyConfirms map[string]struct{}, putter Putter, acker Acker) MsgOpt {
	return msgOptFn(func(m *Msg) error {
		m.consumerId = consumerId
		m.replyConfirms = replyConfirms
		m.putter = putter
		m.acker = acker
		return nil
	})
}

//
// EVENTS
//

type Event struct {
	Id         string    `json:"id"`
	Subject    string    `json:"subject"`
	Reply      string    `json:"reply_to"`
	ReplyCount int64     `json:"reply_count"`
	Data       string    `json:"data"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

func NewEvent(opts ...EventOpt) (*Event, error) {
	evt := &Event{
		ExpiresAt: GetDefaultExpiresAt(),
	}

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

	consumerId    string
	acker         Acker
	putter        Putter
	replyConfirms map[string]struct{}
}

func (m *Msg) Ack(ctx context.Context) error {
	if m.acker == nil || len(m.Events) == 0 {
		return nil
	}

	err := m.acker.Ack(ctx, m.consumerId, m.Events[len(m.Events)-1].Id)
	if err != nil {
		return err
	}

	confirmMsg := struct {
		Type string `json:"type"`
	}{
		Type: "confirm",
	}

	for replySubject := range m.replyConfirms {
		confirmEvent, err := NewEvent(
			WithSubject(replySubject),
			WithData(confirmMsg),
			WithExpiresAt(30*time.Second),
		)
		if err != nil {
			return err
		}

		err = m.putter.Put(ctx, confirmEvent)
		if err != nil {
			return err
		}
	}

	return nil
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
	Ephemeral
	Durable
	Queue
)

func (t ConsumerType) String() string {
	switch t {
	case Ephemeral:
		return "ephemeral"
	case Durable:
		return "durable"
	case Queue:
		return "queue"
	default:
		return "unknown"
	}
}

func ParseConsumerType(s string, defaultType ConsumerType) ConsumerType {
	switch s {
	case "ephemeral":
		return Ephemeral
	case "durable":
		return Durable
	case "queue":
		return Queue
	default:
		return defaultType
	}
}

func ParseBatchSize(s string, defaultSize int64) int64 {
	if s == "" {
		return defaultSize
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultSize
	}

	return i
}

type Consumer struct {
	Id          string       `json:"id"`
	Subject     string       `json:"subject"`
	Type        ConsumerType `json:"type"`
	Online      bool         `json:"online"`
	QueueName   string       `json:"queue_name"`
	BatchSize   int64        `json:"batch_size"`
	AckedCount  int64        `json:"acked_count"`
	LastEventId string       `json:"last_event_id"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

func NewConsumer(opts ...ConsumerOpt) (*Consumer, error) {
	c := &Consumer{}

	for _, opt := range opts {
		err := opt.configureConsumer(c)
		if err != nil {
			return nil, err
		}
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 1
	}

	if c.Type == 0 {
		c.Type = Ephemeral
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

func newID(prefix string) string {
	id := xid.New()
	if prefix == "" {
		return id.String()
	}
	var sb strings.Builder
	sb.WriteString(prefix)
	sb.WriteString(id.String())
	return sb.String()
}
