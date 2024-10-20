package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"
	"unicode"

	"ella.to/sse"
	"github.com/rs/xid"
)

type Response json.RawMessage

func (r Response) Error() error {
	if len(r) == 0 || r[0] == '{' || r[0] == '[' {
		return nil
	}

	return fmt.Errorf("%s", r)
}

func (r Response) Parse(ptr any) error {
	return json.Unmarshal(r, ptr)
}

//
// Putter
//

type putOpt struct {
	event        Event
	confirmCount int
}

type PutOpt interface {
	configurePut(*putOpt) error
}

type PutOptFunc func(*putOpt) error

func (f PutOptFunc) configurePut(p *putOpt) error {
	return f(p)
}

type Putter interface {
	Put(ctx context.Context, opts ...PutOpt) (Response, error)
}

//
// Getter
//

type getOpt struct {
	consumer Consumer
}

// GetOpt is an interface that can be used to configure the Get operation
type GetOpt interface {
	configureGet(*getOpt) error
}

type GetOptFunc func(*getOpt) error

func (f GetOptFunc) configureGet(g *getOpt) error {
	return f(g)
}

// Getter is an interface that can be used to get events from the bus
type Getter interface {
	Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error]
}

//
// Acker
//

// Acker is an interface that can be used to acknowledge the event
type Acker interface {
	Ack(ctx context.Context, consumerId string, eventId string) error
}

type ackOpt struct {
	payload json.RawMessage
}

// AckOpt is an interface that can be used to configure the Ack operation
type AckOpt interface {
	configureAck(*ackOpt) error
}

//
// Event
//

type Event struct {
	Id              string          `json:"id"`
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       time.Time       `json:"created_at"`

	// consumerId is the is of the consumer that processed this event.
	// It is set by the bus client when the event is reached the consumer
	consumerId string
	acker      Acker
	putter     Putter
	// index, size are some information that can be used for keeping track of where the event is
	index int64
	size  int64
}

func (e *Event) String() string {
	typ := "EVT"

	if strings.HasPrefix(e.Subject, "_inbox_.") {
		typ = "RSP"
	} else if strings.HasPrefix(e.ResponseSubject, "_inbox_.") {
		typ = "REQ"
	}

	return fmt.Sprintf("%s: [%s] %s -> %s", e.CreatedAt.Format(time.RFC3339), typ, e.Subject, e.Payload)
}

func (e *Event) encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(e)
}

func (e *Event) decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(e)
}

func (e *Event) validate() error {
	// subject is required
	if e.Subject == "" {
		return errors.New("subject is required")
	}

	// subject must be in a form of "a.b.c"
	if strings.Contains(e.Subject, "*") || strings.Contains(e.Subject, ">") {
		return errors.New("subject should not have * or >")
	}

	// simple validation for response subject
	if e.ResponseSubject != "" {
		if strings.Contains(e.ResponseSubject, "*") || strings.Contains(e.ResponseSubject, ">") {
			return errors.New("response subject should not have * or >")
		}
	}

	return nil
}

// Ack acknowledges the event, indicating that the consumer has processed it.
// if the event contains a response subject, then it will be used to send a response
// response can be nil or any type which will be marshaled to JSON.
// use WithData to set the response
func (e *Event) Ack(ctx context.Context, opts ...AckOpt) error {
	if e.consumerId == "" {
		return nil
	}

	if e.ResponseSubject != "" {
		putOpts := []PutOpt{
			WithSubject(e.ResponseSubject),
		}

		for _, opt := range opts {
			if o, ok := opt.(PutOpt); ok {
				putOpts = append(putOpts, o)
			}
		}

		_, err := e.putter.Put(ctx, putOpts...)
		if err != nil {
			return err
		}
	}

	return e.acker.Ack(ctx, e.consumerId, e.Id)
}

//
// Consumer
//

const (
	DefaultCheckDelay      = 50 * time.Millisecond
	DefaultRedeliveryDelay = 5 * time.Second
)

type ConsumerMeta struct {
	// name is the name of the consumer, if it's provided, the consumer will be durable
	// if multiple consumers with the same name, then the it forms a consumer group
	Name string `json:"name"`
	// subject is required and should be in a form of "a.b.c"
	// it can have wildcards, "*" and ">"
	Subject string `json:"subject"`
	// oldest, newest, or event ID, if empty, it will be set to newest
	// once this value is set, we will never change it
	Position string `json:"position"`
	// count the number of events that the consumer has processed
	// this is usefull when an event needs to be sent to group of consumers
	// and the least acked consumer will be selected
	AckedCount int64 `json:"acked_count"`
	// current index in events log file
	// - if Position is newest, then this will be the index of the last event (size of the Events Log file)
	// - if Position is oldest, then this will be set to 0
	// - if Position is an event ID, then this will be set to 0, however, for every event, it will be incremented and also checked if the current event id is equal to
	// and then it will send that event and all the event after, so basically if event.Id >= Position then send the event
	//
	CurrentIndex int64 `json:"curr_event_index"`
	// current size of the event, this is used to advance the CurrentIndex to the next event
	// once the event is acked
	CurrentEventSize int64 `json:"curr_event_size"`
	// waiting ack for is the event id that the consumer is waiting for
	// if it's empty, it means that consumer is not waiting for any ack
	WaitingAckFor       string    `json:"waiting_ack_for"`
	WaitingAckExpiredAt time.Time `json:"waiting_ack_expired_at"`
	// check delay is the time that the consumer will wait before checking the if there is any event
	// the default is 50 milliseconds
	CheckDelay time.Duration `json:"check_delay"`
	// redelivery delay is the time that the consumer will wait before redelivering the event
	// the default is 5 seconds
	RedeliveryDelay time.Duration `json:"redelivery_delay"`
}

func (c *ConsumerMeta) validate() error {
	// subject is required
	if err := validateConsumerSubject(c.Subject); err != nil {
		return err
	}

	// simple validation for position
	if c.Position != "" {
		switch c.Position {
		case "":
			c.Position = "newest"
		case "newest", "oldest":
			// we are good here
		default:
			// this is a simple validation for event ID
			if !strings.HasPrefix(c.Position, "e_") {
				return errors.New("invalid position")
			}
		}
	}

	return nil
}

func (c *ConsumerMeta) encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(c)
}

func (c *ConsumerMeta) decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(c)
}

type Consumer struct {
	id      string
	meta    ConsumerMeta
	autoAck bool
	// pusher is the pusher that will be used to push events to the consumer
	// using server sent events protocol
	pusher sse.Pusher
	// forceDisconnect is a function that can be used to force disconnect the consumer
	// it should be set by http handler to setup context.WithCancel
	forceDisconnect func()
}

//
// Options
// options are utility functions which can be used to configure the Putter and Getter

// Subject

type subjectOpt string

var _ PutOpt = (*subjectOpt)(nil)
var _ GetOpt = (*subjectOpt)(nil)

func (s subjectOpt) configurePut(p *putOpt) error {
	if p.event.Subject != "" {
		return errors.New("subject already set")
	}

	p.event.Subject = string(s)

	// should not have * or >
	if strings.Contains(string(s), "*") || strings.Contains(string(s), ">") {
		return errors.New("subject should not have * or >")
	}

	return nil
}

func (s subjectOpt) configureGet(g *getOpt) error {
	if g.consumer.meta.Subject != "" {
		return errors.New("subject already set")
	}

	// should not starts with * or >
	if strings.HasPrefix(string(s), "*") || strings.HasPrefix(string(s), ">") {
		return errors.New("subject should not starts with * or >")
	}

	// should not have anything after >
	if strings.Contains(string(s), ">") && !strings.HasSuffix(string(s), ">") {
		return errors.New("subject should not have anything after >")
	}

	g.consumer.meta.Subject = string(s)
	return nil
}

// WithSubject sets the subject of the event and consumer
func WithSubject(subject string) subjectOpt {
	return subjectOpt(subject)
}

// Update the internal state of the consumer to how frequent it should check for new events
// if the consumer is durable, the consumer who has created the queue will enforce this
// the default value is set by DefaultCheckDelay constant
func WithCheckDelay(delay time.Duration) GetOptFunc {
	return func(g *getOpt) error {
		if g.consumer.meta.CheckDelay != 0 {
			return errors.New("check delay already set")
		}

		g.consumer.meta.CheckDelay = delay
		return nil
	}
}

func WithRedeliveryDelay(delay time.Duration) GetOptFunc {
	return func(g *getOpt) error {
		if g.consumer.meta.RedeliveryDelay != 0 {
			return errors.New("redelivery delay already set")
		}

		g.consumer.meta.RedeliveryDelay = delay
		return nil
	}
}

// Name

// WithName sets the name of the consumer which makes it durable
// if multiple consumers with the same name, then the it forms a consumer group
// and round-robin distribution will be applied
func WithName(name string) GetOptFunc {
	return func(p *getOpt) error {
		if p.consumer.meta.Name != "" {
			return errors.New("consumer's name already set")
		}

		p.consumer.meta.Name = name
		return nil
	}
}

// Payload

type dataOpt struct{ value any }

func (d *dataOpt) configurePut(p *putOpt) error {
	if p.event.Payload != nil {
		return errors.New("event's payload already set")
	}

	// how to encode error value ?? {error: "error message"} or simple string?????

	data, err := json.Marshal(d.value)
	if err != nil {
		return err
	}

	p.event.Payload = json.RawMessage(data)
	return nil
}

func (d *dataOpt) configureAck(a *ackOpt) error {
	if a.payload != nil {
		return errors.New("payload already set")
	}

	data, err := json.Marshal(d.value)
	if err != nil {
		return err
	}

	a.payload = json.RawMessage(data)
	return nil
}

func WithData(data any) *dataOpt {
	return &dataOpt{data}
}

// Position

func WithNewestPosition() GetOpt {
	return WithCustomPosition("newest")
}

func WithOldestPosition() GetOpt {
	return WithCustomPosition("oldest")
}

func WithCustomPosition(pos string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if pos == "" {
			pos = "newest"
		}

		if pos != "newest" && pos != "oldest" && !strings.HasPrefix(pos, "e_") {
			return errors.New("invalid position")
		}

		g.consumer.meta.Position = pos
		return nil
	})
}

// Confirm

func WithConfirm(count int) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.confirmCount != 0 || p.event.ResponseSubject != "" {
			return errors.New("confirm count or response subject already set")
		}

		p.confirmCount = count
		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
}

// utils

func newInboxSubject() string {
	return newId("_inbox_.")
}

func newEventId() string {
	return newId("e_")
}

func newConsumerId() string {
	return newId("c_")
}

func newId(prefix string) string {
	var sb strings.Builder

	sb.WriteString(prefix)
	sb.WriteString(xid.New().String())

	return sb.String()
}

func validateConsumerSubject(subject string) error {
	// subject must be in a form of "a.b.c", "a.b.*", "a.b.>"
	// - subject should not have > at the middle of the string
	// - subject should not have spaces or any other characters other than alphanumerics, dots, *, and >
	// - * should have a dot or nothing before and after it
	// - > should have a dot or nothing before it and nothing after it
	// - subject should not starts with .
	// - subject should not ends with .
	// - subject should not have .. in the middle of the string

	if subject == "" {
		return errors.New("subject is empty")
	}

	for i, c := range subject {
		if i == 0 && c == '.' {
			return errors.New("subject should not starts with .")
		}

		if i == len(subject)-1 && c == '.' {
			return errors.New("subject should not ends with .")
		}

		if c == ' ' {
			return errors.New("subject should not have spaces")
		}

		if i > 0 && c == '.' && subject[i-1] == '.' {
			return errors.New("subject should not have series of dots one after another")
		}

		if c == '>' && i != len(subject)-1 {
			return errors.New("subject should not have anything after >")
		}

		if c == '>' && i > 0 && subject[i-1] != '.' {
			return errors.New("subject should have a dot before >")
		}

		if c == '*' {
			if i > 0 && subject[i-1] != '.' {
				return errors.New("subject should have a dot before *")
			}

			if i != len(subject)-1 && subject[i+1] != '.' {
				return errors.New("subject should have a dot after *")
			}
		}

		if c != '.' && c != '*' && c != '>' && !unicode.IsDigit(c) && !unicode.IsLetter(c) {
			return errors.New("subject should have only consists of alphanumerics, dots, *, and >")
		}
	}

	return nil
}
