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
)

var (
	Version   = "dev"
	GitCommit = ""
)

//
// Event
//

type Event struct {
	Id              string          `json:"id"`
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       time.Time       `json:"created_at"`
	Index           int64           `json:"index"`

	// for internal use
	consumerId string
	acker      Acker
	putter     Putter
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

func (e *Event) Ack(ctx context.Context, opts ...AckOpt) error {
	if err := e.acker.Ack(ctx, e.consumerId, e.Id); err != nil {
		return fmt.Errorf("failed to ack event: %w", err)
	}

	if e.ResponseSubject == "" {
		return nil
	}

	var putOpts = []PutOpt{
		WithSubject(e.ResponseSubject),
	}

	for _, opt := range opts {
		if o, ok := opt.(PutOpt); ok {
			putOpts = append(putOpts, o)
		}
	}

	if err := e.putter.Put(ctx, putOpts...).Error(); err != nil {
		return fmt.Errorf("failed to send response: %w", err)
	}

	return nil
}

const (
	AckManual = "manual" // client should ack the event
	AckNone   = "none"   // no need to ack and server push the event to the client as fast as possible
)

const (
	StartOldest = "oldest"
	StartNewest = "newest"
)

const (
	DefaultAck        = AckNone
	DefaultStart      = StartNewest
	DefaultRedelivery = 5 * time.Second
)

//
// Putter
//

type Response struct {
	err       error
	Id        string
	Index     int64
	CreatedAt time.Time
	Payload   json.RawMessage
}

func (s *Response) String() string {
	var sb strings.Builder

	sb.WriteString("id: ")
	sb.WriteString(s.Id)
	if s.Index != -1 {
		sb.WriteString(", index: ")
		sb.WriteString(fmt.Sprintf("%d", s.Index))
	}
	sb.WriteString(", created_at: ")
	sb.WriteString(s.CreatedAt.Format(time.RFC3339Nano))

	return sb.String()
}

func (r *Response) Error() error {
	if r.err != nil {
		return r.err
	}

	if len(r.Payload) == 0 || r.Payload[0] == '{' || r.Payload[0] == '[' {
		return nil
	}

	return fmt.Errorf("%s", r.Payload)
}

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
	Put(ctx context.Context, opts ...PutOpt) *Response
}

//
// Getter
//

type getOpt struct {
	subject     string
	ackStrategy string
	redelivery  time.Duration
	start       string
	metaFn      func(map[string]string)
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

type ackOpt struct {
	payload json.RawMessage
}

// AckOpt is an interface that can be used to configure the Ack operation
type AckOpt interface {
	configureAck(*ackOpt) error
}

// Acker is an interface that can be used to acknowledge the event
type Acker interface {
	Ack(ctx context.Context, consumerId string, eventId string) error
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
	if g.subject != "" {
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

	g.subject = string(s)
	return nil
}

// WithSubject sets the subject of the event and consumer
func WithSubject(subject string) subjectOpt {
	return subjectOpt(subject)
}

func WithStartFrom(start string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if start != StartOldest && start != StartNewest && !strings.HasPrefix(start, "e_") {
			return errors.New("invalid start from")
		}

		g.start = start
		return nil
	})
}

func WithDelivery(duration time.Duration) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if duration < 0 {
			return errors.New("delivery duration should be greater than 0")
		}

		g.redelivery = duration
		return nil
	})
}

func WithAckStrategy(strategy string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if strategy != AckManual && strategy != AckNone {
			return errors.New("invalid ack strategy")
		}

		g.ackStrategy = strategy
		return nil
	})
}

func WithExtractMeta(fn func(map[string]string)) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if g.metaFn != nil {
			return errors.New("meta function already set")
		}

		g.metaFn = fn
		return nil
	})
}

func WithConfirm(n int) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if n < 0 {
			return errors.New("confirm count should be greater than 0")
		}

		if p.event.ResponseSubject != "" {
			return errors.New("response subject already set")
		}

		p.confirmCount = n
		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
}

func WithRequestReply() PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.confirmCount != 0 {
			return errors.New("confirm count already set")
		}

		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
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
