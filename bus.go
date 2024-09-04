package bus

import (
	"context"
	"encoding/json"
	"iter"
	"strings"

	"github.com/rs/xid"
)

type Position int

const (
	Newest Position = iota
	Oldest
)

type Event struct {
	Subject      string
	ReplySubject string
	Data         json.RawMessage

	replyFn func(data []byte) error
	ackFn   func() error
}

func (e *Event) Reply(v any) error {
	if e.replyFn != nil {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return e.replyFn(data)
	}

	return nil
}

func (e *Event) Ack() error {
	if e.ackFn != nil {
		return e.ackFn()
	}
	return nil
}

func (e *Event) Unmarshal(v any) error {
	return json.Unmarshal(e.Data, v)
}

type Putter interface {
	Put(ctx context.Context, opts ...PutOpt) error
}

type Getter interface {
	Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error]
}

type ReplyFunc func(ctx context.Context, req json.RawMessage) (any, error)

type RequestReplier interface {
	Request(ctx context.Context, subject string, data any) (json.RawMessage, error)
	Reply(ctx context.Context, subject string, fn ReplyFunc) error
}

// options

type putOpt struct {
	subject      string
	data         any
	confirmCount int
	replySubject string
}

type PutOpt interface {
	configurePut(*putOpt) error
}

type putOptFunc func(*putOpt) error

func (f putOptFunc) configurePut(p *putOpt) error {
	return f(p)
}

type getOpt struct {
	name      string
	subject   string
	queueName string // not implemented yet
	batchSize int
	position  Position
}

type GetOpt interface {
	configureGet(*getOpt) error
}

type getOptFunc func(*getOpt) error

func (f getOptFunc) configureGet(c *getOpt) error {
	return f(c)
}

type subjectOpt string

var _ PutOpt = (*subjectOpt)(nil)
var _ GetOpt = (*subjectOpt)(nil)

func (s subjectOpt) configurePut(p *putOpt) error {
	p.subject = string(s)
	return nil
}

func (s subjectOpt) configureGet(g *getOpt) error {
	g.subject = string(s)
	return nil
}

func WithSubject(subject string) subjectOpt {
	return subjectOpt(subject)
}

func WithName(name string) GetOpt {
	return getOptFunc(func(g *getOpt) error {
		g.name = name
		return nil
	})
}

func WithQueueName(queueName string) GetOpt {
	return getOptFunc(func(g *getOpt) error {
		g.queueName = queueName
		return nil
	})
}

func WithData(data any) PutOpt {
	return putOptFunc(func(p *putOpt) error {
		p.data = data
		return nil
	})
}

func WithReply(replySubject string) PutOpt {
	return putOptFunc(func(p *putOpt) error {
		p.replySubject = replySubject
		return nil
	})
}

func WithConfirm(confirmCount int) PutOpt {
	return putOptFunc(func(p *putOpt) error {
		p.confirmCount = confirmCount
		return nil
	})
}

func WithBatchSize(batchSize int) GetOpt {
	return getOptFunc(func(g *getOpt) error {
		g.batchSize = batchSize
		return nil
	})
}

func WithFromNewest() GetOpt {
	return getOptFunc(func(g *getOpt) error {
		g.position = Newest
		return nil
	})
}

func WithFromOldest() GetOpt {
	return getOptFunc(func(g *getOpt) error {
		g.position = Oldest
		return nil
	})
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

func newIterErr[T any](err error) iter.Seq2[T, error] {
	var defaultValue T
	return func(yield func(T, error) bool) {
		yield(defaultValue, err)
	}
}
