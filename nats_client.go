package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrFailedConfirmCounts = errors.New("failed to confirm counts")
)

const (
	natsConfirmHeaderKey       = "bus-confirm-subject"
	natsReplyHeaderKey         = "bus-reply-subject"
	natsRequestReplyStreamName = "bus-request-reply"
	natsRequestSubjectPrefix   = "bus-request."
	natsRequestSubject         = natsRequestSubjectPrefix + ">"
	natsReplySubjectPrefix     = "bus-reply."
	natsReplySubject           = natsReplySubjectPrefix + ">"
	natsConfirmStreamName      = "bus-confirm-stream"
	natsConfirmSubjectPrefix   = "bus-confirm."
	natsConfirmSubject         = natsConfirmSubjectPrefix + ">"
)

type NatsClient struct {
	conn              *nats.Conn
	js                jetstream.JetStream
	confirmStream     jetstream.Stream
	stream            jetstream.Stream
	reqestReplyStream jetstream.Stream
}

var _ Bus = (*NatsClient)(nil)

func (n *NatsClient) Put(ctx context.Context, opts ...PutOpt) error {
	opt := &putOpt{}
	for _, fn := range opts {
		if err := fn.configurePut(opt); err != nil {
			return err
		}
	}

	var data []byte
	var err error
	var confirmConsumer jetstream.Consumer

	switch v := opt.data.(type) {
	case []byte:
		data = v
	default:
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}
	}

	msg := nats.NewMsg(opt.subject)

	if opt.replySubject != "" {
		msg.Header.Add(natsReplyHeaderKey, opt.replySubject)
	}

	if opt.confirmCount > 0 {
		confirmConsumerName := newID("confirm-consumer-")
		confirmSubject := newID(natsConfirmSubjectPrefix)

		msg.Header.Add(natsConfirmHeaderKey, confirmSubject)

		confirmConsumer, err = n.confirmStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          confirmConsumerName,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			FilterSubject: confirmSubject,
		})
		if err != nil {
			return err
		}

		defer func() {
			err = n.confirmStream.DeleteConsumer(ctx, confirmConsumerName)
			if err != nil {
				slog.Error("failed to delete consumer", "name", confirmConsumerName, "error", err)
				return
			}
		}()
	}

	msg.Data = data

	pubAck, err := n.js.PublishMsg(ctx, msg)
	if err != nil {
		return err
	}

	_ = pubAck

	if confirmConsumer != nil {
		msgs, err := confirmConsumer.Fetch(int(opt.confirmCount), jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			return err
		}

		i := 0
		for m := range msgs.Messages() {
			i++
			m.Ack()
		}

		if i != int(opt.confirmCount) {
			return ErrFailedConfirmCounts
		}
	}

	return nil
}

func (n *NatsClient) Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error] {
	opt := &getOpt{}
	for _, fn := range opts {
		if err := fn.configureGet(opt); err != nil {
			return newIterErr[*Event](err)
		}
	}

	var durableName string
	var name string

	if opt.name == "" {
		name = newID("consumer-")
	} else {
		durableName = opt.name
		name = opt.name
	}

	var deliverPolicy jetstream.DeliverPolicy

	switch opt.position {
	case Oldest:
		deliverPolicy = jetstream.DeliverAllPolicy
	case Newest:
		deliverPolicy = jetstream.DeliverNewPolicy
	default:
		return newIterErr[*Event](errors.New("invalid position"))
	}

	consumer, err := n.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:            name,
		Durable:         durableName,
		FilterSubject:   opt.subject,
		MaxRequestBatch: opt.batchSize,
		DeliverPolicy:   deliverPolicy,
	})
	if err != nil {
		return newIterErr[*Event](err)
	}

	return func(yield func(*Event, error) bool) {
		if durableName == "" {
			defer func() {
				err = n.stream.DeleteConsumer(ctx, name)
				if err != nil {
					slog.Error("failed to delete consumer", "name", name, "error", err)
					return
				}
			}()
		}

		for {
			msgs, err := consumer.Fetch(opt.batchSize, jetstream.FetchMaxWait(5*time.Second))
			if errors.Is(err, jetstream.ErrNoMessages) {
				continue
			} else if err != nil {
				yield(nil, err)
				return
			}

			for m := range msgs.Messages() {
				headers := m.Headers()

				confirmSubject := headers.Get(natsConfirmHeaderKey)

				if !yield(&Event{
					Subject:      m.Subject(),
					Data:         m.Data(),
					ReplySubject: headers.Get(natsReplyHeaderKey),
					ackFn: func() error {
						err = m.Ack()
						if err != nil {
							return err
						}

						if confirmSubject != "" {
							err = n.conn.Publish(confirmSubject, nil)
							if err != nil {
								return err
							}
						}

						return nil
					},
				}, nil) {
					return
				}
			}
		}
	}
}

func (n *NatsClient) Request(ctx context.Context, subject string, data any) (json.RawMessage, error) {
	requestSubject := fmt.Sprintf("%s%s", natsRequestSubjectPrefix, subject)
	replySubject := newID(natsReplySubjectPrefix)

	var payload []byte
	switch v := data.(type) {
	case []byte:
		payload = v
	default:
		var err error
		payload, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
	}

	replyConsumerName := newID("reply-consumer-")

	consumer, err := n.reqestReplyStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          replyConsumerName,
		FilterSubject: replySubject,
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		err = n.reqestReplyStream.DeleteConsumer(ctx, replyConsumerName)
		if err != nil {
			slog.Error("failed to delete consumer", "name", replyConsumerName, "error", err)
			return
		}
	}()

	msg := nats.NewMsg(requestSubject)
	msg.Header.Add(natsReplyHeaderKey, replySubject)
	msg.Data = payload

	_, err = n.js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, err
	}

	for {
		// Note Since consumer.Fetch doesn't support context, we need to check the context manually
		// this has a small delay which is around 5s (jetstream.FetchMaxWait), but it's fine for now
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			return nil, err
		}

		result, ok := <-msgs.Messages()
		if !ok {
			continue
		}

		// eventthough a defer called in a for loop, but
		// once the code block gets here, for loop will be break and the defer will be called
		// so it is safe here
		defer result.Ack()

		reply := struct {
			Type string
			Data json.RawMessage
		}{}

		err = json.Unmarshal(result.Data(), &reply)
		if err != nil {
			return nil, err
		}

		if reply.Type == "error" {
			return nil, fmt.Errorf("%s", reply.Data)
		}

		return reply.Data, nil
	}
}

func (n *NatsClient) Reply(ctx context.Context, subject string, fn func(ctx context.Context, req json.RawMessage) (out any, err error)) error {
	requestSubject := fmt.Sprintf("%s%s", natsRequestSubjectPrefix, subject)

	replyConsumerName := newID("reply-consumer-")

	consumer, err := n.reqestReplyStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          replyConsumerName,
		FilterSubject: requestSubject,
	})
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			err := n.reqestReplyStream.DeleteConsumer(ctx, replyConsumerName)
			if err != nil {
				slog.Error("failed to delete consumer", "name", replyConsumerName, "error", err)
				return
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if errors.Is(err, jetstream.ErrNoMessages) {
				continue
			} else if errors.Is(err, nats.ErrConnectionDraining) {
				return
			} else if err != nil {
				slog.Error("failed to fetch message", "error", err)
				return
			}

			msg, ok := <-msgs.Messages()
			if !ok {
				continue
			}

			err = msg.Ack()
			if err != nil {
				slog.Error("failed to ack message", "error", err)
				return
			}

			replySubject := msg.Headers().Get(natsReplyHeaderKey)

			result := struct {
				Type string
				Data any
			}{}

			value, err := fn(ctx, msg.Data())
			if err != nil {
				result.Type = "error"
				result.Data = err.Error()
			} else {
				result.Type = "success"
				result.Data = value
			}

			data, err := json.Marshal(result)
			if err != nil {
				slog.Error("failed to marshal data", "error", err)
				return
			}

			_, err = n.js.Publish(ctx, replySubject, data)
			if err != nil {
				slog.Error("failed to publish data", "error", err)
				return
			}
		}
	}()

	return nil
}

func (n *NatsClient) Close() error {
	return n.conn.Drain()
}

type natsOpt struct {
	url        string
	streamName string
	subjects   []string
}

type NatsOpt interface {
	configureNats(*natsOpt) error
}

type natsOptFunc func(*natsOpt) error

func (f natsOptFunc) configureNats(c *natsOpt) error {
	return f(c)
}

func WithNatsURL(url string) NatsOpt {
	return natsOptFunc(func(n *natsOpt) error {
		n.url = url
		return nil
	})
}

func WithNatsStream(name string, subjects ...string) NatsOpt {
	return natsOptFunc(func(n *natsOpt) error {
		n.streamName = name
		n.subjects = subjects
		return nil
	})
}

func NewNatsClient(ctx context.Context, natsOpts ...NatsOpt) (*NatsClient, error) {
	opts := &natsOpt{}
	for _, fn := range natsOpts {
		if err := fn.configureNats(opts); err != nil {
			return nil, err
		}
	}

	nc, err := nats.Connect(opts.url)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	confirmStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        natsConfirmStreamName,
		Description: "Stream for confirm message",
		Subjects:    []string{natsConfirmSubject},
		MaxAge:      10 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        opts.streamName,
		Description: "Stream for all events",
		Subjects:    opts.subjects,
	})
	if err != nil {
		return nil, err
	}

	reqestReplyStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: natsRequestReplyStreamName,
		Subjects: []string{
			natsRequestSubject,
			natsReplySubject,
		},
		Description: "Stream for request reply",
		Retention:   jetstream.WorkQueuePolicy,
	})
	if err != nil {
		return nil, err
	}

	return &NatsClient{
		conn:              nc,
		js:                js,
		confirmStream:     confirmStream,
		stream:            stream,
		reqestReplyStream: reqestReplyStream,
	}, nil
}
