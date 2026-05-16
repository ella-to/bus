package bus

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// HTTP-style headers carried inside every NATS message so that consumers can
// reconstruct the bus.Event without unwrapping a JSON envelope. Keeping the
// payload plain (i.e. msg.Data == event.Payload) makes request/reply cleanly
// interoperable with non-bus NATS clients.
const (
	headerEventId         = "Bus-Event-Id"
	headerTraceId         = "Bus-Trace-Id"
	headerKey             = "Bus-Key"
	headerResponseSubject = "Bus-Response-Subject"
	headerCreatedAt       = "Bus-Created-At"
	headerSubject         = "Bus-Subject"
)

// streamPrefix is prepended to the namespace to derive the JetStream stream
// name (so namespace "orders" lives in stream "BUS_orders").
const streamPrefix = "BUS_"

// ---------------------------------------------------------------------------
// Client construction
// ---------------------------------------------------------------------------

// Client is a NATS-backed implementation of the Putter / Getter / Acker
// interfaces. A single Client wraps one nats.Conn and lazily creates
// per-namespace JetStream streams the first time they are touched.
type Client struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	ownsConn bool

	// Per-namespace stream guard. We don't need to remember the stream
	// definition itself, only that it has already been ensured this
	// process lifetime.
	mu       sync.Mutex
	streams  map[string]struct{}
	streamFn func(namespace string, cfg *jetstream.StreamConfig)

	defaultStreamConfig jetstream.StreamConfig
}

type clientConfig struct {
	natsOpts            []nats.Option
	conn                *nats.Conn
	streamFn            func(namespace string, cfg *jetstream.StreamConfig)
	defaultStreamConfig jetstream.StreamConfig
}

// ClientOpt configures a Client at construction time.
type ClientOpt interface {
	configureClient(*clientConfig) error
}

type clientOptFunc func(*clientConfig) error

func (f clientOptFunc) configureClient(c *clientConfig) error { return f(c) }

// WithNatsOptions appends nats.Option values used when dialing the server.
func WithNatsOptions(opts ...nats.Option) ClientOpt {
	return clientOptFunc(func(c *clientConfig) error {
		c.natsOpts = append(c.natsOpts, opts...)
		return nil
	})
}

// WithNatsConn reuses an already-connected nats.Conn instead of dialing one.
// The Client does not take ownership of the connection: callers must close
// it themselves.
func WithNatsConn(nc *nats.Conn) ClientOpt {
	return clientOptFunc(func(c *clientConfig) error {
		if nc == nil {
			return errors.New("nats conn cannot be nil")
		}
		c.conn = nc
		return nil
	})
}

// WithStreamConfigurer installs a hook called every time the Client is about
// to ensure a JetStream stream. The hook can mutate the stream config (for
// example to set MaxAge, MaxBytes, replicas, or memory storage).
func WithStreamConfigurer(fn func(namespace string, cfg *jetstream.StreamConfig)) ClientOpt {
	return clientOptFunc(func(c *clientConfig) error {
		c.streamFn = fn
		return nil
	})
}

// WithDefaultStreamConfig sets a baseline StreamConfig that will be cloned
// for every namespace before WithStreamConfigurer is invoked. Subjects and
// Name are always overwritten by the bus.
func WithDefaultStreamConfig(cfg jetstream.StreamConfig) ClientOpt {
	return clientOptFunc(func(c *clientConfig) error {
		c.defaultStreamConfig = cfg
		return nil
	})
}

// NewClient connects to NATS at url and returns a ready-to-use Client. The
// connection is established eagerly so that misconfigurations surface
// immediately.
func NewClient(url string, opts ...ClientOpt) (*Client, error) {
	cfg := &clientConfig{
		defaultStreamConfig: jetstream.StreamConfig{
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
		},
	}
	for _, o := range opts {
		if err := o.configureClient(cfg); err != nil {
			return nil, err
		}
	}

	var (
		nc       *nats.Conn
		ownsConn bool
		err      error
	)

	if cfg.conn != nil {
		nc = cfg.conn
	} else {
		nc, err = nats.Connect(url, cfg.natsOpts...)
		if err != nil {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
		ownsConn = true
	}

	js, err := jetstream.New(nc)
	if err != nil {
		if ownsConn {
			nc.Close()
		}
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	return &Client{
		nc:                  nc,
		js:                  js,
		ownsConn:            ownsConn,
		streams:             make(map[string]struct{}),
		streamFn:            cfg.streamFn,
		defaultStreamConfig: cfg.defaultStreamConfig,
	}, nil
}

// Close drains and closes the underlying NATS connection (when owned).
func (c *Client) Close() error {
	if c.ownsConn && c.nc != nil {
		c.nc.Close()
	}
	return nil
}

// Conn returns the underlying NATS connection. Useful for advanced use
// cases that need to reach beyond the bus API.
func (c *Client) Conn() *nats.Conn { return c.nc }

// JetStream returns the underlying JetStream context.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Compile-time interface assertions.
var (
	_ Putter = (*Client)(nil)
	_ Getter = (*Client)(nil)
	_ Acker  = (*Client)(nil)
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// namespaceOf returns the part of subject before the first dot, treating
// "_bus_.xxx" as the inbox namespace.
func namespaceOf(subject string) string {
	if i := strings.IndexByte(subject, '.'); i >= 0 {
		return subject[:i]
	}
	return subject
}

func isInboxSubject(subject string) bool {
	return namespaceOf(subject) == inboxNamespace
}

func (c *Client) ensureStream(ctx context.Context, namespace string) error {
	if namespace == "" {
		return errors.New("invalid subject: missing namespace")
	}
	if namespace == inboxNamespace {
		// Inbox traffic is delivered via Core NATS, never persisted.
		return nil
	}

	c.mu.Lock()
	if _, ok := c.streams[namespace]; ok {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	cfg := c.defaultStreamConfig
	cfg.Name = streamPrefix + namespace
	cfg.Subjects = []string{namespace + ".>"}
	if c.streamFn != nil {
		c.streamFn(namespace, &cfg)
	}

	if _, err := c.js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure stream %s: %w", cfg.Name, err)
	}

	c.mu.Lock()
	c.streams[namespace] = struct{}{}
	c.mu.Unlock()
	return nil
}

// buildMsg converts an Event into a NATS message with the canonical bus headers.
func buildMsg(ev *Event) *nats.Msg {
	msg := nats.NewMsg(ev.Subject)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(headerEventId, ev.Id)
	msg.Header.Set(nats.MsgIdHdr, ev.Id) // JetStream dedup
	if ev.TraceId != "" {
		msg.Header.Set(headerTraceId, ev.TraceId)
	}
	if ev.Key != "" {
		msg.Header.Set(headerKey, ev.Key)
	}
	if ev.ResponseSubject != "" {
		msg.Header.Set(headerResponseSubject, ev.ResponseSubject)
	}
	msg.Header.Set(headerCreatedAt, ev.CreatedAt.Format(time.RFC3339Nano))
	msg.Data = ev.Payload
	return msg
}

// eventFromCoreMsg builds a bus.Event from a Core NATS delivery (no JS metadata).
func eventFromCoreMsg(m *nats.Msg, c *Client, consumerId string) *Event {
	ev := &Event{
		Subject:         m.Subject,
		Payload:         m.Data,
		Id:              m.Header.Get(headerEventId),
		TraceId:         m.Header.Get(headerTraceId),
		Key:             m.Header.Get(headerKey),
		ResponseSubject: m.Header.Get(headerResponseSubject),
		consumerId:      consumerId,
		putter:          c,
		acker:           noopAcker{},
	}
	if t, err := time.Parse(time.RFC3339Nano, m.Header.Get(headerCreatedAt)); err == nil {
		ev.CreatedAt = t
	}
	if ev.Id == "" {
		// Core-NATS senders that don't speak the bus protocol still get
		// a synthetic id so consumers can rely on it being non-empty.
		ev.Id = newEventId()
	}
	return ev
}

// eventFromJsMsg builds a bus.Event from a JetStream message and wires up an
// acker that defers to that specific message's Ack().
func eventFromJsMsg(m jetstream.Msg, c *Client, consumerId string) *Event {
	hdr := m.Headers()
	ev := &Event{
		Subject:         m.Subject(),
		Payload:         m.Data(),
		Id:              hdr.Get(headerEventId),
		TraceId:         hdr.Get(headerTraceId),
		Key:             hdr.Get(headerKey),
		ResponseSubject: hdr.Get(headerResponseSubject),
		consumerId:      consumerId,
		putter:          c,
		acker:           &jsMsgAcker{msg: m},
	}
	if t, err := time.Parse(time.RFC3339Nano, hdr.Get(headerCreatedAt)); err == nil {
		ev.CreatedAt = t
	}
	if meta, err := m.Metadata(); err == nil && meta != nil {
		ev.Index = int64(meta.Sequence.Stream)
	}
	if ev.Id == "" {
		ev.Id = newEventId()
	}
	return ev
}

// noopAcker is used for Core NATS deliveries (inbox responses) where there
// is nothing to acknowledge.
type noopAcker struct{}

func (noopAcker) Ack(context.Context, string, string) error { return nil }

// jsMsgAcker acks a single JetStream message exactly once.
type jsMsgAcker struct {
	msg  jetstream.Msg
	once sync.Once
	err  error
}

func (a *jsMsgAcker) Ack(_ context.Context, _ string, _ string) error {
	a.once.Do(func() { a.err = a.msg.Ack() })
	return a.err
}

// iterError returns an iter.Seq2 that yields a single (nil, err).
func iterError(err error) iter.Seq2[*Event, error] {
	return func(yield func(*Event, error) bool) { yield(nil, err) }
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

// Put publishes an event (or a batch) and, when applicable, waits for the
// configured number of acknowledgements or for a single reply.
func (c *Client) Put(ctx context.Context, opts ...PutOpt) *Response {
	opt := &putOpt{}
	for _, o := range opts {
		if err := o.configurePut(opt); err != nil {
			return &Response{err: err}
		}
	}

	if opt.hasBatch {
		return c.putBatch(ctx, opt)
	}
	return c.putSingle(ctx, opt)
}

func (c *Client) putBatch(ctx context.Context, opt *putOpt) *Response {
	if opt.event.Subject != "" || opt.event.Key != "" || opt.event.ResponseSubject != "" ||
		opt.event.TraceId != "" || opt.event.Id != "" || !opt.event.CreatedAt.IsZero() ||
		opt.confirmCount != 0 {
		return &Response{err: errors.New("cannot mix batch with other options")}
	}
	if len(opt.batch) == 0 {
		return &Response{err: errors.New("batch has no items")}
	}

	groupNS := namespaceOf(opt.batch[0].Subject)
	if groupNS == "" {
		return &Response{err: errors.New("invalid subject: missing namespace")}
	}
	if groupNS == inboxNamespace {
		return &Response{err: errors.New("batch cannot target the inbox namespace")}
	}

	for i := range opt.batch {
		ev := &opt.batch[i]
		if err := ev.validate(); err != nil {
			return &Response{err: err}
		}
		if namespaceOf(ev.Subject) != groupNS {
			return &Response{err: errors.New("all events in batch must belong to the same namespace")}
		}
	}

	if err := c.ensureStream(ctx, groupNS); err != nil {
		return &Response{err: err}
	}

	for i := range opt.batch {
		ev := &opt.batch[i]
		if ev.Id == "" {
			ev.Id = newEventId()
		}
		if ev.CreatedAt.IsZero() {
			ev.CreatedAt = time.Now().UTC()
		}
		ack, err := c.js.PublishMsg(ctx, buildMsg(ev))
		if err != nil {
			return &Response{err: fmt.Errorf("publish batch item %d: %w", i, err)}
		}
		ev.Index = int64(ack.Sequence)
	}

	return &Response{}
}

func (c *Client) putSingle(ctx context.Context, opt *putOpt) *Response {
	ev := &opt.event
	if err := ev.validate(); err != nil {
		return &Response{err: err}
	}

	ns := namespaceOf(ev.Subject)
	if ns == "" {
		return &Response{err: errors.New("invalid subject: missing namespace")}
	}
	coreOnly := ns == inboxNamespace

	if !coreOnly {
		if err := c.ensureStream(ctx, ns); err != nil {
			return &Response{err: err}
		}
	}

	if ev.Id == "" {
		ev.Id = newEventId()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}

	// Subscribe to the response subject *before* publishing so we don't
	// miss a fast reply.
	var sub *nats.Subscription
	if ev.ResponseSubject != "" {
		var err error
		sub, err = c.nc.SubscribeSync(ev.ResponseSubject)
		if err != nil {
			return &Response{err: fmt.Errorf("subscribe response: %w", err)}
		}
		defer func() { _ = sub.Unsubscribe() }()
	}

	msg := buildMsg(ev)

	var pubIndex int64
	if coreOnly {
		if err := c.nc.PublishMsg(msg); err != nil {
			return &Response{err: fmt.Errorf("publish core: %w", err)}
		}
	} else {
		ack, err := c.js.PublishMsg(ctx, msg)
		if err != nil {
			return &Response{err: fmt.Errorf("publish jetstream: %w", err)}
		}
		pubIndex = int64(ack.Sequence)
		ev.Index = pubIndex
	}

	resp := &Response{
		Id:        ev.Id,
		CreatedAt: ev.CreatedAt,
		Index:     pubIndex,
	}

	if ev.ResponseSubject == "" {
		return resp
	}

	waitingForConfirm := opt.confirmCount > 0
	for {
		m, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			resp.err = err
			return resp
		}
		if !waitingForConfirm {
			resp.Payload = m.Data
			return resp
		}
		opt.confirmCount--
		if opt.confirmCount == 0 {
			return resp
		}
	}
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

// Get subscribes to all events whose subject matches the configured pattern.
// The returned iterator yields (event, nil) for each delivery and (nil, err)
// on a transport error. Closing the context (or breaking out of the loop)
// cleans up the underlying subscription/consumer.
func (c *Client) Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error] {
	opt := &getOpt{}
	for _, o := range opts {
		if err := o.configureGet(opt); err != nil {
			return iterError(err)
		}
	}
	if opt.subject == "" {
		return iterError(errors.New("subject is required"))
	}

	if opt.ackStrategy == "" {
		opt.ackStrategy = DefaultAck
	}
	if opt.start == "" {
		opt.start = DefaultStart
	}
	if opt.redelivery <= 0 {
		opt.redelivery = DefaultRedelivery
	}
	if opt.redeliveryCount == 0 {
		opt.redeliveryCount = DefaultRedeliveryCount
	}

	consumerId := newConsumerId()
	if opt.metaFn != nil {
		opt.metaFn(map[string]string{"consumer-id": consumerId})
	}

	if isInboxSubject(opt.subject) {
		return c.getCore(ctx, opt, consumerId)
	}
	return c.getJetStream(ctx, opt, consumerId)
}

func (c *Client) getCore(ctx context.Context, opt *getOpt, consumerId string) iter.Seq2[*Event, error] {
	return func(yield func(*Event, error) bool) {
		sub, err := c.nc.SubscribeSync(opt.subject)
		if err != nil {
			yield(nil, fmt.Errorf("subscribe core: %w", err))
			return
		}
		defer func() { _ = sub.Unsubscribe() }()

		for {
			m, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if !yield(nil, err) {
					return
				}
				continue
			}
			ev := eventFromCoreMsg(m, c, consumerId)
			if !yield(ev, nil) {
				return
			}
		}
	}
}

func (c *Client) getJetStream(ctx context.Context, opt *getOpt, consumerId string) iter.Seq2[*Event, error] {
	ns := namespaceOf(opt.subject)
	if ns == "" {
		return iterError(errors.New("invalid subject: missing namespace"))
	}
	if err := c.ensureStream(ctx, ns); err != nil {
		return iterError(err)
	}

	cfg := jetstream.ConsumerConfig{
		Name:              consumerId,
		FilterSubject:     opt.subject,
		AckWait:           opt.redelivery,
		InactiveThreshold: 5 * time.Minute,
	}

	switch opt.start {
	case StartOldest:
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	case StartNewest:
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	default:
		// "e_xxx" event id form is best-effort: we currently fall back
		// to "all" so the consumer at least receives history. Callers
		// that need strict positioning should use a numeric sequence.
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	switch opt.ackStrategy {
	case AckManual:
		cfg.AckPolicy = jetstream.AckExplicitPolicy
		// In ella.to/bus, redeliveryCount <= 0 means "redeliver
		// indefinitely". JetStream encodes that as -1.
		if opt.redeliveryCount > 0 {
			cfg.MaxDeliver = opt.redeliveryCount
		} else {
			cfg.MaxDeliver = -1
		}
	default:
		cfg.AckPolicy = jetstream.AckNonePolicy
		cfg.MaxDeliver = -1
	}

	streamName := streamPrefix + ns

	return func(yield func(*Event, error) bool) {
		setupCtx, setupCancel := context.WithTimeout(ctx, 30*time.Second)
		cons, err := c.js.CreateOrUpdateConsumer(setupCtx, streamName, cfg)
		setupCancel()
		if err != nil {
			yield(nil, fmt.Errorf("create consumer: %w", err))
			return
		}

		// Best-effort cleanup of the ephemeral consumer when we're done.
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.js.DeleteConsumer(cleanupCtx, streamName, cfg.Name)
			cancel()
		}()

		msgs, err := cons.Messages()
		if err != nil {
			yield(nil, fmt.Errorf("messages iterator: %w", err))
			return
		}

		// Stop the iterator when ctx is cancelled so Next unblocks.
		stopOnCtx := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				msgs.Stop()
			case <-stopOnCtx:
			}
		}()
		defer func() {
			close(stopOnCtx)
			msgs.Stop()
		}()

		for {
			m, err := msgs.Next()
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					return
				}
				if !yield(nil, err) {
					return
				}
				continue
			}
			ev := eventFromJsMsg(m, c, consumerId)
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Acker
// ---------------------------------------------------------------------------

// Ack on the Client is a no-op kept for interface compatibility. The
// NATS-backed bus acks via the per-message acker stitched onto each Event,
// so the recommended path is to call event.Ack(ctx) from the receive loop.
//
// The (consumerId, eventId) tuple is opaque to NATS — there is no way to
// retroactively look up a delivery from these values once the Event has been
// dropped, so this method intentionally always succeeds.
func (c *Client) Ack(ctx context.Context, consumerId string, eventId string) error {
	_ = ctx
	_ = consumerId
	_ = eventId
	return nil
}
