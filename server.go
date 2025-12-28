package bus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"ella.to/bus/internal/cache"
	"ella.to/immuta"
	"ella.to/sse"
	"ella.to/task"
)

//
// handler
//

const (
	HeaderEventId        = "X-BUS-EVENT-ID"
	HeaderEventCreatedAt = "X-BUS-EVENT-CREATED-AT"
	HeaderEventIndex     = "X-BUS-EVENT-INDEX"
	HeaderConsumerId     = "X-BUS-CONSUMER-ID"
)

const (
	DefaultSsePingTimeout = 30 * time.Second
)

type DuplicateChecker interface {
	CheckDuplicate(key string) bool
}

type DuplicateCheckerFunc func(key string) bool

func (f DuplicateCheckerFunc) CheckDuplicate(key string) bool {
	return f(key)
}

type Handler struct {
	mux           *http.ServeMux
	eventsLog     *immuta.Storage
	waitingAckMap map[string]chan struct{} // {consumerId-eventId} -> ack
	runner        task.Runner
	dupChecker    DuplicateChecker
}

func (h *Handler) Close() error {
	return h.eventsLog.Close()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// decode the request body to event
	var event Event

	if _, err := io.Copy(&event, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := event.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.dupChecker != nil {
		if h.dupChecker.CheckDuplicate(event.Key) {
			http.Error(w, "key was processed before", http.StatusConflict)
			return
		}
	}

	if event.Id == "" {
		event.Id = newEventId()
	}

	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}

	var eventIndex int64

	namespaceIdx := strings.Index(event.Subject, ".")

	err := h.runner.Submit(ctx, func(ctx context.Context) (err error) {
		eventIndex, _, err = h.eventsLog.Append(ctx, event.Subject[:namespaceIdx], &event)
		if err != nil {
			return err
		}

		return nil
	}).Await(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(HeaderEventId, event.Id)
	w.Header().Set(HeaderEventCreatedAt, event.CreatedAt.Format(time.RFC3339Nano))
	w.Header().Set(HeaderEventIndex, fmt.Sprintf("%d", eventIndex))

	w.WriteHeader(http.StatusAccepted)
}

// GET /?subject=a.b.*&start=oldest&ack=manual&redelivery=5s&redelivery_count=3
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	id := newConsumerId()
	subject := qs.Get("subject")
	start := defaultString(qs.Get("start"), DefaultStart)
	ack := defaultStringOneOf(qs.Get("ack"), DefaultAck, AckManual, AckNone)
	redelivery := defaultDuration(qs.Get("redelivery"), DefaultRedelivery)
	redeliveryCount := defaultInt(qs.Get("redelivery_count"), DefaultRedeliveryCount)

	if subject == "" {
		http.Error(w, "missing subject in query string", http.StatusBadRequest)
		return
	}

	if err := ValidateSubject(subject); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	logger.InfoContext(ctx, "new consumer", "id", id, "subject", subject, "start", start, "ack", ack, "redelivery", redelivery, "redelivery_count", redeliveryCount)
	defer logger.InfoContext(ctx, "consumer closed", "id", id)

	var startPos int64
	switch start {
	case StartOldest:
		startPos = 0
		start = ""
	case StartNewest:
		startPos = -1
		start = ""
	default:
		// I wrote it like this to indicate the reader that
		// we will start from the beginning of events log and
		// loop through all events until we find the event id associate
		// with start variable
		startPos = 0
	}

	namespaceIdx := strings.Index(subject, ".")

	stream := h.eventsLog.Stream(ctx, subject[:namespaceIdx], startPos)
	defer stream.Done()

	w.Header().Set(HeaderConsumerId, id)

	pusher, err := sse.NewPusher(w, DefaultSsePingTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Close()

	for {
		r, _, err := stream.Next(ctx)
		if errors.Is(err, context.Canceled) || errors.Is(err, immuta.ErrStorageClosed) {
			// the client has closed the connection or the server is shutting down
			// in both cases we should return and the defer will send the done message and also close the pusher
			return
		} else if err != nil {
			_ = pusher.Push(newSseError(err))
			return
		}

		var event Event
		_, err = io.Copy(&event, r)
		_ = r.Done()
		if err != nil {
			_ = pusher.Push(newSseError(err))
			continue
		}

		if start != "" {
			if event.Id == start {
				// we found the event id
				// now we can start pushing events to the client
				start = ""
			}
			continue
		}

		if !MatchSubject(event.Subject, subject) {
			continue
		}

		key := getAckKey(id, event.Id)

		var ch chan struct{}

		if ack == AckManual {
			ch = make(chan struct{})

			setAckMapError := h.runner.Submit(ctx, func(ctx context.Context) error {
				h.waitingAckMap[key] = ch
				return nil
			}).Await(ctx)
			if setAckMapError != nil {
				logger.ErrorContext(ctx, "failed to set signal channel for acking message", "error", setAckMapError, "consumer_id", id, "event_id", event.Id)
				_ = pusher.Push(newSseError(setAckMapError))
				continue
			}
		}

		redeliveryCoutnEnabled := ack == AckManual && redeliveryCount > 0
		redeliveryCountAttempted := 0

		for {
			// reset the read/write state before redelivery
			// this is essential to ensure correct serialization when we copy the event to sse message
			// without this, the event will be sent empty after the first delivery
			event.resetReadWriteState()

			// NOTE: we are creating a new SSE msg since msg has some
			// io.Read and io.Write internal variables which are essential to
			// io.Copy
			msg, err := newSseEvent(&event)
			if err != nil {
				logger.WarnContext(ctx, "failed to create a sse message from event", "error", err, "event_id", event.Id, "consumer_id", id)
				_ = pusher.Push(newSseError(err))
				break
			}

			err = pusher.Push(msg)
			if err != nil {
				logger.WarnContext(ctx, "failed to push sse message to consumer", "error", err, "event_id", event.Id, "consumer_id", id)
				h.runner.Submit(ctx, func(ctx context.Context) error {
					delete(h.waitingAckMap, key)
					return nil
				})
				return
			}

			if ack == AckManual {
				select {
				case <-ctx.Done():
					h.runner.Submit(ctx, func(ctx context.Context) error {
						delete(h.waitingAckMap, key)
						return nil
					})
					return
				case <-ch:
					// ack received, the ch signal will be deleted by Ack function
					logger.DebugContext(ctx, "received acked", "consumer_id", id, "event_id", event.Id)
					break
				case <-time.After(redelivery):
					redeliveryCountAttempted++

					_ = h.runner.Submit(ctx, func(ctx context.Context) error {
						delete(h.waitingAckMap, key)
						return nil
					}).Await(ctx)

					if redeliveryCoutnEnabled && redeliveryCountAttempted >= redeliveryCount {
						logger.WarnContext(ctx, "redelivery count exceeded, dropping event", "consumer_id", id, "event_id", event.Id, "subject", event.Subject, "trace_id", event.TraceId)
						break
					}

					logger.WarnContext(ctx, "redelivery", "consumer_id", id, "event_id", event.Id, "subject", event.Subject, "trace_id", event.TraceId, "redelivery_attempt_count", redeliveryCountAttempted)
					continue
				}
			}
			break
		}
	}
}

// PUT /?consumer_id=c_123&event_id=e_456
func (h *Handler) Ack(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	consumerId := qs.Get("consumer_id")
	eventId := qs.Get("event_id")

	if consumerId == "" {
		http.Error(w, "missing consumer_id in query string", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(consumerId, "c_") {
		http.Error(w, "invalid consumer_id in query string", http.StatusBadRequest)
		return
	}

	if eventId == "" {
		http.Error(w, "missing event_id in query string", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(eventId, "e_") {
		http.Error(w, "invalid event_id in query string", http.StatusBadRequest)
		return
	}

	key := getAckKey(consumerId, eventId)

	err := h.runner.Submit(ctx, func(ctx context.Context) error {
		ack, ok := h.waitingAckMap[key]
		if !ok {
			logger.WarnContext(ctx, "failed to find ack channel key", "consumer_id", consumerId, "event_id", eventId)
			return nil
		}

		delete(h.waitingAckMap, key)
		close(ack)

		return nil
	}).Await(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func CreateHandler(logsDirPath string, namespaces []string, secretKey string, blockSize int, dupCacheSize int) (*Handler, error) {
	{
		// This block is used to validate the namespaces
		// and make sure there is no reserved and duplicate namespaces

		namespacesSet := make(map[string]struct{})
		for _, ns := range namespaces {
			namespacesSet[ns] = struct{}{}
		}

		// NOTE: currently bus has an internal namespace "_bus_" which was used to store
		// RPC and Confirm events. This namespace should not be consumed by the user.
		if _, ok := namespacesSet["_bus_"]; ok {
			return nil, errors.New("namespace _bus_ is reserved")
		}

		namespaces = make([]string, 0, len(namespacesSet)+1)
		namespaces = append(namespaces, "_bus_")
		for ns := range namespacesSet {
			namespaces = append(namespaces, ns)
		}
	}

	immutaOpts := []immuta.OptionFunc{
		immuta.WithLogsDirPath(logsDirPath),
		immuta.WithReaderCount(5),
		immuta.WithFastWrite(true),
		immuta.WithNamespaces(namespaces...),
	}

	if secretKey != "" {
		encryption := NewEncryption(secretKey, blockSize)
		immutaOpts = append(immutaOpts, immuta.WithWriteTransform(encryption.Encode))
		immutaOpts = append(immutaOpts, immuta.WithReadTransform(encryption.Decode))
	}

	eventStorage, err := immuta.New(immutaOpts...)
	if err != nil {
		return nil, err
	}

	runner := task.NewRunner(task.WithWorkerSize(1))

	var dupChecker DuplicateChecker

	if dupCacheSize > 0 {
		dupChecker = func() DuplicateCheckerFunc {
			lru := cache.NewLRU[string](dupCacheSize)
			return func(key string) bool {
				if key == "" {
					return false
				}
				return !lru.Add(key)
			}
		}()
	}

	return NewHandler(eventStorage, runner, dupChecker), nil
}

func NewHandler(eventLogs *immuta.Storage, runner task.Runner, dupChecker DuplicateChecker) *Handler {
	h := &Handler{
		mux:           http.NewServeMux(),
		eventsLog:     eventLogs,
		runner:        runner,
		waitingAckMap: make(map[string]chan struct{}),
		dupChecker:    dupChecker,
	}

	// Wrap handlers with CORS middleware
	h.mux.HandleFunc("POST /", corsMiddleware(h.Put))
	h.mux.HandleFunc("GET /", corsMiddleware(h.Get))
	h.mux.HandleFunc("PUT /", corsMiddleware(h.Ack))
	h.mux.HandleFunc("OPTIONS /", corsMiddleware(handleOptions))

	return h
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	allowHeaders := "Content-Type, " + HeaderEventId + ", " + HeaderEventCreatedAt + ", " + HeaderEventIndex + ", " + HeaderConsumerId
	exposeHeaders := HeaderEventId + ", " + HeaderEventCreatedAt + ", " + HeaderEventIndex + ", " + HeaderConsumerId

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		w.Header().Set("Access-Control-Expose-Headers", exposeHeaders)

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next(w, r)
	}
}

func handleOptions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

//
// utilities
//

func getAckKey(consumerId, eventId string) string {
	return consumerId + "-" + eventId
}

func defaultString(s string, def string) string {
	if s == "" {
		return def
	}

	return s
}

func defaultInt(s string, def int) int {
	if s == "" {
		return def
	}

	value, err := strconv.Atoi(s)
	if err != nil {
		return def
	}

	return value
}

func defaultDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}

	if d <= 0 {
		return def
	}

	return d
}

func defaultStringOneOf(s string, def string, opts ...string) string {
	if s == "" {
		return def
	}

	if slices.Contains(opts, s) {
		return s
	}

	return def
}

func newSseError(err error) *sse.Message {
	return &sse.Message{
		Event: errorType,
		Data:  err.Error(),
	}
}

func newSseEvent(event *Event) (*sse.Message, error) {
	var sb strings.Builder

	_, err := io.Copy(&sb, event)
	if err != nil {
		return nil, err
	}

	return &sse.Message{
		Event: msgType,
		Data:  sb.String(),
	}, nil
}
