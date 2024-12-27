package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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

type Handler struct {
	mux           *http.ServeMux
	eventsLog     *immuta.Storage
	waitingAckMap map[string]chan struct{} // {consumerId-eventId} -> ack
	runner        task.Runner
	buffer        bytes.Buffer
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// decode the request body to event
	var event Event

	if err := event.decode(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := event.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event.Id = newEventId()
	event.CreatedAt = time.Now()

	var eventIndex int64

	err := h.runner.Submit(ctx, func(ctx context.Context) error {
		err := event.encode(&h.buffer)
		if err != nil {
			return err
		}

		eventIndex, _, err = h.eventsLog.Append(ctx, &h.buffer)
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

// GET /?subject=a.b.*&start=oldest&ack=manual&redelivery=5s
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	id := newConsumerId()
	subject := qs.Get("subject")
	start := defaultString(qs.Get("start"), DefaultStart)
	ack := defaultStringOneOf(qs.Get("ack"), DefaultAck, AckManual, AckNone)
	redelivery := defaultDuration(qs.Get("redelivery"), DefaultRedelivery)

	if subject == "" {
		http.Error(w, "missing subject in query string", http.StatusBadRequest)
		return
	}

	if err := ValidateSubject(subject); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.Info("new consumer", "id", id, "subject", subject, "start", start, "ack", ack, "redelivery", redelivery)
	defer slog.Info("consumer closed", "id", id)

	w.Header().Set(HeaderConsumerId, id)

	pusher, err := sse.NewPusher(w, DefaultSsePingTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Close()

	var startPos int64
	if start == StartOldest {
		startPos = 0
		start = ""
	} else if start == StartNewest {
		startPos = -1
		start = ""
	} else {
		// I wrote it like this to indicate the reader that
		// we will start from the beginning of events log and
		// loop through all events until we find the event id associate
		// with start variable
		startPos = 0
	}

	stream := h.eventsLog.Stream(ctx, startPos)
	defer stream.Done()

	for {
		r, _, err := stream.Next(ctx)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			pusher.Push(newSseError(err))
			return
		}

		var event Event
		err = event.decode(r)
		r.Done()
		if err != nil {
			pusher.Push(newSseError(err))
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

	REDLIVERY:
		msg, err := newSseEvent(&event)
		if err != nil {
			pusher.Push(newSseError(err))
			continue
		}

		err = pusher.Push(msg)
		if err != nil {
			return
		}

		if ack == AckManual {
			ch := make(chan struct{})

			err := h.runner.Submit(ctx, func(ctx context.Context) error {
				h.waitingAckMap[key] = ch
				return nil
			}).Await(ctx)
			if err != nil {
				pusher.Push(newSseError(err))
				continue
			}

			select {
			case <-ctx.Done():
				return
			case <-ch:
				// ack received
				slog.Info("received acked", "consumer_id", id, "event_id", event.Id)
				h.runner.Submit(ctx, func(ctx context.Context) error {
					delete(h.waitingAckMap, key)
					return nil
				}).Await(ctx)
			case <-time.After(redelivery):
				slog.Info("redelivery", "consumer_id", id, "event_id", event.Id)
				h.runner.Submit(ctx, func(ctx context.Context) error {
					delete(h.waitingAckMap, key)
					return nil
				}).Await(ctx)
				goto REDLIVERY
			}
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

func NewHandler(eventLogs *immuta.Storage, runner task.Runner) *Handler {
	h := &Handler{
		mux:           http.NewServeMux(),
		eventsLog:     eventLogs,
		runner:        runner,
		waitingAckMap: make(map[string]chan struct{}),
	}

	h.mux.HandleFunc("POST /", h.Put)
	h.mux.HandleFunc("GET /", h.Get)
	h.mux.HandleFunc("PUT /", h.Ack)

	return h
}

func NewServer(addr string, eventLogs string) (*http.Server, error) {
	eventStorage, err := immuta.New(eventLogs, 10, true)
	if err != nil {
		return nil, err
	}

	runner := task.NewRunner(task.WithWorkerSize(1))

	handler := NewHandler(eventStorage, runner)

	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	return server, nil
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

	for _, opt := range opts {
		if s == opt {
			return s
		}
	}

	return def
}

func newSseError(err error) *sse.Message {
	errMsg := err.Error()
	return &sse.Message{
		Event: "error",
		Data:  &errMsg,
	}
}

func newSseEvent(event *Event) (*sse.Message, error) {
	var sb strings.Builder

	err := event.encode(&sb)
	if err != nil {
		return nil, err
	}

	msg := sb.String()

	return &sse.Message{
		Event: "event",
		Data:  &msg,
	}, nil
}
