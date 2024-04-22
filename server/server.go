package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"ella.to/bus"
	"ella.to/bus/internal/batch"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
	"ella.to/sse"
)

// OPTIONS

type config struct {
	dbOpts     []sqlite.OptionFunc
	dbPoolSize int
}

type Opt interface {
	configureHandler(*config) error
}

type optFn func(*config) error

var _ Opt = (optFn)(nil)

func (opt optFn) configureHandler(s *config) error {
	return opt(s)
}

func WithStoragePoolSize(size int) Opt {
	return optFn(func(s *config) error {
		s.dbOpts = append(s.dbOpts, sqlite.WithPoolSize(size))
		s.dbPoolSize = size
		return nil
	})
}

func WithStoragePath(path string) Opt {
	return optFn(func(s *config) error {
		s.dbOpts = append(s.dbOpts, sqlite.WithFile(path))
		return nil
	})
}

func WithStorageMemory() Opt {
	return optFn(func(s *config) error {
		s.dbOpts = append(s.dbOpts, sqlite.WithMemory())
		return nil
	})
}

// HANDLER

type Handler struct {
	mux               http.ServeMux
	dbw               *sqlite.Worker
	consumersEventMap *bus.ConsumersEventMap
	batch             *batch.Sort
}

var _ http.Handler = (*Handler)(nil)

func (h *Handler) publishHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()

	evt := &bus.Event{}
	err := json.NewDecoder(r.Body).Decode(evt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if evt.Subject == "" {
		http.Error(w, "event must have a subject", http.StatusBadRequest)
		return
	}

	if evt.ExpiresAt == nil {
		evt.ExpiresAt = bus.GetDefaultExpiresAt()
	}

	evt.Id = bus.GetEventId()
	evt.CreatedAt = time.Now()

	err = h.AppendEvents(ctx, evt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	header := w.Header()

	header.Set("Event-Id", evt.Id)
	header.Set("Event-Created-At", evt.CreatedAt.Format(time.RFC3339))
	header.Set("Event-Expires-At", evt.ExpiresAt.Format(time.RFC3339))

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) consumerHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	subject := qs.Get("subject")
	if subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}

	isDurable := qs.Has("durable")
	queue := qs.Get("queue")
	isQueue := queue != ""

	if isDurable && isQueue {
		http.Error(w, "durable and queue are mutually exclusive", http.StatusBadRequest)
		return
	}

	// position can be
	// 1: normal (Default) - start from the last event id that was consumed
	// 2: all - start from the beginning
	// 3: <event_id> - start from the last event id provided
	// NOTE: if either durable or queue is set, position is only allowed to be used once during creation
	// subsequent request will be ignored
	pos := qs.Get("pos")
	if pos == "" {
		pos = "newest"
	}

	batchSize, err := strconv.ParseInt(qs.Get("batch_size"), 10, 64)
	if err != nil {
		batchSize = 1
	}

	id := qs.Get("id")

	if id != "" && queue != "" {
		http.Error(w, "id cannot be used with queue", http.StatusBadRequest)
		return
	}

	if id == "" {
		id = bus.GetConsumerId()
	}

	lastEventId, err := h.GetLastEventId(ctx, pos)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	events := h.consumersEventMap.Add(id, batchSize)
	defer h.consumersEventMap.Remove(id)

	consumer, err := h.LoadConsumerById(ctx, id)
	if errors.Is(err, storage.ErrConsumerNotFound) {
		opts := []bus.ConsumerOpt{
			bus.WithId(id),
			bus.WithSubject(subject),
			bus.WithBatchSize(batchSize),
			bus.WithFromEventId(lastEventId),
		}

		if isDurable {
			opts = append(opts, bus.WithDurable())
		}

		if queue != "" {
			opts = append(opts, bus.WithQueue(queue))
		}

		consumer, err = bus.NewConsumer(opts...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = h.CreateConsumer(ctx, consumer)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer func() {
		h.consumersEventMap.Remove(id)
		// NOTE: queue consumers, should be deleted upon disconnection
		// there is no need to store them in the database
		if isQueue || !isDurable {
			err := h.DeleteConsumer(context.Background(), id)
			if err != nil {
				slog.Error("failed to delete consumer", "consumer_id", id, "error", err)
			}
		}
	}()

	pusher, err := sse.CreatePusher(
		w,
		// NOTE: id of consumer will be sent back to the client using `Consumer-Id` http.Header key
		// this is useful for the client to reconnect to the same consumer if id is auto-generated
		sse.WithHeader("Consumer-Id", id),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Done(ctx)

	var msgsCount int64
	var prevEventId string

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			pusher.Push(ctx, "event", event)
			msgsCount++

			prevEventId = event.Id
			if msgsCount%batchSize == 0 {
				h.AckEvent(ctx, consumer.Id, prevEventId)
				prevEventId = ""
			}
		case <-time.After(1 * time.Second):
			if prevEventId != "" {
				h.AckEvent(ctx, consumer.Id, prevEventId)
				prevEventId = ""
			}
		}
	}
}

func (h *Handler) ackedHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	consumerId := qs.Get("consumer_id")
	if consumerId == "" {
		http.Error(w, "consumer_id is required", http.StatusBadRequest)
		return
	}

	eventId := qs.Get("event_id")
	if eventId == "" {
		http.Error(w, "event_id is required", http.StatusBadRequest)
		return
	}

	err := h.AckEvent(ctx, consumerId, eventId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteConsumerHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	consumerId := qs.Get("consumer_id")
	if consumerId == "" {
		http.Error(w, "consumer_id is required", http.StatusBadRequest)
		return
	}

	err := h.DeleteConsumer(ctx, consumerId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) notify(consumerId string, event *bus.Event) {
	slog.Debug("notify", "consumer_id", consumerId, "event_id", event.Id)
	err := h.consumersEventMap.Push(consumerId, event)
	if err != nil {
		slog.Error("failed to notify consumer", "consumer_id", consumerId, "event_id", event.Id, "error", err)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func New(ctx context.Context, opts ...Opt) (*Handler, error) {
	conf := &config{}
	for _, opt := range opts {
		err := opt.configureHandler(conf)
		if err != nil {
			return nil, err
		}
	}

	h := &Handler{
		consumersEventMap: bus.NewConsumersEventMap(conf.dbPoolSize, 2*time.Second),
	}

	h.batch = batch.NewSort(10, 500*time.Millisecond, func(events []*bus.Event) {
		h.dbw.Submit(func(conn *sqlite.Conn) {
			ctx := context.Background()
			err := storage.AppendEvents(ctx, conn, events...)
			if err != nil {
				slog.Error("failed to append events", "error", err)
			}
		})
	})

	db, err := storage.New(ctx, h.notify, conf.dbOpts...)
	if err != nil {
		return nil, err
	}

	h.dbw = sqlite.NewWorker(db, 100, int64(conf.dbPoolSize))

	h.mux.HandleFunc("POST /", h.publishHandler)          // POST /
	h.mux.HandleFunc("GET /", h.consumerHandler)          // GET /?subject=foo&durable&queue=bar&pos=all
	h.mux.HandleFunc("HEAD /", h.ackedHandler)            // HEAD /?consumer_id=123&event_id=456
	h.mux.HandleFunc("DELETE /", h.deleteConsumerHandler) // DELETE /?consumer_id=123

	return h, nil
}
