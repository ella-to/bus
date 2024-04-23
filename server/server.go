package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"ella.to/bus"
	"ella.to/bus/internal/batch"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
	"ella.to/sse"
)

// OPTIONS

type config struct {
	dbOpts                 []sqlite.OptionFunc
	dbPoolSize             int
	batchWindowSize        int
	batchWindowDuration    time.Duration
	workerBufferSize       int64
	cleanExpiredEventsFreq time.Duration
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

func WithBatchWindowSize(size int) Opt {
	return optFn(func(s *config) error {
		s.batchWindowSize = size
		return nil
	})
}

func WithBatchWindowDuration(d time.Duration) Opt {
	return optFn(func(s *config) error {
		s.batchWindowDuration = d
		return nil
	})
}

func WithWorkerBufferSize(size int64) Opt {
	return optFn(func(s *config) error {
		s.workerBufferSize = size
		return nil
	})
}

func WithCleanExpiredEventsFreq(freq time.Duration) Opt {
	return optFn(func(s *config) error {
		s.cleanExpiredEventsFreq = freq
		return nil
	})
}

// HANDLER

type Handler struct {
	mux               http.ServeMux
	dbw               *sqlite.Worker
	consumersEventMap *bus.ConsumersEventMap
	batch             *batch.Sort
	closeCh           chan struct{}
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
	queueName := qs.Get("queue")
	isQueue := queueName != ""
	ackStrategy := qs.Get("ack_strategy")
	if ackStrategy == "" {
		ackStrategy = "auto"
	}
	isAutoAck := ackStrategy == "auto"

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

	// NOTE: Currently only support batch size of 1
	batchSize := int64(1)

	id := qs.Get("id")

	if id != "" && isQueue {
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

	var queueMaxAckedCount int64

	// NOTE: if queue is not found, create a new queue
	// this is essential for creating consumer with queue
	if isQueue {
		queue, err := h.LoadQueueByName(ctx, queueName)
		if errors.Is(err, storage.ErrQueueNotFound) {
			queue = &bus.Queue{
				Name:        queueName,
				Pattern:     strings.ReplaceAll(subject, "*", "%"),
				AckStrategy: ackStrategy,
				LastEventId: lastEventId,
			}

			err = h.CreateQueue(ctx, queue)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		ackedCount, err := h.LoadQueueMaxAckedCount(ctx, queueName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// NOTE: overriding lastEventId and subject with the queue's info
		lastEventId = queue.LastEventId
		subject = queue.Pattern
		ackStrategy = queue.AckStrategy
		queueMaxAckedCount = ackedCount
	}

	events := h.consumersEventMap.Add(id, batchSize)
	defer h.consumersEventMap.Remove(id)

	// need to call this here before creating the consumer
	// because once the consumer is created, triggers will start
	// pumping events to the consumer and it might leads to
	// pipe is full error
	notAckedEvents, err := h.LoadNotAckedEvents(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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

		if isQueue {
			opts = append(opts, bus.WithQueue(queueName))
		}

		consumer, err = bus.NewConsumer(opts...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		consumer.AckedCount = queueMaxAckedCount

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
		sse.WithHeader("Consumer-Subject-Pattern", consumer.Pattern),
		sse.WithHeader("Consumer-Queue", consumer.QueueName),
		sse.WithHeader("Consumer-Durable", fmt.Sprintf("%t", consumer.Durable)),
		sse.WithHeader("Consumer-Auto-Ack", fmt.Sprintf("%t", isAutoAck)),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Done(ctx)

	var msgsCount int64
	var prevEventId string

	// push any pending events
	for _, event := range notAckedEvents {
		err = h.consumersEventMap.Push(consumer.Id, event)
		if err != nil {
			pusher.Push(ctx, "error", err)
			return
		}
	}

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
				if isAutoAck {
					err = h.AckEvent(ctx, consumer.Id, prevEventId)
					if err != nil {
						pusher.Push(ctx, "error", err)
						return
					}
				}
				prevEventId = ""
			}
		case <-time.After(1 * time.Second):
			if prevEventId != "" {
				if isAutoAck {
					err = h.AckEvent(ctx, consumer.Id, prevEventId)
					if err != nil {
						pusher.Push(ctx, "error", err)
						return
					}
				}
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

func (h *Handler) removeExpiredEventsLoop(ctx context.Context, freq time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(freq):
			err := h.DeleteExpiredEvents(ctx)
			if err != nil {
				slog.Error("failed to delete expired events", "error", err)
			}
		case <-h.closeCh:
			return
		}
	}
}

func (h *Handler) Close() {
	close(h.closeCh)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func New(ctx context.Context, opts ...Opt) (*Handler, error) {
	conf := &config{
		dbOpts: []sqlite.OptionFunc{
			sqlite.WithMemory(),
		},
		batchWindowSize:        20,
		batchWindowDuration:    500 * time.Millisecond,
		workerBufferSize:       1000,
		cleanExpiredEventsFreq: 30 * time.Second,
	}
	for _, opt := range opts {
		err := opt.configureHandler(conf)
		if err != nil {
			return nil, err
		}
	}

	if conf.dbPoolSize == 0 {
		return nil, errors.New("db pool size is required")
	}

	h := &Handler{
		consumersEventMap: bus.NewConsumersEventMap(conf.dbPoolSize, 2*time.Second),
		closeCh:           make(chan struct{}),
	}

	h.batch = batch.NewSort(conf.batchWindowSize, conf.batchWindowDuration, func(events []*bus.Event) {
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

	h.dbw = sqlite.NewWorker(db, int64(conf.workerBufferSize), int64(conf.dbPoolSize))

	h.mux.HandleFunc("POST /", h.publishHandler)          // POST /
	h.mux.HandleFunc("GET /", h.consumerHandler)          // GET /?subject=foo&durable&queue=bar&pos=oldest|newest|<event_id>&id=123&auto_ack
	h.mux.HandleFunc("HEAD /", h.ackedHandler)            // HEAD /?consumer_id=123&event_id=456
	h.mux.HandleFunc("DELETE /", h.deleteConsumerHandler) // DELETE /?consumer_id=123

	go h.removeExpiredEventsLoop(ctx, conf.cleanExpiredEventsFreq)

	return h, nil
}
