package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"ella.to/bus"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
	"ella.to/sse"
)

const DefaultAddr = "0.0.0.0:2021"

// OPTIONS

type config struct {
	dbOpts                 []sqlite.OptionFunc
	dbPoolSize             int
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
	dbw                *sqlite.Worker
	mux                http.ServeMux
	consumersEventsMap *ConsumersEventsMap
	actions            *Actions
	closeSignal        chan struct{}
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

	err = h.actions.Put(ctx, evt)
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

	if ackStrategy != "auto" && ackStrategy != "manual" {
		http.Error(w, "invalid ack_strategy", http.StatusBadRequest)
		return
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

	batchSize, err := strconv.ParseInt(qs.Get("batch_size"), 10, 64)
	if err != nil || batchSize < 1 {
		batchSize = 1
	}

	id := qs.Get("id")

	if id != "" && isQueue {
		http.Error(w, "id cannot be used with queue", http.StatusBadRequest)
		return
	}

	if id == "" {
		id = bus.GetConsumerId()
	}

	consOpts := []bus.ConsumerOpt{
		bus.WithId(id),
		bus.WithSubject(subject),
		bus.WithQueue(queueName),
		bus.WithBatchSize(batchSize),
		bus.WithFromEventId(pos),
	}

	if isDurable {
		consOpts = append(consOpts, bus.WithDurable())
	}

	if !isAutoAck {
		consOpts = append(consOpts, bus.WithManualAck())
	}

	consumer, err := bus.NewConsumer(consOpts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	consumer.UpdatedAt = time.Now()

	batch, err := h.actions.Get(ctx, consumer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := h.actions.Delete(ctx, id)
		if err != nil {
			slog.Error("failed to delete action", "error", err)
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
		sse.WithHeader("Consumer-Ack-Strategy", ackStrategy),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Done(ctx)

	var lastEventId string

	for {
		// This is a basic optimization, if the consumer is auto ack
		// then we do couple of extra select operations
		// if the consumer is manual ack, then we just wait for the batch and context.Done
		if isAutoAck {
			if lastEventId != "" {
				select {
				case <-ctx.Done():
					return
				case events, ok := <-batch:
					if !ok {
						return
					}
					pusher.Push(ctx, "event", events)

					lastEventId = events[len(events)-1].Id
					if isAutoAck {
						err = h.actions.Ack(ctx, consumer.Id, lastEventId)
						if err != nil {
							pusher.Push(ctx, "error", err)
							return
						}
						lastEventId = ""
					}

				case <-time.After(1 * time.Second):
					if isAutoAck && lastEventId != "" {
						err = h.actions.Ack(ctx, consumer.Id, lastEventId)
						if err != nil {
							pusher.Push(ctx, "error", err)
							return
						}
						lastEventId = ""
					}
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case events, ok := <-batch:
					if !ok {
						return
					}
					pusher.Push(ctx, "event", events)

					lastEventId = events[len(events)-1].Id
					if isAutoAck {
						err = h.actions.Ack(ctx, consumer.Id, lastEventId)
						if err != nil {
							pusher.Push(ctx, "error", err)
							return
						}
						lastEventId = ""
					}
				}
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case events, ok := <-batch:
				if !ok {
					return
				}
				pusher.Push(ctx, "event", events)
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

	err := h.actions.Ack(ctx, consumerId, eventId)
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

	err := h.actions.Delete(ctx, consumerId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
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
		case <-h.closeSignal:
			return
		}
	}
}

func (h *Handler) Close() {
	close(h.closeSignal)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// once event gets inserted into the storage, a series of triggers will be executed
// which populates the consumers_events table with the event that has not been
// consumed by the consumers yet
func (h *Handler) putAction(ctx context.Context, event *bus.Event) error {
	err := h.AppendEvents(ctx, event)
	if err != nil {
		return err
	}

	consumerIds, err := h.LoadConsumerIdsByEventId(ctx, event.Id)
	if err != nil {
		return err
	}

	// NOTE: try to send the event to the consumers, if the consumer is not ready to receive the event
	// the event will be dropped, there are other chances to send the event to the consumer
	for _, consumerId := range consumerIds {
		h.consumersEventsMap.SendEvent(consumerId, []*bus.Event{event})
	}

	return nil
}

func (h *Handler) getAction(ctx context.Context, consumer *bus.Consumer) (chan []*bus.Event, error) {
	var err error

	_, ok := h.consumersEventsMap.GetConsumer(consumer.Id)
	if ok {
		return nil, errors.New("a consumer with the same id already connected")
	}

	// we need to update the last event id of the consumer
	// currently, the last event id of consumer contains position
	consumer.LastEventId, err = h.GetLastEventId(ctx, consumer.LastEventId)
	if err != nil {
		return nil, err
	}

	// NOTE: if queue is not found, create a new queue
	// this is essential for creating consumer with queue
	if consumer.QueueName != "" {
		queue, err := h.LoadQueueByName(ctx, consumer.QueueName)
		if errors.Is(err, storage.ErrQueueNotFound) {
			queue = &bus.Queue{
				Name:        consumer.QueueName,
				Pattern:     consumer.Pattern,
				AckStrategy: consumer.AckStrategy,
				LastEventId: consumer.LastEventId,
			}

			err = h.CreateQueue(ctx, queue)
			if err != nil {
				return nil, err
			}
		}

		ackedCount, err := h.LoadQueueMaxAckedCount(ctx, consumer.QueueName)
		if err != nil {
			return nil, err
		}

		// NOTE: overriding lastEventId and subject with the queue's info
		consumer.LastEventId = queue.LastEventId
		consumer.Pattern = queue.Pattern
		consumer.AckStrategy = queue.AckStrategy
		consumer.AckedCount = ackedCount
	}

	// load/create consumer
	_, err = h.LoadConsumerById(ctx, consumer.Id)
	if errors.Is(err, storage.ErrConsumerNotFound) {
		err = h.CreateConsumer(ctx, consumer)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	batch := h.consumersEventsMap.AddConsumer(consumer.Id, consumer.BatchSize)

	// need to call this here before creating the consumer
	// because once the consumer is created, triggers will start
	// pumping events to the consumer and it might leads to
	// pipe is full error
	notAckedEvents, err := h.LoadNotAckedEvents(ctx, consumer.Id, "")
	if err != nil {
		return nil, err
	}

	// load events from the storage and pre-populate the events channel
	// with size of the batch size
	if len(notAckedEvents) > 0 {
		batch <- notAckedEvents
	}

	return batch, nil
}

func (h *Handler) ackAction(ctx context.Context, consumerId, eventId string) error {
	err := h.AckEvent(ctx, consumerId, eventId)
	if err != nil {
		return err
	}

	nextId, err := h.LoadNextConsumer(ctx, consumerId)
	if err == nil && nextId != "" {
		consumerId = nextId
	} else if err != nil && !errors.Is(err, storage.ErrConsumerNotFound) {
		return err
	}

	notAckedEvents, err := h.LoadNotAckedEvents(ctx, consumerId, eventId)
	if err != nil {
		return err
	}

	if len(notAckedEvents) == 0 {
		return nil
	}

	batch, ok := h.consumersEventsMap.GetConsumer(consumerId)
	if !ok {
		return errors.New("consumer not found")
	}

	batch <- notAckedEvents

	return nil
}

func (h *Handler) deleteAction(ctx context.Context, consumerId string) error {
	err := h.DeleteConsumer(ctx, consumerId)
	if err != nil {
		return err
	}

	h.consumersEventsMap.RemoveConsumer(consumerId)

	return nil
}

func (h *Handler) processActions() {
	actions := h.actions.Stream()
	for {
		ctx := context.Background()

		select {
		case <-h.closeSignal:
			return
		case action := <-actions:
			switch action.Type {
			case PutActionType:
				action.Error <- h.putAction(ctx, action.Event)

			case GetActionType:
				events, err := h.getAction(ctx, action.Consumer)
				action.Events = events
				action.Error <- err

			case AckActionType:
				action.Error <- h.ackAction(ctx, action.Consumer.Id, action.EventId)

			case DeleteActionType:
				action.Error <- h.deleteAction(ctx, action.Consumer.Id)
			}
		}
	}
}

func New(ctx context.Context, opts ...Opt) (*Handler, error) {
	conf := &config{
		dbOpts: []sqlite.OptionFunc{
			sqlite.WithMemory(),
		},
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
		consumersEventsMap: NewConsumersEventsMap(),
		actions:            NewActions(conf.workerBufferSize),
	}

	go h.processActions()

	db, err := storage.New(ctx, conf.dbOpts...)
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
