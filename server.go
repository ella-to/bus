package bus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"path/filepath"
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
	HeaderConsumerId     = "X-BUS-CONSUMER-ID"
)

type Handler struct {
	mux    *http.ServeMux
	server *server
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

	if err := h.server.SaveEvent(ctx, &event); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(HeaderEventId, event.Id)
	w.Header().Set(HeaderEventCreatedAt, event.CreatedAt.Format(time.RFC3339Nano))

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	consumer := &Consumer{}
	consumer.meta.Name = qs.Get("name")
	consumer.meta.Position = qs.Get("position")
	consumer.meta.Subject = qs.Get("subject")

	var err error

	if err = consumer.meta.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumer.id = newConsumerId()
	consumer.forceDisconnect = cancel

	if consumer.pusher, err = sse.CreatePusher(w, sse.WithHeader(HeaderConsumerId, consumer.id)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// once this defer is called, the consumer will be removed from the list
	defer func() {
		err = h.server.Unsubscribe(context.WithoutCancel(ctx), consumer)
		if err != nil {
			slog.ErrorContext(ctx, "failed to unsubscribe consumer", "id", consumer.id, "error", err)
		}
	}()

	if err = h.server.Subscribe(ctx, consumer); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	<-ctx.Done()

	slog.InfoContext(ctx, "consumer disconnected", "id", consumer.id)
}

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

	if err := h.server.AckEvent(ctx, consumerId, eventId); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func NewHandler(dirPath string) (*Handler, error) {
	s, err := newServer(dirPath)
	if err != nil {
		return nil, err
	}

	h := &Handler{
		server: s,
		mux:    http.NewServeMux(),
	}

	h.mux.HandleFunc("POST /", h.Put)
	h.mux.HandleFunc("GET /", h.Get)
	h.mux.HandleFunc("PUT /", h.Ack)

	return h, nil
}

//
// server
//

const (
	taskDelay       = 10 * time.Millisecond
	expiresDuration = 5 * time.Second
)

type server struct {
	// consumers is a map of all consumers, both ephemeral and durable
	// this is useful during acking events
	consumers    map[string]*Consumer            // id -> consumer
	ephemerals   map[string]*Consumer            // id -> consumer
	durablesMeta map[string]*ConsumerMeta        // name -> meta
	durables     map[string]map[string]*Consumer // name -> id -> consumer
	eventsLog    *immuta.Storage                 // store all events
	consumersLog *immuta.Storage                 // store durable consumers
	tasks        task.Runner
}

func (s *server) Subscribe(ctx context.Context, consumer *Consumer) error {
	slog.Info("subscribing", "id", consumer.id, "name", consumer.meta.Name, "subject", consumer.meta.Subject)

	if consumer.meta.Name == "" {
		// ephemeral consumer

		err := s.tasks.Submit(ctx, func(ctx context.Context) error {
			if _, ok := s.ephemerals[consumer.id]; ok {
				return fmt.Errorf("consumer %s already exists", consumer.id)
			}

			switch consumer.meta.Position {
			case "newest":
				consumer.meta.CurrentIndex = s.eventsLog.Size()
			case "oldest":
				consumer.meta.CurrentIndex = 0
			default:
				consumer.meta.CurrentIndex = 0
			}

			s.consumers[consumer.id] = consumer
			s.ephemerals[consumer.id] = consumer
			return nil
		}).Await(ctx)
		if err != nil {
			return err
		}

		s.tasks.Submit(ctx, func(ctx context.Context) error {
			// this select makes sure that if consumer is disconnected, we don't actually yield this process anymore
			select {
			case <-ctx.Done():
				fmt.Println("context done")
				return nil
			default:
			}

			// we haven't received an ack for the last message yet, should we send it again?
			if consumer.meta.WaitingAckFor != "" && !consumer.meta.WaitingAckExpiredAt.Before(time.Now()) {
				slog.InfoContext(ctx, "waiting for receiving ack for the last event", "id", consumer.id, "subject", consumer.meta.Subject, "event_id", consumer.meta.WaitingAckFor)
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			}

			stream, err := s.eventsLog.Stream(ctx, immuta.WithAbsolutePosition(consumer.meta.CurrentIndex))
			if err != nil {
				slog.ErrorContext(ctx, "failed to create a stream from the events log", "id", consumer.id, "subject", consumer.meta.Subject, "error", err)
				consumer.forceDisconnect()
				return err
			}
			defer stream.Done()

			r, size, err := stream.Next()
			if errors.Is(err, io.EOF) {
				// there is no more events in the stream, so we yeild the task and hope next time there will be some events
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			} else if err != nil {
				slog.ErrorContext(ctx, "failed to read the next event from the stream", "id", consumer.id, "subject", consumer.meta.Subject, "index", consumer.meta.CurrentIndex, "error", err)
				consumer.forceDisconnect()
				return err
			}

			var event Event
			if err = event.decode(r); err != nil {
				slog.ErrorContext(ctx, "failed to decode event from the stream", "id", consumer.id, "subject", consumer.meta.Subject, "index", consumer.meta.CurrentIndex, "error", err)
				consumer.forceDisconnect()
				return err
			}

			if !MatchSubject(event.Subject, consumer.meta.Subject) {
				// this is not the event that this consumer is interested in
				// skip it and yeild the task for the next event
				consumer.meta.CurrentIndex = size + immuta.HeaderSize
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			}

			consumer.meta.CurrentEventSize = size

			slog.InfoContext(ctx, "pushing an event to consumer", "id", consumer.id, "subject", consumer.meta.Subject, "event_id", event.Id)

			if err = consumer.pusher.Push(ctx, "event", &event); err != nil {
				slog.ErrorContext(ctx, "failed to push an event to consumer", "id", consumer.id, "subject", consumer.meta.Subject, "event_id", event.Id, "error", err)
				consumer.forceDisconnect()
				return err
			}

			consumer.meta.WaitingAckFor = event.Id
			consumer.meta.WaitingAckExpiredAt = time.Now().Add(expiresDuration)

			// this yeild is a loop like, it will put this function back to the queue
			// so other items in the queue can be processed, we also add a delay
			// Eventually the delay value can be configured by the user
			return task.Yeild(ctx, task.WithDelay(taskDelay))
		})
		return nil
	}

	// durable consumer
	// we need to make sure that the consumer is not already subscribed
	// and properly registered in the system
	err := s.tasks.Submit(ctx, func(ctx context.Context) error {
		durableMeta, ok := s.durablesMeta[consumer.meta.Name]
		if !ok {
			durableMeta = &ConsumerMeta{
				Name:                consumer.meta.Name,
				Subject:             consumer.meta.Subject,
				Position:            consumer.meta.Position,
				AckedCount:          consumer.meta.AckedCount,
				CurrentIndex:        consumer.meta.CurrentIndex,
				WaitingAckFor:       consumer.meta.WaitingAckFor,
				WaitingAckExpiredAt: consumer.meta.WaitingAckExpiredAt,
			}
			s.durablesMeta[consumer.meta.Name] = durableMeta
		}

		if durableMeta.Subject != consumer.meta.Subject {
			return fmt.Errorf("consumer name '%s' already exists with different subject", consumer.meta.Name)
		}

		// Copy the meta data to the consumer
		consumer.meta.Name = durableMeta.Name
		consumer.meta.Subject = durableMeta.Subject
		consumer.meta.Position = durableMeta.Position
		consumer.meta.AckedCount = durableMeta.AckedCount
		consumer.meta.CurrentIndex = durableMeta.CurrentIndex
		consumer.meta.WaitingAckFor = durableMeta.WaitingAckFor
		consumer.meta.WaitingAckExpiredAt = durableMeta.WaitingAckExpiredAt

		if _, ok := s.durables[consumer.meta.Name]; !ok {
			s.durables[consumer.meta.Name] = make(map[string]*Consumer)
		}

		if _, ok := s.durables[consumer.meta.Name][consumer.id]; ok {
			return fmt.Errorf("consumer %s already exists", consumer.id)
		}

		s.consumers[consumer.id] = consumer
		s.durables[consumer.meta.Name][consumer.id] = consumer

		// we need to make sure that we have at most one task running for this group
		// we don't want to have a task for each consumer in the group
		if len(s.durables[consumer.meta.Name]) > 1 {
			return nil
		}

		s.tasks.Submit(ctx, func(ctx context.Context) error {
			// check if there is at least one consumer in the group
			// if not we can return immediately and that will remove the task from the queue
			if len(s.durables[consumer.meta.Name]) == 0 {
				return nil
			}

			defer func() {
				meta, ok := s.durablesMeta[consumer.meta.Name]
				if !ok {
					return
				}

				pr, pw := io.Pipe()
				go func() {
					if err := meta.encode(pw); err != nil {
						pw.CloseWithError(err)
						slog.ErrorContext(ctx, "failed to encode durable consumer meta data", "name", consumer.meta.Name, "error", err)
						return
					}

					pw.Close()
				}()

				_, err := s.consumersLog.Append(pr)
				if err != nil {
					slog.ErrorContext(ctx, "failed to append durable consumer meta data to the log", "name", consumer.meta.Name, "error", err)
				}
			}()

			meta, ok := s.durablesMeta[consumer.meta.Name]
			if !ok {
				for _, consumer := range s.durables[consumer.meta.Name] {
					consumer.forceDisconnect()
				}
				slog.ErrorContext(ctx, "durable consumer meta data not found", "name", consumer.meta.Name)
				return nil
			}

			if meta.WaitingAckFor != "" && !meta.WaitingAckExpiredAt.Before(time.Now()) {
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			}

			stream, err := s.eventsLog.Stream(ctx, immuta.WithAbsolutePosition(meta.CurrentIndex))
			if err != nil {
				for _, consumer := range s.durables[consumer.meta.Name] {
					consumer.forceDisconnect()
				}
				slog.ErrorContext(ctx, "failed to create a stream from the events log", "name", consumer.meta.Name, "error", err)
				return nil
			}
			defer stream.Done()

			r, size, err := stream.Next()
			if errors.Is(err, io.EOF) {
				// there is no more events in the stream, so we yeild the task and hope next time there will be some events
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			}

			var event Event
			if err = event.decode(r); err != nil {
				for _, consumer := range s.durables[consumer.meta.Name] {
					consumer.forceDisconnect()
				}
				slog.ErrorContext(ctx, "failed to decode event from the stream", "name", consumer.meta.Name, "index", meta.CurrentIndex, "error", err)
				return nil
			}

			if !MatchSubject(event.Subject, meta.Subject) {
				// this is not the event that this consumer is interested in
				// skip it and yeild the task for the next event
				meta.CurrentIndex = size + immuta.HeaderSize
				return task.Yeild(ctx, task.WithDelay(taskDelay))
			}

			meta.CurrentEventSize = size

			// find a random consumer in the group to send the event
			count := len(s.durables[consumer.meta.Name])
			consumers := make([]*Consumer, 0, count)
			for _, consumer := range s.durables[consumer.meta.Name] {
				consumers = append(consumers, consumer)
			}
			consumer := consumers[rand.Intn(count)]

			if err = consumer.pusher.Push(ctx, "event", &event); err != nil {
				slog.ErrorContext(ctx, "failed to push an event to consumer", "id", consumer.id, "subject", meta.Subject, "event_id", event.Id, "error", err)
				consumer.forceDisconnect()
				return nil
			}

			meta.WaitingAckFor = event.Id
			meta.WaitingAckExpiredAt = time.Now().Add(expiresDuration)

			return task.Yeild(ctx, task.WithDelay(taskDelay))
		})

		return nil
	}).Await(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *server) Unsubscribe(ctx context.Context, consumer *Consumer) error {
	slog.InfoContext(ctx, "unsubscribing", "id", consumer.id, "name", consumer.meta.Name, "subject", consumer.meta.Subject)

	return s.tasks.Submit(context.Background(), func(ctx context.Context) error {
		delete(s.consumers, consumer.id)

		if consumer.meta.Name == "" {
			delete(s.ephemerals, consumer.id)
			return nil
		}

		if _, ok := s.durables[consumer.meta.Name]; !ok {
			return nil
		}

		delete(s.durables[consumer.meta.Name], consumer.id)
		if len(s.durables[consumer.meta.Name]) == 0 {
			delete(s.durables, consumer.meta.Name)
		}

		return nil
	}).Await(ctx)
}

func (s *server) SaveEvent(ctx context.Context, event *Event) error {
	slog.InfoContext(ctx, "saving event", "id", event.Id, "subject", event.Subject)

	return s.tasks.Submit(ctx, func(ctx context.Context) error {
		event.Id = newEventId()
		event.CreatedAt = time.Now()

		pr, pw := io.Pipe()
		go func() {
			if err := event.encode(pw); err != nil {
				pw.CloseWithError(err)
				slog.ErrorContext(ctx, "failed to encode event", "id", event.Id, "error", err)
				return
			}

			pw.Close()
		}()

		_, err := s.eventsLog.Append(pr)
		if err != nil {
			slog.ErrorContext(ctx, "failed to append event to the log", "id", event.Id, "error", err)
		}

		return err
	}).Await(ctx)
}

func (s *server) AckEvent(ctx context.Context, consumerId string, eventId string) error {
	slog.InfoContext(ctx, "acking event", "consumer_id", consumerId, "event_id", eventId)

	return s.tasks.Submit(ctx, func(ctx context.Context) error {
		consumer, ok := s.consumers[consumerId]
		if !ok {
			return fmt.Errorf("consumer %s not found", consumerId)
		}

		if consumer.meta.Name == "" {
			if consumer.meta.WaitingAckFor != eventId {
				return fmt.Errorf("consumer %s is not waiting for event %s", consumerId, eventId)
			}

			consumer.meta.WaitingAckFor = ""
			consumer.meta.CurrentIndex += consumer.meta.CurrentEventSize + immuta.HeaderSize
			consumer.meta.CurrentEventSize = 0

			return nil

		} else {
			meta, ok := s.durablesMeta[consumer.meta.Name]
			if !ok {
				return fmt.Errorf("durable consumer %s not found", consumer.meta.Name)
			}

			if meta.WaitingAckFor != eventId {
				return fmt.Errorf("durable consumer %s is not waiting for event %s", consumer.meta.Name, eventId)
			}

			meta.WaitingAckFor = ""
			meta.CurrentIndex += meta.CurrentEventSize + immuta.HeaderSize
			meta.CurrentEventSize = 0

			for _, consumer := range s.durables[consumer.meta.Name] {
				consumer.meta.WaitingAckFor = ""
				consumer.meta.CurrentIndex = meta.CurrentIndex
				consumer.meta.CurrentEventSize = meta.CurrentEventSize
			}

			// save the consumer to consumer logs
			pr, pw := io.Pipe()
			go func() {
				if err := meta.encode(pw); err != nil {
					pw.CloseWithError(err)
					slog.ErrorContext(ctx, "failed to encode durable consumer meta data", "name", consumer.meta.Name, "error", err)
					return
				}

				pw.Close()
			}()

			_, err := s.consumersLog.Append(pr)
			if err != nil {
				slog.ErrorContext(ctx, "failed to append durable consumer meta data to the log", "name", consumer.meta.Name, "error", err)
			}

			return err
		}
	}).Await(ctx)
}

func (s *server) Close(ctx context.Context) error {
	err := s.tasks.Close(ctx)
	if err != nil {
		return err
	}

	err = s.eventsLog.Close()
	if err != nil {
		return err
	}

	err = s.consumersLog.Close()
	if err != nil {
		return err
	}

	return nil
}

func newServer(dirPath string) (*server, error) {
	eventLogs, err := immuta.New(
		immuta.WithQueueSize(10),
		immuta.WithFastWrite(filepath.Join(dirPath, "events.log")),
	)
	if err != nil {
		return nil, err
	}

	consumerLogs, err := immuta.New(
		immuta.WithQueueSize(10),
		immuta.WithFastWrite(filepath.Join(dirPath, "consumers.log")),
	)
	if err != nil {
		return nil, err
	}

	s := &server{
		consumers:    make(map[string]*Consumer),
		ephemerals:   make(map[string]*Consumer),
		durablesMeta: make(map[string]*ConsumerMeta),
		durables:     make(map[string]map[string]*Consumer),
		eventsLog:    eventLogs,
		consumersLog: consumerLogs,
		tasks:        task.NewRunner(),
	}

	// load durable consumers
	stream, err := s.consumersLog.Stream(context.Background())
	if err != nil {
		return nil, err
	}
	defer stream.Done()

	for {
		r, _, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		var meta ConsumerMeta
		if err = meta.decode(r); err != nil {
			return nil, err
		}

		s.durablesMeta[meta.Name] = &meta
	}

	return s, nil
}

func MatchSubject(subject, pattern string) bool {
	i, j := 0, 0
	subLen, patLen := len(subject), len(pattern)

	for i < subLen && j < patLen {
		subStart, patStart := i, j

		// Advance index to next dot in subject
		for i < subLen && subject[i] != '.' {
			i++
		}
		// Advance index to next dot in pattern
		for j < patLen && pattern[j] != '.' {
			j++
		}

		subPart := subject[subStart:i]
		patPart := pattern[patStart:j]

		// Handle ">" (catch-all) in pattern
		if patPart == ">" {
			return true // matches anything that follows
		}

		// Handle "*" (single element wildcard)
		if patPart != "*" && subPart != patPart {
			return false
		}

		// Skip the dots
		i++
		j++
	}

	// If pattern contains ">", it's valid to have remaining parts in subject
	if j < patLen && pattern[j:] == ">" {
		return true
	}

	// Ensure both subject and pattern are fully processed
	return i >= subLen && j >= patLen
}
