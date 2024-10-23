package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
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

	consumer := &Consumer{
		meta: &ConsumerMeta{},
	}
	consumer.meta.Name = qs.Get("name")
	consumer.meta.Position = qs.Get("position")
	consumer.meta.Subject = qs.Get("subject")

	checkDelay, err := time.ParseDuration(qs.Get("check_delay"))
	if err != nil || checkDelay <= 0 {
		checkDelay = DefaultCheckDelay
	}

	redeliveryDelay, err := time.ParseDuration(qs.Get("redelivery_delay"))
	if err != nil || redeliveryDelay <= 0 {
		redeliveryDelay = DefaultRedeliveryDelay
	}

	consumer.meta.CheckDelay = checkDelay
	consumer.meta.RedeliveryDelay = redeliveryDelay

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

func NewHandler(ctx context.Context, dirPath string) (*Handler, error) {
	s, err := newServer(ctx, dirPath)
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

type server struct {
	// consumers is a map of all consumers, both ephemeral and durable
	// this is useful during acking events
	consumers      map[string]*Consumer            // id -> consumer
	ephemerals     map[string]*Consumer            // id -> consumer
	durablesMeta   map[string]*ConsumerMeta        // name -> meta
	durables       map[string]map[string]*Consumer // name -> id -> consumer
	eventsLog      *immuta.Storage                 // store all events
	consumersLog   *immuta.Storage                 // store durable consumers
	tasks          task.Runner
	buffer         bytes.Buffer
	durablesRunner map[string]struct{} // consumer.id -> struct{} to keep track which consumer is running
}

func (s *server) Subscribe(ctx context.Context, consumer *Consumer) error {
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
				return nil
			default:
			}

			// we haven't received an ack for the last message yet, should we send it again?
			if consumer.meta.WaitingAckFor != "" && !consumer.meta.WaitingAckExpiredAt.Before(time.Now()) {
				return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
			}

			stream, err := s.eventsLog.Stream(ctx, immuta.WithAbsolutePosition(consumer.meta.CurrentIndex))
			if err != nil {
				slog.ErrorContext(ctx, "failed to create a stream from the events log", "id", consumer.id, "subject", consumer.meta.Subject, "error", err)
				consumer.forceDisconnect()
				return err
			}
			defer stream.Done()

			r, size, err := stream.Next()
			// size == 0 is a special case, it means that header hasn't been written yet and need more time
			if errors.Is(err, io.EOF) || size == 0 {
				// there is no more events in the stream, so we yeild the task and hope next time there will be some events
				return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
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
				consumer.meta.CurrentIndex += size + immuta.HeaderSize
				return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
			}

			consumer.meta.CurrentEventSize = size

			if err = consumer.pusher.Push(ctx, "event", &event); err != nil {
				slog.ErrorContext(ctx, "failed to push an event to consumer", "id", consumer.id, "subject", consumer.meta.Subject, "event_id", event.Id, "error", err)
				consumer.forceDisconnect()
				return err
			}

			consumer.meta.WaitingAckFor = event.Id
			consumer.meta.WaitingAckExpiredAt = time.Now().Add(consumer.meta.RedeliveryDelay)

			// this yeild is a loop like, it will put this function back to the queue
			// so other items in the queue can be processed, we also add a delay
			// Eventually the delay value can be configured by the user
			return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
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
				CheckDelay:          consumer.meta.CheckDelay,
				RedeliveryDelay:     consumer.meta.RedeliveryDelay,
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
		consumer.meta.CheckDelay = durableMeta.CheckDelay
		consumer.meta.RedeliveryDelay = durableMeta.RedeliveryDelay

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

		// we store which consumer is running the task for this group
		// and once this consumer is disconnected, we can start the task again
		// for another consumer in the group
		s.durablesRunner[consumer.id] = struct{}{}

		s.durableLoop(ctx, consumer)

		return nil
	}).Await(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *server) durableLoop(ctx context.Context, consumer *Consumer) {
	s.tasks.Submit(ctx, func(ctx context.Context) error {
		// check if there is at least one consumer in the group
		// if not we can return immediately and that will remove the task from the queue
		if len(s.durables[consumer.meta.Name]) == 0 {
			return nil
		}

		var requiredSave bool

		defer func() {
			if !requiredSave {
				return
			}

			meta, ok := s.durablesMeta[consumer.meta.Name]
			if !ok {
				return
			}

			s.buffer.Reset()
			err := meta.encode(&s.buffer)
			if err != nil {
				slog.ErrorContext(ctx, "failed to encode durable consumer meta data", "name", consumer.meta.Name, "error", err)
				return
			}

			_, err = s.consumersLog.Append(&s.buffer)
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
			return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
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
		// size == 0 is a special case, it means that header hasn't been written yet and need more time
		if errors.Is(err, io.EOF) || size == 0 {
			// there is no more events in the stream, so we yeild the task and hope next time there will be some events
			return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
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
			oldIndex := meta.CurrentIndex
			meta.CurrentIndex += size + immuta.HeaderSize
			if oldIndex != meta.CurrentIndex {
				requiredSave = true
			}
			return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
		}

		meta.CurrentEventSize = size
		requiredSave = true

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
		meta.WaitingAckExpiredAt = time.Now().Add(consumer.meta.RedeliveryDelay)

		return task.Yeild(ctx, task.WithDelay(consumer.meta.CheckDelay))
	})
}

func (s *server) Unsubscribe(ctx context.Context, consumer *Consumer) error {
	return s.tasks.Submit(context.Background(), func(ctx context.Context) error {
		delete(s.consumers, consumer.id)

		if consumer.meta.Name == "" {
			delete(s.ephemerals, consumer.id)
			return nil
		}

		_, isConsumerRunner := s.durablesRunner[consumer.id]

		if _, ok := s.durables[consumer.meta.Name]; !ok {
			return nil
		}

		delete(s.durables[consumer.meta.Name], consumer.id)
		if len(s.durables[consumer.meta.Name]) == 0 {
			delete(s.durables, consumer.meta.Name)
		}

		if isConsumerRunner {
			// check if there is another consumer in the group
			// if so, we need to start the task for that consumer
			groups, ok := s.durables[consumer.meta.Name]
			if !ok {
				return nil
			}

			if len(groups) == 0 {
				return nil
			}

			// pick the first consumer in the group
			for _, consumer := range groups {
				s.durablesRunner[consumer.id] = struct{}{}
				s.durableLoop(ctx, consumer)
				return nil
			}
		}

		return nil
	}).Await(ctx)
}

func (s *server) SaveEvent(ctx context.Context, event *Event) error {

	return s.tasks.Submit(ctx, func(ctx context.Context) error {
		event.Id = newEventId()
		event.CreatedAt = time.Now()

		s.buffer.Reset()
		err := event.encode(&s.buffer)
		if err != nil {
			slog.ErrorContext(ctx, "failed to encode event", "id", event.Id, "error", err)
			return err
		}

		_, err = s.eventsLog.Append(&s.buffer)
		if err != nil {
			slog.ErrorContext(ctx, "failed to append event to the log", "id", event.Id, "error", err)
		}

		return err
	}).Await(ctx)
}

func (s *server) AckEvent(ctx context.Context, consumerId string, eventId string) error {

	return s.tasks.Submit(ctx, func(ctx context.Context) error {
		consumer, ok := s.consumers[consumerId]
		if !ok {
			return fmt.Errorf("consumer %s not found", consumerId)
		}

		if consumer.meta.Name == "" {
			if consumer.meta.WaitingAckFor != eventId {
				return fmt.Errorf("consumer %s is not waiting for event %s", consumerId, eventId)
			}

			consumer.meta.AckedCount++
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
			meta.AckedCount++
			meta.CurrentIndex += meta.CurrentEventSize + immuta.HeaderSize
			meta.CurrentEventSize = 0

			for _, consumer := range s.durables[consumer.meta.Name] {
				consumer.meta.WaitingAckFor = ""
				consumer.meta.CurrentIndex = meta.CurrentIndex
				consumer.meta.CurrentEventSize = meta.CurrentEventSize
			}

			s.buffer.Reset()
			err := meta.encode(&s.buffer)
			if err != nil {
				slog.ErrorContext(ctx, "failed to encode durable consumer meta data", "name", consumer.meta.Name, "error", err)
				return err
			}

			_, err = s.consumersLog.Append(&s.buffer)
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

func newServer(ctx context.Context, dirPath string) (*server, error) {
	eventLogs, err := immuta.New(
		immuta.WithQueueSize(10),
		immuta.WithFastWrite(filepath.Join(dirPath, "events.log")),
	)
	if err != nil {
		return nil, err
	}

	s := &server{
		consumers:      make(map[string]*Consumer),
		ephemerals:     make(map[string]*Consumer),
		durablesMeta:   make(map[string]*ConsumerMeta),
		durables:       make(map[string]map[string]*Consumer),
		eventsLog:      eventLogs,
		tasks:          task.NewRunner(),
		durablesRunner: make(map[string]struct{}),
	}

	// load durable consumers
	// Since we are using a log file to store durable consumers, the state of the durable consumers
	// it is required to read the log file and load the state of the durable consumers
	// and then flush the state back to the log file
	err = func() error {
		consumersLog, err := immuta.New(
			immuta.WithQueueSize(10),
			immuta.WithFastWrite(filepath.Join(dirPath, "consumers.log")),
		)
		if err != nil {
			return err
		}
		defer func() {
			consumersLog.Close()
			// rename the log file to a new name for backup purposes
			os.Rename(filepath.Join(dirPath, "consumers.log"), filepath.Join(dirPath, fmt.Sprintf("consumers.%s.log", time.Now().Format("2006-01-02T15:04:05"))))
		}()

		stream, err := consumersLog.Stream(context.Background())
		if err != nil {
			return err
		}
		defer stream.Done()

		for {
			r, _, err := stream.Next()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return err
			}

			var meta ConsumerMeta
			if err = meta.decode(r); err != nil {
				return err
			}

			s.durablesMeta[meta.Name] = &meta
		}

		return nil
	}()
	if err != nil {
		return nil, err
	}

	// write back durable consumers into a new log file
	// we do this to make sure that we have the latest state of the durable consumers
	err = func() error {
		consumersLog, err := immuta.New(
			immuta.WithQueueSize(10),
			immuta.WithFastWrite(filepath.Join(dirPath, "consumers.log")),
		)
		if err != nil {
			return err
		}

		var buffer bytes.Buffer

		for _, meta := range s.durablesMeta {
			buffer.Reset()
			err := meta.encode(&buffer)
			if err != nil {
				return err
			}

			_, err = consumersLog.Append(&buffer)
			if err != nil {
				return err
			}
		}

		s.consumersLog = consumersLog
		return nil
	}()

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
