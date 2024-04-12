package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"ella.to/bus.go"
	"ella.to/bus.go/internal/db"
	"ella.to/bus.go/internal/sqlite"
	"ella.to/bus.go/internal/sse"
	"ella.to/bus.go/internal/track"
)

type incomingEvent struct {
	event      *bus.Event
	consumerId string
}

type Server struct {
	mux            http.ServeMux
	dbw            *sqlite.Worker
	consumersMap   *bus.ConsumersEventMap
	incomingEvents chan *incomingEvent
	tick           track.TickFunc
}

var _ http.Handler = &Server{}

func (s *Server) publishHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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

	evt.Id = bus.GetEventId()
	evt.CreatedAt = time.Now()

	err = s.appendEvents(ctx, evt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	header := w.Header()

	header.Set("Event-Id", evt.Id)
	header.Set("Event-CreatedAt", evt.CreatedAt.Format(time.RFC3339))
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) consumeHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	ctx := r.Context()
	qs := r.URL.Query()

	subject := qs.Get("subject")
	if subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}
	subject = strings.ReplaceAll(subject, "*", "%")

	queueName := qs.Get("queue")
	isQueue := queueName != ""

	isDurable := qs.Has("durable")

	if isDurable && isQueue {
		http.Error(w, "durable and queue cannot be used together", http.StatusBadRequest)
		return
	}

	id := qs.Get("id")

	if id != "" && isQueue {
		http.Error(w, "id cannot be used with queue", http.StatusBadRequest)
		return
	}

	if id == "" {
		id = bus.GetConsumerId()
	}

	defer func() {
		s.consumersMap.Remove(id)
		// NOTE: queue consumers, should be deleted upon disconnection
		// there is no need to store them in the database
		if isQueue || !isDurable {
			err := s.deleteConsumer(context.Background(), id)
			if err != nil {
				slog.Error("failed to delete consumer", "consumer_id", id, "error", err)
			}
		}
	}()

	// NOTE: id of consumer will be sent back to the client using `Consumer-Id` http.Header key
	// this is useful for the client to reconnect to the same consumer if id is auto-generated
	w.Header().Add("Consumer-Id", id)

	// position can be
	// 1: normal (Default) - start from the last event id that was consumed
	// 2: all - start from the beginning
	// 3: <event_id> - start from the last event id provided
	// NOTE: if either durable or queue is set, position is only allowed to be used once during creation
	// subsequent request will be ignored
	pos := qs.Get("pos")
	if pos == "" {
		pos = "normal"
	}

	lastEventId, err := s.getLastEventId(ctx, pos)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isQueue {
		_, err = s.createOrGetQueue(ctx, queueName, lastEventId, pos)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// NOTE: since queue will be keep track of lastEventId, we can set it to empty
		// so the consumer doens't get confused
		// lastEventId = ""
	}

	c, err := s.createOrGetConsumer(ctx, id, subject, queueName, lastEventId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pusher, err := sse.Out(w, sse.OutWithHeader("Consumer-Id", id))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Done(ctx)

Outerloop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			event, err := s.getNextEvent(ctx, c.Id, lastEventId)
			if errors.Is(err, db.ErrEventNotFound) {
				break Outerloop
			} else if err != nil {
				pusher.Push(ctx, "error", err)
				continue
			}

			lastEventId = event.Id

			err = pusher.Push(ctx, "event", event)
			if err != nil {
				slog.Error("failed to push event", "error", err)
				return
			}

			err = s.ackEvent(ctx, c.Id, event.Id)
			if err != nil {
				slog.Error("failed to ack event", "error", err)
				pusher.Push(ctx, "error", err)
				return
			}
		}
	}

	events := s.consumersMap.Add(id)
	defer s.consumersMap.Remove(id)

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			pusher.Push(ctx, "event", event)
			s.ackEvent(ctx, c.Id, event.Id)
		}
	}
}

func (s *Server) Close() {
	close(s.incomingEvents)
	s.incomingEvents = nil
}

func (s *Server) notify() func(string, *bus.Event) {
	go func() {
		for incomingEvent := range s.incomingEvents {
			s.consumersMap.Push(incomingEvent.consumerId, incomingEvent.event)
		}
	}()

	return func(consumerId string, event *bus.Event) {
		select {
		case s.incomingEvents <- &incomingEvent{
			event:      event,
			consumerId: consumerId,
		}:
		default:
			slog.Error("failed to push event to consumers map", "consumer_id", consumerId, "event_id", event.Id)
		}
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

//
// OPTIONS
//

type serverOpt struct {
	dbPoolSize               int
	dbPath                   string
	consumerQueueSize        int
	incomingEventsBufferSize int
	workerSize               int
	tickTimeout              time.Duration
	tickSize                 int
}

type Opt interface {
	configureServer(*serverOpt) error
}

type optFn func(*serverOpt) error

func (opt optFn) configureServer(s *serverOpt) error {
	return opt(s)
}

func WithDBPoolSize(size int) Opt {
	return optFn(func(s *serverOpt) error {
		s.dbPoolSize = size
		return nil
	})
}

func WithDBPath(path string) Opt {
	return optFn(func(s *serverOpt) error {
		s.dbPath = path
		return nil
	})
}

func WithConsumerQueueSize(size int) Opt {
	return optFn(func(s *serverOpt) error {
		s.consumerQueueSize = size
		return nil
	})
}

func WithIncomingEventsBufferSize(size int) Opt {
	return optFn(func(s *serverOpt) error {
		s.incomingEventsBufferSize = size
		return nil
	})
}

func WithAckTick(timeout time.Duration, size int) Opt {
	return optFn(func(s *serverOpt) error {
		s.tickTimeout = timeout
		s.tickSize = size
		return nil
	})
}

func New(ctx context.Context, opts ...Opt) (*Server, error) {
	conf := &serverOpt{
		dbPoolSize:               10,
		dbPath:                   "",
		consumerQueueSize:        1000,
		incomingEventsBufferSize: 100_000,
		tickTimeout:              5 * time.Second,
		tickSize:                 100,
	}

	for _, opt := range opts {
		err := opt.configureServer(conf)
		if err != nil {
			return nil, err
		}
	}

	// NOTE: worker size is the same as db pool size
	// to prevent deadlock
	conf.workerSize = conf.dbPoolSize

	s := &Server{
		consumersMap:   bus.NewConsumersEventMap(conf.consumerQueueSize),
		incomingEvents: make(chan *incomingEvent, conf.incomingEventsBufferSize),
		tick:           track.Create(conf.tickTimeout, conf.tickSize),
	}

	dbOpts := []sqlite.OptionFunc{
		sqlite.WithPoolSize(conf.dbPoolSize),
	}

	if conf.dbPath != "" {
		dbOpts = append(dbOpts, sqlite.WithFile(conf.dbPath))
	} else {
		dbOpts = append(dbOpts, sqlite.WithMemory())
	}

	db, err := db.New(ctx, s.notify(), dbOpts...)
	if err != nil {
		return nil, err
	}

	s.dbw = sqlite.NewWorker(db, int64(conf.workerSize*100), int64(conf.workerSize))

	s.mux.HandleFunc("POST /", s.publishHandler)
	s.mux.HandleFunc("GET /", s.consumeHandler)

	if err != nil {
		return nil, err
	}

	return s, nil
}
