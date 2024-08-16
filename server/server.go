package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"ella.to/bus"
	"ella.to/bus/internal/trie"
	"ella.to/bus/storage"
)

type ServerOpt func(*Server)

func WithActionBufferSize(size int) ServerOpt {
	return func(s *Server) {
		s.bufferSize = size
	}
}

func WithActionPoolSize(size int) ServerOpt {
	return func(s *Server) {
		s.poolSize = size
	}
}

func WithDeleteEventFrequency(d time.Duration) ServerOpt {
	return func(s *Server) {
		s.deleteEventFrequency = d
	}
}

type Server struct {
	consumers            *trie.Node[string]
	queuedConsumers      *trie.Node[string]
	consumersBatchEvents map[string]chan []*bus.Event
	storage              storage.Storage
	dispatcher           *Dispatcher
	mux                  *http.ServeMux
	close                chan struct{}
	bufferSize           int
	poolSize             int
	deleteEventFrequency time.Duration
}

func (s *Server) Close() {
	s.dispatcher.Close()
	close(s.close)
	s.storage.Close()
}

func New(ctx context.Context, storage storage.Storage, opts ...ServerOpt) *Server {
	eq := func(a, b string) bool {
		return a == b
	}

	s := &Server{
		consumers:            trie.New[string](eq),
		queuedConsumers:      trie.New[string](eq),
		consumersBatchEvents: make(map[string]chan []*bus.Event),
		storage:              storage,
		mux:                  http.NewServeMux(),
		close:                make(chan struct{}),
		bufferSize:           1000,
		poolSize:             100,
		deleteEventFrequency: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.dispatcher = NewDispatcher(
		s.bufferSize,
		s.poolSize,

		withPutEventFunc(s.putEvent),
		withRegisterConsumerFunc(s.registerConsumer),
		withPushEventFunc(s.pushEvent),
		withAckEventFunc(s.ackEvent),
		withDeleteConsumerFunc(s.deleteConsumer),
		withDeleteExpiredEventsFunc(s.deleteExpiredEvents),
	)

	s.RegisterHandlers()

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.Close()
				return
			case <-time.After(s.deleteEventFrequency):
				err := s.dispatcher.DeleteExpiredEvents(context.Background())
				if err != nil {
					slog.Error("failed to delete expired events", "error", err)
				}
			}
		}
	}()

	return s
}
