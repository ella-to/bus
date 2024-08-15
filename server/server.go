package server

import (
	"context"
	"net/http"

	"ella.to/bus"
	"ella.to/bus/internal/trie"
	"ella.to/bus/storage"
)

type Server struct {
	consumers            *trie.Node[string]
	consumersBatchEvents map[string]chan []*bus.Event
	storage              storage.Storage
	dispatcher           *Dispatcher
	mux                  *http.ServeMux
	close                chan struct{}
}

func (s *Server) Close() {
	s.dispatcher.Close()
	close(s.close)
	s.storage.Close()
}

func New(ctx context.Context, storage storage.Storage) *Server {
	s := &Server{
		consumers:            trie.New[string](),
		consumersBatchEvents: make(map[string]chan []*bus.Event),
		storage:              storage,
		mux:                  http.NewServeMux(),
		close:                make(chan struct{}),
	}

	s.dispatcher = NewDispatcher(
		1000, // buffer size
		1000, // pool size

		s.putEvent,
		s.registerConsumer,
		s.pushEvent,
		s.ackEvent,
		s.deleteConsumer,
	)

	s.RegisterHandlers()

	return s
}
