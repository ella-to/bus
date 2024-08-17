// Description: HTTP server for the bus package.
// The server is responsible for handling HTTP requests and responses.
// The server receives requests from clients and prepare consumer and event objects
// and dispatches them to the actions queue. In this way, the server is decoupled from logic and
// maintain proper synchronization between the actions and the server.
package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ella.to/bus"
	"ella.to/sse"
)

var _ http.Handler = (*Server)(nil)

func (s *Server) publishHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	ctx := r.Context()

	evt := &bus.Event{}

	evt.Subject = r.Header.Get("Event-Subject")
	if evt.Subject == "" {
		http.Error(w, "event must have a subject", http.StatusBadRequest)
		return
	}

	evt.Id = bus.GetEventId()
	evt.Reply = r.Header.Get("Event-Reply")
	evt.ReplyCount = parseInt64(r.Header.Get("Event-Reply-Count"), 0)
	evt.ExpiresAt = parseDate(r.Header.Get("Event-Expires-At"), bus.GetDefaultExpiresAt())
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	evt.Data = string(data)
	evt.CreatedAt = time.Now()

	err = s.dispatcher.PutEvent(ctx, evt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.dispatcher.PushEvent(ctx, "", evt.Id)

	header := w.Header()

	header.Set("Event-Id", evt.Id)
	header.Set("Event-Created-At", evt.CreatedAt.Format(time.RFC3339))
	header.Set("Event-Expires-At", evt.ExpiresAt.Format(time.RFC3339))

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) consumerHandler(w http.ResponseWriter, r *http.Request) {
	var consumer bus.Consumer

	defer func() {
		// delete consumer if it's ephemeral or queue consumer
		if consumer.Id == "" || consumer.Type == bus.Durable {
			err := s.dispatcher.OfflineConsumer(context.Background(), consumer.Id)
			if err != nil {
				slog.Error("failed to offline consumer", "consumer_id", consumer.Id)
			}
			return
		}

		err := s.dispatcher.DeleteConsumer(context.Background(), consumer.Id)
		if err != nil {
			slog.Error("failed to delete consumer", "consumer_id", consumer.Id)
		}
	}()

	ctx := r.Context()
	qs := r.URL.Query()

	subject := qs.Get("subject")
	if subject == "" {
		http.Error(w, "missing subject", http.StatusBadRequest)
		return
	}

	if strings.Contains(subject, ">") && !strings.HasSuffix(subject, ">") {
		http.Error(w, "invalid subject, > must be always last character", http.StatusBadRequest)
		return
	}

	consumer.Id = qs.Get("id")
	consumer.Type = bus.ParseConsumerType(qs.Get("type"), bus.Ephemeral)
	consumer.Subject = subject
	consumer.QueueName = qs.Get("queue_name")

	if consumer.Type == bus.Queue {
		if consumer.QueueName == "" {
			http.Error(w, "missing queue name", http.StatusBadRequest)
			return
		}
		consumer.Id = bus.GetConsumerId()
	} else if consumer.Id == "" {
		consumer.Id = bus.GetConsumerId()
	}

	consumer.BatchSize = bus.ParseBatchSize(qs.Get("batch_size"), 1)
	consumer.LastEventId = qs.Get("pos")
	consumer.UpdatedAt = time.Now()

	batchEvents, err := s.dispatcher.RegisterConsumer(ctx, &consumer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.dispatcher.PushEvent(ctx, consumer.Id, "")

	pusher, err := sse.CreatePusher(
		w,
		sse.WithHeader("Consumer-Id", consumer.Id),
		sse.WithHeader("Consumer-Subject", consumer.Subject),
		sse.WithHeader("Consumer-Type", consumer.Type.String()),
		sse.WithHeader("Consumer-Batch-Size", strconv.FormatInt(consumer.BatchSize, 10)),
		sse.WithHeader("Consumer-Queue-Name", consumer.QueueName),
		sse.WithHeader("Consumer-Last-Event-Id", consumer.LastEventId),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Done(ctx)

	for {
		select {
		case <-s.close:
			return
		case <-ctx.Done():
			return
		case events, ok := <-batchEvents:
			if !ok {
				return
			}

			err = pusher.Push(ctx, "event", events)
			if err != nil {
				slog.Error("failed to push events", "err", err)
				return
			}
		}
	}
}

func (s *Server) ackedHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	qs := r.URL.Query()

	eventId := qs.Get("event_id")
	if eventId == "" {
		http.Error(w, "missing event_id", http.StatusBadRequest)
		return
	}

	consumerId := qs.Get("consumer_id")
	if consumerId == "" {
		http.Error(w, "missing consumer_id", http.StatusBadRequest)
		return
	}

	err := s.dispatcher.AckEvent(ctx, consumerId, eventId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.dispatcher.PushEvent(ctx, consumerId, "")

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	qs := r.URL.Query()

	consumerId := qs.Get("id")

	err := s.dispatcher.DeleteConsumer(ctx, consumerId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) RegisterHandlers() {
	s.mux.HandleFunc("POST /put", s.publishHandler)       // POST /put
	s.mux.HandleFunc("GET /get", s.consumerHandler)       // GET /get?subject=foo&type=queue&id=bar&pos=oldest|newest|<event_id>&ack=auto|manual&batch_size=1
	s.mux.HandleFunc("GET /ack", s.ackedHandler)          // GET /ack?consumer_id=123&event_id=456
	s.mux.HandleFunc("DELETE /consumer", s.deleteHandler) // DELETE /consumer?id=123
}

func parseDate(s string, defaultValue time.Time) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return defaultValue
	}
	return t
}

func parseInt64(s string, defaultValue int64) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultValue
	}
	return i
}
