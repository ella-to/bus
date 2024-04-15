package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

type Pusher interface {
	Push(ctx context.Context, id string, data any) error
	Done(ctx context.Context) error
}

type pusher struct {
	w   http.ResponseWriter
	out http.Flusher
	id  int
}

var _ Pusher = (*pusher)(nil)

func (p *pusher) Push(ctx context.Context, event string, data any) error {
	p.id++

	if err, ok := data.(error); ok {
		data = struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		}
		event = "error"
	}

	p.w.Write(idPrefix)
	p.w.Write([]byte(strconv.Itoa(p.id)))
	p.w.Write(singleEnter)

	p.w.Write(eventPrefix)
	p.w.Write([]byte(event))
	p.w.Write(singleEnter)

	p.w.Write(dataPrefix)
	err := json.NewEncoder(p.w).Encode(data)
	if err != nil {
		return err
	}
	p.w.Write(singleEnter)

	p.out.Flush()

	return nil
}

func (p *pusher) Done(ctx context.Context) error {
	return p.Push(ctx, "done", struct{}{})
}

type pusherOptions struct {
	headers map[string]string
}

type OptionFunc func(*pusherOptions)

func WithHeader(key, value string) OptionFunc {
	return func(o *pusherOptions) {
		if o.headers == nil {
			o.headers = make(map[string]string)
		}
		o.headers[key] = value
	}
}

func CreatePusher(w http.ResponseWriter, argsFns ...OptionFunc) (*pusher, error) {
	opts := &pusherOptions{
		headers: make(map[string]string),
	}
	for _, fn := range argsFns {
		fn(opts)
	}

	out, ok := w.(http.Flusher)
	if !ok {
		return nil, http.ErrNotSupported
	}

	for key, value := range opts.headers {
		w.Header().Set(key, value)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	out.Flush()

	return &pusher{
		w:   w,
		out: out,
	}, nil
}
