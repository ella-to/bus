package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"ella.to/sse"
)

type Client struct {
	http *http.Client
	host string
}

var (
	_ Putter = (*Client)(nil)
	_ Getter = (*Client)(nil)
	_ Acker  = (*Client)(nil)
)

// POST /
func (c *Client) Put(ctx context.Context, opts ...PutOpt) *Response {
	opt := &putOpt{}
	for _, o := range opts {
		if err := o.configurePut(opt); err != nil {
			return &Response{err: err}
		}
	}

	// If batch mode is enabled, ensure there are no other top-level options set
	if opt.hasBatch {
		// disallow mixing batch with other options
		if opt.event.Subject != "" || opt.event.Key != "" || opt.event.ResponseSubject != "" || opt.event.TraceId != "" || opt.event.Id != "" || !opt.event.CreatedAt.IsZero() || opt.confirmCount != 0 {
			return &Response{err: errors.New("cannot mix batch with other options")}
		}

		if len(opt.batch) == 0 {
			return &Response{err: errors.New("batch has no items")}
		}

		body, err := json.Marshal(opt.batch)
		if err != nil {
			return &Response{err: err}
		}

		url := c.host + "/"
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return &Response{err: err}
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return &Response{err: err}
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			return &Response{err: newReaderError(resp.Body)}
		}

		// Success for batch: don't expose single event headers
		return &Response{}
	}

	// single event path (unchanged)

	url := c.host + "/"

	req, err := http.NewRequest(http.MethodPost, url, &opt.event)
	if err != nil {
		return &Response{err: err}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return &Response{err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return &Response{err: newReaderError(resp.Body)}
	}

	id := resp.Header.Get(HeaderEventId)
	createdAt, err := time.Parse(time.RFC3339Nano, resp.Header.Get(HeaderEventCreatedAt))
	if err != nil {
		return &Response{err: err}
	}
	index, err := strconv.ParseInt(resp.Header.Get(HeaderEventIndex), 10, 64)
	if err != nil {
		return &Response{err: err}
	}

	response := &Response{
		Id:        id,
		CreatedAt: createdAt,
		Index:     index,
	}

	if opt.event.ResponseSubject == "" {
		return response
	}

	waitingForConfirm := opt.confirmCount > 0

	for event, err := range c.Get(
		ctx,
		WithSubject(opt.event.ResponseSubject),
		WithStartFrom(StartOldest),
		WithAckStrategy(AckNone),
	) {
		if err != nil {
			response.err = err
			return response
		}

		if !waitingForConfirm {
			response.Payload = event.Payload
			return response
		}

		opt.confirmCount--
		if opt.confirmCount == 0 {
			return response
		}
	}

	return response
}

// GET /?subject=...&start=...&ack=...&redelivery=...
func (c *Client) Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error] {
	opt := &getOpt{}
	for _, o := range opts {
		if err := o.configureGet(opt); err != nil {
			return newIterError(err)
		}
	}

	addr, err := url.Parse(c.host)
	if err != nil {
		return newIterError(err)
	}

	autoAck := opt.ackStrategy == "auto"
	if autoAck {
		opt.ackStrategy = AckManual
	}

	qs := url.Values{}
	qs.Set("subject", opt.subject)
	qs.Set("start", opt.start)
	qs.Set("ack", opt.ackStrategy)
	qs.Set("redelivery", opt.redelivery.String())
	addr.RawQuery = qs.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr.String(), nil)
	if err != nil {
		return newIterError(err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return newIterError(err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return newIterError(newReaderError(resp.Body))
	}

	consumerId := resp.Header.Get(HeaderConsumerId)

	// call the meta function if it's set
	// this is useful if the golang client wants to get access to the consumer id
	// for example, to ack the event using cli
	if opt.metaFn != nil {
		opt.metaFn(map[string]string{
			"consumer-id": consumerId,
		})
	}

	ctx, cancel := context.WithCancel(ctx)

	incomings := sse.NewReceiver(resp.Body)

	return func(yield func(*Event, error) bool) {
		defer cancel()

		event := Event{
			acker:      c,
			putter:     c,
			consumerId: consumerId,
		}

		for {
			msg, err := incomings.Receive(ctx)
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}

			switch msg.Event {
			case msgType:
				if msg.Data == "" {
					continue
				}

				_, err = event.Write([]byte(msg.Data))
				if err != nil {
					if !yield(nil, fmt.Errorf("failed to write event '%s': %w", msg.Data, err)) {
						return
					}
					continue
				}

				if !yield(&event, nil) {
					return
				}

				if autoAck {
					if err := event.Ack(ctx); err != nil {
						if !yield(nil, err) {
							return
						}
					}
				}

			case errorType:
				if msg.Data == "" {
					continue
				}

				if !yield(nil, fmt.Errorf("%s", msg.Data)) {
					return
				}

			case doneType:
				return
			}
		}
	}
}

// PUT /ack?consumer_id=...&event_id=...
func (c *Client) Ack(ctx context.Context, consumerId string, eventId string) error {
	addr, err := url.Parse(c.host)
	if err != nil {
		return err
	}

	qs := url.Values{}
	qs.Set("consumer_id", consumerId)
	qs.Set("event_id", eventId)
	addr.RawQuery = qs.Encode()

	req, err := http.NewRequest(http.MethodPut, addr.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return newReaderError(resp.Body)
	}

	return nil
}

func NewClient(host string) *Client {
	return &Client{
		http: &http.Client{},
		host: host,
	}
}

func newReaderError(r io.Reader) error {
	msg, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return errors.New(string(msg))
}

func newIterError(err error) iter.Seq2[*Event, error] {
	return func(yield func(*Event, error) bool) {
		yield(nil, err)
	}
}
