package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/url"

	"ella.to/sse"
)

type Client struct {
	http *http.Client
	host string
}

var _ Putter = (*Client)(nil)
var _ Getter = (*Client)(nil)
var _ Acker = (*Client)(nil)

func (c *Client) Put(ctx context.Context, opts ...PutOpt) (Response, error) {
	opt := &putOpt{}
	for _, o := range opts {
		if err := o.configurePut(opt); err != nil {
			return nil, err
		}
	}

	evt := opt.event

	pr, pw := io.Pipe()
	go func() {
		err := evt.encode(pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	url := c.host + "/put"

	req, err := http.NewRequest(http.MethodPost, url, pr)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return nil, newReaderError(resp.Body)
	}

	// TODO: we need to implement the Response type
	// which is confirm count and Request/Reply

	return nil, nil
}

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

	qs := url.Values{}
	qs.Set("subject", opt.consumer.meta.Subject)
	qs.Set("position", opt.consumer.meta.Position)
	qs.Set("name", opt.consumer.meta.Name)
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

	ctx, cancel := context.WithCancel(ctx)

	incomings := sse.Receive(ctx, resp.Body)

	return func(yield func(*Event, error) bool) {
		defer cancel()

		var evt Event

		for incoming := range incomings {
			slog.InfoContext(ctx, "received an incoming")

			switch incoming.Event {
			case "event":
				err = evt.decode(bytes.NewReader(incoming.Data))
				if err != nil {
					yield(nil, err)
					return
				}

				evt.consumerId = consumerId
				evt.acker = c

				if !yield(&evt, nil) {
					return
				}
			case "error":
				yield(nil, fmt.Errorf("%s", incoming.Data))
				return
			case "done":
				return
			}
		}
	}
}

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