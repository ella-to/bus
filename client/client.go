package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"

	"ella.to/bus"
	"ella.to/sse"
)

type ClientOpt func(*Client) error

func WithScope(scope string) ClientOpt {
	return func(c *Client) error {
		if strings.HasPrefix(scope, ".") || strings.HasSuffix(scope, ".") {
			return fmt.Errorf("scope must not start or end with a dot")
		}
		c.scope = scope
		return nil
	}
}

func WithHTTPClient(http *http.Client) ClientOpt {
	return func(c *Client) error {
		c.http = http
		return nil
	}
}

func WithAddr(addr string) ClientOpt {
	return func(c *Client) error {
		c.addr = addr
		return nil
	}
}

type Client struct {
	addr  string
	http  *http.Client
	scope string
}

var _ bus.Stream = (*Client)(nil)

func (c *Client) Publish(ctx context.Context, evt *bus.Event) error {
	if c.scope != "" {
		evt.Subject = fmt.Sprintf("%s.%s", c.scope, evt.Subject)
		if evt.Reply != "" {
			evt.Reply = fmt.Sprintf("%s.%s", c.scope, evt.Reply)
		}
	}

	pr, pw := io.Pipe()
	go func() {
		err := json.NewEncoder(pw).Encode(evt)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	url := c.addr + "/publish"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to publish event")
	}

	evt.Id = resp.Header.Get("Event-Id")
	evt.CreatedAt, err = time.Parse(time.RFC3339, resp.Header.Get("Event-Created-At"))
	if err != nil {
		return err
	}

	resp.Body.Close()
	if evt.ReplyCount == 0 {
		return nil
	}

	for _, err := range c.Consume(ctx, bus.WithSubject(evt.Reply), bus.WithFromOldest()) {
		if err != nil {
			return err
		}

		evt.ReplyCount--
		if evt.ReplyCount == 0 {
			break
		}
	}

	return nil
}

func (c *Client) Consume(ctx context.Context, consumerOpts ...bus.ConsumerOpt) iter.Seq2[*bus.Event, error] {
	consumer, err := bus.NewConsumer(consumerOpts...)
	if err != nil {
		return newIterErr(err)
	}

	qs := url.Values{}

	qs.Set("subject", consumer.Pattern)
	if consumer.Id != "" {
		qs.Set("id", consumer.Id)
	}
	if consumer.QueueName != "" {
		qs.Set("queue", consumer.QueueName)
	}
	if consumer.LastEventId != "" {
		qs.Set("pos", consumer.LastEventId)
	}
	if consumer.AckStrategy != "" {
		qs.Set("ack_strategy", consumer.AckStrategy)
	}

	url := c.addr + "/consume?" + qs.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return newIterErr(err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return newIterErr(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	msgs := sse.Receive(ctx, resp.Body)

	return func(yield func(*bus.Event, error) bool) {
		defer func() {
			cancel()
		}()

		for msg := range msgs {
			var err error
			var evt *bus.Event

			switch msg.Event {
			case "event":
				evt = &bus.Event{}
				err = json.Unmarshal(msg.Data, evt)
				if err == nil && evt.ReplyCount > 0 {
					confirmEvent, err := bus.NewEvent(bus.WithSubject(evt.Reply), bus.WithData([]byte(`{"type":"confirm"}`)))
					if err != nil {
						return
					}
					err = c.Publish(ctx, confirmEvent)
				}

			case "error":
				evt = nil
				err = fmt.Errorf("%s", msg.Data)
			case "done":
				return
			}

			if !yield(evt, err) {
				break
			}
		}
	}
}

func New(opts ...ClientOpt) (*Client, error) {
	c := &Client{
		addr:  "http://localhost:2021",
		http:  &http.Client{},
		scope: "",
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

func newIterErr(err error) iter.Seq2[*bus.Event, error] {
	return func(yield func(*bus.Event, error) bool) {
		yield(nil, err)
	}
}