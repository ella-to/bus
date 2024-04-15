package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"time"

	"ella.to/bus.go"
	"ella.to/bus.go/internal/sse"
)

type Client struct {
	addr string
	http *http.Client
}

var _ bus.Stream = (*Client)(nil)

func (c *Client) Publish(ctx context.Context, evt *bus.Event) error {
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

	for _, err := range c.Consume(ctx, bus.WithSubject(evt.Reply), bus.WithFromBeginning()) {
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

	qs.Set("subject", consumer.Subject)
	if consumer.Id != "" {
		qs.Set("id", consumer.Id)
	}
	if consumer.Queue != "" {
		qs.Set("queue", consumer.Queue)
	}
	if consumer.LastEventId != "" {
		qs.Set("pos", consumer.LastEventId)
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
			var confirmEvent *bus.Event

			switch msg.Event {
			case "event":
				evt = &bus.Event{}
				err = json.Unmarshal(msg.Data, evt)
				if err == nil && evt.ReplyCount > 0 {
					confirmEvent, err = bus.NewEvent(bus.WithSubject(evt.Reply), bus.WithData(""))
					if err == nil {
						err = c.Publish(ctx, confirmEvent)
					}
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

func New(addr string) *Client {
	return &Client{
		addr: addr,
		http: &http.Client{},
	}
}

func newIterErr(err error) iter.Seq2[*bus.Event, error] {
	return func(yield func(*bus.Event, error) bool) {
		yield(nil, err)
	}
}
