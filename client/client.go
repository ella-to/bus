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

func (c *Client) Put(ctx context.Context, evt *bus.Event) error {
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

	url := c.addr + "/put"

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

	for _, err := range c.Get(ctx, bus.WithSubject(evt.Reply), bus.WithFromOldest()) {
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

func (c *Client) Get(ctx context.Context, consumerOpts ...bus.ConsumerOpt) iter.Seq2[*bus.Msg, error] {
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
	if consumer.BatchSize > 0 {
		qs.Set("batch_size", fmt.Sprintf("%d", consumer.BatchSize))
	}
	if consumer.Durable {
		qs.Set("durable", "true")
	}

	url := c.addr + "/get?" + qs.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return newIterErr(err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return newIterErr(err)
	}

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return newIterErr(err)
		}

		return newIterErr(fmt.Errorf(string(b)))
	}

	header := resp.Header
	consumer.Id = header.Get("Consumer-Id")
	consumer.QueueName = header.Get("Consumer-Queue")
	isAutoAck := header.Get("Consumer-Ack-Strategy") == "auto"

	ctx, cancel := context.WithCancel(ctx)

	incomings := sse.Receive(ctx, resp.Body)

	return func(yield func(*bus.Msg, error) bool) {
		defer func() {
			cancel()
		}()

		for incoming := range incomings {
			var err error
			var msg *bus.Msg
			var events []*bus.Event

			switch incoming.Event {
			case "event":
				err = json.Unmarshal(incoming.Data, &events)
				if err != nil {
					return
				}

				if !isAutoAck {
					msg, err = bus.NewMsg(events, bus.WithInitAck(consumer.Id, c))
					if err != nil {
						return
					}
				} else {
					msg, err = bus.NewMsg(events)
					if err != nil {
						return
					}
				}

				for _, evt := range events {
					if evt.ReplyCount > 0 {
						confirmEvent, err := bus.NewEvent(bus.WithSubject(evt.Reply), bus.WithData([]byte(`{"type":"confirm"}`)))
						if err != nil {
							return
						}
						err = c.Put(ctx, confirmEvent)
						if err != nil {
							return
						}
					}
				}

			case "error":
				events = nil
				msg = nil
				err = fmt.Errorf("%s", incoming.Data)
			case "done":
				return
			}

			if !yield(msg, err) {
				break
			}
		}
	}
}

func (c *Client) Ack(ctx context.Context, consumerId, eventId string) error {
	var url strings.Builder

	url.WriteString(c.addr)
	url.WriteString("/ack?")
	url.WriteString("event_id=")
	url.WriteString(eventId)
	url.WriteString("&consumer_id=")
	url.WriteString(consumerId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf(string(b))
	}

	return nil
}

func (c *Client) Close(ctx context.Context, consumerId string) error {
	var url strings.Builder

	url.WriteString(c.addr)
	url.WriteString("/delete?consumer_id=")
	url.WriteString(consumerId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf(string(b))
	}

	return nil
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

func newIterErr(err error) iter.Seq2[*bus.Msg, error] {
	return func(yield func(*bus.Msg, error) bool) {
		yield(nil, err)
	}
}
