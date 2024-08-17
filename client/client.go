package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"ella.to/bus"
	"ella.to/sse"
)

type clientOpt func(*Client)

type Client struct {
	addr string
	http *http.Client
}

func WithHttpClient(http *http.Client) clientOpt {
	return func(c *Client) {
		c.http = http
	}
}

func New(addr string, opts ...clientOpt) *Client {
	c := &Client{
		addr: addr,
		http: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Client) Put(ctx context.Context, evt *bus.Event) error {
	url := c.addr + "/put"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(evt.Data))
	if err != nil {
		return err
	}

	header := req.Header

	header.Set("Event-Subject", evt.Subject)
	header.Set("Event-Reply", evt.Reply)
	header.Set("Event-Reply-Count", strconv.FormatInt(evt.ReplyCount, 10))
	header.Set("Event-Expires-At", evt.ExpiresAt.Format(time.RFC3339))

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return newErrorFromResp(resp)
	}

	evt.Id = resp.Header.Get("Event-Id")
	evt.ExpiresAt, err = time.Parse(time.RFC3339, resp.Header.Get("Event-Expires-At"))
	if err != nil {
		return err
	}
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

	qs.Set("subject", consumer.Subject)
	qs.Set("id", consumer.Id)
	qs.Set("type", consumer.Type.String())
	qs.Set("pos", consumer.LastEventId)
	qs.Set("queue_name", consumer.QueueName)
	qs.Set("batch_size", fmt.Sprintf("%d", consumer.BatchSize))

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
		return newIterErr(newErrorFromResp(resp))
	}

	header := resp.Header

	consumer.Id = header.Get("Consumer-Id")
	consumer.Subject = header.Get("Consumer-Subject")
	consumer.Type = bus.ParseConsumerType(header.Get("Consumer-Type"), bus.Ephemeral)
	consumer.BatchSize = bus.ParseBatchSize(header.Get("Consumer-Batch-Size"), 1)
	consumer.QueueName = header.Get("Consumer-Queue-Name")
	consumer.LastEventId = header.Get("Consumer-Last-Event-Id")

	ctx, cancel := context.WithCancel(req.Context())

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

				replyConfirms := make(map[string]struct{})
				for _, evt := range events {
					if evt.ReplyCount > 0 && evt.Reply != "" {
						replyConfirms[evt.Reply] = struct{}{}
					}
				}

				msg, err = bus.NewMsg(events, bus.WithInitAck(consumer.Id, replyConfirms, c, c))
				if err != nil {
					return
				}

				if !yield(msg, err) {
					return
				}

			case "error":
				events = nil
				msg = nil
				err = fmt.Errorf("%s", incoming.Data)

				if !yield(msg, err) {
					return
				}

			case "done":
				return
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

	if resp.StatusCode != http.StatusAccepted {
		return newErrorFromResp(resp)
	}

	return nil
}

func (c *Client) Close(ctx context.Context, consumerId string) error {
	return nil
}

func newIterErr(err error) iter.Seq2[*bus.Msg, error] {
	return func(yield func(*bus.Msg, error) bool) {
		yield(nil, err)
	}
}

func newErrorFromResp(resp *http.Response) error {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return fmt.Errorf("%s", strings.TrimSpace(string(b)))
}
