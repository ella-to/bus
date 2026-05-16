// Package bus provides a NATS-backed implementation of the ella.to/bus
// public API. Events are persisted in NATS JetStream while the request/reply
// inbox traffic flows over Core NATS.
//
// The exposed types (Event, Response, Putter, Getter, Acker, Client and all
// the With... helpers) match the upstream ella.to/bus interface so that
// existing call sites can be ported over by just swapping the engine.
//
// Subject pattern matching ("*" / ">") is delegated to NATS itself, both for
// Core NATS subscriptions and for JetStream consumer FilterSubject filters,
// so no client-side matcher is needed.
package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

var (
	// Version of this NATS-backed implementation.
	Version   = "v0.6.2-nats"
	GitCommit = ""
)

// internal SSE event names (kept for parity with upstream parsers).
var (
	msgType   = "msg"
	errorType = "error"
	doneType  = "done"
)

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

// Event represents a single message flowing through the bus. Its layout is
// identical to ella.to/bus.Event so that JSON encoded events remain
// compatible across implementations.
type Event struct {
	Id              string          `json:"id"`
	TraceId         string          `json:"trace_id,omitempty"`
	Key             string          `json:"key,omitempty"`
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	Index           int64           `json:"index,omitempty"`

	// fields used by Event.Ack to talk back to the bus
	consumerId string
	acker      Acker
	putter     Putter

	// internal state used by the streaming Read implementation
	writeState int
	tc         trackCopy

	// scratch buffer used by Write to accumulate partial JSON across calls
	writeBuf []byte
}

// resetReadWriteState resets the internal state used for reading and writing.
// During the redelivery of events, we need to reset the state to ensure
// correct serialization.
func (e *Event) resetReadWriteState() {
	e.writeState = 0
	e.tc.reset()
}

// Read serialises the event to its canonical JSON representation. The method
// is preserved verbatim from upstream so io.Copy(w, event) keeps working in
// downstream code.
func (e *Event) Read(p []byte) (n int, err error) {
	for len(p) > 0 {
		switch e.writeState {
		case 0:
			n1 := e.tc.Copy(p, []byte(`{`), 0)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 1

		case 1:
			field := []byte(`"id":"`)
			n1 := e.tc.Copy(p, field, 1)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 2

		case 2:
			if len(e.Id) == 0 {
				e.writeState = 3
				continue
			}
			n1 := e.tc.Copy(p, []byte(e.Id), 2)
			n += n1
			p = p[n1:]
			if n1 < len(e.Id) {
				return n, nil
			}
			e.writeState = 3

		case 3:
			n1 := e.tc.Copy(p, []byte(`"`), 3)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 4

		case 4:
			if len(e.TraceId) == 0 {
				e.writeState = 7
				continue
			}
			field := []byte(`,"trace_id":"`)
			n1 := e.tc.Copy(p, field, 4)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 5

		case 5:
			n1 := e.tc.Copy(p, []byte(e.TraceId), 5)
			n += n1
			p = p[n1:]
			if n1 < len(e.TraceId) {
				return n, nil
			}
			e.writeState = 6

		case 6:
			n1 := e.tc.Copy(p, []byte(`"`), 6)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 7

		case 7:
			if len(e.Key) == 0 {
				e.writeState = 10
				continue
			}
			field := []byte(`,"key":"`)
			n1 := e.tc.Copy(p, field, 7)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 8

		case 8:
			n1 := e.tc.Copy(p, []byte(e.Key), 8)
			n += n1
			p = p[n1:]
			if n1 < len(e.Key) {
				return n, nil
			}
			e.writeState = 9

		case 9:
			n1 := e.tc.Copy(p, []byte(`"`), 9)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 10

		case 10:
			field := []byte(`,"subject":"`)
			n1 := e.tc.Copy(p, field, 10)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 11

		case 11:
			n1 := e.tc.Copy(p, []byte(e.Subject), 11)
			n += n1
			p = p[n1:]
			if n1 < len(e.Subject) {
				return n, nil
			}
			e.writeState = 12

		case 12:
			n1 := e.tc.Copy(p, []byte(`"`), 12)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 13

		case 13:
			if len(e.ResponseSubject) == 0 {
				e.writeState = 16
				continue
			}
			field := []byte(`,"response_subject":"`)
			n1 := e.tc.Copy(p, field, 13)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 14

		case 14:
			n1 := e.tc.Copy(p, []byte(e.ResponseSubject), 14)
			n += n1
			p = p[n1:]
			if n1 < len(e.ResponseSubject) {
				return n, nil
			}
			e.writeState = 15

		case 15:
			n1 := e.tc.Copy(p, []byte(`"`), 15)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 16

		case 16:
			field := []byte(`,"created_at":"`)
			n1 := e.tc.Copy(p, field, 16)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 17

		case 17:
			createdAt := e.CreatedAt.Format(time.RFC3339)
			n1 := e.tc.Copy(p, []byte(createdAt), 17)
			n += n1
			p = p[n1:]
			if n1 < len(createdAt) {
				return n, nil
			}
			e.writeState = 18

		case 18:
			n1 := e.tc.Copy(p, []byte(`"`), 18)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 19

		case 19:
			if len(e.Payload) == 0 {
				e.writeState = 21
				continue
			}
			field := []byte(`,"payload":`)
			n1 := e.tc.Copy(p, field, 19)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 20

		case 20:
			n1 := e.tc.Copy(p, e.Payload, 20)
			n += n1
			p = p[n1:]
			if n1 < len(e.Payload) {
				return n, nil
			}
			e.writeState = 21

		case 21:
			if e.Index == 0 {
				e.writeState = 23
				continue
			}
			field := []byte(`,"index":`)
			n1 := e.tc.Copy(p, field, 21)
			n += n1
			p = p[n1:]
			if n1 < len(field) {
				return n, nil
			}
			e.writeState = 22

		case 22:
			index := strconv.FormatInt(e.Index, 10)
			n1 := e.tc.Copy(p, []byte(index), 22)
			n += n1
			p = p[n1:]
			if n1 < len(index) {
				return n, nil
			}
			e.writeState = 23

		case 23:
			n1 := e.tc.Copy(p, []byte(`}`), 23)
			n += n1
			p = p[n1:]
			if n1 < 1 {
				return n, nil
			}
			e.writeState = 24

		default:
			return n, io.EOF
		}
	}
	return n, nil
}

// Write parses an Event JSON encoding incrementally so io.Copy(event, body)
// keeps working. Behaviour is preserved verbatim from upstream.
func (e *Event) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty input")
	}
	e.writeBuf = append(e.writeBuf, b...)
	consumed, err := tryParseEvent(e.writeBuf, e)
	if err == io.ErrUnexpectedEOF {
		return len(b), nil
	}
	if err != nil {
		return 0, err
	}
	if consumed >= len(e.writeBuf) {
		e.writeBuf = e.writeBuf[:0]
	} else {
		e.writeBuf = append([]byte(nil), e.writeBuf[consumed:]...)
	}
	return len(b), nil
}

func parseNumber(b []byte) (string, int, error) {
	if len(b) == 0 {
		return "", 0, errors.New("expected number")
	}
	pos := 0
	if b[pos] == '-' {
		pos++
	}
	if pos >= len(b) || (b[pos] < '0' || b[pos] > '9') {
		return "", 0, errors.New("expected number")
	}
	for pos < len(b) && b[pos] >= '0' && b[pos] <= '9' {
		pos++
	}
	if pos < len(b) && b[pos] == '.' {
		pos++
		if pos >= len(b) {
			return "", 0, io.ErrUnexpectedEOF
		}
		for pos < len(b) && b[pos] >= '0' && b[pos] <= '9' {
			pos++
		}
	}
	if pos < len(b) && (b[pos] == 'e' || b[pos] == 'E') {
		pos++
		if pos < len(b) && (b[pos] == '+' || b[pos] == '-') {
			pos++
		}
		if pos >= len(b) {
			return "", 0, io.ErrUnexpectedEOF
		}
		if b[pos] < '0' || b[pos] > '9' {
			return "", 0, errors.New("expected number")
		}
		for pos < len(b) && b[pos] >= '0' && b[pos] <= '9' {
			pos++
		}
	}
	return string(b[:pos]), pos, nil
}

func parseString(b []byte) (string, int, error) {
	if len(b) == 0 || b[0] != '"' {
		return "", 0, errors.New("expected string")
	}
	pos := 1
	var result bytes.Buffer
	for pos < len(b) {
		switch b[pos] {
		case '\\':
			if pos+1 >= len(b) {
				return "", 0, io.ErrUnexpectedEOF
			}
			pos++
			switch b[pos] {
			case '"', '\\', '/':
				result.WriteByte(b[pos])
			case 'b':
				result.WriteByte('\b')
			case 'f':
				result.WriteByte('\f')
			case 'n':
				result.WriteByte('\n')
			case 'r':
				result.WriteByte('\r')
			case 't':
				result.WriteByte('\t')
			default:
				return "", 0, errors.New("invalid escape sequence")
			}
		case '"':
			return result.String(), pos + 1, nil
		default:
			result.WriteByte(b[pos])
		}
		pos++
	}
	return "", 0, io.ErrUnexpectedEOF
}

func tryParseEvent(b []byte, out *Event) (int, error) {
	pos := 0
	for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
		pos++
	}
	if pos >= len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	if b[pos] != '{' {
		return 0, errors.New("expected opening brace")
	}
	pos++

	var te Event
	for pos < len(b) {
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}
		if b[pos] == '}' {
			pos++
			out.Id = te.Id
			out.TraceId = te.TraceId
			out.Key = te.Key
			out.Subject = te.Subject
			out.ResponseSubject = te.ResponseSubject
			out.Payload = te.Payload
			out.CreatedAt = te.CreatedAt
			out.Index = te.Index
			return pos, nil
		}
		if b[pos] != '"' {
			return 0, errors.New("expected quote before field name")
		}
		pos++
		fieldStart := pos
		for pos < len(b) && b[pos] != '"' {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}
		fieldName := string(b[fieldStart:pos])
		pos++
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}
		if b[pos] != ':' {
			return 0, errors.New("expected colon after field name")
		}
		pos++
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}

		switch fieldName {
		case "id":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Id = val
			pos += np
		case "trace_id":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.TraceId = val
			pos += np
		case "key":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Key = val
			pos += np
		case "subject":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Subject = val
			pos += np
		case "response_subject":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.ResponseSubject = val
			pos += np
		case "created_at":
			val, np, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			t, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return 0, errors.New("invalid timestamp format")
			}
			te.CreatedAt = t
			pos += np
		case "payload":
			if b[pos] == 'n' {
				if pos+3 >= len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				if string(b[pos:pos+4]) == "null" {
					te.Payload = nil
					pos += 4
					break
				}
			}
			depth := 0
			dataStart := pos
			inString := false
			for pos < len(b) {
				if !inString {
					if b[pos] == '{' || b[pos] == '[' {
						depth++
					} else if b[pos] == '}' || b[pos] == ']' {
						depth--
						if depth < 0 {
							break
						}
					} else if b[pos] == '"' {
						inString = true
					} else if b[pos] == ',' && depth == 0 {
						break
					}
				} else {
					switch b[pos] {
					case '\\':
						pos++
					case '"':
						inString = false
					}
				}
				pos++
			}
			if pos > len(b) {
				return 0, io.ErrUnexpectedEOF
			}
			if pos == len(b) && (depth != 0 || inString) {
				return 0, io.ErrUnexpectedEOF
			}
			te.Payload = json.RawMessage(b[dataStart:pos])
		case "index":
			val, np, err := parseNumber(b[pos:])
			if err != nil {
				return 0, err
			}
			idx, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return 0, err
			}
			te.Index = idx
			pos += np
		default:
			if b[pos] == '"' {
				_, np, err := parseString(b[pos:])
				if err != nil {
					return 0, err
				}
				pos += np
			} else if (b[pos] >= '0' && b[pos] <= '9') || b[pos] == '-' {
				_, np, err := parseNumber(b[pos:])
				if err != nil {
					return 0, err
				}
				pos += np
			} else if b[pos] == '{' || b[pos] == '[' {
				depth := 0
				inString := false
				for pos < len(b) {
					if !inString {
						if b[pos] == '{' || b[pos] == '[' {
							depth++
						} else if b[pos] == '}' || b[pos] == ']' {
							depth--
							if depth < 0 {
								break
							}
						} else if b[pos] == '"' {
							inString = true
						} else if b[pos] == ',' && depth == 0 {
							break
						}
					} else {
						switch b[pos] {
						case '\\':
							pos++
						case '"':
							inString = false
						}
					}
					pos++
				}
				if pos == len(b) && inString {
					return 0, io.ErrUnexpectedEOF
				}
			} else if b[pos] == 'n' {
				if pos+3 >= len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				if string(b[pos:pos+4]) == "null" {
					pos += 4
				} else {
					return 0, errors.New("invalid token")
				}
			} else if b[pos] == 't' || b[pos] == 'f' {
				end := pos
				for end < len(b) && ((b[end] >= 'a' && b[end] <= 'z') || (b[end] >= 'A' && b[end] <= 'Z')) {
					end++
				}
				if end == len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				pos = end
			} else {
				return 0, errors.New("unexpected token")
			}
		}

		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}
		if b[pos] == ',' {
			pos++
			continue
		} else if b[pos] == '}' {
			pos++
			out.Id = te.Id
			out.TraceId = te.TraceId
			out.Key = te.Key
			out.Subject = te.Subject
			out.ResponseSubject = te.ResponseSubject
			out.Payload = te.Payload
			out.CreatedAt = te.CreatedAt
			out.Index = te.Index
			return pos, nil
		} else {
			return 0, errors.New("expected comma or closing brace")
		}
	}
	return 0, io.ErrUnexpectedEOF
}

func (e *Event) validate() error {
	if e.Subject == "" {
		return errors.New("subject is required")
	}
	if strings.Contains(e.Subject, "*") || strings.Contains(e.Subject, ">") {
		return errors.New("subject should not have * or >")
	}
	if e.ResponseSubject != "" {
		if strings.Contains(e.ResponseSubject, "*") || strings.Contains(e.ResponseSubject, ">") {
			return errors.New("response subject should not have * or >")
		}
	}
	return nil
}

// Ack acknowledges this event with the bus and, if a response subject is
// present, publishes the supplied options as the reply payload.
func (e *Event) Ack(ctx context.Context, opts ...AckOpt) error {
	if e.acker != nil {
		if err := e.acker.Ack(ctx, e.consumerId, e.Id); err != nil {
			return fmt.Errorf("failed to ack event: %w", err)
		}
	}

	if e.ResponseSubject == "" {
		return nil
	}

	if e.putter == nil {
		return errors.New("event has no putter to send response")
	}

	putOpts := []PutOpt{
		WithSubject(e.ResponseSubject),
	}
	for _, opt := range opts {
		if o, ok := opt.(PutOpt); ok {
			putOpts = append(putOpts, o)
		}
	}

	if err := e.putter.Put(ctx, putOpts...).Error(); err != nil {
		return fmt.Errorf("failed to send response: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	AckManual = "manual"
	AckNone   = "none"
)

const (
	StartOldest = "oldest"
	StartNewest = "newest"
)

const (
	DefaultAck             = AckNone
	DefaultStart           = StartNewest
	DefaultRedelivery      = 5 * time.Second
	DefaultRedeliveryCount = 3
)

// ---------------------------------------------------------------------------
// Putter / Response
// ---------------------------------------------------------------------------

// Response represents the outcome of a Put call.
type Response struct {
	err       error
	Id        string
	Index     int64
	CreatedAt time.Time
	Payload   json.RawMessage
}

func (s *Response) String() string {
	var sb strings.Builder
	sb.WriteString("id: ")
	sb.WriteString(s.Id)
	if s.Index != -1 {
		sb.WriteString(", index: ")
		sb.WriteString(fmt.Sprintf("%d", s.Index))
	}
	sb.WriteString(", created_at: ")
	sb.WriteString(s.CreatedAt.Format(time.RFC3339Nano))
	return sb.String()
}

// Error returns a non-nil error if the put failed or the response payload is
// itself a JSON-encoded error string.
func (r *Response) Error() error {
	if r.err != nil {
		return r.err
	}
	if len(r.Payload) == 0 || r.Payload[0] == '{' || r.Payload[0] == '[' {
		return nil
	}
	return fmt.Errorf("%s", r.Payload)
}

type putOpt struct {
	event        Event
	confirmCount int
	batch        []Event
	hasBatch     bool
}

// PutOpt configures a Put operation.
type PutOpt interface {
	configurePut(*putOpt) error
}

type PutOptFunc func(*putOpt) error

func (f PutOptFunc) configurePut(p *putOpt) error { return f(p) }

// Putter publishes events.
type Putter interface {
	Put(ctx context.Context, opts ...PutOpt) *Response
}

// ---------------------------------------------------------------------------
// Getter
// ---------------------------------------------------------------------------

type getOpt struct {
	subject         string
	ackStrategy     string
	redelivery      time.Duration
	redeliveryCount int
	start           string
	metaFn          func(map[string]string)
}

// GetOpt configures a Get operation.
type GetOpt interface {
	configureGet(*getOpt) error
}

type GetOptFunc func(*getOpt) error

func (f GetOptFunc) configureGet(g *getOpt) error { return f(g) }

// Getter subscribes to events matching a subject pattern.
type Getter interface {
	Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error]
}

// ---------------------------------------------------------------------------
// Acker
// ---------------------------------------------------------------------------

type ackOpt struct {
	payload json.RawMessage
}

// AckOpt configures an Ack operation.
type AckOpt interface {
	configureAck(*ackOpt) error
}

// Acker acknowledges an event by consumer + event id.
type Acker interface {
	Ack(ctx context.Context, consumerId string, eventId string) error
}

// ---------------------------------------------------------------------------
// Subject option (Put + Get)
// ---------------------------------------------------------------------------

type subjectOpt string

var (
	_ PutOpt = (*subjectOpt)(nil)
	_ GetOpt = (*subjectOpt)(nil)
)

func (s subjectOpt) configurePut(p *putOpt) error {
	if p.event.Subject != "" {
		return errors.New("subject already set")
	}
	if strings.Contains(string(s), "*") || strings.Contains(string(s), ">") {
		return errors.New("subject should not have * or >")
	}
	p.event.Subject = string(s)
	return nil
}

func (s subjectOpt) configureGet(g *getOpt) error {
	if g.subject != "" {
		return errors.New("subject already set")
	}
	if strings.HasPrefix(string(s), "*") || strings.HasPrefix(string(s), ">") {
		return errors.New("subject should not starts with * or >")
	}
	if strings.Contains(string(s), ">") && !strings.HasSuffix(string(s), ">") {
		return errors.New("subject should not have anything after >")
	}
	g.subject = string(s)
	return nil
}

// WithSubject sets the subject of the event or consumer.
func WithSubject(subject string) subjectOpt { return subjectOpt(subject) }

// ---------------------------------------------------------------------------
// Get options
// ---------------------------------------------------------------------------

// WithStartFrom sets where the consumer should begin reading from. Accepts
// StartOldest, StartNewest or an event id (e_xxx). The event-id form is
// best-effort and only honoured when the corresponding message is still
// retained in the underlying NATS stream.
func WithStartFrom(start string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if start != StartOldest && start != StartNewest && !strings.HasPrefix(start, "e_") {
			return errors.New("invalid start from")
		}
		g.start = start
		return nil
	})
}

// WithDelivery sets the redelivery duration and count for the consumer. If
// ack strategy is manual and the event is not acked within the duration, the
// event is redelivered up to the redelivery count. A count of 0 is treated as
// the default.
func WithDelivery(duration time.Duration, redeliveryCount int) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if duration < 0 {
			return errors.New("delivery duration should be greater than 0")
		}
		g.redelivery = duration
		g.redeliveryCount = redeliveryCount
		return nil
	})
}

// WithAckStrategy selects whether events must be acked manually or fire-and-forget.
func WithAckStrategy(strategy string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if strategy != AckManual && strategy != AckNone {
			return errors.New("invalid ack strategy")
		}
		g.ackStrategy = strategy
		return nil
	})
}

// WithExtractMeta installs a callback that receives consumer metadata such as
// the generated consumer id at the time the subscription is established.
func WithExtractMeta(fn func(map[string]string)) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if g.metaFn != nil {
			return errors.New("meta function already set")
		}
		g.metaFn = fn
		return nil
	})
}

// ---------------------------------------------------------------------------
// Put options: confirm / request-reply / key / batch / data / trace / id / created
// ---------------------------------------------------------------------------

// WithConfirm configures the publisher to wait for n acks before returning.
func WithConfirm(n int) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if n < 0 {
			return errors.New("confirm count should be greater than 0")
		}
		if n == 0 {
			return nil
		}
		if p.event.ResponseSubject != "" {
			return errors.New("response subject already set")
		}
		p.confirmCount = n
		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
}

// WithRequestReply turns the Put into an RPC-style call: the publisher waits
// for a single response from a consumer.
func WithRequestReply() PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.confirmCount != 0 {
			return errors.New("confirm count already set")
		}
		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
}

// WithKey sets a key on the event. When duplicate prevention is enabled on
// the underlying engine, events with the same (subject, key) are deduped.
func WithKey(key string) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.event.Key != "" {
			return errors.New("key already set")
		}
		p.event.Key = key
		return nil
	})
}

// Batch publishes multiple events in a single Put call. Each Batch item must
// only set subject, key, trace id, id and data and they must all share the
// same namespace (first subject segment).
func Batch(opts ...PutOpt) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		p.hasBatch = true
		temp := &putOpt{}
		for _, o := range opts {
			if err := o.configurePut(temp); err != nil {
				return err
			}
		}
		if temp.event.ResponseSubject != "" || temp.confirmCount != 0 {
			return errors.New("batch only supports WithSubject, WithKey, WithTraceId, WithId and WithData options")
		}
		if temp.event.Subject == "" {
			return errors.New("batch item must have a subject")
		}
		if temp.event.Payload == nil {
			return errors.New("batch item must have data")
		}
		p.batch = append(p.batch, temp.event)
		return nil
	})
}

// dataOpt represents a payload for an event or an ack.
type dataOpt struct {
	value any
}

// WithData attaches a JSON-encodable payload to the event or ack response.
func WithData(data any) *dataOpt { return &dataOpt{value: data} }

func (d *dataOpt) configurePut(p *putOpt) error {
	if p.event.Payload != nil {
		return errors.New("event payload already set")
	}
	if d.value == nil {
		return errors.New("data value cannot be nil")
	}
	payload, err := d.marshalPayload()
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	p.event.Payload = payload
	return nil
}

func (d *dataOpt) configureAck(a *ackOpt) error {
	if a.payload != nil {
		return errors.New("payload already set")
	}
	if d.value == nil {
		return errors.New("data value cannot be nil")
	}
	payload, err := json.Marshal(d.value)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	a.payload = json.RawMessage(payload)
	return nil
}

func (d *dataOpt) marshalPayload() (json.RawMessage, error) {
	switch v := d.value.(type) {
	case json.RawMessage:
		return v, nil
	case string:
		if json.Valid([]byte(v)) {
			return json.RawMessage(v), nil
		}
		return json.Marshal(v)
	case []byte:
		if json.Valid(v) {
			return json.RawMessage(v), nil
		}
		return json.Marshal(string(v))
	case error:
		return json.Marshal(map[string]string{"error": v.Error()})
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return fmt.Appendf(nil, "%v", v), nil
	case fmt.Stringer:
		return json.Marshal(v)
	default:
		return json.Marshal(d.value)
	}
}

type traceIdOpt struct{ value string }

var _ PutOpt = (*traceIdOpt)(nil)

func (o *traceIdOpt) configurePut(opt *putOpt) error {
	if opt.event.TraceId != "" {
		return fmt.Errorf("trace id option already set to %s", opt.event.TraceId)
	}
	opt.event.TraceId = o.value
	return nil
}

// WithTraceId attaches a trace id to the event for distributed tracing.
func WithTraceId(traceId string) *traceIdOpt { return &traceIdOpt{traceId} }

type idOpt struct{ value string }

var _ PutOpt = (*idOpt)(nil)

func (o *idOpt) configurePut(opt *putOpt) error {
	if opt.event.Id != "" {
		return fmt.Errorf("id option already set to %s", opt.event.Id)
	}
	opt.event.Id = o.value
	return nil
}

// WithId sets the identifier of the event. Care should be taken not to reuse
// the same id for different events.
func WithId(id string) *idOpt { return &idOpt{id} }

type createdAtOpt struct{ value time.Time }

var _ PutOpt = (*createdAtOpt)(nil)

func (o *createdAtOpt) configurePut(opt *putOpt) error {
	if !opt.event.CreatedAt.IsZero() {
		return fmt.Errorf("created at option already set to %s", opt.event.CreatedAt)
	}
	opt.event.CreatedAt = o.value
	return nil
}

// WithCreatedAt sets the creation time of the event.
func WithCreatedAt(createdAt time.Time) *createdAtOpt { return &createdAtOpt{createdAt} }

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

var logger = slog.Default()

// SetLogger replaces the package logger.
func SetLogger(l *slog.Logger) { logger = l }
