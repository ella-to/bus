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
	"unicode/utf16"
	"unicode/utf8"
)

var (
	// these variables are set during build time
	Version   = "v0.6.2"
	GitCommit = ""
	// the following variables are used in the project
	msgType   = "msg"
	errorType = "error"
	doneType  = "done"
)

//
// Event
//

type Event struct {
	Id              string          `json:"id"`
	TraceId         string          `json:"trace_id,omitempty"`
	Key             string          `json:"key"`
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       time.Time       `json:"created_at"`
	Index           int64           `json:"index"`

	// for internal use
	consumerId string
	acker      Acker
	putter     Putter

	// Internal state for serialization: the event is encoded once into
	// readBuf on the first Read call and streamed out from there.
	readBuf   []byte
	readOff   int
	readReady bool

	// Internal buffer used to accumulate partial input across multiple Write
	// calls. This allows the Write method to receive small chunks and
	// complete parsing when enough data has been provided.
	writeBuf []byte
}

// appendJSONString appends s to dst as a JSON string value (without the
// surrounding quotes), escaping quotes, backslashes and control characters.
func appendJSONString(dst []byte, s string) []byte {
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '"' && c != '\\' && c >= 0x20 {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			const hex = "0123456789abcdef"
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		}
		start = i + 1
	}
	return append(dst, s[start:]...)
}

// appendJSON serializes the event as a compact JSON object, appending to dst.
// This is the single source of truth for the event wire format.
func (e *Event) appendJSON(dst []byte) []byte {
	dst = append(dst, `{"id":"`...)
	dst = appendJSONString(dst, e.Id)
	dst = append(dst, '"')

	if e.TraceId != "" {
		dst = append(dst, `,"trace_id":"`...)
		dst = appendJSONString(dst, e.TraceId)
		dst = append(dst, '"')
	}

	if e.Key != "" {
		dst = append(dst, `,"key":"`...)
		dst = appendJSONString(dst, e.Key)
		dst = append(dst, '"')
	}

	dst = append(dst, `,"subject":"`...)
	dst = appendJSONString(dst, e.Subject)
	dst = append(dst, '"')

	if e.ResponseSubject != "" {
		dst = append(dst, `,"response_subject":"`...)
		dst = appendJSONString(dst, e.ResponseSubject)
		dst = append(dst, '"')
	}

	dst = append(dst, `,"created_at":"`...)
	dst = e.CreatedAt.AppendFormat(dst, time.RFC3339Nano)
	dst = append(dst, '"')

	if len(e.Payload) > 0 {
		dst = append(dst, `,"payload":`...)
		dst = append(dst, e.Payload...)
	}

	if e.Index != 0 {
		dst = append(dst, `,"index":`...)
		dst = strconv.AppendInt(dst, e.Index, 10)
	}

	return append(dst, '}')
}

// Read implements io.Reader. The event is serialized once into an internal
// buffer on the first call and streamed out from there; call
// resetReadWriteState (internal) to serialize the event again after mutation.
func (e *Event) Read(p []byte) (n int, err error) {
	if !e.readReady {
		e.readBuf = e.appendJSON(e.readBuf[:0])
		e.readOff = 0
		e.readReady = true
	}

	if e.readOff >= len(e.readBuf) {
		return 0, io.EOF
	}

	n = copy(p, e.readBuf[e.readOff:])
	e.readOff += n
	return n, nil
}

func (e *Event) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty input")
	}

	// Append incoming bytes to the internal buffer
	e.writeBuf = append(e.writeBuf, b...) // accept the bytes and try to parse a full event

	// Try to parse a single event from the buffered data. The parser will
	// return io.ErrUnexpectedEOF when the buffer ends in the middle of a
	// token (so we'll wait for more data). Any other error is considered a
	// hard parse error and will be returned to the caller.
	data := e.writeBuf
	consumed, err := tryParseEvent(data, e)
	if err == io.ErrUnexpectedEOF {
		// Need more data; accept the bytes and wait for more
		return len(b), nil
	}
	if err != nil {
		// parsing error; don't keep the bad prefix
		return 0, err
	}

	// successful parse, remove the consumed bytes from the buffer
	if consumed >= len(e.writeBuf) {
		e.writeBuf = e.writeBuf[:0]
	} else {
		e.writeBuf = append([]byte(nil), e.writeBuf[consumed:]...)
	}
	return len(b), nil
}

// Helper function to parse a JSON number
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

// parseHex4 parses exactly 4 hex digits.
func parseHex4(b []byte) (uint16, bool) {
	var v uint16
	for _, c := range b {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= uint16(c - '0')
		case c >= 'a' && c <= 'f':
			v |= uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return v, true
}

// Helper function to parse a JSON string
func parseString(b []byte) (string, int, error) {
	if len(b) == 0 || b[0] != '"' {
		return "", 0, errors.New("expected string")
	}

	// fast path: no escape sequences before the closing quote
	hasEscape := false
	for i := 1; i < len(b); i++ {
		c := b[i]
		if c == '"' {
			return string(b[1:i]), i + 1, nil
		}
		if c == '\\' {
			hasEscape = true
			break
		}
	}
	if !hasEscape {
		return "", 0, io.ErrUnexpectedEOF
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
			case 'u':
				if pos+4 >= len(b) {
					return "", 0, io.ErrUnexpectedEOF
				}
				r1, ok := parseHex4(b[pos+1 : pos+5])
				if !ok {
					return "", 0, errors.New("invalid unicode escape")
				}
				pos += 4
				r := rune(r1)
				if utf16.IsSurrogate(r) {
					// a high surrogate must be followed by a \uXXXX low surrogate
					if pos+2 >= len(b) {
						return "", 0, io.ErrUnexpectedEOF
					}
					if b[pos+1] == '\\' && b[pos+2] == 'u' {
						if pos+6 >= len(b) {
							return "", 0, io.ErrUnexpectedEOF
						}
						if r2, ok2 := parseHex4(b[pos+3 : pos+7]); ok2 {
							if combined := utf16.DecodeRune(r, rune(r2)); combined != utf8.RuneError {
								result.WriteRune(combined)
								pos += 6
								break
							}
						}
					}
					result.WriteRune(utf8.RuneError)
				} else {
					result.WriteRune(r)
				}
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

// tryParseEvent attempts to parse a single JSON event from the provided
// buffer. If the buffer contains a complete event, it fills the provided
// target Event fields and returns the number of bytes consumed.
// If the buffer ends while parsing, it returns io.ErrUnexpectedEOF. Any
// other parse error is returned directly.
func tryParseEvent(b []byte, out *Event) (int, error) {
	pos := 0
	// Skip leading whitespace
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

	// temp event to only commit on successful parse
	var te Event

	for pos < len(b) {
		// Skip whitespace
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}

		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}

		if b[pos] == '}' {
			pos++
			// commit parsed fields
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

		// Read field name
		fieldStart := pos
		for pos < len(b) && b[pos] != '"' {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}
		fieldName := string(b[fieldStart:pos])
		pos++ // skip closing quote

		// Skip whitespace and colon
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

		// Skip whitespace before value
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}

		switch fieldName {
		case "id":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Id = val
			pos += newPos

		case "trace_id":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.TraceId = val
			pos += newPos

		case "key":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Key = val
			pos += newPos

		case "subject":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.Subject = val
			pos += newPos

		case "response_subject":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			te.ResponseSubject = val
			pos += newPos

		case "created_at":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			// Parse ISO 8601 timestamp
			t, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return 0, errors.New("invalid timestamp format")
			}
			te.CreatedAt = t
			pos += newPos

		case "payload":
			if b[pos] == 'n' {
				// possible null
				if pos+3 >= len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				if string(b[pos:pos+4]) == "null" {
					te.Payload = nil
					pos += 4
					break
				}
			}

			// Find end of JSON value
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
			val, newPos, err := parseNumber(b[pos:])
			if err != nil {
				return 0, err
			}
			index, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return 0, err
			}
			te.Index = index
			pos += newPos
		default:
			// Unknown fields: attempt to skip a JSON value (string, number, object, array, literal)
			// For simplicity, reuse the payload logic to skip the value
			// but do not store it.
			if b[pos] == '"' {
				_, newPos, err := parseString(b[pos:])
				if err != nil {
					return 0, err
				}
				pos += newPos
			} else if (b[pos] >= '0' && b[pos] <= '9') || b[pos] == '-' {
				_, newPos, err := parseNumber(b[pos:])
				if err != nil {
					return 0, err
				}
				pos += newPos
			} else if b[pos] == '{' || b[pos] == '[' {
				// use same scanning as payload
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
				// null
				if pos+3 >= len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				if string(b[pos:pos+4]) == "null" {
					pos += 4
				} else {
					return 0, errors.New("invalid token")
				}
			} else if b[pos] == 't' || b[pos] == 'f' {
				// true/false
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

		// Skip whitespace
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}

		if pos >= len(b) {
			return 0, io.ErrUnexpectedEOF
		}

		// Check for comma or closing brace
		if b[pos] == ',' {
			pos++
			continue
		} else if b[pos] == '}' {
			pos++
			// commit parsed fields
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
	// subject is required
	if e.Subject == "" {
		return errors.New("subject is required")
	}

	// subject must be in a form of "a.b.c"
	if strings.Contains(e.Subject, "*") || strings.Contains(e.Subject, ">") {
		return errors.New("subject should not have * or >")
	}

	// simple validation for response subject
	if e.ResponseSubject != "" {
		if strings.Contains(e.ResponseSubject, "*") || strings.Contains(e.ResponseSubject, ">") {
			return errors.New("response subject should not have * or >")
		}
	}

	return nil
}

func (e *Event) Ack(ctx context.Context, opts ...AckOpt) error {
	if err := e.acker.Ack(ctx, e.consumerId, e.Id); err != nil {
		return fmt.Errorf("failed to ack event: %w", err)
	}

	if e.ResponseSubject == "" {
		return nil
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

const (
	AckManual = "manual" // client should ack the event
	AckNone   = "none"   // no need to ack and server push the event to the client as fast as possible
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

//
// Putter
//

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
		sb.WriteString(strconv.FormatInt(s.Index, 10))
	}
	sb.WriteString(", created_at: ")
	sb.WriteString(s.CreatedAt.Format(time.RFC3339Nano))

	return sb.String()
}

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

	// batch support
	batch    []Event
	hasBatch bool
}

type PutOpt interface {
	configurePut(*putOpt) error
}

type PutOptFunc func(*putOpt) error

func (f PutOptFunc) configurePut(p *putOpt) error {
	return f(p)
}

type Putter interface {
	Put(ctx context.Context, opts ...PutOpt) *Response
}

//
// Getter
//

type getOpt struct {
	subject         string
	ackStrategy     string
	redelivery      time.Duration
	redeliveryCount int
	redeliverySet   bool
	start           string
	metaFn          func(map[string]string)
}

// GetOpt is an interface that can be used to configure the Get operation
type GetOpt interface {
	configureGet(*getOpt) error
}

type GetOptFunc func(*getOpt) error

func (f GetOptFunc) configureGet(g *getOpt) error {
	return f(g)
}

// Getter is an interface that can be used to get events from the bus
type Getter interface {
	Get(ctx context.Context, opts ...GetOpt) iter.Seq2[*Event, error]
}

//
// Acker
//

type ackOpt struct {
	payload json.RawMessage
}

// AckOpt is an interface that can be used to configure the Ack operation
type AckOpt interface {
	configureAck(*ackOpt) error
}

// Acker is an interface that can be used to acknowledge the event
type Acker interface {
	Ack(ctx context.Context, consumerId string, eventId string) error
}

//
// Options
// options are utility functions which can be used to configure the Putter and Getter

// Subject

type subjectOpt string

var (
	_ PutOpt = (*subjectOpt)(nil)
	_ GetOpt = (*subjectOpt)(nil)
)

func (s subjectOpt) configurePut(p *putOpt) error {
	if p.event.Subject != "" {
		return errors.New("subject already set")
	}

	p.event.Subject = string(s)

	// should not have * or >
	if strings.Contains(string(s), "*") || strings.Contains(string(s), ">") {
		return errors.New("subject should not have * or >")
	}

	return nil
}

func (s subjectOpt) configureGet(g *getOpt) error {
	if g.subject != "" {
		return errors.New("subject already set")
	}

	// should not starts with * or >
	if strings.HasPrefix(string(s), "*") || strings.HasPrefix(string(s), ">") {
		return errors.New("subject should not starts with * or >")
	}

	// should not have anything after >
	if strings.Contains(string(s), ">") && !strings.HasSuffix(string(s), ">") {
		return errors.New("subject should not have anything after >")
	}

	g.subject = string(s)
	return nil
}

// WithSubject sets the subject of the event and consumer
func WithSubject(subject string) subjectOpt {
	return subjectOpt(subject)
}

func WithStartFrom(start string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if start != StartOldest && start != StartNewest && !strings.HasPrefix(start, "e_") {
			return errors.New("invalid start from")
		}

		g.start = start
		return nil
	})
}

// WithDelivery sets the redelivery duration and count for the consumer
// if the ack strategy is manual and the event is not acked within the duration
// the event will be redelivered to the consumer up to the redelivery count
// if the redelivery count is <= 0, the event will be redelivered indefinitely
func WithDelivery(duration time.Duration, redeliveryCount int) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if duration < 0 {
			return errors.New("delivery duration should be greater than 0")
		}

		g.redelivery = duration
		g.redeliveryCount = redeliveryCount
		g.redeliverySet = true
		return nil
	})
}

func WithAckStrategy(strategy string) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if strategy != AckManual && strategy != AckNone {
			return errors.New("invalid ack strategy")
		}

		g.ackStrategy = strategy
		return nil
	})
}

func WithExtractMeta(fn func(map[string]string)) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if g.metaFn != nil {
			return errors.New("meta function already set")
		}

		g.metaFn = fn
		return nil
	})
}

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

func WithRequestReply() PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.confirmCount != 0 {
			return errors.New("confirm count already set")
		}

		p.event.ResponseSubject = newInboxSubject()
		return nil
	})
}

func WithKey(key string) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		if p.event.Key != "" {
			return errors.New("key already set")
		}

		p.event.Key = key
		return nil
	})
}

// Batch allows publishing multiple events in a single Put call. Each Batch call
// defines one event using only WithSubject, WithKey and WithData. When using
// batch mode, the Put call must contain only Batch items (mixing with other
// top-level options is disallowed).
func Batch(opts ...PutOpt) PutOpt {
	return PutOptFunc(func(p *putOpt) error {
		// mark that we are in batch mode
		p.hasBatch = true

		// Apply provided opts to a temporary putOpt to validate what they try to set
		temp := &putOpt{}
		for _, o := range opts {
			if err := o.configurePut(temp); err != nil {
				return err
			}
		}

		// Only allow subject, key and payload to be set inside a batch item
		if temp.event.ResponseSubject != "" || temp.confirmCount != 0 {
			return errors.New("batch only supports WithSubject, WithKey, WithTraceId, WithId and WithData options")
		}

		if temp.event.Subject == "" {
			return errors.New("batch item must have a subject")
		}

		if temp.event.Payload == nil {
			return errors.New("batch item must have data")
		}

		// append the validated event
		p.batch = append(p.batch, temp.event)
		return nil
	})
}

// Payload

// dataOpt represents an option that sets payload data for events and acknowledgments.
type dataOpt struct {
	value any
}

// WithData creates a data option with the provided value.
// Passing nil will result in an error when the option is applied.
func WithData(data any) *dataOpt {
	return &dataOpt{value: data}
}

// configurePut sets the event payload based on the data value's type.
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

// configureAck sets the acknowledgment payload.
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

// marshalPayload converts the data value to json.RawMessage based on its type.
func (d *dataOpt) marshalPayload() (json.RawMessage, error) {
	switch v := d.value.(type) {
	case json.RawMessage:
		return v, nil

	case string:
		return d.marshalString(v)

	case []byte:
		return d.marshalBytes(v)

	case error:
		return d.marshalError(v)

	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return d.marshalPrimitive(v)

	case fmt.Stringer:
		return json.Marshal(v)

	default:
		return json.Marshal(d.value)
	}
}

// marshalString handles string values.
// If the string is valid JSON, it's used as-is, otherwise it's encoded as a JSON string.
func (d *dataOpt) marshalString(s string) (json.RawMessage, error) {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s), nil
	}
	return json.Marshal(s)
}

// marshalBytes handles byte slice values.
// If the bytes represent valid JSON, they're used as-is, otherwise converted to a JSON string.
func (d *dataOpt) marshalBytes(b []byte) (json.RawMessage, error) {
	if json.Valid(b) {
		return json.RawMessage(b), nil
	}
	return json.Marshal(string(b))
}

// marshalError converts an error to a JSON object with an "error" field.
func (d *dataOpt) marshalError(err error) (json.RawMessage, error) {
	return json.Marshal(map[string]string{"error": err.Error()})
}

// marshalPrimitive handles numeric and boolean primitive types.
func (d *dataOpt) marshalPrimitive(v any) (json.RawMessage, error) {
	return fmt.Appendf(nil, "%v", v), nil
}

//
// Trace Id
//

type traceIdOpt struct {
	value string
}

var _ PutOpt = (*traceIdOpt)(nil)

func (o *traceIdOpt) configurePut(opt *putOpt) error {
	if opt.event.TraceId != "" {
		return fmt.Errorf("trace id option already set to %s", opt.event.TraceId)
	}

	opt.event.TraceId = o.value
	return nil
}

func WithTraceId(traceId string) *traceIdOpt {
	return &traceIdOpt{traceId}
}

//
// Id
//

type idOpt struct {
	value string
}

var _ PutOpt = (*idOpt)(nil)

func (o *idOpt) configurePut(opt *putOpt) error {
	if opt.event.Id != "" {
		return fmt.Errorf("id option already set to %s", opt.event.Id)
	}

	opt.event.Id = o.value
	return nil
}

// WithId sets the identifier of the event
// Note: setting the id manually may lead to conflicts if the same id is used multiple times
// so it should be used with caution, if you are not sure about it, do not use it.
func WithId(id string) *idOpt {
	return &idOpt{id}
}

//
// CreatedAt
//

type createdAtOpt struct {
	value time.Time
}

var _ PutOpt = (*createdAtOpt)(nil)

func (o *createdAtOpt) configurePut(opt *putOpt) error {
	if !opt.event.CreatedAt.IsZero() {
		return fmt.Errorf("created at option already set to %s", opt.event.CreatedAt)
	}

	opt.event.CreatedAt = o.value
	return nil
}

// WithCreatedAt sets the creation time of the event
// Note: setting the created at manually may lead to confusion if the time is in the past or future
// so it should be used with caution, if you are not sure about it, do not use it.
func WithCreatedAt(createdAt time.Time) *createdAtOpt {
	return &createdAtOpt{createdAt}
}

var logger = slog.Default()

func SetLogger(l *slog.Logger) {
	logger = l
}
