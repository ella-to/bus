package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"
)

var (
	// these variables are set during build time
	Version   = "v0.3.18"
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
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	CreatedAt       time.Time       `json:"created_at"`
	Index           int64           `json:"index"`

	// for internal use
	consumerId string
	acker      Acker
	putter     Putter

	// Internal state for serialization
	writeState int // Tracks which field we're writing
	tc         trackCopy
}

// NOTE: I had to implement Read method to enhance the performance of the code
// with the current implementation I gained around 50x performance improvement
func (e *Event) Read(p []byte) (n int, err error) {
	for len(p) > 0 {
		switch e.writeState {
		case 0:
			{
				n1 := e.tc.Copy(p, []byte(`{`), 0)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}
				e.writeState = 1
			}

		case 1: // Write "id" field
			{
				field := []byte(`"id":"`)
				n1 := e.tc.Copy(p, field, 1)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 2
			}

		case 2: // Write Id value
			{
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
			}

		case 3: // close "id" field
			{
				n1 := e.tc.Copy(p, []byte(`"`), 3)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 4
			}

		case 4: // Write "trace_id" field
			{
				if len(e.TraceId) == 0 {
					e.writeState = 7 // skip trace_id field
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
			}

		case 5: // Write TraceId value
			{
				n1 := e.tc.Copy(p, []byte(e.TraceId), 5)
				n += n1
				p = p[n1:]
				if n1 < len(e.TraceId) {
					return n, nil
				}

				e.writeState = 6
			}

		case 6: // close "trace_id" field
			{
				n1 := e.tc.Copy(p, []byte(`"`), 6)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 7
			}

		case 7: // Write "subject" field
			{
				field := []byte(`,"subject":"`)
				n1 := e.tc.Copy(p, field, 7)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 8
			}

		case 8: // Write Subject value
			{
				n1 := e.tc.Copy(p, []byte(e.Subject), 8)
				n += n1
				p = p[n1:]
				if n1 < len(e.Subject) {
					return n, nil
				}

				e.writeState = 9
			}

		case 9: // close "subject" field
			{
				n1 := e.tc.Copy(p, []byte(`"`), 9)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 10
			}

		case 10: // Write "response_subject" field
			{
				if len(e.ResponseSubject) == 0 {
					e.writeState = 13 // skip response_subject field
					continue
				}

				field := []byte(`,"response_subject":"`)
				n1 := e.tc.Copy(p, field, 10)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 11
			}

		case 11: // Write ResponseSubject value
			{
				n1 := e.tc.Copy(p, []byte(e.ResponseSubject), 11)
				n += n1
				p = p[n1:]
				if n1 < len(e.ResponseSubject) {
					return n, nil
				}

				e.writeState = 12
			}

		case 12: // close "response_subject" field
			{
				n1 := e.tc.Copy(p, []byte(`"`), 12)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 13
			}

		case 13: // Write "created_at" field
			{
				field := []byte(`,"created_at":"`)
				n1 := e.tc.Copy(p, field, 13)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 14
			}

		case 14: // Write CreatedAt value
			{
				createdAt := e.CreatedAt.Format(time.RFC3339)
				n1 := e.tc.Copy(p, []byte(createdAt), 14)
				n += n1
				p = p[n1:]
				if n1 < len(createdAt) {
					return n, nil
				}

				e.writeState = 15
			}

		case 15: // close "created_at" field
			{
				n1 := e.tc.Copy(p, []byte(`"`), 15)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 16
			}

		case 16: // Write "payload" field
			{
				if len(e.Payload) == 0 {
					e.writeState = 18 // skip payload field
					continue
				}

				field := []byte(`,"payload":`)
				n1 := e.tc.Copy(p, field, 16)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 17
			}

		case 17: // Write Payload value
			{
				n1 := e.tc.Copy(p, e.Payload, 17)
				n += n1
				p = p[n1:]
				if n1 < len(e.Payload) {
					return n, nil
				}

				e.writeState = 18
			}

		case 18: // Write Index
			{
				if e.Index == 0 {
					e.writeState = 20 // skip index field
					continue
				}

				field := []byte(`,"index":`)
				n1 := e.tc.Copy(p, field, 18)
				n += n1
				p = p[n1:]
				if n1 < len(field) {
					return n, nil
				}

				e.writeState = 19
			}

		case 19: // Write Index value
			{
				index := strconv.FormatInt(e.Index, 10)
				n1 := e.tc.Copy(p, []byte(index), 19)
				n += n1
				p = p[n1:]
				if n1 < len(index) {
					return n, nil
				}

				e.writeState = 20
			}

		case 20: // close
			{
				n1 := e.tc.Copy(p, []byte(`}`), 18)
				n += n1
				p = p[n1:]
				if n1 < 1 {
					return n, nil
				}

				e.writeState = 21
			}

		default:
			return n, io.EOF
		}
	}

	return n, nil
}

func (e *Event) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty input")
	}

	// Skip leading whitespace
	pos := 0
	for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
		pos++
	}

	// Expect opening brace
	if pos >= len(b) || b[pos] != '{' {
		return 0, errors.New("expected opening brace")
	}
	pos++

	for pos < len(b) {
		// Skip whitespace
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}

		if pos >= len(b) {
			return 0, errors.New("unexpected end of input")
		}

		// Check for closing brace
		if b[pos] == '}' {
			pos++
			break
		}

		// Expect quote for field name
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
			return 0, errors.New("unterminated field name")
		}
		fieldName := string(b[fieldStart:pos])
		pos++ // Skip closing quote

		// Skip whitespace and colon
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
		if pos >= len(b) || b[pos] != ':' {
			return 0, errors.New("expected colon after field name")
		}
		pos++

		// Skip whitespace before value
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}

		// Parse value based on field name
		switch fieldName {
		case "id":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			e.Id = val
			pos += newPos

		case "trace_id":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			e.TraceId = val
			pos += newPos

		case "subject":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			e.Subject = val
			pos += newPos

		case "response_subject":
			val, newPos, err := parseString(b[pos:])
			if err != nil {
				return 0, err
			}
			e.ResponseSubject = val
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
			e.CreatedAt = t
			pos += newPos

		case "payload":
			if b[pos] == 'n' && pos+3 < len(b) && string(b[pos:pos+4]) == "null" {
				e.Payload = nil
				pos += 4
			} else {
				// Find the end of the JSON value (could be object, array, string, number, etc.)
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
				e.Payload = json.RawMessage(b[dataStart:pos])
			}
		case "index":
			val, newPos, err := parseNumber(b[pos:])
			if err != nil {
				return 0, err
			}

			index, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return 0, err
			}

			e.Index = index
			pos += newPos
		}

		// Skip whitespace
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}

		// Check for comma or closing brace
		if pos >= len(b) {
			return 0, errors.New("unexpected end of input")
		}
		if b[pos] == ',' {
			pos++
		} else if b[pos] != '}' {
			return 0, errors.New("expected comma or closing brace")
		}
	}

	return pos, nil
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
		for pos < len(b) && b[pos] >= '0' && b[pos] <= '9' {
			pos++
		}
	}

	if pos < len(b) && (b[pos] == 'e' || b[pos] == 'E') {
		pos++
		if pos < len(b) && (b[pos] == '+' || b[pos] == '-') {
			pos++
		}
		if pos >= len(b) || (b[pos] < '0' || b[pos] > '9') {
			return "", 0, errors.New("expected number")
		}

		for pos < len(b) && b[pos] >= '0' && b[pos] <= '9' {
			pos++
		}

	}

	return string(b[:pos]), pos, nil
}

// Helper function to parse a JSON string
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
				return "", 0, errors.New("incomplete escape sequence")
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
	return "", 0, errors.New("unterminated string")
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
	DefaultAck        = AckNone
	DefaultStart      = StartNewest
	DefaultRedelivery = 5 * time.Second
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
		sb.WriteString(fmt.Sprintf("%d", s.Index))
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
	subject     string
	ackStrategy string
	redelivery  time.Duration
	start       string
	metaFn      func(map[string]string)
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

func WithDelivery(duration time.Duration) GetOpt {
	return GetOptFunc(func(g *getOpt) error {
		if duration < 0 {
			return errors.New("delivery duration should be greater than 0")
		}

		g.redelivery = duration
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
