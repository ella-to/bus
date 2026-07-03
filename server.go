package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"ella.to/immuta"
	"ella.to/sse"

	"ella.to/bus/internal/cache"
)

//
// handler
//

const (
	HeaderEventId        = "X-BUS-EVENT-ID"
	HeaderEventCreatedAt = "X-BUS-EVENT-CREATED-AT"
	HeaderEventIndex     = "X-BUS-EVENT-INDEX"
	HeaderConsumerId     = "X-BUS-CONSUMER-ID"
	headerLastEventId    = "Last-Event-ID"
)

const (
	DefaultSsePingTimeout = 30 * time.Second

	// busNamespace is the reserved namespace used for ephemeral inbox
	// subjects (request/reply and confirm). Events published to it are
	// routed in memory to currently connected consumers and are NOT
	// persisted to the events log.
	busNamespace = "_bus_"

	// inboxBufferSize is the per-consumer buffer of pending inbox events.
	// Inbox consumers are request/reply waiters, so this only needs to
	// absorb short bursts of replies.
	inboxBufferSize = 256
)

type DuplicateChecker interface {
	// CheckDuplicate checks if the given key for the subject was already processed.
	// It returns true if the key was already present, false otherwise.
	CheckDuplicate(key string, subject string) bool
}

type DuplicateCheckerFunc func(key string, subject string) bool

func (f DuplicateCheckerFunc) CheckDuplicate(key string, subject string) bool {
	return f(key, subject)
}

// inboxMsg is a fully serialized event ready to be pushed over SSE.
type inboxMsg struct {
	id   string
	data string
}

// inbox routes events published to _bus_.* subjects directly to connected
// consumers, in memory. Inbox subjects are point-to-point and ephemeral by
// design: the requester subscribes before publishing the request, so there
// is nothing to replay and no reason to grow the events log with them.
type inbox struct {
	mu   sync.RWMutex
	subs map[string]map[chan inboxMsg]struct{}
}

func newInbox() *inbox {
	return &inbox{
		subs: make(map[string]map[chan inboxMsg]struct{}),
	}
}

func (ib *inbox) subscribe(subject string) chan inboxMsg {
	ch := make(chan inboxMsg, inboxBufferSize)
	ib.mu.Lock()
	set, ok := ib.subs[subject]
	if !ok {
		set = make(map[chan inboxMsg]struct{})
		ib.subs[subject] = set
	}
	set[ch] = struct{}{}
	ib.mu.Unlock()
	return ch
}

func (ib *inbox) unsubscribe(subject string, ch chan inboxMsg) {
	ib.mu.Lock()
	if set, ok := ib.subs[subject]; ok {
		delete(set, ch)
		if len(set) == 0 {
			delete(ib.subs, subject)
		}
	}
	ib.mu.Unlock()
}

// publish delivers msg to every consumer of the exact subject. It never
// blocks: a consumer that has fallen inboxBufferSize messages behind loses
// the message (inbox replies are only meaningful to a live waiter).
func (ib *inbox) publish(subject string, msg inboxMsg) {
	ib.mu.RLock()
	defer ib.mu.RUnlock()

	for ch := range ib.subs[subject] {
		select {
		case ch <- msg:
		default:
			logger.Warn("inbox consumer too slow, dropping reply", "subject", subject, "event_id", msg.id)
		}
	}
}

type Handler struct {
	mux        *http.ServeMux
	eventsLog  *immuta.Storage
	dupChecker DuplicateChecker
	inbox      *inbox

	// waitingAckMap holds one channel per in-flight manual-ack delivery,
	// keyed by consumerId-eventId. Guarded by ackMu.
	ackMu         sync.Mutex
	waitingAckMap map[string]chan struct{}

	// appendLocks serializes appends per namespace: immuta's Append/Save
	// pair is single-writer. Different namespaces append in parallel.
	appendLocks sync.Map // namespace -> *sync.Mutex

	// bufPool recycles scratch buffers used to read records and serialize
	// events, so the hot paths do not allocate per event.
	bufPool sync.Pool // *[]byte
}

func (h *Handler) Close() error {
	return h.eventsLog.Close()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) getBuf() *[]byte {
	if b, ok := h.bufPool.Get().(*[]byte); ok {
		return b
	}
	b := make([]byte, 0, 4096)
	return &b
}

func (h *Handler) putBuf(b *[]byte) {
	// don't let one huge payload pin memory forever
	if cap(*b) > 1<<20 {
		return
	}
	h.bufPool.Put(b)
}

func (h *Handler) namespaceLock(namespace string) *sync.Mutex {
	if mu, ok := h.appendLocks.Load(namespace); ok {
		return mu.(*sync.Mutex)
	}
	mu, _ := h.appendLocks.LoadOrStore(namespace, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// appendEvents serializes and appends the given events to the namespace log
// as a single committed transaction, and returns the index of the first
// appended event.
func (h *Handler) appendEvents(ctx context.Context, namespace string, events []Event) (firstIndex int64, err error) {
	buf := h.getBuf()
	defer h.putBuf(buf)

	mu := h.namespaceLock(namespace)
	mu.Lock()
	defer mu.Unlock()
	defer h.eventsLog.Save(namespace, &err)

	firstIndex = -1
	for i := range events {
		*buf = events[i].appendJSON((*buf)[:0])
		index, _, aerr := h.eventsLog.Append(ctx, namespace, bytes.NewReader(*buf))
		if aerr != nil {
			err = aerr
			return -1, err
		}
		if firstIndex == -1 {
			firstIndex = index
		}
	}

	return firstIndex, nil
}

// Put handles incoming events - delegates to putBatch or putSingle based on content
func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	if trimmed[0] == '[' {
		h.putBatch(w, r, trimmed)
	} else {
		h.putSingle(w, r, trimmed)
	}
}

// prepareEvent validates an incoming event and assigns the server-side
// fields (id, created_at). Returns the event's namespace.
func (h *Handler) prepareEvent(e *Event) (string, int, error) {
	if err := e.validate(); err != nil {
		return "", http.StatusBadRequest, err
	}

	if h.dupChecker != nil && h.dupChecker.CheckDuplicate(e.Key, e.Subject) {
		return "", http.StatusConflict, errors.New("key was processed before")
	}

	if e.Id == "" {
		e.Id = newEventId()
	}

	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}

	namespace := extractNamespace(e.Subject)
	if namespace == "" {
		return "", http.StatusBadRequest, errors.New("invalid subject: missing namespace")
	}

	return namespace, 0, nil
}

// putBatch handles batch event submissions (JSON array of events).
// All events must share one namespace and are committed atomically.
func (h *Handler) putBatch(w http.ResponseWriter, r *http.Request, body []byte) {
	ctx := r.Context()

	var events []Event
	if err := json.Unmarshal(body, &events); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(events) == 0 {
		http.Error(w, "empty batch", http.StatusBadRequest)
		return
	}

	var groupNamespace string
	for i := range events {
		e := &events[i]

		if e.ResponseSubject != "" {
			http.Error(w, "batch items must not have response subject", http.StatusBadRequest)
			return
		}

		namespace, code, err := h.prepareEvent(e)
		if err != nil {
			http.Error(w, err.Error(), code)
			return
		}

		if namespace == busNamespace {
			http.Error(w, "batch cannot publish to the reserved _bus_ namespace", http.StatusBadRequest)
			return
		}

		if groupNamespace == "" {
			groupNamespace = namespace
		} else if groupNamespace != namespace {
			http.Error(w, "all events in batch must belong to the same namespace", http.StatusBadRequest)
			return
		}
	}

	if _, err := h.appendEvents(ctx, groupNamespace, events); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// putSingle handles single event submissions
func (h *Handler) putSingle(w http.ResponseWriter, r *http.Request, body []byte) {
	ctx := r.Context()

	var event Event
	if _, err := tryParseEvent(body, &event); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = errors.New("incomplete event")
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	namespace, code, err := h.prepareEvent(&event)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}

	eventIndex := int64(-1)

	if namespace == busNamespace {
		// inbox events are routed in memory to live consumers only
		buf := h.getBuf()
		*buf = event.appendJSON((*buf)[:0])
		h.inbox.publish(event.Subject, inboxMsg{id: event.Id, data: string(*buf)})
		h.putBuf(buf)
	} else {
		eventIndex, err = h.appendEvents(ctx, namespace, []Event{event})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set(HeaderEventId, event.Id)
	w.Header().Set(HeaderEventCreatedAt, event.CreatedAt.Format(time.RFC3339Nano))
	w.Header().Set(HeaderEventIndex, strconv.FormatInt(eventIndex, 10))
	w.WriteHeader(http.StatusAccepted)
}

// extractNamespace extracts the namespace from a subject (everything before the first dot)
func extractNamespace(subject string) string {
	if before, _, ok := strings.Cut(subject, "."); ok {
		return before
	}
	return ""
}

// namespaceCount returns the number of committed events in the namespace.
// It is used to snapshot the log position for start=newest and to bound the
// scan for start=<event-id>. The namespace append lock is held while reading:
// immuta updates its counters during Save without synchronization, and every
// append goes through that same lock.
func (h *Handler) namespaceCount(ctx context.Context, namespace string) (int64, error) {
	mu := h.namespaceLock(namespace)
	mu.Lock()
	details, err := h.eventsLog.Details(ctx, namespace)
	mu.Unlock()
	if err != nil {
		return -1, err
	}

	var size, count int64
	if _, err := fmt.Sscanf(details, "size: %d, count: %d", &size, &count); err != nil {
		return -1, fmt.Errorf("failed to parse namespace details %q: %w", details, err)
	}

	return count, nil
}

// GET /?subject=a.b.*&start=oldest&ack=manual&redelivery=5s&redelivery_count=3
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	id := newConsumerId()
	subject := qs.Get("subject")
	start := defaultString(qs.Get("start"), DefaultStart)
	ack := defaultStringOneOf(qs.Get("ack"), DefaultAck, AckManual, AckNone)
	redelivery := defaultDuration(qs.Get("redelivery"), DefaultRedelivery)
	redeliveryCount := defaultInt(qs.Get("redelivery_count"), DefaultRedeliveryCount)

	if subject == "" {
		http.Error(w, "missing subject in query string", http.StatusBadRequest)
		return
	}

	if err := ValidateSubject(subject); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	namespace := extractNamespace(subject)
	if namespace == "" {
		http.Error(w, "invalid subject: missing namespace", http.StatusBadRequest)
		return
	}
	if strings.ContainsAny(namespace, "*>") {
		http.Error(w, "subject namespace cannot contain wildcards", http.StatusBadRequest)
		return
	}

	// SSE clients reconnect with the id of the last event they received;
	// resuming from there (exclusive) takes precedence over the start
	// parameter so no events are lost or duplicated across reconnects.
	if lastEventId := r.Header.Get(headerLastEventId); strings.HasPrefix(lastEventId, "e_") {
		start = lastEventId
	}

	logger.InfoContext(ctx, "new consumer", "id", id, "subject", subject, "start", start, "ack", ack, "redelivery", redelivery, "redelivery_count", redeliveryCount)
	defer logger.InfoContext(ctx, "consumer closed", "id", id)

	if namespace == busNamespace {
		h.getInbox(w, r, id, subject)
		return
	}

	// snapshot the committed event count: it is the position for
	// start=newest, and the upper bound of the start=<event-id> scan
	count, err := h.namespaceCount(ctx, namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var startPos int64
	switch start {
	case StartOldest:
		startPos = 0
		start = ""
	case StartNewest:
		// start right after every event committed so far
		startPos = count
		start = ""
	default:
		// start holds an event id: scan from the beginning until we find
		// it, then deliver everything after it
		startPos = 0
	}

	stream := h.eventsLog.Stream(ctx, namespace, startPos)
	defer stream.Done()

	w.Header().Set(HeaderConsumerId, id)

	pusher, err := sse.CreateHttpPusher(w, sse.WithHttpPusherPingDuration(DefaultSsePingTimeout))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Close()

	buf := h.getBuf()
	defer h.putBuf(buf)

	// reusable timer for manual-ack redelivery waits
	var ackTimer *time.Timer
	defer func() {
		if ackTimer != nil {
			ackTimer.Stop()
		}
	}()

	var scanned int64

	for {
		rd, size, err := stream.Next(ctx)
		if errors.Is(err, context.Canceled) || errors.Is(err, immuta.ErrStorageClosed) {
			// the client has closed the connection or the server is shutting down
			return
		} else if err != nil {
			_ = pusher.Push(newSseError(err))
			return
		}

		if int64(cap(*buf)) < size {
			*buf = make([]byte, size)
		}
		record := (*buf)[:size]
		_, err = io.ReadFull(rd, record)
		rd.Done()
		if err != nil {
			_ = pusher.Push(newSseError(err))
			continue
		}

		eventId, eventSubject, err := extractIdSubject(record)
		if err != nil {
			_ = pusher.Push(newSseError(err))
			continue
		}

		if start != "" {
			scanned++
			if eventId == start {
				// found the requested event; deliver everything after it
				start = ""
			} else if scanned >= count {
				// the id is not in the log; error out instead of silently
				// never delivering anything
				_ = pusher.Push(newSseError(fmt.Errorf("start event id %s not found in namespace %s", start, namespace)))
				return
			}
			continue
		}

		if !MatchSubject(eventSubject, subject) {
			continue
		}

		msg := &sse.Message{
			Id:    eventId,
			Event: msgType,
			Data:  string(record),
		}

		if ack != AckManual {
			if err := pusher.Push(msg); err != nil {
				logger.WarnContext(ctx, "failed to push sse message to consumer", "error", err, "event_id", eventId, "consumer_id", id)
				return
			}
			continue
		}

		if !h.deliverManualAck(ctx, pusher, msg, id, eventId, eventSubject, redelivery, redeliveryCount, &ackTimer) {
			return
		}
	}
}

// deliverManualAck pushes msg and waits for the consumer's ack, redelivering
// on timeout up to redeliveryCount attempts (or indefinitely if
// redeliveryCount <= 0). It reports whether the consumer connection is still
// usable.
func (h *Handler) deliverManualAck(
	ctx context.Context,
	pusher sse.Pusher,
	msg *sse.Message,
	consumerId string,
	eventId string,
	eventSubject string,
	redelivery time.Duration,
	redeliveryCount int,
	ackTimer **time.Timer,
) bool {
	key := getAckKey(consumerId, eventId)
	limited := redeliveryCount > 0

	for attempt := 1; ; attempt++ {
		// a fresh channel is registered before every attempt: the previous
		// attempt's channel was removed on timeout, and an ack that raced
		// the timeout must still be able to find an entry to close.
		ch := make(chan struct{})
		h.ackMu.Lock()
		h.waitingAckMap[key] = ch
		h.ackMu.Unlock()

		if err := pusher.Push(msg); err != nil {
			logger.WarnContext(ctx, "failed to push sse message to consumer", "error", err, "event_id", eventId, "consumer_id", consumerId)
			h.removeAck(key)
			return false
		}

		if *ackTimer == nil {
			*ackTimer = time.NewTimer(redelivery)
		} else {
			(*ackTimer).Reset(redelivery)
		}

		select {
		case <-ctx.Done():
			(*ackTimer).Stop()
			h.removeAck(key)
			return false

		case <-ch:
			// acked; the entry was already removed by the Ack handler
			(*ackTimer).Stop()
			logger.DebugContext(ctx, "received ack", "consumer_id", consumerId, "event_id", eventId)
			return true

		case <-(*ackTimer).C:
			h.removeAck(key)

			if limited && attempt >= redeliveryCount {
				logger.WarnContext(ctx, "redelivery count exceeded, dropping event", "consumer_id", consumerId, "event_id", eventId, "subject", eventSubject)
				return true
			}

			logger.WarnContext(ctx, "redelivery", "consumer_id", consumerId, "event_id", eventId, "subject", eventSubject, "redelivery_attempt_count", attempt)
		}
	}
}

func (h *Handler) removeAck(key string) {
	h.ackMu.Lock()
	delete(h.waitingAckMap, key)
	h.ackMu.Unlock()
}

// getInbox streams in-memory inbox events (_bus_.* subjects) to the consumer.
// Inbox subjects are exact (no wildcards) and acking is not applicable.
func (h *Handler) getInbox(w http.ResponseWriter, r *http.Request, id string, subject string) {
	ctx := r.Context()

	if strings.ContainsAny(subject, "*>") {
		http.Error(w, "inbox subjects cannot contain wildcards", http.StatusBadRequest)
		return
	}

	ch := h.inbox.subscribe(subject)
	defer h.inbox.unsubscribe(subject, ch)

	w.Header().Set(HeaderConsumerId, id)

	pusher, err := sse.CreateHttpPusher(w, sse.WithHttpPusherPingDuration(DefaultSsePingTimeout))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pusher.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if err := pusher.Push(&sse.Message{Id: msg.id, Event: msgType, Data: msg.data}); err != nil {
				logger.WarnContext(ctx, "failed to push inbox message to consumer", "error", err, "event_id", msg.id, "consumer_id", id)
				return
			}
		}
	}
}

// PUT /?consumer_id=c_123&event_id=e_456
func (h *Handler) Ack(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	qs := r.URL.Query()

	consumerId := qs.Get("consumer_id")
	eventId := qs.Get("event_id")

	if consumerId == "" {
		http.Error(w, "missing consumer_id in query string", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(consumerId, "c_") {
		http.Error(w, "invalid consumer_id in query string", http.StatusBadRequest)
		return
	}

	if eventId == "" {
		http.Error(w, "missing event_id in query string", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(eventId, "e_") {
		http.Error(w, "invalid event_id in query string", http.StatusBadRequest)
		return
	}

	key := getAckKey(consumerId, eventId)

	h.ackMu.Lock()
	ch, ok := h.waitingAckMap[key]
	if ok {
		delete(h.waitingAckMap, key)
	}
	h.ackMu.Unlock()

	if ok {
		close(ch)
	} else {
		logger.WarnContext(ctx, "failed to find ack channel key", "consumer_id", consumerId, "event_id", eventId)
	}

	w.WriteHeader(http.StatusAccepted)
}

func CreateHandler(logsDirPath string, namespaces []string, secretKey string, blockSize int, dupChecker DuplicateChecker) (*Handler, error) {
	{
		// This block is used to validate the namespaces
		// and make sure there is no reserved and duplicate namespaces

		namespacesSet := make(map[string]struct{})
		for _, ns := range namespaces {
			namespacesSet[ns] = struct{}{}
		}

		// NOTE: the _bus_ namespace is reserved for ephemeral inbox subjects
		// (request/reply, confirm) which are routed in memory and never
		// touch the events log.
		if _, ok := namespacesSet[busNamespace]; ok {
			return nil, errors.New("namespace _bus_ is reserved")
		}

		namespaces = make([]string, 0, len(namespacesSet))
		for ns := range namespacesSet {
			namespaces = append(namespaces, ns)
		}
	}

	immutaOpts := []immuta.OptionFunc{
		immuta.WithLogsDirPath(logsDirPath),
		immuta.WithReaderCount(16),
		immuta.WithFastWrite(true),
		immuta.WithNamespaces(namespaces...),
	}

	if secretKey != "" {
		encryption := NewEncryption(secretKey, blockSize)
		immutaOpts = append(immutaOpts, immuta.WithWriteTransform(encryption.Encode))
		immutaOpts = append(immutaOpts, immuta.WithReadTransform(encryption.Decode))
	}

	eventStorage, err := immuta.New(immutaOpts...)
	if err != nil {
		return nil, err
	}

	return NewHandler(eventStorage, dupChecker), nil
}

func DefaultDuplicateChecker(size int, ttl time.Duration) DuplicateChecker {
	var mtx sync.Mutex

	cacheSubjects := make(map[string]*cache.LRU[string])

	getCache := func(subject string) *cache.LRU[string] {
		mtx.Lock()
		defer mtx.Unlock()

		lru, ok := cacheSubjects[subject]
		if !ok {
			lru = cache.NewLRU[string](size, ttl)
			cacheSubjects[subject] = lru
		}

		return lru
	}

	return DuplicateCheckerFunc(func(key string, subject string) bool {
		if key == "" {
			return false
		}
		return !getCache(subject).Add(key)
	})
}

func NewHandler(eventLogs *immuta.Storage, dupChecker DuplicateChecker) *Handler {
	h := &Handler{
		mux:           http.NewServeMux(),
		eventsLog:     eventLogs,
		waitingAckMap: make(map[string]chan struct{}),
		dupChecker:    dupChecker,
		inbox:         newInbox(),
	}

	// Wrap handlers with CORS middleware
	h.mux.HandleFunc("POST /", corsMiddleware(h.Put))
	h.mux.HandleFunc("GET /", corsMiddleware(h.Get))
	h.mux.HandleFunc("PUT /", corsMiddleware(h.Ack))
	h.mux.HandleFunc("OPTIONS /", corsMiddleware(handleOptions))

	return h
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	allowHeaders := "Content-Type, " + HeaderEventId + ", " + HeaderEventCreatedAt + ", " + HeaderEventIndex + ", " + HeaderConsumerId
	exposeHeaders := HeaderEventId + ", " + HeaderEventCreatedAt + ", " + HeaderEventIndex + ", " + HeaderConsumerId

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		w.Header().Set("Access-Control-Expose-Headers", exposeHeaders)

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next(w, r)
	}
}

func handleOptions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

//
// utilities
//

// extractIdSubject scans a serialized event and returns its id and subject
// without parsing the rest of the record. This is the per-consumer hot path:
// the raw record bytes are forwarded to the consumer as-is, so only these
// two fields are ever needed server side.
func extractIdSubject(b []byte) (id string, subject string, err error) {
	pos := 0

	skipWs := func() {
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\n' || b[pos] == '\t' || b[pos] == '\r') {
			pos++
		}
	}

	skipWs()
	if pos >= len(b) || b[pos] != '{' {
		return "", "", errors.New("expected opening brace")
	}
	pos++

	haveId, haveSubject := false, false

	for pos < len(b) {
		skipWs()
		if pos >= len(b) {
			break
		}
		if b[pos] == '}' {
			break
		}
		if b[pos] != '"' {
			return "", "", errors.New("expected quote before field name")
		}
		fieldStart := pos + 1
		pos++
		for pos < len(b) && b[pos] != '"' {
			pos++
		}
		if pos >= len(b) {
			break
		}
		fieldEnd := pos
		pos++
		skipWs()
		if pos >= len(b) || b[pos] != ':' {
			return "", "", errors.New("expected colon after field name")
		}
		pos++
		skipWs()
		if pos >= len(b) {
			break
		}

		field := b[fieldStart:fieldEnd]
		switch {
		case bytes.Equal(field, []byte("id")):
			val, n, perr := parseString(b[pos:])
			if perr != nil {
				return "", "", perr
			}
			id = val
			pos += n
			haveId = true

		case bytes.Equal(field, []byte("subject")):
			val, n, perr := parseString(b[pos:])
			if perr != nil {
				return "", "", perr
			}
			subject = val
			pos += n
			haveSubject = true

		default:
			n, perr := skipJSONValue(b[pos:])
			if perr != nil {
				return "", "", perr
			}
			pos += n
		}

		if haveId && haveSubject {
			return id, subject, nil
		}

		skipWs()
		if pos < len(b) && b[pos] == ',' {
			pos++
		}
	}

	if haveId && haveSubject {
		return id, subject, nil
	}
	return "", "", errors.New("event record missing id or subject")
}

// skipJSONValue returns the number of bytes occupied by the next JSON value.
func skipJSONValue(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, io.ErrUnexpectedEOF
	}

	switch b[0] {
	case '"':
		_, n, err := parseString(b)
		return n, err

	case '{', '[':
		depth := 0
		inString := false
		pos := 0
		for pos < len(b) {
			c := b[pos]
			if inString {
				switch c {
				case '\\':
					pos++
				case '"':
					inString = false
				}
			} else {
				switch c {
				case '{', '[':
					depth++
				case '}', ']':
					depth--
					if depth == 0 {
						return pos + 1, nil
					}
				case '"':
					inString = true
				}
			}
			pos++
		}
		return 0, io.ErrUnexpectedEOF

	default:
		// number, true, false, null
		pos := 0
		for pos < len(b) {
			c := b[pos]
			if c == ',' || c == '}' || c == ']' || c == ' ' || c == '\n' || c == '\t' || c == '\r' {
				break
			}
			pos++
		}
		if pos == 0 {
			return 0, errors.New("unexpected token")
		}
		return pos, nil
	}
}

func getAckKey(consumerId, eventId string) string {
	return consumerId + "-" + eventId
}

func defaultString(s string, def string) string {
	if s == "" {
		return def
	}

	return s
}

func defaultInt(s string, def int) int {
	if s == "" {
		return def
	}

	value, err := strconv.Atoi(s)
	if err != nil {
		return def
	}

	return value
}

func defaultDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}

	if d <= 0 {
		return def
	}

	return d
}

func defaultStringOneOf(s string, def string, opts ...string) string {
	if s == "" {
		return def
	}

	if slices.Contains(opts, s) {
		return s
	}

	return def
}

func newSseError(err error) *sse.Message {
	return &sse.Message{
		Event: errorType,
		Data:  err.Error(),
	}
}
