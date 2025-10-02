# Bus HTTP API Documentation

The Bus HTTP API provides three main endpoints for publishing events, subscribing to event streams, and acknowledging message delivery. This REST-like API uses Server-Sent Events (SSE) for real-time event streaming.

## Base URL

By default, the Bus server runs on:
```
http://localhost:2021
```

## Endpoints Overview

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/` | Publish an event to a subject |
| `GET` | `/` | Subscribe to events via Server-Sent Events (SSE) |
| `PUT` | `/` | Acknowledge receipt of a message |

---

## 1. PUT (Publish Event)

Publish a new event to a subject.

### Request

**Method:** `POST`

**Endpoint:** `/`

**Headers:**
```
Content-Type: application/json
```

**Body:**
```json
{
  "subject": "string",
  "payload": any,
  "trace_id": "string (optional)"
}
```

**Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `subject` | string | Yes | The subject to publish to. Must follow format: `namespace.topic` (e.g., `drawing.room123`) |
| `payload` | any | Yes | The event payload. Can be any valid JSON value |
| `trace_id` | string | No | Optional trace ID for distributed tracing |

**Subject Format:**

Subjects must be in the format `namespace.topic[.subtopic...]`:
- First segment before `.` is the namespace (must be enabled on server)
- Remaining segments form the topic hierarchy
- Valid characters: alphanumeric, `-`, `_`, `.`
- Wildcards not allowed in publish subject

**Example Request:**

```bash
curl -X POST http://localhost:2021/ \
  -H "Content-Type: application/json" \
  -d '{
    "subject": "drawing.room123",
    "payload": {
      "type": "draw",
      "from": {"x": 100, "y": 150},
      "to": {"x": 105, "y": 155},
      "color": "#000000"
    }
  }'
```

### Response

**Status Code:** `202 Accepted` (on success)

**Headers:**

| Header | Description | Example |
|--------|-------------|---------|
| `X-BUS-EVENT-ID` | Unique event identifier | `e_abc12345` |
| `X-BUS-EVENT-CREATED-AT` | Event creation timestamp (RFC3339Nano) | `2024-03-15T10:30:45.123456789Z` |
| `X-BUS-EVENT-INDEX` | Sequential index in the event log | `42` |

**Example Response Headers:**
```
HTTP/1.1 202 Accepted
X-BUS-EVENT-ID: e_abc12345
X-BUS-EVENT-CREATED-AT: 2024-03-15T10:30:45.123456789Z
X-BUS-EVENT-INDEX: 42
```

**Error Responses:**

| Status Code | Description |
|-------------|-------------|
| `400 Bad Request` | Invalid request body or missing required fields |
| `500 Internal Server Error` | Failed to append event to log |

---

## 2. GET (Subscribe to Events)

Subscribe to events from a subject using Server-Sent Events (SSE). This endpoint establishes a long-lived connection and streams events in real-time.

### Request

**Method:** `GET`

**Endpoint:** `/`

**Query String Parameters:**

| Parameter | Type | Required | Possible Values | Default | Description |
|-----------|------|----------|-----------------|---------|-------------|
| `subject` | string | **Yes** | Valid subject with wildcards | - | Subject pattern to subscribe to |
| `start` | string | No | `oldest`, `newest`, or event ID (e.g., `e_abc123`) | `newest` | Where to start consuming events from |
| `ack` | string | No | `manual`, `none` | `none` | Acknowledgment strategy |
| `redelivery` | duration | No | Valid duration (e.g., `5s`, `1m`, `30s`) | `5s` | Time to wait before redelivering unacknowledged messages (only used with `ack=manual`) |

**Subject Patterns with Wildcards:**

Subjects in GET requests support wildcards for flexible subscriptions:
- `*` matches a single segment
- `**` matches zero or more segments
- Examples:
  - `drawing.*` - matches `drawing.room1`, `drawing.room2`, etc.
  - `chat.room1.*` - matches all events in room1
  - `logs.**` - matches all events under logs namespace

**Start Position Values:**

| Value | Description |
|-------|-------------|
| `oldest` | Start from the first event in the log |
| `newest` | Start from new events only (don't replay history) |
| `e_<id>` | Start from a specific event ID (e.g., `e_abc123`) |

**Acknowledgment Strategies:**

| Value | Description | Use Case |
|-------|-------------|----------|
| `none` | No acknowledgment required. Server pushes events as fast as possible | High-throughput scenarios where message loss is acceptable |
| `manual` | Client must explicitly acknowledge each message via ACK endpoint | Critical messages requiring guaranteed delivery |

**Redelivery Duration:**

Only applicable when `ack=manual`. If a message is not acknowledged within this duration, it will be redelivered.
- Format: Go duration string (e.g., `5s`, `100ms`, `1m30s`)
- Must be positive
- Default: `5s`

**Example Requests:**

```bash
# Subscribe to all drawing events, starting from oldest
curl "http://localhost:2021/?subject=drawing.*&start=oldest"

# Subscribe to new events only with manual acknowledgment
curl "http://localhost:2021/?subject=chat.room1.*&start=newest&ack=manual&redelivery=10s"

# Resume from a specific event
curl "http://localhost:2021/?subject=logs.**&start=e_xyz789"
```

### Response

**Status Code:** `200 OK`

**Headers:**

| Header | Description | Example |
|--------|-------------|---------|
| `Content-Type` | SSE content type | `text/event-stream` |
| `X-BUS-CONSUMER-ID` | Unique consumer identifier | `c_def67890` |

**Server-Sent Events Format:**

The server streams events in SSE format. Each event has:

```
event: msg
data: <JSON>

event: error
data: <error message>
```

**Event Types:**

| Event Type | Description | Data Format |
|------------|-------------|-------------|
| `msg` | Normal event message | JSON object with event data |
| `error` | Error occurred | Plain text error message |

**Message Event Data Structure:**

```json
{
  "id": "e_abc12345",
  "subject": "drawing.room123",
  "payload": { ... },
  "trace_id": "optional-trace-id",
  "created_at": "2024-03-15T10:30:45.123456789Z"
}
```

**Example JavaScript Client:**

```javascript
const sse = new EventSource(
  'http://localhost:2021/?subject=drawing.*&start=oldest'
);

sse.addEventListener('msg', (event) => {
  const data = JSON.parse(event.data);
  console.log('Received event:', data);
  
  // If using manual ack, acknowledge the message
  // await fetch(`http://localhost:2021/?consumer_id=${consumerId}&event_id=${data.id}`, {
  //   method: 'PUT'
  // });
});

sse.addEventListener('error', (event) => {
  console.error('SSE error:', event.data);
});
```

**Error Responses:**

| Status Code | Description |
|-------------|-------------|
| `400 Bad Request` | Missing or invalid `subject` parameter |
| `500 Internal Server Error` | Failed to initialize event stream |

---

## 3. ACK (Acknowledge Message)

Acknowledge receipt of a message. Only used when subscribing with `ack=manual`.

### Request

**Method:** `PUT`

**Endpoint:** `/`

**Query String Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `consumer_id` | string | **Yes** | Consumer ID received in the `X-BUS-CONSUMER-ID` header from GET request (format: `c_<id>`) |
| `event_id` | string | **Yes** | Event ID to acknowledge (format: `e_<id>`) |

**Example Request:**

```bash
curl -X PUT "http://localhost:2021/?consumer_id=c_def67890&event_id=e_abc12345"
```

**Example JavaScript:**

```javascript
async function ackMessage(consumerId, eventId) {
  await fetch(`http://localhost:2021/?consumer_id=${consumerId}&event_id=${eventId}`, {
    method: 'PUT'
  });
}

// In SSE handler
sse.addEventListener('msg', async (event) => {
  const data = JSON.parse(event.data);
  
  // Process the message
  handleMessage(data);
  
  // Acknowledge it
  await ackMessage(consumerId, data.id);
});
```

### Response

**Status Code:** `202 Accepted` (on success)

**Error Responses:**

| Status Code | Description |
|-------------|-------------|
| `400 Bad Request` | Missing or invalid `consumer_id` or `event_id` parameter |
| `500 Internal Server Error` | Failed to process acknowledgment |

---

## CORS Support

All endpoints include CORS headers to allow browser-based clients:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PUT, OPTIONS
Access-Control-Allow-Headers: Content-Type, X-BUS-EVENT-ID, X-BUS-EVENT-CREATED-AT, X-BUS-EVENT-INDEX, X-BUS-CONSUMER-ID
Access-Control-Expose-Headers: X-BUS-EVENT-ID, X-BUS-EVENT-CREATED-AT, X-BUS-EVENT-INDEX, X-BUS-CONSUMER-ID
```

Preflight `OPTIONS` requests are supported on all endpoints.

---

## Complete Usage Examples

### Example 1: Simple Pub/Sub (No Acknowledgment)

**Publisher:**
```bash
# Publish events
curl -X POST http://localhost:2021/ \
  -H "Content-Type: application/json" \
  -d '{"subject": "notifications.user1", "payload": {"message": "Hello!"}}'
```

**Subscriber:**
```javascript
const sse = new EventSource(
  'http://localhost:2021/?subject=notifications.user1&start=newest'
);

sse.addEventListener('msg', (event) => {
  const data = JSON.parse(event.data);
  console.log('New notification:', data.payload);
});
```

### Example 2: Reliable Delivery with Manual Acknowledgment

**Subscriber with ACK:**
```javascript
// Start subscription with manual ack
const sse = new EventSource(
  'http://localhost:2021/?subject=orders.*&start=oldest&ack=manual&redelivery=30s'
);

let consumerId = null;

// Get consumer ID from initial connection
sse.addEventListener('open', () => {
  // Consumer ID is available in the response headers
  // In browser, you'd need to capture it from the fetch response before EventSource
});

sse.addEventListener('msg', async (event) => {
  const data = JSON.parse(event.data);
  
  try {
    // Process the order
    await processOrder(data.payload);
    
    // Acknowledge successful processing
    await fetch(`http://localhost:2021/?consumer_id=${consumerId}&event_id=${data.id}`, {
      method: 'PUT'
    });
  } catch (error) {
    console.error('Failed to process order:', error);
    // Don't ack - message will be redelivered after 30s
  }
});
```

### Example 3: Real-Time Chat Application

**Send message:**
```javascript
async function sendMessage(roomId, message) {
  const response = await fetch('http://localhost:2021/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      subject: `chat.${roomId}`,
      payload: {
        user: 'Alice',
        text: message,
        timestamp: Date.now()
      }
    })
  });
  
  const eventId = response.headers.get('X-BUS-EVENT-ID');
  console.log('Message sent with ID:', eventId);
}
```

**Receive messages:**
```javascript
function joinChatRoom(roomId) {
  const sse = new EventSource(
    `http://localhost:2021/?subject=chat.${roomId}&start=oldest`
  );
  
  sse.addEventListener('msg', (event) => {
    const data = JSON.parse(event.data);
    displayMessage(data.payload);
  });
  
  return sse;
}
```

---

## Performance Considerations

### Throttling

When publishing events at high rates, consider throttling to reduce message volume:

```javascript
// Throttle to 30 events per second
const throttle = (func, limit) => {
  let lastCall = 0;
  return (...args) => {
    const now = Date.now();
    if (now - lastCall >= limit) {
      lastCall = now;
      func(...args);
    }
  };
};

const publishThrottled = throttle(publishEvent, 1000 / 30);
```

### Message History

- Using `start=oldest` replays all historical events, which can be bandwidth-intensive for large logs
- For new subscribers, consider using `start=newest` or a recent event ID
- Archive or compact old events based on your retention policy

## See Also

- [Drawing Example](examples/drawing/) - Complete multiplayer drawing app
- [Bus CLI Documentation](README.md) - Server setup and configuration
