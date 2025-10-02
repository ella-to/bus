# Multiplayer Drawing App

A real-time collaborative drawing application built with Bus event streaming. Multiple users can draw together on a shared canvas with synchronized strokes, colors, and canvas clearing.

## Features

- 🎨 Real-time multiplayer drawing
- 🌈 Color selection (Black, Red, Green, Blue, White)
- ✏️ Adjustable brush thickness (1-50px)
- 🧹 Synchronized canvas clearing
- 📱 Touch support for mobile devices
- ⚡ Optimized with throttling (30 fps)
- 🔄 Automatic replay of drawing history for new joiners

## Prerequisites

- Bus CLI installed on your system
- A modern web browser with JavaScript enabled

## Installation

### Install Bus CLI

To install the Bus CLI tool, run:

```bash
go install ella.to/bus/cmd/bus@latest
```

Make sure your Go bin directory is in your PATH. You can verify the installation:

```bash
bus --version
```

## Running the Application

### 1. Start the Bus Server

Start the Bus server with the "drawing" namespace:

```bash
bus server --namespaces "drawing"
```

This will start the Bus server on `http://localhost:2021` (default port).

**Server Options:**
- `--namespaces`: Specify the namespaces to enable (required)
- `--addr`: Change the server address (default: `:2021`)
- `--logs-dir`: Specify the directory for event logs (default: `./logs`)

### 2. Open the HTML File

Simply open the `index.html` file in your web browser:

```bash
open index.html
```

Or double-click the file in your file explorer.

### 3. Create a Multiplayer Session

When you open the page for the first time, it will automatically generate a unique room ID and add it to the URL:

```
file:///path/to/index.html?room=abc12345
```

To enable multiplayer drawing:

1. **Copy the URL** from your browser's address bar
2. **Open a new browser tab or window** (or send to a friend)
3. **Paste the URL** and press Enter

Now both browser windows are connected to the same drawing room! Any drawing action in one window will appear in real-time in the other.

## How It Works

### Architecture

The application uses Bus as a message broker to synchronize drawing events across multiple clients:

```
┌──────────┐          ┌──────────┐          ┌──────────┐
│ Client 1 │◄────────►│   Bus    │◄────────►│ Client 2 │
│ Browser  │  Events  │  Server  │  Events  │ Browser  │
└──────────┘          └──────────┘          └──────────┘
```

### Event Flow

1. **Connection**: Each client connects to Bus via Server-Sent Events (SSE) on subject `drawing.{roomId}`
2. **Drawing**: When a user draws, the client:
   - Renders the line locally
   - Publishes a `draw` event to Bus with line coordinates, color, and thickness
3. **Receiving**: Other clients subscribed to the same room receive the event and render it
4. **History**: New clients automatically receive all previous drawing events (`start=oldest`)

### Message Types

**Draw Event:**
```json
{
  "id": "client123",
  "type": "draw",
  "from": {"x": 100, "y": 150},
  "to": {"x": 105, "y": 155},
  "color": "#000000",
  "thickness": 5
}
```

**Clear Event:**
```json
{
  "id": "client123",
  "type": "clear"
}
```

## Customization

### Changing the Server URL

If your Bus server is running on a different host or port, update these lines in `index.html`:

```javascript
const sse = new EventSource(
  `http://localhost:2021/?subject=${subject}&start=oldest`
);

async function putMsg(payload) {
  return fetch("http://localhost:2021/", {
    // ...
  });
}
```

### Adding More Colors

Add more color buttons in the HTML:

```html
<button
  class="color-btn"
  data-color="#ffcc00"
  style="background: #ffcc00"
  title="Yellow"
></button>
```

### Adjusting Performance

Change the throttle rate (currently 30 fps):

```javascript
// Increase for smoother drawing (more messages)
const throttledDraw = throttle(draw, 1000 / 60); // 60 fps

// Decrease for better performance (fewer messages)
const throttledDraw = throttle(draw, 1000 / 15); // 15 fps
```

## Troubleshooting

### Connection Errors

If you see connection errors in the browser console:

1. Make sure the Bus server is running: `bus server --namespaces "drawing"`
2. Check that the server is accessible at `http://localhost:2021`
3. Verify no firewall is blocking the connection

## License

This example is part of the Bus project. See the main repository for license information.
