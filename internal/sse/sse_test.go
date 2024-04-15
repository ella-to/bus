package sse_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"ella.to/bus.go/internal/sse"
)

func TestSSE(t *testing.T) {
	ctx := context.Background()

	w := httptest.NewRecorder()

	pusher, err := sse.CreatePusher(w, sse.WithHeader("Sample-Id", "123"))
	if err != nil {
		t.Fatal(err)
	}

	err = pusher.Push(ctx, "event", "data")
	if err != nil {
		t.Fatal(err)
	}

	err = pusher.Done(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ch := sse.Receive(ctx, w.Result().Body)

	msg := <-ch

	if msg.Id != 1 {
		t.Fatalf("expected id 1, got %d", msg.Id)
	}

	if msg.Event != "event" {
		t.Fatalf("expected event event, got %s", msg.Event)
	}

	if string(msg.Data) != `"data"` {
		t.Fatalf(`expected data "data", got %s`, string(msg.Data))
	}

	msg = <-ch

	if msg.Id != 2 {
		t.Fatalf("expected id 2, got %d", msg.Id)
	}

	if msg.Event != "done" {
		t.Fatalf("expected event done, got %s", msg.Event)
	}

	if string(msg.Data) != `{}` {
		t.Fatalf(`expected data {}, got %s`, string(msg.Data))
	}
}
