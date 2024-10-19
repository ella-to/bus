package bus_test

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ella.to/bus"
)

func setupTestBusServer(t *testing.T, testName string, truncate bool) *bus.Client {
	dirPath := filepath.Join(".", "test_output", testName)

	if truncate {
		os.RemoveAll(dirPath)
	}

	os.MkdirAll(dirPath, os.ModePerm)
	handler, err := bus.NewHandler(dirPath)
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		os.Remove(dirPath)
	})

	fmt.Println("server started at", server.URL)

	return bus.NewClient(server.URL)
}

func TestBusClient_Put(t *testing.T) {
	client := setupTestBusServer(t, "TestBusClient_Put", true)

	_, err := client.Put(context.TODO(), bus.WithSubject("a.b.c"), bus.WithData("hello"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestBusClient_PutGet(t *testing.T) {
	client := setupTestBusServer(t, "TestBusClient_PutGet", true)

	_, err := client.Put(context.TODO(), bus.WithSubject("a.b.c"), bus.WithData("hello"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for event, err := range client.Get(ctx, bus.WithSubject("a.b.c")) {
		if err != nil {
			t.Fatal(err)
		}

		if event.Subject != "a.b.c" {
			t.Fatalf("expected subject to be a.b.c, got %s", event.Subject)
		}

		if bytes.Equal(event.Payload, []byte("hello")) {
			t.Fatalf("expected data to be hello, got %s", event.Payload)
		}
	}
}

func TestBusClient_Redelivery(t *testing.T) {
	client := setupTestBusServer(t, "TestBusClient_Redelivery", true)

	_, err := client.Put(context.TODO(), bus.WithSubject("a.b.c"), bus.WithData(fmt.Sprintf("hello")))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	next, stop := iter.Pull2(client.Get(ctx, bus.WithSubject("a.b.c")))
	defer stop()

	event, err, ok := next()
	if !ok {
		t.Fatal("expected event")
	}

	if err != nil {
		t.Fatal(err)
	}

	if event.Subject != "a.b.c" {
		t.Fatalf("expected subject to be a.b.c, got %s", event.Subject)
	}

	if bytes.Equal(event.Payload, []byte("hello")) {
		t.Fatalf("expected data to be hello, got %s", event.Payload)
	}

	fmt.Println(event)

	// ignoring acking the event
	// we should get the same event again

	event, err, ok = next()
	if !ok {
		t.Fatal("expected event")
	}

	if err != nil {
		t.Fatal(err)
	}
}
