package bus_test

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

func TestBusClient_PutNGet(t *testing.T) {
	client := setupTestBusServer(t, "TestBusClient_PutNGet", true)

	n := 10

	for i := 0; i < n; i++ {
		_, err := client.Put(context.TODO(), bus.WithSubject("a.b.c"), bus.WithData(fmt.Sprintf("hello %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	i := 0

	for event, err := range client.Get(ctx, bus.WithSubject("a.b.c")) {
		if err != nil {
			t.Fatal(err)
		}

		if event.Subject != "a.b.c" {
			t.Fatalf("expected subject to be a.b.c, got %s", event.Subject)
		}

		if bytes.Equal(event.Payload, []byte(fmt.Sprintf("hello %d", i))) {
			t.Fatalf("expected data to be hello %d, got %s", i, event.Payload)
		}

		err := event.Ack(ctx)
		if err != nil {
			t.Fatal(err)
		}

		i++
	}

	if i != n {
		t.Fatalf("expected %d events, got %d", n, i)
	}
}

func TestBusClientQueue_PutNGetM(t *testing.T) {
	client := setupTestBusServer(t, "TestBusClientQueue_PutNGetM", true)

	n := 5

	for i := 0; i < n; i++ {
		_, err := client.Put(context.TODO(), bus.WithSubject("a.b.c"), bus.WithData(fmt.Sprintf("hello %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	i := 0
	j := 0

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for event, err := range client.Get(ctx, bus.WithName("test"), bus.WithSubject("a.b.c")) {
			if err != nil {
				t.Error(err)
				return
			}

			if event.Subject != "a.b.c" {
				t.Errorf("expected subject to be a.b.c, got %s", event.Subject)
				return
			}

			if bytes.Equal(event.Payload, []byte(fmt.Sprintf("hello %d", i))) {
				t.Errorf("expected data to be hello %d, got %s", i, event.Payload)
				return
			}

			err := event.Ack(ctx)
			if err != nil {
				t.Error(err)
			}

			i++
		}
	}()

	go func() {
		defer wg.Done()

		for event, err := range client.Get(ctx, bus.WithName("test"), bus.WithSubject("a.b.c")) {
			if err != nil {
				t.Error(err)
				return
			}

			if event.Subject != "a.b.c" {
				t.Errorf("expected subject to be a.b.c, got %s", event.Subject)
				return
			}

			if bytes.Equal(event.Payload, []byte(fmt.Sprintf("hello %d", i))) {
				t.Errorf("expected data to be hello %d, got %s", i, event.Payload)
				return
			}

			err := event.Ack(ctx)
			if err != nil {
				t.Error(err)
			}

			j++
		}
	}()

	wg.Wait()

	if i+j != n {
		t.Fatalf("expected %d events, got %d", n, i+j)
	}

	if i == 0 {
		t.Fatal("expected i to be greater than 0")
	}

	if j == 0 {
		t.Fatal("expected j to be greater than 0")
	}
}

func TestMatchSubject(t *testing.T) {
	tests := []struct {
		subject  string
		pattern  string
		expected bool
	}{
		// Exact matches
		{"a.b.c", "a.b.c", true},
		{"a.b", "a.b", true},

		// Single wildcard '*'
		{"a.b.c", "a.*.c", true},
		{"a.b.d", "a.*.d", true},
		{"a.x.y", "a.*.*", true},
		{"a.b", "a.*", true},
		{"ab.c", "ab.*", true},

		// Catch-all '>'
		{"a.b.c", "a.>", true},
		{"a.b.c.d", "a.>", true},
		{"a.b.c.d.e", "a.>", true},
		{"a.b", "a.>", true},

		// Mixed wildcard '*' and catch-all '>'
		{"a.b.c", "a.*.>", true},
		{"a.b.c.d", "a.*.>", true},
		{"a.b.c", "a.*.*", true},

		// Non-matching cases
		{"a.b.c", "a.b.d", false},
		{"a.b.c", "a.b", false},
		{"a.b.c", "a.c.>", false},
		{"a.b.c", "a.*.d", false},

		// No wildcards
		{"a.b.c", "a.b.d", false},
		{"a.b.c.d", "a.b.c", false},

		// Edge cases
		{"", "", true},              // Both empty
		{"a.b.c", "", false},        // Empty pattern
		{"", "a.b.c", false},        // Empty subject
		{"a.b.c", "a.b.c.d", false}, // Pattern longer than subject
		{"a.b.c.d", "a.b.c", false}, // Subject longer than pattern
		{"a.b.c", ">", true},        // Catch-all matches everything
		{"a.b.c", "*", false},       // '*' doesn't span across dots
		{"a.b.c", "*.*", false},     // Wildcards must match dot-separated segments
		{"a.b.c", "*.*.*", true},    // Wildcards must match dot-separated segments
	}

	for _, test := range tests {
		result := bus.MatchSubject(test.subject, test.pattern)
		if result != test.expected {
			t.Errorf("MatchSubject(%q, %q) = %v; want %v", test.subject, test.pattern, result, test.expected)
		}
	}
}

func BenchmarkMatchSubject(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bus.MatchSubject("a.b.c", "a.*.c")
	}
}
