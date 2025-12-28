package cache

import (
	"sync"
	"testing"
)

func TestAddReturnsTrueForNewAndFalseForExisting(t *testing.T) {
	l := NewLRU[string](3)

	if !l.Add("a") {
		t.Fatal(`expected Add("a") to return true for new key`)
	}

	if l.Add("a") {
		t.Fatal(`expected Add("a") to return false for existing key`)
	}

	if !l.Add("b") {
		t.Fatal(`expected Add("b") to return true for new key`)
	}

	if l.Add("b") {
		t.Fatal(`expected Add("b") to return false for existing key`)
	}
}

func TestLRUEvictionAndOrdering(t *testing.T) {
	l := NewLRU[int](2)

	// Add 1 and 2
	if !l.Add(1) {
		t.Fatal("expected Add(1) to return true")
	}
	if !l.Add(2) {
		t.Fatal("expected Add(2) to return true")
	}

	// Add 1 again -> exists
	if l.Add(1) {
		t.Fatal("expected Add(1) to return false for existing key")
	}

	// Adding 3 should evict the least recently used (which is 2)
	if !l.Add(3) {
		t.Fatal("expected Add(3) to return true")
	}

	// 2 was evicted, so adding 2 should return true
	if !l.Add(2) {
		t.Fatal("expected Add(2) to return true after eviction")
	}

	// Adding 2 will evict the previous back (1), so 1 should no longer be present
	if !l.Add(1) {
		t.Fatal("expected Add(1) to return true because 1 should have been evicted")
	}
}

func TestAddConcurrent(t *testing.T) {
	l := NewLRU[int](100)
	var wg sync.WaitGroup
	const goroutines = 1000

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(i % 200)
		}(i)
	}

	wg.Wait()

	if l.list.Len() > 100 {
		t.Fatalf("list length %d exceeds capacity %d", l.list.Len(), 100)
	}
}
