package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable] struct {
	key   K
	added time.Time
}

type LRU[K comparable] struct {
	capacity int
	ttl      time.Duration
	cache    map[K]*list.Element
	list     *list.List
	mtx      sync.Mutex
}

func NewLRU[K comparable](capacity int, ttl time.Duration) *LRU[K] {
	return &LRU[K]{
		capacity: capacity,
		ttl:      ttl,
		cache:    make(map[K]*list.Element),
		list:     list.New(),
	}
}

// Add adds a key to the LRU cache.
// It returns true if the key was not already present, false otherwise.
func (l *LRU[K]) Add(key K) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if elem, ok := l.cache[key]; ok {
		e := elem.Value.(entry[K])
		if l.ttl > 0 && time.Since(e.added) > l.ttl {
			// expired, remove it
			l.list.Remove(elem)
			delete(l.cache, key)
		} else {
			l.list.MoveToFront(elem)
			return false
		}
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			e := back.Value.(entry[K])
			l.list.Remove(back)
			delete(l.cache, e.key)
		}
	}

	elem := l.list.PushFront(entry[K]{key: key, added: time.Now()})
	l.cache[key] = elem

	return true
}
