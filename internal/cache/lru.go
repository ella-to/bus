package cache

import (
	"container/list"
	"sync"
)

type LRU[K comparable] struct {
	capacity int
	cache    map[K]*list.Element
	list     *list.List
	mtx      sync.Mutex
}

func NewLRU[K comparable](capacity int) *LRU[K] {
	return &LRU[K]{
		capacity: capacity,
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
		l.list.MoveToFront(elem)
		return false
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			l.list.Remove(back)
			delete(l.cache, back.Value.(K))
		}
	}

	elem := l.list.PushFront(key)
	l.cache[key] = elem

	return true
}
