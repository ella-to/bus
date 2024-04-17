package track

import (
	"sync"
	"time"
)

type TickFunc func(key string, fn func())

type track struct {
	timer *time.Timer
	count int
	fn    func()
}

func Create(d time.Duration, n int) TickFunc {
	var mu sync.Mutex
	var tracks = make(map[string]*track)

	return func(key string, fn func()) {
		mu.Lock()
		t, ok := tracks[key]
		if !ok {
			t = &track{
				count: n,
			}
			t.timer = time.AfterFunc(d, func() {
				mu.Lock()
				delete(tracks, key)
				mu.Unlock()
				t.fn()
			})
			tracks[key] = t // No need to lock/unlock here
		}
		mu.Unlock()
		t.fn = fn
		t.count--

		if t.count == 0 {
			t.timer.Stop()
			mu.Lock()
			delete(tracks, key)
			mu.Unlock()
			go fn()
		}
	}
}
