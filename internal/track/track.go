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
		defer mu.Unlock()

		t, ok := tracks[key]
		if !ok {
			t = &track{
				count: n,
			}
			t.timer = time.AfterFunc(d, func() {
				delete(tracks, key)
				t.fn()
			})
			tracks[key] = t
		}
		t.fn = fn
		t.count--

		if t.count == 0 {
			t.timer.Stop()
			delete(tracks, key)
			go fn()
		}
	}
}
