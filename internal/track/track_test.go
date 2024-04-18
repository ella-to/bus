package track_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/bus/internal/track"
	"github.com/stretchr/testify/assert"
)

func TestTrack(t *testing.T) {
	const timeout = 500 * time.Millisecond
	const n = 100
	const key = "key"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn := track.Create(ctx, timeout, n)

	for i := 0; i < n; i++ {
		fn(key, func() {
			fmt.Println("key Called on", i)
		})
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	start := time.Now()
	fn(key, func() {
		defer wg.Done()
		fmt.Println("key Called on", time.Since(start).Seconds())
	})

	wg.Wait()
}

func TestTrackShouldBeCalledOnce(t *testing.T) {
	const timeout = 500 * time.Millisecond
	const n = 100
	const key = "key"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn := track.Create(ctx, timeout, n)

	for i := 0; i < n; i++ {
		fn(key, func() {
			fmt.Println("key Called on", i)
		})
	}

	var counter int64

	start := time.Now()
	fn(key, func() {
		atomic.AddInt64(&counter, 1)
		fmt.Println("key Called on", time.Since(start).Seconds())
	})

	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
}

func TestTrackUnderLoad(t *testing.T) {
	const timeout = 500 * time.Millisecond
	const n = 10
	const workerCount = 3
	const workerCalls = 4
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn := track.Create(ctx, timeout, n)

	var wg sync.WaitGroup

	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < workerCalls; j++ {
				fn("key", func() {
					fmt.Printf("@@@@@key from %d Called on %d\n", i, j)
				})
			}
		}(i)
	}

	wg.Wait()

	time.Sleep(timeout * 2)
}
