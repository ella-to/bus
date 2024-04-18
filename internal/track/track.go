package track

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type TickFunc func(key string, fn func())

type trackCmd int

const (
	call trackCmd = iota
	remove
	execute
)

func (c trackCmd) String() string {
	switch c {
	case call:
		return "call"
	case remove:
		return "remove"
	case execute:
		return "execute"
	default:
		return "unknown"
	}
}

type track struct {
	key   string
	cmd   trackCmd
	timer *time.Timer
	count int
	fn    func()
}

func (t *track) String() string {
	return fmt.Sprintf("key: %s, cmd: %s, count: %d", t.key, t.cmd, t.count)
}

func Create(ctx context.Context, d time.Duration, n int) TickFunc {
	ch := make(chan *track, 10)
	tracks := make(map[string]*track)

	go func() {
		for {
			select {
			case <-ctx.Done():
				slog.Debug("close track goroutine")
				for _, t := range tracks {
					if t.timer != nil {
						t.timer.Stop()
					}
				}
				return
			case t := <-ch:
				switch t.cmd {
				case call:
					curr, ok := tracks[t.key]
					if ok {
						curr.timer.Stop()
						curr.fn = t.fn
					} else {
						tracks[t.key] = t
						curr = t
					}

					curr.count--
					if curr.count <= 0 {
						curr.timer.Stop()
						ch <- &track{
							key: t.key,
							cmd: execute,
						}
					} else {
						curr.timer.Reset(d)
					}
				case remove:
					curr, ok := tracks[t.key]
					if ok {
						curr.timer.Stop()
						delete(tracks, t.key)
					}
				case execute:
					curr, ok := tracks[t.key]
					if ok {
						delete(tracks, t.key)
						curr.fn()
					}
				}
			}
		}
	}()

	return func(key string, fn func()) {
		ch <- &track{
			key:   key,
			cmd:   call,
			fn:    fn,
			count: n,
			timer: time.AfterFunc(d, func() {
				ch <- &track{
					key: key,
					cmd: execute,
				}
			}),
		}
	}
}
