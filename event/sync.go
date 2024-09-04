package event

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"

	"sync/atomic"
	"time"

	"ella.to/bus"
)

type Func func(ctx context.Context, evt *bus.Event) error

type FuncsMap map[string][]Func

type Sync struct {
	getter       bus.Getter
	mapper       FuncsMap
	locked       atomic.Int32
	subject      string
	continueName string
}

func (s *Sync) Register(subject string, fn Func) {
	if strings.Contains(subject, "*") || strings.Contains(subject, ">") {
		panic("subject should not contains * or > in event.Sync.Register")
	}

	if s.IsLocked() {
		panic("event.Sync.Register should not be called after event.Sync.Lock")
	}

	s.mapper[subject] = append(s.mapper[subject], fn)
}

func (s *Sync) Lock() error {
	if !s.locked.CompareAndSwap(0, 1) {
		return fmt.Errorf("event.Sync.Lock already called")
	}
	return nil
}

func (s *Sync) IsLocked() bool {
	return s.locked.Load() == 1
}

func (s *Sync) Once(ctx context.Context, wait time.Duration) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pull, stop := iter.Pull2(s.getter.Get(
		ctx,
		bus.WithBatchSize(1),
		bus.WithName("bus-event-sync-once"),
		bus.WithFromOldest(),
		bus.WithSubject(s.subject),
	))
	defer func() {
		cancel()
		stop()
	}()

	timer := time.AfterFunc(wait, stop)

	for {
		timer.Reset(wait)
		msg, err, ok := pull()
		timer.Stop()

		if !ok {
			return nil
		} else if err != nil {
			return err
		}

		for _, fn := range s.mapper[msg.Subject] {
			err := fn(ctx, msg)
			if err != nil {
				return err
			}
		}

		err = msg.Ack()
		if err != nil {
			return err
		}
	}
}

func (s *Sync) Continue(ctx context.Context) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	for msg, err := range s.getter.Get(
		ctx,
		bus.WithBatchSize(10),
		bus.WithName(s.continueName),
		bus.WithFromNewest(),
		bus.WithSubject(s.subject),
	) {
		if err != nil {
			return err
		}

		for _, fn := range s.mapper[msg.Subject] {
			err := fn(ctx, msg)
			if err != nil {
				return err
			}
		}

		err = msg.Ack()
		if err != nil {
			return err
		}
	}

	return nil
}

type syncOption interface {
	configureSync(*Sync)
}

type syncOptionFunc func(*Sync)

func (f syncOptionFunc) configureSync(s *Sync) {
	f(s)
}

func WithSubject(subject string) syncOption {
	return syncOptionFunc(func(s *Sync) {
		s.subject = subject
	})
}

func WithContinueName(name string) syncOption {
	return syncOptionFunc(func(s *Sync) {
		s.continueName = name
	})
}

func NewSync(getter bus.Getter, opts ...syncOption) *Sync {
	sync := &Sync{
		mapper:       make(FuncsMap),
		getter:       getter,
		subject:      "*",
		continueName: "bus-event-sync-continue",
	}

	for _, opt := range opts {
		opt.configureSync(sync)
	}

	return sync
}

func FnDB[T any](fn func(context.Context, *T) error) Func {
	return func(ctx context.Context, evt *bus.Event) error {
		var obj T
		err := json.Unmarshal([]byte(evt.Data), &obj)
		if err != nil {
			return err
		}

		return fn(ctx, &obj)
	}
}
