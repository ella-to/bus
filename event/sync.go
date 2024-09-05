package event

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"sync/atomic"
	"time"

	"ella.to/bus"
)

type Func func(ctx context.Context, evt *bus.Event) error

type FuncsMap map[string][]Func

const (
	onceName     = "bus-event-sync-once"
	continueName = "bus-event-sync-continue"
)

type Sync struct {
	getter     bus.Getter
	mapper     FuncsMap
	locked     atomic.Int32
	subject    string
	namePrefix string
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

// Start is a simple wrapper around Once and Continue
// if the context gets canceled, contine will be stopped as well
func (s *Sync) Start(ctx context.Context, onceTimeout time.Duration) error {
	err := s.Once(ctx, onceTimeout)
	if err != nil {
		return err
	}

	go func() {
		err = s.Continue(ctx)
		if err != nil {
			slog.Error("event sync has an error", "err", err)
			return
		}
	}()

	// the reason we are sleeping here is to make sure that the continue
	// event is processed before the caller can continue and also if it encounter
	// an error during the start, we can capture it and return it to the caller
	time.Sleep(1 * time.Second)

	return err
}

func (s *Sync) Once(ctx context.Context, wait time.Duration) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	for evt, err := range s.getter.Get(
		ctx,
		bus.WithBatchSize(1),
		bus.WithName(onceName+s.namePrefix),
		bus.WithFromOldest(),
		bus.WithSubject(s.subject),
	) {
		if err != nil {
			return err
		}

		err = s.process(ctx, evt)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Sync) Continue(ctx context.Context) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	for msg, err := range s.getter.Get(
		ctx,
		bus.WithBatchSize(1),
		bus.WithName(continueName+s.namePrefix),
		bus.WithFromNewest(),
		bus.WithSubject(s.subject),
	) {
		if err != nil {
			return err
		}

		err = s.process(ctx, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Sync) process(ctx context.Context, msg *bus.Event) error {
	// ack the message since this each handler can be execute with x amount of time
	// and we don't want to redeliver the same message
	err := msg.Ack()
	if err != nil {
		return err
	}

	for _, fn := range s.mapper[msg.Subject] {
		err := fn(ctx, msg)
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

// WithNamePrefix is used to set the name prefix for the continue event
// This is useful if you have multiple instance of same serive running
// and using a unique name for each instance make sure that each instance
// process their own queue of messages. Restarting the instance with the same
// name will make sure that the instance will continue processing the messages
// from the last point.
func WithNamePrefix(name string) syncOption {
	return syncOptionFunc(func(s *Sync) {
		s.namePrefix = "bus-event-sync-continue-" + name
	})
}

func NewSync(getter bus.Getter, opts ...syncOption) *Sync {
	sync := &Sync{
		mapper: make(FuncsMap),
		getter: getter,
	}

	for _, opt := range opts {
		opt.configureSync(sync)
	}

	if sync.subject == "" {
		panic("event.NewSync: subject is required")
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
