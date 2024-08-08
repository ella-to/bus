package event

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/sqlite"
)

type Func func(ctx context.Context, wdb *sqlite.Worker, evt *bus.Event) error

type FuncsMap map[string][]Func

type Sync struct {
	wdb          *sqlite.Worker
	busClient    *client.Client
	mapper       FuncsMap
	mux          sync.RWMutex
	locked       bool
	subject      string
	continueName string
}

func (s *Sync) Register(subject string, fn Func) {
	if strings.Contains(subject, "*") {
		panic("subject should not contains * in event.Sync.Register")
	}

	if s.IsLocked() {
		panic("event.Sync.Register should not be called after event.Sync.Lock")
	}

	s.mapper[subject] = append(s.mapper[subject], fn)
}

func (s *Sync) Lock() error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.locked {
		return fmt.Errorf("event.Sync.Lock already called")
	}

	s.locked = true
	return nil
}

func (s *Sync) IsLocked() bool {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.locked
}

func (s *Sync) Once(ctx context.Context, wait time.Duration) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	pull, stop := iter.Pull2(s.busClient.Get(
		ctx,
		bus.WithBatchSize(10),
		bus.WithDurable(),
		bus.WithId("event.sync.once"),
		bus.WithFromOldest(),
		bus.WithSubject(s.subject),
	))
	defer stop()

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

		for _, evt := range msg.Events {
			for _, fn := range s.mapper[evt.Subject] {
				err := fn(ctx, s.wdb, evt)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (s *Sync) Continue(ctx context.Context) error {
	if !s.IsLocked() {
		return fmt.Errorf("event.Sync.Once should be called after event.Sync.Lock")
	}

	for msg, err := range s.busClient.Get(
		ctx,
		bus.WithBatchSize(10),
		bus.WithDurable(),
		bus.WithId(s.continueName),
		bus.WithFromNewest(),
		bus.WithSubject(s.subject),
	) {
		if err != nil {
			return err
		}

		for _, evt := range msg.Events {
			for _, fn := range s.mapper[evt.Subject] {
				err := fn(ctx, s.wdb, evt)
				if err != nil {
					return err
				}
			}
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

func NewSync(wdb *sqlite.Worker, busClient *client.Client, opts ...syncOption) *Sync {
	sync := &Sync{
		wdb:          wdb,
		mapper:       make(FuncsMap),
		busClient:    busClient,
		subject:      "*",
		continueName: "event.sync.continue",
	}

	for _, opt := range opts {
		opt.configureSync(sync)
	}

	return sync
}

func FnDB[T any](fn func(context.Context, *sqlite.Worker, *T) error) Func {
	return func(ctx context.Context, wdb *sqlite.Worker, evt *bus.Event) error {
		var obj T
		err := json.Unmarshal(evt.Data, &obj)
		if err != nil {
			return err
		}

		return fn(ctx, wdb, &obj)
	}
}
