package server

import (
	"context"

	"ella.to/bus"
)

type ActionType int

const (
	_                ActionType = iota
	PutEvent                    // PutEvent is used to put an event into the bus
	RegisterConsumer            // RegisterConsumer is used to register a consumer
	PushEvent                   // PushEvent is used to push an event to relevent consumers
	AckEvent                    // AckEvent is used to acknowledge an event
	DeleteConsumer              // DeleteConsumer is used to delete a consumer
	Cleanup                     // Cleanup is used to clean up the bus for expired events and consumers
)

type Action struct {
	Type        ActionType
	Event       *bus.Event
	Consumer    *bus.Consumer
	ConsumerId  string
	EventId     string
	Error       chan error
	BatchEvents <-chan []*bus.Event
	Ctx         context.Context

	isPool bool
}

func (a *Action) clean() {
	a.Type = 0
	a.Event = nil
	a.Error = nil
	a.Ctx = nil
}

func newAction(isPool bool) *Action {
	return &Action{
		Error:  make(chan error, 1),
		isPool: isPool,
	}
}

type Dispatcher struct {
	actions     chan *Action
	actionsPool chan *Action
	closeSignal chan struct{}

	putEventFunc         func(ctx context.Context, evt *bus.Event) error
	registerConsumerFunc func(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error)
	pushEventFunc        func(ctx context.Context, eventId string, consumerId string)
	ackEventFunc         func(ctx context.Context, consumerId, eventId string) error
	deleteConsumerFunc   func(ctx context.Context, consumerId string) error
}

func (d *Dispatcher) PutEvent(ctx context.Context, evt *bus.Event) error {
	action := d.getAction()
	action.Type = PutEvent
	action.Event = evt
	action.Ctx = ctx

	d.pushAction(action)

	return <-action.Error
}

func (d *Dispatcher) RegisterConsumer(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error) {
	action := d.getAction()
	action.Type = RegisterConsumer
	action.Consumer = consumer
	action.Ctx = ctx

	d.pushAction(action)

	err := <-action.Error
	if err != nil {
		return nil, err
	}

	return action.BatchEvents, nil
}

func (d *Dispatcher) PushEvent(ctx context.Context, eventId string, consumerId string) {
	action := d.getAction()
	action.Type = PushEvent
	action.Ctx = ctx
	action.EventId = eventId
	action.ConsumerId = consumerId

	d.pushAction(action)
}

func (d *Dispatcher) AckEvent(ctx context.Context, consumerId, eventId string) error {
	action := d.getAction()
	action.Type = AckEvent
	action.ConsumerId = consumerId
	action.EventId = eventId
	action.Ctx = ctx

	d.pushAction(action)

	return <-action.Error
}

func (d *Dispatcher) DeleteConsumer(ctx context.Context, consumerId string) error {
	action := d.getAction()
	action.Type = DeleteConsumer
	action.ConsumerId = consumerId
	action.Ctx = ctx

	d.pushAction(action)

	return <-action.Error
}

func (d *Dispatcher) getAction() *Action {
	select {
	case action := <-d.actionsPool:
		return action
	default:
		// if the pool is empty, create a new action
		// the new action will not be returned to the pool
		// and will be garbage collected by the Go runtime
		return newAction(false)
	}
}

func (d *Dispatcher) pushAction(action *Action) {
	d.actions <- action
}

func (d *Dispatcher) run() {
	for {
		select {
		case <-d.closeSignal:
			return
		case action := <-d.actions:
			switch action.Type {
			case PutEvent:
				action.Error <- d.putEventFunc(action.Ctx, action.Event)
			case RegisterConsumer:
				batchEvents, err := d.registerConsumerFunc(action.Ctx, action.Consumer)
				action.BatchEvents = batchEvents
				action.Error <- err
			case PushEvent:
				d.pushEventFunc(action.Ctx, action.EventId, action.ConsumerId)
			case AckEvent:
				action.Error <- d.ackEventFunc(action.Ctx, action.ConsumerId, action.EventId)
			case DeleteConsumer:
				action.Error <- d.deleteConsumerFunc(action.Ctx, action.ConsumerId)
			}

			action.clean()

			if action.isPool {
				d.actionsPool <- action
			}
		}
	}
}

func (d *Dispatcher) Close() {
	close(d.closeSignal)
}

func NewDispatcher(bufferSize int, poolSize int, fns ...any) *Dispatcher {
	d := &Dispatcher{
		actions:     make(chan *Action, bufferSize),
		actionsPool: make(chan *Action, poolSize),
		closeSignal: make(chan struct{}),
	}

	for _, fn := range fns {
		switch f := fn.(type) {
		case func(context.Context, *bus.Event) error:
			d.putEventFunc = f
		case func(context.Context, *bus.Consumer) (<-chan []*bus.Event, error):
			d.registerConsumerFunc = f
		case func(context.Context, string, string):
			d.pushEventFunc = f
		case func(context.Context, string, string) error:
			d.ackEventFunc = f
		case func(context.Context, string) error:
			d.deleteConsumerFunc = f
		}
	}

	if d.putEventFunc == nil {
		panic("putEventFunc is required")
	}

	if d.registerConsumerFunc == nil {
		panic("registerConsumerFunc is required")
	}

	if d.pushEventFunc == nil {
		panic("pushEventFunc is required")
	}

	if d.ackEventFunc == nil {
		panic("ackEventFunc is required")
	}

	if d.deleteConsumerFunc == nil {
		panic("deleteConsumerFunc is required")
	}

	for i := 0; i < poolSize; i++ {
		d.actionsPool <- newAction(true)
	}

	go d.run()

	return d
}
