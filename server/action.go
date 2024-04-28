package server

import (
	"context"

	"ella.to/bus"
)

type ActionType int

const (
	_ ActionType = iota
	PutActionType
	GetActionType
	AckActionType
	DeleteActionType
)

type Action struct {
	Type     ActionType
	Event    *bus.Event
	Consumer *bus.Consumer
	EventId  string
	Error    chan error
	Events   <-chan []*bus.Event

	retrunCh chan *Action
}

func (a *Action) Return() {
	// if the return channel is nil, we don't need to return
	// because this is a temporary action which will be garbage
	// collected by the Go runtime
	if a.retrunCh == nil {
		return
	}

	a.retrunCh <- a
}

func newAction(returnCh chan *Action) *Action {
	return &Action{
		Consumer: &bus.Consumer{},
		Error:    make(chan error, 1),
		retrunCh: returnCh,
	}
}

type Actions struct {
	pipe  chan *Action
	queue chan *Action
}

func NewActions(size int64) *Actions {
	a := &Actions{
		pipe:  make(chan *Action, size),
		queue: make(chan *Action, size),
	}

	// fill the queue
	for range size {
		a.queue <- newAction(a.queue)
	}

	return a
}

// grab will try to get an action from the queue
// to prevent creating new action every time we need
// this is pure optimization
func (a *Actions) grab(ctx context.Context) (*Action, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case action := <-a.queue:

		return action, nil
	default:
		// if the queue is empty, we create temporary action
		// this is to avoid blocking the caller and not losing
		// any action. This is usually happen during burst
		return newAction(nil), nil
	}
}

func (a *Actions) push(ctx context.Context, action *Action) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case a.pipe <- action:
		return nil
	}

}

func (a *Actions) Stream() <-chan *Action {
	return a.pipe
}

func (a *Actions) Put(ctx context.Context, event *bus.Event) error {
	action, err := a.grab(ctx)
	if err != nil {
		return err
	}

	defer action.Return()

	action.Type = PutActionType
	action.Event = event

	err = a.push(ctx, action)
	if err != nil {
		return err
	}

	return <-action.Error
}

func (a *Actions) Get(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error) {
	action, err := a.grab(ctx)
	if err != nil {
		return nil, err
	}
	defer action.Return()

	action.Type = GetActionType
	action.Consumer = consumer

	err = a.push(ctx, action)
	if err != nil {
		return nil, err
	}

	err = <-action.Error
	if err != nil {
		return nil, err
	}

	return action.Events, nil
}

func (a *Actions) Ack(ctx context.Context, consumerId string, eventId string) error {
	action, err := a.grab(ctx)
	if err != nil {
		return err
	}
	defer action.Return()

	action.Type = AckActionType
	action.Consumer.Id = consumerId
	action.EventId = eventId

	err = a.push(ctx, action)
	if err != nil {
		return err
	}

	return <-action.Error
}

func (a *Actions) Delete(ctx context.Context, consumerId string) error {
	action, err := a.grab(ctx)
	if err != nil {
		return err
	}
	defer action.Return()

	action.Type = DeleteActionType
	action.Consumer.Id = consumerId

	err = a.push(ctx, action)
	if err != nil {
		return err
	}

	return <-action.Error
}
