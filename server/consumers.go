package server

import (
	"ella.to/bus"
)

// NOTE: there is no need to protect this map with a mutex
// as it will be accessed only by a single goroutine
type ConsumersEventsMap struct {
	consumers map[string]chan *bus.Event
}

func (c *ConsumersEventsMap) AddConsumer(consumerId string, batchSize int64) chan *bus.Event {
	events := make(chan *bus.Event, batchSize)
	c.consumers[consumerId] = events
	return events
}

func (c *ConsumersEventsMap) RemoveConsumer(consumerId string) {
	delete(c.consumers, consumerId)
}

func (c *ConsumersEventsMap) GetConsumer(consumerId string) (chan *bus.Event, bool) {
	consumer, ok := c.consumers[consumerId]
	return consumer, ok
}

// SendEvent is a non-blocking operation
// if the consumer is not ready to receive the event, the message will be dropped
// there are other chances to send the event to the consumer
func (c *ConsumersEventsMap) SendEvent(consumerId string, event *bus.Event) {
	consumer, ok := c.GetConsumer(consumerId)
	if !ok {
		return
	}

	select {
	case consumer <- event:
	default:
	}
}

func NewConsumersEventsMap() *ConsumersEventsMap {
	return &ConsumersEventsMap{
		consumers: make(map[string]chan *bus.Event),
	}
}
