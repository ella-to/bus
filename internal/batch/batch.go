package batch

import (
	"sort"
	"time"

	"ella.to/bus"
)

type Sort struct {
	events     []*bus.Event
	duration   time.Duration
	size       int
	currSize   int
	addEventCh chan *bus.Event
	closeCh    chan struct{}
	batchFn    func(events []*bus.Event)
}

func (s *Sort) process() {
	for {
		select {
		case event := <-s.addEventCh:
			s.events[s.currSize] = event
			s.currSize++

			if s.currSize == s.size {
				s.callBatch(s.events[:s.currSize])
			}

		case <-time.After(s.duration):
			if s.currSize > 0 {
				s.callBatch(s.events[:s.currSize])
			}
		}
	}
}

func (s *Sort) Close() {
	close(s.closeCh)
}

func (s *Sort) Add(event *bus.Event) {
	select {
	case <-s.closeCh:
		return
	case s.addEventCh <- event:
	}
}

func (s *Sort) callBatch(events []*bus.Event) {
	sort.Slice(events, func(i, j int) bool {
		return events[i].Id < events[j].Id
	})

	s.batchFn(events)
	s.currSize = 0
}

func NewSort(maxSize int, duration time.Duration, batchFn func(events []*bus.Event)) *Sort {
	sort := &Sort{
		events:     make([]*bus.Event, maxSize),
		duration:   duration,
		size:       maxSize,
		currSize:   0,
		closeCh:    make(chan struct{}),
		addEventCh: make(chan *bus.Event, maxSize*2),
		batchFn:    batchFn,
	}

	go sort.process()

	return sort
}
