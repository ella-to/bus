# BUS

## Server

- remove all all ephemeral consumers
- remvoe all expired events
- load durable and queued consumers into memory using special Trie data structure for pattern matching
- when a consumer connects load events based on `last_event_id` and `bacth_size` and push all those events to consumer
- when a consumer acks. `last_event_id` will set to that event and we trigger event load again
- when a new event gets published, first we store it to events table then push the message to consumers' channel

```golang



```
