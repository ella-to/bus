```
██████╗░██╗░░░██╗░██████╗
██╔══██╗██║░░░██║██╔════╝
██████╦╝██║░░░██║╚█████╗░
██╔══██╗██║░░░██║░╚═══██╗
██████╦╝╚██████╔╝██████╔╝
╚═════╝░░╚═════╝░╚═════╝░
```

# Introduction

`bus` is a high-performance, persistent message bus designed for simplicity and flexibility, utilizing SQLite as its storage backend. It currently supports the following features:

- [x] Manual Acknowledgment: Consumers must explicitly acknowledge messages.
- [x] Queue Consumers: Implements round-robin distribution among consumers of the same queue.
- [x] Request/Reply: Facilitates building RPC-like features.
- [x] Batching: Supports the grouping of events/messages to enhance performance.
- [x] N Confirm: Enables synchronous operations by requiring N number of acks.
- [x] In-Memory Usage: Can be used in-memory for testing purposes, ensuring speed and flexibility.
- [x] Guaranteed Persistence: Ensures data is not lost and is reliably stored.
- [x] HTTP Compatibility: Allows bindings in other languages through HTTP compatibility.

# Reference

logo was created using https://fsymbols.com/generators/carty
