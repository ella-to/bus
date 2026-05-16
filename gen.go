package bus

import "github.com/rs/xid"

const (
	// inboxNamespace is the namespace used for short-lived reply subjects.
	// Events on this namespace are routed through Core NATS so they aren't
	// persisted in JetStream.
	inboxNamespace = "_bus_"
	inboxPrefix    = inboxNamespace + "."
)

func newInboxSubject() string { return newId(inboxPrefix) }
func newEventId() string      { return newId("e_") }
func newConsumerId() string   { return newId("c_") }

func newId(prefix string) string { return prefix + xid.New().String() }
