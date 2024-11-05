package bus

import (
	"github.com/rs/xid"
)

func newInboxSubject() string {
	return newId("i.")
}

func newEventId() string {
	return newId("e_")
}

func newConsumerId() string {
	return newId("c_")
}

func newId(prefix string) string {
	return prefix + xid.New().String()
}