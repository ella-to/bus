package sse

import "encoding/json"

var (
	doubleEnters = []byte("\n\n")
	singleEnter  = []byte("\n")
	idPrefix     = []byte("id: ")
	eventPrefix  = []byte("event: ")
	dataPrefix   = []byte("data: ")
)

type Payload struct {
	Id    int64           `json:"id"`
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func (p *Payload) Decode(v any) error {
	return json.Unmarshal(p.Data, v)
}
