package data

import "fmt"

type Req struct {
	A int
	B int
}

func (r *Req) String() string {
	return fmt.Sprintf("func.div(%d, %d)", r.A, r.B)
}

type Resp struct {
	Result int
}

func (r *Resp) String() string {
	return fmt.Sprintf("%d", r.Result)
}
