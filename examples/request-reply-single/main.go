package main

import (
	"context"
	"fmt"

	"ella.to/bus"
	"ella.to/bus/client"
)

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

func main() {
	c, err := client.New(client.WithAddr("http://localhost:2021"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	funcName := "func.div"

	bus.Reply(ctx, c, funcName, func(ctx context.Context, req *Req) (*Resp, error) {
		fmt.Println("Got a request")
		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return &Resp{Result: req.A / req.B}, nil
	})

	fn := bus.Request(c, funcName)

	for range 1000 {
		req := &Req{A: 4, B: 2}
		resp := &Resp{}
		err := fn(ctx, req, resp)
		if err != nil {
			fmt.Printf("%s = %s\n", req, err)
		} else {
			fmt.Printf("%s = %s\n", req, resp)
		}
	}
}
