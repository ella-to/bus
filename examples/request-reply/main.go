package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ella.to/bus.go"
	"ella.to/bus.go/client"
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
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := client.New("http://localhost:2021")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus.Reply(ctx, c, "func.div", func(ctx context.Context, req *Req) (*Resp, error) {
		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return &Resp{Result: req.A / req.B}, nil
	})

	fn := bus.Request[*Req, *Resp](c, "func.div")

	var req *Req
	var resp *Resp
	var err error

	req = &Req{A: 4, B: 2}
	resp, err = fn(ctx, req)
	if err != nil {
		fmt.Printf("%s = %s\n", req, err)
	} else {
		fmt.Printf("%s = %s\n", req, resp)
	}

	time.Sleep(1 * time.Second)

	req = &Req{A: 2, B: 0}
	resp, err = fn(ctx, req)
	if err != nil {
		fmt.Printf("%s = %s\n", req, err)
	} else {
		fmt.Printf("%s = %s\n", req, resp)
	}
}
