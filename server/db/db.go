package db

import (
	"context"
	"time"

	"ella.to/bus.go"
	"ella.to/bus.go/internal/sqlite"
)

func New(ctx context.Context, notify NotifyFunc, opts ...sqlite.OptionFunc) (*sqlite.Database, error) {
	opts = append(opts, sqlite.WithFunctions(map[string]*sqlite.FunctionImpl{
		"notify": {
			NArgs: 8,
			MakeAggregate: func(ctx sqlite.Context) (sqlite.AggregateFunction, error) {
				return notify, nil
			},
			Deterministic: true,
			AllowIndirect: true,
		},
	}))

	db, err := sqlite.New(opts...)
	if err != nil {
		return nil, err
	}

	err = migration(ctx, db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type NotifyFunc func(consumerId string, event *bus.Event)

func (fn NotifyFunc) Step(ctx sqlite.Context, args []sqlite.Value) error {
	id := args[0].Text()
	subject := args[1].Text()
	reply := args[2].Text()
	size := args[3].Int64()
	data := make([]byte, size)
	copy(data, args[4].Blob())
	createdAt := time.Unix(args[5].Int64(), 0)
	expiresAt := time.Unix(args[6].Int64(), 0)
	consumerId := args[7].Text()

	fn(consumerId, &bus.Event{
		Id:        id,
		Subject:   subject,
		Reply:     reply,
		Data:      data,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	})

	return nil
}

func (NotifyFunc) WindowInverse(ctx sqlite.Context, args []sqlite.Value) error {
	return nil
}

func (NotifyFunc) WindowValue(ctx sqlite.Context) (sqlite.Value, error) {
	return sqlite.IntegerValue(0), nil
}

func (NotifyFunc) Finalize(ctx sqlite.Context) {}
