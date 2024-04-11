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
			NArgs: 6,
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
	size := args[2].Int64()
	data := make([]byte, size)
	copy(data, args[3].Blob())
	createdAt := time.Unix(0, args[4].Int64())
	consumerId := args[5].Text()

	_ = size

	fn(consumerId, &bus.Event{
		Id:        id,
		Subject:   subject,
		Data:      data,
		CreatedAt: createdAt,
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
