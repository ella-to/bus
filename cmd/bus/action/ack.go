package action

import (
	"context"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
)

func AckCommand() *cli.Command {
	return &cli.Command{
		Name:  "ack",
		Usage: "acking event",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2021",
			},
			&cli.StringFlag{
				Name:     "consumer-id",
				Usage:    "consumer id",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "event-id",
				Usage:    "event id",
				Required: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			host := cmd.String("host")
			consumerId := cmd.String("consumer-id")
			eventId := cmd.String("event-id")

			client := bus.NewClient(host)

			return client.Ack(ctx, consumerId, eventId)
		},
	}
}
