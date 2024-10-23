package action

import (
	"ella.to/bus"
	"github.com/urfave/cli/v2"
)

func AckCommand() *cli.Command {
	return &cli.Command{
		Name:  "ack",
		Usage: "acking event",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2024",
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
		Action: func(c *cli.Context) error {
			host := c.String("host")
			consumerId := c.String("consumer-id")
			eventId := c.String("event-id")

			client := bus.NewClient(host)

			return client.Ack(c.Context, consumerId, eventId)
		},
	}
}
