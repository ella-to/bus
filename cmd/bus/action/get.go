package action

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
)

// get command
// bus get --host http://localhost:2021 --subject a.b.c --start newest --ack auto --redelivery 5s

func GetCommand() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "subscribing to a subject pattern and receiving events",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2021",
			},
			&cli.StringFlag{
				Name:     "subject",
				Usage:    "subject to subscribe to",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "start",
				Usage: "start consuming from, oldest, newest or event id",
				Action: func(ctx context.Context, cmd *cli.Command, value string) error {
					if value != "newest" && value != "oldest" && !strings.HasPrefix(value, "e_") {
						return cli.Exit("invalid position, it should be one of newest, oldest or and event id", 1)
					}
					return nil
				},
				Value: bus.StartNewest,
			},
			&cli.StringFlag{
				Name:  "ack",
				Usage: "ack strategy, auto, manual or none",
				Action: func(ctx context.Context, cmd *cli.Command, value string) error {
					if value != "auto" && value != "manual" && value != "none" {
						return cli.Exit("invalid ack strategy, it should be one of auto, manual or none", 1)
					}
					return nil
				},
				Value: "auto",
			},
			&cli.DurationFlag{
				Name:  "redelivery-duration",
				Usage: "redelivery duration",
				Value: 5 * time.Second,
			},
			&cli.IntFlag{
				Name:  "redelivery-count",
				Usage: "redelivery count",
				Value: 3,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			host := cmd.String("host")
			subject := cmd.String("subject")
			start := cmd.String("start")
			ack := cmd.String("ack")
			redelivery := cmd.Duration("redelivery-duration")
			redeliveryCount := cmd.Int("redelivery-count")
			autoAck := ack == "auto"

			if autoAck {
				ack = bus.AckManual
			}

			client := bus.NewClient(host)

			for event, err := range client.Get(
				ctx,
				bus.WithSubject(subject),
				bus.WithStartFrom(start),
				bus.WithDelivery(redelivery, redeliveryCount),
				bus.WithAckStrategy(ack),
				bus.WithExtractMeta(func(meta map[string]string) {
					if v, ok := meta["consumer-id"]; ok {
						fmt.Fprintf(cmd.ErrWriter, "consumer-id: %s\n", v)
					}
				}),
			) {
				if err != nil {
					return err
				}

				err = json.NewEncoder(cmd.Writer).Encode(event)
				if err != nil {
					return err
				}

				if autoAck {
					if err := event.Ack(ctx); err != nil {
						return err
					}
				}
			}

			return nil
		},
	}
}
