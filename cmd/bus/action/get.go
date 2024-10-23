package action

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ella.to/bus"
	"github.com/urfave/cli/v2"
)

// get command
// bus get --host http://localhost:2024 --subject a.b.c --name test --position newest --auto-ack --redelivery-duration 5s

func GetCommand() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "subscribing to a subject pattern and receiving events",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2024",
			},
			&cli.StringFlag{
				Name:     "subject",
				Usage:    "subject to subscribe to",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "name",
				Usage: "name of durable consumer(s)",
			},
			&cli.StringFlag{
				Name:  "position",
				Usage: "position to start consuming from",
				Action: func(c *cli.Context, value string) error {
					if value != "newest" && value != "oldest" && !strings.HasPrefix(value, "e_") {
						return cli.Exit("invali position, it should be one of newest, oldest or and event id", 1)
					}
					return nil
				},
				Value: "newest",
			},
			&cli.BoolFlag{
				Name:  "auto-ack",
				Usage: "auto acknowledge messages",
			},
			&cli.DurationFlag{
				Name:  "redelivery-duration",
				Usage: "redelivery duration",
				Value: 5 * time.Second,
			},
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "show all the raw data of event",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			host := c.String("host")
			subject := c.String("subject")
			name := c.String("name")
			position := c.String("position")
			autoAck := c.Bool("auto-ack")
			redeliveryDuration := c.Duration("redelivery-duration")
			showRaw := c.Bool("raw")

			client := bus.NewClient(host)

			for event, err := range client.Get(c.Context,
				bus.WithSubject(subject),
				bus.WithName(name),
				bus.WithCustomPosition(position),
				bus.WithRedeliveryDelay(redeliveryDuration),
				bus.WithExtractMeta(func(meta map[string]string) {
					if v, ok := meta["consumer-id"]; ok {
						fmt.Fprintf(c.App.ErrWriter, "consumer-id: %s\n", v)
					}
				}),
			) {
				if err != nil {
					return err
				}

				if showRaw {
					err = json.NewEncoder(c.App.Writer).Encode(event)
					if err != nil {
						return err
					}
				} else {
					fmt.Fprintf(c.App.Writer, "%s\n", event)
				}

				if autoAck {
					if err := event.Ack(c.Context); err != nil {
						return err
					}
				}
			}

			return nil
		},
	}
}
