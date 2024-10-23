package action

import (
	"fmt"

	"ella.to/bus"
	"github.com/urfave/cli/v2"
)

// bus put --host http://localhost:8080 --type event|req-resp|confirm --subject a.b.c --data "{}"
// bus put --host http://localhost:8080 --confirm-count 1 --subject a.b.c --data "hello"

func PutCommand() *cli.Command {
	return &cli.Command{
		Name:  "put",
		Usage: "publishing an event",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2024",
			},
			&cli.StringFlag{
				Name:     "subject",
				Usage:    "subject to publish to",
				Required: true,
			},
			&cli.Int64Flag{
				Name:  "confirm-count",
				Usage: "number of confirmations to wait for",
				Action: func(c *cli.Context, value int64) error {
					if value < 1 {
						return cli.Exit("confirm-count should be greater than 0", 1)
					}

					return nil
				},
			},
			&cli.StringFlag{
				Name:  "data",
				Usage: "data to publish",
				Value: "{}",
			},
		},
		Action: func(c *cli.Context) error {
			host := c.String("host")
			subject := c.String("subject")
			confirmCount := c.Int64("confirm-count")
			data := c.String("data")

			client := bus.NewClient(host)

			resp, err := client.Put(c.Context,
				bus.WithSubject(subject),
				bus.WithData(data),
				bus.WithConfirm(int(confirmCount)),
			)
			if err != nil {
				return err
			}

			if resp != nil {
				if err := resp.Error(); err != nil {
					return err
				}

				fmt.Fprintf(c.App.Writer, "%s\n", string(resp))
			}

			return nil
		},
	}
}
