package action

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
)

// bus put --host http://localhost:8080 --confirm-count 1 --subject a.b.c --data "hello"

func PutCommand() *cli.Command {
	return &cli.Command{
		Name:  "put",
		Usage: "publishing an event",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2021",
			},
			&cli.StringFlag{
				Name:     "subject",
				Usage:    "subject to publish to",
				Required: true,
			},
			&cli.Int64Flag{
				Name:  "confirm",
				Usage: "number of confirmations to wait for",
				Action: func(ctx context.Context, cmd *cli.Command, value int64) error {
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
		Action: func(ctx context.Context, cmd *cli.Command) error {
			host := cmd.String("host")
			subject := cmd.String("subject")
			confirmCount := cmd.Int64("confirm")
			data := cmd.String("data")

			client := bus.NewClient(host)

			resp := client.Put(
				ctx,
				bus.WithSubject(subject),
				bus.WithData(data),
				bus.WithConfirm(int(confirmCount)),
			)
			if resp.Error() != nil {
				return resp.Error()
			}

			fmt.Fprintf(cmd.ErrWriter, "%s\n", resp)
			fmt.Fprintf(cmd.Writer, "%s\n", string(resp.Payload))

			return nil
		},
	}
}
