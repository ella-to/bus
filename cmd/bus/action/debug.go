package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"ella.to/immuta"
	"github.com/urfave/cli/v2"

	"ella.to/bus"
)

func DebugCommand() *cli.Command {
	return &cli.Command{
		Name:  "debug",
		Usage: "show all events by reading events.log",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dir",
				Usage: "directory to store events and consumers information",
				Value: "./bus_data",
			},
		},
		Action: func(c *cli.Context) error {
			dir := c.String("dir")

			filepath := fmt.Sprintf("%s/events.log", dir)

			im, err := immuta.New(filepath, 2, true)
			if err != nil {
				log.Fatal(err)
			}

			defer im.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream := im.Stream(ctx, 0)
			defer stream.Done()

			var count int64
			var size int64

			for {
				err := func() error {
					ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()

					r, s, err := stream.Next(ctx)
					if err != nil {
						return err
					}
					defer r.Done()

					var event bus.Event

					err = json.NewDecoder(r).Decode(&event)
					if err != nil {
						return err
					}

					count++
					size += s

					json.NewEncoder(c.App.Writer).Encode(event)
					return nil
				}()
				if errors.Is(err, io.EOF) {
					break
				} else if errors.Is(err, context.DeadlineExceeded) {
					break
				} else if err != nil {
					return err
				}
			}

			fmt.Fprintf(c.App.Writer, "-----\n")
			fmt.Fprintf(c.App.Writer, "total events:\t%d\n", count)
			fmt.Fprintf(c.App.Writer, "total size:\t%d bytes\n", size)
			fmt.Fprintf(c.App.Writer, "-----\n")

			return nil
		},
	}
}
