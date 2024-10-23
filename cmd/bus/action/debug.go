package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"ella.to/bus"
	"ella.to/immuta"
	"github.com/urfave/cli/v2"
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
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "show all the raw data of event",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			dir := c.String("dir")
			showRaw := c.Bool("raw")

			filepath := fmt.Sprintf("%s/events.log", dir)

			im, err := immuta.New(immuta.WithFastWrite(filepath))
			if err != nil {
				log.Fatal(err)
			}

			defer im.Close()

			stream, err := im.Stream(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			defer stream.Done()

			var count int64
			var size int64
			for {
				r, s, err := stream.Next()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					return err
				}

				var event bus.Event

				err = json.NewDecoder(r).Decode(&event)
				if err != nil {
					return err
				}

				count++
				size += s

				if showRaw {
					json.NewEncoder(c.App.Writer).Encode(event)
				} else {
					fmt.Fprintf(c.App.Writer, "% 6d: %s\n", count, event.String())
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
