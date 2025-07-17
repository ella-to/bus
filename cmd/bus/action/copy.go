package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"time"

	"ella.to/immuta"
	"github.com/urfave/cli/v2"

	"ella.to/bus"
)

func CopyCommand() *cli.Command {
	return &cli.Command{
		Name:  "copy",
		Usage: "copy events from one namespace to another",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dir",
				Usage: "directory to store events and consumers information",
				Value: "./bus_data",
			},
			&cli.StringFlag{
				Name:     "src",
				Usage:    "source namespace to copy events from",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "dst",
				Usage:    "destination namespace to copy events to",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "skip-ids",
				Usage: "skip events with these ids, comma-separated",
			},
		},
		Action: func(c *cli.Context) error {
			dir := c.String("dir")
			src := c.String("src")
			dst := c.String("dst")
			skipIDs := strings.Split(c.String("skip-ids"), ",")

			immutaSrc, err := immuta.New(
				immuta.WithLogsDirPath(dir),
				immuta.WithNamespaces(src),
				immuta.WithFastWrite(true),
				immuta.WithReaderCount(2),
			)
			if err != nil {
				log.Fatal(err)
			}

			defer immutaSrc.Close()

			immutaDst, err := immuta.New(
				immuta.WithLogsDirPath(dir),
				immuta.WithNamespaces(dst),
				immuta.WithFastWrite(true),
				immuta.WithReaderCount(2),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream := immutaSrc.Stream(ctx, src, 0)
			defer stream.Done()

			var count int64
			var size int64
			var skippedCount int64

			for {
				err := func() error {
					ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
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

					if slices.Contains(skipIDs, event.Id) {
						skippedCount++
						return nil
					}

					_, _, err = immutaDst.Append(ctx, dst, &event)
					if err != nil {
						return err
					}

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
			fmt.Fprintf(c.App.Writer, "skipped events:\t%d\n", skippedCount)
			fmt.Fprintf(c.App.Writer, "destination namespace:\t%s\n", dst)
			fmt.Fprintf(c.App.Writer, "-----\n")

			return nil
		},
	}
}
