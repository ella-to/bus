package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
	"ella.to/immuta"
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
			&cli.StringFlag{
				Name:     "namespace",
				Usage:    "namespace to filter events",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "secret-key",
				Usage: `secret key used to encrypt the events log files`,
				Value: "",
			},
			&cli.IntFlag{
				Name:  "block-size",
				Usage: `block size used to encrypt the events log files`,
				Value: 4 * 1024,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dir := cmd.String("dir")
			namespace := cmd.String("namespace")

			secretKey := cmd.String("secret-key")
			blockSize := cmd.Int("block-size")

			opts := []immuta.OptionFunc{
				immuta.WithLogsDirPath(dir),
				immuta.WithNamespaces(namespace),
				immuta.WithFastWrite(true),
				immuta.WithReaderCount(2),
			}

			if secretKey != "" {
				encryption := bus.NewEncryption(secretKey, blockSize)
				opts = append(opts, immuta.WithWriteTransform(encryption.Encode))
				opts = append(opts, immuta.WithReadTransform(encryption.Decode))
			}

			im, err := immuta.New(opts...)
			if err != nil {
				log.Fatal(err)
			}

			defer im.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream := im.Stream(ctx, namespace, 0)
			defer stream.Done()

			var count int64
			var size int64

			for {
				err := func() error {
					timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()

					r, s, err := stream.Next(timeoutCtx)
					if err != nil {
						return err
					}
					defer func() { _ = r.Done() }()

					var event bus.Event

					err = json.NewDecoder(r).Decode(&event)
					if err != nil {
						return err
					}

					count++
					size += s

					_ = json.NewEncoder(cmd.Writer).Encode(event)
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

			fmt.Fprintf(cmd.Writer, "-----\n")
			fmt.Fprintf(cmd.Writer, "total events:\t%d\n", count)
			fmt.Fprintf(cmd.Writer, "total size:\t%d bytes\n", size)
			fmt.Fprintf(cmd.Writer, "-----\n")

			return nil
		},
	}
}
