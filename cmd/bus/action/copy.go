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

	"github.com/urfave/cli/v3"

	"ella.to/bus"
	"ella.to/immuta"
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
			&cli.StringFlag{
				Name:  "src-secret-key",
				Usage: `secret key used to encrypt the events log files for the source`,
				Value: "",
			},
			&cli.IntFlag{
				Name:  "src-block-size",
				Usage: `block size used to encrypt the events log files for the source`,
				Value: 4 * 1024,
			},
			&cli.StringFlag{
				Name:  "dst-secret-key",
				Usage: `secret key used to encrypt the events log file for the destination`,
				Value: "",
			},
			&cli.IntFlag{
				Name:  "dst-block-size",
				Usage: `block size used to encrypt the events log files for the destination`,
				Value: 4 * 1024,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dir := cmd.String("dir")
			src := cmd.String("src")
			dst := cmd.String("dst")
			skipIDs := strings.Split(cmd.String("skip-ids"), ",")
			srcSecretKey := cmd.String("src-secret-key")
			srcBlockSize := cmd.Int("src-block-size")
			dstSecretKey := cmd.String("dst-secret-key")
			dstBlockSize := cmd.Int("dst-block-size")

			srcOpts := []immuta.OptionFunc{
				immuta.WithLogsDirPath(dir),
				immuta.WithNamespaces(src),
				immuta.WithFastWrite(true),
				immuta.WithReaderCount(2),
			}

			if srcSecretKey != "" {
				srcEncryption := bus.NewEncryption(srcSecretKey, srcBlockSize)
				srcOpts = append(srcOpts, immuta.WithWriteTransform(srcEncryption.Encode))
				srcOpts = append(srcOpts, immuta.WithReadTransform(srcEncryption.Decode))
			}

			immutaSrc, err := immuta.New(srcOpts...)
			if err != nil {
				log.Fatal(err)
			}

			defer immutaSrc.Close()

			dstOpts := []immuta.OptionFunc{
				immuta.WithLogsDirPath(dir),
				immuta.WithNamespaces(dst),
				immuta.WithFastWrite(true),
				immuta.WithReaderCount(2),
			}

			if dstSecretKey != "" {
				dstEncryption := bus.NewEncryption(dstSecretKey, dstBlockSize)
				dstOpts = append(dstOpts, immuta.WithWriteTransform(dstEncryption.Encode))
				dstOpts = append(dstOpts, immuta.WithReadTransform(dstEncryption.Decode))
			}

			immutaDst, err := immuta.New(dstOpts...)
			if err != nil {
				return err
			}

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			stream := immutaSrc.Stream(ctx, src, 0)
			defer stream.Done()

			var count int64
			var size int64
			var skippedCount int64

			for {
				err := func() error {
					timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
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

					if slices.Contains(skipIDs, event.Id) {
						skippedCount++
						return nil
					}

					_, _, err = immutaDst.Append(timeoutCtx, dst, &event)
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

			fmt.Fprintf(cmd.Writer, "-----\n")
			fmt.Fprintf(cmd.Writer, "total events:\t%d\n", count)
			fmt.Fprintf(cmd.Writer, "total size:\t%d bytes\n", size)
			fmt.Fprintf(cmd.Writer, "skipped events:\t%d\n", skippedCount)
			fmt.Fprintf(cmd.Writer, "destination namespace:\t%s\n", dst)
			fmt.Fprintf(cmd.Writer, "-----\n")

			return nil
		},
	}
}
