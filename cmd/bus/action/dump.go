package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"ella.to/bus"
	"github.com/urfave/cli/v2"
)

// bus dump --host http://localhost:2021 --namespace test --output-dir ./

func DumpCommand() *cli.Command {
	return &cli.Command{
		Name:  "dump",
		Usage: "connectiong to the bus server and dumping all events",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2021",
			},
			&cli.StringFlag{
				Name:     "namespaces",
				Usage:    "list of namespaces separated by comma",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "output-dir",
				Usage: "output dir to save the dumped events",
				Value: "./",
			},
		},
		Action: func(c *cli.Context) error {
			host := c.String("host")
			namespaces := getSliceValues("", c.String("namespaces"), ",")
			outputDir := c.String("output-dir")

			_, err := os.Stat(outputDir)
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("output dir is already exist: %s", outputDir)
			}

			err = os.MkdirAll(outputDir, os.ModePerm)
			if err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}

			client := bus.NewClient(host)

			for _, namespace := range namespaces {
				fmt.Printf("dumping namespace: %s\r", namespace)

				err := func() error {
					outputFile, err := os.Create(filepath.Join(outputDir, namespace) + ".log")
					if err != nil {
						return fmt.Errorf("failed to create output file for %s: %w", namespace, err)
					}
					defer outputFile.Close()

					var i int64

					signal := make(chan struct{}, 1)
					ctx, cancel := context.WithCancel(c.Context)
					go func() {
						defer cancel()

						for {
							select {
							case <-signal:
								// reset the timeout
							case <-time.After(2 * time.Second):
								return
							case <-ctx.Done():
								return
							}
						}
					}()

					for msg, err := range client.Get(
						ctx,
						bus.WithSubject(namespace+".>"),
						bus.WithStartFrom(bus.StartOldest),
						bus.WithAckStrategy(bus.AckNone),
					) {
						if errors.Is(err, context.Canceled) {
							return nil
						} else if err != nil {
							return fmt.Errorf("failed to pull message for %s: %w", namespace, err)
						}

						// singla the goroutine to reset the timeout and not cancel the context
						signal <- struct{}{}

						if i != 0 {
							_, err = fmt.Fprintf(outputFile, "\n")
							if err != nil {
								return fmt.Errorf("failed to write new line for %s: %w", namespace, err)
							}
						}

						if err != nil {
							return fmt.Errorf("failed to pull message for %s: %w", namespace, err)
						}

						b, err := json.Marshal(msg)
						if err != nil {
							return fmt.Errorf("failed to marshal message for %s: %w", namespace, err)
						}

						_, err = outputFile.Write(b)
						if err != nil {
							return fmt.Errorf("failed to write message for %s: %w", namespace, err)
						}

						i++
						fmt.Printf("dumping namespace: %s, (%d)\r", namespace, i)
					}

					return nil
				}()
				if err != nil {
					return err
				}
			}

			return nil
		},
	}
}
