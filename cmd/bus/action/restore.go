package action

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
)

// bus restore --host http://localhost:2021 --input-dir ./

func RestoreCommand() *cli.Command {
	return &cli.Command{
		Name:  "restore",
		Usage: "restoring events from a backup file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "bus server address to connect to",
				Value: "http://localhost:2021",
			},
			&cli.StringFlag{
				Name:     "input-dir",
				Usage:    "input dir to read the backup files from",
				Required: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			inputDir := cmd.String("input-dir")
			host := cmd.String("host")

			files, err := getFilesInDir(inputDir)
			if err != nil {
				return err
			}

			if len(files) == 0 {
				return fmt.Errorf("no files found in the input dir: %s", inputDir)
			}

			client := bus.NewClient(host)

			for _, file := range files {
				err := func() error {
					inputFile, err := os.Open(file)
					if err != nil {
						return fmt.Errorf("failed to open input file: %w", err)
					}
					defer inputFile.Close()

					scanner := bufio.NewScanner(inputFile)

					for scanner.Scan() {
						line := scanner.Bytes()

						decoder := json.NewDecoder(bytes.NewReader(line))

						var event bus.Event
						if err = decoder.Decode(&event); err != nil {
							return fmt.Errorf("failed to decode event from file %s: %w", file, err)
						}

						err = client.Put(
							ctx,
							bus.WithId(event.Id),
							bus.WithTraceId(event.TraceId),
							bus.WithSubject(event.Subject),
							bus.WithData(event.Payload),
							bus.WithCreatedAt(event.CreatedAt),
						).Error()
						if err != nil {
							return fmt.Errorf("failed to put event from file %s: %w", file, err)
						}
					}

					fmt.Printf("restored event from file: %s\n", filepath.Base(file))

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

func getFilesInDir(dirPath string) ([]string, error) {
	var files []string

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, filepath.Join(dirPath, entry.Name()))
		}
	}

	return files, nil
}
