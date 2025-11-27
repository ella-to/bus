package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"

	"ella.to/bus"
	"ella.to/bus/cmd/bus/action"
)

func main() {
	app := &cli.Command{
		Name:        "bus",
		Description: "a simple event bus system",
		Version:     bus.Version,
		Commands: []*cli.Command{
			action.ServerCommand(),
			action.GetCommand(),
			action.PutCommand(),
			action.AckCommand(),
			action.DebugCommand(),
			action.CopyCommand(),
			action.DumpCommand(),
			action.RestoreCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
