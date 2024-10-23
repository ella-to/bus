package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"

	"ella.to/bus"
	"ella.to/bus/cmd/bus/action"
)

func main() {
	app := &cli.App{
		Name:        "bus",
		Description: "a simple event bus system",
		Version:     bus.Version,
		Commands: []*cli.Command{
			action.ServerCommand(),
			action.GetCommand(),
			action.PutCommand(),
			action.AckCommand(),
			action.DebugCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
