package action

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"ella.to/bus"
)

const logo = `
██████╗░██╗░░░██╗░██████╗
██╔══██╗██║░░░██║██╔════╝
██████╦╝██║░░░██║╚█████╗░
██╔══██╗██║░░░██║░╚═══██╗
██████╦╝╚██████╔╝██████╔╝
╚═════╝░░╚═════╝░╚═════╝░ %s

`

func ServerCommand() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "starting the bus server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "addr",
				Usage: "address to listen on",
				Value: "0.0.0.0:2024",
			},
			&cli.StringFlag{
				Name:  "dir",
				Usage: "directory to store events and consumers information",
				Value: "./bus_data",
			},
		},
		Action: func(c *cli.Context) error {
			addr := c.String("addr")
			dir := c.String("dir")

			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return err
			}

			handler, err := bus.NewHandler(c.Context, dir)
			if err != nil {
				return err
			}

			server := http.Server{
				Addr:    addr,
				Handler: handler,
			}

			// Channel to listen for interrupt signals
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

			// Goroutine to start the server
			go func() {
				fmt.Printf(logo, bus.Version)
				slog.Info("server started", "address", addr, "dir", dir)

				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("failed to start server", "error", err)
				}
			}()

			// Wait for interrupt signal (Ctrl+C)
			<-stop
			slog.Info("Shutting down server...")

			// Create a context with a timeout for the shutdown
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Attempt graceful shutdown
			return server.Shutdown(ctx)
		},
	}
}
