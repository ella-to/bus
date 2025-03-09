package action

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
╚═════╝░░╚═════╝░╚═════╝░ %s, %s

`

func ServerCommand() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "starting the bus server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "addr",
				Usage: "address to listen on",
				Value: "0.0.0.0:2021",
			},
			&cli.StringFlag{
				Name:  "path",
				Usage: "dir path to events log files",
				Value: "./bus_data",
			},
			&cli.StringFlag{
				Name:  "namespaces",
				Usage: "list of namespaces separated by comma",
			},
		},
		Action: func(c *cli.Context) error {
			logLevel := getLogLevel(getValue(os.Getenv("BUS_LOG_LEVEL"), "INFO"))
			addr := getValue(os.Getenv("BUS_ADDR"), c.String("addr"))
			path := getValue(os.Getenv("BUS_PATH"), c.String("path"))
			namespaces := getSliceValues(os.Getenv("BUS_NAMESPACES"), c.String("namespaces"), ",")

			slog.SetLogLoggerLevel(logLevel)

			if len(namespaces) == 0 {
				return fmt.Errorf("no namespaces provided")
			}

			if err := os.MkdirAll(filepath.Base(path), os.ModePerm); err != nil {
				return err
			}

			server, err := bus.NewServer(addr, path, namespaces)
			if err != nil {
				return err
			}

			// Channel to listen for interrupt signals
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

			// Goroutine to start the server
			go func() {
				fmt.Printf(logo, bus.Version, bus.GitCommit)
				slog.Info("server started", "address", addr, "namespaces", namespaces, "events_log_file", path)

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

func getSliceValues(a string, b string, split string) []string {
	if a == "" {
		a = b
	}

	if a == "" {
		return []string{}
	}

	return strings.Split(b, split)
}

func getValue(seq ...string) string {
	for _, s := range seq {
		if s != "" {
			return s
		}
	}
	return ""
}

func getLogLevel(value string) slog.Level {
	switch strings.ToUpper(value) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
