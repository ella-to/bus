package action

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

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
			&cli.IntFlag{
				Name:  "dup-size",
				Usage: `size of the duplicate checker cache (0 to disable)`,
				Value: 1000,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			logLevel := getLogLevel(getValue(os.Getenv("BUS_LOG_LEVEL"), "INFO"))
			addr := getValue(os.Getenv("BUS_ADDR"), cmd.String("addr"))
			path := getValue(os.Getenv("BUS_PATH"), cmd.String("path"))
			namespaces := getSliceValues(os.Getenv("BUS_NAMESPACES"), cmd.String("namespaces"), ",")
			secretKey := getValue(os.Getenv("BUS_SECRET_KEY"), cmd.String("secret-key"))
			blockSize := getIntValue(os.Getenv("BUS_BLOCK_SIZE"), cmd.Int("block-size"))
			dupCacheSize := getIntValue(os.Getenv("BUS_DUP_SIZE"), cmd.Int("dup-size"))

			if len(namespaces) == 0 {
				return errors.New("no namespaces provided")
			}

			slog.SetLogLoggerLevel(logLevel)

			if len(namespaces) == 0 {
				return fmt.Errorf("no namespaces provided")
			}

			if err := os.MkdirAll(filepath.Base(path), os.ModePerm); err != nil {
				return err
			}

			var dupChecker bus.DuplicateChecker
			if dupCacheSize <= 0 {
				dupChecker = bus.DefaultDuplicateChecker(dupCacheSize)
			}

			handler, err := bus.CreateHandler(path, namespaces, secretKey, blockSize, dupChecker)
			if err != nil {
				return err
			}

			server := &http.Server{
				Addr:    addr,
				Handler: handler,
			}

			// Channel to listen for interrupt signals
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

			// Goroutine to start the server
			go func() {
				fmt.Printf(logo, bus.Version, bus.GitCommit)
				slog.Info("server started", "address", addr, "namespaces", namespaces, "events_log_file", path, "log_level", logLevel.String())

				if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("failed to start server", "error", err)
				}
			}()

			// Wait for interrupt signal (Ctrl+C)
			<-stop
			slog.Info("Shutting down server...")

			// closes all the streams and causes all connected clients to disconnect
			if err = handler.Close(); err != nil {
				slog.Error("failed to close handler", "error", err)
			}

			// Create a context with a timeout for the shutdown
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
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

	results := strings.Split(a, split)

	// Trim spaces and remove empty strings
	var filtered []string
	for _, item := range results {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			filtered = append(filtered, trimmed)
		}
	}

	return filtered
}

func getValue(seq ...string) string {
	for _, s := range seq {
		if s != "" {
			return s
		}
	}
	return ""
}

func getIntValue(value string, defaultValue int) int {
	if value == "" {
		return defaultValue
	}

	v, err := strconv.ParseInt(value, 10, 4)
	if err != nil {
		return defaultValue
	}

	return int(v)
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
