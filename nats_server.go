package bus

import (
	"errors"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

var (
	ErrFailedTimeout = errors.New("not ready for connection ")
)

type Server = server.Server

type Options struct {
	path       string
	timeout    time.Duration
	storgePath string
	port       int
}

type OptionsNatsFunc func(opt *Options) error

func WithNatsTimeout(timeout time.Duration) OptionsNatsFunc {
	return func(opt *Options) error {
		opt.timeout = timeout
		return nil
	}
}

func WithNatsConfigPath(path string) OptionsNatsFunc {
	return func(opt *Options) error {
		opt.path = path
		return nil
	}
}

func WithNatsStoragePath(storagePath string) OptionsNatsFunc {
	return func(opt *Options) error {
		opt.storgePath = storagePath
		return nil
	}
}

func WithNatsAutoPort(from, to int) OptionsNatsFunc {
	return func(opt *Options) error {
		port, err := findOpenPortInRange(from, to)
		if err != nil {
			return err
		}

		opt.port = port

		return nil
	}
}

func NewNatsServer(optFns ...OptionsNatsFunc) (*server.Server, error) {
	serverOpts := &Options{
		timeout: 5 * time.Second,
		port:    4222,
	}

	for _, optFn := range optFns {
		if err := optFn(serverOpts); err != nil {
			return nil, err
		}
	}

	opts := server.Options{
		Port: serverOpts.port,
	}

	err := opts.ProcessConfigFile(serverOpts.path)
	if err != nil {
		return nil, err
	}

	if serverOpts.storgePath != "" {
		opts.StoreDir = serverOpts.storgePath
	}

	ns, err := server.NewServer(&opts)
	if err != nil {
		return nil, err
	}

	go ns.Start()

	if !ns.ReadyForConnections(serverOpts.timeout) {
		return nil, ErrFailedTimeout
	}

	return ns, nil
}
