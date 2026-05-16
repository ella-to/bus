package bus

import (
	"errors"
	"fmt"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// DevServer is an embedded NATS server with JetStream enabled. It is meant
// for tests and local development — never for production.
type DevServer struct {
	srv         *natsserver.Server
	storeDir    string
	cleanupDir  bool
	wasMemStore bool
}

type devServerOpts struct {
	host     string
	port     int
	storeDir string
	memStore bool
	logger   natsserver.Logger
	debug    bool
	trace    bool
	mutators []func(*natsserver.Options)
}

// DevServerOption configures NewDevServer.
type DevServerOption func(*devServerOpts)

// WithDevServerHost overrides the listen host (default 127.0.0.1).
func WithDevServerHost(host string) DevServerOption {
	return func(o *devServerOpts) { o.host = host }
}

// WithDevServerPort overrides the listen port. Use -1 (the default) for a
// random free port.
func WithDevServerPort(port int) DevServerOption {
	return func(o *devServerOpts) { o.port = port }
}

// WithDevServerStoreDir uses dir as JetStream's storage location instead of
// a temporary directory. The caller becomes responsible for cleaning it up.
func WithDevServerStoreDir(dir string) DevServerOption {
	return func(o *devServerOpts) { o.storeDir = dir }
}

// WithDevServerMemStore makes JetStream use in-memory storage. Useful for
// tests that want zero on-disk side effects.
func WithDevServerMemStore() DevServerOption {
	return func(o *devServerOpts) { o.memStore = true }
}

// WithDevServerLogger installs a custom logger on the embedded server. By
// default the server is silent.
func WithDevServerLogger(l natsserver.Logger, debug, trace bool) DevServerOption {
	return func(o *devServerOpts) {
		o.logger = l
		o.debug = debug
		o.trace = trace
	}
}

// WithDevServerOptions applies additional raw mutators to the underlying
// natsserver.Options for advanced use cases (TLS, accounts, etc.).
func WithDevServerOptions(fn func(*natsserver.Options)) DevServerOption {
	return func(o *devServerOpts) { o.mutators = append(o.mutators, fn) }
}

// NewDevServer starts an embedded NATS server with JetStream enabled and
// blocks until it is ready to accept connections.
func NewDevServer(opts ...DevServerOption) (*DevServer, error) {
	cfg := &devServerOpts{
		host: "127.0.0.1",
		port: -1,
	}
	for _, o := range opts {
		o(cfg)
	}

	storeDir := cfg.storeDir
	cleanupDir := false
	if storeDir == "" && !cfg.memStore {
		dir, err := os.MkdirTemp("", "bus-nats-")
		if err != nil {
			return nil, fmt.Errorf("create store dir: %w", err)
		}
		storeDir = dir
		cleanupDir = true
	}

	natsOpts := &natsserver.Options{
		Host:      cfg.host,
		Port:      cfg.port,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     cfg.logger == nil,
		NoSigs:    true,
		Debug:     cfg.debug,
		Trace:     cfg.trace,
	}
	for _, m := range cfg.mutators {
		m(natsOpts)
	}

	srv, err := natsserver.NewServer(natsOpts)
	if err != nil {
		if cleanupDir {
			_ = os.RemoveAll(storeDir)
		}
		return nil, fmt.Errorf("new nats server: %w", err)
	}

	if cfg.logger != nil {
		srv.SetLoggerV2(cfg.logger, cfg.debug, cfg.trace, false)
	}

	go srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		if cleanupDir {
			_ = os.RemoveAll(storeDir)
		}
		return nil, errors.New("nats dev server failed to become ready in time")
	}

	return &DevServer{
		srv:         srv,
		storeDir:    storeDir,
		cleanupDir:  cleanupDir,
		wasMemStore: cfg.memStore,
	}, nil
}

// ClientURL returns a URL suitable for nats.Connect / NewClient.
func (d *DevServer) ClientURL() string { return d.srv.ClientURL() }

// Server exposes the underlying nats-server for advanced inspection.
func (d *DevServer) Server() *natsserver.Server { return d.srv }

// StoreDir returns the JetStream storage directory (empty when in-memory).
func (d *DevServer) StoreDir() string {
	if d.wasMemStore {
		return ""
	}
	return d.storeDir
}

// Shutdown stops the embedded server, waits for it to fully close, and
// removes its temporary store directory when one was auto-created.
func (d *DevServer) Shutdown() {
	if d.srv != nil {
		d.srv.Shutdown()
		d.srv.WaitForShutdown()
	}
	if d.cleanupDir && d.storeDir != "" {
		_ = os.RemoveAll(d.storeDir)
	}
}
