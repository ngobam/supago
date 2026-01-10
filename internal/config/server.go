package config

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rosfandy/supago/pkg/logger"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var Logger = logger.HcLog().Named("supago.server")

type Server struct {
	Config      *Config
	HttpServer  *fasthttp.Server
	ShutdownFns []func(ctx context.Context) error
}

// --- create new server ---
func NewServer(config *Config) *Server {
	return &Server{
		Config: config,
		HttpServer: &fasthttp.Server{
			MaxRequestBodySize: config.MaxServerRequestBodySize,
			Handler: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.SetBodyString("Hello from Supago server!")
			},
		},
	}
}

// --- prepare listener with graceful shutdown ---
func (s *Server) prepareListener() (net.Listener, error) {
	addr := s.Config.Address()
	ln, err := reuseport.Listen("tcp", addr) // bind both IPv4 and IPv6
	if err != nil {
		Logger.Error("failed to bind address", "err", err)
		return nil, err
	}
	gracefulLn := NewGracefulListener(ln, 5*time.Second)
	return gracefulLn, nil
}

// --- run server in goroutine ---
func (s *Server) runHttpServer(listener net.Listener, errChan chan error) {
	Logger.Info("server is running", "address", s.Config.Address())
	errChan <- s.HttpServer.Serve(listener)
}

// --- main Run method ---
func (s *Server) RunHttpServer() {
	listener, err := s.prepareListener()
	if err != nil {
		os.Exit(1)
	}

	// Error channel
	errChan := make(chan error, 1)

	// Start server in goroutine
	go s.runHttpServer(listener, errChan)

	// Handle OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case err := <-errChan:
			Logger.Error("server stopped with error", "err", err)
			s.gracefulShutdown()
			os.Exit(1)

		case <-sigs:
			Logger.Warn("shutdown signal received")
			s.HttpServer.DisableKeepalive = true
			if err := listener.Close(); err != nil {
				Logger.Error("error closing listener", "err", err)
			}
			s.gracefulShutdown()
			Logger.Info("server gracefully stopped")
			os.Exit(0)
		}
	}
}

// --- call shutdown functions if any ---
func (s *Server) gracefulShutdown() {
	Logger.Info("running shutdown functions")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, fn := range s.ShutdownFns {
		if err := fn(ctx); err != nil {
			Logger.Error("shutdown fn error", "err", err)
		}
	}
}

// --- GracefulListener similar to Raiden ---
type GracefulListener struct {
	ln          net.Listener
	maxWaitTime time.Duration
	done        chan struct{}
	connsCount  uint64
	shutdown    uint64
}

func NewGracefulListener(ln net.Listener, maxWait time.Duration) *GracefulListener {
	return &GracefulListener{
		ln:          ln,
		maxWaitTime: maxWait,
		done:        make(chan struct{}),
	}
}

func (l *GracefulListener) Accept() (net.Conn, error) {
	c, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&l.connsCount, 1)
	return &gracefulConn{Conn: c, ln: l}, nil
}

func (l *GracefulListener) Close() error {
	err := l.ln.Close()
	if err != nil {
		return err
	}
	return l.waitZeroConns()
}

func (l *GracefulListener) Addr() net.Addr {
	return l.ln.Addr()
}

func (l *GracefulListener) waitZeroConns() error {
	atomic.AddUint64(&l.shutdown, 1)
	if atomic.LoadUint64(&l.connsCount) == 0 {
		close(l.done)
		return nil
	}
	select {
	case <-l.done:
		return nil
	case <-time.After(l.maxWaitTime):
		return fmt.Errorf("graceful shutdown timeout after %s", l.maxWaitTime)
	}
}

func (l *GracefulListener) closeConn() {
	conns := atomic.AddUint64(&l.connsCount, ^uint64(0))
	if atomic.LoadUint64(&l.shutdown) != 0 && conns == 0 {
		close(l.done)
	}
}

type gracefulConn struct {
	net.Conn
	ln *GracefulListener
}

func (c *gracefulConn) Close() error {
	err := c.Conn.Close()
	if err != nil {
		return err
	}
	c.ln.closeConn()
	return nil
}
