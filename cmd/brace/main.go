// Command brace runs the crash-test lab: an HTTP API in front of a pool of
// disposable PostgreSQL twins.
//
// The listener starts before the database work does. That is not an
// optimisation — seeding the ten-million-row template takes minutes, every
// platform health-checks long before that, and a process that waits for its
// database to be ready before it will answer /healthz is a process that never
// becomes ready.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vinayaksonthalia/brace/internal/api"
	"github.com/vinayaksonthalia/brace/internal/boot"
)

const (
	// shutdownGrace bounds the HTTP drain, teardownGrace the twin cleanup that
	// follows it. They are separate budgets on purpose: see the shutdown block.
	shutdownGrace = 10 * time.Second
	teardownGrace = 45 * time.Second
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := boot.FromEnv()
	if err != nil {
		log.Error("brace.config", "err", err)
		os.Exit(1)
	}
	cfg.Logger = log

	srv := api.New(api.Config{Logger: log, WebDir: cfg.WebDir})
	srv.SetPhase("starting")

	httpServer := &http.Server{
		Addr:    net.JoinHostPort("", cfg.Port),
		Handler: srv.Handler(),
		// No WriteTimeout: /api/stream is an open-ended response, and a write
		// deadline would cut every dashboard off at the same interval.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	listener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		log.Error("brace.listen", "addr", httpServer.Addr, "err", err)
		os.Exit(1)
	}
	log.Info("brace.listening", "addr", httpServer.Addr)

	serveErr := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	labCh := make(chan *boot.Lab, 1)
	go func() {
		defer close(labCh)
		lab, err := boot.Open(ctx, cfg, srv.SetPhase)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("brace.boot_failed", "err", err)
			srv.SetPhase("failed: " + err.Error())
			return
		}
		srv.Ready(lab.Engine, lab.Pool, api.TemplateInfo{
			Rows: lab.Template.Rows,
			Size: lab.Template.Size,
		})
		log.Info("brace: lab open", "template_rows", lab.Template.Rows,
			"template_size", lab.Template.Size, "pool_size", cfg.PoolSize)
		labCh <- lab
	}()

	select {
	case <-ctx.Done():
		log.Info("brace.shutdown_signal")
	case err := <-serveErr:
		if err != nil {
			log.Error("brace.serve", "err", err)
		}
	}

	// Streams first. An SSE response never completes on its own, so
	// http.Shutdown would wait out its entire grace period on every connected
	// dashboard and then have no time left to drop the twins. srv.Close ends
	// the streams and abandons any in-flight rehearsal; only then is
	// "wait for requests to drain" a question with an answer.
	srv.Close()

	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := httpServer.Shutdown(shutCtx); err != nil {
		log.Warn("brace.http_shutdown", "err", err)
		_ = httpServer.Close()
	}
	cancel()

	// Teardown gets its own budget. Dropping the warm twins is the one part of
	// shutdown that leaves something behind if it is skipped — a database on
	// disk until the next boot's orphan sweep — so it must never inherit a
	// context the HTTP drain has already spent.
	labCtx, labCancel := context.WithTimeout(context.Background(), teardownGrace)
	defer labCancel()
	select {
	case lab := <-labCh:
		lab.Close(labCtx)
	default:
		// Boot never finished. Whatever it half-built is torn down when the
		// process exits; there is nothing here to close.
	}
	log.Info("brace.stopped")
}
