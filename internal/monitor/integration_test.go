//go:build integration

// Integration tests for the lock monitor. They need a real PostgreSQL 16: the
// whole point of this package is what pg_stat_activity and pg_locks say while a
// migration holds a lock, and no fake can testify to that.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration ./internal/monitor/
package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const itPrefix = "brace_mon_test_"

var (
	adminURL  string
	dbSkipMsg string
)

func TestMain(m *testing.M) {
	adminURL = os.Getenv("TEST_ADMIN_URL")
	if adminURL == "" {
		adminURL = "postgres://postgres@localhost:5433/postgres"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := waitForPostgres(ctx, adminURL, 20*time.Second)
	cancel()
	if err != nil {
		dbSkipMsg = fmt.Sprintf("PostgreSQL at TEST_ADMIN_URL=%s is unreachable: %v", adminURL, err)
		os.Exit(m.Run())
	}
	bg := context.Background()
	_ = dropScratchDatabases(bg, conn)
	conn.Close(bg)

	code := m.Run()

	if c, err := pgx.Connect(bg, adminURL); err == nil {
		_ = dropScratchDatabases(bg, c)
		c.Close(bg)
	}
	os.Exit(code)
}

func waitForPostgres(ctx context.Context, dsn string, limit time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(limit)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := pgx.Connect(attemptCtx, dsn)
		if err == nil {
			if err = conn.Ping(attemptCtx); err == nil {
				cancel()
				return conn, nil
			}
			conn.Close(attemptCtx)
		}
		cancel()
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, lastErr
		}
		time.Sleep(time.Second)
	}
}

func dropScratchDatabases(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE starts_with(datname, $1)`, itPrefix)
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		names = append(names, n)
	}
	rows.Close()
	for _, n := range names {
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS "`+n+`" WITH (FORCE)`); err != nil {
			return err
		}
	}
	return nil
}

func newScratchDB(t *testing.T) string {
	t.Helper()
	if dbSkipMsg != "" {
		t.Skip(dbSkipMsg)
	}
	name := fmt.Sprintf("%s%d", itPrefix, time.Now().UnixNano())
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatalf("create scratch db: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
	})

	u, _ := url.Parse(adminURL)
	u.Path = "/" + name
	dsn := u.String()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect scratch: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE orders (
		id bigserial primary key,
		customer_id bigint not null,
		amount_cents bigint not null)`); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	return dsn
}

// TestObservesBlockedWriters is the corroboration test: a session posing as the
// migration takes a SHARE lock (exactly what a naive CREATE INDEX takes), real
// writers queue behind it, and the monitor must see both the waiters and the
// lock mode.
func TestObservesBlockedWriters(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	m := New(Config{DSN: dsn, Interval: 100 * time.Millisecond, Logger: quietLogger()})
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The "migration": application_name is what the monitor keys on.
	migCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	migCfg.RuntimeParams["application_name"] = DefaultAppName
	mig, err := pgx.ConnectConfig(ctx, migCfg)
	if err != nil {
		t.Fatalf("connect migration: %v", err)
	}
	defer mig.Close(ctx)
	tx, err := mig.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE orders IN SHARE MODE`); err != nil {
		t.Fatalf("LOCK TABLE: %v", err)
	}

	// Three writers that will park on the lock.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(context.Background(), dsn)
			if err != nil {
				return
			}
			defer c.Close(context.Background())
			_, _ = c.Exec(context.Background(),
				`INSERT INTO orders (customer_id, amount_cents) VALUES (1, 1)`)
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("rollback: %v", err)
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)

	rep := m.StopAndReport()
	t.Logf("report: %+v", rep)

	if rep.Samples == 0 {
		t.Fatalf("monitor took no successful samples: %+v", rep)
	}
	if rep.BlockedPeakCount < 1 {
		t.Errorf("BlockedPeakCount = %d, want >= 1 (three writers were parked on a SHARE lock)", rep.BlockedPeakCount)
	}
	if rep.BlockedSeconds < 0.5 {
		t.Errorf("BlockedSeconds = %v, want >= 0.5 for a ~1.5s hold", rep.BlockedSeconds)
	}
	if rep.BlockedSeconds > 2.5 {
		t.Errorf("BlockedSeconds = %v, implausibly high for a ~1.5s hold", rep.BlockedSeconds)
	}
	if rep.StrongestLockMode != "ShareLock" {
		t.Errorf("StrongestLockMode = %q, want ShareLock; modes seen: %v", rep.StrongestLockMode, rep.LockModes)
	}
	if rep.Degraded() {
		t.Errorf("healthy run reported %d failed samples: %s", rep.FailedSamples, rep.LastError)
	}
}

// TestQuietDatabaseReportsNothing: no migration, no waiters, so the monitor
// must report a confident zero rather than noise.
func TestQuietDatabaseReportsNothing(t *testing.T) {
	dsn := newScratchDB(t)
	m := New(Config{DSN: dsn, Interval: 100 * time.Millisecond, Logger: quietLogger()})
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(700 * time.Millisecond)
	rep := m.StopAndReport()

	if rep.Samples < 3 {
		t.Fatalf("expected several samples, got %+v", rep)
	}
	if rep.BlockedSeconds != 0 || rep.BlockedPeakCount != 0 {
		t.Errorf("idle database reported blocking: %+v", rep)
	}
	if len(rep.LockModes) != 0 {
		t.Errorf("idle database reported lock modes: %v", rep.LockModes)
	}
	if rep.Degraded() {
		t.Errorf("idle run degraded: %s", rep.LastError)
	}
}

// TestSurvivesDatabaseDisappearing: the twin can be dropped out from under the
// monitor. It must count failures and still return a report — never block the
// orchestrator's teardown.
func TestSurvivesDatabaseDisappearing(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	m := New(Config{DSN: dsn, Interval: 100 * time.Millisecond, Logger: quietLogger()})
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(400 * time.Millisecond)

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	u, _ := url.Parse(dsn)
	dbName := u.Path[1:]
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+dbName+`" WITH (FORCE)`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	admin.Close(ctx)

	time.Sleep(600 * time.Millisecond)

	done := make(chan Report, 1)
	go func() { done <- m.StopAndReport() }()
	select {
	case rep := <-done:
		t.Logf("degraded report: %+v", rep)
		if !rep.Degraded() {
			t.Error("twin vanished but the monitor reported no failed samples")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StopAndReport blocked after the twin vanished")
	}
}
