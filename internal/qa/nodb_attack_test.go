// Package qa: attacks that need no database. Kept untagged so the package
// always has at least one buildable file.
package qa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: "synthetic " + code, Severity: "ERROR"}
}

// ---------------------------------------------------------------------------
// ATTACK C1: Classify — the function the headline number is made of.
// Every SQLSTATE that is NOT 55P03 must be incapable of producing a casualty,
// no matter how it is wrapped.
// ---------------------------------------------------------------------------

func TestAttackClassifyCannotBeTrickedIntoACasualty(t *testing.T) {
	notCasualties := []struct {
		name string
		err  error
	}{
		{"57014 query_canceled", pgErr("57014")},
		{"57014 wrapped", fmt.Errorf("exec: %w", pgErr("57014"))},
		{"55006 object_in_use", pgErr("55006")},
		{"53300 too_many_connections", pgErr("53300")},
		{"40P01 deadlock_detected", pgErr("40P01")},
		{"40001 serialization_failure", pgErr("40001")},
		{"42601 syntax_error", pgErr("42601")},
		{"3D000 invalid_catalog_name", pgErr("3D000")},
		{"08006 connection_failure", pgErr("08006")},
		{"57P01 admin_shutdown", pgErr("57P01")},
		{"57P03 cannot_connect_now", pgErr("57P03")},
		{"XX000 internal_error", pgErr("XX000")},
		{"empty sqlstate", pgErr("")},
		{"lowercase lock code", pgErr("55p03")},
		{"prefix of the lock code", pgErr("55P0")},
		{"lock code with trailing space", pgErr("55P03 ")},
		{"plain error", errors.New("boom")},
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"wrapped context.Canceled", fmt.Errorf("query failed: %w", context.Canceled)},
		{"io.EOF", io.EOF},
	}
	for _, tc := range notCasualties {
		t.Run(tc.name, func(t *testing.T) {
			if got := loadgen.Classify(tc.err); got == loadgen.OutcomeDied {
				t.Errorf("SACRED INVARIANT VIOLATED: Classify(%v) = died", tc.err)
			} else {
				t.Logf("Classify = %v", got)
			}
		})
	}

	// And the one code that MUST be a casualty, however it arrives.
	casualties := []struct {
		name string
		err  error
	}{
		{"bare", pgErr("55P03")},
		{"wrapped once", fmt.Errorf("exec: %w", pgErr("55P03"))},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", pgErr("55P03")))},
		{"joined with context.Canceled", errors.Join(context.Canceled, pgErr("55P03"))},
	}
	for _, tc := range casualties {
		t.Run("casualty/"+tc.name, func(t *testing.T) {
			if got := loadgen.Classify(tc.err); got != loadgen.OutcomeDied {
				t.Errorf("FINDING: a real 55P03 lock timeout classified as %v (%v); casualties are being UNDER-counted", got, tc.err)
			}
		})
	}

	if got := loadgen.Classify(nil); got != loadgen.OutcomeOK {
		t.Errorf("Classify(nil) = %v, want ok", got)
	}
}

// ---------------------------------------------------------------------------
// ATTACK C2: a typed-nil *pgconn.PgError. errors.As matches it and the code
// then reads a field off a nil pointer.
// ---------------------------------------------------------------------------

func TestAttackClassifyTypedNilPgError(t *testing.T) {
	var typedNil *pgconn.PgError
	var err error = typedNil

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("FINDING (robustness, LOW): Classify panicked on a typed-nil *pgconn.PgError: %v. "+
				"errors.As matches the type and the switch then dereferences the nil pointer. "+
				"Guard with `if pgErr != nil` after errors.As.", r)
		}
	}()
	got := loadgen.Classify(err)
	t.Logf("Classify(typed-nil *pgconn.PgError) = %v", got)
	if got == loadgen.OutcomeDied {
		t.Error("SACRED INVARIANT VIOLATED: a nil PgError produced a casualty")
	}
}

// ---------------------------------------------------------------------------
// ATTACK C3: Outcome.String must never be ambiguous.
// ---------------------------------------------------------------------------

func TestAttackOutcomeStringIsUnambiguous(t *testing.T) {
	seen := map[string]bool{}
	for _, o := range []loadgen.Outcome{
		loadgen.OutcomeOK, loadgen.OutcomeDied, loadgen.OutcomeStopped, loadgen.OutcomeErrored,
	} {
		s := o.String()
		if seen[s] {
			t.Errorf("two outcomes render as %q", s)
		}
		seen[s] = true
	}
	if s := loadgen.Outcome(99).String(); !strings.Contains(s, "unknown") {
		t.Errorf("Outcome(99).String() = %q, want something containing \"unknown\"", s)
	}
}

// ---------------------------------------------------------------------------
// ATTACK C4: config rejection that needs no server.
// ---------------------------------------------------------------------------

func TestAttackConfigRejectionWithoutServer(t *testing.T) {
	ctx := context.Background()

	t.Run("loadgen empty dsn", func(t *testing.T) {
		if _, err := loadgen.New(ctx, loadgen.Config{DSN: "  ", Logger: quietLogger()}); err == nil {
			t.Error("FINDING: loadgen.New accepted a blank DSN")
		}
	})
	t.Run("loadgen unparseable dsn", func(t *testing.T) {
		if _, err := loadgen.New(ctx, loadgen.Config{DSN: "postgres://[::1", Logger: quietLogger()}); err == nil {
			t.Error("FINDING: loadgen.New accepted an unparseable DSN")
		}
	})
	t.Run("twins empty admin url", func(t *testing.T) {
		if _, err := twins.NewManager(ctx, twins.Config{TemplateName: "t", Logger: quietLogger()}); err == nil {
			t.Error("FINDING: twins.NewManager accepted a blank AdminURL")
		}
	})
	t.Run("twins unparseable admin url", func(t *testing.T) {
		if _, err := twins.NewManager(ctx, twins.Config{
			AdminURL: "postgres://[::1", TemplateName: "t", Logger: quietLogger(),
		}); err == nil {
			t.Error("FINDING: twins.NewManager accepted an unparseable AdminURL")
		}
	})
	t.Run("twins prefix injection", func(t *testing.T) {
		if _, err := twins.NewManager(ctx, twins.Config{
			AdminURL:     "postgres://postgres@127.0.0.1:1/postgres",
			TemplateName: "t",
			NamePrefix:   `x"; DROP DATABASE postgres; --`,
			Logger:       quietLogger(),
		}); err == nil {
			t.Error("FINDING: twins.NewManager accepted a NamePrefix containing SQL punctuation")
		}
	})
	t.Run("twins cancelled context", func(t *testing.T) {
		dead, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := twins.NewManager(dead, twins.Config{
			AdminURL: "postgres://postgres@127.0.0.1:5433/postgres", TemplateName: "t", Logger: quietLogger(),
		}); err == nil {
			t.Error("FINDING: twins.NewManager succeeded with an already-cancelled context")
		}
	})
	t.Run("loadgen default labels are self-consistent", func(t *testing.T) {
		total := 0
		names := map[string]bool{}
		for _, l := range loadgen.DefaultLabels() {
			if l.Weight <= 0 {
				t.Errorf("DefaultLabels contains non-positive weight: %+v", l)
			}
			if names[l.Name] {
				t.Errorf("DefaultLabels contains duplicate name %q", l.Name)
			}
			names[l.Name] = true
			total += l.Weight
			if strings.Count(l.Statement, "$1") != 1 || strings.Count(l.Statement, "$2") != 1 {
				t.Errorf("DefaultLabels statement %q does not use exactly $1 and $2", l.Statement)
			}
		}
		if total <= 0 {
			t.Errorf("DefaultLabels total weight is %d", total)
		}
	})
}
