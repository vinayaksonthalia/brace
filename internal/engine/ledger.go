package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The run ledger.
//
// It lives in the APPLICATION database — the one the admin DSN points at — and
// never in a twin. Twins are destroyed at the end of every run, so a ledger
// written into one would be destroyed with it; that is the entire reason this
// is a separate concern rather than a table on the twin.
//
// The ledger is deliberately append-only and deliberately not on the critical
// path: a run whose ledger insert fails is still a completed run with a real
// verdict, and losing the history entry is a smaller harm than throwing away
// the measurement the user just waited for.

// ledgerSchema is idempotent and runs at startup. IF NOT EXISTS everywhere
// rather than a migration tool: this is one table, and Brace of all products
// should not need a migration framework to install it.
const ledgerSchema = `
CREATE TABLE IF NOT EXISTS brace_runs (
	id                  bigserial PRIMARY KEY,
	created_at          timestamptz NOT NULL DEFAULT now(),
	"sql"               text        NOT NULL,
	mode                text        NOT NULL,
	classification      text        NOT NULL DEFAULT '',
	verdict             text        NOT NULL,
	blocked_seconds     double precision NOT NULL,
	duration_seconds    double precision NOT NULL,
	casualties_total    bigint      NOT NULL,
	casualties_by_label jsonb       NOT NULL DEFAULT '{}'::jsonb,
	baseline_wps        bigint      NOT NULL,
	lock_modes          text[]      NOT NULL DEFAULT '{}',
	twin                text        NOT NULL DEFAULT '',
	error               text
);
CREATE INDEX IF NOT EXISTS brace_runs_created_at_idx ON brace_runs (created_at DESC);
-- casualties_total is meaningless without the deadline that defines a casualty,
-- so history rows carry it too. ADD COLUMN IF NOT EXISTS with a constant default
-- is catalog-only on PG11+ — the one ALTER TABLE this product may run on itself.
ALTER TABLE brace_runs ADD COLUMN IF NOT EXISTS casualty_deadline_ms bigint NOT NULL DEFAULT 0;
`

// RunRecord is one row of history, as the API serves it.
type RunRecord struct {
	ID                int64            `json:"id"`
	CreatedAt         time.Time        `json:"created_at"`
	SQL               string           `json:"sql"`
	Mode              Mode             `json:"mode"`
	Classification    Classification   `json:"classification"`
	Verdict           string           `json:"verdict"`
	BlockedSeconds    float64          `json:"blocked_seconds"`
	DurationSeconds   float64          `json:"duration_seconds"`
	CasualtiesTotal   int64            `json:"casualties_total"`
	CasualtiesByLabel map[string]int64 `json:"casualties_by_label"`
	// CasualtyDeadlineMS is the simulated request deadline those casualties were
	// counted against. Rows written before this column existed report 0, which
	// means "unrecorded", not "no deadline".
	CasualtyDeadlineMS int64    `json:"casualty_deadline_ms"`
	BaselineWPS        int64    `json:"baseline_writes_per_sec"`
	LockModes          []string `json:"lock_modes"`
	Twin               string   `json:"twin"`
	Error              string   `json:"error,omitempty"`
}

// Ledger is the run history store.
type Ledger struct {
	pool *pgxpool.Pool
}

// NewLedger wraps an admin pool. The pool must point at the application
// database, not at a twin.
func NewLedger(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// CreateSchema installs the table. Idempotent; called at startup.
func (l *Ledger) CreateSchema(ctx context.Context) error {
	if _, err := l.pool.Exec(ctx, ledgerSchema); err != nil {
		return fmt.Errorf("engine: create ledger schema: %w", err)
	}
	return nil
}

// Insert records a finished run and returns its id.
func (l *Ledger) Insert(ctx context.Context, r *RunResult) (int64, error) {
	byLabel, err := json.Marshal(r.Casualties.ByLabel)
	if err != nil {
		return 0, fmt.Errorf("engine: encode casualties: %w", err)
	}
	if r.Casualties.ByLabel == nil {
		byLabel = []byte(`{}`)
	}
	modes := r.LockModes
	if modes == nil {
		modes = []string{}
	}

	var id int64
	err = l.pool.QueryRow(ctx, `
		INSERT INTO brace_runs
			("sql", mode, classification, verdict, blocked_seconds, duration_seconds,
			 casualties_total, casualties_by_label, baseline_wps, lock_modes, twin, error,
			 casualty_deadline_ms)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13)
		RETURNING id`,
		r.SQL, string(r.Mode), string(r.Classification), r.Verdict,
		r.BlockedSeconds, r.DurationSeconds,
		r.Casualties.Total, string(byLabel), r.BaselineWPS, modes, r.Twin,
		nullIfEmpty(r.Error), r.Casualties.DeadlineMS,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("engine: insert run: %w", err)
	}
	return id, nil
}

// List returns the most recent runs, newest first.
func (l *Ledger) List(ctx context.Context, limit int) ([]RunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	rows, err := l.pool.Query(ctx, `
		SELECT id, created_at, "sql", mode, classification, verdict,
		       blocked_seconds, duration_seconds, casualties_total,
		       casualties_by_label, baseline_wps, lock_modes, twin, coalesce(error,''),
		       casualty_deadline_ms
		FROM brace_runs
		ORDER BY created_at DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("engine: list runs: %w", err)
	}
	defer rows.Close()

	out := make([]RunRecord, 0, limit)
	for rows.Next() {
		var (
			rec     RunRecord
			mode    string
			class   string
			byLabel []byte
		)
		if err := rows.Scan(&rec.ID, &rec.CreatedAt, &rec.SQL, &mode, &class, &rec.Verdict,
			&rec.BlockedSeconds, &rec.DurationSeconds, &rec.CasualtiesTotal,
			&byLabel, &rec.BaselineWPS, &rec.LockModes, &rec.Twin, &rec.Error,
			&rec.CasualtyDeadlineMS); err != nil {
			return nil, fmt.Errorf("engine: scan run: %w", err)
		}
		rec.Mode = Mode(mode)
		rec.Classification = Classification(class)
		if len(byLabel) > 0 {
			if err := json.Unmarshal(byLabel, &rec.CasualtiesByLabel); err != nil {
				// A row we cannot decode is still a row worth listing; the
				// history view must not 500 because one JSON blob went bad.
				rec.CasualtiesByLabel = nil
			}
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engine: list runs: %w", err)
	}
	return out, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
