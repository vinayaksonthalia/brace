package boot

// Bring your own data.
//
// The stock lab seeds a synthetic ten-million-row `orders` table and hammers it
// with three INSERT labels. That is a good demo and a bad rehearsal: the
// migration you are actually afraid of runs against YOUR schema, with YOUR
// indexes, YOUR row widths and YOUR traffic mix. This file is the surface that
// lets a self-hoster point the lab at a restored production dump instead, with
// nothing but environment variables — no fork, no recompile.
//
// Two knobs, and they are independent:
//
//	BRACE_SEED=off        stop seeding; brace_template is whatever you restored
//	BRACE_LOAD_LABELS=…   replace the traffic mix with statements against your
//	                      own tables
//
// Setting only the first gives you real data under synthetic `orders` traffic,
// which will simply error out on every write unless your dump happens to have
// an `orders` table — so in practice the two go together, and the boot
// self-check below is what turns "happens to" into a named error at startup
// rather than a silently flooded `errored` counter on the first run.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// labelSep separates one "name:weight:statement" triple from the next.
	// Statements therefore may not contain a semicolon — which is no loss: the
	// load generator runs exactly one statement per write, and a multi-statement
	// string could not be prepared with parameters anyway.
	labelSep = ";"

	// validateTwinWait bounds how long boot waits for the first warm twin when
	// it has custom labels to check. A BYOD template is a restored production
	// dump and can be large, so the first CREATE DATABASE ... TEMPLATE can take
	// minutes; this is generous on purpose.
	validateTwinWait = 10 * time.Minute

	// validateTwinPoll is how often the wait re-asks the pool. Acquire never
	// blocks and never clones, so polling is the only way to wait for one.
	validateTwinPoll = 2 * time.Second

	// validateStmtTimeout bounds ONE self-check statement. It exists so a typo
	// that happens to be a valid-but-enormous statement cannot hang the boot.
	validateStmtTimeout = 30 * time.Second
)

// ErrTemplateMissing is what boot returns when BRACE_SEED=off and there is no
// template database to run against. It carries the whole BYOD recipe, because
// this is the error a self-hoster hits first and the shell they are standing in
// is where the fix belongs.
var ErrTemplateMissing = errors.New("boot: template database is missing")

// seedDisabled reports whether the environment asked for BYOD mode.
//
// Two spellings are accepted because both are things people actually type:
// BRACE_SEED=off is the explicit switch, and SEED_ROWS=0 is the natural way to
// say "seed nothing" with the variable that was already there. An unrecognised
// BRACE_SEED value is an error rather than a default, because silently seeding
// ten million rows into somebody's restored production dump is the single worst
// thing this package could do.
func seedDisabled(braceSeed, seedRows string) (off bool, err error) {
	switch strings.ToLower(strings.TrimSpace(braceSeed)) {
	case "":
		// Unset: fall through to SEED_ROWS.
	case "off", "0", "no", "false", "none", "skip":
		return true, nil
	case "on", "1", "yes", "true":
		return false, nil
	default:
		return false, fmt.Errorf("boot: BRACE_SEED=%q is not understood; use \"off\" (bring your own data) or \"on\"", braceSeed)
	}

	rows := strings.TrimSpace(seedRows)
	if rows == "" {
		return false, nil
	}
	n, convErr := strconv.ParseInt(rows, 10, 64)
	if convErr != nil {
		// Not a number: leave it to the existing lenient SEED_ROWS handling,
		// which falls back to the default. Only an explicit 0 means "off".
		return false, nil
	}
	return n <= 0, nil
}

// ParseLabels turns BRACE_LOAD_LABELS into a traffic mix.
//
// The format is semicolon-separated `name:weight:statement` triples:
//
//	checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2)
//
// Only the FIRST TWO colons are structural — everything after them is the
// statement, so `SET last_seen = now()::timestamptz` survives intact.
//
// The $1/$2 rule is not a style preference, it is the wire protocol. The load
// generator binds exactly two bigints to every statement it runs (see
// loadgen.LabelSpec), and PostgreSQL rejects a bind that supplies more
// parameters than the parsed statement declares:
//
//	PREPARE p AS INSERT INTO t (id) VALUES ($1); EXECUTE p(1, 2);
//	ERROR:  wrong number of parameters for prepared statement "p"
//	DETAIL:  Expected 1 parameters but got 2.
//
// So a statement that mentions only $1 would fail on EVERY write — verified on
// PostgreSQL 16.14. Requiring both here turns that into one clear boot error
// instead of a hundred thousand silent entries in the `errored` bucket.
func ParseLabels(spec string) ([]loadgen.LabelSpec, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}

	var out []loadgen.LabelSpec
	seen := make(map[string]bool)
	for _, raw := range strings.Split(spec, labelSep) {
		triple := strings.TrimSpace(raw)
		if triple == "" {
			// A trailing or doubled separator. Harmless; a config file that ends
			// its list with a `;` should not fail to boot.
			continue
		}

		parts := strings.SplitN(triple, ":", 3)
		if len(parts) < 3 {
			return nil, badTriple(triple, "expected three colon-separated fields: name:weight:statement")
		}
		name := strings.TrimSpace(parts[0])
		weightText := strings.TrimSpace(parts[1])
		stmt := strings.TrimSpace(parts[2])

		if name == "" {
			return nil, badTriple(triple, "the label name is empty")
		}
		if strings.ContainsAny(name, " \t\r\n") {
			return nil, badTriple(triple, fmt.Sprintf("label name %q contains whitespace; it is used as a JSON key on the dashboard", name))
		}
		if seen[name] {
			return nil, badTriple(triple, fmt.Sprintf("label %q is defined twice; every label needs its own counter", name))
		}
		seen[name] = true

		weight, err := strconv.Atoi(weightText)
		if err != nil {
			return nil, badTriple(triple, fmt.Sprintf("weight %q is not a number", weightText))
		}
		if weight <= 0 {
			return nil, badTriple(triple, fmt.Sprintf("weight %d must be greater than 0", weight))
		}

		if stmt == "" {
			return nil, badTriple(triple, "the statement is empty")
		}
		if err := checkPlaceholders(stmt); err != nil {
			return nil, badTriple(triple, err.Error())
		}

		out = append(out, loadgen.LabelSpec{Name: name, Weight: weight, Statement: stmt})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("boot: BRACE_LOAD_LABELS is set but contains no labels")
	}
	return out, nil
}

// checkPlaceholders enforces the two-parameter contract.
func checkPlaceholders(stmt string) error {
	if !strings.Contains(stmt, "$1") {
		return errors.New("the statement does not use $1; every label statement is run with two bigint " +
			"parameters bound ($1 customer_id, $2 amount/value)")
	}
	if !strings.Contains(stmt, "$2") {
		return errors.New("the statement uses $1 but not $2; PostgreSQL rejects a bind that supplies more " +
			"parameters than the statement declares, so this would fail on every write. Give $2 a home — " +
			"`VALUES ($1, $2)`, or a harmless `AND $2 > 0` if the second value is genuinely unused")
	}
	// $3 and beyond can never be bound, so they are a guaranteed runtime error.
	for n := '3'; n <= '9'; n++ {
		if strings.Contains(stmt, "$"+string(n)) {
			return fmt.Errorf("the statement uses $%c; only $1 and $2 are ever bound", n)
		}
	}
	return nil
}

func badTriple(triple, why string) error {
	return fmt.Errorf("boot: BRACE_LOAD_LABELS: bad entry %q: %s", triple, why)
}

// verifyTemplate is ensureTemplate's counterpart for BYOD mode: it asserts that
// the template a human restored is actually there and actually has tables,
// without writing a single row into it.
//
// A missing template is fatal with the full recipe. An empty one is fatal too —
// a template with no user tables would clone fine and then fail every write,
// which is a far more confusing failure an hour later.
func verifyTemplate(ctx context.Context, admin *pgxpool.Pool, cfg Config, log *slog.Logger) (TemplateStats, error) {
	var stats TemplateStats

	var exists bool
	err := admin.QueryRow(ctx, `SELECT true FROM pg_database WHERE datname = $1`, TemplateName).Scan(&exists)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return stats, fmt.Errorf("boot: look up template: %w", err)
		}
		return stats, fmt.Errorf("%w: BRACE_SEED=off means Brace will not create %[2]q for you, and it does not exist.\n"+
			"Restore your dump into it, then redeploy:\n"+
			"    createdb -h <host> -U <superuser> %[2]s\n"+
			"    pg_restore -h <host> -U <superuser> -d %[2]s your-dump.dump\n"+
			"(Brace pins the template to ALLOW_CONNECTIONS false itself on every start — you do not have to.)",
			ErrTemplateMissing, TemplateName)
	}

	// The twin manager keeps the template at ALLOW_CONNECTIONS false; open it
	// just long enough to look inside, exactly as the seeding path does.
	if _, err := admin.Exec(ctx,
		`ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS true`); err != nil {
		return stats, fmt.Errorf("boot: unlock template: %w", err)
	}
	defer func() {
		lockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(lockCtx,
			`ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS false`); err != nil {
			log.Error("boot.template_lock_failed", "err", err)
		}
	}()

	connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	conn, err := pgx.Connect(connCtx, dsnForDB(cfg.AdminURL, TemplateName))
	cancel()
	if err != nil {
		return stats, fmt.Errorf("boot: connect template %q: %w", TemplateName, err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	// Ordinary and partitioned tables in any non-system schema. reltuples is the
	// planner's estimate rather than a count(*): on a restored production dump a
	// real count is minutes of I/O for a number that is only ever displayed.
	var tables int64
	var rows float64
	if err := conn.QueryRow(ctx, `
		SELECT count(*), coalesce(sum(GREATEST(c.reltuples, 0)), 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p')
		  AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname NOT LIKE 'pg_toast%'`).Scan(&tables, &rows); err != nil {
		return stats, fmt.Errorf("boot: inspect template %q: %w", TemplateName, err)
	}
	if tables == 0 {
		return stats, fmt.Errorf("boot: template %q exists but has no user tables. "+
			"BRACE_SEED=off expects a restored dump there; did pg_restore run against a different database?",
			TemplateName)
	}
	stats.Rows = int64(rows)

	if err := admin.QueryRow(ctx,
		`SELECT pg_size_pretty(pg_database_size($1))`, TemplateName).Scan(&stats.Size); err != nil {
		log.Warn("boot.template_size_unavailable", "err", err)
	}
	log.Info("byod.template_verified", "template", TemplateName,
		"tables", tables, "estimated_rows", stats.Rows, "size", stats.Size)
	return stats, nil
}

// validateLabels runs every custom statement ONCE against a real, freshly cloned
// twin, then throws that twin away.
//
// This is the difference between a typo costing five seconds at boot and a typo
// costing a whole rehearsal: without it, `INSERT INTO invoces (...)` produces a
// perfectly healthy-looking run whose casualty count is zero and whose `errored`
// count is a quarter of a million, and the user has to know to look at the
// second number to find out the first one was meaningless.
//
// The twin is used and discarded rather than rolled back. It is a disposable
// clone that would have been destroyed anyway, and an INSERT that only succeeds
// inside an aborted transaction is not evidence that it will succeed in a run.
// It also runs for the STOCK labels when seeding is off, because that
// configuration has the same failure: the built-in mix writes to `orders`, and
// a restored dump that has no such table would produce exactly the same
// zero-casualty, all-errored run. Whether the mismatch came from the user's
// statements or from ours, the honest place to find out is boot.
func validateLabels(ctx context.Context, mgr *twins.Manager, labels []loadgen.LabelSpec, custom bool, log *slog.Logger) error {
	if len(labels) == 0 {
		return nil
	}
	// The advice attached to a failure differs: a custom statement is wrong, a
	// stock statement means the variable was never set.
	hint := "the stock traffic mix writes to `orders`, which this template does not appear to have. " +
		"Set BRACE_LOAD_LABELS to statements against your own tables — see docs/BYOD.md"
	source := "the default traffic mix"
	if custom {
		hint = "check the table and column names against the restored template"
		source = "BRACE_LOAD_LABELS"
	}

	twin, err := waitForTwin(ctx, mgr, log)
	if err != nil {
		return err
	}
	defer func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := mgr.Release(relCtx, twin); err != nil && !errors.Is(err, twins.ErrClosed) {
			log.Error("byod.validation_twin_release_failed", "twin", twin.Name, "err", err)
		}
	}()

	connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	conn, err := pgx.Connect(connCtx, twin.DSN)
	cancel()
	if err != nil {
		return fmt.Errorf("boot: connect validation twin %q: %w", twin.Name, err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	for _, l := range labels {
		stmtCtx, cancel := context.WithTimeout(ctx, validateStmtTimeout)
		// The same two bigints the load generator binds, with unremarkable
		// values: this is a smoke test of the statement, not of the data.
		_, err := conn.Exec(stmtCtx, l.Statement, int64(1), int64(1))
		cancel()
		if err != nil {
			return fmt.Errorf("boot: %s: label %q does not run against this template: %w\n  statement: %s\n  %s",
				source, l.Name, err, l.Statement, hint)
		}
		log.Info("byod.label_ok", "label", l.Name, "weight", l.Weight)
	}
	log.Info("byod.labels_validated", "count", len(labels), "twin", twin.Name)
	return nil
}

// waitForTwin polls Acquire until the background worker has produced one.
// Acquire deliberately never clones and never blocks, so waiting is the
// caller's job; boot is the one caller that genuinely has to wait.
func waitForTwin(ctx context.Context, mgr *twins.Manager, log *slog.Logger) (*twins.Twin, error) {
	deadline := time.Now().Add(validateTwinWait)
	for attempt := 1; ; attempt++ {
		twin, err := mgr.Acquire(ctx)
		if err == nil {
			return twin, nil
		}
		if !errors.Is(err, twins.ErrNoWarmTwin) {
			return nil, fmt.Errorf("boot: acquire validation twin: %w", err)
		}
		if time.Now().After(deadline) {
			status := mgr.Status()
			return nil, fmt.Errorf("boot: no twin became available within %s to validate BRACE_LOAD_LABELS "+
				"(pool warm=%d warming=%d last_error=%q)",
				validateTwinWait, status.Warm, status.Warming, status.LastError)
		}
		if attempt%15 == 0 {
			log.Info("byod.waiting_for_validation_twin", "waited_s", int(time.Until(deadline).Seconds()))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(validateTwinPoll):
		}
	}
}
