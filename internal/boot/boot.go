// Package boot turns environment variables and an empty PostgreSQL server into
// a running lab.
//
// The ordering here is the whole point. Seeding a ten-million-row template
// takes minutes, and a platform that health-checks a container before its
// database is ready will kill it every single time — so the HTTP listener comes
// up FIRST, /healthz answers immediately, /api/* says "starting, phase: seeding"
// with a phase the dashboard can show, and only then does the slow work begin
// in the background. Nothing in this package blocks the listener.
package boot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinayaksonthalia/brace/internal/engine"
	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// TemplateName is the database every twin is cloned from.
	TemplateName = "brace_template"

	// seedBatch is how many rows one INSERT … generate_series adds. A million
	// rows is roughly a second of work and a bounded amount of WAL, which keeps
	// progress visible in the log and keeps a single statement from being the
	// thing that runs out of disk.
	seedBatch = 1_000_000

	// seedTolerance: a template within 10% of target is close enough to reuse.
	// Restarting the service must not mean re-seeding ten million rows, and the
	// exact count is uninteresting — what matters is that the table is big
	// enough that a naive CREATE INDEX takes real time.
	seedTolerance = 0.9

	connectTimeout = 30 * time.Second
)

// Config is the resolved environment.
type Config struct {
	// AdminURL is a superuser DSN pointing at the `postgres` maintenance
	// database. Everything — the ledger, the template, the twins — hangs off it.
	AdminURL string
	Port     string
	PoolSize int
	Workers  int
	SeedRows int64
	WebDir   string
	Logger   *slog.Logger

	// TargetRows is BRACE_TARGET_ROWS: the operator's statement of how many rows
	// their REAL table has. The twin is a sample, and a freeze measured on a
	// sample is not the freeze production gets — this is the number that lets
	// Brace say "0.2s here means ~10s on yours" instead of shipping a false SAFE.
	// 0 (the default) means unstated, and no projection is made.
	TargetRows int64

	// Seed is false in bring-your-own-data mode (BRACE_SEED=off, or
	// SEED_ROWS=0). The template is then whatever a human restored into
	// brace_template, and boot verifies it instead of writing to it.
	Seed bool
	// Labels is the traffic mix from BRACE_LOAD_LABELS. Nil means the stock
	// orders-table mix — see loadgen.DefaultLabels.
	Labels []loadgen.LabelSpec
}

// BYOD reports whether this configuration points the lab at a user-supplied
// dataset in any way — either by not seeding, or by running custom traffic.
func (c Config) BYOD() bool { return !c.Seed || len(c.Labels) > 0 }

// FromEnv resolves configuration from the environment, using Zerops' own
// variable names for the database so nothing has to be wired by hand: a
// `db_` reference in zerops.yml is enough.
func FromEnv() (Config, error) {
	c := Config{
		Port:     envOr("PORT", "8080"),
		PoolSize: envInt("POOL_SIZE", 2),
		Workers:  envInt("WORKERS", 24),
		SeedRows: int64(envInt("SEED_ROWS", 10_000_000)),
		WebDir:   envOr("BRACE_WEB_DIR", ""),
		Seed:     true,
	}

	// BRACE_TARGET_ROWS is optional and deliberately forgiving: a garbled value
	// costs the deployment its scale projection, not its startup. It is logged
	// as unset either way by the caller.
	c.TargetRows = int64(envInt("BRACE_TARGET_ROWS", 0))

	// Bring-your-own-data. Both knobs are resolved before anything touches the
	// database, so a malformed value is a startup error and never a half-seeded
	// template.
	off, err := seedDisabled(os.Getenv("BRACE_SEED"), os.Getenv("SEED_ROWS"))
	if err != nil {
		return c, err
	}
	c.Seed = !off

	labels, err := ParseLabels(os.Getenv("BRACE_LOAD_LABELS"))
	if err != nil {
		return c, err
	}
	c.Labels = labels

	if u := strings.TrimSpace(os.Getenv("BRACE_ADMIN_URL")); u != "" {
		c.AdminURL = u
	} else {
		user := firstNonEmpty(os.Getenv("superUser"), os.Getenv("db_superUser"))
		pass := firstNonEmpty(os.Getenv("superUserPassword"), os.Getenv("db_superUserPassword"))
		host := firstNonEmpty(os.Getenv("db_hostname"), os.Getenv("hostname"), "db")
		if user == "" {
			return c, errors.New("boot: set BRACE_ADMIN_URL, or superUser/superUserPassword for the db service")
		}
		c.AdminURL = fmt.Sprintf("postgres://%s:%s@%s:5432/postgres",
			url.QueryEscape(user), url.QueryEscape(pass), host)
	}

	// The floor only applies when we are the ones writing the rows. In BYOD mode
	// SeedRows is meaningless and clamping it would put a misleading number in
	// the log line.
	if c.Seed && c.SeedRows < 1000 {
		c.SeedRows = 1000
	}
	if !c.Seed {
		c.SeedRows = 0
	}
	return c, nil
}

// Lab is everything the API layer needs, plus the handles the process must
// close on the way out.
type Lab struct {
	Engine   *engine.Engine
	Twins    *twins.Manager
	Pool     *pgxpool.Pool
	Template TemplateStats
}

// TemplateStats is the cached description of the seed template.
type TemplateStats struct {
	Rows int64
	Size string
}

// Close tears the lab down in dependency order: engine first (it holds the
// ledger pool), then the twin pool (which drops every warm database), then the
// admin pool this package owns.
func (l *Lab) Close(ctx context.Context) {
	if l == nil {
		return
	}
	if l.Engine != nil {
		_ = l.Engine.Close()
	}
	if l.Twins != nil {
		_ = l.Twins.Close(ctx)
	}
	if l.Pool != nil {
		l.Pool.Close()
	}
}

// Phase reports progress to whoever is watching the boot — in practice the API
// server, which surfaces it on /api/* while the lab is still assembling.
type Phase func(string)

// Open runs the slow half of startup: wait for PostgreSQL, seed the template if
// it is missing or short, start the twin pool, build the engine.
func Open(ctx context.Context, cfg Config, phase Phase) (*Lab, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if phase == nil {
		phase = func(string) {}
	}

	phase("connecting")
	pool, err := waitForPool(ctx, cfg.AdminURL, log)
	if err != nil {
		return nil, err
	}
	lab := &Lab{Pool: pool}

	// Bring-your-own-data splits here. Seeding WRITES to the template; verifying
	// only reads it. Getting this wrong in the "off" direction would mean
	// appending ten million synthetic orders to somebody's restored production
	// dump, so the branch is on the config value and nothing else.
	var stats TemplateStats
	if cfg.Seed {
		phase("seeding")
		stats, err = ensureTemplate(ctx, pool, cfg, log)
	} else {
		phase("verifying_template")
		log.Info("byod.seed_disabled", "template", TemplateName,
			"note", "BRACE_SEED=off — using the restored template as-is")
		stats, err = verifyTemplate(ctx, pool, cfg, log)
	}
	if err != nil {
		lab.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	lab.Template = stats

	phase("warming")
	mgr, err := twins.NewManager(ctx, twins.Config{
		AdminURL:     cfg.AdminURL,
		TemplateName: TemplateName,
		PoolSize:     cfg.PoolSize,
		Logger:       log,
	})
	if err != nil {
		lab.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("boot: twin pool: %w", err)
	}
	lab.Twins = mgr

	// The self-check runs BEFORE the engine is published, on a twin nobody else
	// can be holding, so a bad statement is a boot failure rather than a
	// rehearsal whose casualty count is quietly meaningless.
	//
	// It runs whenever the data is not ours: custom labels obviously, but also
	// custom TEMPLATE with the stock mix, which writes to an `orders` table a
	// restored dump probably does not have.
	if cfg.BYOD() {
		phase("validating_labels")
		labels, custom := cfg.Labels, true
		if len(labels) == 0 {
			labels, custom = loadgen.DefaultLabels(), false
		}
		if err := validateLabels(ctx, mgr, labels, custom, log); err != nil {
			lab.Close(context.WithoutCancel(ctx))
			return nil, err
		}
	}

	// TwinRows is the template's ACTUAL row count as measured a moment ago, not
	// cfg.SeedRows: a reused template within the 10% tolerance, or a restored
	// BYOD dump, is whatever size it is, and the projection's denominator has to
	// be the truth rather than the intent.
	if cfg.TargetRows > 0 && stats.Rows > 0 {
		log.Info("boot.scale_projection", "twin_rows", stats.Rows, "target_rows", cfg.TargetRows,
			"ratio", float64(cfg.TargetRows)/float64(stats.Rows))
	} else if cfg.TargetRows == 0 {
		log.Info("boot.scale_projection_disabled", "twin_rows", stats.Rows,
			"note", "set BRACE_TARGET_ROWS to your production table's row count to have runs projected onto it")
	}
	eng, err := engine.New(ctx, engine.Config{
		AdminURL:          cfg.AdminURL,
		Twins:             mgr,
		Workers:           cfg.Workers,
		Labels:            cfg.Labels,
		TwinRows:          stats.Rows,
		DefaultTargetRows: cfg.TargetRows,
		Logger:            log,
	})
	if err != nil {
		lab.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("boot: engine: %w", err)
	}
	lab.Engine = eng
	return lab, nil
}

// waitForPool retries the first connection. On a fresh Zerops project the API
// container is frequently up before PostgreSQL has finished its own init, and
// crash-looping through that is noise rather than information.
func waitForPool(ctx context.Context, dsn string, log *slog.Logger) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("boot: parse admin url: %w", err)
	}
	poolCfg.MaxConns = 4

	var lastErr error
	for attempt := 1; ; attempt++ {
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, fmt.Errorf("boot: connect admin database: %w", lastErr)
		}
		log.Warn("boot.waiting_for_postgres", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("boot: connect admin database: %w", lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

// ensureTemplate makes brace_template exist and be big enough. It is idempotent
// by design: this runs on every boot, and the common case — a template that is
// already seeded — must cost one count query and nothing else.
func ensureTemplate(ctx context.Context, admin *pgxpool.Pool, cfg Config, log *slog.Logger) (TemplateStats, error) {
	var stats TemplateStats

	var exists bool
	if err := admin.QueryRow(ctx,
		`SELECT true FROM pg_database WHERE datname = $1`, TemplateName).Scan(&exists); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return stats, fmt.Errorf("boot: look up template: %w", err)
		}
		if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdent(TemplateName)); err != nil {
			return stats, fmt.Errorf("boot: create template: %w", err)
		}
		log.Info("boot.template_created", "template", TemplateName)
	}

	// twins.Manager pins the template to ALLOW_CONNECTIONS false, which is
	// exactly what we cannot have while inspecting or seeding it. Reopen it for
	// the duration and close it again before the pool starts cloning.
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
		return stats, fmt.Errorf("boot: connect template: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	rows, err := seedTemplate(ctx, conn, cfg.SeedRows, log)
	if err != nil {
		return stats, err
	}
	stats.Rows = rows

	if err := admin.QueryRow(ctx,
		`SELECT pg_size_pretty(pg_database_size($1))`, TemplateName).Scan(&stats.Size); err != nil {
		log.Warn("boot.template_size_unavailable", "err", err)
	}
	return stats, nil
}

// ordersSchema is deliberately unindexed beyond the primary key. Every index on
// this table would be an index the user's CREATE INDEX no longer has to build —
// the whole rig depends on the naive statement having real work to do.
const ordersSchema = `
CREATE TABLE IF NOT EXISTS orders (
	id           bigserial   PRIMARY KEY,
	customer_id  bigint      NOT NULL,
	amount_cents bigint      NOT NULL,
	status       text        NOT NULL DEFAULT 'pending',
	created_at   timestamptz NOT NULL DEFAULT now()
)`

func seedTemplate(ctx context.Context, conn *pgx.Conn, target int64, log *slog.Logger) (int64, error) {
	if _, err := conn.Exec(ctx, ordersSchema); err != nil {
		return 0, fmt.Errorf("boot: create orders: %w", err)
	}

	var have int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&have); err != nil {
		return 0, fmt.Errorf("boot: count orders: %w", err)
	}
	if float64(have) >= float64(target)*seedTolerance {
		log.Info("boot.template_ready", "rows", have, "target", target)
		return have, nil
	}

	log.Info("boot.seeding", "have", have, "target", target, "batch", seedBatch)
	start := time.Now()
	for have < target {
		n := target - have
		if n > seedBatch {
			n = seedBatch
		}
		batchStart := time.Now()
		if _, err := conn.Exec(ctx, `
			INSERT INTO orders (customer_id, amount_cents, status, created_at)
			SELECT (random()*100000)::bigint + 1,
			       (random()*50000)::bigint + 1,
			       (ARRAY['pending','paid','shipped','refunded'])[1 + floor(random()*4)::int],
			       now() - (random() * interval '90 days')
			FROM generate_series(1, $1)`, n); err != nil {
			return have, fmt.Errorf("boot: seed orders: %w", err)
		}
		have += n
		log.Info("boot.seed_progress", "rows", have, "target", target,
			"batch_ms", time.Since(batchStart).Milliseconds())
		if ctx.Err() != nil {
			return have, ctx.Err()
		}
	}
	if _, err := conn.Exec(ctx, `ANALYZE orders`); err != nil {
		log.Warn("boot.analyze_failed", "err", err)
	}
	log.Info("boot.seeded", "rows", have, "took", time.Since(start).Round(time.Second).String())
	return have, nil
}

// ---------------------------------------------------------------- helpers

func dsnForDB(base, db string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	u.Path = "/" + db
	return u.String()
}

// quoteIdent is for identifiers this package chooses itself, never for user
// input. It is here so a template name with a capital letter cannot silently
// become a different database.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
