package engine

// Bring your own schema.
//
// The stock lab rehearses a migration against ONE table (orders). This file
// lets a caller paste their OWN single CREATE TABLE and rehearse the migration
// against THAT shape instead — their columns, their widths, their NOT NULLs —
// so the freeze story is about their table, not the demo's.
//
// The security model is the same one the rest of the engine already leans on,
// and the same one the whole product's credibility rests on: the database ROLE
// is the boundary, not a regex. A bring-your-own-schema run adds exactly one new
// thing to protect — arbitrary user DDL — and it is contained the same way
// arbitrary user MIGRATIONS already are:
//
//  1. ISOLATION. The pasted table is created in a FRESH, throwaway database with
//     its own name namespace (ScratchPrefix, distinct from the twin pool and the
//     template). It is never created in brace_template, the app database, or a
//     warm twin from the shared pool, so it cannot be reached from any other run,
//     and it is DROPPED at the end of the run no matter how the run ends. A crash
//     that leaks one is swept at the next startup.
//
//  2. NON-SUPERUSER CREATION AND SEEDING. The CREATE TABLE and the synthetic
//     seeding run as brace_migrator — the same NOSUPERUSER, owns-nothing role the
//     migration under test runs as. This is what closes the escalation the traffic
//     role closes for writes: a smuggled column DEFAULT / CHECK / generated
//     expression (say DEFAULT pg_read_file('/etc/passwd')) is evaluated by the
//     session that inserts the row, and that session is unprivileged, so
//     PostgreSQL itself refuses the privileged call. Seeding fails cleanly; no file
//     is read, no privilege is gained. The migrator OWNS the table it created,
//     exactly as it owns a twin's tables, which is all an ALTER TABLE needs.
//
// The DDL is also pre-filtered (validateCreateTable) for the same reason assess.go
// pre-filters migrations: to reject a paste that is obviously not one CREATE TABLE
// before spending a database on it. That check is UX and blast-radius hygiene. It
// is NOT the security boundary — isolation and the unprivileged role are.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// DefaultScratchPrefix is the database-name namespace for schema runs.
	DefaultScratchPrefix = "brace_byos_"

	// defaultSeedRows is how many synthetic rows a schema run seeds when the
	// caller states no row count. A million rows is enough that a naive index
	// build has real work to do, and cheap enough for the shared pool.
	defaultSeedRows = 1_000_000
	// maxSeedRows is the hard ceiling on ACTUAL seeding, regardless of the row
	// count the caller states. Their stated size drives the scale PROJECTION (see
	// RehearseSchema); it must never drive how many rows the shared pool physically
	// writes, or one request could seed tens of millions and starve every other.
	maxSeedRows = 5_000_000
	// seedBatch bounds one INSERT ... generate_series, mirroring boot.seedBatch:
	// a million rows is roughly a second of work and a bounded slice of WAL.
	seedBatch = 1_000_000

	// schemaProvisionTimeout bounds the whole create-table-plus-seed step. It is
	// generous because seeding up to maxSeedRows in seedBatch-sized chunks is real
	// I/O, but bounded so a pathological table cannot wedge the single run slot
	// before the measured run even begins.
	schemaProvisionTimeout = 4 * time.Minute
	// schemaStmtTimeout bounds one provisioning statement (the CREATE TABLE, one
	// seed batch, the ANALYZE), so a single hostile statement cannot hang inside
	// the larger budget.
	schemaStmtTimeout = 90 * time.Second
)

// createTableLeadingVerb requires the statement to begin with CREATE TABLE (the
// IF NOT EXISTS form is folded in by the \b after TABLE). CREATE TEMP TABLE,
// CREATE UNLOGGED TABLE and every other variant are deliberately refused: the
// scope is exactly one plain table.
var createTableLeadingVerb = regexp.MustCompile(`^CREATE TABLE\b`)

// createTableAsQuery matches the CREATE TABLE ... AS <query> forms (CTAS). The
// query keyword after AS is what distinguishes CTAS from a GENERATED ALWAYS AS
// (expr) column, which is always followed by "(" rather than a query keyword.
var createTableAsQuery = regexp.MustCompile(`\bAS (SELECT|VALUES|TABLE|EXECUTE|WITH)\b`)

// SchemaRequest is one bring-your-own-schema rehearsal.
type SchemaRequest struct {
	// SchemaDDL is the single CREATE TABLE statement defining the table to
	// rehearse against.
	SchemaDDL string
	// RowCount is the caller's approximate REAL table size. It does two things,
	// deliberately decoupled: it is the default scale-projection target ("at your
	// real size"), and it is clamped to [1, maxSeedRows] to decide how many rows
	// are ACTUALLY seeded into the disposable database.
	RowCount int64
	// SQL is the migration under test — an ordinary allow-listed statement
	// (CREATE INDEX, ALTER TABLE, ...) that will be run against the pasted table.
	SQL string
	// Mode is carried into the ledger for framing only, exactly as for Rehearse.
	Mode Mode
}

// validateCreateTable pre-filters the pasted DDL. It returns the single
// statement to execute (original text, so PostgreSQL parses it, not the
// case-folded flat form) or a *RejectedError with a reason the caller can act on.
//
// It reuses the migration pre-filter's splitter and size cap on purpose: the
// "exactly one statement, no semicolon-smuggling" guarantee is precisely the
// thing splitStatements already provides for migrations, and re-implementing it
// would be a second, weaker copy of the same logic.
func validateCreateTable(ddl string) (string, error) {
	trimmed := strings.TrimSpace(ddl)
	if trimmed == "" {
		return "", &RejectedError{
			Kind:   RejectRejected,
			Code:   "schema_empty",
			Reason: "No CREATE TABLE was provided. Paste a single CREATE TABLE statement to rehearse against your own shape.",
		}
	}
	if len(trimmed) > maxSQLBytes {
		return "", &RejectedError{
			Kind:   RejectRejected,
			Code:   "schema_too_long",
			Reason: fmt.Sprintf("That CREATE TABLE is longer than Brace accepts (limit %d KB). Trim it to the table you want to rehearse against.", maxSQLBytes>>10),
		}
	}

	// splitStatements splits on real statement boundaries only (semicolons that
	// are not inside a string, identifier, dollar-quoted body or comment), so a
	// second statement — smuggled with `;` or a trailing `; DROP ...` — becomes a
	// second segment here and is refused.
	segments := splitStatements(trimmed)
	if len(segments) != 1 {
		return "", &RejectedError{
			Kind: RejectRejected,
			Code: "schema_multi_statement",
			Reason: "Brace's bring-your-own-schema takes EXACTLY ONE CREATE TABLE statement. That paste contains more than " +
				"one statement — remove the extra statements (and any trailing semicolon-separated commands).",
		}
	}
	stmt := segments[0]
	flat := flatten(stmt)
	if flat == "" {
		return "", &RejectedError{
			Kind:   RejectRejected,
			Code:   "schema_empty",
			Reason: "That is a comment, not a CREATE TABLE. Paste the table definition itself.",
		}
	}
	if !createTableLeadingVerb.MatchString(flat) {
		return "", &RejectedError{
			Kind: RejectRejected,
			Code: "schema_not_create_table",
			Reason: "The bring-your-own-schema box takes a single CREATE TABLE statement (CREATE TABLE, or CREATE TABLE IF " +
				"NOT EXISTS). Temporary, unlogged and other variants are not accepted.",
			Suggestion: "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id bigint NOT NULL, amount_cents bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT now());",
		}
	}
	if createTableAsQuery.MatchString(flat) || strings.Contains(flat, "EXECUTE") {
		return "", &RejectedError{
			Kind: RejectRejected,
			Code: "schema_create_table_as",
			Reason: "CREATE TABLE ... AS (and any form that runs a query or EXECUTE to populate the table) is not accepted. " +
				"Paste a plain column definition; Brace seeds the rows itself.",
		}
	}
	return stmt, nil
}

// resolveSeedRows turns the caller's stated real size into the number of rows we
// will actually write into the disposable database: their number when small,
// clamped to a bounded sample for the shared pool. The projection carries the
// rest of the story.
func resolveSeedRows(rowCount int64) int64 {
	if rowCount <= 0 {
		return defaultSeedRows
	}
	if rowCount > maxSeedRows {
		return maxSeedRows
	}
	return rowCount
}

// schemaColumn is one column as read back from the catalog after the table is
// created — the ground truth, never a parse of the DDL.
type schemaColumn struct {
	Name       string
	DataType   string // information_schema.data_type, e.g. "integer", "text", "ARRAY"
	UDTName    string // information_schema.udt_name, e.g. "int4", "_text"
	Nullable   bool
	HasDefault bool
	Identity   bool
	Generated  bool // GENERATED ALWAYS AS (...) STORED
	CharMaxLen int  // character_maximum_length, 0 when not applicable
}

// required reports whether a value MUST be supplied for this column on insert:
// it is NOT NULL, has no default, and is neither an identity nor a generated
// column (both of which fill themselves).
func (c schemaColumn) required() bool {
	return !c.Nullable && !c.HasDefault && !c.Identity && !c.Generated
}

// supplyable reports whether Brace is allowed to supply a value for this column
// at all — anything except identity and generated columns, which reject or
// ignore an explicit value.
func (c schemaColumn) supplyable() bool {
	return !c.Identity && !c.Generated
}

// RehearseSchema crash-tests one migration against a disposable copy of the
// caller's OWN table.
//
// The lifecycle is: validate the DDL, assess the migration, create a fresh
// throwaway database, create the pasted table in it AS the unprivileged
// migrator, seed a bounded sample of synthetic rows, then run the ordinary
// measured rehearsal (generic INSERT traffic + the migration under test) and
// finally drop the whole database. The disposable database is dropped on EVERY
// exit path.
//
// The error contract matches Rehearse (so the API maps it the same way): a
// *RejectedError for anything the user can fix (bad DDL, a migration that is not
// a schema change, a table shape Brace cannot seed or drive traffic at), and a
// (*RunResult, nil) with RunResult.Error set for a migration PostgreSQL refused —
// a finding, not an engine error.
func (e *Engine) RehearseSchema(ctx context.Context, req SchemaRequest, opts ...RunOption) (*RunResult, error) {
	ro := e.resolveOptions(opts)
	mode, err := req.Mode.normalize()
	if err != nil {
		return nil, err
	}

	// 1. The DDL. Refused before a database is spent, exactly as a migration is
	// refused before a twin is acquired.
	stmt, err := validateCreateTable(req.SchemaDDL)
	if err != nil {
		return nil, err
	}

	// 2. The migration. Assessed with the run's options so the hostile carve-out
	// still applies; gateAlreadySafe is deliberately NOT used here, because the
	// target relation lives in the fresh database we are about to build, not in a
	// warm twin whose catalog is the stock orders schema.
	as, err := assessForRun(req.SQL, ro)
	if err != nil {
		return nil, err
	}

	// 3. Sizing. The stated size becomes the projection target (unless the caller
	// overrode target_rows explicitly), and is clamped to decide the ACTUAL seed.
	if ro.TargetSource != TargetSourceRequest && req.RowCount > 0 {
		if v := validTargetRows(req.RowCount); v > 0 {
			ro.TargetRows = v
			ro.TargetSource = TargetSourceRequest
		}
	}
	seedRows := resolveSeedRows(req.RowCount)
	// When the caller's real table is larger than the seed cap, the twin is only a
	// capped SAMPLE. Record the requested size so the run note says so loudly: a
	// SAFE here is a SAFE on seedRows rows, and everything above is projection.
	if req.RowCount > seedRows {
		ro.SeedCapRequested = req.RowCount
	}

	runID, err := newRunID()
	if err != nil {
		return nil, err
	}
	state := &RunState{
		RunID:     runID,
		Mode:      mode,
		SQL:       as.SQL,
		Phase:     phaseProvisioning,
		StartedAt: time.Now(),
	}
	if err := e.begin(state); err != nil {
		return nil, err
	}
	defer e.finish()

	e.log.Info("engine.schema_run_started", "run", runID, "seed_rows", seedRows,
		"target_rows", ro.TargetRows, "classification", as.Classification)

	// 4. Provision. provisionSchemaDB creates the database FIRST and returns its
	// name even when a later step fails, so the drop below always has something to
	// reap. A nil twin means the database itself was never created.
	twin, labels, provErr := e.provisionSchemaDB(ctx, stmt, seedRows, state)
	if twin != nil {
		e.mu.Lock()
		if e.cur != nil {
			e.cur.Twin = twin.Name
		}
		e.mu.Unlock()
		// The database is dropped on every exit — measured run, provisioning
		// failure, panic, or a cancelled request. The context is detached because
		// teardown has to work precisely when the caller's request is already gone.
		defer func() {
			dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
			defer cancel()
			if err := e.dropScratchDB(dropCtx, twin.Name); err != nil {
				e.log.Error("engine.scratch_drop_failed", "run", runID, "db", twin.Name, "err", err)
			} else {
				e.log.Info("engine.scratch_dropped", "run", runID, "db", twin.Name)
			}
		}()
	}
	if provErr != nil {
		return nil, provErr
	}

	// 5. The measured run — identical to a twin run, only the traffic mix and the
	// projection denominator are this run's own.
	ro.Labels = labels
	ro.TwinRows = seedRows
	return e.runOn(ctx, twin, as, mode, state, ro)
}

// AssessSchemaRun is the synchronous pre-check for the HTTP layer: it makes the
// same admit/refuse decision RehearseSchema will, without provisioning anything,
// so a bad paste is a 400 on the submitting request rather than an ERRORED run a
// few seconds later. It validates the DDL and assesses the migration; it cannot
// (and does not) check anything about the not-yet-created table.
func (e *Engine) AssessSchemaRun(_ context.Context, schemaDDL, sql string, opts ...RunOption) (*Assessment, error) {
	if _, err := validateCreateTable(schemaDDL); err != nil {
		return nil, err
	}
	return assessForRun(sql, e.resolveOptions(opts))
}

// provisionSchemaDB builds the disposable database: create it, create the pasted
// table in it as the unprivileged migrator, introspect the columns, synthesize
// the seed and the traffic mix, and seed a bounded sample. It returns the
// database as a twins.Twin (Name + admin DSN) so runOn can treat it exactly like
// a pool twin.
//
// The twin is returned as soon as the database exists, EVEN on a later error, so
// the caller can always drop it. A *RejectedError is returned for a shape the
// user can fix (a required column of a type Brace cannot synthesize, a table with
// nothing to insert, a seed refused because a smuggled expression demanded
// privilege the unprivileged role does not have); other failures are wrapped
// plainly.
func (e *Engine) provisionSchemaDB(ctx context.Context, tableDDL string, seedRows int64, state *RunState) (*twins.Twin, []loadgen.LabelSpec, error) {
	provCtx, cancel := context.WithTimeout(ctx, schemaProvisionTimeout)
	defer cancel()

	name, err := newScratchName(e.cfg.ScratchPrefix)
	if err != nil {
		return nil, nil, err
	}

	// CREATE DATABASE runs on the admin pool (which is connected to the
	// maintenance database, never to a scratch database) so it is a plain
	// autocommit statement. No TEMPLATE clause: the default (template1) gives a
	// pristine empty database, isolated from brace_template and every twin.
	if _, err := e.pool.Exec(provCtx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		return nil, nil, fmt.Errorf("engine: create scratch database: %w", err)
	}

	adminDSN, err := scratchDSN(e.cfg.AdminURL, name)
	if err != nil {
		// The database exists; hand it back (with a nil DSN twin) so it is dropped.
		return &twins.Twin{Name: name}, nil, err
	}
	twin := &twins.Twin{Name: name, DSN: adminDSN, CreatedAt: time.Now()}

	// Grants, issued from inside the fresh database over its admin (superuser)
	// DSN. The migrator gets exactly what it needs to CREATE and own the pasted
	// table; PUBLIC's CREATE on the schema is revoked so nothing else — including
	// the traffic role added later by runOn — can place an object here.
	if err := e.prepareScratchSchema(provCtx, adminDSN); err != nil {
		return twin, nil, err
	}

	// The CREATE TABLE and the seeding both run as the UNPRIVILEGED migrator. This
	// is the escalation defence: a column DEFAULT / CHECK / generated expression is
	// evaluated by this session when a row is inserted, and this session is
	// NOSUPERUSER, so a privileged call inside it is refused by PostgreSQL itself.
	migConn, err := connectAsMigrator(provCtx, adminDSN, e.cfg.MigratorRole, e.migratorPassword)
	if err != nil {
		return twin, nil, fmt.Errorf("engine: connect scratch as migrator: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = migConn.Close(closeCtx)
		cancel()
	}()

	ddlCtx, cancelDDL := context.WithTimeout(provCtx, schemaStmtTimeout)
	_, err = migConn.Exec(ddlCtx, tableDDL)
	cancelDDL()
	if err != nil {
		// The pasted DDL did not apply as an unprivileged role in a clean database.
		// That is the user's table definition failing, not a Brace error — surface
		// the PostgreSQL reason so they can fix it.
		return twin, nil, schemaDDLRejected("The pasted CREATE TABLE could not be created", err)
	}

	tableName, cols, err := introspectScratchTable(provCtx, migConn)
	if err != nil {
		return twin, nil, err
	}

	seedSQL, trafficSQL, insertCols, err := synthesizeIO(tableName, cols)
	if err != nil {
		return twin, nil, err
	}

	state.Phase = phaseSeeding
	e.setPhase(phaseSeeding)
	if err := seedScratch(provCtx, migConn, seedSQL, seedRows); err != nil {
		return twin, nil, seedRejected(err)
	}
	// ANALYZE so the planner has statistics before the migration is measured — a
	// CREATE INDEX cost and reltuples both depend on it. Best-effort.
	analyzeCtx, cancelAnalyze := context.WithTimeout(provCtx, schemaStmtTimeout)
	if _, err := migConn.Exec(analyzeCtx, `ANALYZE `+pgx.Identifier{tableName}.Sanitize()); err != nil {
		e.log.Warn("engine.scratch_analyze_failed", "db", name, "err", err)
	}
	cancelAnalyze()

	labels := []loadgen.LabelSpec{{Name: "write", Weight: 1, Statement: trafficSQL}}
	e.log.Info("engine.scratch_ready", "db", name, "table", tableName,
		"seeded_rows", seedRows, "insert_columns", strings.Join(insertCols, ","))
	return twin, labels, nil
}

// prepareScratchSchema tightens the fresh database and grants the migrator the
// least it needs to create and own the pasted table.
func (e *Engine) prepareScratchSchema(ctx context.Context, adminDSN string) error {
	if !safeIdent.MatchString(e.cfg.MigratorRole) {
		return fmt.Errorf("engine: migrator role %q must match %s", e.cfg.MigratorRole, safeIdent)
	}
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("engine: connect scratch as admin: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = conn.Close(closeCtx)
		cancel()
	}()
	qRole := pgx.Identifier{e.cfg.MigratorRole}.Sanitize()
	for _, s := range []string{
		// No object may be placed in this schema except by roles we grant it to.
		// On PG15+ this is already the default; issuing it is idempotent and makes
		// the intent explicit on older servers.
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT USAGE, CREATE ON SCHEMA public TO ` + qRole,
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("engine: prepare scratch schema: %w", err)
		}
	}
	return nil
}

// introspectScratchTable finds the one user table the DDL created and reads its
// columns from the catalog. Reading back the created table — rather than parsing
// the DDL — is what makes the seed and traffic synthesis correct: identifiers,
// types, defaults and NOT NULLs are whatever PostgreSQL actually stored.
func introspectScratchTable(ctx context.Context, conn *pgx.Conn) (string, []schemaColumn, error) {
	var name string
	err := conn.QueryRow(ctx, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
		ORDER BY c.relname
		LIMIT 1`).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, schemaShapeRejected("The pasted CREATE TABLE created no table in the public schema; Brace rehearses one plain table in public.")
	}
	if err != nil {
		return "", nil, fmt.Errorf("engine: locate scratch table: %w", err)
	}

	rows, err := conn.Query(ctx, `
		SELECT column_name,
		       data_type,
		       udt_name,
		       (is_nullable = 'YES'),
		       (column_default IS NOT NULL),
		       (is_identity = 'YES'),
		       (is_generated = 'ALWAYS'),
		       COALESCE(character_maximum_length, 0)
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`, name)
	if err != nil {
		return "", nil, fmt.Errorf("engine: read scratch columns: %w", err)
	}
	defer rows.Close()
	var cols []schemaColumn
	for rows.Next() {
		var c schemaColumn
		if err := rows.Scan(&c.Name, &c.DataType, &c.UDTName, &c.Nullable,
			&c.HasDefault, &c.Identity, &c.Generated, &c.CharMaxLen); err != nil {
			return "", nil, fmt.Errorf("engine: scan scratch column: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("engine: read scratch columns: %w", err)
	}
	if len(cols) == 0 {
		return "", nil, schemaShapeRejected("The pasted table has no columns Brace can read.")
	}
	return name, cols, nil
}

// synthesizeIO builds the seed statement and the traffic INSERT for the table.
//
// It supplies values ONLY for the columns that require one (NOT NULL, no default,
// not identity/generated). Every other column is deliberately OMITTED so its
// DEFAULT actually fires during seeding — under the unprivileged role — which is
// both realistic and the thing that makes a smuggled privileged default refuse
// itself. When a table has no required columns, a single supplyable column is
// used as the carrier so there is still somewhere to write and to bind traffic
// parameters.
//
// A required column whose type Brace cannot synthesize is a *RejectedError: the
// honest answer is "this shape is not supported", never a crash or a silent NULL
// into a NOT NULL column.
func synthesizeIO(table string, cols []schemaColumn) (seedSQL, trafficSQL string, insertCols []string, err error) {
	qTable := pgx.Identifier{table}.Sanitize()

	var chosen []schemaColumn
	for _, c := range cols {
		if c.required() {
			if _, ok := seedValueExpr(c, "g"); !ok {
				return "", "", nil, schemaShapeRejected(fmt.Sprintf(
					"Column %q is NOT NULL with no default and has type %q, which Brace cannot generate synthetic values for. "+
						"Make it nullable, give it a default, or drop it to rehearse this table.", c.Name, columnTypeLabel(c)))
			}
			chosen = append(chosen, c)
		}
	}
	// No required columns: pick the first supplyable column Brace can synthesize
	// as the carrier, letting every other column take its default / NULL.
	if len(chosen) == 0 {
		for _, c := range cols {
			if !c.supplyable() {
				continue
			}
			if _, ok := seedValueExpr(c, "g"); ok {
				chosen = append(chosen, c)
				break
			}
		}
	}
	if len(chosen) == 0 {
		return "", "", nil, schemaShapeRejected(
			"Brace could not synthesize any INSERT for this table shape (every column is generated, identity, or a type it " +
				"cannot fill), so it cannot seed rows or drive write traffic against it.")
	}

	colIdents := make([]string, len(chosen))
	seedVals := make([]string, len(chosen))
	trafVals := make([]string, len(chosen))
	// The traffic base is randomised and offset well above the seeded range so a
	// user PRIMARY KEY / UNIQUE column does not collide with the sequential seed on
	// most inserts (a rare collision lands, visibly, in the errored bucket — never
	// in casualties).
	const trafBase = "(1000000000 + (random() * 1000000000)::bigint)"
	for i, c := range chosen {
		colIdents[i] = pgx.Identifier{c.Name}.Sanitize()
		sv, _ := seedValueExpr(c, "g")
		tv, _ := seedValueExpr(c, trafBase)
		seedVals[i] = sv
		if i == 0 {
			// Weave BOTH bound parameters into the first value with a no-op CASE, so
			// every traffic statement references $1 and $2 (loadgen binds exactly two
			// bigints and PostgreSQL rejects a statement that declares fewer) without
			// changing the value or its type. The ::bigint casts are load-bearing: a
			// bare `$1 = $1` gives the planner no type to infer, so it resolves the
			// parameter to text and pgx then cannot encode the bound int64 into it.
			// Pinning them to bigint is what the stock orders labels get for free from
			// inserting into a bigint column.
			tv = "(CASE WHEN $1::bigint = $1::bigint AND $2::bigint = $2::bigint THEN (" + tv + ") ELSE (" + tv + ") END)"
		}
		trafVals[i] = tv
		insertCols = append(insertCols, c.Name)
	}

	colList := strings.Join(colIdents, ", ")
	seedSQL = "INSERT INTO " + qTable + " (" + colList + ") SELECT " + strings.Join(seedVals, ", ") +
		" FROM generate_series(1, $1) AS s(g)"
	trafficSQL = "INSERT INTO " + qTable + " (" + colList + ") VALUES (" + strings.Join(trafVals, ", ") + ")"
	return seedSQL, trafficSQL, insertCols, nil
}

// seedValueExpr returns a SQL value expression for a column, given a base
// expression that stands in for the row's ordinal ("g" for the seed's
// generate_series, a random expression for traffic). The second return is false
// for a type Brace does not synthesize.
//
// The type is used ONLY to select a fixed expression from this whitelist; it is
// never interpolated into SQL, so an exotic type name can do nothing but fall
// through to the unsupported branch.
func seedValueExpr(c schemaColumn, base string) (string, bool) {
	sub := func(tpl string) string { return strings.ReplaceAll(tpl, "@B@", base) }
	switch strings.ToLower(c.DataType) {
	case "smallint":
		return sub("((@B@ % 30000))::smallint"), true
	case "integer":
		return sub("((@B@ % 2000000000))::int"), true
	case "bigint":
		return sub("(@B@)::bigint"), true
	case "numeric", "decimal":
		return sub("((@B@ % 100000))::numeric"), true
	case "double precision":
		return sub("((@B@ % 100000))::double precision"), true
	case "real":
		return sub("((@B@ % 100000))::real"), true
	case "boolean":
		return sub("((@B@ % 2) = 0)"), true
	case "text":
		return textValueExpr(base, 0), true
	case "character varying", "character":
		return textValueExpr(base, c.CharMaxLen), true
	case "uuid":
		return "gen_random_uuid()", true
	case "timestamp with time zone":
		return sub("(now() - ((@B@ % 1000000) * interval '1 second'))"), true
	case "timestamp without time zone":
		return sub("((now() - ((@B@ % 1000000) * interval '1 second'))::timestamp)"), true
	case "date":
		return sub("((now() - ((@B@ % 100000) * interval '1 day'))::date)"), true
	case "time without time zone":
		return "(now()::time)", true
	case "time with time zone":
		return "(now()::timetz)", true
	case "jsonb":
		return sub("jsonb_build_object('n', @B@)"), true
	case "json":
		return sub("json_build_object('n', @B@)"), true
	case "inet":
		return sub("(('10.' || ((@B@ % 254) + 1)::text || '.' || (((@B@ / 254) % 254) + 1)::text || '.1')::inet)"), true
	case "bytea":
		return sub("decode(md5((@B@)::text), 'hex')"), true
	case "array":
		if strings.EqualFold(c.UDTName, "_text") {
			return sub("(ARRAY['r' || (@B@)::text]::text[])"), true
		}
		return "", false
	default:
		return "", false
	}
}

// textValueExpr builds a short text value, clamped to a declared length so a
// varchar(n) or char(n) column cannot overflow.
func textValueExpr(base string, maxLen int) string {
	body := "('r' || (" + base + ")::text)"
	if maxLen > 0 {
		return "left(" + body + ", " + fmt.Sprintf("%d", maxLen) + ")"
	}
	return body
}

// columnTypeLabel renders a human-friendly type name for a rejection message.
func columnTypeLabel(c schemaColumn) string {
	if strings.EqualFold(c.DataType, "array") && c.UDTName != "" {
		return c.UDTName
	}
	if c.DataType == "USER-DEFINED" && c.UDTName != "" {
		return c.UDTName
	}
	return c.DataType
}

// seedScratch runs the seed statement in bounded batches, mirroring boot's
// seeding so a single INSERT never becomes the thing that runs out of WAL.
func seedScratch(ctx context.Context, conn *pgx.Conn, seedSQL string, target int64) error {
	var done int64
	for done < target {
		n := target - done
		if n > seedBatch {
			n = seedBatch
		}
		batchCtx, cancel := context.WithTimeout(ctx, schemaStmtTimeout)
		_, err := conn.Exec(batchCtx, seedSQL, n)
		cancel()
		if err != nil {
			return err
		}
		done += n
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

// newScratchName builds a unique disposable-database name, laid out like a twin
// name (prefix + UTC timestamp + random suffix) so it is readable in psql \l and
// still collision-proof.
func newScratchName(prefix string) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("engine: generate scratch name: %w", err)
	}
	name := prefix + time.Now().UTC().Format("20060102t150405") + "_" + hex.EncodeToString(b[:])
	if len(name) > 63 {
		return "", fmt.Errorf("engine: scratch name %q exceeds 63 bytes; shorten ScratchPrefix", name)
	}
	return name, nil
}

// scratchDSN rewrites the admin URL to point at the scratch database, swapping
// only the path so sslmode and every other option survive verbatim — the same
// technique twins.dsnForDB uses.
func scratchDSN(adminURL, dbname string) (string, error) {
	u, err := url.Parse(adminURL)
	if err != nil {
		return "", fmt.Errorf("engine: parse admin URL for scratch DSN: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", fmt.Errorf("engine: admin URL is not a postgres URL (scheme %q); cannot derive a scratch DSN", u.Scheme)
	}
	u.Path = "/" + dbname
	return u.String(), nil
}

// dropScratchDB removes one disposable database, forcing off any straggler
// backends (a load generator winding down) with WITH (FORCE), exactly as the
// twin pool drops a twin.
func (e *Engine) dropScratchDB(ctx context.Context, name string) error {
	if !strings.HasPrefix(name, e.cfg.ScratchPrefix) {
		// A name outside our namespace must never reach DROP DATABASE. This can
		// only happen through a programming error, but the guard is cheap and the
		// operation is irreversible.
		return fmt.Errorf("engine: refusing to drop %q: outside scratch namespace %q", name, e.cfg.ScratchPrefix)
	}
	_, err := e.pool.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "3D000" {
			return nil // already gone
		}
		return fmt.Errorf("engine: drop scratch database %q: %w", name, err)
	}
	return nil
}

// sweepScratchOrphans drops every database in the scratch namespace at startup.
// A fresh process tracks none, and Brace is single-tenant per server, so anything
// matching the prefix is debris from a crashed run.
func (e *Engine) sweepScratchOrphans(ctx context.Context) error {
	rows, err := e.pool.Query(ctx,
		`SELECT datname FROM pg_database WHERE starts_with(datname, $1) ORDER BY datname`, e.cfg.ScratchPrefix)
	if err != nil {
		return fmt.Errorf("engine: list scratch orphans: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return fmt.Errorf("engine: scan scratch orphan: %w", err)
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: list scratch orphans: %w", err)
	}
	for _, n := range names {
		if err := e.dropScratchDB(ctx, n); err != nil {
			return err
		}
		e.log.Info("engine.scratch_orphan_swept", "db", n)
	}
	return nil
}

// schemaDDLRejected wraps a PostgreSQL error from applying the pasted CREATE
// TABLE into a user-facing rejection, carrying the server's own message (which is
// about the user's columns, not Brace internals).
func schemaDDLRejected(prefix string, err error) error {
	return &RejectedError{
		Kind:   RejectRejected,
		Code:   "schema_ddl_failed",
		Reason: prefix + ": " + pgMessage(err),
	}
}

// seedRejected explains a seeding failure. A permission-denied here is the
// escalation defence working: a column expression demanded a privilege the
// unprivileged seeding role does not have, so it was refused.
func seedRejected(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		return &RejectedError{
			Kind: RejectRejected,
			Code: "schema_seed_denied",
			Reason: "Brace could not seed this table: a column default, CHECK, or generated expression was REFUSED when " +
				"evaluated as Brace's unprivileged role (\"" + pgErr.Message + "\"). Brace seeds and writes as a NOSUPERUSER role " +
				"on purpose, so an expression that needs superuser privileges (for example one calling pg_read_file) cannot run. " +
				"Remove it and rehearse again.",
		}
	}
	return &RejectedError{
		Kind:   RejectRejected,
		Code:   "schema_seed_failed",
		Reason: "Brace could not seed synthetic rows into this table: " + pgMessage(err) + ".",
	}
}

func schemaShapeRejected(reason string) error {
	return &RejectedError{Kind: RejectRejected, Code: "schema_unsupported_shape", Reason: reason}
}

// pgMessage extracts the clean server message from a pgx error, falling back to
// the error string. It never returns a wrapped DSN: a PgError.Message is the
// server's own text about the statement.
func pgMessage(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Message
	}
	return err.Error()
}
