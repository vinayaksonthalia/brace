package engine

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The security boundary.
//
// Brace runs arbitrary user SQL. The endpoint is public. The only defence that
// survives contact with a creative attacker is the one the database enforces
// itself, so that is where the boundary lives:
//
// Every migration runs as brace_migrator — a LOGIN role that is NOSUPERUSER,
// NOCREATEDB, NOCREATEROLE, NOREPLICATION and NOBYPASSRLS, with a strong random
// password generated at startup and held only in this process's memory. Being
// NOSUPERUSER is what makes PostgreSQL itself refuse COPY TO/FROM PROGRAM,
// COPY to a server file, pg_read_file() and friends, and pg_terminate_backend
// against other users' sessions. Not owning any database is what makes it
// refuse DROP DATABASE.
//
// Two designs were considered and rejected:
//
//   - SET ROLE on a superuser connection. Rejected outright: RESET ROLE is a
//     single statement away, and the user controls the statement. A privilege
//     you can hand back is not a boundary.
//   - A NOLOGIN role plus SECURITY DEFINER wrappers. More moving parts, same
//     result, and a bug in a wrapper is a superuser bug.
//
// Inside the twin, brace_migrator is given OWNERSHIP of the public schema's
// tables and sequences, because ALTER TABLE, DROP INDEX, REINDEX and VACUUM
// require ownership rather than a grantable privilege. Ownership of a database
// that is destroyed at the end of the run — and that contains nothing but
// generated rows — costs nothing. It confers no power over the server.

const (
	// DefaultMigratorRole is the role every migration runs as.
	DefaultMigratorRole = "brace_migrator"

	// MigrationAppName is the application_name the migration connection sets.
	// The lock monitor keys on it to find the migration's backend.
	MigrationAppName = "brace-migration"
)

// safeIdent is what a role name must match before it is interpolated into DDL.
// Role names cannot be bind parameters, so this plus quoting is the guard.
var safeIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// safePassword is the alphabet the generated password is drawn from. Restricted
// on purpose: the password has to be interpolated into a CREATE ROLE literal,
// and an alphabet with no quote, no backslash and no whitespace makes that
// interpolation trivially correct rather than delicately correct.
var safePassword = regexp.MustCompile(`^[A-Za-z0-9_-]{20,}$`)

// generatePassword returns a 256-bit password in URL-safe base64, which lands
// in [A-Za-z0-9_-].
func generatePassword() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("engine: generate migrator password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// ensureMigratorRole creates or re-secures the migrator role on the server and
// sets its password to a freshly generated one.
//
// It runs on every startup and always re-issues ALTER ROLE, so a role left
// behind by a previous deploy — possibly with a password somebody else knows,
// or with attributes somebody widened — is brought back to a known-safe state
// and a password only this process holds.
//
// The consequence, and it is deliberate: exactly ONE Brace process may run
// against a given PostgreSQL server. A second one would rotate the password out
// from under the first, whose next migration connection would fail to
// authenticate. Brace is a single-tenant lab that owns its server — the twin
// pool already assumes that — so this is a property, not a limitation. If it
// ever needs to be multi-process, give each process its own role name.
func ensureMigratorRole(ctx context.Context, pool *pgxpool.Pool, role, password string) error {
	if !safeIdent.MatchString(role) {
		return fmt.Errorf("engine: migrator role %q must match %s", role, safeIdent)
	}
	if !safePassword.MatchString(password) {
		return fmt.Errorf("engine: generated migrator password has an unexpected shape")
	}
	q := pgx.Identifier{role}.Sanitize()
	lit := "'" + password + "'"

	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
		return fmt.Errorf("engine: look up role %q: %w", role, err)
	}

	// The attribute list is repeated on both paths deliberately: CREATE states
	// the intent, ALTER re-asserts it over whatever the role drifted into.
	const attrs = ` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT`
	if !exists {
		if _, err := pool.Exec(ctx, `CREATE ROLE `+q+attrs+` PASSWORD `+lit); err != nil {
			return fmt.Errorf("engine: create role %q: %w", role, err)
		}
		return nil
	}
	if _, err := pool.Exec(ctx, `ALTER ROLE `+q+attrs+` PASSWORD `+lit); err != nil {
		return fmt.Errorf("engine: re-secure role %q: %w", role, err)
	}
	return nil
}

// grantOnTwin gives the migrator exactly the privileges it needs inside one
// twin, and nothing anywhere else. Idempotent — it runs once per acquired twin
// and every statement is safe to repeat.
//
// The connection is opened with the twin's own (superuser) DSN, because
// object-level grants must be issued from inside the database that owns the
// objects.
func grantOnTwin(ctx context.Context, twinDSN, twinName, role string) error {
	if !safeIdent.MatchString(role) {
		return fmt.Errorf("engine: migrator role %q must match %s", role, safeIdent)
	}
	conn, err := pgx.Connect(ctx, twinDSN)
	if err != nil {
		return fmt.Errorf("engine: connect twin for grants: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = conn.Close(closeCtx)
		cancel()
	}()

	qRole := pgx.Identifier{role}.Sanitize()
	qDB := pgx.Identifier{twinName}.Sanitize()

	stmts := []string{
		`GRANT CONNECT ON DATABASE ` + qDB + ` TO ` + qRole,
		// CREATE on the schema is what lets CREATE INDEX place a new relation.
		`GRANT USAGE, CREATE ON SCHEMA public TO ` + qRole,
		`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ` + qRole,
		`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ` + qRole,
		// Ownership, not privileges: ALTER TABLE / DROP INDEX / REINDEX /
		// VACUUM are owner-only operations in PostgreSQL — no GRANT can confer
		// them. Handing the twin's tables to the migrator is the only way to
		// rehearse an ALTER TABLE at all, and the twin is destroyed minutes
		// later. Indexes follow their table's owner automatically.
		`DO $$
		 DECLARE r record;
		 BEGIN
		   FOR r IN
		     SELECT c.relname, c.relkind
		     FROM pg_class c
		     JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','S')
		   LOOP
		     IF r.relkind = 'S' THEN
		       EXECUTE format('ALTER SEQUENCE public.%I OWNER TO %I', r.relname, ` + literal(role) + `);
		     ELSE
		       EXECUTE format('ALTER TABLE public.%I OWNER TO %I', r.relname, ` + literal(role) + `);
		     END IF;
		   END LOOP;
		 END $$`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("engine: grant on twin %q: %w", twinName, err)
		}
	}
	return nil
}

// literal renders a string as a SQL literal. Only ever called with a role name
// that has already passed safeIdent, so the doubling is belt-and-braces.
func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// connectAsMigrator opens the single connection the migration runs on.
func connectAsMigrator(ctx context.Context, twinDSN, role, password string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(twinDSN)
	if err != nil {
		return nil, fmt.Errorf("engine: parse twin DSN: %w", err)
	}
	cfg.User = role
	cfg.Password = password
	cfg.RuntimeParams["application_name"] = MigrationAppName

	// statement_timeout is set EXPLICITLY to 0, never left to inherit.
	//
	// The migration is the thing under test. If the server (or a future
	// ALTER DATABASE / ALTER ROLE, or a PgBouncer default) imposed a
	// statement_timeout, a long CREATE INDEX would be killed part-way and Brace
	// would report a short, survivable freeze for a migration that in
	// production would have frozen the table for ten minutes. Under-reporting
	// harm is the one failure mode this product cannot have. A run that takes
	// too long is bounded by the caller's context instead, which cancels the
	// query without lying about what the statement does.
	cfg.RuntimeParams["statement_timeout"] = "0"
	// Likewise no lock_timeout: the migration must be allowed to wait for its
	// lock exactly as it would in production. lock_timeout belongs on the load
	// generator's pool, where it models a request deadline.
	cfg.RuntimeParams["lock_timeout"] = "0"

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("engine: connect as %s: %w", role, err)
	}
	return conn, nil
}

// findInvalidIndexes returns the names of indexes left in an INVALID state.
//
// This is the CONCURRENTLY failure mode made visible: a concurrent build that
// fails (unique violation, deadlock, cancelled session) leaves an index that
// still costs write overhead but is never used by the planner. It is
// recoverable — DROP INDEX and retry — but it is invisible unless somebody
// looks, which is exactly what a rehearsal is for.
func findInvalidIndexes(ctx context.Context, twinDSN string) ([]string, error) {
	conn, err := pgx.Connect(ctx, twinDSN)
	if err != nil {
		return nil, fmt.Errorf("engine: connect twin for index check: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = conn.Close(closeCtx)
		cancel()
	}()

	rows, err := conn.Query(ctx, `
		SELECT c.relname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT i.indisvalid AND n.nspname NOT IN ('pg_catalog','pg_toast')
		ORDER BY c.relname`)
	if err != nil {
		return nil, fmt.Errorf("engine: query invalid indexes: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("engine: scan invalid index: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engine: query invalid indexes: %w", err)
	}
	return out, nil
}
