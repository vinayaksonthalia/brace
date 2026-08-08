package engine

// The already_safe existence gate.
//
// Assess (assess.go) classifies a statement by SQL SHAPE and short-circuits the
// catalog-only shapes — ADD COLUMN with a constant default, ANALYZE, COMMENT —
// to an already_safe rejection that the API and the MCP both read as SAFE. That
// classification never checks that the referenced object actually EXISTS, so a
// typo'd or absent table (ALTER TABLE users ADD COLUMN x int, where users is not
// in the lab) was blessed SAFE without ever running. A SAFE verdict for a
// statement that cannot even execute is the worst thing this tool can say.
//
// This file closes that hole: before the already_safe answer stands, the target
// relation is verified against the lab's actual data. The check is a single
// catalog query (to_regclass) against a warm twin — an exact clone of the
// template — so it needs no clone and no Acquire, and it keeps the fast-path
// fast for the common case where the table is there. A missing relation is
// turned into a plain REJECTED that names the object and lists what the lab does
// have, which is clearly NOT safe.
//
// Only the already_safe short-circuit is gated here. Every other shape is
// rehearsed on a real twin, where a missing relation makes the DDL fail and the
// run already comes back ERRORED on its own — the fast-path was the only place a
// nonexistent relation could slip through as SAFE.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// existenceCheckTimeout bounds the whole peek — connect plus one or two catalog
// queries against a warm twin. Generous, because it is off the hot path and a
// slow answer is still better than a false SAFE.
const existenceCheckTimeout = 10 * time.Second

// gateAlreadySafe intercepts an already_safe rejection and downgrades it to a
// clear REJECTED when the target relation does not exist in the lab. Any other
// error (a genuine rejection, or nil) passes through untouched, so callers can
// wrap every assess result in it unconditionally.
func (e *Engine) gateAlreadySafe(ctx context.Context, as *Assessment, err error) error {
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Kind != RejectAlreadySafe {
		return err
	}
	rel, ok := targetRelation(as)
	if !ok {
		// No single relation we can name with confidence — a bare ANALYZE (which
		// targets everything), an IF EXISTS form (where a missing table is a
		// deliberate no-op, genuinely safe), a COMMENT on a non-relation object, or
		// a quoted identifier whose case Flat has folded and we will not risk
		// guessing. Nothing to verify, so the fast-path answer stands.
		return err
	}
	exists, known, tables := e.relationExistsInLab(ctx, rel)
	if !known || exists {
		// Either the relation is really there, or the lab could not be reached to
		// say otherwise. Never manufacture a false REJECTED from an unanswerable
		// check: the original already_safe stands, and a real run (if it happens)
		// would surface a missing relation as ERRORED anyway.
		return err
	}
	return relationNotFound(rel, tables)
}

// relationExistsInLab reports whether rel (a to_regclass-compatible textual
// relation name) resolves to a relation in the lab's data. It peeks at a warm
// twin and runs a single to_regclass, which honours search_path,
// schema-qualification and quoting exactly as the migration itself would — so
// name resolution is Postgres's job, not a reimplementation in Go.
//
// known is false when the check could not be answered (no warm twin right now,
// or the peeked twin vanished under us): treat that as "unknown", never as
// absence. When the relation is absent, tables is the lab's public tables, so
// the caller can tell the user what the lab actually has.
func (e *Engine) relationExistsInLab(ctx context.Context, rel string) (exists, known bool, tables []string) {
	dsn, ok := e.cfg.Twins.WarmTwinDSN()
	if !ok {
		return false, false, nil
	}
	qctx, cancel := context.WithTimeout(ctx, existenceCheckTimeout)
	defer cancel()

	conn, err := pgx.Connect(qctx, dsn)
	if err != nil {
		// The warm twin may have been acquired and released (dropped) between the
		// peek and the connect. That is not evidence the relation is absent.
		return false, false, nil
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = conn.Close(closeCtx)
		cancel()
	}()

	var found bool
	// to_regclass returns NULL for a name it cannot resolve rather than erroring,
	// so this is a single boolean round trip. A rare error (a malformed name we
	// failed to filter, the twin dropped mid-query) leaves the answer unknown.
	if err := conn.QueryRow(qctx, `SELECT to_regclass($1) IS NOT NULL`, rel).Scan(&found); err != nil {
		return false, false, nil
	}
	if found {
		return true, true, nil
	}
	return false, true, publicTables(qctx, conn)
}

// publicTables lists the ordinary and partitioned tables in schema public — the
// tables the lab actually has — for the "did you mean one of these?" hint on a
// not-found. Best-effort: any error just yields no list.
func publicTables(ctx context.Context, conn *pgx.Conn) []string {
	rows, err := conn.Query(ctx, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
		ORDER BY c.relname`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return out
		}
		out = append(out, name)
	}
	return out
}

// relationNotFound builds the REJECTED that replaces a would-be false SAFE. Kind
// is RejectRejected, never RejectAlreadySafe, so nothing downstream (the API's
// 400 mapping, the MCP's already_safe→SAFE read) can mistake it for safe.
func relationNotFound(rel string, tables []string) error {
	// The token was lifted from the case-folded Flat form; render it lower case,
	// which is exactly the name an unquoted identifier resolves to in Postgres, so
	// the message names the relation the way the server would look for it.
	rel = strings.ToLower(rel)
	reason := fmt.Sprintf("Relation %q does not exist in the lab, so there is nothing to rehearse — and this "+
		"statement cannot be called safe: it would fail with \"relation does not exist\" the moment it ran.", rel)
	suggestion := ""
	if len(tables) > 0 {
		list := strings.Join(tables, ", ")
		reason += " The lab's tables are: " + list + "."
		suggestion = "Target one of the lab's tables: " + list + "."
	} else {
		reason += " The lab has no tables in its public schema to target."
	}
	return &RejectedError{
		Kind:       RejectRejected,
		Code:       "relation_not_found",
		Reason:     reason,
		Suggestion: suggestion,
	}
}

// targetRelation extracts the single relation an already_safe statement targets,
// so its existence can be verified before the fast-path blesses it SAFE. It
// returns ok=false whenever a single relation cannot be named CONFIDENTLY: a
// bare ANALYZE (targets everything), an IF EXISTS ALTER (a missing table is a
// deliberate no-op, genuinely safe), a COMMENT on a non-relation object, or a
// quoted/over-qualified identifier we will not risk. Returning false keeps the
// original already_safe answer; the only cost is not catching a not-found in
// that narrow shape — never a wrong rejection of a relation that is really there.
//
// The returned text is fed verbatim to to_regclass, which folds unquoted names
// to lower case and resolves schema-qualification itself, so an upper-cased token
// from Flat ("ORDERS", "PUBLIC.ORDERS") resolves correctly for the unquoted names
// that are the overwhelming common case.
func targetRelation(as *Assessment) (string, bool) {
	if as == nil {
		return "", false
	}
	flat := as.Flat
	switch as.Classification {
	case ClassAddColumnConstDefault:
		// ALTER TABLE [IF EXISTS] [ONLY] <name> ADD COLUMN ...
		rest := strings.TrimPrefix(flat, "ALTER TABLE ")
		if rest == flat {
			return "", false
		}
		if strings.HasPrefix(rest, "IF EXISTS ") {
			// With IF EXISTS a missing table is a no-op, not an error: safe as-is.
			return "", false
		}
		rest = strings.TrimPrefix(rest, "ONLY ")
		return firstRelationToken(rest)

	case ClassAnalyze:
		// ANALYZE [ (options) ] [ table [ (columns) ] ]. A bare ANALYZE targets the
		// whole database; a parenthesised option list is not parsed here.
		rest := strings.TrimSpace(strings.TrimPrefix(flat, "ANALYZE"))
		if rest == "" || strings.HasPrefix(rest, "(") {
			return "", false
		}
		return firstRelationToken(rest)

	case ClassComment:
		// Only the forms that name a relation to_regclass can resolve are checked;
		// COMMENT ON FUNCTION / SCHEMA / ROLE / ... are skipped.
		if rest := strings.TrimPrefix(flat, "COMMENT ON TABLE "); rest != flat {
			return firstRelationToken(rest)
		}
		if rest := strings.TrimPrefix(flat, "COMMENT ON COLUMN "); rest != flat {
			// The object is <relation>.<column>; verify the relation part.
			tok, ok := firstRelationToken(rest)
			if !ok {
				return "", false
			}
			i := strings.LastIndex(tok, ".")
			if i <= 0 {
				return "", false
			}
			return tok[:i], true
		}
		return "", false
	}
	return "", false
}

// firstRelationToken returns the leading relation name of a flattened clause —
// everything up to the first whitespace or '(' — and false when it is empty,
// contains a double-quoted identifier (whose case Flat has folded, so to_regclass
// would look for the wrong name), or carries more than one schema qualifier
// (which to_regclass rejects outright). The common unquoted "orders" and
// "public.orders" forms pass straight through.
func firstRelationToken(s string) (string, bool) {
	s = strings.TrimSpace(s)
	end := len(s)
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '(' {
			end = i
			break
		}
	}
	tok := s[:end]
	if tok == "" || strings.Contains(tok, `"`) || strings.Count(tok, ".") > 1 {
		return "", false
	}
	return tok, true
}
