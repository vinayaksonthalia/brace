package engine

import (
	"regexp"
	"strings"
)

// The pre-filter.
//
// READ THIS BEFORE CHANGING ANYTHING HERE. The security boundary for user SQL
// is NOT this file. It is the PostgreSQL role the migration runs as:
// brace_migrator is NOSUPERUSER, owns nothing outside the throwaway twin, and
// so is denied COPY TO/FROM PROGRAM, server-side file access, DROP DATABASE and
// every other dangerous verb by the server itself, not by a regex. See
// migrate.go.
//
// What this file is for is UX and blast-radius hygiene: telling a user that
// Brace measures one DDL statement at a time, before we spend an 80-second twin
// clone finding out. A bypass here is a usability bug. It is not a breach.
//
// That distinction is what lets the checks stay simple and readable instead of
// growing into the un-winnable game of parsing SQL well enough to be a security
// control.

const (
	// maxSQLBytes caps the submission. Migrations Brace can rehearse are single
	// statements; 4KB is a generous ceiling for one, and a small enough one that
	// nobody can post a novel at the endpoint.
	maxSQLBytes = 4096
)

// allowedLeadingVerb is the allowlist. Anything that does not begin with one of
// these is refused with an explanation — a rehearsal only means something for
// schema changes run against live traffic.
var allowedLeadingVerb = regexp.MustCompile(
	`^(CREATE INDEX|CREATE UNIQUE INDEX|DROP INDEX|ALTER TABLE|ALTER INDEX|REINDEX|VACUUM|ANALYZE|COMMENT ON)\b`)

// defaultKeyword finds each DEFAULT clause in a flattened ADD COLUMN statement.
// A statement may add several columns, each with its own default; every one has
// to be proven constant before the whole thing is metadata-only.
var defaultKeyword = regexp.MustCompile(`\bDEFAULT\b`)

// constantDefaultValue is the ALLOWLIST of default expressions PostgreSQL 11+
// stores as a column's "missing value" instead of rewriting every existing row:
// a bare numeric, string, boolean or NULL literal (optionally signed or wrapped
// in parentheses, optionally followed by ordinary column-constraint clauses like
// NOT NULL). It is deliberately an allowlist, not a denylist of known-volatile
// functions.
//
// The reason is the one error this product cannot make. A denylist of built-in
// volatile functions (now(), random(), nextval(), ...) waves through ANY default
// it does not recognise — a user-defined function, an unlisted built-in, a
// stable expression like CURRENT_TIMESTAMP — as "constant", and on PG16 a
// non-constant default forces a full ACCESS EXCLUSIVE table rewrite. That is a
// real, minutes-long outage reported SAFE without ever running. Inverting the
// logic means the failure mode flips to the harmless direction: an expression we
// cannot PROVE constant is rehearsed (or classified volatile), never waved
// through. Being wrong toward "rewrite" costs a rehearsal; being wrong toward
// "constant" ships an outage.
//
// Note what is NOT here: CURRENT_TIMESTAMP / now() and friends are STABLE, not
// constant — PostgreSQL evaluates them per row semantics and does NOT fold them
// to a stored missing value, so they still rewrite. Only literals qualify.
var constantDefaultValue = regexp.MustCompile(
	`^[(\s]*` + // optional leading parentheses / whitespace
		`(?:` +
		`NULL` + // the NULL literal
		`|TRUE|FALSE` + // boolean literals
		`|[+-]?\s*\d+(?:\.\d+)?(?:E[+-]?\d+)?` + // numeric literal (optional sign / exponent)
		`|'(?:[^']|'')*'` + // string literal (doubled-quote aware)
		`)` +
		`[)\s]*` + // optional trailing parentheses / whitespace
		// Anything after the literal must be an ordinary column-constraint clause,
		// never an operator or cast that would continue the expression (0 + fn(),
		// 'a' || fn(), 0::sometype) — those are not provably constant.
		`(?:(?:NOT\s+NULL|NULL|CHECK|REFERENCES|UNIQUE|PRIMARY\s+KEY|GENERATED|COLLATE|CONSTRAINT|DEFERRABLE)\b.*)?$`)

// addColumnDefaultsConstant reports whether EVERY DEFAULT clause in an
// already-flattened ADD COLUMN statement is a provably-constant literal. A
// statement with no DEFAULT clause is handled by the caller, not here. Any
// default it cannot prove constant makes the whole statement a candidate rewrite,
// so the caller stops treating it as catalog-only.
func addColumnDefaultsConstant(flat string) bool {
	rest := flat
	for {
		loc := defaultKeyword.FindStringIndex(rest)
		if loc == nil {
			return true
		}
		rest = rest[loc[1]:]
		expr := columnDefaultExpr(rest)
		if !constantDefaultValue.MatchString(strings.TrimSpace(expr)) {
			return false
		}
	}
}

// columnDefaultExpr returns the default expression that begins s: everything up
// to the first top-level comma (which would start the next column definition),
// skipping commas inside string literals and parentheses so a function call's
// argument list does not truncate it. A function-call or operator default is
// therefore returned whole and fails the constant check, rather than being
// clipped to a prefix that might spuriously match.
func columnDefaultExpr(s string) string {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			for i++; i < len(s); i++ {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i++
						continue
					}
					break
				}
			}
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				return s[:i]
			}
		}
	}
	return s
}

// indexBearingColumnOption matches the ADD COLUMN options that quietly build an
// index or validate a reference, which is what turns a "metadata-only" column
// addition into a blocking one.
var indexBearingColumnOption = regexp.MustCompile(`\b(UNIQUE|PRIMARY KEY|REFERENCES)\b`)

// rewritingColumnDef matches ADD COLUMN forms that force a full table rewrite
// under ACCESS EXCLUSIVE even though they carry no literal DEFAULT keyword, so
// the PostgreSQL 11 "constant default is metadata-only" fast path does NOT
// apply to them:
//
//   - SERIAL / BIGSERIAL / SMALLSERIAL expand to a volatile nextval() default,
//     so every existing row has to be filled in.
//   - GENERATED ... AS IDENTITY is the same story with the modern spelling.
//   - GENERATED ALWAYS AS (expr) STORED computes a value per row.
//
// Miss any of these and Brace would tell a user "there is nothing here to
// crash" about a statement that freezes the table for the length of a rewrite —
// a false-"safe" on a dangerous migration, which is the one error this product
// cannot make.
var rewritingColumnDef = regexp.MustCompile(`\b(SERIAL|BIGSERIAL|SMALLSERIAL|GENERATED)\b`)

// RejectKind classifies a refusal for the UI.
type RejectKind string

const (
	// RejectRejected: Brace will not run this.
	RejectRejected RejectKind = "rejected"
	// RejectAlreadySafe: Brace COULD run this, but the statement is
	// catalog-only, so the rehearsal would draw a flat line. Saying so is more
	// useful than spending a twin to prove nothing happens.
	RejectAlreadySafe RejectKind = "already_safe"
)

// RejectedError is the typed refusal. The API maps it to 400 with
// {error:"rejected", kind, reason, suggestion, qualifier, note}.
type RejectedError struct {
	Kind           RejectKind     `json:"kind"`
	Code           string         `json:"code"`
	Reason         string         `json:"reason"`
	Suggestion     string         `json:"suggestion,omitempty"`
	Classification Classification `json:"classification,omitempty"`
	// Qualifier names what an already_safe answer did NOT establish. A
	// metadata-only shape that still takes ACCESS EXCLUSIVE — ADD COLUMN with a
	// constant default is the canonical one — is answered already_safe WITHOUT
	// running, but that lock conflicts with every other mode including plain
	// SELECT, so under a long-running transaction the change queues and every
	// query behind it queues too. A flat rehearsal would never show that wait, and
	// an unqualified SAFE would let a CI gate ship it as clean. Carrying
	// acquisition_not_simulated here is what stops that. Empty for a genuinely
	// harmless shape (COMMENT, ANALYZE) whose lock blocks nobody.
	Qualifier string `json:"qualifier,omitempty"`
	// Note is the one-line caveat that travels with Qualifier: what the untested
	// risk is, and that hostile mode measures it.
	Note string `json:"note,omitempty"`
}

func (e *RejectedError) Error() string { return "engine: rejected: " + e.Reason }

// Assessment is everything Brace can say about a statement without running it.
type Assessment struct {
	// SQL is the statement as submitted, trimmed.
	SQL string `json:"sql"`
	// Flat is the statement with comments removed, whitespace collapsed and
	// case folded — the string every pattern in this file matches against.
	Flat string `json:"-"`
	// Classification is the recognised shape.
	Classification Classification `json:"classification"`
	// Lesson is the teach entry for that shape.
	Lesson Lesson `json:"lesson"`
	// PredictedSafe is the shape-based prediction that writes keep flowing.
	// The rehearsal, not this field, decides the verdict.
	PredictedSafe bool `json:"predicted_safe"`
	// Concurrent records that the statement uses CONCURRENTLY, which is what
	// makes an INVALID index possible on failure.
	Concurrent bool `json:"concurrent"`
}

// Assess pre-filters and classifies a statement. A non-nil error is always a
// *RejectedError, so the API can map every refusal to a 400 with a reason the
// user can act on.
//
// Note what Assess does NOT do: it does not refuse statements it believes are
// safe. CREATE INDEX CONCURRENTLY is predicted safe and is still the single
// most valuable thing to rehearse — proving the safe rewrite hurts nobody is
// half the product. Only the catalog-only shapes (Lesson.TriviallySafe) are
// turned away, and even those are turned away with the reason spelled out.
func Assess(sql string) (*Assessment, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return nil, &RejectedError{
			Kind:   RejectRejected,
			Code:   "empty",
			Reason: "The console is empty. Paste a migration, or load one of the examples.",
		}
	}
	if len(trimmed) > maxSQLBytes {
		return nil, &RejectedError{
			Kind: RejectRejected,
			Code: "too_long",
			Reason: "That statement is longer than Brace accepts (limit 4 KB). Brace rehearses one migration " +
				"statement at a time — split the script and run the interesting statement on its own.",
		}
	}

	segments := splitStatements(trimmed)
	if len(segments) > 1 {
		return nil, &RejectedError{
			Kind: RejectRejected,
			Code: "multi_statement",
			Reason: "Brace rehearses exactly one statement per run, because the whole output — the freeze window, " +
				"the casualty count — is attributed to a single statement. Submit them one at a time.",
		}
	}

	flat := flatten(segments[0])
	if flat == "" {
		return nil, &RejectedError{
			Kind:   RejectRejected,
			Code:   "empty",
			Reason: "That is a comment, not a statement. Paste the migration itself.",
		}
	}
	if !allowedLeadingVerb.MatchString(flat) {
		return nil, &RejectedError{
			Kind: RejectRejected,
			Code: "not_allowlisted",
			Reason: "Brace rehearses schema changes: CREATE INDEX, DROP INDEX, ALTER TABLE, ALTER INDEX, REINDEX, " +
				"VACUUM, ANALYZE and COMMENT ON. That statement is not one of them, so there is no lock story to tell.",
			Suggestion: "CREATE INDEX idx_orders_customer_id ON orders (customer_id);",
		}
	}

	class := classify(flat)
	lesson := Explain(class)
	as := &Assessment{
		SQL:            trimmed,
		Flat:           flat,
		Classification: class,
		Lesson:         lesson,
		PredictedSafe:  !lesson.BlocksWrites,
		Concurrent:     strings.Contains(flat, "CONCURRENTLY"),
	}

	if lesson.TriviallySafe {
		rej := &RejectedError{
			Kind:           RejectAlreadySafe,
			Code:           string(class),
			Reason:         lesson.Explanation,
			Suggestion:     firstNonEmpty(lesson.Suggestion, "CREATE INDEX idx_orders_customer_id ON orders (customer_id);"),
			Classification: class,
		}
		// An already_safe shape that still takes ACCESS EXCLUSIVE (metadata-only ADD
		// COLUMN with a constant default) is safe about the WORK but not about the
		// WAIT: on an idle table it gets the lock instantly, but in production it
		// queues behind the longest open transaction and everything queues behind it.
		// A default run does not simulate that, so the already_safe answer must carry
		// the same acquisition_not_simulated qualifier a measured run would — never a
		// bare, unconditional SAFE — and point the caller at hostile mode to measure
		// the wait. A shape whose lock blocks nobody (COMMENT, ANALYZE) needs none of
		// this.
		if lesson.ExpectedLock == "AccessExclusiveLock" {
			rej.Qualifier = QualifierAcquisitionNotSimulated
			rej.Note = "This change is catalog-only, but it still takes ACCESS EXCLUSIVE, which conflicts with every " +
				"other lock mode including plain SELECT. On an idle table it acquires that lock instantly; in production " +
				"it waits behind your longest open transaction, and because PostgreSQL grants locks in arrival order, " +
				"every query that arrives after it waits too — the table stops until that transaction ends. This run did " +
				"not simulate that wait. Re-run with hostile mode (hostile: true, or BRACE_HOSTILE=on) to measure it."
		}
		return as, rej
	}
	return as, nil
}

// classify recognises the shape of an already-flattened, already-allowlisted
// statement. Order matters throughout: the CONCURRENTLY forms are checked
// before their blocking counterparts, and the specific ALTER TABLE forms before
// the catch-all.
func classify(flat string) Classification {
	concurrent := strings.Contains(flat, "CONCURRENTLY")

	switch {
	case strings.HasPrefix(flat, "CREATE INDEX"), strings.HasPrefix(flat, "CREATE UNIQUE INDEX"):
		if concurrent {
			return ClassCreateIndexConcurrently
		}
		return ClassCreateIndex

	case strings.HasPrefix(flat, "DROP INDEX"):
		if concurrent {
			return ClassDropIndexConcurrently
		}
		return ClassDropIndex

	case strings.HasPrefix(flat, "REINDEX"):
		if concurrent {
			return ClassReindexConcurrently
		}
		return ClassReindex

	case strings.HasPrefix(flat, "VACUUM"):
		if strings.Contains(flat, "FULL") {
			return ClassVacuumFull
		}
		return ClassVacuum

	case strings.HasPrefix(flat, "ANALYZE"):
		return ClassAnalyze

	case strings.HasPrefix(flat, "COMMENT ON"):
		return ClassComment

	case strings.HasPrefix(flat, "ALTER INDEX"):
		// RENAME is the common, benign form: on PG12+ it takes only SHARE UPDATE
		// EXCLUSIVE and does not conflict with writes. Every other ALTER INDEX form
		// (SET TABLESPACE moves the index files; others are treated conservatively)
		// takes ACCESS EXCLUSIVE, so they get the blocking lesson.
		if strings.Contains(flat, "RENAME") {
			return ClassAlterIndex
		}
		return ClassAlterIndexRewrite

	case strings.HasPrefix(flat, "ALTER TABLE"):
		return classifyAlterTable(flat)
	}
	return ClassGeneric
}

func classifyAlterTable(flat string) Classification {
	switch {
	case strings.Contains(flat, "VALIDATE CONSTRAINT"):
		return ClassValidateConstraint

	case strings.Contains(flat, "ALTER COLUMN") && strings.Contains(flat, " TYPE "),
		strings.Contains(flat, "ALTER COLUMN") && strings.Contains(flat, " SET DATA TYPE "):
		return ClassAlterColumnType

	case strings.Contains(flat, "SET NOT NULL"):
		return ClassSetNotNull

	case strings.Contains(flat, "ADD COLUMN"):
		// A column that carries UNIQUE, PRIMARY KEY or REFERENCES is not a
		// catalog-only change at all — it builds an index or validates a
		// reference under the same ACCESS EXCLUSIVE lock.
		if indexBearingColumnOption.MatchString(flat) {
			return ClassAddConstraint
		}
		// SERIAL / BIGSERIAL / SMALLSERIAL and GENERATED ... AS IDENTITY / STORED
		// carry an implicit per-row computation with no literal DEFAULT keyword.
		if rewritingColumnDef.MatchString(flat) {
			return ClassAddColumnVolatile
		}
		// A DEFAULT is metadata-only ONLY when it is a provably-constant literal;
		// PG11+ stores that as the column's "missing value". Anything else — a
		// function call (built-in OR user-defined), a stable expression like
		// CURRENT_TIMESTAMP, any operator expression — forces a full ACCESS
		// EXCLUSIVE rewrite, so it is classified volatile and REHEARSED, never
		// short-circuited to already_safe. addColumnDefaultsConstant is an
		// allowlist: an unrecognised default is treated as a rewrite, because a
		// false "constant" here is the flagship false-SAFE this product exists to
		// prevent.
		if strings.Contains(flat, "DEFAULT") && !addColumnDefaultsConstant(flat) {
			return ClassAddColumnVolatile
		}
		// A constant default, or no default at all: metadata-only since PG11.
		return ClassAddColumnConstDefault

	case strings.Contains(flat, "ADD CONSTRAINT"),
		strings.Contains(flat, "ADD FOREIGN KEY"),
		strings.Contains(flat, "ADD PRIMARY KEY"),
		strings.Contains(flat, "ADD UNIQUE"),
		strings.Contains(flat, "ADD CHECK"):
		if strings.Contains(flat, "NOT VALID") {
			return ClassAddConstraintNotValid
		}
		return ClassAddConstraint
	}
	// Catalog-only ALTER TABLE forms: they take ACCESS EXCLUSIVE but do no O(rows)
	// work, so their cost is constant at any table size. Splitting them out of the
	// generic ClassAlterTableOther (which is marked O(rows)) is what stops the
	// scale layer from projecting a phantom multi-second freeze from a 250ms
	// resolution floor and shipping every clean DROP COLUMN / RENAME / SET DEFAULT
	// as effective-UNSAFE. Forms that DO rewrite (SET LOGGED / SET UNLOGGED, SET
	// ACCESS METHOD) are deliberately NOT here — they stay in ClassAlterTableOther
	// and keep scaling.
	if isConstantTimeAlterTable(flat) {
		return ClassAlterTableConstantTime
	}
	return ClassAlterTableOther
}

// isConstantTimeAlterTable recognises the ALTER TABLE sub-forms that are
// catalog-only: they change metadata under ACCESS EXCLUSIVE without touching or
// rewriting any row, so their duration does not grow with the table. This is an
// allowlist — an unrecognised ALTER TABLE stays ClassAlterTableOther, where the
// rehearsal (and, above the floor, the scale projection) measures it rather than
// assuming it is free.
func isConstantTimeAlterTable(flat string) bool {
	// A rewrite keyword anywhere disqualifies the whole statement, even if it also
	// contains a catalog-only clause.
	if strings.Contains(flat, "SET LOGGED") || strings.Contains(flat, "SET UNLOGGED") ||
		strings.Contains(flat, "SET ACCESS METHOD") || strings.Contains(flat, "SET TABLESPACE") {
		return false
	}
	switch {
	case strings.Contains(flat, "DROP COLUMN"),
		strings.Contains(flat, "RENAME"), // RENAME COLUMN / CONSTRAINT / TO
		strings.Contains(flat, "SET DEFAULT"),
		strings.Contains(flat, "DROP DEFAULT"),
		strings.Contains(flat, "DROP NOT NULL"),
		strings.Contains(flat, "SET STORAGE"),
		strings.Contains(flat, "SET STATISTICS"),
		strings.Contains(flat, "SET COMPRESSION"),
		strings.Contains(flat, "ALTER CONSTRAINT"),
		strings.Contains(flat, "CLUSTER ON"),
		strings.Contains(flat, "SET WITHOUT CLUSTER"),
		strings.Contains(flat, "OWNER TO"):
		return true
	}
	return false
}

// splitStatements returns the non-blank statements in sql, splitting on
// semicolons that are not inside a string literal, a quoted identifier, a
// dollar-quoted body, or a comment.
//
// This is the "naive check" done properly. Naive in the sense that matters —
// it does not parse SQL, and a determined user can certainly confuse it — but
// not so naive that a trailing `;` or an apostrophe in a COMMENT ON string
// produces a baffling rejection. Comments are replaced with a space rather than
// removed so that `CREATE/**/INDEX` cannot be glued into a different token.
func splitStatements(sql string) []string {
	var (
		out []string
		cur strings.Builder
	)
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}

	n := len(sql)
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == '\'':
			// An E'' string treats backslash as an escape; a plain one does not
			// (standard_conforming_strings has been on by default since 9.1).
			escapes := i > 0 && (sql[i-1] == 'E' || sql[i-1] == 'e') &&
				(i == 1 || !isIdentChar(sql[i-2]))
			j := scanQuoted(sql, i, '\'', escapes)
			cur.WriteString(sql[i:j])
			i = j

		case c == '"':
			j := scanQuoted(sql, i, '"', false)
			cur.WriteString(sql[i:j])
			i = j

		case c == '-' && i+1 < n && sql[i+1] == '-':
			j := i
			for j < n && sql[j] != '\n' {
				j++
			}
			cur.WriteByte(' ')
			i = j

		case c == '/' && i+1 < n && sql[i+1] == '*':
			// Block comments nest in PostgreSQL.
			depth, j := 1, i+2
			for j < n && depth > 0 {
				if sql[j] == '/' && j+1 < n && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if sql[j] == '*' && j+1 < n && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			cur.WriteByte(' ')
			i = j

		case c == '$':
			if end, ok := scanDollarQuoted(sql, i); ok {
				cur.WriteString(sql[i:end])
				i = end
				continue
			}
			cur.WriteByte(c)
			i++

		case c == ';':
			flush()
			i++

		default:
			cur.WriteByte(c)
			i++
		}
	}
	flush()
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

// scanQuoted returns the index just past the closing quote, treating a doubled
// quote as an escaped one. An unterminated literal consumes to end of input;
// the server will reject it, and guessing where it "should" have ended would be
// worse than passing it through.
func scanQuoted(s string, start int, q byte, backslashEscapes bool) int {
	n := len(s)
	for i := start + 1; i < n; i++ {
		if backslashEscapes && s[i] == '\\' {
			i++
			continue
		}
		if s[i] == q {
			if i+1 < n && s[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return n
}

// scanDollarQuoted handles $$body$$ and $tag$body$tag$. Returns false when the
// '$' is not opening a dollar-quoted string (a positional parameter, say).
func scanDollarQuoted(s string, start int) (int, bool) {
	n := len(s)
	i := start + 1
	for i < n && isIdentChar(s[i]) {
		// A tag is letters, digits and underscore, but may not START with a
		// digit — `$1` is a positional parameter, not an opening tag.
		if i == start+1 && isDigit(s[i]) {
			return 0, false
		}
		i++
	}
	if i >= n || s[i] != '$' {
		return 0, false
	}
	tag := s[start : i+1] // includes both '$'
	rest := s[i+1:]
	idx := strings.Index(rest, tag)
	if idx < 0 {
		return n, true // unterminated: consume the remainder
	}
	return i + 1 + idx + len(tag), true
}

func isIdentChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || isDigit(c)
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// flatten collapses whitespace and folds case, producing the canonical string
// every pattern here matches. Case folding the whole statement (literals
// included) is fine: nothing in this file depends on a literal's contents.
func flatten(stmt string) string {
	return strings.Join(strings.Fields(strings.ToUpper(stmt)), " ")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
