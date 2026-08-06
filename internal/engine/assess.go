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

// volatileDefault matches the default expressions that force a full table
// rewrite on ADD COLUMN, because they must produce a different value per row.
var volatileDefault = regexp.MustCompile(
	`DEFAULT\s*\(?\s*(NOW\s*\(|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|LOCALTIME|LOCALTIMESTAMP|CLOCK_TIMESTAMP|STATEMENT_TIMESTAMP|TIMEOFDAY|RANDOM\s*\(|GEN_RANDOM_UUID|UUID_GENERATE|NEXTVAL)`)

// indexBearingColumnOption matches the ADD COLUMN options that quietly build an
// index or validate a reference, which is what turns a "metadata-only" column
// addition into a blocking one.
var indexBearingColumnOption = regexp.MustCompile(`\b(UNIQUE|PRIMARY KEY|REFERENCES)\b`)

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
// {error:"rejected", kind, reason, suggestion}.
type RejectedError struct {
	Kind           RejectKind     `json:"kind"`
	Code           string         `json:"code"`
	Reason         string         `json:"reason"`
	Suggestion     string         `json:"suggestion,omitempty"`
	Classification Classification `json:"classification,omitempty"`
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
		return as, &RejectedError{
			Kind:           RejectAlreadySafe,
			Code:           string(class),
			Reason:         lesson.Explanation,
			Suggestion:     firstNonEmpty(lesson.Suggestion, "CREATE INDEX idx_orders_customer_id ON orders (customer_id);"),
			Classification: class,
		}
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
		return ClassAlterIndex

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
		if volatileDefault.MatchString(flat) {
			return ClassAddColumnVolatile
		}
		// Constant default, or no default: metadata-only since PG11.
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
	return ClassAlterTableOther
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
