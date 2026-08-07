package boot

import (
	"strings"
	"testing"
)

// TestParseLabels is the whole BYOD config contract in one table. Every case
// here is a thing a self-hoster will actually type into an environment
// variable, and the failures matter as much as the successes: a bad triple that
// boots anyway becomes a run whose casualty count is silently zero.
func TestParseLabels(t *testing.T) {
	type want struct {
		name   string
		weight int
		stmt   string
	}
	tests := []struct {
		name    string
		spec    string
		want    []want
		wantErr string // substring; empty means "must parse"
	}{
		{
			name: "unset falls back to defaults",
			spec: "",
			want: nil,
		},
		{
			name: "whitespace only falls back to defaults",
			spec: "   \n\t ",
			want: nil,
		},
		{
			name: "single triple",
			spec: `checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2)`,
			want: []want{{"checkout", 3, `INSERT INTO invoices (customer_id, cents) VALUES ($1, $2)`}},
		},
		{
			name: "two triples",
			spec: `checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2);` +
				`login:2:UPDATE sessions SET last_seen = now() WHERE id = $1 % 50 AND $2 > 0`,
			want: []want{
				{"checkout", 3, `INSERT INTO invoices (customer_id, cents) VALUES ($1, $2)`},
				{"login", 2, `UPDATE sessions SET last_seen = now() WHERE id = $1 % 50 AND $2 > 0`},
			},
		},
		{
			name: "only the first two colons are structural",
			spec: `cast:1:UPDATE t SET seen = now()::timestamptz, tag = 'a:b:c' WHERE id = $1 AND $2 > 0`,
			want: []want{{"cast", 1, `UPDATE t SET seen = now()::timestamptz, tag = 'a:b:c' WHERE id = $1 AND $2 > 0`}},
		},
		{
			name: "surrounding whitespace is trimmed",
			spec: "  checkout : 3 : INSERT INTO t (a, b) VALUES ($1, $2)  ",
			want: []want{{"checkout", 3, `INSERT INTO t (a, b) VALUES ($1, $2)`}},
		},
		{
			name: "trailing separator is tolerated",
			spec: `a:1:INSERT INTO t (a, b) VALUES ($1, $2);`,
			want: []want{{"a", 1, `INSERT INTO t (a, b) VALUES ($1, $2)`}},
		},
		{
			name:    "missing statement field",
			spec:    `checkout:3`,
			wantErr: "three colon-separated fields",
		},
		{
			name:    "no colons at all",
			spec:    `INSERT INTO t VALUES ($1, $2)`,
			wantErr: "three colon-separated fields",
		},
		{
			name:    "empty name",
			spec:    `:3:INSERT INTO t (a, b) VALUES ($1, $2)`,
			wantErr: "label name is empty",
		},
		{
			name:    "name with whitespace",
			spec:    `add to cart:3:INSERT INTO t (a, b) VALUES ($1, $2)`,
			wantErr: "contains whitespace",
		},
		{
			name: "duplicate name",
			spec: `a:1:INSERT INTO t (a, b) VALUES ($1, $2);a:2:INSERT INTO t (a, b) VALUES ($1, $2)`,
			// loadgen would reject this too, but only after a twin has been
			// cloned; catching it here costs nothing.
			wantErr: "defined twice",
		},
		{
			name:    "non-numeric weight",
			spec:    `checkout:heavy:INSERT INTO t (a, b) VALUES ($1, $2)`,
			wantErr: `weight "heavy" is not a number`,
		},
		{
			name:    "zero weight",
			spec:    `checkout:0:INSERT INTO t (a, b) VALUES ($1, $2)`,
			wantErr: "must be greater than 0",
		},
		{
			name:    "negative weight",
			spec:    `checkout:-1:INSERT INTO t (a, b) VALUES ($1, $2)`,
			wantErr: "must be greater than 0",
		},
		{
			name:    "empty statement",
			spec:    `checkout:3:`,
			wantErr: "statement is empty",
		},
		{
			name:    "statement without any placeholder",
			spec:    `checkout:3:INSERT INTO t (a, b) VALUES (1, 2)`,
			wantErr: "does not use $1",
		},
		{
			// The load generator binds two bigints unconditionally, and
			// PostgreSQL rejects a bind with more parameters than the statement
			// declares. A $1-only statement would fail on EVERY write.
			name:    "statement using $1 but not $2",
			spec:    `login:2:UPDATE sessions SET last_seen = now() WHERE id = $1`,
			wantErr: "uses $1 but not $2",
		},
		{
			name:    "statement using $3",
			spec:    `x:1:INSERT INTO t (a, b, c) VALUES ($1, $2, $3)`,
			wantErr: "uses $3",
		},
		{
			name:    "set but empty after separators",
			spec:    `;;;`,
			wantErr: "contains no labels",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLabels(tc.spec)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseLabels(%q) = %+v, want error containing %q", tc.spec, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseLabels(%q) error = %v, want it to contain %q", tc.spec, err, tc.wantErr)
				}
				// Every rejection must name the offending triple, or the
				// operator has to guess which one of six is wrong.
				if !strings.Contains(err.Error(), "BRACE_LOAD_LABELS") {
					t.Errorf("error does not mention the variable: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLabels(%q) unexpected error: %v", tc.spec, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParseLabels(%q) returned %d labels, want %d: %+v", tc.spec, len(got), len(tc.want), got)
			}
			for i, w := range tc.want {
				if got[i].Name != w.name || got[i].Weight != w.weight || got[i].Statement != w.stmt {
					t.Errorf("label[%d] = %+v, want {%s %d %s}", i, got[i], w.name, w.weight, w.stmt)
				}
			}
		})
	}
}

// TestParseLabelsErrorQuotesTriple: the error has to be actionable from a
// deploy log with no other context.
func TestParseLabelsErrorQuotesTriple(t *testing.T) {
	_, err := ParseLabels(`ok:1:INSERT INTO t (a, b) VALUES ($1, $2);broken:zero:SELECT $1, $2`)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "broken:zero:SELECT $1, $2") {
		t.Fatalf("error does not quote the bad triple: %v", err)
	}
	if strings.Contains(err.Error(), "ok:1:") {
		t.Fatalf("error blames the good triple too: %v", err)
	}
}

func TestSeedDisabled(t *testing.T) {
	tests := []struct {
		name      string
		braceSeed string
		seedRows  string
		wantOff   bool
		wantErr   bool
	}{
		{name: "both unset seeds", wantOff: false},
		{name: "BRACE_SEED=off", braceSeed: "off", wantOff: true},
		{name: "BRACE_SEED=OFF is case-insensitive", braceSeed: "OFF", wantOff: true},
		{name: "BRACE_SEED=false", braceSeed: "false", wantOff: true},
		{name: "BRACE_SEED=0", braceSeed: "0", wantOff: true},
		{name: "BRACE_SEED=no", braceSeed: "no", wantOff: true},
		{name: "BRACE_SEED= off with spaces", braceSeed: "  off  ", wantOff: true},
		{name: "BRACE_SEED=on", braceSeed: "on", wantOff: false},
		{name: "BRACE_SEED=true", braceSeed: "true", wantOff: false},
		{name: "SEED_ROWS=0", seedRows: "0", wantOff: true},
		{name: "SEED_ROWS=-1", seedRows: "-1", wantOff: true},
		{name: "SEED_ROWS=1000 seeds", seedRows: "1000", wantOff: false},
		{
			// The explicit switch wins over the row count, in both directions.
			name: "BRACE_SEED=on beats SEED_ROWS=0", braceSeed: "on", seedRows: "0", wantOff: false,
		},
		{name: "BRACE_SEED=off beats SEED_ROWS=1000000", braceSeed: "off", seedRows: "1000000", wantOff: true},
		{
			// Garbage in SEED_ROWS keeps the pre-existing lenient behaviour
			// (fall back to the default row count); only BRACE_SEED is strict.
			name: "unparseable SEED_ROWS still seeds", seedRows: "lots", wantOff: false,
		},
		{name: "unknown BRACE_SEED is fatal", braceSeed: "maybe", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			off, err := seedDisabled(tc.braceSeed, tc.seedRows)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("seedDisabled(%q, %q) = %v, want an error", tc.braceSeed, tc.seedRows, off)
				}
				return
			}
			if err != nil {
				t.Fatalf("seedDisabled(%q, %q) unexpected error: %v", tc.braceSeed, tc.seedRows, err)
			}
			if off != tc.wantOff {
				t.Fatalf("seedDisabled(%q, %q) = %v, want %v", tc.braceSeed, tc.seedRows, off, tc.wantOff)
			}
		})
	}
}

// TestFromEnvBYOD covers the wiring between the environment and Config, which
// is the part a self-hoster interacts with.
func TestFromEnvBYOD(t *testing.T) {
	setEnv := func(t *testing.T, kv map[string]string) {
		t.Helper()
		// Every variable this test cares about is set explicitly, so a
		// developer's own shell cannot change the outcome.
		for _, k := range []string{"BRACE_SEED", "SEED_ROWS", "BRACE_LOAD_LABELS", "POOL_SIZE", "WORKERS"} {
			t.Setenv(k, "")
		}
		t.Setenv("BRACE_ADMIN_URL", "postgres://postgres@localhost:5433/postgres")
		for k, v := range kv {
			t.Setenv(k, v)
		}
	}

	t.Run("defaults seed", func(t *testing.T) {
		setEnv(t, nil)
		cfg, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Seed {
			t.Error("Seed = false, want true by default")
		}
		if cfg.SeedRows != 10_000_000 {
			t.Errorf("SeedRows = %d, want 10000000", cfg.SeedRows)
		}
		if len(cfg.Labels) != 0 {
			t.Errorf("Labels = %+v, want nil (loadgen supplies the defaults)", cfg.Labels)
		}
		if cfg.BYOD() {
			t.Error("BYOD() = true for a stock configuration")
		}
	})

	t.Run("BRACE_SEED=off", func(t *testing.T) {
		setEnv(t, map[string]string{"BRACE_SEED": "off"})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Seed {
			t.Error("Seed = true, want false")
		}
		// The 1000-row floor must not resurrect seeding.
		if cfg.SeedRows != 0 {
			t.Errorf("SeedRows = %d, want 0 when seeding is off", cfg.SeedRows)
		}
		if !cfg.BYOD() {
			t.Error("BYOD() = false")
		}
	})

	t.Run("SEED_ROWS=0", func(t *testing.T) {
		setEnv(t, map[string]string{"SEED_ROWS": "0"})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Seed {
			t.Error("Seed = true for SEED_ROWS=0, want false")
		}
		if cfg.SeedRows != 0 {
			t.Errorf("SeedRows = %d, want 0", cfg.SeedRows)
		}
	})

	t.Run("custom labels", func(t *testing.T) {
		setEnv(t, map[string]string{
			"BRACE_SEED":        "off",
			"BRACE_LOAD_LABELS": `checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2)`,
		})
		cfg, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.Labels) != 1 || cfg.Labels[0].Name != "checkout" || cfg.Labels[0].Weight != 3 {
			t.Fatalf("Labels = %+v", cfg.Labels)
		}
	})

	t.Run("bad labels fail the boot", func(t *testing.T) {
		setEnv(t, map[string]string{"BRACE_LOAD_LABELS": `checkout:3:INSERT INTO t (a) VALUES ($1)`})
		if _, err := FromEnv(); err == nil {
			t.Fatal("FromEnv accepted a statement that cannot bind two parameters")
		}
	})

	t.Run("bad BRACE_SEED fails the boot", func(t *testing.T) {
		setEnv(t, map[string]string{"BRACE_SEED": "sometimes"})
		if _, err := FromEnv(); err == nil {
			t.Fatal("FromEnv accepted BRACE_SEED=sometimes")
		}
	})
}
