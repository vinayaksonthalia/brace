// brace-check — the deploy gate.
//
// Run your migration through a Brace crash-test lab from CI (or anywhere):
//
//	brace-check "CREATE INDEX idx_orders_email ON orders (email);"
//	brace-check -f migrations/0042_add_index.sql
//
// Exit codes: 0 = SAFE · 1 = UNSAFE (block the deploy) · 2 = rejected/error/inconclusive.
// The lab measures the migration on a twin under live labeled write traffic;
// UNSAFE means it froze writes for >1s and real writes died.
//
// The twin is a SAMPLE. Lock presence is scale-free but lock duration is not, so
// a statement measured at 0.2s against a ten-million-row twin can be a ten-second
// outage against a five-hundred-million-row production table. Tell the lab how
// big your table really is and the gate will fail on the PROJECTED outage too:
//
//	brace-check -target-rows 500000000 -f migrations/0042_add_index.sql
//
// The exit code follows effective_verdict — measured OR projected — because a CI
// gate that only fails on what a sample happened to show is the false-safe this
// tool exists to prevent.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type scaleProjection struct {
	TwinRows        int64   `json:"twin_rows"`
	TargetRows      int64   `json:"target_rows"`
	Measured        float64 `json:"measured_blocked_seconds"`
	Projected       float64 `json:"projected_blocked_seconds"`
	MeasuredAtFloor bool    `json:"measured_at_floor"`
	Model           string  `json:"model"`
	Verdict         string  `json:"verdict"`
}

type runResult struct {
	RunID          string  `json:"run_id"`
	Verdict        string  `json:"verdict"`
	BlockedSeconds float64 `json:"blocked_seconds"`
	Duration       float64 `json:"duration_seconds"`
	Casualties     struct {
		Total   int64            `json:"total"`
		ByLabel map[string]int64 `json:"by_label"`
	} `json:"casualties"`
	BaselineWPS int64    `json:"baseline_writes_per_sec"`
	LockModes   []string `json:"lock_modes"`
	Explanation string   `json:"explanation"`
	Suggestion  string   `json:"suggestion"`
	Error       string   `json:"error"`

	// EffectiveVerdict is measured-or-projected, and it is what the exit code
	// follows. Older labs do not send it; gateVerdict falls back to Verdict so
	// this binary keeps working against them rather than failing open on "".
	EffectiveVerdict string          `json:"effective_verdict"`
	ScaleProjection  scaleProjection `json:"scale_projection"`
	ScaleWarning     string          `json:"scale_warning"`
}

// gateVerdict is the single place the exit code is decided.
func (r *runResult) gateVerdict() string {
	if r.EffectiveVerdict != "" {
		return r.EffectiveVerdict
	}
	return r.Verdict
}

func main() {
	api := flag.String("api", envOr("BRACE_API", "https://api-2a61-8080.prg1.zerops.app"), "Brace lab API base URL")
	file := flag.String("f", "", "read migration SQL from file")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall wait budget")
	targetRows := flag.Int64("target-rows", envInt64("BRACE_TARGET_ROWS", 0),
		"rows in YOUR production table; the freeze measured on the twin is projected onto it and the gate fails on the projection")
	flag.Parse()

	sql := strings.TrimSpace(strings.Join(flag.Args(), " "))
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			fatal(2, "reading %s: %v", *file, err)
		}
		sql = strings.TrimSpace(string(b))
	}
	if sql == "" {
		fatal(2, "usage: brace-check [flags] \"<migration sql>\"  (or -f file.sql)")
	}

	fmt.Printf("brace-check · lab %s\n", *api)
	fmt.Printf("specimen     · %s\n", oneline(sql))

	if *targetRows > 0 {
		fmt.Printf("your table   · %d rows (freeze will be projected onto this)\n", *targetRows)
	}

	req := map[string]any{"sql": sql, "mode": "custom"}
	if *targetRows > 0 {
		req["target_rows"] = *targetRows
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(*api+"/api/run", "application/json", bytes.NewReader(body))
	if err != nil {
		fatal(2, "lab unreachable: %v", err)
	}
	defer resp.Body.Close()

	var submit struct {
		RunID      string  `json:"run_id"`
		Error      string  `json:"error"`
		Kind       string  `json:"kind"`
		Reason     string  `json:"reason"`
		Suggestion string  `json:"suggestion"`
		ETASeconds float64 `json:"eta_seconds"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&submit)

	switch resp.StatusCode {
	case http.StatusAccepted: // 202 — crash test running
	case http.StatusBadRequest:
		if submit.Kind == "already_safe" {
			fmt.Printf("verdict      · SAFE (statically: %s)\n", submit.Reason)
			os.Exit(0)
		}
		fatal(2, "rejected: %s%s", submit.Reason, suffixIf("; try: ", submit.Suggestion))
	case http.StatusConflict:
		fatal(2, "lab busy: another crash test is running — retry shortly")
	case http.StatusServiceUnavailable:
		fatal(2, "no warm twin (next dummy in ~%.0fs) — retry shortly", submit.ETASeconds)
	default:
		fatal(2, "lab error: HTTP %d %s", resp.StatusCode, submit.Error)
	}

	fmt.Printf("run          · %s (crash test in progress…)\n", submit.RunID)

	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)
		sr, err := http.Get(*api + "/api/stats")
		if err != nil {
			continue
		}
		var stats struct {
			LastRun *runResult `json:"last_run"`
		}
		_ = json.NewDecoder(sr.Body).Decode(&stats)
		sr.Body.Close()
		lr := stats.LastRun
		if lr == nil || lr.RunID != submit.RunID {
			continue
		}
		report(lr)
		switch lr.gateVerdict() {
		case "SAFE":
			os.Exit(0)
		case "UNSAFE":
			os.Exit(1)
		default: // INCONCLUSIVE or error
			os.Exit(2)
		}
	}
	fatal(2, "timed out waiting for the crash test")
}

func report(r *runResult) {
	fmt.Printf("verdict      · %s (measured on the twin)\n", r.Verdict)
	fmt.Printf("write freeze · %.2fs (measured)\n", r.BlockedSeconds)
	if p := r.ScaleProjection; p.Verdict != "" {
		at := ""
		if p.MeasuredAtFloor {
			// Never print a projection tighter than the instrument: below one
			// 250ms sample the measurement is an upper bound, and so is anything
			// scaled from it.
			at = "at most "
		}
		fmt.Printf("projection   · %s%.1fs at %d rows (%dx this twin's %d) · %s\n",
			at, p.Projected, p.TargetRows, ratio(p.TwinRows, p.TargetRows), p.TwinRows, p.Model)
	} else if r.ScaleProjection.TargetRows == 0 {
		fmt.Printf("projection   · none — pass -target-rows to project this onto your real table\n")
	}
	if r.ScaleWarning != "" {
		fmt.Printf("SCALE WARNING· %s\n", r.ScaleWarning)
	}
	if ev := r.gateVerdict(); ev != r.Verdict {
		fmt.Printf("effective    · %s (gate follows this)\n", ev)
	}
	fmt.Printf("casualties   · %d writes died", r.Casualties.Total)
	if len(r.Casualties.ByLabel) > 0 {
		parts := make([]string, 0, 3)
		for k, v := range r.Casualties.ByLabel {
			parts = append(parts, fmt.Sprintf("%s %d", k, v))
		}
		fmt.Printf(" (%s)", strings.Join(parts, " · "))
	}
	fmt.Printf("\nbaseline     · %d writes/sec · locks %s\n", r.BaselineWPS, strings.Join(r.LockModes, "+"))
	if r.Error != "" {
		fmt.Printf("error        · %s\n", r.Error)
	}
	if r.gateVerdict() == "UNSAFE" && r.Suggestion != "" {
		fmt.Printf("try instead  · %s\n", r.Suggestion)
	}
}

func oneline(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 88 {
		s = s[:85] + "…"
	}
	return s
}

func ratio(twinRows, targetRows int64) int64 {
	if twinRows <= 0 {
		return 0
	}
	return int64(float64(targetRows)/float64(twinRows) + 0.5)
}

func envInt64(k string, d int64) int64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return d
	}
	return n
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func suffixIf(p, s string) string {
	if s == "" {
		return ""
	}
	return p + s
}

func fatal(code int, f string, a ...any) {
	fmt.Fprintf(os.Stderr, "brace-check: "+f+"\n", a...)
	os.Exit(code)
}
