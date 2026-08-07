// brace-check — the deploy gate.
//
// Run your migration through a Brace crash-test lab from CI (or anywhere):
//
//	brace-check "CREATE INDEX idx_orders_email ON orders (email);"
//	brace-check -f migrations/0042_add_index.sql
//
// Exit codes: 0 = SAFE · 1 = UNSAFE (block the deploy) · 2 = rejected/error/inconclusive.
// The lab measures the migration on a production-scale twin under live labeled
// write traffic; UNSAFE means it froze writes for >1s and real writes died.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

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
}

func main() {
	api := flag.String("api", envOr("BRACE_API", "https://api-2a61-8080.prg1.zerops.app"), "Brace lab API base URL")
	file := flag.String("f", "", "read migration SQL from file")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall wait budget")
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

	body, _ := json.Marshal(map[string]string{"sql": sql, "mode": "custom"})
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
		switch lr.Verdict {
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
	fmt.Printf("verdict      · %s\n", r.Verdict)
	fmt.Printf("write freeze · %.2fs (measured)\n", r.BlockedSeconds)
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
	if r.Verdict == "UNSAFE" && r.Suggestion != "" {
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
