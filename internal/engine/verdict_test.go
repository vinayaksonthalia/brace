package engine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/monitor"
)

// timeline builds a synthetic trace: one point every 250ms, taking the write
// rate from wps in order.
func timeline(wps ...int64) []RatePoint {
	out := make([]RatePoint, 0, len(wps))
	for i, w := range wps {
		out = append(out, RatePoint{T: roundSeconds(float64(i+1) * 0.25), WPS: w})
	}
	return out
}

func TestLongestBlockedWindow(t *testing.T) {
	cases := []struct {
		name      string
		points    []RatePoint
		threshold float64
		want      float64
	}{
		{"steady traffic", timeline(800, 810, 790, 805, 800), 120, 0},
		{"one sample dip", timeline(800, 0, 800, 800), 120, 0.25},
		{"one second freeze", timeline(800, 0, 0, 0, 0, 800, 800), 120, 1.0},
		{"freeze at the end", timeline(800, 800, 0, 0, 0), 120, 0.75},
		// Two separate dips are NOT one long outage. This is the anti-
		// exaggeration invariant: cumulative counting would report 1.0s here.
		{"two separate dips", timeline(800, 0, 0, 800, 0, 0, 800), 120, 0.5},
		{"empty", nil, 120, 0},
		{"single point", timeline(0), 120, 0},
		{"no threshold", timeline(0, 0, 0, 0), 0, 0},
		// A shallow dip — an index build competing for I/O — is not a freeze.
		{"shallow dip is not a freeze", timeline(800, 700, 690, 720, 800), 120, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := longestBlockedWindow(c.points, c.threshold); got != c.want {
				t.Errorf("longestBlockedWindow = %v, want %v", got, c.want)
			}
		})
	}
}

// TestBlockedSecondsIgnoresPreDDLSamples: the load generator's own warm-up must
// never be charged to the migration.
func TestBlockedSecondsIgnoresPreDDLSamples(t *testing.T) {
	// Samples at 0.25 .. 2.00. The first four are the pool warming up (rate 0);
	// the DDL starts at 1.0 and everything after it is healthy.
	pts := timeline(0, 0, 0, 800, 800, 800, 800, 800)
	if got := blockedSecondsFrom(pts, 1.0, 800); got != 0 {
		t.Errorf("warm-up before the DDL was charged to the migration: %v", got)
	}

	// Same shape, but the collapse is after the DDL starts.
	pts = timeline(800, 800, 800, 0, 0, 0, 0, 800)
	if got := blockedSecondsFrom(pts, 0.75, 800); got != 1.0 {
		t.Errorf("blocked = %v, want 1.0", got)
	}
}

// TestBlockedSecondsCatchesTheFirstFrozenSample: the sample straight after the
// DDL begins must be measurable, which requires reaching back one sample for
// its interval. Without that, the first 250ms of every freeze is invisible.
func TestBlockedSecondsCatchesTheFirstFrozenSample(t *testing.T) {
	pts := timeline(800, 800, 0, 0, 800)
	// DDL starts at 0.6s, i.e. between the second and third samples.
	if got := blockedSecondsFrom(pts, 0.6, 800); got != 0.5 {
		t.Errorf("blocked = %v, want 0.5", got)
	}
}

func TestBlockedSecondsWithoutBaseline(t *testing.T) {
	// No baseline means no honest way to call anything a collapse.
	if got := blockedSecondsFrom(timeline(0, 0, 0, 0), 0, 0); got != 0 {
		t.Errorf("blocked = %v with a zero baseline, want 0", got)
	}
}

func TestVerdictThreshold(t *testing.T) {
	cases := []struct {
		blocked float64
		want    string
	}{
		{0, VerdictSafe},
		{0.25, VerdictSafe},
		{1.0, VerdictSafe},     // exactly at the threshold is still safe
		{1.001, VerdictUnsafe}, // past it is not
		{12.5, VerdictUnsafe},
	}
	for _, c := range cases {
		if got := verdictFor(c.blocked, 0); got != c.want {
			t.Errorf("verdictFor(%v) = %s, want %s", c.blocked, got, c.want)
		}
	}
}

// TestCollapseFractionIsMeaningful guards the two constants the verdict rests
// on against a careless edit.
func TestCollapseFractionIsMeaningful(t *testing.T) {
	if collapseFraction <= 0 || collapseFraction >= 0.5 {
		t.Errorf("collapseFraction = %v; outside (0, 0.5) it stops meaning \"stopped\"", collapseFraction)
	}
	if unsafeThresholdSeconds <= 0 {
		t.Errorf("unsafeThresholdSeconds = %v", unsafeThresholdSeconds)
	}
}

// TestRunResultJSONShape pins the frozen dashboard contract. The dashboard was
// built against these names; renaming one silently breaks the demo.
func TestRunResultJSONShape(t *testing.T) {
	r := &RunResult{
		RunID:           "rdeadbeef",
		Verdict:         VerdictUnsafe,
		BlockedSeconds:  12.4,
		DurationSeconds: 13.9,
		Casualties: Casualties{
			Total:   1842,
			ByLabel: map[string]int64{"checkout": 400, "login": 560, "add_to_cart": 882},
		},
		Error: "",
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{
		"run_id", "verdict", "blocked_seconds", "duration_seconds", "casualties",
		"classification", "explanation", "baseline_writes_per_sec", "lock_modes",
		"monitor_blocked_seconds", "timeline", "ddl_start_t", "ddl_end_t", "twin", "mode",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("run_finished payload is missing %q", key)
		}
	}
	cas, ok := got["casualties"].(map[string]any)
	if !ok {
		t.Fatalf("casualties is %T, want an object", got["casualties"])
	}
	if _, ok := cas["total"]; !ok {
		t.Error("casualties.total missing")
	}
	byLabel, ok := cas["by_label"].(map[string]any)
	if !ok {
		t.Fatalf("casualties.by_label is %T, want an object", cas["by_label"])
	}
	if byLabel["checkout"] != float64(400) {
		t.Errorf("casualties.by_label.checkout = %v, want 400", byLabel["checkout"])
	}
	// error is omitempty: a successful run must not ship a spurious "error":""
	if _, ok := got["error"]; ok {
		t.Error(`a successful run emitted an "error" key`)
	}
}

func TestNoteForIsHonestAboutFailures(t *testing.T) {
	as, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatal(err)
	}
	res := &RunResult{Error: "permission denied", BaselineWPS: 800, Verdict: VerdictSafe}
	if note := noteFor(res, as, monitorReportStub()); note == "" {
		t.Error("a failed migration must carry a note explaining the truncated measurement")
	}

	res = &RunResult{BaselineWPS: 0, Verdict: VerdictSafe}
	if note := noteFor(res, as, monitorReportStub()); note == "" {
		t.Error("a run with no baseline must say so rather than claim SAFE quietly")
	}

	res = &RunResult{BaselineWPS: 800, Verdict: VerdictSafe, InvalidIndexes: []string{"idx"}}
	if note := noteFor(res, as, monitorReportStub()); note == "" {
		t.Error("an INVALID index must be called out")
	}
}

// TestExplanationForResolvesHostileContradiction pins the honesty fix: the
// static lesson for a metadata-only ADD COLUMN promises "nothing here for Brace
// to crash", which is right on the default path but flatly contradicts an UNSAFE
// verdict when hostile mode actually froze traffic. explanationFor must leave the
// default reading alone and retire the false all-clear only when contention
// engaged and produced a freeze.
func TestExplanationForResolvesHostileContradiction(t *testing.T) {
	as, _ := Assess(`ALTER TABLE orders ADD COLUMN foo int`)
	if as == nil {
		t.Fatal("Assess returned no assessment for a metadata-only ADD COLUMN")
	}
	if as.Classification != ClassAddColumnConstDefault {
		t.Fatalf("classification = %q, want %q", as.Classification, ClassAddColumnConstDefault)
	}
	const reassurance = "There is nothing here for Brace to crash."

	// Direction 1 — default / uncontended run: the static lesson, reassuring tail
	// included, is exactly the honest reading and must be returned verbatim.
	uncontended := &RunResult{Verdict: VerdictSafe, BlockedSeconds: 0}
	if got := explanationFor(uncontended, as); got != as.Lesson.Explanation {
		t.Errorf("uncontended explanation was rewritten; must be unchanged:\n%s", got)
	}
	if !strings.Contains(explanationFor(uncontended, as), reassurance) {
		t.Error("uncontended explanation must keep the reassuring conclusion")
	}

	// Direction 2 — hostile mode engaged AND the shape actually froze traffic. The
	// card cannot say UNSAFE / frozen and "nothing to crash" at once.
	contended := &RunResult{
		Verdict:        VerdictUnsafe,
		BlockedSeconds: 2.1,
		Contention:     Contention{Requested: true, Enabled: true, HoldSeconds: 2},
	}
	got := explanationFor(contended, as)
	if strings.Contains(got, reassurance) {
		t.Errorf("contended explanation still contradicts the verdict:\n%s", got)
	}
	if !strings.Contains(got, "2.1s") {
		t.Errorf("contended explanation must cite the measured freeze:\n%s", got)
	}
	if !strings.Contains(got, "queued for its lock") {
		t.Errorf("contended explanation must land the acquisition-wait lesson:\n%s", got)
	}
	// The correct catalog-only mechanism half is educational and must survive.
	if !strings.Contains(got, "catalog-only change") {
		t.Errorf("contended explanation dropped the correct mechanism half:\n%s", got)
	}

	// Guard: contention requested but NOT engaged (no measured overlap) is still a
	// default reading — the static text must stand, tail and all.
	notEngaged := &RunResult{
		Verdict:    VerdictSafe,
		Contention: Contention{Requested: true, Enabled: false},
	}
	if got := explanationFor(notEngaged, as); got != as.Lesson.Explanation {
		t.Errorf("contention-not-engaged explanation must be unchanged:\n%s", got)
	}
}

func TestRoundSeconds(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0, 0},
		{1.23456, 1.235},
		{0.0004, 0},
		{12.4, 12.4},
	}
	for _, c := range cases {
		if got := roundSeconds(c.in); got != c.want {
			t.Errorf("roundSeconds(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestSamplerDerivesRates checks the timeline arithmetic without a database:
// cumulative counters in, per-interval rates out.
func TestSamplerDerivesRates(t *testing.T) {
	base := time.Now()
	s := &sampler{interval: 250 * time.Millisecond, begin: base}
	s.samples = []sample{
		{at: base, ok: 0, died: 0},
		{at: base.Add(250 * time.Millisecond), ok: 200, died: 0},  // 800/s
		{at: base.Add(500 * time.Millisecond), ok: 200, died: 12}, // frozen
		{at: base.Add(750 * time.Millisecond), ok: 400, died: 12}, // 800/s
	}
	pts := s.points()
	if len(pts) != 3 {
		t.Fatalf("points() returned %d points, want 3 (the first sample bounds no interval)", len(pts))
	}
	if pts[0].WPS != 800 || pts[1].WPS != 0 || pts[2].WPS != 800 {
		t.Errorf("derived rates = %v", []int64{pts[0].WPS, pts[1].WPS, pts[2].WPS})
	}
	if pts[1].Casualties != 12 || pts[2].Casualties != 12 {
		t.Errorf("casualties are cumulative, got %v", pts)
	}
	if pts[0].T != 0.25 || pts[2].T != 0.75 {
		t.Errorf("timestamps = %v %v", pts[0].T, pts[2].T)
	}

	// The baseline window.
	if got := s.rateBetween(base, base.Add(250*time.Millisecond)); got != 800 {
		t.Errorf("rateBetween over the first interval = %v, want 800", got)
	}
	// A window with fewer than two samples yields nothing rather than a
	// fabricated rate.
	if got := s.rateBetween(base.Add(time.Hour), base.Add(2*time.Hour)); got != 0 {
		t.Errorf("rateBetween over an empty window = %v, want 0", got)
	}
}

func TestSamplerEmpty(t *testing.T) {
	s := &sampler{interval: 250 * time.Millisecond, begin: time.Now()}
	if pts := s.points(); len(pts) != 0 {
		t.Errorf("an unsampled sampler produced %v", pts)
	}
}

func TestCasualtiesFromCarriesOnlyLockDeaths(t *testing.T) {
	stats := loadgenStatsStub()
	c := casualtiesFrom(stats)
	if c.Total != 30 {
		t.Errorf("total = %d, want 30", c.Total)
	}
	if c.ByLabel["checkout"] != 10 || c.ByLabel["login"] != 20 {
		t.Errorf("by_label = %v", c.ByLabel)
	}
	// OK and Errored must never leak into the casualty numbers.
	var sum int64
	for _, v := range c.ByLabel {
		sum += v
	}
	if sum != c.Total {
		t.Errorf("by_label sums to %d but total says %d", sum, c.Total)
	}
}

func monitorReportStub() monitor.Report {
	return monitor.Report{Samples: 10}
}

func loadgenStatsStub() loadgen.Stats {
	return loadgen.Stats{
		CasualtiesTotal: 30,
		ErroredTotal:    5,
		ByLabel: map[string]loadgen.LabelStats{
			"checkout":    {OK: 100, Died: 10, Errored: 2},
			"login":       {OK: 200, Died: 20, Errored: 3},
			"add_to_cart": {OK: 300, Died: 0, Errored: 0},
		},
	}
}
