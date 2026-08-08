//go:build integration

// Integration tests for hostile mode — the feature whose whole claim is that a
// fast DDL's lock-acquisition freeze is MEASURED, not assumed. They need the
// same real PostgreSQL 16 the rest of the engine suite uses; see
// integration_test.go for the harness (TEST_ADMIN_URL, the seeded template, and
// newLab).
package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestHostileAddColumnMeasuresAcquisitionFreeze pins BUG 1: a metadata-only ADD
// COLUMN is answered already_safe for a default run — but with hostile:true it
// must proceed to a real rehearsal and MEASURE the acquisition freeze the
// fast-path assumes away, rather than being refused before it can.
func TestHostileAddColumnMeasuresAcquisitionFreeze(t *testing.T) {
	_, eng := newLab(t, "hostile_addcol", 1)
	ctx := context.Background()

	const sql = `ALTER TABLE orders ADD COLUMN hostile_probe int`

	// The precondition: WITHOUT hostile, this shape is short-circuited
	// already_safe and never runs. (This is the correct behaviour to preserve.)
	if _, err := eng.Rehearse(ctx, sql, ModeCustom); err == nil {
		t.Fatal("a metadata-only ADD COLUMN should be answered already_safe without a run")
	} else {
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Kind != RejectAlreadySafe {
			t.Fatalf("non-hostile ADD COLUMN error = %v, want an already_safe rejection", err)
		}
	}

	// With hostile:true it must RUN — no rejection — and measure the freeze.
	res, err := eng.Rehearse(ctx, sql, ModeCustom, WithHostile(true))
	if err != nil {
		t.Fatalf("hostile Rehearse was REJECTED instead of measuring (this is BUG 1): %v", err)
	}
	t.Logf("verdict=%s blocked=%.3fs monitor_blocked=%.3fs duration=%.3fs enabled=%v waited=%v cycles=%d hold=%.1fs casualties=%d qualifier=%q",
		res.Verdict, res.BlockedSeconds, res.MonitorBlockedSeconds, res.DurationSeconds,
		res.Contention.Enabled, res.Monitor.MigrationLockWaited, res.Contention.Cycles,
		res.Contention.HoldSeconds, res.Casualties.Total, res.Qualifier)

	if res.Error != "" {
		t.Fatalf("the migration itself failed: %s", res.Error)
	}
	if res.Classification != ClassAddColumnConstDefault {
		t.Errorf("classification = %q, want %q", res.Classification, ClassAddColumnConstDefault)
	}
	if !res.Contention.Requested {
		t.Error("hostile was requested; the result must record it")
	}
	// The DDL was dispatched into a confirmed-active hold and had to queue behind
	// it — the overlap must be MEASURED, not assumed.
	if !res.Contention.Enabled {
		t.Fatalf("hostile mode did not engage on an ACCESS EXCLUSIVE shape held behind a 2s transaction: %+v",
			res.Contention)
	}
	if !res.Monitor.MigrationLockWaited {
		t.Error("contention reported engaged, but the migration was never observed waiting for its lock")
	}
	// The freeze must be on the order of the hold, and the verdict UNSAFE rather
	// than the false green light the already_safe fast-path would have implied.
	if res.Verdict != VerdictUnsafe {
		t.Errorf("verdict = %s (blocked %.3fs), want UNSAFE — writes froze behind the queued ACCESS EXCLUSIVE request",
			res.Verdict, res.BlockedSeconds)
	}
	hold := res.Contention.HoldSeconds
	if hold <= 0 {
		hold = 2
	}
	if res.BlockedSeconds < hold*0.5 {
		t.Errorf("blocked_seconds = %.3f, want on the order of the %.1fs hold — the acquisition freeze was not measured",
			res.BlockedSeconds, hold)
	}
	if res.LedgerID == 0 {
		t.Error("a completed hostile run must still be recorded in the ledger")
	}
}

// TestHostileRenameDoesNotFalselyGreenLight pins BUG 2: an ALTER INDEX ... RENAME
// takes only SHARE UPDATE EXCLUSIVE on PG12+, which does NOT conflict with the
// contender's ROW EXCLUSIVE hold — so even behind a 2s open transaction the DDL
// acquires instantly and never contends. The engine must therefore NOT report it
// as engaged, must NOT emit the confident "strongest green light" note, and must
// flag the SAFE verdict with contention_not_engaged.
func TestHostileRenameDoesNotFalselyGreenLight(t *testing.T) {
	_, eng := newLab(t, "hostile_rename", 1)
	ctx := context.Background()

	// orders_pkey is the primary-key index the seeded template creates.
	const sql = `ALTER INDEX orders_pkey RENAME TO orders_pkey_renamed`

	res, err := eng.Rehearse(ctx, sql, ModeCustom, WithHostile(true))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s blocked=%.3fs enabled=%v requested=%v waited=%v cycles=%d qualifier=%q note=%q",
		res.Verdict, res.BlockedSeconds, res.Contention.Enabled, res.Contention.Requested,
		res.Monitor.MigrationLockWaited, res.Contention.Cycles, res.Qualifier, res.Note)

	if res.Error != "" {
		t.Fatalf("the rename itself failed: %s", res.Error)
	}
	if res.Verdict != VerdictSafe {
		t.Fatalf("verdict = %s; a non-conflicting rename should not freeze writes", res.Verdict)
	}
	if !res.Contention.Requested {
		t.Error("hostile was requested; the result must record it")
	}
	// The heart of BUG 2: a hold that was held but never actually contended must
	// NOT be latched as engaged.
	if res.Contention.Enabled {
		t.Errorf("contention reported ENGAGED for a rename whose lock does not conflict with the hold: %+v",
			res.Contention)
	}
	if res.Monitor.MigrationLockWaited {
		t.Error("the rename was seen waiting for a lock; on PG12+ it should acquire instantly")
	}
	// The false green light this bug produced.
	const confident = "the strongest green light this rig can give"
	if strings.Contains(res.Note, confident) {
		t.Errorf("the confident contention note was emitted WITHOUT a measured overlap:\n%s", res.Note)
	}
	if res.Qualifier != QualifierContentionNotEngaged {
		t.Errorf("qualifier = %q, want %q — a SAFE hostile run that never contended must say so",
			res.Qualifier, QualifierContentionNotEngaged)
	}
	if !strings.Contains(res.Note, "did not engage") {
		t.Errorf("the note must say hostile mode did not engage; got:\n%s", res.Note)
	}
}
