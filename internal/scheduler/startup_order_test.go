package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestLockRecoveryRunsBeforeLeftoverStaging pins the startup
// ordering invariant that RecoverOtherWorkerLocks executes BEFORE
// processLeftoverStaging.
//
// Background: in v0.18.16 and earlier the order was reversed — the
// comment above the lock-recovery call claimed "Immediately reclaim
// all locks" but the actual call sat AFTER processLeftoverStaging.
// processLeftoverStaging blocks for minutes on a realistic backlog
// (370K unprocessed rows across 15 repos took ~12 min in the
// v0.18.15 observed deployment), during which time the orphan
// locks from a previously-crashed worker stayed stale. A user
// watching the monitor would see 48 "collecting" rows with 2h40m+
// lock ages while their new scheduler was busy draining staging.
//
// Lock recovery is a single UPDATE that takes milliseconds and has
// zero data dependency on leftover staging. Running it first costs
// nothing and gets the queue into a correct state immediately.
//
// This is a pure-ordering test — it reads the source and asserts
// the call positions, so a future refactor can't silently swap
// the order back.
func TestLockRecoveryRunsBeforeLeftoverStaging(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	lockIdx := strings.Index(src, "RecoverOtherWorkerLocks")
	if lockIdx < 0 {
		t.Fatal("cannot find RecoverOtherWorkerLocks call site in scheduler.go")
	}
	leftoverIdx := strings.Index(src, "processLeftoverStaging(ctx)")
	if leftoverIdx < 0 {
		t.Fatal("cannot find processLeftoverStaging call site in scheduler.go")
	}

	if lockIdx >= leftoverIdx {
		t.Errorf("scheduler startup ordering is wrong: RecoverOtherWorkerLocks "+
			"appears at byte %d, processLeftoverStaging at byte %d. Lock "+
			"recovery must come FIRST so orphan locks from a crashed prior "+
			"process are released in milliseconds, not after the multi-minute "+
			"leftover-staging drain.", lockIdx, leftoverIdx)
	}
}

// TestRecoverStaleRunsBeforeLeftoverStaging — same logic for the
// general stale-lock sweep (not just this-process's worker ID).
// recoverStale reclaims any lock older than StaleLockTimeout (1h
// default); orphans from pre-restart crashes are always >1h in
// practice by the time a human notices, so this sweep is what
// actually reclaims them.
func TestRecoverStaleRunsBeforeLeftoverStaging(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find the first call to s.recoverStale(ctx) (a bare call, not
	// part of a comment or the recoveryTicker.C branch). The simpler
	// check: look for s.recoverStale(ctx) before processLeftoverStaging.
	recoverIdx := strings.Index(src, "s.recoverStale(ctx)")
	if recoverIdx < 0 {
		t.Fatal("cannot find s.recoverStale(ctx) call site in scheduler.go")
	}
	leftoverIdx := strings.Index(src, "processLeftoverStaging(ctx)")
	if leftoverIdx < 0 {
		t.Fatal("cannot find processLeftoverStaging call site in scheduler.go")
	}

	if recoverIdx >= leftoverIdx {
		t.Errorf("scheduler startup ordering is wrong: s.recoverStale(ctx) "+
			"appears at byte %d, processLeftoverStaging at byte %d. The "+
			"stale-lock sweep must come FIRST so locks older than "+
			"StaleLockTimeout get reclaimed in milliseconds.", recoverIdx, leftoverIdx)
	}
}
