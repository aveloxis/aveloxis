// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Behaviour the ScancodeStore interface unlocks (v0.28.19).
//
// These contracts were held by source needles alone because the worker
// carried a concrete *db.PostgresStore and nothing could observe what
// it asked the database to do. With a fake they are executed instead.

func storeTestWorker(t *testing.T, store ScancodeStore) *ScancodeWorker {
	t.Helper()
	return NewScancodeWorker(store, slog.New(slog.NewTextHandler(io.Discard, nil)),
		ScancodeWorkerOptions{CloneDir: t.TempDir()})
}

// failedExecution mirrors what executeScan returns on a non-zero exit:
// every capture buffer populated and an outputPath set, as its single
// non-nil return path always does. outputPath matters because
// classifyScanOutcome consults it first — the v0.23.4 salvage — so
// leaving it empty would make outcomeSalvaged unreachable from these
// tests and the mirror claim untrue (pass 51).
func failedExecution(t *testing.T, err error) *scanExecution {
	t.Helper()
	return &scanExecution{
		pid:              1234,
		outputPath:       filepath.Join(t.TempDir(), "scan.json"),
		waitErr:          err,
		effectiveTimeout: 2 * time.Hour,
		stderrTail:       &tailBuffer{cap: scancodeStderrTailBytes},
		stdoutTail:       &tailBuffer{cap: scancodeStderrTailBytes},
		stderrFull:       &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes},
		stdoutFull:       &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes},
	}
}

// A scan killed by shutdown is neither a failure nor a timeout: the
// lock is cleared and the repo re-claims on the next start. Before
// pass 36 the SIGKILL text read as a wall-clock timeout, so every stop
// recorded a strike, printed a false "stretched timeout" line and, at
// the cap, sidelined the repo for 180 days.
func TestFinishScanOnShutdownClearsLockWithoutStrike(t *testing.T) {
	store := &fakeScancodeStore{}
	w := storeTestWorker(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the shutdown that killed the scan

	w.finishScan(ctx, db.ScancodeJob{RepoID: 42, RepoOwner: "o", RepoName: "r"},
		failedExecution(t, errors.New("signal: killed")))

	got := store.snapshot()
	if len(got.cleared) != 1 || got.cleared[0] != 42 {
		t.Errorf("shutdown-killed scan cleared %v, want exactly [42] — the lock must be released so the repo re-claims on the next start rather than waiting out the stale-lock window (pass 37).", got.cleared)
	}
	if len(got.failures) != 0 {
		t.Errorf("shutdown-killed scan recorded a FAILURE for %v — strikes drive the v0.21.4 quadratic backoff and the 10-strike, 180-day sideline. A `stop serve` is not the repo's fault (pass 36).", got.failures)
	}
	if len(got.timeouts) != 0 || got.sidelines != 0 {
		t.Errorf("shutdown-killed scan recorded a TIMEOUT (%v, sidelines=%d) — `signal: killed` is also what cmd.Cancel produces on a wall-clock timeout, and telling them apart is exactly what the ctx check is for (pass 36).", got.timeouts, got.sidelines)
	}
	if len(got.completed) != 0 {
		t.Errorf("shutdown-killed scan marked repos %v COMPLETE — nothing was ingested; stamping completion would suppress the repo for a full cadence (180 days).", got.completed)
	}
}

// The same shape with a LIVE ctx is a real failure and must still
// record a strike — otherwise the fix above would silently disable the
// backoff it is carving an exception out of.
func TestFinishScanOnRealFailureRecordsStrike(t *testing.T) {
	store := &fakeScancodeStore{}
	w := storeTestWorker(t, store)

	w.finishScan(context.Background(), db.ScancodeJob{RepoID: 7, RepoOwner: "o", RepoName: "r"},
		failedExecution(t, errors.New("exit status 2")))

	got := store.snapshot()
	if len(got.failures) != 1 || got.failures[0] != 7 {
		t.Errorf("a genuine scan failure recorded %v, want exactly [7] — the shutdown carve-out must not swallow real failures, or the 10-strike sideline never fires and a permanently broken repo is retried forever (v0.21.4).", got.failures)
	}
	if len(got.completed) != 0 {
		t.Errorf("a failed scan marked repos %v complete", got.completed)
	}
}

// While the toolchain is BROKEN the dispatcher claims NOTHING. On
// 2026-06-11 the preflight logged a system-level failure at startup
// and the dispatcher then ran 2,473 scans on the broken toolchain
// anyway, producing stderr artifacts up to 9.5 GB (v0.27.6).
func TestDispatcherClaimsNothingWhileToolchainBroken(t *testing.T) {
	store := &fakeScancodeStore{}
	w := storeTestWorker(t, store)
	w.healthy.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	jobs := make(chan db.ScancodeJob)
	done := make(chan struct{})
	go func() { defer close(done); w.dispatcher(ctx, jobs) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dispatcher did not return after its context was canceled")
	}

	if got := store.snapshot(); got.claims != 0 {
		t.Errorf("the dispatcher claimed %d repo(s) while the toolchain was marked BROKEN — detection without gating just timestamps the damage: this is the path that ran 2,473 scans on a corrupt libmagic and filled the disk with stderr (v0.27.6).", got.claims)
	}
}

// The healthy dispatcher does claim, so the gate above is proving
// something. Without this, marking every dispatcher path dead would
// pass the test above.
func TestDispatcherClaimsWhenToolchainHealthy(t *testing.T) {
	store := &fakeScancodeStore{}
	w := storeTestWorker(t, store)
	w.healthy.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	jobs := make(chan db.ScancodeJob)
	done := make(chan struct{})
	go func() { defer close(done); w.dispatcher(ctx, jobs) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dispatcher did not return after its context was canceled")
	}

	if got := store.snapshot(); got.claims == 0 {
		t.Error("a healthy dispatcher claimed nothing — then TestDispatcherClaimsNothingWhileToolchainBroken proves nothing either, because a dispatcher that never claims passes it trivially (lens L4).")
	}
}

// storeAvailable exists to survive the trap the ScancodeStore
// interface introduced: an interface holding a TYPED nil pointer is
// itself non-nil, so the `w.store == nil` guard that was correct while
// the field was a concrete *db.PostgresStore would now pass and the
// first call would panic. The helper was written for that and had no
// test (pass 51) — reverting it to `w.store != nil` left the whole
// suite green.
func TestStoreAvailableSurvivesTheTypedNilTrap(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if w := NewScancodeWorker(nil, logger, ScancodeWorkerOptions{}); w.storeAvailable() {
		t.Error("a worker built with an untyped nil store reports a usable store")
	}

	var typed *fakeScancodeStore // nil POINTER in a non-nil interface
	w := NewScancodeWorker(typed, logger, ScancodeWorkerOptions{})
	if w.store == nil {
		t.Fatal("this test no longer exercises the typed-nil trap: the interface compared equal to nil, so storeAvailable's reflect check is not what is keeping the worker safe. Re-derive the case before trusting the assertion below.")
	}
	if w.storeAvailable() {
		t.Error("a worker holding a TYPED nil store reports a usable store — every store call then panics on the first dereference. This is the trap the interface introduced and the whole reason storeAvailable is not just `w.store == nil` (v0.28.19).")
	}

	if w := NewScancodeWorker(&fakeScancodeStore{}, logger, ScancodeWorkerOptions{}); !w.storeAvailable() {
		t.Error("a worker holding a real store reports it unusable — the guard would silently skip every status write (scancode_preflight.go's recordScancodeStatus)")
	}
}
