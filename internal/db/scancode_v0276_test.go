// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.27.6 — store-layer contracts for the scancode self-healing +
// dedicated-host work: the derived stale-lock window, the locked-host
// column, the generated-content skip, and the at-cap timeout
// sideline.

func readScancodeStoreSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func scancodeStoreFuncBody(t *testing.T, src, decl string) string {
	t.Helper()
	idx := strings.Index(src, decl)
	if idx < 0 {
		t.Fatalf("cannot find %q in scancode_worker_store.go", decl)
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]
	// Strip // comments (the v0.21.5 / v0.27.4 lesson: the extracted
	// region runs to the NEXT func declaration, which pulls in the
	// following function's doc comment — comments legitimately NAME
	// forbidden patterns, so negative pins must match code only).
	var code []string
	for _, line := range strings.Split(body, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	return strings.Join(code, "\n")
}

func TestSchemaHasV0276ScancodeColumns(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{"scancode_locked_host", "scancode_skip_reason"} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare %s on aveloxis_data.repos", needle)
		}
	}
}

func TestMigrateAddsV0276ScancodeColumns(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for col, typ := range map[string]string{
		"scancode_locked_host": "TEXT",
		"scancode_skip_reason": "TEXT DEFAULT ''",
	} {
		needle := `addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_data.repos", "` + col + `", "` + typ + `"`
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must add aveloxis_data.repos.%s (%s) for upgrading fleets", col, typ)
		}
	}
}

// TestClaimTakesDerivedStaleLockWindow is the negative tripwire for
// the June 2026 duplicate-claim bug: ScancodeStaleLockWindow (12h)
// used to be baked directly into the claim SQL while v0.23.8
// stretched timeouts legitimately ran to the 24h cap — a scan past
// 12h had its lock treated as stale and a SECOND worker claimed the
// same repo (confirmed interleaving on pytorch/docs). The window is
// now caller-derived (runTimeoutCap + 2h) with the constant demoted
// to a floor.
func TestClaimTakesDerivedStaleLockWindow(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) ClaimNextScancodeRepo(")

	if !strings.Contains(body, "staleLockWindow time.Duration") &&
		!strings.Contains(body, ", staleLockWindow ") {
		t.Error("ClaimNextScancodeRepo must take the caller-derived staleLockWindow parameter")
	}
	// Floor clamp: zero / undersized inputs can never re-shrink the
	// window below 12h.
	if !strings.Contains(body, "staleLockWindow < ScancodeStaleLockWindow") {
		t.Error("ClaimNextScancodeRepo must clamp staleLockWindow to the ScancodeStaleLockWindow floor — an accidental zero would make EVERY lock instantly stale")
	}
	// The SQL parameter must be the derived value…
	if !strings.Contains(body, "staleLockWindow.String()") {
		t.Error("the claim SQL's lock-age parameter must be the derived staleLockWindow")
	}
	// …and NOT the bare constant (the pre-v0.27.6 shape).
	if strings.Contains(body, "ScancodeStaleLockWindow.String()") {
		t.Error("NEGATIVE TRIPWIRE: ClaimNextScancodeRepo must not pass ScancodeStaleLockWindow.String() into the SQL — the bare 12h constant undercutting the 24h adaptive-timeout cap is the June 2026 duplicate-claim bug")
	}
}

func TestClaimReturnsLanguagesForSkipPolicy(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) ClaimNextScancodeRepo(")
	if !strings.Contains(body, "languages") {
		t.Error("ClaimNextScancodeRepo's RETURNING must include the repos.languages JSONB — the worker's generated-content skip decides from it BEFORE cloning")
	}
	if !strings.Contains(src, "Languages map[string]int64") {
		t.Error("ScancodeJob must carry Languages map[string]int64")
	}
}

func TestRecordScancodeLockStateStoresHost(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) RecordScancodeLockState(")
	if !strings.Contains(body, "host string") && !strings.Contains(body, ", host ") {
		t.Error("RecordScancodeLockState must take the recording machine's hostname (v0.27.6 dedicated-host support)")
	}
	if !strings.Contains(body, "scancode_locked_host") {
		t.Error("RecordScancodeLockState must write scancode_locked_host — recoverOrphans only adjudicates (pid, boot_id) liveness for own-host locks")
	}
}

func TestListLockedScancodeRowsCarriesHost(t *testing.T) {
	src := readScancodeStoreSource(t)
	if !strings.Contains(src, "LockedHost string") {
		t.Error("ScancodeLockedRow must expose LockedHost")
	}
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) ListLockedScancodeRows(")
	if !strings.Contains(body, "scancode_locked_host") {
		t.Error("ListLockedScancodeRows must select scancode_locked_host so the recovery pass and the startup sweep can partition locks by owner")
	}
}

func TestAllLockClearsAlsoClearHost(t *testing.T) {
	src := readScancodeStoreSource(t)
	// Every path that clears scancode_locked_at must clear the host
	// too: MarkScancodeComplete, MarkScancodeSkipped,
	// RecordScancodeTimeout, RecordScancodeFailure, ClearScancodeLock,
	// ClearStaleNullPidLocks.
	if n := strings.Count(src, "scancode_locked_host = NULL"); n < 6 {
		t.Errorf("expected scancode_locked_host = NULL in all 6 lock-clearing statements, found %d — a stale host value would make the next own-host adjudication (or the startup sweep's keep set) lie", n)
	}
}

func TestRecordScancodeTimeoutSidelineParameter(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) RecordScancodeTimeout(")
	if !strings.Contains(body, "sideline bool") {
		t.Error("RecordScancodeTimeout must take the v0.27.6 sideline flag — N consecutive AT-CAP timeouts prove the repo cannot be scanned within budget (the pytorch/docs 27-claim spin loop)")
	}
	if !strings.Contains(body, "CASE WHEN $2 THEN NOW() ELSE scancode_last_run END") {
		t.Error("sideline=true must stamp scancode_last_run = NOW() (the v0.21.4 cadence-gate mechanism); sideline=false must leave it untouched")
	}
	// The diagnostic trail survives: the attempts counter increments
	// on BOTH branches.
	if !strings.Contains(body, "scancode_timeout_attempts = COALESCE(scancode_timeout_attempts, 0) + 1") {
		t.Error("RecordScancodeTimeout must keep incrementing scancode_timeout_attempts even when sidelining — the operator's diagnostic trail")
	}
}

func TestMarkScancodeSkippedContract(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) MarkScancodeSkipped(")
	if !strings.Contains(body, "scancode_last_run = NOW()") {
		t.Error("MarkScancodeSkipped must stamp scancode_last_run so the cadence gate applies to skips like scans (the decision is re-evaluated at normal cadence)")
	}
	if !strings.Contains(body, "scancode_skip_reason = $2") {
		t.Error("MarkScancodeSkipped must record the reason for operator visibility")
	}
	if strings.Contains(body, "scancode_failed_attempts") || strings.Contains(body, "scancode_timeout_attempts") {
		t.Error("MarkScancodeSkipped must NOT touch the failure/timeout counters — a previously spinning repo keeps its diagnostic trail")
	}
}

func TestMarkScancodeCompleteClearsSkipReason(t *testing.T) {
	src := readScancodeStoreSource(t)
	body := scancodeStoreFuncBody(t, src, "func (s *PostgresStore) MarkScancodeComplete(")
	if !strings.Contains(body, "scancode_skip_reason = ''") {
		t.Error("MarkScancodeComplete must clear scancode_skip_reason — a repo that later gets a REAL successful scan must stop advertising a stale skip")
	}
}
