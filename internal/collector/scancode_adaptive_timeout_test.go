// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.23.8 — adaptive per-repo scancode wall-clock timeout.
//
// The v0.23.3 cap was a hardcoded `scancodeRunTimeout = 2 * time.Hour`,
// chosen as a balance between "let typical scans finish" and "don't
// let a wedged worker eat a slot forever." It works for the average
// repo. It does NOT work for kernel-class repos (Linux kernel ~80K
// files, scancode-toolkit-mini at ~7 files/sec ≈ 3 hours minimum) —
// those get SIGKILL'd mid-scan every cycle and never complete.
//
// v0.23.8 makes the base timeout configurable AND adds per-repo
// adaptive scaling: when a scan exits with `signal: killed` (the
// wall-clock-timeout cmd.Cancel signature), the row's
// scancode_timeout_attempts counter increments and the next attempt
// uses `min(base * 2^attempts, cap)`. Kernel-sized repos discover
// their natural runtime over a few cycles without operator action.
//
// Critical design choice: timeout-class failures DO NOT increment
// scancode_failed_attempts (the v0.21.4 10-strike sideline counter).
// A repo that takes 16h to scan isn't broken — it's big. Sidelining
// it would lose coverage. Distinguishing the two failure classes
// requires that runOne gate on `signal: killed` specifically before
// calling RecordScancodeTimeout vs RecordScancodeFailure.

func TestRunOneGatesTimeoutFailureOnSignalKilled(t *testing.T) {
	// v0.27.6: the timeout-vs-failure routing was consolidated into
	// the classifyScanOutcome classifier in scancode_policy.go.
	src, err := os.ReadFile("scancode_policy.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The classifier must inspect err.Error() for the "signal: killed"
	// substring that Go's exec package emits when a subprocess is
	// SIGKILL'd. That's the cmd.Cancel signature when scanCtx times out.
	if !strings.Contains(code, `"signal: killed"`) {
		t.Error("classifyScanOutcome must gate the timeout-vs-real-failure routing on " +
			"the literal substring `signal: killed` (Go's exec.ExitError " +
			"text for a SIGKILL'd subprocess). Without the gate, a " +
			"genuine scancode crash (`exit status 1`) would get treated " +
			"as a timeout and get a bigger timeout next cycle — wasting " +
			"budget for nothing. v0.23.8.")
	}
}

func TestRunOneCallsRecordScancodeTimeout(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "RecordScancodeTimeout") {
		t.Error("runOne must call store.RecordScancodeTimeout on the " +
			"timeout-class failure path. This increments " +
			"scancode_timeout_attempts (used for adaptive timeout " +
			"sizing) but does NOT increment scancode_failed_attempts " +
			"(which would trigger the v0.21.4 10-strike sideline " +
			"erroneously on kernel-class repos).")
	}
}

func TestRunOneComputesAdaptiveTimeout(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The adaptive formula is `min(base * 2^attempts, cap)`. Pin a
	// recognizable token: 1 << attempts (or similar bit-shift), OR
	// math.Pow with 2. Either approach is fine; we just need to see
	// the doubling exists.
	hasBitShift := regexp.MustCompile(`1\s*<<\s*\w+`).MatchString(code)
	hasPow := strings.Contains(code, "math.Pow")
	if !hasBitShift && !hasPow {
		t.Error("runOne must compute an exponentially-growing per-repo " +
			"timeout based on scancode_timeout_attempts. Expected either " +
			"`1 << attempts` or math.Pow(2, attempts).")
	}
}

func TestScancodeJobCarriesTimeoutAttempts(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The dispatcher claims a row and passes it to the runner via
	// ScancodeJob. The runner needs the current attempts counter
	// to compute its effective timeout. So the struct must carry
	// the field, and ClaimNextScancodeRepo's RETURNING must include
	// the column.
	if !strings.Contains(code, "TimeoutAttempts") {
		t.Error("ScancodeJob must expose a TimeoutAttempts field so the " +
			"runner can compute `base * 2^attempts` from the claimed row's " +
			"state. v0.23.8.")
	}
	if !strings.Contains(code, "scancode_timeout_attempts") {
		t.Error("ClaimNextScancodeRepo's SELECT/RETURNING must include " +
			"scancode_timeout_attempts so the runner has the value to " +
			"compute its effective timeout. Without it, every claim " +
			"would see attempts=0 and use the base.")
	}
}

func TestRecordScancodeTimeoutExists(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "func (s *PostgresStore) RecordScancodeTimeout") {
		t.Error("v0.23.8 introduces RecordScancodeTimeout as a separate " +
			"failure path from RecordScancodeFailure. It increments " +
			"scancode_timeout_attempts and stamps scancode_last_failed_at " +
			"(so the v0.21.4 backoff gate still applies) but does NOT " +
			"increment scancode_failed_attempts. Kernel-class repos need " +
			"this distinction or they hit the 10-strike sideline.")
	}
	// The implementation must NOT also bump scancode_failed_attempts.
	// Find the helper body and verify.
	helperStart := strings.Index(code, "func (s *PostgresStore) RecordScancodeTimeout")
	if helperStart < 0 {
		return
	}
	// Crude scan: read until the next top-level func.
	rest := code[helperStart:]
	nextFn := strings.Index(rest[len("func (s *PostgresStore) RecordScancodeTimeout"):], "\nfunc ")
	if nextFn > 0 {
		rest = rest[:len("func (s *PostgresStore) RecordScancodeTimeout")+nextFn]
	}
	if strings.Contains(rest, "scancode_failed_attempts = COALESCE(scancode_failed_attempts, 0) + 1") {
		t.Error("RecordScancodeTimeout must NOT increment " +
			"scancode_failed_attempts — that's the v0.21.4 10-strike " +
			"counter for genuine failures. Timeouts on big repos are " +
			"not failures.")
	}
}

func TestMarkScancodeCompleteResetsTimeoutAttempts(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// MarkScancodeComplete must reset both counters on success — a
	// kernel-sized repo that finishes (after the adaptive timeout
	// stretched enough) starts fresh on the next cycle.
	startIdx := strings.Index(code, "func (s *PostgresStore) MarkScancodeComplete")
	if startIdx < 0 {
		t.Fatal("MarkScancodeComplete not found")
	}
	body := code[startIdx:]
	if endIdx := strings.Index(body[len("func (s *PostgresStore) MarkScancodeComplete"):], "\nfunc "); endIdx > 0 {
		body = body[:len("func (s *PostgresStore) MarkScancodeComplete")+endIdx]
	}
	if !strings.Contains(body, "scancode_timeout_attempts = 0") {
		t.Error("MarkScancodeComplete must reset scancode_timeout_attempts " +
			"to 0 on success — without the reset, a previously-stretched " +
			"repo would carry its high attempt count forward and use a " +
			"too-large timeout on the next scan (wasting nothing, but " +
			"making the formula less responsive to repo size changes).")
	}
}
