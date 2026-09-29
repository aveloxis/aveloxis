// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestJobSlotsReport — worklist item 77 (Stage 1, observation only): a
// 10-hour facade blocked a slot with nothing in the log between "job
// started" and its end, and "job interrupted" did not say how long the job
// had run. The registry reports the active slots — the count, per phase,
// and the oldest (its repo, phase and age) — and a job's elapsed time.
func TestJobSlotsReport(t *testing.T) {
	var js jobSlots
	now := time.Now()
	js.beginAt(1, now.Add(-3*time.Hour))
	js.setPhase(1, "facade/analysis")
	js.beginAt(2, now.Add(-10*time.Minute))
	js.setPhase(2, "api collection")
	js.beginAt(3, now.Add(-time.Minute))
	js.setPhase(3, "api collection")
	if got := js.elapsed(1); got < 3*time.Hour-time.Second || got > 3*time.Hour+time.Minute {
		t.Errorf("elapsed(1) = %v; want ~3h", got)
	}
	var logs strings.Builder
	js.log(slog.New(slog.NewTextHandler(&logs, nil)))
	l := logs.String()
	for _, want := range []string{"active=3", "oldest_repo_id=1", "oldest_phase=facade/analysis", "oldest_age=3h", `per_phase.api collection"=2`} {
		if !strings.Contains(l, want) {
			t.Errorf("the slot line must carry %q:\n%s", want, l)
		}
	}
	js.end(1)
	js.end(2)
	js.end(3)
	logs.Reset()
	js.log(slog.New(slog.NewTextHandler(&logs, nil)))
	if logs.Len() != 0 {
		t.Errorf("no active slots logs nothing:\n%s", logs.String())
	}
	if js.elapsed(1) != 0 {
		t.Error("an ended job has no elapsed time")
	}
}

// TestJobInterruptedSaysHowLongTheJobRan — item 77: the interrupted line
// carries the job's elapsed time from the registry runJob fills.
func TestJobInterruptedSaysHowLongTheJobRan(t *testing.T) {
	var logs strings.Builder
	s := &Scheduler{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	s.slots.beginAt(5, time.Now().Add(-2*time.Minute))
	s.jobInterrupted(5, "sbom")
	if !strings.Contains(logs.String(), "elapsed=2m") {
		t.Errorf("the interrupted line must say how long the job ran:\n%s", logs.String())
	}
}

// TestRunJobRegistersItsSlot — the wiring: runJob registers the job and
// removes it on every exit, marks each phase, and the 5-minute summary
// logs the slots.
func TestRunJobRegistersItsSlot(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	body := srctest.FuncBody(t, src, "func (s *Scheduler) runJob(")
	for _, want := range []string{"s.slots.beginAt(job.RepoID, start)", "defer s.slots.end(job.RepoID)",
		`s.slots.setPhase(job.RepoID, "api collection")`, `s.slots.setPhase(job.RepoID, "facade/analysis")`,
		`s.slots.setPhase(job.RepoID, "commit resolution")`, `s.slots.setPhase(job.RepoID, "sbom")`,
		`s.slots.setPhase(job.RepoID, "vulnerability scan")`} {
		if !strings.Contains(body, want) {
			t.Errorf("runJob must %s", want)
		}
	}
	if !strings.Contains(src, "s.slots.log(s.logger)") {
		t.Error("the periodic summary must log the slots")
	}
}

// TestXIDStatusIsLoggedHourly — item 78's wiring: the hourly tick runs the
// XID line single-flight (a stalled database must not stack readers).
func TestXIDStatusIsLoggedHourly(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	if !strings.Contains(src, `s.singleFlight(&s.xidStatusActive, "xid-status", func() { s.logXIDStatus(ctx) })`) {
		t.Error("the hourly tick must log the transaction-ID status, single-flight")
	}
}

// TestResetAgreementLine — Phase 0: the summary's aggregate carries each
// bucket's totals and names the endpoints whose resets disagreed.
func TestResetAgreementLine(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	logResetAgreement(logger, nil)
	if logs.Len() != 0 {
		t.Errorf("no counts must log nothing:\n%s", logs.String())
	}
	logResetAgreement(logger, map[platform.ResetAgreementKey]platform.ResetAgreementCounts{
		{Bucket: "core", Endpoint: "/repos/{owner}/{repo}/pulls/…"}: {Equal: 5, Later: 2},
		{Bucket: "core", Endpoint: "/repos/{owner}/{repo}/issues"}:  {Equal: 3},
		{Bucket: "graphql", Endpoint: "/graphql"}:                   {Earlier: 1, Untracked: 4},
	})
	l := logs.String()
	for _, want := range []string{`msg="key pool reset agreement"`, "core.equal=8", "core.later=2", "graphql.earlier=1", "graphql.untracked=4",
		"core /repos/{owner}/{repo}/pulls/… earlier=0 later=2", "graphql /graphql earlier=1 later=0"} {
		if !strings.Contains(l, want) {
			t.Errorf("the reset-agreement line must carry %q:\n%s", want, l)
		}
	}
	if strings.Contains(l, "/repos/{owner}/{repo}/issues earlier") {
		t.Errorf("an endpoint that always agreed is counted, not named:\n%s", l)
	}
}
