// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Force-recollect (v0.18.24): scheduler-side contract for the flag that
// was added to collection_queue. Two behaviors:
//
//   - determineSince must return zero time (full collection) when the
//     job row's ForceFullCollect flag is set, regardless of whether the
//     repo was previously collected.
//   - the outcome of a job whose error wraps platform.ErrPRBatch — the
//     GraphQL-batch failures that leave PR child data incomplete (stream
//     CANCEL, validation timeout, retry exhaustion; the 2026-04-22
//     production errors) — arms force_full (typed since v0.29.71; it
//     used to match the error text).

func TestDetermineSince_RespectsForceFullCollect(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{
		Collection: &config.CollectionConfig{DaysUntilRecollect: 1},
	})

	// Repo was previously collected (LastCollected set) AND has the
	// ForceFullCollect flag. Normal logic would return now-24h; the flag
	// must override to zero.
	now := time.Now()
	job := &db.QueueJob{
		RepoID:           42,
		LastCollected:    &now,
		ForceFullCollect: true,
	}
	since := s.determineSince(job)
	if !since.IsZero() {
		t.Errorf("determineSince with ForceFullCollect=true returned %v, want zero time — the flag must override the last_collected incremental window", since)
	}

	// Flag off: normal incremental window.
	job.ForceFullCollect = false
	since = s.determineSince(job)
	if since.IsZero() {
		t.Error("determineSince with ForceFullCollect=false and LastCollected set must not return zero time — that would wipe the incremental contract for healthy repos")
	}
}

// TestBuildOutcomeForceFullIsTyped — the force_full escalation (v0.18.24)
// used to be decided by searching the recorded error TEXT for "graphql PR
// batch" (shouldForceFullRecollect) — the error-text class the ratchet
// cannot see across a function boundary (v0.29.71, summary/43 item 8). It
// is now errors.Is(err, platform.ErrPRBatch) on the error buildOutcome
// records, through the production wrap chains; the text alone decides
// nothing.
func TestBuildOutcomeForceFullIsTyped(t *testing.T) {
	batch := func(tail string) error { return fmt.Errorf("%w: %s", platform.ErrPRBatch, tail) }
	s := &Scheduler{}
	for _, tc := range []struct {
		name                string
		result              *collector.CollectResult
		collectionErr, fill error
		want                bool
	}{
		{name: "success", want: false},
		{name: "shard stream CANCEL (result.Errors)", result: &collector.CollectResult{Errors: []error{fmt.Errorf("pull requests graphql batch shard 1: %w", batch("read graphql response: stream error"))}}, want: true},
		{name: "retry exhaustion (collection error)", collectionErr: fmt.Errorf("pull requests graphql batch: %w", batch("graphql: exhausted 10 retries")), want: true},
		{name: "child pagination", result: &collector.CollectResult{Errors: []error{fmt.Errorf("%w: paginating children for PR #7: %w", platform.ErrPRBatch, errors.New("x"))}}, want: true},
		{name: "gap fill through errors.Join", fill: errors.Join(fmt.Errorf("PR gap fill: %w", fmt.Errorf("gap fill PR batch: %w", batch("stream error")))), want: true},
		{name: "the text alone", collectionErr: errors.New("pull requests graphql batch: graphql PR batch: stream error"), want: false},
		{name: "database error", collectionErr: errors.New("failed to connect to database: connection refused"), want: false},
		{name: "gap fill ignored when collection failed otherwise", collectionErr: errors.New("issues: 500"), fill: batch("x"), want: false},
	} {
		got := s.buildOutcome(false, tc.result, nil, nil, tc.collectionErr, tc.fill, nil)
		if got.forceFull != tc.want {
			t.Errorf("%s: forceFull = %v, want %v (errMsg %q)", tc.name, got.forceFull, tc.want, got.errMsg)
		}
	}
}

// TestAutoFlagErrorMessageMentionsForceFull asserts the logger message
// used when auto-flagging is descriptive enough for an operator reading
// the log to understand what happened. This is an invariant test — the
// exact phrasing can change, but the log line must include the repo id
// and the feature name.
//
// Rationale: CLAUDE.md says "everything that errors should be logged".
// Auto-flagging is a derived decision (the scheduler chose to re-collect
// everything because of an error pattern); operators need to see that.
func TestAutoFlagErrorMessageMentionsForceFull(t *testing.T) {
	// Read the scheduler.go source and grep for the log line. Source-
	// contract test because testing the logger output would require
	// plumbing a fake logger through New() and a full job completion.
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The implementation must log at WARN or INFO when it flips the flag
	// so the event is visible in normal log levels.
	if !strings.Contains(src, "force_full_recollect") && !strings.Contains(src, "force full recollect") {
		t.Error("scheduler must log when it auto-sets the force_full_collect flag — operators need to see this happened")
	}
}

// TestCompleteJobPathWiresAutoFlag ensures the scheduler calls the DB
// setter when the outcome's typed forceFull is set. This is the contract
// between the outcome and the DB state.
func TestCompleteJobPathWiresAutoFlag(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	if !strings.Contains(src, "if !outcome.success && outcome.forceFull {") {
		t.Error("runJob must arm force_full from the outcome's typed forceFull")
	}
	if strings.Contains(src, "shouldForceFullRecollect") {
		t.Error("the text matcher must not return — force_full is decided by errors.Is(err, platform.ErrPRBatch)")
	}
	if !strings.Contains(src, "SetForceFullCollect") {
		t.Error("scheduler.go must call store.SetForceFullCollect when auto-flag triggers — otherwise the flag is never persisted")
	}
}

// TestFailedJobCompleteLogsItsError (worklist 66): the `job complete` line of
// a failed job carries the error; through v0.29.68 it logged success=false
// and nothing else, so six giant repositories' 6–21 h failures had no reason
// in the log.
// Item 83 (review round 2): the line carries the message on a SUCCESSFUL
// job too (a recorded facade failure), and the source pin that keyed on
// `if !outcome.success {` had started passing by accident — the attributes
// are a function now, tested by behaviour.
func TestFailedJobCompleteLogsItsError(t *testing.T) {
	repo := &model.Repo{Owner: "chaoss", Name: "augur"}
	attrOf := func(attrs []any, key string) (any, bool) {
		for i := 0; i+1 < len(attrs); i += 2 {
			if attrs[i] == key {
				return attrs[i+1], true
			}
		}
		return nil, false
	}
	// Since item 83 `error=` appears on successful jobs too, so `success=`
	// is the line's only failure signal: asserted in every case.
	for name, out := range map[string]jobOutcome{
		"failed":                  {success: false, errMsg: "rate limited"},
		"recorded facade failure": {success: true, errMsg: "facade collection failed: exit status 128"},
		"clean":                   {success: true},
	} {
		attrs := jobCompleteAttrs(7, repo, out, time.Second)
		if got, ok := attrOf(attrs, "success"); !ok || got != out.success {
			t.Errorf("%s: the line must carry success=%v, got %v ok=%v", name, out.success, got, ok)
		}
		msg, ok := attrOf(attrs, "error")
		if out.errMsg != "" && (!ok || msg != out.errMsg) {
			t.Errorf("%s: the line must carry the message %q, got %v ok=%v", name, out.errMsg, msg, ok)
		}
		if out.errMsg == "" && ok {
			t.Errorf("%s: a clean success carries no error attribute, got %v", name, msg)
		}
	}
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	if !strings.Contains(srctest.FuncBody(t, src, "func (s *Scheduler) runJob("), `s.logger.Info("job complete", jobCompleteAttrs(job.RepoID, repo, outcome, duration)...)`) {
		t.Error("runJob must log the job-complete line through jobCompleteAttrs")
	}
}
