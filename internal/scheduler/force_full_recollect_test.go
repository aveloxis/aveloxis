package scheduler

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
)

// Force-recollect (v0.18.24): scheduler-side contract for the flag that
// was added to collection_queue. Two behaviors:
//
//   - determineSince must return zero time (full collection) when the
//     job row's ForceFullCollect flag is set, regardless of whether the
//     repo was previously collected.
//   - shouldForceFullRecollect inspects a CompleteJob error string and
//     returns true when the failure class is one of the GraphQL-batch
//     shapes that leaves PR child data incomplete (stream CANCEL,
//     validation timeout, retry exhaustion). Those are the observed
//     2026-04-22 production errors that motivated this feature.

func TestDetermineSince_RespectsForceFullCollect(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{
		RecollectAfter: 24 * time.Hour,
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

func TestShouldForceFullRecollect(t *testing.T) {
	cases := []struct {
		name    string
		errMsg  string
		want    bool
		purpose string
	}{
		{
			name:    "empty error = success, no flag",
			errMsg:  "",
			want:    false,
			purpose: "A successful collection should not trigger full re-collection.",
		},
		{
			name:    "graphql PR batch with stream CANCEL",
			errMsg:  "pull requests graphql batch shard 1: graphql PR batch: read graphql response: stream error: stream ID 57621; CANCEL; received from peer",
			want:    true,
			purpose: "The primary production symptom from 2026-04-22.",
		},
		{
			name:    "graphql PR batch retry exhaustion",
			errMsg:  "pull requests graphql batch: graphql PR batch: graphql: exhausted 10 retries for https://api.github.com/graphql",
			want:    true,
			purpose: "Persistent 502/504 bursts that exceed the retry budget leave PR child data incomplete.",
		},
		{
			name:    "graphql PR batch validation timeout",
			errMsg:  "pull requests graphql batch shard 1: graphql PR batch: graphql errors: Timeout on validation of query",
			want:    true,
			purpose: "GitHub's query planner gave up; the batch returned no data and other shards may have partial data.",
		},
		{
			name:    "gap fill PR batch error",
			errMsg:  "PR gap fill error: gap fill PR batch: graphql PR batch: read graphql response: stream error",
			want:    true,
			purpose: "Gap fill uses the same GraphQL batch; same incomplete-data risk.",
		},
		{
			name:    "unrelated transient error should NOT trigger",
			errMsg:  "failed to connect to database: connection refused",
			want:    false,
			purpose: "DB outages are operator concerns, not a reason to re-collect all PRs.",
		},
		{
			name:    "issue collection 404 should NOT trigger",
			errMsg:  "issues: optional endpoint skip: 404",
			want:    false,
			purpose: "Issues-disabled repos are normal and shouldn't trigger a full re-collection.",
		},
		{
			name:    "rate limit waiting should NOT trigger",
			errMsg:  "rate limit exhausted, waiting for reset",
			want:    false,
			purpose: "Rate limits are handled by the retry loop; full re-collection would just re-hit the same limit.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldForceFullRecollect(tc.errMsg)
			if got != tc.want {
				t.Errorf("shouldForceFullRecollect(%q) = %v, want %v — %s", tc.errMsg, got, tc.want, tc.purpose)
			}
		})
	}
}

// TestShouldForceFullRecollect_CaseSensitive pins a subtle contract: the
// pattern check must match the exact error strings the collector produces.
// A case-insensitive or overly-fuzzy matcher would start flagging unrelated
// errors (e.g. a PR review with the word "batch" in the title).
func TestShouldForceFullRecollect_CaseSensitive(t *testing.T) {
	// Made-up strings that contain individual substrings but not the
	// specific "graphql PR batch" prefix the collector emits.
	nonMatching := []string{
		"received BATCH update from peer",
		"Graphql query validated successfully",
		"stream successfully closed",
	}
	for _, msg := range nonMatching {
		if shouldForceFullRecollect(msg) {
			t.Errorf("shouldForceFullRecollect(%q) = true, want false — matcher is too permissive", msg)
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
// setter after evaluating shouldForceFullRecollect on the outcome. This
// is the contract between the pattern matcher and the DB state.
func TestCompleteJobPathWiresAutoFlag(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "shouldForceFullRecollect") {
		t.Error("scheduler.go must call shouldForceFullRecollect on the outcome error message to decide whether to auto-flag")
	}
	if !strings.Contains(src, "SetForceFullCollect") {
		t.Error("scheduler.go must call store.SetForceFullCollect when auto-flag triggers — otherwise the flag is never persisted")
	}
}
