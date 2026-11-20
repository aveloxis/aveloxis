// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// An older answer does not act (summary/70, 4.2 and 5.4). The scheduler and a
// heal-vulnerabilities run each hold a day-long answer cache, so one process
// can apply an answer asked long before another process's decision. Each
// finding records the asked-at time of the answer that last decided it, and
// the target-set read time of the scan that last wrote or closed it; a write
// or a close acts only if its own times are not older.

type decisionRow struct {
	Open       bool
	Severity   string
	Resolution string
	Reason     string
	Asked      *time.Time
	Targets    *time.Time
}

func readDecision(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64, vulnID string) decisionRow {
	t.Helper()
	var d decisionRow
	if err := store.pool.QueryRow(ctx, `
		SELECT resolved_at IS NULL, COALESCE(severity, ''), COALESCE(version_resolution, ''), COALESCE(closed_reason, ''),
		       decided_asked_at, decided_targets_at
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND vuln_id = $2`, repoID, vulnID).
		Scan(&d.Open, &d.Severity, &d.Resolution, &d.Reason, &d.Asked, &d.Targets); err != nil {
		t.Fatal(err)
	}
	return d
}

func guardRow(id string, asked, targets time.Time, severity, resolution string) *model.VulnerabilityRow {
	return &model.VulnerabilityRow{VulnID: id, PackageName: "p", PackagePurl: "pkg:pypi/p@1.0", Ecosystem: "PyPI", Source: "osv.dev",
		DependencyKind: "direct", Severity: severity, VersionResolution: resolution, AskedAt: asked, TargetsReadAt: targets}
}

func TestAnOlderAnswerDoesNotAct(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "older-answer")
	t1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)
	const key = "|pkg:pypi/p@1.0"
	insert := func(r *model.VulnerabilityRow) {
		t.Helper()
		if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{r}); err != nil {
			t.Fatal(err)
		}
	}
	closeWith := func(asked time.Time, reasons map[string]string) int64 {
		t.Helper()
		n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, reasons,
			model.DecisionGuard{TargetsReadAt: t3, AnswerAskedAt: map[string]time.Time{"pkg:pypi/p@1.0": asked}})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Direction one: an older answer does not REOPEN what a newer answer
	// closed. The newer answer (asked t2) omits the advisory and closes it,
	// with a reason; then a run holding an answer asked at t1 reports it.
	insert(guardRow("reopen", t1, t1, "LOW", "locked"))
	if n := closeWith(t2, map[string]string{"reopen" + key: model.ClosedVersionChanged}); n != 1 {
		t.Fatalf("the newer answer closed %d findings, want 1", n)
	}
	insert(guardRow("reopen", t1, t3, "CRITICAL", "exact"))
	got := readDecision(t, store, ctx, repoID, "reopen")
	if got.Open || got.Severity != "LOW" || got.Reason != model.ClosedVersionChanged || !got.Asked.Equal(t2) {
		t.Errorf("after an older answer: %+v; want still closed, severity LOW, its reason kept, decided at t2", got)
	}
	// The columns that track the manifest refresh regardless: they come from
	// the target, not the answer.
	if got.Resolution != "exact" {
		t.Errorf("version_resolution = %q; the manifest columns refresh even when the answer is refused", got.Resolution)
	}
	// An answer asked at the same time, or later, acts.
	insert(guardRow("reopen", t2, t3, "HIGH", "exact"))
	if got := readDecision(t, store, ctx, repoID, "reopen"); !got.Open || got.Severity != "HIGH" || got.Reason != "" {
		t.Errorf("an answer as new as the decision: %+v; want reopened", got)
	}

	// Direction two: an older answer does not CLOSE what a newer answer
	// opened.
	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	insert(guardRow("close", t2, t2, "HIGH", "locked"))
	if n := closeWith(t1, nil); n != 0 {
		t.Errorf("an answer asked before the one that opened the finding closed %d", n)
	}
	if got := readDecision(t, store, ctx, repoID, "close"); !got.Open || !got.Asked.Equal(t2) {
		t.Errorf("after an older answer's close: %+v; want open, decided at t2", got)
	}
	if n := closeWith(t3, nil); n != 1 {
		t.Errorf("a newer answer closed %d, want 1", n)
	}
	if got := readDecision(t, store, ctx, repoID, "close"); got.Open || !got.Asked.Equal(t3) || !got.Targets.Equal(t3) {
		t.Errorf("after a newer answer's close: %+v; want closed, both times moved to t3", got)
	}
}

// A close because the dependency is gone has no answer to compare. It is
// governed by the target-set read time, as an upsert is too: a scan whose
// read of the targets is older than the finding's last decision changes
// nothing at all.
func TestAStaleTargetReadDoesNotAct(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "stale-targets")
	t1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	insert := func(r *model.VulnerabilityRow) {
		t.Helper()
		if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{r}); err != nil {
			t.Fatal(err)
		}
	}
	closeGone := func(targets time.Time) int64 {
		t.Helper()
		n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, nil, model.DecisionGuard{TargetsReadAt: targets})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// A stale run does not close a finding a later scan opened (from an
	// answer that may have been asked long before: t1 here).
	insert(guardRow("opened-later", t1, t2, "HIGH", "locked"))
	if n := closeGone(t1); n != 0 {
		t.Errorf("a scan that read its targets before the finding was opened closed %d", n)
	}
	if n := closeGone(t2); n != 1 {
		t.Errorf("a scan that read its targets at the same time closed %d, want 1", n)
	}
	// A close with no answer leaves the asked-at time as it was.
	if got := readDecision(t, store, ctx, repoID, "opened-later"); got.Open || !got.Asked.Equal(t1) || !got.Targets.Equal(t2) {
		t.Errorf("after the close: %+v; want closed, asked-at still t1, targets t2", got)
	}

	// A stale run's upsert does not reopen a finding a later scan closed
	// because the dependency was gone, and changes no column at all, the
	// manifest columns included.
	insert(guardRow("opened-later", t2, t1, "CRITICAL", "exact"))
	if got := readDecision(t, store, ctx, repoID, "opened-later"); got.Open || got.Severity != "HIGH" || got.Resolution != "locked" || !got.Targets.Equal(t2) {
		t.Errorf("after a stale run's upsert: %+v; want nothing changed", got)
	}
}

// A finding written before this release has no decision times: it was never
// decided under the rule, and any answer acts. So does a write that carries
// none (a caller that does not track them).
func TestAFindingWithNoDecisionTimesIsActedOn(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "no-decision")
	long := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{guardRow("legacy", time.Time{}, time.Time{}, "LOW", "locked")}); err != nil {
		t.Fatal(err)
	}
	if got := readDecision(t, store, ctx, repoID, "legacy"); got.Asked != nil || got.Targets != nil {
		t.Fatalf("a write with no times stored %+v", got)
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{guardRow("legacy", long, long, "HIGH", "locked")}); err != nil {
		t.Fatal(err)
	}
	if got := readDecision(t, store, ctx, repoID, "legacy"); got.Severity != "HIGH" || got.Asked == nil {
		t.Errorf("any answer acts on a row with no decision times: %+v", got)
	}
	// And a write with no times acts on a row that has them, and keeps them.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{guardRow("legacy", time.Time{}, time.Time{}, "MEDIUM", "locked")}); err != nil {
		t.Fatal(err)
	}
	if got := readDecision(t, store, ctx, repoID, "legacy"); got.Severity != "MEDIUM" || got.Asked == nil || !got.Asked.Equal(long) {
		t.Errorf("a write with no times: %+v; want it applied and the stored times kept", got)
	}
	if n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, nil, model.DecisionGuard{}); err != nil || n != 1 {
		t.Errorf("a close with no guard closed %d (err %v), want 1", n, err)
	}
}

// Which findings a newer answer has decided, so the scan can ask about their
// purls again, live.
func TestGetFindingsDecidedAfter(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "decided-after")
	t1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	rows := []*model.VulnerabilityRow{guardRow("newer", t2, t2, "LOW", "locked"), guardRow("same", t1, t1, "LOW", "locked"), guardRow("legacy", time.Time{}, time.Time{}, "LOW", "locked")}
	rows[1].PackagePurl, rows[2].PackagePurl = "pkg:pypi/q@1.0", "pkg:pypi/r@1.0"
	if err := store.InsertVulnerabilityBatch(ctx, repoID, rows); err != nil {
		t.Fatal(err)
	}
	// Closed rows count too: the reopen direction.
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, nil, model.DecisionGuard{}); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetFindingsDecidedAfter(ctx, repoID, map[string]time.Time{
		"newer|pkg:pypi/p@1.0": t1, "same|pkg:pypi/q@1.0": t1, "legacy|pkg:pypi/r@1.0": t1, "absent|pkg:pypi/z@1": t1})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"newer|pkg:pypi/p@1.0"}) {
		t.Errorf("decided after = %v, want only the finding a newer answer decided", got)
	}
}
