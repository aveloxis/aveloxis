// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// activity_classification_test.go — TDD suite for the v0.27.57
// contributor activity-classification ticker. Contracts pinned:
//   - the GitHub-only fetch is reached via a NARROW capability
//     interface satisfied by *github.Client (the DigestMailer /
//     breadthStore pattern) — platform.Client is NOT widened, so no
//     test fake or GitLab implementation changes (regression safety);
//   - a login the fetch got no answer about is never marked (it retries
//     next tick — a transient GraphQL outage must not burn a cooldown
//     period for 2,500 contributors); rows that were fetched are always
//     written (v0.29.56);
//   - contributors ABSENT from a chunk that COMPLETED are mark-only
//     (deleted/renamed accounts leave the claim head, v0.20.17) — even
//     when another chunk in the same tick failed (v0.29.56);
//   - the ticker rides Run's select loop under singleFlight.

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func readSchedulerFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestActivityFetcherIsNarrowCapabilityInterface(t *testing.T) {
	src := readSchedulerFile(t, "activity_classification.go")
	if !strings.Contains(src, "type contributorActivityFetcher interface") {
		t.Fatal("the GitHub-only fetch must be consumed via a scheduler-side capability interface, not by widening platform.Client (which would force every implementation and test fake to change)")
	}
	if !strings.Contains(src, "FetchContributorActivity(") {
		t.Error("the capability interface must declare FetchContributorActivity")
	}
	if !strings.Contains(src, ".(contributorActivityFetcher)") {
		t.Error("runActivityClassification must type-assert s.ghClient against the capability interface and no-op when absent (GitLab-only or fake clients)")
	}
}

// TestActivityClassificationErrorPathMarksOnlyAnsweredChunks (v0.29.56
// contract): a login the fetch got no answer about is never MARKED — it is
// unknown, not absent. A login whose own chunk COMPLETED and came back
// missing is deleted or renamed, and retires even when another chunk in the
// same tick failed; marking only on a wholly successful fetch meant that
// during a run of failing ticks nobody ever retired and the cohort pinned
// the claim head (v0.20.17). Rows that WERE fetched are always written
// (proven data, SR-3); before v0.29.56 a failed chunk discarded the whole
// tick, so a sweep whose chunks failed intermittently made no progress at
// all (7 of 7 ticks on 2026-09-17, nobody checked since 2026-09-14).
func TestActivityClassificationErrorPathMarksOnlyAnsweredChunks(t *testing.T) {
	claimed := []db.ActivityCheckContributor{{ID: "a", Login: "alice"}, {ID: "b", Login: "bob"}, {ID: "c", Login: "carol"}}
	fetched := map[string]model.ContributionActivity{"alice": {Login: "alice", CalendarTotal: 3, ContributionYears: []int{2026}}}

	// bob and carol's chunk never completed: their absence says nothing.
	updates, absent := planActivityWrites(claimed, fetched, []string{"bob", "carol"})
	if len(absent) != 0 {
		t.Errorf("marked %v absent from a chunk that did not complete — a missing login is unknown, not deleted", absent)
	}
	if len(updates) != 1 || updates[0].CntrbID != "a" {
		t.Errorf("updates = %+v, want only alice's fetched row", updates)
	}

	// Every chunk completed: the two GitHub did not return are deleted or
	// renamed and retire from the claim head.
	updates, absent = planActivityWrites(claimed, fetched, nil)
	if len(updates) != 1 || len(absent) != 2 {
		t.Errorf("successful fetch: updates=%d absent=%v, want 1 classified and bob+carol absent", len(updates), absent)
	}

	// A mixed tick: carol's chunk failed, bob's completed — bob retires, and
	// carol is left for the next tick. One failing chunk must not stop every
	// deleted account from ever retiring (the v0.20.17 claim-head lesson).
	_, absent = planActivityWrites(claimed, fetched, []string{"carol"})
	if len(absent) != 1 || absent[0] != "b" {
		t.Errorf("mixed tick: absent = %v, want only bob", absent)
	}

	// Wiring: the error return must come before any mark call. Bounded to
	// the function and comment-stripped, so prose mentioning a needle
	// cannot satisfy or invert the check (SR-12).
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/scheduler/activity_classification.go"),
		"func (s *Scheduler) runActivityClassification("))
	warn := strings.Index(body, "activity classification: some chunks failed")
	mark := strings.Index(body, "MarkActivityCheckedBatch(")
	update := strings.Index(body, "UpdateContributorActivityBatch(")
	if warn < 0 || mark < 0 || update < 0 {
		t.Fatal("cannot find the fetch-failure WARN, the update or the mark call")
	}
	// Write the fetched rows, retire the absentees of chunks that COMPLETED,
	// and only then return on the error. The previous order (warn before
	// mark) meant the per-chunk split could never fire.
	if !(update < mark && mark < warn) {
		t.Error("runActivityClassification must write fetched rows and mark completed-chunk absentees BEFORE returning on a fetch error")
	}
}

func TestActivityClassificationMarksAbsentContributors(t *testing.T) {
	src := readSchedulerFile(t, "activity_classification.go")
	if !strings.Contains(src, "MarkActivityCheckedBatch") {
		t.Error("contributors absent from a chunk that COMPLETED (deleted/renamed) must be mark-only stamped so they leave the NULLS-FIRST claim head")
	}
	if !strings.Contains(src, "UpdateContributorActivityBatch") {
		t.Error("contributors present in the fetch must get the full classified update")
	}
	if !strings.Contains(src, "ClassifyContributorActivity") {
		t.Error("classification must go through model.ClassifyContributorActivity — the single source of the class rules")
	}
}

func TestActivityTickerWiredIntoRun(t *testing.T) {
	src := readSchedulerFile(t, "scheduler.go")
	if !strings.Contains(src, "activityTicker") {
		t.Fatal("Run must declare an activityTicker for the classification sweep")
	}
	if !strings.Contains(src, "runActivityClassification") {
		t.Fatal("Run's select loop must dispatch runActivityClassification")
	}
	if !strings.Contains(src, "s.singleFlight(&s.activityClassActive") {
		t.Error("the ticker case must run under singleFlight so a slow sweep can't stack concurrent instances (the breadth/enrichment pattern)")
	}
}
