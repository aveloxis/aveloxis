// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// activity_classification_test.go — TDD suite for the v0.27.57
// contributor activity-classification ticker. Contracts pinned:
//   - the GitHub-only fetch is reached via a NARROW capability
//     interface satisfied by *github.Client (the digestMailer /
//     breadthStore pattern) — platform.Client is NOT widened, so no
//     test fake or GitLab implementation changes (regression safety);
//   - a failed fetch marks NOTHING (the whole batch retries next
//     tick — a transient GraphQL outage must not burn a cooldown
//     period for 2,500 contributors);
//   - contributors ABSENT from a successful fetch are mark-only
//     (deleted/renamed accounts leave the claim head, v0.20.17);
//   - the ticker rides Run's select loop under singleFlight.

import (
	"os"
	"strings"
	"testing"
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

func TestActivityClassificationErrorPathMarksNothing(t *testing.T) {
	src := readSchedulerFile(t, "activity_classification.go")
	idx := strings.Index(src, "FetchContributorActivity(")
	if idx < 0 {
		t.Fatal("cannot find the fetch call")
	}
	// The error-handling window immediately after the fetch: it must
	// return without any Mark/Update call, so the batch stays claimable
	// for the next tick instead of burning a cooldown on an outage.
	window := src[idx:]
	if end := strings.Index(window, "// classification split"); end > 0 {
		window = window[:end]
	}
	if !strings.Contains(window, "return") {
		t.Fatal("the fetch-error path must return early")
	}
	if strings.Contains(window, "MarkActivityCheckedBatch") || strings.Contains(window, "UpdateContributorActivityBatch") {
		t.Error("the fetch-error path must not mark or update anything — marking on outage would stamp 2,500 contributors with no data for a full cooldown period")
	}
}

func TestActivityClassificationMarksAbsentContributors(t *testing.T) {
	src := readSchedulerFile(t, "activity_classification.go")
	if !strings.Contains(src, "MarkActivityCheckedBatch") {
		t.Error("contributors absent from a successful fetch (deleted/renamed) must be mark-only stamped so they leave the NULLS-FIRST claim head")
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
