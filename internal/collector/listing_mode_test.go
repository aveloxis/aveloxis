package collector

import (
	"os"
	"strings"
	"testing"
)

// TestListingModeGateInStagedCollector — source-contract test for the
// phase 2 feature gate. When cfg.Collection.ListingMode == "graphql",
// the staged collector must use platform.Client.ListIssuesAndPRs to
// enumerate in one unified GraphQL call instead of the two separate
// REST iterators (ListIssues + ListPullRequests). In "rest" mode the
// collector uses the existing two-iterator path byte-for-byte.
//
// Without this gate, phase 2's new method is unreachable and the
// equivalence test can't tell the two modes apart.
func TestListingModeGateInStagedCollector(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ListingMode") {
		t.Error("staged.go must reference ListingMode to gate between REST and GraphQL " +
			"listing paths — without the gate, operators can't opt in to the GraphQL " +
			"enumerator and the equivalence test harness can't compare the two")
	}
	if !strings.Contains(code, "ListIssuesAndPRs") {
		t.Error("staged.go must call ListIssuesAndPRs when mode is 'graphql' — otherwise " +
			"the gate has no effect")
	}
}

// TestListingModeConfigExists — the config struct must declare the field
// with a snake_case JSON tag and default to "rest" (the pre-phase-2
// behavior) so existing deployments pick up v0.18.2 without a behavior
// change until operators explicitly opt in.
func TestListingModeConfigExists(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ListingMode") {
		t.Error("CollectionConfig must declare ListingMode to control the issue+PR " +
			"listing REST↔GraphQL gate")
	}
	if !strings.Contains(code, `json:"listing_mode"`) {
		t.Error("ListingMode must have the json tag 'listing_mode' (snake_case convention)")
	}
}

// TestListingModeFlowsThroughScheduler — Scheduler.Config must carry the
// mode from cmd wiring down to the collector constructor. Without the
// plumbing, aveloxis.json's listing_mode value is silently ignored.
func TestListingModeFlowsThroughScheduler(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ListingMode") {
		t.Error("scheduler.Config must carry ListingMode so cmd can plumb the config " +
			"value through to the collector")
	}
}

// TestListingModeRestPathUnchanged — defense-in-depth. The rest-mode
// branch must still use the two legacy iterators (ListIssues +
// ListPullRequests) so the existing REST shadow baseline stays valid
// across phase 2. A refactor that accidentally removes the REST path
// would invalidate the baseline and cost operators a multi-hour
// re-collection.
func TestListingModeRestPathUnchanged(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Both legacy iterators must still be callable from the staged
	// collector (from the rest-mode branch).
	if !strings.Contains(code, "ListIssues(") {
		t.Error("staged.go must still call ListIssues in rest-mode to preserve " +
			"pre-phase-2 behavior")
	}
	if !strings.Contains(code, "ListPullRequests(") {
		t.Error("staged.go must still call ListPullRequests in rest-mode to preserve " +
			"pre-phase-2 behavior")
	}
}
