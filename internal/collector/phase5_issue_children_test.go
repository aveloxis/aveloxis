// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestPhase5IssueAndPRBatchCarriesLabelsAndAssignees — source-contract
// test pinning that the platform.IssueAndPRBatch envelope carries the
// inline issue labels and assignees delivered by phase 5's GraphQL
// listing.
//
// Without these map fields, the mappers have nowhere to put label /
// assignee data and the staged collector has nothing to drain, so
// phase 5 would silently fall back to the REST per-issue iterators in
// every mode — making the config flag a no-op.
func TestPhase5IssueAndPRBatchCarriesLabelsAndAssignees(t *testing.T) {
	src, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Whitespace-tolerant regex: gofmt aligns struct fields in columns,
	// so a longer-named sibling field shifts spacing here.
	labelsRE := regexp.MustCompile(`\bIssueLabels\s+map\[int\]\[\]model\.IssueLabel\b`)
	if !labelsRE.MatchString(code) {
		t.Error("platform.IssueAndPRBatch must declare " +
			"IssueLabels map[int][]model.IssueLabel for inline issue labels " +
			"delivered by phase 5's GraphQL listing — keyed by issue number")
	}
	assigneesRE := regexp.MustCompile(`\bIssueAssignees\s+map\[int\]\[\]model\.IssueAssignee\b`)
	if !assigneesRE.MatchString(code) {
		t.Error("platform.IssueAndPRBatch must declare " +
			"IssueAssignees map[int][]model.IssueAssignee for inline issue " +
			"assignees delivered by phase 5's GraphQL listing — keyed by issue number")
	}
}

// TestPhase5ListIssuesGraphQLSelectsLabelsAndAssignees — source-contract
// test pinning that the GraphQL issues listing query selects labels and
// assignees connections inline.
//
// If a refactor drops these selections from the query but leaves the
// surrounding map plumbing in place, every map would silently be empty
// and the staged collector would fall back to REST without telling
// anyone — collapsing the phase 5 speedup to zero.
func TestPhase5ListIssuesGraphQLSelectsLabelsAndAssignees(t *testing.T) {
	src, err := os.ReadFile("../platform/github/graphql_listing.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The two connection selections must appear in listIssuesGraphQL's
	// embedded query string.
	if !strings.Contains(code, "labels(first: 100)") {
		t.Error("listIssuesGraphQL must select labels(first: 100) on each " +
			"issue node — phase 5 of the REST → GraphQL refactor")
	}
	if !strings.Contains(code, "assignees(first: 100)") {
		t.Error("listIssuesGraphQL must select assignees(first: 100) on each " +
			"issue node — phase 5 of the REST → GraphQL refactor")
	}
}

// TestPhase5PaginateIssueLabelsAndAssigneesHelpersExist — source-contract
// test pinning the two pagination follow-up helpers for issues with
// >100 labels or >100 assignees. Without them, pathological issues
// silently lose data past the first 100 entries.
func TestPhase5PaginateIssueLabelsAndAssigneesHelpersExist(t *testing.T) {
	src, err := os.ReadFile("../platform/github/graphql_listing.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "func (c *Client) paginateIssueLabels(") {
		t.Error("graphql_listing.go must define paginateIssueLabels — issues " +
			"with more than 100 labels lose data past the first page without it")
	}
	if !strings.Contains(code, "func (c *Client) paginateIssueAssignees(") {
		t.Error("graphql_listing.go must define paginateIssueAssignees — issues " +
			"with more than 100 assignees lose data past the first page without it")
	}
	// Both helpers must actually be CALLED from listIssuesGraphQL on the
	// HasNextPage branch, not just defined. A refactor that drops the call
	// site reintroduces the truncation bug.
	if !strings.Contains(code, "c.paginateIssueLabels(") {
		t.Error("listIssuesGraphQL must call paginateIssueLabels when " +
			"Labels.PageInfo.HasNextPage is true on an issue node")
	}
	if !strings.Contains(code, "c.paginateIssueAssignees(") {
		t.Error("listIssuesGraphQL must call paginateIssueAssignees when " +
			"Assignees.PageInfo.HasNextPage is true on an issue node")
	}
}

// TestPhase5CollectIssuesSkipsRESTChildrenInGraphQLMode — source-
// contract test pinning that collectIssues skips the per-issue
// ListIssueLabels and ListIssueAssignees REST iterators when the
// staged collector is configured with issueChildMode=graphql AND the
// inline maps from ListIssuesAndPRs are non-nil.
//
// Without this gate, phase 5 buys us the inline GraphQL fetch but then
// the REST iterators still run alongside it — double the work, no
// speedup. With the gate, the REST loop only runs in rest mode or
// when the inline maps are nil (e.g., GitLab REST composition path).
func TestPhase5CollectIssuesSkipsRESTChildrenInGraphQLMode(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "issueChildMode") {
		t.Error("staged.go must define an issueChildMode field on " +
			"StagedCollector — phase 5 config gate for inline issue label / " +
			"assignee fetching via GraphQL")
	}
	// The gate must appear in collectIssues and be paired with a map-non-nil
	// check so the REST fallback still fires when the inline maps are absent
	// (GitLab path, or graphql listing failed and fell back).
	if !strings.Contains(code, "preLabels") {
		t.Error("collectIssues must accept a preLabels map parameter — " +
			"phase 5 drains inline labels by issue number into the stagedIssue " +
			"envelope without touching ListIssueLabels")
	}
	if !strings.Contains(code, "preAssignees") {
		t.Error("collectIssues must accept a preAssignees map parameter — " +
			"phase 5 drains inline assignees by issue number into the " +
			"stagedIssue envelope without touching ListIssueAssignees")
	}
}

// TestPhase5StagedCollectorConstructorAcceptsIssueChildMode —
// source-contract test pinning the explicit constructor accepts an
// issueChildMode parameter and threads it through to the struct.
// Without this wiring the config field set in aveloxis.json never
// reaches the StagedCollector and the gate stays at its default.
func TestPhase5StagedCollectorConstructorAcceptsIssueChildMode(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// One of the constructor signatures must accept the new mode. The
	// established pattern (phases 1, 2, 3) added a new "WithAll…" variant
	// per phase. Phase 5 may extend NewStagedCollectorWithAllModes or
	// introduce a phase-5-named constructor — either is acceptable as
	// long as the field is threaded through.
	if !strings.Contains(code, "issueChildMode") {
		t.Error("StagedCollector must thread issueChildMode through a " +
			"constructor — phase 5 wiring from CollectionConfig.IssueChildMode")
	}
	if !strings.Contains(code, "sc.issueChildMode") &&
		!strings.Contains(code, "issueChildMode: issueChildMode") &&
		!strings.Contains(code, "issueChildMode:   issueChildMode") {
		t.Error("StagedCollector constructor must set the struct's " +
			"issueChildMode field from a parameter (literal assignment in " +
			"struct literal or via a sc.issueChildMode = … line)")
	}
}

// TestPhase5ConfigHasIssueChildMode — source-contract test pinning
// the CollectionConfig field + JSON tag for operator-facing config.
//
// Without the field, the JSON parser silently ignores any
// "issue_child_mode" key in aveloxis.json and the config flag does
// nothing — exactly the failure mode v0.20.18 ripped out for
// `batch_size`.
func TestPhase5ConfigHasIssueChildMode(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "IssueChildMode") {
		t.Error("CollectionConfig must declare an IssueChildMode field — " +
			"phase 5 operator-facing config flag for inline GraphQL issue children")
	}
	if !strings.Contains(code, `json:"issue_child_mode"`) {
		t.Error("CollectionConfig.IssueChildMode must carry " +
			`json:"issue_child_mode"` + " — operators set this in aveloxis.json")
	}
	// v0.22.3 (phase 5.2): default flipped to "graphql" after
	// shadow-diff confirmed zero regressions on phase-5 target
	// tables. REST mode remains available as the escape hatch via
	// `"issue_child_mode": "rest"` in aveloxis.json — same posture
	// as pr_child_mode kept after its v0.19.0 default flip.
	if !strings.Contains(code, `IssueChildMode:          "graphql"`) &&
		!strings.Contains(code, `IssueChildMode: "graphql"`) {
		t.Error("DefaultConfig must set IssueChildMode: \"graphql\" — v0.22.3 " +
			"phase 5.2 default flip after shadow-diff equivalence on augur. " +
			"REST stays available as escape hatch; the field hasn't been removed.")
	}
}

// TestPhase5SchedulerConfigHasIssueChildMode — source-contract test
// pinning scheduler.Config carries the field. Without it, main.go's
// wiring from cfg.Collection.IssueChildMode to the scheduler has
// nowhere to land.
func TestPhase5SchedulerConfigHasIssueChildMode(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "IssueChildMode") {
		t.Error("scheduler.Config must declare an IssueChildMode field — " +
			"phase 5 wiring from CollectionConfig through to NewStagedCollector…")
	}
}
