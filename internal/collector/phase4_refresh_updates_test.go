package collector

import (
	"os"
	"strings"
	"testing"
)

// TestPhase4RefreshOpenUpdatesStillFetchComments — source-contract test.
//
// Phase 4 skips the repo-wide /issues/comments iterator only in the primary
// staged collector's collectMessages. The refresh-open path
// (OpenItemRefresher.refreshIssues / .refreshPRs) has a SEPARATE safety-net
// fetch per open item — `ListCommentsForIssue`, `ListCommentsForPR`, and
// `ListReviewCommentsForPR` — introduced in v0.16.12 to catch comments
// that the primary path missed (rate-limited, failed, or outside the
// since-window for gap-filled items).
//
// Phase 4 must not remove those per-item REST calls. If a future refactor
// accidentally drops them in the name of "fully GraphQL" — say by trying
// to reuse StagedPR.Comments from FetchPRBatch — then:
//
//   - Open issues / PRs that gain a NEW comment between collection cycles
//     won't get it refreshed (the primary staged path runs with a since
//     filter that may miss items with only a body edit).
//   - Review-inline comments would silently lose side / startSide again.
//
// This test pins the three per-item comment calls at the source level so
// a drop-by-refactor fails loudly instead of silently losing comment
// refreshes across restarts.
func TestPhase4RefreshOpenUpdatesStillFetchComments(t *testing.T) {
	src, err := os.ReadFile("refresh_open.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "r.client.ListCommentsForIssue(") {
		t.Error("refresh_open.go must call ListCommentsForIssue per refreshed issue — " +
			"otherwise open issues that gain comments between cycles stop receiving " +
			"comment updates")
	}
	if !strings.Contains(code, "r.client.ListCommentsForPR(") {
		t.Error("refresh_open.go must call ListCommentsForPR per refreshed PR — " +
			"otherwise open PRs that gain conversation comments between cycles stop " +
			"receiving comment updates")
	}
	if !strings.Contains(code, "r.client.ListReviewCommentsForPR(") {
		t.Error("refresh_open.go must call ListReviewCommentsForPR per refreshed PR — " +
			"otherwise open PRs that gain inline review comments between cycles stop " +
			"receiving comment updates (and lose side / startSide fidelity)")
	}
}

// TestPhase4GapFillStillFetchesCommentsForBackfilledItems — source-contract
// test for the gap-fill path, which is the second independent code path
// that needs to retain per-item REST comment calls.
//
// Gap fill backfills historical issues / PRs whose age is outside any
// since-window. The primary staged collector can't cover them (they're
// older than `days_until_recollect`). Without per-item comment calls,
// backfilled items would permanently lack their comments — a silent
// data-loss bug that motivated the v0.16.12 fix in the first place.
func TestPhase4GapFillStillFetchesCommentsForBackfilledItems(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "gf.client.ListCommentsForIssue(") {
		t.Error("gap_fill.go must call ListCommentsForIssue per backfilled issue — " +
			"otherwise historical issues brought in by gap fill permanently lack " +
			"their conversation comments (the v0.16.12 regression)")
	}
	if !strings.Contains(code, "gf.client.ListCommentsForPR(") {
		t.Error("gap_fill.go must call ListCommentsForPR per backfilled PR — " +
			"otherwise historical PRs brought in by gap fill permanently lack their " +
			"conversation comments")
	}
	if !strings.Contains(code, "gf.client.ListReviewCommentsForPR(") {
		t.Error("gap_fill.go must call ListReviewCommentsForPR per backfilled PR — " +
			"otherwise historical PRs brought in by gap fill permanently lack their " +
			"inline review comments")
	}
}

// TestPhase4RefreshPRCoreStillUsesFetchPRBatch — phase 1 moved PR core +
// children refresh onto FetchPRBatch when prChildMode=graphql, and phase 4
// must not regress that. This test pins the GraphQL refresh path at the
// source level so the PR core refresh keeps the phase-1 speedup regardless
// of any phase-4 follow-up refactors.
func TestPhase4RefreshPRCoreStillUsesFetchPRBatch(t *testing.T) {
	src, err := os.ReadFile("refresh_open.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "r.client.FetchPRBatch(") {
		t.Error("refresh_open.go must call FetchPRBatch for the graphql refresh path — " +
			"otherwise open-PR state / reviews / labels / etc. fall back to the " +
			"per-PR REST waterfall on every incremental cycle")
	}
	if !strings.Contains(code, "fetchPRsForRefreshGraphQL") {
		t.Error("refresh_open.go must have a graphql refresh path distinct from REST — " +
			"the mode gate selects between fetchPRsForRefreshREST and " +
			"fetchPRsForRefreshGraphQL")
	}
}
