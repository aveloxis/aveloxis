// Source-contract tests for /api/queue pagination (v0.18.30 monitor
// perf fix #4).
//
// At v0.18.29, GET /api/queue called s.store.ListQueue(ctx) with no
// LIMIT/OFFSET — at 100K repos the endpoint returned a 100K-row JSON
// blob on every call. Any client polling it (the dashboard's
// JavaScript, external tooling) faced multi-second response times and
// pgx pool pressure.
//
// The fix: same pagination contract as handleDashboard. Accept page
// and page_size query params (with sensible defaults and clamps) and
// route through ListQueuePage. The response gains a meta envelope
// with total_repos / page / page_size so clients can paginate.

package monitor

import (
	"strings"
	"testing"
)

// TestHandleQueueUsesListQueuePageNotListQueue is the headline
// contract: /api/queue must NOT call ListQueue (which is unbounded).
// All paginated reads route through ListQueuePage.
func TestHandleQueueUsesListQueuePageNotListQueue(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleQueue")
	if body == "" {
		t.Fatal("could not locate handleQueue")
	}

	if strings.Contains(body, "s.store.ListQueue(") {
		t.Error("handleQueue must NOT call s.store.ListQueue — the unbounded variant returns every " +
			"row in the queue. On a 100K-repo fleet that's a massive JSON payload per request. " +
			"Route through s.store.ListQueuePage with the page/page_size from parsePageParams.")
	}
	if !strings.Contains(body, "ListQueuePage") {
		t.Error("handleQueue must use s.store.ListQueuePage so /api/queue is paginated")
	}
}

// TestHandleQueueParsesPaginationParams pins that handleQueue uses the
// existing parsePageParams helper (mirroring handleDashboard) so the
// pagination contract is consistent across the two endpoints.
func TestHandleQueueParsesPaginationParams(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleQueue")
	if body == "" {
		t.Skip("handleQueue not yet refactored")
	}

	if !strings.Contains(body, "parsePageParams") {
		t.Error("handleQueue must call parsePageParams(r) — the same helper handleDashboard uses — " +
			"so page, page_size, and search query params have consistent semantics across the two " +
			"endpoints.")
	}
}

// TestHandleQueueReturnsTotalCount pins that the JSON envelope
// includes the total row count so paginating clients can show the
// equivalent of "Page X of Y".
func TestHandleQueueReturnsTotalCount(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleQueue")
	if body == "" {
		t.Skip("handleQueue not yet refactored")
	}

	// We accept `total`, `total_repos`, or any field that surfaces the
	// count. The presence of `total` in the response envelope is what
	// matters.
	hasTotal := strings.Contains(body, `"total"`) ||
		strings.Contains(body, `"total_repos"`) ||
		strings.Contains(body, `"total_count"`)
	if !hasTotal {
		t.Error("handleQueue must include a total count in its JSON response so paginating clients " +
			"can render 'Page X of Y' navigation. ListQueuePage already returns the count.")
	}
}
