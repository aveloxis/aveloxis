// Source-contract tests for the dashboard freshness header
// (v0.18.30 monitor perf fix #5).
//
// User-visible requirement: "freshness CAN be periodic. Perhaps every 10
// minutes, and the addition of a 'last updated' timestamp, and a 'next
// update' timestamp displayed at the top of each page." These tests pin
// that the dashboard surfaces both timestamps so the user knows how
// stale the numbers are and when they'll refresh next.

package monitor

import (
	"strings"
	"testing"
)

// TestDashboardSourceUsesQueueStatsCache pins that the dashboard
// handler routes its stats lookup through the cache (not the raw
// QueueStats call). This is the change that turns "every page render =
// one GROUP BY scan" into "one GROUP BY scan per cache TTL window".
func TestDashboardSourceUsesQueueStatsCache(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleDashboard")
	if body == "" {
		t.Fatal("could not locate handleDashboard")
	}

	if strings.Contains(body, "s.store.QueueStats(ctx)") {
		t.Error("handleDashboard must NOT call s.store.QueueStats directly — that re-runs the GROUP BY " +
			"scan on every page render. Route through the cache (s.queueStatsCache.Get(ctx, ...)) " +
			"so the GROUP BY runs at most once per TTL window.")
	}
	if !strings.Contains(body, "queueStatsCache") {
		t.Error("handleDashboard must read QueueStats via the queueStatsCache field on the Server")
	}
}

// TestDashboardTemplateShowsLastUpdated pins the user-visible "last
// updated" timestamp. The exact wording is flexible; we look for any
// reasonable variant.
func TestDashboardTemplateShowsLastUpdated(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleDashboard")
	if body == "" {
		t.Skip("handleDashboard not yet refactored")
	}

	hasIndicator := strings.Contains(body, "Stats last refreshed") ||
		strings.Contains(body, "Last updated") ||
		strings.Contains(body, "last_refreshed") ||
		strings.Contains(body, "lastRefreshed")
	if !hasIndicator {
		t.Error("handleDashboard must display a 'last updated' / 'stats last refreshed' indicator " +
			"in the rendered HTML so the operator can see how fresh the data is.")
	}
}

// TestDashboardTemplateShowsNextRefresh pins the "next update"
// timestamp.
func TestDashboardTemplateShowsNextRefresh(t *testing.T) {
	src := mustReadMonitorSource(t)
	body := extractMonitorFn(src, "handleDashboard")
	if body == "" {
		t.Skip("handleDashboard not yet refactored")
	}

	hasIndicator := strings.Contains(body, "Next refresh") ||
		strings.Contains(body, "Next update") ||
		strings.Contains(body, "nextRefresh") ||
		strings.Contains(body, "next_refresh")
	if !hasIndicator {
		t.Error("handleDashboard must display a 'next refresh' / 'next update' indicator so the operator " +
			"knows when the cached stats will be re-queried.")
	}
}

// TestDashboardMetaRefreshIsLowered pins that the per-page
// auto-refresh is no longer 10 seconds. A 10-second meta-refresh on
// every browser tab compounded with the underlying GROUP BY scans is
// what made the dashboard expensive at 100K-repo scale. v0.18.30
// raises it to 60 seconds (still polls, but reads from the cached
// snapshot most of the time).
func TestDashboardMetaRefreshIsLowered(t *testing.T) {
	src := mustReadMonitorSource(t)
	if strings.Contains(src, `<meta http-equiv="refresh" content="10">`) {
		t.Error(`monitor.go must not emit <meta http-equiv="refresh" content="10"> — the 10-second ` +
			`per-tab refresh compounded with the underlying scans on a 100K-repo fleet. ` +
			`Raise to 60 seconds (or use the constant DefaultDashboardRefreshSeconds).`)
	}
}

// TestServerHasQueueStatsCacheField pins that the cache is wired into
// the Server struct (not constructed inline per request, which would
// defeat the cache).
func TestServerHasQueueStatsCacheField(t *testing.T) {
	src := mustReadMonitorSource(t)
	idx := strings.Index(src, "type Server struct {")
	if idx < 0 {
		t.Fatal("could not locate Server struct")
	}
	end := strings.Index(src[idx:], "\n}")
	if end < 0 {
		t.Fatal("could not find end of Server struct")
	}
	decl := src[idx : idx+end]
	if !strings.Contains(decl, "queueStatsCache") {
		t.Error("Server struct must hold a queueStatsCache field — a process-wide cache, not a per-request one")
	}
}

// extractMonitorFn locates a method on *Server in the given source.
func extractMonitorFn(src, name string) string {
	marker := "func (s *Server) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
