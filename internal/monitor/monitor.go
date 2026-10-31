// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package monitor provides an HTTP dashboard for observing Aveloxis collection
// progress. Serves the same purpose as Flower does for Celery, but backed by
// Postgres queue state — no additional infrastructure required.
package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/queueview"
	"github.com/aveloxis/aveloxis/internal/scheduler"
	"github.com/aveloxis/aveloxis/internal/static"
)

// Dashboard pagination bounds. The default size keeps the rendered table
// small enough that client-side JS sort stays responsive, and the cap
// prevents a hostile or careless ?page_size=10000000 from flooding the
// response and competing with collection workers for DB connections.
const (
	defaultDashboardPageSize = 100
	maxDashboardPageSize     = 500
)

// pageParams is the validated, bounded result of parsing a dashboard
// request. Offset is derived from Page and PageSize so callers pass it
// straight to ListQueuePage.
type pageParams struct {
	Page     int
	PageSize int
	Search   string
	Offset   int
	// SearchTooLong is the parser's verdict on q (queueview.MaxSearchBytes):
	// nothing can match, no query runs, the page says so (whole-branch L10
	// pass of 0.29.109: this copy dropped it and listed the whole queue).
	SearchTooLong bool
}

// parsePageParams reads page, page_size and q through the one queue-page
// parser (queueview.ParseParams, 0.29.109): the defaults and the clamp to
// maxDashboardPageSize are this dashboard's; the arithmetic is shared.
func parsePageParams(r *http.Request) pageParams {
	p := queueview.ParseParams(r.URL.Query(), defaultDashboardPageSize, maxDashboardPageSize)
	return pageParams{Page: p.Page, PageSize: p.PageSize, Search: p.Search, Offset: p.Offset, SearchTooLong: p.SearchTooLong}
}

// view is the shared loader's parameters: this dashboard sorts in the
// browser, so the store's default order is asked for.
func (p pageParams) view() queueview.Params {
	return queueview.Params{Page: p.Page, PageSize: p.PageSize, Search: p.Search, Offset: p.Offset, SearchTooLong: p.SearchTooLong}
}

// totalPages is queueview.TotalPages under the name this package's tests
// and template use.
func totalPages(total, pageSize int) int { return queueview.TotalPages(total, pageSize) }

// renderMatviewBanner writes a visible pause banner at the top of the
// dashboard while scheduler.MatviewRebuildActive is set. Operators were
// previously unable to distinguish a genuinely stuck scheduler from a
// healthy weekly rebuild; the banner makes the pause explicit.
func renderMatviewBanner(w io.Writer) {
	if !scheduler.MatviewRebuildActive.Load() {
		return
	}
	fmt.Fprint(w, `<div style="background:#fde68a;color:#78350f;padding:0.75rem 1rem;border-radius:8px;margin-bottom:1rem;border:1px solid #f59e0b;font-weight:600">Weekly materialized view rebuild in progress — collection paused. New jobs will resume automatically when the rebuild completes.</div>`)
}

// formatRelativeAgo renders a duration as "Xs ago" / "Ym Zs ago" /
// "Hh Mm ago" — short enough to fit in the dashboard header without
// wrapping but precise enough that operators can tell at a glance how
// stale the numbers are.
func formatRelativeAgo(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// QueueStatsCache is the shared queue stats cache (internal/queueview since
// 0.29.109, stage 3d) under the name this package has used since v0.18.30;
// NewQueueStatsCache and DefaultQueueStatsCacheTTL likewise.
type QueueStatsCache = queueview.StatsCache

// NewQueueStatsCache returns a cache with the given TTL.
func NewQueueStatsCache(ttl time.Duration, logger *slog.Logger) *QueueStatsCache {
	return queueview.NewStatsCache(ttl, logger)
}

// DefaultQueueStatsCacheTTL is the cache window for QueueStats results.
const DefaultQueueStatsCacheTTL = queueview.DefaultStatsTTL

// DefaultDashboardRefreshSeconds is the meta-refresh cadence emitted
// in the dashboard HTML. v0.18.30 raised this from 10s — the
// per-render scans the 10s refresh triggered against a 100K-repo
// fleet were the proximate cause of the dashboard's perceived
// slowness.
const DefaultDashboardRefreshSeconds = 60

// monitorStore is the part of *db.PostgresStore the dashboard reads, so a
// test can fault each call (PR #218 review C5).
type monitorStore interface {
	QueueStats(ctx context.Context) (map[string]int, error)
	ListQueuePage(ctx context.Context, limit, offset int, search, sortKey, sortDir string) ([]db.QueueJob, int, error)
	GetReposBatch(ctx context.Context, repoIDs []int64) (map[int64]*model.Repo, error)
	GetRepoStatsBatch(ctx context.Context, repoIDs []int64) (map[int64]*db.RepoStats, error)
	PrioritizeRepo(ctx context.Context, repoID int64) error
}

// Server is the monitoring HTTP server.
type Server struct {
	store           monitorStore
	logger          *slog.Logger
	mux             *http.ServeMux
	queueStatsCache *QueueStatsCache
	// v0.23.0: operator-configurable meta-refresh cadence. Zero
	// falls through to DefaultDashboardRefreshSeconds.
	refreshSeconds int
}

// Options configures a monitor Server beyond the required (store,
// logger). All fields are optional — zero values fall back to package
// defaults. Added in v0.23.0 so growing the configuration surface
// doesn't require breaking the New signature again.
type Options struct {
	// RefreshSeconds is the HTML meta-refresh interval emitted on
	// the dashboard. Zero falls through to DefaultDashboardRefreshSeconds.
	RefreshSeconds int
}

// New creates a monitor server with default options.
func New(store *db.PostgresStore, logger *slog.Logger) *Server {
	return NewWithOptions(store, logger, Options{})
}

// NewWithOptions creates a monitor server with explicit configuration.
// v0.23.0.
func NewWithOptions(store *db.PostgresStore, logger *slog.Logger, opts Options) *Server {
	return newServer(store, logger, opts)
}

// newServer builds the Server over any monitorStore. A nil
// *db.PostgresStore stays a (typed) nil store, as it always was.
func newServer(store monitorStore, logger *slog.Logger, opts Options) *Server {
	refresh := opts.RefreshSeconds
	if refresh <= 0 {
		refresh = DefaultDashboardRefreshSeconds
	}
	if logger == nil {
		logger = slog.Default() // one default layer: the handlers use s.logger unguarded
	}
	s := &Server{
		store:           store,
		logger:          logger,
		mux:             http.NewServeMux(),
		queueStatsCache: NewQueueStatsCache(DefaultQueueStatsCacheTTL, logger),
		refreshSeconds:  refresh,
	}
	s.mux.HandleFunc("GET /", s.handleDashboard)
	s.mux.HandleFunc("GET /api/queue", s.handleQueue)
	s.mux.HandleFunc("GET /api/stats", s.handleStats)
	s.mux.HandleFunc("POST /api/prioritize/{repoID}", s.handlePrioritize)
	s.mux.HandleFunc("GET /icon.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(static.IconPNG)
	})
	return s
}

// isMobile returns true when the request's User-Agent matches a
// known mobile pattern. Used by handleDashboard to select the
// mobile-friendly CSS variant.
//
// Detection is intentionally conservative — false negatives (showing
// the desktop layout to a mobile user) are recoverable; false
// positives (cramming the mobile layout onto a wide screen) waste
// the dashboard's whole point. The patterns below match >99% of
// real mobile traffic and almost no desktop traffic.
//
// v0.23.0.
func isMobile(r *http.Request) bool {
	ua := r.Header.Get("User-Agent")
	if ua == "" {
		return false
	}
	// Substrings checked case-insensitively against the UA.
	for _, marker := range []string{"iPhone", "iPad", "iPod", "Android", "Mobile", "Windows Phone", "BlackBerry"} {
		if strings.Contains(ua, marker) {
			return true
		}
	}
	return false
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every answer, the 404s and the mux's own 405s included (ASVS
		// V14.4.4, the 0.29.125 review): a browser must not sniff a JSON
		// answer or an error text into HTML.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		s.mux.ServeHTTP(w, r)
	})
}

// serverError is the one arm for a store failure (PR #218 review C5): the
// cause goes to the log at ERROR with the handler's name, the client gets a
// generic 500 — never the store's text. A request the client abandoned
// (context.Canceled) is not a failure and logs at Debug.
func (s *Server) serverError(w http.ResponseWriter, r *http.Request, handler string, err error) {
	if httpserver.RequestEnded(r.Context(), err) { // client gone, or http_timeout_seconds fired (its WARN reports it)
		s.logger.Debug("request ended before its handler finished", "handler", handler, "error", err)
		return
	}
	s.logger.Error("request failed", "handler", handler, "error", err)
	http.Error(w, "internal error; try again", http.StatusInternalServerError)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	// v0.18.30: route through queueStatsCache so /api/stats polls
	// don't re-trigger the GROUP BY scan on every call.
	stats, lastRefreshed, nextRefresh, err := s.queueStatsCache.Get(r.Context(), s.store.QueueStats)
	if err != nil {
		s.serverError(w, r, "handleStats", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"stats":          stats,
		"last_refreshed": lastRefreshed.Format(time.RFC3339),
		"next_refresh":   nextRefresh.Format(time.RFC3339),
	})
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	// v0.18.30: paginated. The pre-fix endpoint called ListQueue which
	// returned every row in the queue — at 100K repos that's a 100K-row
	// JSON payload per request, which any polling client (the
	// dashboard's JavaScript, external tooling) would re-fetch
	// repeatedly. Route through ListQueuePage with the same
	// page/page_size/q semantics as the dashboard. Left apart from
	// queueview.Load on purpose (L18, 0.29.109): this answer is the queue
	// rows only — no repository names, no counts — so the two enrichment
	// reads Load adds would be paid for nothing; the parameters do go
	// through the one parser.
	params := parsePageParams(r)
	var jobs []db.QueueJob
	var total int
	if !params.SearchTooLong { // an over-long q matches nothing: no query, said in the envelope
		var err error
		jobs, total, err = s.store.ListQueuePage(r.Context(), params.PageSize, params.Offset, params.Search, "", "")
		if err != nil {
			s.serverError(w, r, "handleQueue", err)
			return
		}
	}

	type jobView struct {
		RepoID        int64   `json:"repo_id"`
		Priority      int     `json:"priority"`
		Status        string  `json:"status"`
		DueAt         string  `json:"due_at"`
		LockedBy      *string `json:"locked_by,omitempty"`
		LastCollected *string `json:"last_collected,omitempty"`
		LastError     *string `json:"last_error,omitempty"`
		Issues        int     `json:"issues"`
		PRs           int     `json:"pull_requests"`
		Messages      int     `json:"messages"`
		Events        int     `json:"events"`
		Releases      int     `json:"releases"`
		Contributors  int     `json:"contributors"`
		Commits       int     `json:"commits"`
		DurationMs    int64   `json:"duration_ms"`
	}

	views := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		v := jobView{
			RepoID:       j.RepoID,
			Priority:     j.Priority,
			Status:       j.Status,
			DueAt:        j.DueAt.Format(time.RFC3339),
			LockedBy:     j.LockedBy,
			LastError:    j.LastError,
			Issues:       j.LastIssues,
			PRs:          j.LastPRs,
			Messages:     j.LastMessages,
			Events:       j.LastEvents,
			Releases:     j.LastReleases,
			Contributors: j.LastContributors,
			Commits:      j.LastCommits,
			DurationMs:   j.LastDurationMs,
		}
		if j.LastCollected != nil {
			ts := j.LastCollected.Format(time.RFC3339)
			v.LastCollected = &ts
		}
		views = append(views, v)
	}
	w.Header().Set("Content-Type", "application/json")
	env := map[string]any{
		"total":     total,
		"page":      params.Page,
		"page_size": params.PageSize,
		"jobs":      views,
	}
	if params.SearchTooLong {
		env["warnings"] = []string{queueview.WarnSearchTooLong}
	}
	json.NewEncoder(w).Encode(env)
}

func (s *Server) handlePrioritize(w http.ResponseWriter, r *http.Request) {
	// The monitor's one state change takes no credential, so a page on
	// another site could post it from the operator's browser (ASVS V4.3.3,
	// the 0.29.125 review): the same-site rule the web GUI's guard uses,
	// with no second origin admitted.
	if origin, ok := httpserver.RequestOrigin(r, nil); !ok {
		s.logger.Warn("cross-site prioritize refused", "path", r.URL.Path, "origin", origin)
		http.Error(w, "cross-site request refused", http.StatusForbidden)
		return
	}
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if err := s.store.PrioritizeRepo(r.Context(), repoID); err != nil {
		if errors.Is(err, db.ErrRepoNotInQueue) {
			http.Error(w, "repo not found in queue", http.StatusNotFound)
			return
		}
		// A store failure is not "not found" (SR-5, follow-up 12).
		s.serverError(w, r, "handlePrioritize", fmt.Errorf("prioritize repo %d: %w", repoID, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": fmt.Sprintf("repo %d pushed to top of queue", repoID),
	})
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := parsePageParams(r)

	// v0.18.30: stats served from in-memory cache (default 60s TTL).
	// At v0.18.29 every dashboard render fired QueueStats — a `SELECT
	// status, COUNT(*) … GROUP BY status` against a 100K-row queue
	// table per browser tab per 10 seconds. Now once per TTL window.
	// A failed read is a logged 500, not an empty fleet (PR #218 review C5:
	// both errors were discarded, so an outage rendered as zero jobs).
	stats, lastRefreshed, nextRefresh, err := s.queueStatsCache.Get(ctx, s.store.QueueStats)
	if err != nil {
		s.serverError(w, r, "handleDashboard", err)
		return
	}
	// The page's rows through the one loader (queueview.Load, 0.29.109):
	// the listing, the two batched enrichment reads and their failure mode
	// (a bare row, logged) are shared with the web GUI's /monitor and the
	// API's queue route.
	page, err := queueview.Load(ctx, s.store, params.view(), s.logger)
	if err != nil {
		s.serverError(w, r, "handleDashboard", err)
		return
	}
	enriched, total := page.Rows, page.Total

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// v0.23.0: meta-refresh cadence is operator-configurable via
	// monitor.refresh_seconds: the config accessor clamps the range and
	// newServer defaults a zero Option (SR-10, v0.29.98: a third clamp here
	// was removed; TestDashboardRefreshFollowsTheConfigEndToEnd).
	refreshSecs := s.refreshSeconds
	// v0.23.0: mobile-friendly bodyclass when a known mobile UA is
	// observed. The desktop CSS below applies for everyone; the
	// inline @media block AND the body.is-mobile class override the
	// table layout on small screens. We set both signals so phone
	// emulators with non-mobile UAs still get responsive sizing.
	bodyClass := ""
	if isMobile(r) {
		bodyClass = " class=\"is-mobile\""
	}
	fmt.Fprintf(w, `<!DOCTYPE html>
<html><head><title>Aveloxis Monitor</title>
<meta http-equiv="refresh" content="%d">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>`, refreshSecs)
	fmt.Fprint(w, `
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #f5f5f5; color: #333; }
  h1 { margin-bottom: 0.5rem; }
  .sub { color: #666; margin-bottom: 1.5rem; }
  .stats { display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }
  .stat { background: white; padding: 1rem 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  .stat .value { font-size: 2rem; font-weight: bold; }
  .stat .label { color: #666; font-size: 0.85rem; }
  table { border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  th, td { padding: 0.5rem 0.7rem; text-align: left; border-bottom: 1px solid #eee; font-size: 0.85rem; }
  th { background: #f8f8f8; font-weight: 600; font-size: 0.75rem; }
  .collecting { color: #2563eb; font-weight: bold; }
  .queued { color: #6b7280; }
  .error { color: #dc2626; }
  .p0 { background: #fef3c7; }
  .btn { padding: 0.25rem 0.75rem; border: 1px solid #ddd; border-radius: 4px; background: white; cursor: pointer; font-size: 0.8rem; }
  .btn:hover { background: #f0f0f0; }
  .mono { font-family: monospace; font-size: 0.8rem; }
  .gathered { color: #059669; }
  .meta { color: #6b7280; font-size: 0.8rem; }
  .count-cell { text-align: right; }
  th { cursor: pointer; user-select: none; }
  th:hover { background: #eef; }
  th .arrow { font-size: 0.6rem; margin-left: 4px; color: #999; }
  th .arrow.active { color: #2563eb; }
  /* v0.23.0: mobile layout. Applies at narrow viewports OR when
     the server detected a mobile UA and emitted body.is-mobile.
     Tables become a list of stacked cards — each row renders
     vertically with the column name prefixed via data-label.

     Wide-content fixes (v0.23.0 post-fix): word-break on cells so
     long repo URLs / error strings don't blow out the card; the
     hidden header row uses display:none (not absolute positioning,
     which can leak horizontal scroll); a handful of less-critical
     columns are hidden entirely so the cards aren't 14 stanzas
     tall per repo. The 'wide' class on the data-label suffix lets
     us collapse Gathered/Meta pairs into single rows visually. */
  * { box-sizing: border-box; }
  html, body { max-width: 100vw; overflow-x: hidden; }
  @media (max-width: 768px) {
    body { margin: 0.5rem; font-size: 0.95rem; }
    h1 { font-size: 1.3rem; }
    .sub { font-size: 0.8rem; }
    .stats { gap: 0.5rem; }
    .stat { padding: 0.5rem 0.7rem; flex: 1 1 30%; min-width: 0; }
    .stat .value { font-size: 1.4rem; }
    /* Search form: drop the inline min-width:240px on the input so
       the form fits in a 320px viewport. flex-wrap is already set
       so the select and button wrap to a second line naturally. */
    form input[type="text"] { min-width: 0 !important; flex: 1 1 100%; }
    form select, form button { flex: 0 0 auto; }
    form span { margin-left: 0 !important; flex: 1 1 100%; }
    /* Table → stacked cards. */
    table, tbody, tr, td { display: block; width: 100%; }
    thead { display: none; } /* header row hidden entirely on mobile */
    tbody tr { border: 1px solid #ddd; border-radius: 8px; margin-bottom: 0.5rem; padding: 0.5rem; background: white; }
    tbody td { border: none; padding: 0.2rem 0; font-size: 0.88rem; text-align: left;
               word-break: break-word; overflow-wrap: anywhere; }
    tbody td.count-cell { text-align: left; }
    tbody td::before { content: attr(data-label) ": "; font-weight: 600; color: #555; margin-right: 0.25rem; }
    tbody td[data-label=""]::before { content: ""; }
    /* Hide less-essential columns to keep the card compact.
       # (col 1), Platform (col 3), Priority (col 5) are rarely
       useful on a phone-sized dashboard view. */
    tbody td:nth-child(1), tbody td:nth-child(3), tbody td:nth-child(5) { display: none; }
  }
  /* Same overrides for the UA-detected mobile case, in case the
     viewport is wider than the breakpoint (some phones report odd
     widths). Mirrors the @media block above. */
  body.is-mobile { margin: 0.5rem; font-size: 0.95rem; }
  body.is-mobile h1 { font-size: 1.3rem; }
  body.is-mobile .sub { font-size: 0.8rem; }
  body.is-mobile .stat { padding: 0.5rem 0.7rem; flex: 1 1 30%; min-width: 0; }
  body.is-mobile form input[type="text"] { min-width: 0 !important; flex: 1 1 100%; }
  body.is-mobile form select, body.is-mobile form button { flex: 0 0 auto; }
  body.is-mobile form span { margin-left: 0 !important; flex: 1 1 100%; }
  body.is-mobile table, body.is-mobile tbody,
  body.is-mobile tr, body.is-mobile td { display: block; width: 100%; }
  body.is-mobile thead { display: none; }
  body.is-mobile tbody tr { border: 1px solid #ddd; border-radius: 8px; margin-bottom: 0.5rem; padding: 0.5rem; background: white; }
  body.is-mobile tbody td { border: none; padding: 0.2rem 0; font-size: 0.88rem; text-align: left;
                            word-break: break-word; overflow-wrap: anywhere; }
  body.is-mobile tbody td.count-cell { text-align: left; }
  body.is-mobile tbody td::before { content: attr(data-label) ": "; font-weight: 600; color: #555; margin-right: 0.25rem; }
  body.is-mobile tbody td[data-label=""]::before { content: ""; }
  body.is-mobile tbody td:nth-child(1),
  body.is-mobile tbody td:nth-child(3),
  body.is-mobile tbody td:nth-child(5) { display: none; }
</style>
<script>
// Client-side table sorting. Click a column header to sort ascending, click again for descending.
let sortCol = -1, sortAsc = true;
function sortTable(col) {
  const table = document.querySelector('table');
  const tbody = table.querySelector('tbody') || table;
  const rows = Array.from(tbody.querySelectorAll('tr')).slice(1); // skip header
  if (sortCol === col) { sortAsc = !sortAsc; } else { sortCol = col; sortAsc = true; }
  rows.sort((a, b) => {
    let av = a.cells[col].textContent.trim();
    let bv = b.cells[col].textContent.trim();
    // Try numeric comparison first.
    const an = parseFloat(av.replace(/[^0-9.\-]/g, ''));
    const bn = parseFloat(bv.replace(/[^0-9.\-]/g, ''));
    if (!isNaN(an) && !isNaN(bn)) { return sortAsc ? an - bn : bn - an; }
    return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(r => tbody.appendChild(r));
  // Update arrow indicators.
  document.querySelectorAll('th .arrow').forEach(a => { a.className = 'arrow'; a.textContent = '\u25B4'; });
  const arrow = table.querySelectorAll('th')[col]?.querySelector('.arrow');
  if (arrow) { arrow.className = 'arrow active'; arrow.textContent = sortAsc ? '\u25B4' : '\u25BE'; }
}
</script>
</head>`)
	fmt.Fprintf(w, `<body%s>
<div style="display:flex;align-items:center;justify-content:space-between"><h1>Aveloxis Monitor</h1><img src="/icon.png" alt="Aveloxis" style="height:48px;border-radius:8px"></div>
`, bodyClass)
	// v0.18.30: freshness header. Shows when the cached fleet stats
	// were last refreshed and when the next refresh is due. Surfaces
	// the trade-off the user explicitly accepted ("freshness CAN be
	// periodic") so it's clear how stale numbers can be.
	lastRefreshedTxt := "just now"
	if !lastRefreshed.IsZero() {
		lastRefreshedTxt = formatRelativeAgo(time.Since(lastRefreshed))
	}
	nextRefreshTxt := "—"
	if !nextRefresh.IsZero() {
		dur := time.Until(nextRefresh)
		if dur < 0 {
			nextRefreshTxt = "due now"
		} else {
			nextRefreshTxt = "in " + formatRelativeAgo(dur)
		}
	}
	fmt.Fprintf(w, `<div class="sub">Auto-refreshes every %ds. Stats last refreshed %s. Next refresh %s. API: <code>aveloxis api --addr :8383</code></div>`,
		refreshSecs, lastRefreshedTxt, nextRefreshTxt)

	renderMatviewBanner(w)

	fmt.Fprint(w, `<div class="stats">`)
	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Total</div></div>`, stats["total"])
	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Queued</div></div>`, stats["queued"])
	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Collecting</div></div>`, stats["collecting"])
	// v0.29.64: repositories parked by the startup staging drain or by
	// heal-collection-gaps hold no worker slot; counting them as
	// Collecting showed more jobs than workers.
	fmt.Fprintf(w, `<div class="stat" title="Parked by serve's leftover-staging drain or by heal-collection-gaps; they hold no worker slot"><div class="value">%d</div><div class="label">Parked (drain / heal)</div></div>`, stats["draining"])
	fmt.Fprint(w, `</div>`)
	// A failed enrichment read is said, not shown as zeros (queueview.Page).
	for _, warn := range page.Warnings {
		fmt.Fprintf(w, `<div style="background:#fde68a;color:#78350f;padding:0.75rem 1rem;border-radius:8px;margin-bottom:1rem;border:1px solid #f59e0b">%s</div>`, template.HTMLEscapeString(warn))
	}

	// Search box + page-size selector. Server-side search replaces the
	// Ctrl-F workflow from the all-rows-at-once era: much better for
	// known lookups on large fleets.
	escSearch := template.HTMLEscapeString(params.Search)
	fmt.Fprintf(w, `<form method="GET" action="/" style="margin-bottom:1rem;display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap">
<input type="text" name="q" value="%s" placeholder="Search owner/name…" style="padding:0.4rem 0.6rem;border:1px solid #ddd;border-radius:4px;min-width:240px">
<select name="page_size" style="padding:0.4rem 0.6rem;border:1px solid #ddd;border-radius:4px">`, escSearch)
	for _, sz := range []int{50, 100, 200, 500} {
		selected := ""
		if sz == params.PageSize {
			selected = ` selected`
		}
		fmt.Fprintf(w, `<option value="%d"%s>%d per page</option>`, sz, selected, sz)
	}
	fmt.Fprint(w, `</select>
<button class="btn" type="submit">Apply</button>`)
	if params.Search != "" || params.PageSize != defaultDashboardPageSize {
		fmt.Fprint(w, ` <a class="btn" href="/" style="text-decoration:none;color:inherit">Clear</a>`)
	}
	fmt.Fprintf(w, `<span style="color:#666;font-size:0.85rem;margin-left:auto">Matched %d repos</span></form>`, total)

	fmt.Fprint(w, `<table>
<tr>
  <th onclick="sortTable(0)"># <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(1)">Repo <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(2)">Platform <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(3)">Status <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(4)">Priority <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(5)">Due <span class="arrow">&#9652;</span></th>
  <th onclick="sortTable(6)">Last Run <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(7)">Gathered Issues <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(8)">Meta Issues <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(9)">Gathered PRs <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(10)">Meta PRs <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(11)">Gathered Commits <span class="arrow">&#9652;</span></th>
  <th class="count-cell" onclick="sortTable(12)">Meta Commits <span class="arrow">&#9652;</span></th>
  <th>Action</th>
</tr>`)

	for i, j := range enriched {
		statusClass := j.Status
		rowClass := ""
		if j.Priority == 0 {
			rowClass = ` class="p0"`
		}

		// Match the Last Run column format so the date is visible — critical
		// now that due_at can be up to days_until_recollect days in the future.
		due := j.DueAt.Format("Jan 2 15:04")
		if j.DueAt.Before(time.Now()) && j.Status == "queued" {
			due = "now"
		}

		lastRun := "-"
		if j.LastCollected != nil {
			lastRun = j.LastCollected.Format("Jan 2 15:04")
			if j.LastDurationMs > 0 {
				lastRun += fmt.Sprintf(" (%ds)", j.LastDurationMs/1000)
			}
		}

		worker := ""
		if j.LockedBy != nil {
			worker = fmt.Sprintf(` <span class="mono">%s</span>`, template.HTMLEscapeString(*j.LockedBy))
		}

		errInfo := ""
		if j.LastError != nil && *j.LastError != "" {
			errInfo = fmt.Sprintf(` <span class="error" title="%s">err</span>`, template.HTMLEscapeString(*j.LastError))
		}

		// HTML-escape user-controlled values to prevent stored XSS.
		// repo_owner/repo_name come from user-submitted URLs via the web GUI.
		// The label is the id when the names could not be read (never a
		// bare slash; the banner above the table says why).
		escLabel := template.HTMLEscapeString(j.Label())

		// v0.23.0: every <td> carries a data-label="..." attribute so
		// the mobile stacked-card layout (CSS above) can prefix the
		// column name. Column order matches the <thead> emitted
		// earlier so client-side JS sort still indexes correctly.
		fmt.Fprintf(w, `<tr%s><td data-label="#">%d</td><td data-label="Repo">%s</td><td data-label="Platform">%s</td><td data-label="Status" class="%s">%s%s%s</td><td data-label="Priority">%d</td><td data-label="Due">%s</td><td data-label="Last Run">%s</td>`,
			rowClass, i+1, escLabel, j.Plat,
			statusClass, j.Status, worker, errInfo,
			j.Priority, due, lastRun)

		// Gathered vs Metadata columns.
		fmt.Fprintf(w, `<td data-label="Gathered Issues" class="count-cell"><span class="gathered">%d</span></td><td data-label="Meta Issues" class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredIssues, j.MetaIssues)
		fmt.Fprintf(w, `<td data-label="Gathered PRs" class="count-cell"><span class="gathered">%d</span></td><td data-label="Meta PRs" class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredPRs, j.MetaPRs)
		fmt.Fprintf(w, `<td data-label="Gathered Commits" class="count-cell"><span class="gathered">%d</span></td><td data-label="Meta Commits" class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredCommits, j.MetaCommits)

		fmt.Fprint(w, `<td data-label="">`)
		if j.Status == "queued" {
			fmt.Fprintf(w, `<form method="POST" action="/api/prioritize/%d" style="display:inline"><button class="btn" type="submit">Boost</button></form>`, j.RepoID)
		}
		fmt.Fprint(w, `</td></tr>`)
	}

	fmt.Fprint(w, `</table>`)

	// Pagination controls. Page links preserve the active search and
	// page_size so the three controls compose cleanly.
	//
	// XSS note: params.Search is user-controlled. Build the query with
	// url.Values (URL-escapes each value) and then HTML-escape the
	// whole result before interpolating into the href — the HTML pass
	// turns "&" into "&amp;" per HTML spec AND gives a static analyzer
	// an obvious "HTML-escaped" trust boundary. Previously the raw "&"
	// separator also meant href="/?page=1&q=foo" was not spec-correct
	// HTML.
	pages := totalPages(total, params.PageSize)
	if pages > 1 {
		qs := url.Values{}
		qs.Set("page_size", strconv.Itoa(params.PageSize))
		if params.Search != "" {
			qs.Set("q", params.Search)
		}
		qsNoPage := template.HTMLEscapeString(qs.Encode())
		fmt.Fprintf(w, `<div style="display:flex;gap:0.5rem;margin-top:1rem;align-items:center;font-size:0.9rem">`)
		if params.Page > 1 {
			fmt.Fprintf(w, `<a class="btn" href="/?page=%d&amp;%s" style="text-decoration:none;color:inherit">&larr; Prev</a>`, params.Page-1, qsNoPage)
		}
		fmt.Fprintf(w, `<span style="color:#666">Page %d of %d</span>`, params.Page, pages)
		if params.Page < pages {
			fmt.Fprintf(w, `<a class="btn" href="/?page=%d&amp;%s" style="text-decoration:none;color:inherit">Next &rarr;</a>`, params.Page+1, qsNoPage)
		}
		fmt.Fprint(w, `</div>`)
	}

	fmt.Fprint(w, `<p style="color:#999;margin-top:1rem;font-size:0.8rem">
API: GET /api/queue | GET /api/stats | POST /api/prioritize/{repoID} | REST API: aveloxis api --addr :8383
</p></body></html>`)
}
