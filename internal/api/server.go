// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package api provides a REST API for Aveloxis data access.
// Started via `aveloxis api` as a separate process from the collection scheduler.
// The web GUI and monitoring dashboard call this API for repo statistics.
//
// Endpoints:
//
//	GET /api/v1/repos/{repoID}/stats    — gathered vs metadata counts for one repo
//	GET /api/v1/repos/stats?ids=1,2,3   — batch stats for multiple repos
//	GET /api/v1/repos/{repoID}/sbom?format=cyclonedx|spdx — download SBOM
//	GET /api/v1/health                   — health check
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
)

// Server is the Aveloxis REST API server.
type Server struct {
	store    *db.PostgresStore
	logger   *slog.Logger
	mux      *http.ServeMux
	limiter  *rateLimiter   // v0.27.0: nil only when construction failed
	auth     *authenticator // v0.27.1: Bearer sessions + repo scope
	cmpCache *compareCache  // v0.27.2: 60s TTL for hot compare responses
}

// New creates an API server with default middleware options
// (rate limiting active with defaults, LAN exempt, no CORS origins).
// Kept for tests and backward compatibility; runAPI uses
// NewWithOptions with the aveloxis.json `api` block.
func New(store *db.PostgresStore, logger *slog.Logger) *Server {
	s, _ := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	return s
}

// NewWithOptions creates an API server whose Handler routes every
// request through the CORS + rate-limit middleware chain (v0.27.0 —
// plan: summary/api-analytics-plan-2026-07-10.md).
func NewWithOptions(store *db.PostgresStore, logger *slog.Logger, opts Options) (*Server, error) {
	s := &Server{store: store, logger: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/mailing-list/stats", s.handleMailingListStats)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/stats", s.handleRepoStats)
	s.mux.HandleFunc("GET /api/v1/repos/stats", s.handleRepoStatsBatch)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/sbom", s.handleSBOMDownload)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/timeseries", s.handleTimeSeries)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/licenses", s.handleLicenses)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/scancode-licenses", s.handleScancodeLicenses)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/scancode-files", s.handleScancodeFiles)
	// v0.23.10 — list-of-identities + affiliation-breakdown over an
	// operator-supplied time window. Nested under contributions/ so the
	// paths don't collide with the Augur-compatible
	// /api/v1/repos/{repoID}/contributors metric (which returns
	// aggregated counts per Augur's swagger spec). The two endpoints
	// share a single SQL CTE on the store side (contributorsInWindowCTE)
	// so they can never drift on the definition of "contribution."
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributions/identities", s.handleRepoContributors)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributions/affiliations", s.handleRepoAffiliations)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributions/coverage", s.handleRepoContributionsCoverage)
	s.mux.HandleFunc("GET /api/v1/repos/search", s.handleRepoSearch)
	// v0.27.2 — comparison analytics (plan §4): metric catalog
	// (docs-as-data), ≤7-entity temporal + snapshot comparison,
	// three-class entity picker search.
	s.mux.HandleFunc("GET /api/v1/metrics", s.handleMetricsCatalog)
	s.mux.HandleFunc("GET /api/v1/compare", s.handleCompare)
	s.mux.HandleFunc("GET /api/v1/compare/snapshot", s.handleCompareSnapshot)
	s.mux.HandleFunc("GET /api/v1/entities/search", s.handleEntitiesSearch)
	// v0.27.3 — portal + admin endpoints for the SPA pages. ALWAYS
	// require a Bearer identity (admin routes require admin), even
	// while api.require_auth is off for the read endpoints.
	s.mux.HandleFunc("GET /api/v1/me", s.handleMe)
	s.mux.HandleFunc("GET /api/v1/groups", s.handleGroupsList)
	s.mux.HandleFunc("POST /api/v1/groups", s.handleGroupCreate)
	s.mux.HandleFunc("GET /api/v1/groups/{groupID}/repos", s.handleGroupRepos)
	s.mux.HandleFunc("POST /api/v1/groups/{groupID}/repos", s.handleGroupAddRepo)
	s.mux.HandleFunc("GET /api/v1/admin/users", s.handleAdminUsers)
	s.mux.HandleFunc("POST /api/v1/admin/users/{userID}/admin", s.handleAdminSetUserAdmin)
	s.mux.HandleFunc("GET /api/v1/admin/groups/pending", s.handleAdminPendingGroups)
	s.mux.HandleFunc("POST /api/v1/admin/groups/{groupID}/{decision}", s.handleAdminGroupDecision)
	s.mux.HandleFunc("GET /api/v1/admin/monitor/stats", s.handleAdminMonitorStats)
	s.mux.HandleFunc("GET /api/v1/admin/monitor/queue", s.handleAdminMonitorQueue)
	s.registerMetricRoutes()
	rl, err := newRateLimiter(opts)
	if err != nil {
		return nil, err
	}
	s.limiter = rl
	s.auth = newAuthenticator(store, opts.RequireAuth)
	s.cmpCache = &compareCache{m: map[string]compareCacheEntry{}}
	return s, nil
}

// Handler returns the HTTP handler: CORS outermost (preflights are
// never rate-limited), then the per-IP limiter, then Bearer auth +
// scope, then the routes.
func (s *Server) Handler() http.Handler {
	return s.limiter.cors(s.limiter.middleware(s.auth.middleware(s.limiter, s.mux)))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "version": db.ToolVersion})
}

// handleMailingListStats (v0.25.7, #11) surfaces the mailing-list collection
// coverage rollup over the REST API.
func (s *Server) handleMailingListStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.MailingListStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

func (s *Server) handleRepoStats(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	stats, err := s.store.GetRepoStats(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleRepoStatsBatch(w http.ResponseWriter, r *http.Request) {
	idsParam := r.URL.Query().Get("ids")
	if idsParam == "" {
		http.Error(w, "ids parameter required (comma-separated repo IDs)", http.StatusBadRequest)
		return
	}
	var ids []int64
	for _, s := range strings.Split(idsParam, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		http.Error(w, "no valid repo IDs", http.StatusBadRequest)
		return
	}
	stats, err := s.store.GetRepoStatsBatch(r.Context(), ids)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleSBOMDownload generates and returns an SBOM for a repo.
// Query param: format=cyclonedx (default) or format=spdx
func (s *Server) handleSBOMDownload(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "cyclonedx"
	}

	var sbomFormat collector.SBOMFormat
	var filename string
	switch format {
	case "cyclonedx":
		sbomFormat = collector.FormatCycloneDX
		filename = fmt.Sprintf("sbom-repo-%d-cyclonedx.json", repoID)
	case "spdx":
		sbomFormat = collector.FormatSPDX
		filename = fmt.Sprintf("sbom-repo-%d-spdx.json", repoID)
	default:
		http.Error(w, "format must be 'cyclonedx' or 'spdx'", http.StatusBadRequest)
		return
	}

	data, err := collector.GenerateSBOM(r.Context(), s.store, repoID, sbomFormat)
	if err != nil {
		http.Error(w, "SBOM generation failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Write(data)
}

func (s *Server) handleTimeSeries(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	// Default window: last 2 years to now. Both endpoints overridable via
	// ?since=YYYY-MM-DD and ?until=YYYY-MM-DD. An invalid value falls back
	// to the default rather than erroring, so charts keep rendering.
	since := time.Now().AddDate(-2, 0, 0)
	if sinceParam := r.URL.Query().Get("since"); sinceParam != "" {
		if t, err := time.Parse("2006-01-02", sinceParam); err == nil {
			since = t
		}
	}
	var until time.Time
	if untilParam := r.URL.Query().Get("until"); untilParam != "" {
		if t, err := time.Parse("2006-01-02", untilParam); err == nil {
			// Treat the date as inclusive by advancing one day (store uses < upper).
			until = t.AddDate(0, 0, 1)
		}
	}
	if !until.IsZero() && !since.Before(until) {
		http.Error(w, "since must be before until", http.StatusBadRequest)
		return
	}
	ts, err := s.store.GetRepoTimeSeries(r.Context(), repoID, since, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin for the web GUI (different port).
	// Allow cross-origin only from localhost origins (web GUI on different port).
	// Wildcard "*" was removed because it exposes data to any website the operator visits.
	json.NewEncoder(w).Encode(ts)
}

func (s *Server) handleRepoSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "q parameter required", http.StatusBadRequest)
		return
	}
	repos, err := s.store.SearchRepos(r.Context(), q, 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin only from localhost origins (web GUI on different port).
	// Wildcard "*" was removed because it exposes data to any website the operator visits.
	json.NewEncoder(w).Encode(repos)
}

func (s *Server) handleLicenses(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	licenses, err := s.store.GetRepoLicenses(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin only from localhost origins (web GUI on different port).
	// Wildcard "*" was removed because it exposes data to any website the operator visits.
	json.NewEncoder(w).Encode(licenses)
}

// handleScancodeLicenses returns source code license detections from ScanCode.
// Response includes per-license file counts, copyrights, and scan metadata.
func (s *Server) handleScancodeLicenses(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}

	licenses, err := s.store.GetScancodeSourceLicenses(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode licenses", "repo_id", repoID, "error", err)
	}
	copyrights, err := s.store.GetScancodeCopyrights(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode copyrights", "repo_id", repoID, "error", err)
	}

	// v0.21.0 — Freshness fields surface the cadence/run state of
	// the decoupled ScancodeWorker. Web UI uses these to render
	// "Last run YYYY-MM-DD" above the per-file table. NULL
	// last_run on the repos row means scancode hasn't yet run for
	// this repo and the dashboard should say "Not yet run" instead
	// of "Loading...".
	lastRun, scancodeVer, err := s.store.ScancodeFreshness(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode freshness", "repo_id", repoID, "error", err)
	}
	var lastRunStr string
	if !lastRun.IsZero() {
		lastRunStr = lastRun.UTC().Format("2006-01-02")
	}

	resp := struct {
		Licenses        []db.ScancodeSourceLicense   `json:"licenses"`
		Copyrights      []db.ScancodeSourceCopyright `json:"copyrights"`
		LastRun         string                       `json:"last_run"`
		ScancodeVersion string                       `json:"scancode_version"`
	}{
		Licenses:        licenses,
		Copyrights:      copyrights,
		LastRun:         lastRunStr,
		ScancodeVersion: scancodeVer,
	}

	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin only from localhost origins (web GUI on different port).
	// Wildcard "*" was removed because it exposes data to any website the operator visits.
	json.NewEncoder(w).Encode(resp)
}

// handleScancodeFiles returns per-file scancode data for the sortable web GUI table.
// Each entry has: path, normalized SPDX license, truncated copyright holder.
func (s *Server) handleScancodeFiles(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	files, err := s.store.GetScancodeFileEntries(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode file entries", "repo_id", repoID, "error", err)
	}
	if files == nil {
		files = []db.ScancodeFileEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin only from localhost origins (web GUI on different port).
	// Wildcard "*" was removed because it exposes data to any website the operator visits.
	json.NewEncoder(w).Encode(files)
}
