// Package api provides a REST API for Aveloxis data access.
// Started via `aveloxis api` as a separate process from the collection scheduler.
// The web GUI and monitoring dashboard call this API for repo statistics.
//
// Endpoints:
//   GET /api/v1/repos/{repoID}/stats    — gathered vs metadata counts for one repo
//   GET /api/v1/repos/stats?ids=1,2,3   — batch stats for multiple repos
//   GET /api/v1/repos/{repoID}/sbom?format=cyclonedx|spdx — download SBOM
//   GET /api/v1/health                   — health check
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/collector"
	"github.com/augurlabs/aveloxis/internal/db"
)

// Server is the Aveloxis REST API server.
type Server struct {
	store  *db.PostgresStore
	logger *slog.Logger
	mux    *http.ServeMux
}

// New creates an API server.
func New(store *db.PostgresStore, logger *slog.Logger) *Server {
	s := &Server{store: store, logger: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/stats", s.handleRepoStats)
	s.mux.HandleFunc("GET /api/v1/repos/stats", s.handleRepoStatsBatch)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/sbom", s.handleSBOMDownload)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/timeseries", s.handleTimeSeries)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/licenses", s.handleLicenses)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/scancode-licenses", s.handleScancodeLicenses)
	s.mux.HandleFunc("GET /api/v1/repos/search", s.handleRepoSearch)
	s.registerMetricRoutes()
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "version": db.ToolVersion})
}

func (s *Server) handleRepoStats(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
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
	// Default to last 2 years of data.
	since := time.Now().AddDate(-2, 0, 0)
	if sinceParam := r.URL.Query().Get("since"); sinceParam != "" {
		if t, err := time.Parse("2006-01-02", sinceParam); err == nil {
			since = t
		}
	}
	ts, err := s.store.GetRepoTimeSeries(r.Context(), repoID, since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Allow cross-origin for the web GUI (different port).
	w.Header().Set("Access-Control-Allow-Origin", "*")
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
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(repos)
}

func (s *Server) handleLicenses(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	licenses, err := s.store.GetRepoLicenses(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
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

	licenses, err := s.store.GetScancodeSourceLicenses(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode licenses", "repo_id", repoID, "error", err)
	}
	copyrights, err := s.store.GetScancodeCopyrights(r.Context(), repoID)
	if err != nil {
		s.logger.Warn("failed to get scancode copyrights", "repo_id", repoID, "error", err)
	}

	resp := struct {
		Licenses   []db.ScancodeSourceLicense   `json:"licenses"`
		Copyrights []db.ScancodeSourceCopyright  `json:"copyrights"`
	}{
		Licenses:   licenses,
		Copyrights: copyrights,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(resp)
}
