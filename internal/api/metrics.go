// Package api — metrics.go implements Augur-compatible REST API endpoints
// for CHAOSS metrics. These endpoints follow the swagger.json specification
// ported from Augur's Python API.
//
// Endpoint patterns:
//   /api/v1/repos/{repoID}/{metric}           — per-repo metric
//   /api/v1/repo-groups/{groupID}/{metric}     — per-group metric (aggregated)
//
// Common query parameters:
//   begin_date (YYYY-MM-DD) — start of date range, default 1 year ago
//   end_date   (YYYY-MM-DD) — end of date range, default today
//   period     (day|week|month|year) — aggregation period, default month
//
// Excluded: Login endpoints, DEI Badging endpoints (per task spec).
package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// registerMetricRoutes registers all Augur-compatible metric endpoints.
func (s *Server) registerMetricRoutes() {
	// === Utility / Lookup ===
	s.mux.HandleFunc("GET /api/v1/repo-groups", s.handleRepoGroups)
	s.mux.HandleFunc("GET /api/v1/repos", s.handleAllRepos)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}", s.handleRepoByID)
	s.mux.HandleFunc("GET /api/v1/repo-groups/{groupID}/repos", s.handleReposByGroup)
	s.mux.HandleFunc("GET /api/v1/owner/{owner}/repo/{repo}", s.handleRepoByOwnerName)
	s.mux.HandleFunc("GET /api/v1/rg-name/{rgName}", s.handleRepoGroupByName)
	s.mux.HandleFunc("GET /api/v1/rg-name/{rgName}/repo-name/{repoName}", s.handleRepoByGroupAndName)

	// === Issue Metrics ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issues-new", s.handleIssuesNew)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issues-closed", s.handleIssuesClosed)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issues-active", s.handleIssuesActive)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issue-backlog", s.handleIssueBacklog)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issue-throughput", s.handleIssueThroughput)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/issue-duration", s.handleIssueDuration)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/average-issue-resolution-time", s.handleAvgIssueResolution)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/abandoned-issues", s.handleAbandonedIssues)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/open-issues-count", s.handleOpenIssuesCount)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/closed-issues-count", s.handleClosedIssuesCount)

	// === Pull Request Metrics ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/pull-requests-new", s.handlePRsNew)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/reviews", s.handleReviews)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/reviews-accepted", s.handleReviewsAccepted)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/reviews-declined", s.handleReviewsDeclined)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/review-duration", s.handleReviewDuration)

	// === Commit Metrics ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/committers", s.handleCommitters)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/code-changes", s.handleCodeChanges)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/code-changes-lines", s.handleCodeChangesLines)

	// === Contributor Metrics ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributors", s.handleContributors)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributors-new", s.handleContributorsNew)

	// === Repo Metadata ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/stars", s.handleStars)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/stars-count", s.handleStarsCount)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/forks", s.handleForks)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/fork-count", s.handleForkCount)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/watchers", s.handleWatchers)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/watchers-count", s.handleWatchersCount)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/languages", s.handleLanguages)

	// === Dependencies ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/deps", s.handleDeps)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/libyear", s.handleDeps) // Same as deps — libyear data is in the same table.

	// === Messages ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/repo-messages", s.handleRepoMessages)

	// === Releases ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/releases", s.handleReleases)

	// === Complexity (SCC) ===
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/project-languages", s.handleProjectLanguages)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/project-files", s.handleProjectLanguages)
	s.mux.HandleFunc("GET /api/v1/repos/{repoID}/project-lines", s.handleProjectLanguages)
}

// ============================================================
// Common parameter parsing
// ============================================================

func parseRepoID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("repoID"), 10, 64)
}

func parseGroupID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("groupID"), 10, 64)
}

// parseDateRange extracts begin_date and end_date from query params.
// Defaults: begin = 1 year ago, end = today.
func parseDateRange(r *http.Request) (begin, end time.Time) {
	begin = time.Now().AddDate(-1, 0, 0)
	end = time.Now()
	if v := r.URL.Query().Get("begin_date"); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			begin = t
		}
	}
	if v := r.URL.Query().Get("end_date"); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			end = t
		}
	}
	return
}

// parsePeriod extracts the aggregation period from query params.
// Valid: day, week, month, year. Default: month.
func parsePeriod(r *http.Request) string {
	p := r.URL.Query().Get("period")
	switch p {
	case "day", "week", "month", "year":
		return p
	default:
		return "month"
	}
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(data)
}

// ============================================================
// Utility / Lookup handlers
// ============================================================

func (s *Server) handleRepoGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.store.GetAllRepoGroups(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, groups)
}

func (s *Server) handleAllRepos(w http.ResponseWriter, r *http.Request) {
	repos, err := s.store.GetAllRepos(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, repos)
}

func (s *Server) handleRepoByID(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	repo, err := s.store.GetRepoByID(r.Context(), repoID)
	if err != nil {
		http.Error(w, "repo not found", http.StatusNotFound)
		return
	}
	jsonResponse(w, repo)
}

func (s *Server) handleReposByGroup(w http.ResponseWriter, r *http.Request) {
	groupID, err := parseGroupID(r)
	if err != nil {
		http.Error(w, "invalid repo_group_id", http.StatusBadRequest)
		return
	}
	repos, err := s.store.GetReposByGroup(r.Context(), groupID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, repos)
}

func (s *Server) handleRepoByOwnerName(w http.ResponseWriter, r *http.Request) {
	owner := r.PathValue("owner")
	name := r.PathValue("repo")
	repo, err := s.store.GetRepoByOwnerName(r.Context(), owner, name)
	if err != nil {
		http.Error(w, "repo not found", http.StatusNotFound)
		return
	}
	jsonResponse(w, repo)
}

func (s *Server) handleRepoGroupByName(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rgName")
	group, err := s.store.GetRepoGroupByName(r.Context(), name)
	if err != nil {
		http.Error(w, "repo group not found", http.StatusNotFound)
		return
	}
	jsonResponse(w, group)
}

func (s *Server) handleRepoByGroupAndName(w http.ResponseWriter, r *http.Request) {
	// Look up group first, then find repo with matching name in that group.
	rgName := r.PathValue("rgName")
	repoName := r.PathValue("repoName")
	group, err := s.store.GetRepoGroupByName(r.Context(), rgName)
	if err != nil {
		http.Error(w, "repo group not found", http.StatusNotFound)
		return
	}
	repos, err := s.store.GetReposByGroup(r.Context(), group.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, repo := range repos {
		if repo.Name == repoName {
			jsonResponse(w, repo)
			return
		}
	}
	http.Error(w, "repo not found in group", http.StatusNotFound)
}

// ============================================================
// Issue metric handlers
// ============================================================

func (s *Server) handleIssuesNew(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.IssuesNew(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleIssuesClosed(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.IssuesClosed(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleIssuesActive(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.IssuesActive(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleIssueBacklog(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	count, err := s.store.IssueBacklog(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]int{"issue_backlog": count})
}

func (s *Server) handleIssueThroughput(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	throughput, err := s.store.IssueThroughput(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]float64{"throughput": throughput})
}

func (s *Server) handleIssueDuration(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	data, err := s.store.IssueDuration(r.Context(), repoID, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleAvgIssueResolution(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	avg, err := s.store.AverageIssueResolutionTime(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]float64{"avg_issue_resolution_days": avg})
}

func (s *Server) handleAbandonedIssues(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.AbandonedIssues(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleOpenIssuesCount(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	count, err := s.store.IssueBacklog(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]int{"open_count": count})
}

func (s *Server) handleClosedIssuesCount(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	var count int
	s.store.Pool().QueryRow(r.Context(), `
		SELECT COUNT(issue_id)
		FROM aveloxis_data.issues
		WHERE repo_id = $1 AND issue_state = 'closed' AND pull_request IS NULL`, repoID).Scan(&count)
	jsonResponse(w, map[string]int{"closed_count": count})
}

// ============================================================
// PR metric handlers
// ============================================================

func (s *Server) handlePRsNew(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.PRsNew(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleReviews(w http.ResponseWriter, r *http.Request) {
	// "reviews" in Augur is equivalent to PRs created.
	s.handlePRsNew(w, r)
}

func (s *Server) handleReviewsAccepted(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.ReviewsAccepted(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleReviewsDeclined(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.ReviewsDeclined(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleReviewDuration(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	data, err := s.store.ReviewDuration(r.Context(), repoID, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Commit metric handlers
// ============================================================

func (s *Server) handleCommitters(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.Committers(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleCodeChanges(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.CodeChanges(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleCodeChangesLines(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.CodeChangesLines(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Contributor metric handlers
// ============================================================

func (s *Server) handleContributors(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	data, err := s.store.Contributors(r.Context(), repoID, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleContributorsNew(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.ContributorsNew(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Repo metadata handlers
// ============================================================

func (s *Server) handleStars(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.StarsTimeSeries(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleStarsCount(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	count, name, err := s.store.LatestCount(r.Context(), repoID, "stars_count")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]interface{}{"repo_name": name, "stars": count})
}

func (s *Server) handleForks(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.ForksTimeSeries(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

func (s *Server) handleForkCount(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	count, name, err := s.store.LatestCount(r.Context(), repoID, "fork_count")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]interface{}{"repo_name": name, "forks": count})
}

func (s *Server) handleWatchers(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	// Watchers time series — same pattern as stars/forks but with watchers_count.
	rows, err := s.store.Pool().Query(r.Context(), `
		SELECT data_collection_date AS date, watchers_count AS value, r.repo_name
		FROM aveloxis_data.repo_info ri
		JOIN aveloxis_data.repos r ON ri.repo_id = r.repo_id
		WHERE ri.repo_id = $1
		ORDER BY data_collection_date`, repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	var result []map[string]interface{}
	for rows.Next() {
		var date time.Time
		var value int
		var name string
		if err := rows.Scan(&date, &value, &name); err == nil {
			result = append(result, map[string]interface{}{"date": date, "watchers": value, "repo_name": name})
		}
	}
	jsonResponse(w, result)
}

func (s *Server) handleWatchersCount(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	count, name, err := s.store.LatestCount(r.Context(), repoID, "watchers_count")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]interface{}{"repo_name": name, "watchers": count})
}

func (s *Server) handleLanguages(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	lang, err := s.store.Languages(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]interface{}{"repo_id": repoID, "primary_language": lang})
}

// ============================================================
// Dependency handlers
// ============================================================

func (s *Server) handleDeps(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.Deps(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Message handlers
// ============================================================

func (s *Server) handleRepoMessages(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	begin, end := parseDateRange(r)
	period := parsePeriod(r)
	data, err := s.store.RepoMessages(r.Context(), repoID, period, begin, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Release handlers
// ============================================================

func (s *Server) handleReleases(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.Releases(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}

// ============================================================
// Complexity handlers
// ============================================================

func (s *Server) handleProjectLanguages(w http.ResponseWriter, r *http.Request) {
	repoID, err := parseRepoID(r)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	data, err := s.store.ProjectLanguages(r.Context(), repoID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, data)
}
