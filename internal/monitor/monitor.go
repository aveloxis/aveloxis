// Package monitor provides an HTTP dashboard for observing Aveloxis collection
// progress. Serves the same purpose as Flower does for Celery, but backed by
// Postgres queue state — no additional infrastructure required.
package monitor

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/static"
)

// Server is the monitoring HTTP server.
type Server struct {
	store  *db.PostgresStore
	logger *slog.Logger
	mux    *http.ServeMux
}

// New creates a monitor server.
func New(store *db.PostgresStore, logger *slog.Logger) *Server {
	s := &Server{store: store, logger: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /", s.handleDashboard)
	s.mux.HandleFunc("GET /api/queue", s.handleQueue)
	s.mux.HandleFunc("GET /api/stats", s.handleStats)
	s.mux.HandleFunc("POST /api/prioritize/{repoID}", s.handlePrioritize)
	s.mux.HandleFunc("POST /api/repos", s.handleAddRepo)
	s.mux.HandleFunc("GET /icon.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(static.IconPNG)
	})
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.QueueStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListQueue(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type jobView struct {
		RepoID       int64   `json:"repo_id"`
		Priority     int     `json:"priority"`
		Status       string  `json:"status"`
		DueAt        string  `json:"due_at"`
		LockedBy     *string `json:"locked_by,omitempty"`
		LastCollected *string `json:"last_collected,omitempty"`
		LastError    *string `json:"last_error,omitempty"`
		Issues       int     `json:"issues"`
		PRs          int     `json:"pull_requests"`
		Messages     int     `json:"messages"`
		Events       int     `json:"events"`
		Releases     int     `json:"releases"`
		Contributors int     `json:"contributors"`
		Commits      int     `json:"commits"`
		DurationMs   int64   `json:"duration_ms"`
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
	json.NewEncoder(w).Encode(views)
}

func (s *Server) handlePrioritize(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if err := s.store.PrioritizeRepo(r.Context(), repoID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": fmt.Sprintf("repo %d pushed to top of queue", repoID),
	})
}

func (s *Server) handleAddRepo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL      string `json:"url"`
		Priority int    `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "info",
		"message": "Use 'aveloxis add-repo " + req.URL + "' CLI command to add repos",
	})
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	stats, _ := s.store.QueueStats(r.Context())
	jobs, _ := s.store.ListQueue(r.Context())

	// Look up repo details and stats for display.
	type enrichedJob struct {
		db.QueueJob
		Owner          string
		Repo           string
		URL            string
		Plat           string
		GatheredPRs    int
		GatheredIssues int
		GatheredCommits int
		MetaPRs        int
		MetaIssues     int
		MetaCommits    int
	}

	// Collect repo IDs for batch stats lookup.
	repoIDs := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		repoIDs = append(repoIDs, j.RepoID)
	}
	repoStats, _ := s.store.GetRepoStatsBatch(r.Context(), repoIDs)

	enriched := make([]enrichedJob, 0, len(jobs))
	for _, j := range jobs {
		ej := enrichedJob{QueueJob: j}
		if repo, err := s.store.GetRepoByID(r.Context(), j.RepoID); err == nil {
			ej.Owner = repo.Owner
			ej.Repo = repo.Name
			ej.URL = repo.GitURL
			ej.Plat = repo.Platform.String()
		}
		if st, ok := repoStats[j.RepoID]; ok {
			ej.GatheredPRs = st.GatheredPRs
			ej.GatheredIssues = st.GatheredIssues
			ej.GatheredCommits = st.GatheredCommits
			ej.MetaPRs = st.MetadataPRs
			ej.MetaIssues = st.MetadataIssues
			ej.MetaCommits = st.MetadataCommits
		}
		enriched = append(enriched, ej)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html>
<html><head><title>Aveloxis Monitor</title>
<meta http-equiv="refresh" content="10">
<style>
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
</head><body>
<div style="display:flex;align-items:center;justify-content:space-between"><h1>Aveloxis Monitor</h1><img src="/icon.png" alt="Aveloxis" style="height:48px;border-radius:8px"></div>
<div class="sub">Auto-refreshes every 10s. API: <code>aveloxis api --addr :8383</code></div>
<div class="stats">`)

	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Total</div></div>`, stats["total"])
	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Queued</div></div>`, stats["queued"])
	fmt.Fprintf(w, `<div class="stat"><div class="value">%d</div><div class="label">Collecting</div></div>`, stats["collecting"])
	fmt.Fprint(w, `</div>
<table>
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

		due := j.DueAt.Format("15:04:05")
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
		escOwner := template.HTMLEscapeString(j.Owner)
		escRepo := template.HTMLEscapeString(j.Repo)

		fmt.Fprintf(w, `<tr%s><td>%d</td><td>%s/%s</td><td>%s</td><td class="%s">%s%s%s</td><td>%d</td><td>%s</td><td>%s</td>`,
			rowClass, i+1, escOwner, escRepo, j.Plat,
			statusClass, j.Status, worker, errInfo,
			j.Priority, due, lastRun)

		// Gathered vs Metadata columns.
		fmt.Fprintf(w, `<td class="count-cell"><span class="gathered">%d</span></td><td class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredIssues, j.MetaIssues)
		fmt.Fprintf(w, `<td class="count-cell"><span class="gathered">%d</span></td><td class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredPRs, j.MetaPRs)
		fmt.Fprintf(w, `<td class="count-cell"><span class="gathered">%d</span></td><td class="count-cell"><span class="meta">%d</span></td>`,
			j.GatheredCommits, j.MetaCommits)

		fmt.Fprint(w, `<td>`)
		if j.Status == "queued" {
			fmt.Fprintf(w, `<form method="POST" action="/api/prioritize/%d" style="display:inline"><button class="btn" type="submit">Boost</button></form>`, j.RepoID)
		}
		fmt.Fprint(w, `</td></tr>`)
	}

	fmt.Fprint(w, `</table>
<p style="color:#999;margin-top:1rem;font-size:0.8rem">
API: GET /api/queue | GET /api/stats | POST /api/prioritize/{repoID} | REST API: aveloxis api --addr :8383
</p></body></html>`)
}
