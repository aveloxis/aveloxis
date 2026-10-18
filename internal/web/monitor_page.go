// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

const monitorPageSize = 200

func (s *Server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	// Parse pagination and search.
	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	offset := (page - 1) * monitorPageSize

	stats, err := s.store.QueueStats(r.Context())
	if err != nil {
		s.serverError(w, r, "handleMonitor", err)
		return
	}
	jobs, total, err := s.store.ListQueuePage(r.Context(), monitorPageSize, offset, query, "", "")
	if err != nil {
		s.serverError(w, r, "handleMonitor", err)
		return
	}

	totalPages := (total + monitorPageSize - 1) / monitorPageSize
	if totalPages < 1 {
		totalPages = 1
	}

	// Sliding window of up to 5 page numbers centered on the current
	// page, same shape as handleGroup. The shared paginationNav block
	// renders clickable links for each; combined with First/Prev/Next/
	// Last at the edges, a user is never more than one click from any
	// nearby page.
	const windowSize = 5
	winStart := page - windowSize/2
	if winStart < 1 {
		winStart = 1
	}
	winEnd := winStart + windowSize - 1
	if winEnd > totalPages {
		winEnd = totalPages
		winStart = winEnd - windowSize + 1
		if winStart < 1 {
			winStart = 1
		}
	}
	var pageWindow []int
	for i := winStart; i <= winEnd; i++ {
		pageWindow = append(pageWindow, i)
	}

	// Enrich jobs with repo details and gathered vs metadata counts.
	// Both lookups are batched (single SQL round-trip each). The prior
	// version called GetRepoByID inside the per-row loop — 200 serial
	// SELECTs per page render that made Prev/Next click navigation race
	// against the 10s auto-refresh and get cancelled.
	repoIDs := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		repoIDs = append(repoIDs, j.RepoID)
	}
	// Enrichment of the rows: the page renders without it, so a failure is
	// logged, not a 500.
	repos, err := s.store.GetReposBatch(r.Context(), repoIDs)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("monitor page: repository details unavailable", "rows", len(repoIDs), "error", err)
	}
	repoStats, err := s.store.GetRepoStatsBatch(r.Context(), repoIDs)
	if err != nil && !httpserver.RequestEnded(r.Context(), err) {
		s.logger.Warn("monitor page: repository stats unavailable", "rows", len(repoIDs), "error", err)
	}

	type monitorRow struct {
		RowNum          int
		RepoID          int64
		Owner           string
		Repo            string
		Plat            string
		Status          string
		Priority        int
		Due             string
		LastRun         string
		Worker          string
		ErrInfo         string
		GatheredIssues  int
		MetaIssues      int
		GatheredPRs     int
		MetaPRs         int
		GatheredCommits int
		MetaCommits     int
	}

	rows := make([]monitorRow, 0, len(jobs))
	for i, j := range jobs {
		row := monitorRow{
			RowNum:   offset + i + 1,
			RepoID:   j.RepoID,
			Status:   j.Status,
			Priority: j.Priority,
		}

		if repo, ok := repos[j.RepoID]; ok {
			row.Owner = repo.Owner
			row.Repo = repo.Name
			row.Plat = repo.Platform.String()
		}
		if st, ok := repoStats[j.RepoID]; ok {
			row.GatheredIssues = st.GatheredIssues
			row.GatheredPRs = st.GatheredPRs
			row.GatheredCommits = st.GatheredCommits
			row.MetaIssues = st.MetadataIssues
			row.MetaPRs = st.MetadataPRs
			row.MetaCommits = st.MetadataCommits
		}

		row.Due = j.DueAt.Format("Jan 2 15:04")
		if j.DueAt.Before(time.Now()) && j.Status == "queued" {
			row.Due = "now"
		}
		row.LastRun = "-"
		if j.LastCollected != nil {
			row.LastRun = j.LastCollected.Format("Jan 2 15:04")
			if j.LastDurationMs > 0 {
				row.LastRun += fmt.Sprintf(" (%ds)", j.LastDurationMs/1000)
			}
		}
		if j.LockedBy != nil {
			row.Worker = *j.LockedBy
		}
		if j.LastError != nil && *j.LastError != "" {
			row.ErrInfo = *j.LastError
		}

		rows = append(rows, row)
	}

	s.render(w, "monitor", map[string]any{
		"Session":    sess,
		"Stats":      stats,
		"Jobs":       rows,
		"Page":       page,
		"TotalPages": totalPages,
		"Total":      total,
		"Query":      query,
		"PageWindow": pageWindow,
	})
}

func (s *Server) handleMonitorPrioritize(w http.ResponseWriter, r *http.Request) {
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
		s.serverError(w, r, "handleMonitorPrioritize", err)
		return
	}
	// Redirect back to the monitor page.
	http.Redirect(w, r, "/monitor", http.StatusFound)
}
