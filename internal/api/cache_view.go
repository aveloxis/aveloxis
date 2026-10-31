// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// The administrator's view of the api's caches (D4, 0.29.123, summary/63
// §9): what they hold, what they did since this process started, and, per
// repository, whether each held answer is current. Read-only (SR-7). The
// caches live in this process's memory, so the view is this process's.

import (
	"net/http"
	"sort"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// cacheRepoPageSize is how many repositories the fleet listing answers: the
// queue page's size (handleAdminMonitorQueue), the console's other
// per-repository table.
const cacheRepoPageSize = 100

// cacheRepoSorts is the fleet listing's sort allowlist (the listing-table
// grammar: an unknown key falls back, and the answer echoes the effective
// one); cacheRepoSortDefault is the fallback.
var cacheRepoSorts = map[string]func(a, b cacheRepoSummary) bool{
	"bytes":   func(a, b cacheRepoSummary) bool { return a.Bytes > b.Bytes },
	"entries": func(a, b cacheRepoSummary) bool { return a.Entries > b.Entries },
	"hits":    func(a, b cacheRepoSummary) bool { return a.Hits > b.Hits },
}

const cacheRepoSortDefault = "bytes"

// cacheRepoSummary is one repository's held answers, as the cache holds them.
type cacheRepoSummary struct {
	RepoID  int64 `json:"repo_id"`
	Entries int   `json:"entries"`
	Bytes   int64 `json:"bytes"`
	Hits    int   `json:"hits"`
	Saved   int   `json:"saved_requests"` // seeds loaded at start, not yet replayed
}

// cacheAnswerView is one held answer.
type cacheAnswerView struct {
	URI        string    `json:"uri"`
	ComputedAt time.Time `json:"computed_at"`
	CostMS     int64     `json:"cost_ms"`
	Hits       int       `json:"hits"`
	Bytes      int64     `json:"bytes"`
	Status     string    `json:"status"`
	fp, window string
}

// repoSummaries is one summary per repository with held answers or seeds.
// It walks every entry under the cache lock, as replaySet does once per
// re-warm pass, so requests wait on the lock for one walk — linear in the
// entries the budget holds — each time an administrator opens the view
// (whole-branch review of 0.29.119–0.29.123, A12).
func (c *repoPageCache) repoSummaries() []cacheRepoSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	by := map[int64]*cacheRepoSummary{}
	get := func(id int64) *cacheRepoSummary {
		if by[id] == nil {
			by[id] = &cacheRepoSummary{RepoID: id}
		}
		return by[id]
	}
	for id, set := range c.byRepo {
		r := get(id)
		for el := range set {
			e := el.Value.(*pageEntry)
			r.Entries++
			r.Bytes += e.size()
			r.Hits += e.hits
		}
	}
	for id, set := range c.seeds {
		get(id).Saved += len(set)
	}
	out := make([]cacheRepoSummary, 0, len(by))
	for _, r := range by {
		out = append(out, *r)
	}
	return out
}

// repoAnswers is a copy of repoID's held answers and its seeds' URIs.
func (c *repoPageCache) repoAnswers(repoID int64) (answers []cacheAnswerView, seeds []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := range c.byRepo[repoID] {
		e := el.Value.(*pageEntry)
		answers = append(answers, cacheAnswerView{URI: e.uri, ComputedAt: e.computed, CostMS: e.cost.Milliseconds(),
			Hits: e.hits, Bytes: e.size(), fp: e.fingerprint, window: e.window})
	}
	for uri := range c.seeds[repoID] {
		seeds = append(seeds, uri)
	}
	sort.Slice(answers, func(i, j int) bool { return answers[i].URI < answers[j].URI })
	sort.Strings(seeds)
	return answers, seeds
}

// GET /api/v1/admin/cache — the caches' totals and the re-warm's last pass.
func (s *Server) handleAdminCache(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	out := map[string]any{}
	if c := s.pageCache; c != nil {
		c.mu.Lock()
		st, entries, bytes := c.stats, c.ll.Len(), c.bytes
		repos := len(c.byRepo)
		seeds := 0
		for _, set := range c.seeds {
			seeds += len(set)
		}
		c.mu.Unlock()
		out["page_cache"] = map[string]any{
			"max_bytes": c.maxBytes, "bytes": bytes, "entries": entries, "repositories": repos, "saved_requests": seeds,
			"hits": st.Hits, "revalidated": st.Revalidated, "misses": st.Misses, "stores": st.Stores, "evictions": st.Evictions,
		}
	}
	s.rewarmMu.Lock()
	last := s.rewarmLast
	s.rewarmMu.Unlock()
	s.rewarmMu.Lock()
	saved := s.savedReplayCount
	s.rewarmMu.Unlock()
	rewarm := map[string]any{"interval_seconds": int64(s.rewarmInterval / time.Second), "saved_requests": saved}
	if !last.At.IsZero() {
		rewarm["last_pass_at"] = last.At.UTC()
		rewarm["last_pass_repositories"] = last.Repositories
		rewarm["last_pass_requests"] = last.Requests
		rewarm["last_pass_duration_ms"] = last.Duration.Milliseconds()
	}
	out["rewarm"] = rewarm
	if c := s.seriesCache; c != nil {
		c.mu.Lock()
		out["series_cache"] = map[string]any{"entries": len(c.m), "max_entries": collectionCacheMaxEntries,
			"hits": c.hits, "misses": c.misses, "resets": c.resets}
		c.mu.Unlock()
	}
	if c := s.cmpCache; c != nil {
		c.mu.Lock()
		out["compare_body_cache"] = map[string]any{"entries": len(c.m)}
		c.mu.Unlock()
	}
	s.writeJSON(w, r, out)
}

// GET /api/v1/admin/cache/repos?sort=bytes|entries|hits — the repositories
// with held answers, largest first, at most cacheRepoPageSize.
func (s *Server) handleAdminCacheRepos(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	sortKey := r.URL.Query().Get("sort")
	less, known := cacheRepoSorts[sortKey]
	if !known {
		sortKey, less = cacheRepoSortDefault, cacheRepoSorts[cacheRepoSortDefault]
	}
	var rows []cacheRepoSummary
	if s.pageCache != nil {
		rows = s.pageCache.repoSummaries()
	}
	sort.Slice(rows, func(i, j int) bool {
		if less(rows[i], rows[j]) != less(rows[j], rows[i]) {
			return less(rows[i], rows[j])
		}
		return rows[i].RepoID < rows[j].RepoID
	})
	total := len(rows)
	if len(rows) > cacheRepoPageSize {
		rows = rows[:cacheRepoPageSize]
	}
	if rows == nil {
		rows = []cacheRepoSummary{}
	}
	s.writeJSON(w, r, map[string]any{"sort": sortKey, "total": total, "repositories": rows})
}

// GET /api/v1/admin/cache/repos/{repoID} — one repository's held answers,
// each current or why not, and its saved requests not yet replayed.
func (s *Server) handleAdminCacheRepo(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	// The repository routes' prelude: 400 for an id that is not one, 404 for
	// one no repository has (a vanished repository's answers leave the cache
	// at the re-warm's next pass anyway).
	repoID, ok := s.repoFromPath(w, r)
	if !ok {
		return
	}
	var answers []cacheAnswerView
	var seeds []string
	if s.pageCache != nil {
		answers, seeds = s.pageCache.repoAnswers(repoID)
	}
	out := map[string]any{"repo_id": repoID}
	states, err := s.repoStates(r.Context(), []int64{repoID})
	if err != nil {
		s.serverError(w, r, "handleAdminCacheRepo", err)
		return
	}
	st := states[repoID]
	out["collecting"] = st.Collecting
	for i := range answers {
		answers[i].Status = s.answerStatus(answers[i], st)
	}
	if answers == nil {
		answers = []cacheAnswerView{}
	}
	if seeds == nil {
		seeds = []string{}
	}
	out["answers"], out["saved_requests"] = answers, seeds
	s.writeJSON(w, r, out)
}

// answerStatus is whether a held answer is what its request computes now —
// the re-warm's rule (entryState) — or why not.
func (s *Server) answerStatus(a cacheAnswerView, st db.RepoCacheState) string {
	window, ok := s.windowOfURI(a.URI)
	switch {
	case !ok:
		return "outdated: no longer a cached request"
	case a.fp != st.Fingerprint():
		return "outdated: the repository changed"
	case a.window != window:
		return "outdated: its window moved"
	}
	return "current"
}
