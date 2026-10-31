// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package queueview is the one view model of a collection-queue page
// (stage 3d of summary/59, 0.29.109; WEB-10 and WEB-11 of the refactoring
// review): the page parameters with their effective echo, the loader that
// lists a page and enriches it with repository names and counts, the pager
// arithmetic, and the stats cache. serve's :5555 monitor, the web GUI's
// /monitor page and the API's admin queue route all read the queue through
// it, so they agree on sorting, pagination, the enrichment's failure mode
// and what a page of the queue is — they disagreed on each before.
package queueview

import (
	"context"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/model"
)

// Params are one page's parameters as EFFECTIVE values: the page and size
// after the defaults and the clamp, the search trimmed, and the sort as
// the store applies it — an unknown key means the store's default order,
// and then both Sort and Dir are "" (the envelope never claims a sort the
// store fell back from, nor a direction it never applied).
type Params struct {
	Page     int
	PageSize int
	Offset   int
	Search   string
	Sort     string
	Dir      string
	// SizeGiven is true when the query carried a usable page_size that
	// differs from the caller's default: the page's links then carry it,
	// so the pager and the rows agree (whole-branch review of 0.29.109:
	// the web monitor honoured the size and paged at its default).
	SizeGiven bool
	// SearchTooLong is true when q was longer than any repository name
	// can be (MaxSearchBytes): Search is then empty, Load answers an empty
	// page without a query, and the page says so (WarnSearchTooLong).
	SearchTooLong bool
}

// MaxSearchBytes bounds the search text: a repository is found by its
// owner/name, which its URL holds, and the longest URL a repository can
// have is db.MaxAddURLBytes (derived from the index row limit). Longer
// text matches nothing, so the trigram scan it would cost on the
// unauthenticated serve dashboard is never run (whole-branch review of
// 0.29.109).
const MaxSearchBytes = db.MaxAddURLBytes

// ParseParams reads page, page_size, q, sort and dir. A missing or
// non-positive page is 1; a missing or non-positive page_size is
// defaultSize, and one above maxSize is maxSize; a page so large that
// its offset would not fit an int is the last page an int can address
// (the store then answers an empty page, never a negative OFFSET —
// round-2 review of 0.29.109); dir is "desc" when it says so in any
// case, otherwise "asc".
func ParseParams(q url.Values, defaultSize, maxSize int) Params {
	// The sizes are the caller's constants; a non-positive one would
	// divide by zero in the clamp below (round-3 review of 0.29.109), so
	// the parser keeps its own precondition: at least one row a page, and a
	// maximum no smaller than the default.
	defaultSize = max(defaultSize, 1)
	maxSize = max(maxSize, defaultSize)
	p := Params{Page: 1, PageSize: defaultSize}
	if v, err := strconv.Atoi(q.Get("page")); err == nil && v > 0 {
		p.Page = v
	}
	if v, err := strconv.Atoi(q.Get("page_size")); err == nil && v > 0 {
		if v > maxSize {
			v = maxSize
		}
		p.PageSize = v
		p.SizeGiven = v != defaultSize
	}
	if search := strings.TrimSpace(q.Get("q")); len(search) > MaxSearchBytes {
		p.SearchTooLong = true
	} else {
		p.Search = search
	}
	if key := q.Get("sort"); db.QueueSortValid(key) {
		p.Sort = key
		p.Dir = "asc"
		if strings.EqualFold(q.Get("dir"), "desc") {
			p.Dir = "desc"
		}
	}
	if maxPage := db.MaxListingPage(p.PageSize); p.Page > maxPage {
		p.Page = maxPage
	}
	p.Offset = (p.Page - 1) * p.PageSize
	return p
}

// Row is one queue job as a page shows it: the job, the repository's
// names and platform (empty when the repository row is unreadable) and
// the gathered-versus-metadata counts (zero when unreadable), numbered
// from the page's first row.
type Row struct {
	db.QueueJob
	RowNum          int
	Owner           string
	Repo            string
	URL             string
	Plat            string
	GatheredIssues  int
	GatheredPRs     int
	GatheredCommits int
	MetaIssues      int
	MetaPRs         int
	MetaCommits     int
}

// Label is "owner/name", or the repository id when the row's names are
// unreadable (ids alone mean nothing to a person, but they are never
// hidden).
func (r Row) Label() string {
	if r.Owner == "" && r.Repo == "" {
		return strconv.FormatInt(r.RepoID, 10)
	}
	return r.Owner + "/" + r.Repo
}

// Store is what a page reads: the queue page and the two batched
// enrichment reads (never one query per row: the pre-v0.18.6 N+1 loop
// starved the collection workers of pool connections).
type Store interface {
	ListQueuePage(ctx context.Context, limit, offset int, search, sortKey, sortDir string) ([]db.QueueJob, int, error)
	GetReposBatch(ctx context.Context, repoIDs []int64) (map[int64]*model.Repo, error)
	GetRepoStatsBatch(ctx context.Context, repoIDs []int64) (map[int64]*db.RepoStats, error)
}

// Page is one loaded page: its rows, the total the listing counted, and
// the sentences a page shows when an enrichment read failed (empty when
// both succeeded) — the rows are then bare (ids, zero counts), and the
// page must say so rather than show zeros as the truth (round-1 review of
// 0.29.109, F1: two of the three pages showed a bare slash and silent
// zeros).
type Page struct {
	Rows     []Row
	Total    int
	Warnings []string
}

// The sentences every page shows for a failed enrichment read.
const (
	WarnNamesUnreadable  = "Repository names could not be read; the rows show repository ids."
	WarnCountsUnreadable = "Repository counts could not be read; the counts show 0."
	WarnSearchTooLong    = "The search text is longer than any repository name; nothing matches."
)

// Load lists one page and enriches it. A failed listing is the error; a
// failed enrichment read leaves the rows bare, adds the page's warning and
// is logged (D-17 and 58 §7 item 10: both errors were discarded once) —
// LogFailure demotes the line to DEBUG when the request has ended.
func Load(ctx context.Context, st Store, p Params, logger *slog.Logger) (Page, error) {
	if p.SearchTooLong {
		return Page{Warnings: []string{WarnSearchTooLong}}, nil
	}
	jobs, total, err := st.ListQueuePage(ctx, p.PageSize, p.Offset, p.Search, p.Sort, p.Dir)
	if err != nil {
		return Page{}, err
	}
	page := Page{Total: total}
	ids := make([]int64, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.RepoID)
	}
	repos, err := st.GetReposBatch(ctx, ids)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "queue page: repository names unreadable — ids shown bare", "rows", len(ids), "error", err)
		page.Warnings = append(page.Warnings, WarnNamesUnreadable)
	}
	stats, err := st.GetRepoStatsBatch(ctx, ids)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "queue page: repository stats unreadable — metadata counts shown as 0", "rows", len(ids), "error", err)
		page.Warnings = append(page.Warnings, WarnCountsUnreadable)
	}
	rows := make([]Row, 0, len(jobs))
	for i, j := range jobs {
		row := Row{QueueJob: j, RowNum: p.Offset + i + 1}
		if rp, ok := repos[j.RepoID]; ok && rp != nil {
			row.Owner, row.Repo, row.URL, row.Plat = rp.Owner, rp.Name, rp.GitURL, rp.Platform.String()
		}
		if st, ok := stats[j.RepoID]; ok && st != nil {
			row.GatheredIssues, row.GatheredPRs, row.GatheredCommits = st.GatheredIssues, st.GatheredPRs, st.GatheredCommits
			row.MetaIssues, row.MetaPRs, row.MetaCommits = st.MetadataIssues, st.MetadataPRs, st.MetadataCommits
		}
		rows = append(rows, row)
	}
	page.Rows = rows
	return page, nil
}

// TotalPages is how many pages total items span at pageSize: at least 1,
// so pagination renders for an empty list, and 1 for a zero size rather
// than a division by zero.
func TotalPages(total, pageSize int) int {
	if pageSize <= 0 || total <= 0 {
		return 1
	}
	n := total / pageSize
	if total%pageSize != 0 {
		n++
	}
	return n
}

// WindowSize is how many page numbers a pager shows around the current
// page: a person is never more than one click from any nearby page, and
// First/Last reach the ends.
const WindowSize = 5

// Window is the sliding window of up to WindowSize page numbers centred
// on page, clamped to 1..totalPages (the group page and the web monitor
// each had their own copy of this arithmetic).
func Window(page, totalPages int) []int {
	totalPages = max(totalPages, 1)
	start := max(page-WindowSize/2, 1)
	end := start + WindowSize - 1
	if end > totalPages {
		end = totalPages
		start = max(end-WindowSize+1, 1)
	}
	window := make([]int, 0, end-start+1)
	for i := start; i <= end; i++ {
		window = append(window, i)
	}
	return window
}

// SortHrefs builds, for each allowlisted sort key, the link that sorts
// by it: ascending, or descending when the page is already sorted by it
// ascending. The search term rides along.
func SortHrefs(base string, p Params, keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		dir := "asc"
		if p.Sort == key && p.Dir == "asc" {
			dir = "desc"
		}
		v := url.Values{"sort": {key}, "dir": {dir}}
		if p.Search != "" {
			v.Set("q", p.Search)
		}
		if p.SizeGiven {
			v.Set("page_size", strconv.Itoa(p.PageSize))
		}
		out[key] = base + "?" + v.Encode()
	}
	return out
}
