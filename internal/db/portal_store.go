// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.3 — small store helpers for the portal JSON endpoints.
// v0.27.14 — the group listing paginates (page/page_size + total) so
// fleet-scale groups don't ship thousands of rows per render.
// v0.27.75 — the group listing adopts the COLLECTIONS table grammar
// (operator request 2026-08-02, "same columns as the Collections
// listing"): per-row cached issue/PR/commit counts from the queue's
// cumulative totals (v0.19.11/v0.21.2 — never per-request
// aggregation, the v0.27.4 nginx-timeout class), last_collected, the
// forge-reported last_activity via the repo_info LATERAL, and the
// caller's starred flag, with server-side sort through the SAME
// collectionRepoSorts allowlist GetCollectionRepos uses — the two
// tables can never disagree on what a sort key means.

import (
	"context"
	"math"
	"strings"
	"time"
)

// PortalGroupRepo is one repo inside a user group, for the group page.
// Same row shape as CollectionRepo (v0.27.74) so the two GUI tables
// render identically.
type PortalGroupRepo struct {
	RepoID int64  `json:"repo_id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	GitURL string `json:"git_url"`
	// Cached cumulative gathered counts from collection_queue — the
	// values the monitor renders, refreshed by CompleteJob.
	Issues        int64      `json:"issues"`
	PRs           int64      `json:"prs"`
	Commits       int64      `json:"commits"`
	LastCollected *time.Time `json:"last_collected,omitempty"`
	LastActivity  *time.Time `json:"last_activity,omitempty"`
	Starred       bool       `json:"starred"`
}

// ListingPage is the EFFECTIVE paging and sort of a repository listing:
// what the store applied and what the envelope echoes (58 §7 item 20: the
// collection detail and the group listing echoed `dir` as sent, `DESC`
// stayed `DESC`; the log-the-effective-value rule applied to envelopes).
type ListingPage struct {
	Page     int
	PageSize int
	Sort     string // an allowlisted key (collectionRepoSorts), "name" otherwise
	Dir      string // "asc" or "desc"
}

// NormalizeListingPage is the one clamp and allowlist for every repository
// listing (DBS-2 / API-4): page below 1 reads as 1, page_size below 1 as
// 50 and above 100 as 100 (two bare single-comparison clamps — the CodeQL
// go/uncontrolled-allocation-size barrier shape, v0.27.65), an unknown sort
// key as "name", any dir but "desc" (in any case) as "asc".
func NormalizeListingPage(page, pageSize int, sortKey, sortDir string) ListingPage {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}
	// The offset (page−1)×page_size must fit an int: past this page it
	// overflowed negative and PostgreSQL refused the OFFSET with a 500 (ASVS
	// review of 0.29.125, A-N2). Nothing lists that far; the clamp lands on
	// an empty page.
	if maxPage := MaxListingPage(pageSize); page > maxPage {
		page = maxPage
	}
	if !CollectionRepoSortValid(sortKey) {
		sortKey = "name"
	}
	dir := "asc"
	if strings.EqualFold(sortDir, "desc") {
		dir = "desc"
	}
	return ListingPage{Page: page, PageSize: pageSize, Sort: sortKey, Dir: dir}
}

// repoListingSQL is the ONE repository-listing query (DBS-2): the group
// page and the collection detail differ only in the member set, which the
// caller names as a subquery over $1 yielding repo_id. $2 is the reader
// (star state), $3 the page size, $4 the offset. Counts come from
// collection_queue's CACHED totals and the forge's last_updated from the
// latest repo_info snapshot — never a per-request aggregate (the v0.27.4
// nginx-timeout class).
func repoListingSQL(memberSubquery string, p ListingPage) string {
	dir := "ASC"
	if p.Dir == "desc" {
		dir = "DESC"
	}
	orderBy := strings.ReplaceAll(collectionRepoSorts[p.Sort], "%s", dir) + ", r.repo_id"
	return `
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git,
		       COALESCE(q.last_issues, 0), COALESCE(q.last_prs, 0), COALESCE(q.last_commits, 0),
		       q.last_collected, ri.last_updated,
		       (ucs.user_id IS NOT NULL) AS starred
		FROM (` + memberSubquery + `) member
		JOIN aveloxis_data.repos r ON r.repo_id = member.repo_id
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		LEFT JOIN LATERAL (
			SELECT last_updated FROM aveloxis_data.repo_info
			WHERE repo_id = r.repo_id
			ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC LIMIT 1
		) ri ON TRUE
		LEFT JOIN aveloxis_ops.user_repo_stars ucs
			ON ucs.repo_id = r.repo_id AND ucs.user_id = $2
		ORDER BY ` + orderBy + `
		LIMIT $3 OFFSET $4`
}

// listRepos runs repoListingSQL and scans its rows.
func (s *PostgresStore) listRepos(ctx context.Context, memberSubquery string, ownerID int64, userID int, p ListingPage) ([]PortalGroupRepo, error) {
	rows, err := s.pool.Query(ctx, repoListingSQL(memberSubquery, p), ownerID, userID, p.PageSize, (p.Page-1)*p.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// CONSTANT capacity hint — never the page size (the CodeQL
	// go/uncontrolled-allocation-size rule doesn't credit clamps as
	// barriers; v0.27.66).
	out := make([]PortalGroupRepo, 0, 50)
	for rows.Next() {
		var g PortalGroupRepo
		if err := rows.Scan(&g.RepoID, &g.Owner, &g.Name, &g.GitURL,
			&g.Issues, &g.PRs, &g.Commits, &g.LastCollected, &g.LastActivity, &g.Starred); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GetPortalGroupRepos lists one page of a group's repos plus the group's
// TOTAL repo count (for the pager), through the one listing query. Who may
// read the group is decided before this is called
// (admin.Service.GroupAccess, stage 1 of summary/59); userID is the reader
// whose star state the rows carry. page, pageSize, sortKey and sortDir are
// normalised by NormalizeListingPage, the values the caller echoes.
func (s *PostgresStore) GetPortalGroupRepos(ctx context.Context, userID int, groupID int64, page, pageSize int, sortKey, sortDir string) ([]PortalGroupRepo, int, error) {
	p := NormalizeListingPage(page, pageSize, sortKey, sortDir)
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID).Scan(&total); err != nil {
		return nil, 0, err
	}
	out, err := s.listRepos(ctx, `SELECT repo_id FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID, userID, p)
	if err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// GetUserIdentity returns the /api/v1/me display identity: login,
// display name (first + last as stored from OAuth, trimmed), and
// avatar URL. Replaces the v0.27.77 GetUserLogin (v0.27.84: the home
// greeting and nav avatar render the REAL user — a leftover GUI mock
// showed every user "Welcome back, Sean").
func (s *PostgresStore) GetUserIdentity(ctx context.Context, userID int) (login, name, avatarURL string, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT login_name,
		       TRIM(first_name || ' ' || last_name),
		       COALESCE(avatar_url, '')
		FROM aveloxis_ops.users WHERE user_id = $1`, userID).Scan(&login, &name, &avatarURL)
	return login, name, avatarURL, err
}

// GetUserAccount is the account identity the profile page shows: the
// forge the account signed in with ("github" or "gitlab"; a row from
// before oauth_provider existed is a GitHub row, as the OAuth upsert
// treats it) and the confirmed email — the forge's own address, taken at
// sign-in, or one confirmed through the click-to-confirm link. Empty
// email: the forge gave none and nothing was confirmed since (2026-10-04).
func (s *PostgresStore) GetUserAccount(ctx context.Context, userID int) (provider, email string, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT `+effectiveProviderSQL("")+`, COALESCE(email, '')
		FROM aveloxis_ops.users WHERE user_id = $1`, userID).Scan(&provider, &email)
	return provider, email, err
}

// MaxListingPage is the last page whose offset (page−1)×pageSize still fits
// an int — the one spelling of that clamp (queueview.ParseParams, the
// 0.29.109 round 2, and NormalizeListingPage; L10 round 2 of 0.29.125: a
// second spelling, math.MaxInt/pageSize+1, wrapped to MinInt for pageSize 1
// and emptied every page of that size). pageSize is at least 1.
func MaxListingPage(pageSize int) int {
	return (math.MaxInt-1)/pageSize + 1
}
