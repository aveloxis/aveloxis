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
	"fmt"
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

// GetPortalGroupReposForUser lists one page of a group's repos plus
// the group's TOTAL repo count (for the pager), enforcing ownership:
// non-admin callers may only read their own groups. sortKey/sortDir
// resolve through the collectionRepoSorts ALLOWLIST before any
// interpolation — unknown values fall back to name asc.
func (s *PostgresStore) GetPortalGroupReposForUser(ctx context.Context, userID int, groupID int64, isAdmin bool, page, pageSize int, sortKey, sortDir string) ([]PortalGroupRepo, int, error) {
	if !isAdmin {
		var owner int
		err := s.pool.QueryRow(ctx,
			`SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner)
		if err != nil || owner != userID {
			return nil, 0, fmt.Errorf("group %d is not yours", groupID)
		}
	}
	if page < 1 {
		page = 1
	}
	// Two bare single-comparison clamps, NOT one compound condition —
	// the CodeQL go/uncontrolled-allocation-size barrier shape
	// (v0.27.65 lesson).
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortExpr, ok := collectionRepoSorts[sortKey]
	if !ok {
		sortExpr = collectionRepoSorts["name"]
	}
	dir := "ASC"
	if sortDir == "desc" {
		dir = "DESC"
	}
	orderBy := strings.ReplaceAll(sortExpr, "%s", dir) + ", r.repo_id"

	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git,
		       COALESCE(q.last_issues, 0), COALESCE(q.last_prs, 0), COALESCE(q.last_commits, 0),
		       q.last_collected, ri.last_updated,
		       (ucs.user_id IS NOT NULL) AS starred
		FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		LEFT JOIN LATERAL (
			SELECT last_updated FROM aveloxis_data.repo_info
			WHERE repo_id = r.repo_id
			ORDER BY data_collection_date DESC LIMIT 1
		) ri ON TRUE
		LEFT JOIN aveloxis_ops.user_repo_stars ucs
			ON ucs.repo_id = r.repo_id AND ucs.user_id = $2
		WHERE ur.group_id = $1
		ORDER BY `+orderBy+`
		LIMIT $3 OFFSET $4`, groupID, userID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	// CONSTANT capacity hint — never `pageSize` (the CodeQL
	// go/uncontrolled-allocation-size rule doesn't credit clamps as
	// barriers; v0.27.66).
	out := make([]PortalGroupRepo, 0, 50)
	for rows.Next() {
		var g PortalGroupRepo
		if err := rows.Scan(&g.RepoID, &g.Owner, &g.Name, &g.GitURL,
			&g.Issues, &g.PRs, &g.Commits, &g.LastCollected, &g.LastActivity, &g.Starred); err != nil {
			return nil, 0, err
		}
		out = append(out, g)
	}
	return out, total, rows.Err()
}

// GetPortalGroupOrgsForUser lists a group's tracked organizations
// (aveloxis_ops.user_org_requests — presence there means approved to
// scan, per the v0.27.20 rule), enforcing the same ownership gate as
// GetPortalGroupReposForUser: non-admin callers may only read their
// own groups. Read-only; delegates to loadGroupOrgs (the web GUI's
// loader, web_store.go) so the two surfaces can never disagree on
// what a "tracked org" is. Added 2026-07-21 — the SPA group page
// listed only repositories, leaving tracked orgs invisible.
func (s *PostgresStore) GetPortalGroupOrgsForUser(ctx context.Context, userID int, groupID int64, isAdmin bool) ([]GroupOrg, error) {
	if !isAdmin {
		var owner int
		err := s.pool.QueryRow(ctx,
			`SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner)
		if err != nil || owner != userID {
			return nil, fmt.Errorf("group %d is not yours", groupID)
		}
	}
	return s.loadGroupOrgs(ctx, groupID)
}
