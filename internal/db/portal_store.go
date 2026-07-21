// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.3 — small store helpers for the portal JSON endpoints.
// v0.27.14 — the group listing paginates (limit/offset + total) so
// fleet-scale groups don't ship thousands of rows per render. The
// per-row count columns (annotated by the API layer from
// GetRepoStatsBatch) are ALL-TIME totals from the latest repo_info
// snapshot — deliberately NOT a 90-day activity query, which is the
// v0.27.4 nginx-timeout class at fleet scale.

import (
	"context"
	"fmt"
)

// PortalGroupRepo is one repo inside a user group, for the group page.
// The *_all_time counts and Starred are filled by the API layer per
// PAGE (GetRepoStatsBatch + GetUserStarredRepoIDs), never here — the
// store returns the bare listing slice.
type PortalGroupRepo struct {
	RepoID int64  `json:"repo_id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	GitURL string `json:"git_url"`
	// All-time totals from the latest repo_info snapshot (the forge's
	// own reported counts) — labeled all_time so the GUI says so.
	CommitsAllTime int  `json:"commits_all_time"`
	IssuesAllTime  int  `json:"issues_all_time"`
	PRsAllTime     int  `json:"prs_all_time"`
	Starred        bool `json:"starred"`
}

// GetPortalGroupReposForUser lists one page of a group's repos plus
// the group's TOTAL repo count (for the pager), enforcing ownership:
// non-admin callers may only read their own groups. limit/offset
// slice the (repo_owner, repo_name)-ordered listing; limit <= 0 means
// no limit (offset still applies).
func (s *PostgresStore) GetPortalGroupReposForUser(ctx context.Context, userID int, groupID int64, isAdmin bool, limit, offset int) ([]PortalGroupRepo, int, error) {
	if !isAdmin {
		var owner int
		err := s.pool.QueryRow(ctx,
			`SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner)
		if err != nil || owner != userID {
			return nil, 0, fmt.Errorf("group %d is not yours", groupID)
		}
	}
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupID).Scan(&total); err != nil {
		return nil, 0, err
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git
		FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_data.repos r USING (repo_id)
		WHERE ur.group_id = $1
		ORDER BY r.repo_owner, r.repo_name, r.repo_id
		LIMIT NULLIF($2, 0) OFFSET $3`, groupID, max(limit, 0), offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []PortalGroupRepo
	for rows.Next() {
		var g PortalGroupRepo
		if err := rows.Scan(&g.RepoID, &g.Owner, &g.Name, &g.GitURL); err != nil {
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
