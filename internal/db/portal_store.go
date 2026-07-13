// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.3 — small store helpers for the portal JSON endpoints.

import (
	"context"
	"fmt"
)

// PortalGroupRepo is one repo inside a user group, for the group page.
type PortalGroupRepo struct {
	RepoID int64  `json:"repo_id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	GitURL string `json:"git_url"`
}

// GetPortalGroupReposForUser lists a group's repos, enforcing ownership:
// non-admin callers may only read their own groups.
func (s *PostgresStore) GetPortalGroupReposForUser(ctx context.Context, userID int, groupID int64, isAdmin bool) ([]PortalGroupRepo, error) {
	if !isAdmin {
		var owner int
		err := s.pool.QueryRow(ctx,
			`SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner)
		if err != nil || owner != userID {
			return nil, fmt.Errorf("group %d is not yours", groupID)
		}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git
		FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_data.repos r USING (repo_id)
		WHERE ur.group_id = $1
		ORDER BY r.repo_owner, r.repo_name`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PortalGroupRepo
	for rows.Next() {
		var g PortalGroupRepo
		if err := rows.Scan(&g.RepoID, &g.Owner, &g.Name, &g.GitURL); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}
