// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"
)

// NewRepo is one row in the home page's "New Repositories" feed
// (v0.27.62): a repo that entered the fleet since the window start.
type NewRepo struct {
	RepoID  int64     `json:"repo_id"`
	Owner   string    `json:"owner"`
	Name    string    `json:"name"`
	Org     string    `json:"org"`
	AddedAt time.Time `json:"added_at"`
}

// newReposArmSQL builds one feed arm. The org set differs per arm —
// fleet = orgs registered by ADMIN users (the "curated fleet" signal:
// on the production deployment that is the operator's org tracking,
// generalized to every admin); mine = orgs the CALLER registered —
// but both share every other gate:
//
//   - the org's owning group is not 'rejected' (v0.27.20: RejectGroup
//     is the abuse lever; a rejected group's orgs surface nowhere),
//   - repos.added_at >= window start (the v0.27.60 fleet-entry stamp;
//     pre-v0.27.60 rows carry a last-touch approximation — the feed
//     is honest-noisy for one window after that deploy, by design),
//   - archived repos excluded,
//   - owner ↔ org matching is case-insensitive (GitHub logins are
//     case-preserving but case-insensitive; org_url casing is
//     whatever the registrant typed). KNOWN v1 EDGE: GitLab nested
//     group paths ("group/subgroup") won't equal repo_owner and fall
//     out of the feed silently — accepted in the plan.
//
// org_url is "https://host/org", so SPLIT_PART(…, '/', 4) is the org
// login.
const newReposArmSQL = `
WITH org_set AS (
    SELECT DISTINCT LOWER(SPLIT_PART(uor.org_url, '/', 4)) AS org_login
    FROM aveloxis_ops.user_org_requests uor
    JOIN aveloxis_ops.users u ON u.user_id = uor.user_id
    JOIN aveloxis_ops.user_groups g ON g.group_id = uor.group_id
    WHERE COALESCE(g.status, 'approved') <> 'rejected'
      AND %s
)
SELECT r.repo_id, r.repo_owner, r.repo_name, os.org_login, r.added_at
FROM aveloxis_data.repos r
JOIN org_set os ON LOWER(r.repo_owner) = os.org_login
JOIN aveloxis_ops.collection_queue q
  ON q.repo_id = r.repo_id AND q.last_collected IS NOT NULL
WHERE r.added_at >= $1
  AND NOT COALESCE(r.repo_archived, FALSE)
ORDER BY r.added_at DESC, r.repo_id DESC
LIMIT $2`

// GetNewRepos returns the two "New Repositories" feed arms for userID:
// fleet (admin-registered orgs) and mine (the caller's own orgs), each
// newest-first and capped at limit rows. A repo under an org that is
// both admin-registered AND the caller's own appears in both arms —
// the frontend labels, it doesn't dedupe.
func (s *PostgresStore) GetNewRepos(ctx context.Context, userID int, since time.Time, limit int) (fleet, mine []NewRepo, err error) {
	if limit <= 0 {
		limit = 100
	}

	run := func(predicate string, args ...any) ([]NewRepo, error) {
		sql := fmt.Sprintf(newReposArmSQL, predicate)
		rows, err := s.pool.Query(ctx, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("GetNewRepos query: %w", err)
		}
		defer rows.Close()
		out := make([]NewRepo, 0, 32)
		for rows.Next() {
			var nr NewRepo
			if err := rows.Scan(&nr.RepoID, &nr.Owner, &nr.Name, &nr.Org, &nr.AddedAt); err != nil {
				return nil, fmt.Errorf("GetNewRepos scan: %w", err)
			}
			out = append(out, nr)
		}
		return out, rows.Err()
	}

	// Fleet arm: orgs registered by any admin user.
	fleet, err = run("u.admin", since, limit)
	if err != nil {
		return nil, nil, err
	}
	// Mine arm: orgs THIS user registered (admin or not).
	mine, err = run("uor.user_id = $3", since, limit, userID)
	if err != nil {
		return nil, nil, err
	}
	return fleet, mine, nil
}
