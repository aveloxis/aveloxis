// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// CountActiveRepos returns the number of non-archived repositories in
// the catalog — the landing page's "N repositories under analysis"
// number (v0.27.59). Exact COUNT(*) on purpose: repos is ~100K rows
// (tens of milliseconds), and pg_class estimates cannot filter the
// archived cohort. Callers cache (60s, stale-on-error in the api
// layer), so this runs at most once a minute regardless of traffic.
func (s *PostgresStore) CountActiveRepos(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repos WHERE NOT COALESCE(repo_archived, FALSE)`).Scan(&n)
	return n, err
}
