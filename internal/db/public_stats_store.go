// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// CountActiveRepos returns the TOTAL number of repositories in the
// catalog — the landing page's "N repositories under analysis" number.
//
// v0.27.68 (operator decision 2026-08-01): archived repos COUNT.
// v0.27.59 excluded them ("read-only isn't under analysis"), but that
// made the landing number (74,317) visibly disagree with the monitor
// (~94K queue rows) — a definition mismatch that reads as a bug to
// anyone who sees both. Archived repos carry full collected history,
// SBOMs, and vulnerability data; "under analysis" includes them.
//
// Exact COUNT(*) on purpose: repos is ~100K rows (tens of
// milliseconds). Callers cache (60s, stale-on-error in the api
// layer), so this runs at most once a minute regardless of traffic.
func (s *PostgresStore) CountActiveRepos(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repos`).Scan(&n)
	return n, err
}
