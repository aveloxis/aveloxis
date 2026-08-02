// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// contributor_activity_store.go — store layer for the v0.27.57
// contributor activity classification (GitHub contributionsCollection).
// The scheduler's activity ticker claims contributors here, fetches
// their trailing-year contribution summary via the batched GraphQL
// lookup, and writes the classification back. GITHUB-ONLY: the pool
// filter is gh_login (GitLab has no restricted-contributions
// equivalent — documented parity gap).

// ActivityCheckContributor is one claimable contributor for the
// activity-classification sweep.
type ActivityCheckContributor struct {
	ID    string // cntrb_id
	Login string // gh_login
}

// ContributorActivityUpdate carries one contributor's classified
// trailing-year activity for the batched write.
type ContributorActivityUpdate struct {
	CntrbID              string
	PublicContribs       int
	RestrictedContribs   int
	LastContributionYear int
	ActivityClass        string
}

// GetContributorsForActivityCheck returns contributors due for an
// activity check, never-checked first, then oldest-checked — the same
// claim contract as GetContributorsForBreadth, including the jittered
// cooldown (BreadthCooldownJitterFrac): the sweep has the same
// cohort-echo dynamics as breadth, and an unjittered cliff would
// re-bunch check dates forever. Served by
// idx_contributors_activity_checked (ASC NULLS FIRST — the v0.27.8
// lesson: without the explicit ordering the claim full-sorts the 2.4M
// contributor rows on every tick).
func (s *PostgresStore) GetContributorsForActivityCheck(ctx context.Context, limit int, cooldown time.Duration) ([]ActivityCheckContributor, error) {
	if limit <= 0 {
		return nil, nil
	}
	if cooldown <= 0 {
		cooldown = 7 * 24 * time.Hour
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, gh_login
		FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL
		  AND gh_login != ''
		  AND COALESCE(cntrb_deleted, 0) = 0
		  AND (gh_activity_checked_at IS NULL
		       OR gh_activity_checked_at < NOW() - ($2::interval * (1.0 - $3::float8 + random() * 2.0 * $3::float8)))
		ORDER BY gh_activity_checked_at ASC NULLS FIRST
		LIMIT $1`, limit, cooldown.String(), BreadthCooldownJitterFrac)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActivityCheckContributor
	for rows.Next() {
		var c ActivityCheckContributor
		if err := rows.Scan(&c.ID, &c.Login); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpdateContributorActivityBatch writes classified activity for a batch
// of contributors in one transaction, stamping gh_activity_checked_at
// alongside the data so a stored classification and its freshness can
// never drift apart.
func (s *PostgresStore) UpdateContributorActivityBatch(ctx context.Context, updates []ContributorActivityUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, u := range updates {
		b.Queue(`
			UPDATE aveloxis_data.contributors
			SET gh_public_contribs_year = $2,
			    gh_restricted_contribs_year = $3,
			    gh_last_contribution_year = $4,
			    gh_activity_class = $5,
			    gh_activity_checked_at = NOW()
			WHERE cntrb_id = $1::uuid`,
			u.CntrbID, u.PublicContribs, u.RestrictedContribs, u.LastContributionYear, u.ActivityClass)
	}
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	for range updates {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("update contributor activity batch: %w", err)
		}
	}
	return nil
}

// MarkActivityCheckedBatch stamps gh_activity_checked_at for
// contributors whose check yielded NO data (deleted/renamed accounts
// absent from the GraphQL result). It deliberately touches ONLY the
// stamp — clobbering a previously-stored class with empties on a later
// dataless check would erase good data. Chunked ANY(uuid[]) per the
// MarkBreadthAttemptedBatch pattern. Marking unconditionally is the
// v0.20.17 lesson: unmarked dead-enders pin the NULLS-FIRST queue head
// forever.
func (s *PostgresStore) MarkActivityCheckedBatch(ctx context.Context, cntrbIDs []string) error {
	const chunk = 500
	for start := 0; start < len(cntrbIDs); start += chunk {
		end := start + chunk
		if end > len(cntrbIDs) {
			end = len(cntrbIDs)
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_activity_checked_at = NOW()
			WHERE cntrb_id = ANY($1::uuid[])`, cntrbIDs[start:end]); err != nil {
			return fmt.Errorf("mark activity checked batch: %w", err)
		}
	}
	return nil
}
