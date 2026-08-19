// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — forge-numeric-ID identity for repos (rename/transfer dedup).
//
// repos.platform_repo_id has existed since inception but was 0% populated
// on production (0 of 142,480 rows) with zero readers. The 2026-08-19
// audit of `aveloxis reconcile-repos` output proved every one of the 12
// data-bearing duplicate pairs was an upstream rename or transfer that
// URL-based dedup structurally cannot catch. This file adds the readers
// and the opportunistic backfill writer; UpsertRepo (postgres.go) hosts
// the rename-heal branch; UpdateRepoMetadata (repo_metadata.go) is the
// Phase 0 fleet backfill.
package db

import (
	"context"
	"errors"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5"
)

// FindRepoByPlatformRepoID resolves a repo by the forge's numeric
// repository/project ID — the only identity that survives renames and
// transfers. Returns 0 when no row carries the ID (including the
// pre-backfill fleet, where the column is empty). forgeID "" always
// returns 0 without a query so an absent listing field can never match
// the (many) empty-column rows.
//
// When more than one row carries the same forge ID (pre-fix rename-dup
// residue that reconcile-repos hasn't consolidated yet), the OLDEST row
// wins — it has been referenced by child FKs the longest, matching the
// v0.20.2 merge-winner rule.
func (s *PostgresStore) FindRepoByPlatformRepoID(ctx context.Context, platform model.Platform, forgeID string) (int64, error) {
	if forgeID == "" {
		return 0, nil
	}
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE platform_id = $1 AND platform_repo_id = $2
		ORDER BY repo_id
		LIMIT 1`, int16(platform), forgeID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// SetPlatformRepoIDIfEmpty backfills the forge numeric ID onto an
// already-tracked row — fill-empty-only, never an overwrite (the forge
// ID never changes for a given repo; a differing stored value means a
// pre-existing consolidation problem that a scan pass must not paper
// over). Org scans call this on their found branch so the org-tracked
// cohort — exactly the population at risk of rename re-discovery —
// gains rename protection on the NEXT scan pass instead of waiting for
// each repo's Phase 0 collection cycle.
func (s *PostgresStore) SetPlatformRepoIDIfEmpty(ctx context.Context, repoID int64, forgeID string) error {
	if forgeID == "" {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET platform_repo_id = $2
		WHERE repo_id = $1 AND COALESCE(platform_repo_id, '') = ''`,
		repoID, forgeID)
	return err
}

// ensureForgeIDIndex builds the partial lookup index serving
// FindRepoByPlatformRepoID. Partial on non-empty values (the v0.19.9
// idx_contributors_gh_login precedent — parameter probes plan fine
// against it) so it stays near-empty until the fleet backfill fills the
// column. repos is small (~142K rows) but the CONCURRENTLY helper is
// the house pattern and costs nothing here.
func ensureForgeIDIndex(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	execCreateIndexConcurrently(ctx, pg, logger, errs,
		"aveloxis_data", "idx_repos_platform_repo_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_platform_repo_id
		 ON aveloxis_data.repos (platform_id, platform_repo_id)
		 WHERE platform_repo_id <> ''`)
}
