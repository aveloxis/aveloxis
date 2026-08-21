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
	"fmt"
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
	// v0.27.125 (Copilot round 16, suppressed): the literal
	// `platform_repo_id <> ''` predicate matches idx_repos_platform_repo_id's
	// partial-index condition so a GENERIC prepared plan can still use
	// the index — a bound `$2` alone cannot prove the predicate at plan
	// time, and without it every org-listing lookup risks a full repos
	// scan once Postgres switches to a generic plan (the v0.27.54
	// partial-index lesson, on the parameter side this time).
	// Semantically free: the Go guard above already rejects "".
	err := s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE platform_id = $1 AND platform_repo_id = $2
		  AND platform_repo_id <> ''
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
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET platform_repo_id = $2
		WHERE repo_id = $1 AND COALESCE(platform_repo_id, '') = ''`,
		repoID, forgeID)
	if err != nil {
		return err
	}
	// v0.27.116 (Copilot round 10, active): a zero-row update is EITHER
	// "already set to the same value" (the common case) OR "set to a
	// DIFFERENT value" — and the second is an identity conflict this
	// function's own doc comment promised not to paper over, yet nothing
	// detected it. The shape: a repo deleted upstream and RE-CREATED
	// under the same URL gets a NEW forge ID; org scans keep linking the
	// URL-matched row and the two unrelated histories silently merge.
	// Detection is OBSERVATION-ONLY (the house never-auto-mutate rule) —
	// the ERROR names the conflict and the remediation surface; the scan
	// pass itself proceeds unchanged.
	if tag.RowsAffected() == 0 {
		var stored string
		perr := s.pool.QueryRow(ctx,
			`SELECT COALESCE(platform_repo_id, '') FROM aveloxis_data.repos WHERE repo_id = $1`,
			repoID).Scan(&stored)
		if errors.Is(perr, pgx.ErrNoRows) {
			// Row deleted between the UPDATE and the probe — nothing to
			// verify; the only benign probe miss.
			return nil
		}
		if perr != nil {
			// v0.27.123 (Copilot round 15, suppressed): a probe FAILURE
			// is not success — swallowing it lost both the DB error and
			// the promised conflict signal (the "a lookup ERROR is not
			// 'no'" rule, inside this function's own detector).
			return fmt.Errorf("verify stored forge ID for repo %d: %w", repoID, perr)
		}
		if stored != "" && stored != forgeID {
			s.logger.Error("forge-ID mismatch on URL-matched repo — likely upstream delete-and-recreate under the same URL; unrelated histories may be merging on this row",
				"repo_id", repoID, "stored_forge_id", stored, "observed_forge_id", forgeID,
				"remediation", "inspect the row's data eras; a split needs operator action (reconcile-repos consolidates the INVERSE case only)")
		}
	}
	return nil
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
