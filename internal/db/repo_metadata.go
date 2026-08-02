// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"encoding/json"
)

// UpdateRepoMetadata writes description + primary_language + languages
// to the repos table for a single repo. Distinct from UpsertRepo so the
// staged collector's Phase 0 and the startup backfill task don't
// accidentally overwrite owner/name/archived (which UpsertRepo handles
// via the prelim path).
//
// languages is serialized to JSONB. nil/empty map writes '{}' so the
// row column never holds NULL — analytics queries can rely on
// jsonb_each over the column without NULL guards.
//
// COALESCE semantics:
//   - description: written unconditionally (description CAN change as
//     a repo's tagline evolves; staged collector runs every cycle)
//   - primary_language: written unconditionally (same reasoning;
//     dominant language shifts as code is added/removed)
//   - languages: written unconditionally (full breakdown is point-in-time)
//
// This is intentionally NOT a "fill-empty-only" UPDATE — operators want
// the displayed data to reflect the API's current state.
//
// v0.23.0; v0.27.78 adds forkedFrom (callers pass
// model.RepoInfo.ForkedFrom() — the single source of the stored
// representation). Written unconditionally like the other fields:
// the forge's current statement of fork lineage is the truth, and a
// repo detaching from its upstream honestly clears the column.
func (s *PostgresStore) UpdateRepoMetadata(ctx context.Context, repoID int64, description, primaryLanguage string, languages map[string]int, archived bool, forkedFrom string) error {
	langJSON := []byte("{}")
	if len(languages) > 0 {
		b, err := json.Marshal(languages)
		if err != nil {
			return err
		}
		langJSON = b
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.repos
			SET repo_description = $2,
			    primary_language = $3,
			    languages        = $4::jsonb,
			    -- v0.27.50: propagate the forge's archived status so
			    -- repos.repo_archived stops disagreeing with the
			    -- accurate repo_info.status (17,665 forge-archived
			    -- repos had the flag false). Both directions: an
			    -- un-archived repo flips back to false. prelim's
			    -- dead-repo ArchiveRepo path is unaffected — those
			    -- repos are dequeued and never reach Phase 0.
			    repo_archived    = $5,
			    -- v0.27.78: fork lineage from the forge (upstream
			    -- owner/name, model.UnknownForkParent for a fork with
			    -- a deleted upstream, '' for a non-fork). The showcase
			    -- fork filter and future GUI fork badges read this.
			    forked_from      = $6,
			    data_collection_date = NOW()
			WHERE repo_id = $1`,
			repoID, description, primaryLanguage, langJSON, archived, forkedFrom)
		return err
	})
}

// ReposNeedingMetadataBackfill returns repo IDs whose description AND
// primary_language are both empty. Used by the startup backfill task.
// Capped at `limit` to avoid pulling 100K rows into memory; the caller
// pages by repeatedly calling until len(result) < limit.
//
// Filters archived repos out — archived projects' descriptions rarely
// matter and we don't want to spend API budget on them. Operators can
// remove the filter manually if needed.
//
// v0.23.0.
func (s *PostgresStore) ReposNeedingMetadataBackfill(ctx context.Context, limit int) ([]RepoMetadataBackfillTarget, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_owner, repo_name, platform_id
		FROM aveloxis_data.repos
		WHERE COALESCE(repo_description, '') = ''
		  AND COALESCE(primary_language, '') = ''
		  AND COALESCE(repo_archived, FALSE) = FALSE
		  AND COALESCE(repo_owner, '') != ''
		  AND COALESCE(repo_name, '') != ''
		ORDER BY repo_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoMetadataBackfillTarget
	for rows.Next() {
		var t RepoMetadataBackfillTarget
		if err := rows.Scan(&t.RepoID, &t.Owner, &t.Name, &t.PlatformID); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RepoMetadataBackfillTarget is one row from ReposNeedingMetadataBackfill.
type RepoMetadataBackfillTarget struct {
	RepoID     int64
	Owner      string
	Name       string
	PlatformID int16
}

// GetReposForMetadataRefresh pages the WHOLE forge-backed fleet
// (GitHub + GitLab; generic git has no API to ask) by repo_id keyset
// for the operator-driven `aveloxis backfill-repo-metadata` sweep.
// Unlike ReposNeedingMetadataBackfill this does NOT filter on empty
// fields — the sweep's point is refreshing values that cannot be
// distinguished from absent (forked_from = ” means both "not a fork"
// and "never checked"), so every repo gets one visit. Archived repos
// are INCLUDED: they appear in public collections and their fork
// status matters there.
//
// v0.27.79.
func (s *PostgresStore) GetReposForMetadataRefresh(ctx context.Context, afterRepoID int64, limit int) ([]RepoMetadataBackfillTarget, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_owner, repo_name, platform_id
		FROM aveloxis_data.repos
		WHERE repo_id > $1
		  AND platform_id IN (1, 2)
		  AND COALESCE(repo_owner, '') != ''
		  AND COALESCE(repo_name, '') != ''
		ORDER BY repo_id
		LIMIT $2`, afterRepoID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoMetadataBackfillTarget
	for rows.Next() {
		var t RepoMetadataBackfillTarget
		if err := rows.Scan(&t.RepoID, &t.Owner, &t.Name, &t.PlatformID); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
