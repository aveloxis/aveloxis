// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Stranded-repo reconciliation (v0.27.39, summary/18 Phase 2).
//
// Production audit 2026-07-21: 222 non-archived repos rows had NO
// collection_queue row — invisible to the scheduler forever. Root
// cause (verified by live redirect checks on 12/12 samples): GitHub
// renames + prelim's duplicate branch, which skip+DEQUEUES without
// archiving. Two sub-populations: dataless pre-v0.27.22 leftovers the
// heal can never reach (prelim only runs on QUEUED repos), and
// data-bearing v0.27.22 "manual consolidation" carve-outs that had no
// consolidation tooling (dedup-repos discovers only case variants).
// `aveloxis reconcile-repos` consumes this surface.

package db

import (
	"context"
)

// StrandedRepo is a non-archived repos row with no collection_queue
// row.
type StrandedRepo struct {
	RepoID    int64
	GitURL    string
	Collected bool // has at least one repo_info snapshot (data-bearing)
}

// CountStrandedRepos returns how many non-archived repos have no
// queue row — the scheduler-invisibility gauge.
func (s *PostgresStore) CountStrandedRepos(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repos r
		WHERE COALESCE(r.repo_archived, FALSE) = FALSE
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = r.repo_id)`).Scan(&n)
	return n, err
}

// ListStrandedRepos returns up to limit stranded repos, oldest ids
// first (stable across passes; healed rows drop out of the set).
func (s *PostgresStore) ListStrandedRepos(ctx context.Context, limit int) ([]StrandedRepo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git,
		       EXISTS (SELECT 1 FROM aveloxis_data.repo_info ri WHERE ri.repo_id = r.repo_id)
		FROM aveloxis_data.repos r
		WHERE COALESCE(r.repo_archived, FALSE) = FALSE
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = r.repo_id)
		ORDER BY r.repo_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StrandedRepo
	for rows.Next() {
		var sr StrandedRepo
		if err := rows.Scan(&sr.RepoID, &sr.GitURL, &sr.Collected); err != nil {
			return nil, err
		}
		out = append(out, sr)
	}
	return out, rows.Err()
}

// DedupRenamedRepoPair consolidates a DATA-BEARING rename duplicate
// (stranded loser whose URL 301-redirects to the tracked winner) via
// the SAME per-pair machinery dedup-repos uses for case variants —
// repoints for shared-copy tables, leaves-first deletes, FOR UPDATE
// mid-collection re-check. Only pair DISCOVERY differs (redirect-
// resolved rather than LOWER(repo_git)-grouped).
func DedupRenamedRepoPair(ctx context.Context, store *PostgresStore, winnerID, loserID int64, winnerGit, loserGit string) error {
	return dedupOnePair(ctx, store, RepoDupPair{
		LowerGit:  loserGit + " -> " + winnerGit, // label for error/log text
		WinnerID:  winnerID,
		WinnerGit: winnerGit,
		LoserID:   loserID,
		LoserGit:  loserGit,
		GroupSize: 2,
	})
}
