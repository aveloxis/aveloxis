// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.105 — store side of the whitespace measurement (fill-audit
// Workstream C). See internal/collector/whitespace.go for the walker
// and the Augur-parity semantics.

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CommitWhitespaceStat carries one file's whitespace-ADJUSTED counters
// for one commit (Augur algorithm — added/removed here supersede the
// raw numstat values the facade's first pass stored).
type CommitWhitespaceStat struct {
	Hash       string
	Filename   string
	Added      int
	Removed    int
	Whitespace int
}

// whitespaceUpdateChunk bounds one UPDATE's unnest arrays. 5000 rows ×
// 5 array params is far under the 65,535-param wire limit (arrays are
// single params) — the bound is about statement size and lock scope.
const whitespaceUpdateChunk = 5000

// UpdateCommitWhitespaceBatch applies adjusted counters onto existing
// commits rows, joined on the exact (repo_id, cmt_commit_hash,
// cmt_filename) key the facade stored (idx_commits_repo_hash_file).
// Rows whose values already match are untouched (IS DISTINCT guard) so
// steady-state incremental walks and re-runs are near-free.
//
// Returns (updated, matched): matched counts the stats whose
// (hash, filename) key exists as a stored row REGARDLESS of whether the
// values changed (v0.27.116, Copilot round 10 active finding). Rows the
// numstat pass never stored — historical per-row write failures the
// pre-v0.27.107 facade swallowed while last_collected still advanced —
// are invisible to the UPDATE, and the walker must NOT stamp the
// incremental marker over them (the marker-over-missing-rows class,
// third path): a later facade cycle re-inserts the rows, but the
// incremental walk would never revisit their whitespace. The caller
// compares matched against its emitted-stat total and refuses the
// stamp on shortfall.
func (s *PostgresStore) UpdateCommitWhitespaceBatch(ctx context.Context, repoID int64, stats []CommitWhitespaceStat) (updated, matched int64, err error) {
	for start := 0; start < len(stats); start += whitespaceUpdateChunk {
		end := min(start+whitespaceUpdateChunk, len(stats))
		chunk := stats[start:end]
		hashes := make([]string, len(chunk))
		files := make([]string, len(chunk))
		added := make([]int32, len(chunk))
		removed := make([]int32, len(chunk))
		ws := make([]int32, len(chunk))
		for i, c := range chunk {
			hashes[i] = c.Hash
			files[i] = c.Filename
			added[i] = int32(c.Added)
			removed[i] = int32(c.Removed)
			ws[i] = int32(c.Whitespace)
		}
		cerr := s.withRetry(ctx, func(ctx context.Context) error {
			tag, uerr := s.pool.Exec(ctx, `
				UPDATE aveloxis_data.commits c
				SET cmt_added = v.added,
				    cmt_removed = v.removed,
				    cmt_whitespace = v.ws
				FROM (
					SELECT unnest($2::text[]) AS hash,
					       unnest($3::text[]) AS filename,
					       unnest($4::int[])  AS added,
					       unnest($5::int[])  AS removed,
					       unnest($6::int[])  AS ws
				) v
				WHERE c.repo_id = $1
				  AND c.cmt_commit_hash = v.hash
				  AND c.cmt_filename = v.filename
				  AND (c.cmt_added IS DISTINCT FROM v.added
				    OR c.cmt_removed IS DISTINCT FROM v.removed
				    OR c.cmt_whitespace IS DISTINCT FROM v.ws)`,
				repoID, hashes, files, added, removed, ws)
			if uerr == nil {
				updated += tag.RowsAffected()
			}
			return uerr
		})
		if cerr != nil {
			return updated, matched, fmt.Errorf("whitespace batch update (chunk at %d): %w", start, cerr)
		}
		// Existence probe, independent of the IS DISTINCT guard — one
		// indexed join per chunk via idx_commits_repo_hash_file, so a
		// re-run whose values all already match still reports full
		// coverage.
		cerr = s.withRetry(ctx, func(ctx context.Context) error {
			var n int64
			perr := s.pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM (SELECT unnest($2::text[]) AS hash, unnest($3::text[]) AS filename) v
				JOIN aveloxis_data.commits c
				  ON c.repo_id = $1
				 AND c.cmt_commit_hash = v.hash
				 AND c.cmt_filename = v.filename`,
				repoID, hashes, files).Scan(&n)
			if perr == nil {
				matched += n
			}
			return perr
		})
		if cerr != nil {
			return updated, matched, fmt.Errorf("whitespace match probe (chunk at %d): %w", start, cerr)
		}
	}
	return updated, matched, nil
}

// GetWhitespaceHead reads the repo's whitespace walk marker — the
// branch head at the last completed walk. "" = never walked.
func (s *PostgresStore) GetWhitespaceHead(ctx context.Context, repoID int64) (string, error) {
	var head string
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(whitespace_head_hash, '') FROM aveloxis_data.repos WHERE repo_id = $1`,
		repoID).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return head, err
}

// SetWhitespaceHead stamps the marker after a successful walk.
func (s *PostgresStore) SetWhitespaceHead(ctx context.Context, repoID int64, head string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET whitespace_head_hash = $2 WHERE repo_id = $1`,
		repoID, head)
	return err
}

// IsRepoCollecting reports whether the repo is currently claimed by a
// collection worker. The rewalk command re-checks this immediately
// before each walk (v0.27.112, Copilot active finding): the page-time
// status filter can go stale, and while overlapping with a collection
// is DATA-safe (whitespace updates are same-value idempotent; the
// marker stamps only on success), the two would contend on the shared
// persistent clone — facade's stale-clone RemoveAll or concurrent git
// fetch locks can fail the walk. Skipped repos keep an empty marker
// and are picked up by a rerun. A durable queue claim was considered
// and REJECTED: marking the repo 'collecting' for the walk would block
// normal collection during a multi-hour fleet job; the residual
// seconds-wide race after this re-check only produces a retryable
// failure, never bad data.
func (s *PostgresStore) IsRepoCollecting(ctx context.Context, repoID int64) (bool, error) {
	var collecting bool
	err := s.pool.QueryRow(ctx, `
		SELECT status = 'collecting' FROM aveloxis_ops.collection_queue
		WHERE repo_id = $1`, repoID).Scan(&collecting)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return collecting, err
}

// RepoWhitespaceRewalkState reports, for the command's single-repo
// mode, the two gates the fleet query (GetReposForWhitespaceRewalk)
// applies in SQL (v0.27.113 — Copilot round-8 active finding: the
// --repo-id path bypassed both): collected = the repo has EVER
// completed a collection (a never-collected repo has NO commit rows —
// walking it stamps the marker over nothing, and the later first
// collection's incremental phase then skips all historical whitespace
// forever, the marker-over-missing-rows class); collecting = currently
// claimed by a collection worker. An untracked repo (no queue row)
// reports (false, false) — untracked repos are outside the rewalk's
// contract entirely (the v0.27.20 "tracked" definition).
func (s *PostgresStore) RepoWhitespaceRewalkState(ctx context.Context, repoID int64) (collected, collecting bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT last_collected IS NOT NULL, status = 'collecting'
		FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&collected, &collecting)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	return collected, collecting, err
}

// WhitespaceRewalkTarget is one repo the rewalk command should visit.
type WhitespaceRewalkTarget struct {
	RepoID int64
	GitURL string
}

// GetReposForWhitespaceRewalk pages the un-walked tracked fleet by
// repo_id keyset for `aveloxis rewalk-whitespace`. Tracked = a
// collection_queue row exists (the v0.27.20 definition — dead/stranded
// catalog residue is excluded structurally); repos mid-collection are
// skipped this pass and picked up on a later one (overlap with the
// facade's own walk would be same-value idempotent anyway, this just
// avoids competing for the clone).
func (s *PostgresStore) GetReposForWhitespaceRewalk(ctx context.Context, afterRepoID int64, limit int) ([]WhitespaceRewalkTarget, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git
		FROM aveloxis_data.repos r
		JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE r.repo_id > $1
		  AND COALESCE(r.whitespace_head_hash, '') = ''
		  AND COALESCE(r.repo_archived, FALSE) = FALSE
		  AND q.status <> 'collecting'
		  -- v0.27.112 (wrongly-suppressed Copilot finding): never-collected
		  -- repos have NO commit rows yet — walking them stamps the marker
		  -- over nothing, and the later first collection's incremental walk
		  -- then skips those commits FOREVER (the exact class the facade's
		  -- clean-pass gate prevents). First collection bootstraps them
		  -- inline instead (marker NULL → full walk).
		  AND q.last_collected IS NOT NULL
		ORDER BY r.repo_id
		LIMIT $2`, afterRepoID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WhitespaceRewalkTarget
	for rows.Next() {
		var t WhitespaceRewalkTarget
		if err := rows.Scan(&t.RepoID, &t.GitURL); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
