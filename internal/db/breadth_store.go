// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// BreadthContributor is a contributor needing breadth collection.
//
// v0.22.12 added GHUserID to enable the 404 rename-detection
// fallback: when /users/{Login}/events returns 404, the breadth
// worker calls /user/{GHUserID} (GitHub's by-id endpoint) to
// recover the current login. Without GHUserID we'd be stuck with
// a stale login forever.
type BreadthContributor struct {
	ID       string // cntrb_id
	Login    string // gh_login
	GHUserID int64  // numeric GitHub user ID; 0 if not yet observed
}

// ContributorRepoRow is a row to insert into contributor_repo.
type ContributorRepoRow struct {
	CntrbID   string
	RepoGit   string
	RepoName  string
	GHRepoID  int64
	Category  string // event type (PushEvent, PullRequestEvent, etc.)
	EventID   int64
	CreatedAt time.Time
}

// GetContributorsForBreadth returns contributors that need breadth
// collection, prioritizing those never attempted (NULL
// cntrb_last_breadth_at) then those past the cooldown window.
//
// v0.20.17: replaces the pre-fix JOIN against contributor_repo's
// MAX(data_collection_date). The old query treated a contributor
// as "processed" only if at least one event row had been
// inserted, so contributors whose /users/{login}/events returned
// empty stayed at the head of the queue forever and the worker
// reprocessed the same dead-end users every cycle. The new query
// filters by cntrb_last_breadth_at — which MarkBreadthAttempted
// stamps after EVERY attempt regardless of events found — so
// every contributor exits the queue after one attempt and
// re-enters only when the cooldown expires.
//
// Filter on cntrb_deleted = 0 (since v0.20.2 logical merges) so
// soft-deleted loser rows aren't re-attempted on every cycle.
// Filter on gh_login IS NOT NULL AND != ” since the breadth
// worker hits the GitHub user events API.
func (s *PostgresStore) GetContributorsForBreadth(ctx context.Context, limit int, cooldown time.Duration) ([]BreadthContributor, error) {
	if limit <= 0 {
		limit = 2000
	}
	if cooldown <= 0 {
		cooldown = 7 * 24 * time.Hour
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, gh_login, COALESCE(gh_user_id, 0)
		FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL
		  AND gh_login != ''
		  AND COALESCE(cntrb_deleted, 0) = 0
		  AND (cntrb_last_breadth_at IS NULL
		       OR cntrb_last_breadth_at < NOW() - $2::interval)
		ORDER BY cntrb_last_breadth_at ASC NULLS FIRST
		LIMIT $1`, limit, cooldown.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []BreadthContributor
	for rows.Next() {
		var c BreadthContributor
		if err := rows.Scan(&c.ID, &c.Login, &c.GHUserID); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// MarkBreadthAttempted stamps cntrb_last_breadth_at = NOW() for a
// contributor. The breadth worker calls this AFTER every attempt
// regardless of whether events were found. The unconditional
// stamp is what makes the cooldown-based queue actually drain —
// a contributor with zero public events still exits the
// unprocessed-queue and won't reappear until the cooldown window
// passes.
func (s *PostgresStore) MarkBreadthAttempted(ctx context.Context, cntrbID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_last_breadth_at = NOW()
		WHERE cntrb_id = $1::uuid`, cntrbID)
	return err
}

// breadthMarkChunkSize bounds how many cntrb_ids a single
// MarkBreadthAttemptedBatch UPDATE carries. 500 keeps each statement's
// array parameter and row-lock footprint small while still collapsing
// a fleet-scale cycle (18K contributors) into ~36 statements instead
// of 18,000.
const breadthMarkChunkSize = 500

// MarkBreadthAttemptedBatch stamps cntrb_last_breadth_at = NOW() for a
// set of contributors in chunked multi-row UPDATEs (v0.27.8). Same
// semantics as the single-row MarkBreadthAttempted — the unconditional
// stamp is what drains the cooldown queue — but one statement per
// breadthMarkChunkSize IDs instead of one per contributor. The
// single-row method is kept for compatibility.
//
// ORDERING CONTRACT (v0.27.8): callers must invoke this only AFTER the
// contributors' events are durably inserted into contributor_repo. A
// crash between fetch and insert must leave the contributor UNMARKED so
// the cooldown queue re-selects them next cycle (re-inserting is safe:
// contributor_repo has ON CONFLICT DO NOTHING). The breadth worker's
// coordinator loop enforces this by buffering IDs and flushing marks
// strictly after the corresponding InsertContributorRepoBatch calls.
func (s *PostgresStore) MarkBreadthAttemptedBatch(ctx context.Context, cntrbIDs []string) error {
	for start := 0; start < len(cntrbIDs); start += breadthMarkChunkSize {
		end := start + breadthMarkChunkSize
		if end > len(cntrbIDs) {
			end = len(cntrbIDs)
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET cntrb_last_breadth_at = NOW()
			WHERE cntrb_id = ANY($1::uuid[])`, cntrbIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// GetNewestContributorRepoEvent returns the most recent event timestamp
// for a contributor in contributor_repo. Returns zero time if none exist.
func (s *PostgresStore) GetNewestContributorRepoEvent(ctx context.Context, cntrbID string) (time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(created_at), '0001-01-01'::timestamptz)
		FROM aveloxis_data.contributor_repo
		WHERE cntrb_id = $1::uuid`, cntrbID).Scan(&t)
	if err != nil {
		return time.Time{}, nil
	}
	if t.Year() < 1970 {
		return time.Time{}, nil
	}
	return t, nil
}

// InsertContributorRepoBatch inserts multiple contributor-repo events in a single
// round-trip. Breadth collection can generate hundreds of events per contributor,
// so batching provides a significant speedup over individual inserts.
func (s *PostgresStore) InsertContributorRepoBatch(ctx context.Context, rows []*ContributorRepoRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO aveloxis_data.contributor_repo
				(cntrb_id, repo_git, repo_name, gh_repo_id, cntrb_category,
				 event_id, created_at,
				 tool_source, tool_version, data_source, data_collection_date)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7,
				'aveloxis-breadth', $8, 'GitHub API', NOW())
			ON CONFLICT (event_id, tool_version) DO NOTHING`,
			row.CntrbID, row.RepoGit, row.RepoName, row.GHRepoID,
			row.Category, row.EventID, row.CreatedAt, ToolVersion)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// InsertContributorRepo inserts a contributor-repo event. Returns nil on
// duplicate (ON CONFLICT DO NOTHING).
func (s *PostgresStore) InsertContributorRepo(ctx context.Context, row *ContributorRepoRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_repo
			(cntrb_id, repo_git, repo_name, gh_repo_id, cntrb_category,
			 event_id, created_at,
			 tool_source, tool_version, data_source, data_collection_date)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7,
			'aveloxis-breadth', $8, 'GitHub API', NOW())
		ON CONFLICT (event_id, tool_version) DO NOTHING`,
		row.CntrbID, row.RepoGit, row.RepoName, row.GHRepoID,
		row.Category, row.EventID, row.CreatedAt, ToolVersion)
	return err
}
