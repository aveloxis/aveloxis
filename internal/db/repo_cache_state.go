// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// RepoCacheState is what a cached repository-page answer is valid against
// (v0.29.73). Each field is a writer the page can observe:
//   - the queue row: updated_at (claim, CompleteJob success and failure,
//     re-queue, gap-heal — see CollectionGeneration) and last_collected;
//   - scancode_last_run, stamped by the decoupled ScancodeWorker outside
//     the collection job;
//   - vuln_scan_last_run, stamped at the scan's completed exits.
//
// Writers outside the job stamp the queue row through stampRepoCacheStateSQL:
// ReplaceScorecard (`aveloxis run-scorecard`) and UpdateCVSSScoreForVector
// (`heal-vulnerabilities --rescore-only`). A new writer of repository-page
// data outside the job must do the same, or the page serves its old answer
// until the next collection.
// Heartbeats stamp only locked_at, so a running job does not churn it.
// Collecting is the queue status, read so a re-warm waits for the job's end
// instead of caching a half-written repository.
type RepoCacheState struct {
	QueueUpdatedAt  time.Time
	LastCollected   time.Time
	ScancodeLastRun time.Time
	VulnScanLastRun time.Time
	HasQueueRow     bool
	Collecting      bool
}

// Fingerprint is the state as one string: equal fingerprints mean no
// observed writer ran in between. The one spelling every consumer compares
// (SR-17).
func (st RepoCacheState) Fingerprint() string {
	f := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	return strings.Join([]string{
		fmt.Sprint(st.HasQueueRow), f(st.QueueUpdatedAt), f(st.LastCollected),
		f(st.ScancodeLastRun), f(st.VulnScanLastRun),
	}, "|")
}

// stampRepoCacheStateSQL is how a writer outside the collection job tells
// the repository-page cache its data changed: it moves the queue row's
// updated_at, which RepoCacheState (and CollectionGeneration) read. Used by
// ReplaceScorecard and UpdateCVSSScoreForVector; the caller appends the
// WHERE clause. updated_at is otherwise only displayed (ListQueuePage), never
// scheduled on.
const stampRepoCacheStateSQL = `UPDATE aveloxis_ops.collection_queue SET updated_at = NOW()`

// RepoCacheStates reads the cache state of each repository in repoIDs. An id
// with no repos row is absent from the map. One index scan over each
// table's primary key.
func (s *PostgresStore) RepoCacheStates(ctx context.Context, repoIDs []int64) (map[int64]RepoCacheState, error) {
	out := make(map[int64]RepoCacheState, len(repoIDs))
	if len(repoIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, q.repo_id IS NOT NULL, q.updated_at, q.last_collected,
		       COALESCE(q.status = 'collecting', FALSE), r.scancode_last_run, r.vuln_scan_last_run
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE r.repo_id = ANY($1::bigint[])`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("repo cache states: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id                                    int64
			st                                    RepoCacheState
			updated, collected, scancode, vulnRun *time.Time
		)
		if err := rows.Scan(&id, &st.HasQueueRow, &updated, &collected, &st.Collecting, &scancode, &vulnRun); err != nil {
			return nil, fmt.Errorf("repo cache states: %w", err)
		}
		for _, p := range []struct {
			src *time.Time
			dst *time.Time
		}{{updated, &st.QueueUpdatedAt}, {collected, &st.LastCollected}, {scancode, &st.ScancodeLastRun}, {vulnRun, &st.VulnScanLastRun}} {
			if p.src != nil {
				*p.dst = *p.src
			}
		}
		out[id] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo cache states: %w", err)
	}
	return out, nil
}
