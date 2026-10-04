// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// RepoCacheState is what a cached repository-page answer is valid against
// (v0.29.73). Each field is a writer the page can observe:
//
//   - the queue row: updated_at (claim, CompleteJob success and failure,
//     re-queue, gap-heal — see CollectionGeneration) and last_collected;
//
//   - scancode_last_run, stamped by the decoupled ScancodeWorker outside
//     the collection job;
//
//   - vuln_scan_last_run, stamped at the scan's completed exits;
//
//   - data_changed_at, stamped through stampRepoCacheStateSQL, in the same
//     transaction as the data, by the writers that can run outside the job
//     (scorecard, scancode snapshot, vulnerability insert/resolve/rescore,
//     heal-libyear), and by StampRepoDataChanged at the end of a run outside
//     the queue (`aveloxis collect`). The authoritative list, enforced, is
//     pageCacheWriters in page_cache_writers_test.go. The completion stamps
//     above are separate statements that can fail after the data commits
//     (PR #226 review), so the data writers stamp themselves. A new writer
//     of repository-page data outside the job must do the same, or the page
//     serves its old answer until the next collection.
//
//   - the repository's identity (repo_owner, repo_name, repo_git), which
//     the SBOM and the time series name: a rename moves the state through any writer, with or
//     without a queue transition after it (PR #226 review 5404268512).
//
// Heartbeats stamp only locked_at, so a running job does not churn it.
// Collecting is the queue status, read so a re-warm waits for the job's end
// instead of caching a half-written repository.
type RepoCacheState struct {
	QueueUpdatedAt  time.Time
	LastCollected   time.Time
	ScancodeLastRun time.Time
	VulnScanLastRun time.Time
	DataChangedAt   time.Time
	// Identity is a digest of repo_owner, repo_name and repo_git.
	Identity    string
	HasQueueRow bool
	Collecting  bool
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
		f(st.ScancodeLastRun), f(st.VulnScanLastRun), f(st.DataChangedAt), st.Identity,
	}, "|")
}

// stampRepoCacheStateSQL is how a writer outside the collection job tells
// the repository-page cache its data changed: it moves repos.data_changed_at,
// which RepoCacheState reads. On repos, not the queue row (PR #226 review): a
// gone repository is dequeued with its data kept, so a queue stamp missed
// it. Used, in the same transaction as their data, by every stamping writer
// (pageCacheWriters in page_cache_writers_test.go is the enforced list) and
// by StampRepoDataChanged; the caller appends the WHERE clause on repo_id.
const stampRepoCacheStateSQL = `UPDATE aveloxis_data.repos SET data_changed_at = NOW()`

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
		       COALESCE(q.status = 'collecting', FALSE), r.scancode_last_run, r.vuln_scan_last_run, r.data_changed_at,
		       COALESCE(r.repo_owner, ''), COALESCE(r.repo_name, ''), COALESCE(r.repo_git, '')
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE r.repo_id = ANY($1::bigint[])`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("repo cache states: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id                                             int64
			st                                             RepoCacheState
			updated, collected, scancode, vulnRun, changed *time.Time
			owner, name, git                               string
		)
		if err := rows.Scan(&id, &st.HasQueueRow, &updated, &collected, &st.Collecting, &scancode, &vulnRun, &changed,
			&owner, &name, &git); err != nil {
			return nil, fmt.Errorf("repo cache states: %w", err)
		}
		for _, p := range []struct {
			src *time.Time
			dst *time.Time
		}{{updated, &st.QueueUpdatedAt}, {collected, &st.LastCollected}, {scancode, &st.ScancodeLastRun}, {vulnRun, &st.VulnScanLastRun}, {changed, &st.DataChangedAt}} {
			if p.src != nil {
				*p.dst = *p.src
			}
		}
		st.Identity = repoIdentityDigest(owner, name, git)
		out[id] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo cache states: %w", err)
	}
	return out, nil
}

// StampRepoDataChanged moves one repository's cache state: the stamp for a
// whole run that writes the repository's data outside the collection queue
// (the one-shot `aveloxis collect`), whose end no CompleteJob marks.
func (s *PostgresStore) StampRepoDataChanged(ctx context.Context, repoID int64) error {
	if _, err := s.pool.Exec(ctx, stampRepoCacheStateSQL+` WHERE repo_id = $1`, repoID); err != nil {
		return fmt.Errorf("stamp repository data changed (repo %d): %w", repoID, err)
	}
	return nil
}

// repoIdentityDigest is the repository's identity as one fixed-width token
// for the fingerprint: NUL-separated (no field can contain NUL), so two
// identities never read alike, and no URL text lands in a key.
func repoIdentityDigest(owner, name, git string) string {
	sum := sha256.Sum256([]byte(owner + "\x00" + name + "\x00" + git))
	return hex.EncodeToString(sum[:8])
}
