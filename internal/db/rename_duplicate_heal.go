// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — rename_duplicate_heal.go (v0.27.22): self-healing for
// the rename-duplicate add. When a user adds a repo by its OLD name
// (danswer-ai/danswer), FindRepoByURL cannot match the collected row
// under the NEW name (onyx-dot-app/onyx — a real rename, not a case
// variant), so a fresh dataless row is created and enqueued. Prelim
// then follows the 301, finds the target already collected, and
// pre-v0.27.22 just skipped + dequeued — leaving the user's group
// pointing at a row that will never have data. The heal repoints the
// user-facing links to the collected winner and removes the dataless
// duplicate, so an add-by-old-name silently lands on the repo the
// user meant. Observed live 2026-07-17 (repos 95832-95835 → winners
// 94164/94180/94190/94193).
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// HealRenamedDuplicate repoints user_repos + user_repo_stars from the
// never-collected duplicate onto the collected winner and deletes the
// duplicate's queue/status/staging/repos rows, all in one transaction.
//
// healed=false (no error) when the duplicate is NOT safe to remove:
//   - it HAS been collected (last_collected set) — two data-bearing
//     rows for one project is dedup territory, not a silent delete
//     (the v0.25.32 dedup-repos posture: full consolidation with
//     child handling, operator-invoked); or
//   - the delete trips a child FK at commit (some table holds rows
//     for it despite the never-collected gate — fail-safe: keep it).
//
// Callers fall back to the legacy skip+dequeue on healed=false.
func (s *PostgresStore) HealRenamedDuplicate(ctx context.Context, dupRepoID, winnerRepoID int64) (bool, error) {
	if dupRepoID == 0 || winnerRepoID == 0 || dupRepoID == winnerRepoID {
		return false, fmt.Errorf("invalid heal pair: dup=%d winner=%d", dupRepoID, winnerRepoID)
	}

	// Gate: never-collected duplicates only. A collected row's
	// children make the delete both unsafe and semantically wrong
	// (its history belongs in a deliberate consolidation).
	var collected bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM aveloxis_ops.collection_queue
			WHERE repo_id = $1 AND last_collected IS NOT NULL
		)`, dupRepoID).Scan(&collected); err != nil {
		return false, fmt.Errorf("checking duplicate collection state: %w", err)
	}
	if collected {
		return false, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// Repoint the user-facing links (insert-then-delete: both tables'
	// PKs include repo_id, so a user already linked/starred on the
	// winner just no-ops the insert).
	steps := []struct {
		label       string
		sql         string
		needsWinner bool
	}{
		{"repoint stars", `
			INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id)
			SELECT user_id, $2 FROM aveloxis_ops.user_repo_stars WHERE repo_id = $1
			ON CONFLICT DO NOTHING`, true},
		{"delete dup stars", `DELETE FROM aveloxis_ops.user_repo_stars WHERE repo_id = $1`, false},
		{"repoint group links", `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
			SELECT group_id, $2 FROM aveloxis_ops.user_repos WHERE repo_id = $1
			ON CONFLICT DO NOTHING`, true},
		{"delete dup group links", `DELETE FROM aveloxis_ops.user_repos WHERE repo_id = $1`, false},
		{"delete dup status", `DELETE FROM aveloxis_ops.collection_status WHERE repo_id = $1`, false},
		{"delete dup staging", `DELETE FROM aveloxis_ops.staging WHERE repo_id = $1`, false},
		{"delete dup queue row", `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, false},
		{"delete dup repos row", `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, false},
	}
	for _, st := range steps {
		args := []any{dupRepoID}
		if st.needsWinner {
			args = append(args, winnerRepoID)
		}
		if _, err := tx.Exec(ctx, st.sql, args...); err != nil {
			if isFKViolation(err) {
				// Some child table holds rows for the "never-collected"
				// duplicate — fail safe: keep it, legacy skip applies.
				return false, nil
			}
			return false, fmt.Errorf("%s: %w", st.label, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		// The repos FKs are DEFERRABLE INITIALLY DEFERRED (v0.22.7),
		// so a child-row violation surfaces HERE, not at Exec time.
		if isFKViolation(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// isFKViolation reports SQLSTATE 23503 (foreign_key_violation).
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
