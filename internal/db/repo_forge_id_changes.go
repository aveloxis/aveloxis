// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ForgeIDChange is one observed change of a repository's forge ID — the
// repository was deleted and re-created upstream under the same URL.
// AdoptedAt is nil while the change is only observed; the repository page
// shows adopted changes (v0.29.62, 2026-09-23 operator decision: treat the
// new repository as a continuation, and say so where the data is read).
type ForgeIDChange struct {
	RepoID          int64      `json:"repo_id"`
	OldForgeID      string     `json:"old_forge_id"`
	NewForgeID      string     `json:"new_forge_id"`
	FirstObservedAt time.Time  `json:"first_observed_at"`
	LastObservedAt  time.Time  `json:"last_observed_at"`
	ForgeCreatedAt  *time.Time `json:"forge_created_at,omitempty"` // when the forge created the new repository
	AdoptedAt       *time.Time `json:"adopted_at,omitempty"`
	AdoptedBy       string     `json:"adopted_by,omitempty"`
	Note            string     `json:"note,omitempty"`
	// ObservedPrivate (0.29.140): the listing that last observed the change
	// was not public (GitHub private; GitLab internal or private). The
	// adoption of such a change moves the id but leaves a gone stamp in
	// place — an unauthenticated probe cannot see the repository.
	ObservedPrivate bool `json:"observed_private,omitempty"`
	// Listing fields (ListForgeIDChanges): the repository as stored, and
	// whether a pending change is SUPERSEDED — its old ID is no longer the
	// stored one (usually because another change was adopted), so it can no
	// longer be adopted. Derived from the current state on every read, never stored,
	// so it cannot drift; the row stays as the scan's history.
	RepoGit    string `json:"repo_git,omitempty"`
	Superseded bool   `json:"superseded,omitempty"`
}

// ErrForgeIDNotAsExpected — AdoptForgeID's guard: the stored forge ID is
// not the old ID the operator adopted over (someone else changed it, or
// the wrong repository was named). Nothing is written.
var ErrForgeIDNotAsExpected = errors.New("stored forge ID is not the expected old ID")

// recordForgeIDObservation writes what the org scan saw: stored ≠
// observed. A repeat observation only moves last_observed_at. It never
// touches repos (the observation-only rule, SR-7).
func (s *PostgresStore) recordForgeIDObservation(ctx context.Context, repoID int64, stored, observed string, forgeCreatedAt time.Time, private bool) error {
	var created *time.Time
	if !forgeCreatedAt.IsZero() {
		c := forgeCreatedAt.UTC()
		created = &c
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_forge_id_changes (repo_id, old_forge_id, new_forge_id, forge_created_at, observed_private)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (repo_id, old_forge_id, new_forge_id) DO UPDATE SET
			last_observed_at = NOW(),
			forge_created_at = COALESCE(EXCLUDED.forge_created_at, repo_forge_id_changes.forge_created_at),
			observed_private = EXCLUDED.observed_private`,
		repoID, stored, observed, created, private)
	return err
}

// ErrNoPendingForgeIDChange — AdoptPendingForgeIDChange found no
// recorded, unadopted change with that (old, new) pair for the repository.
var ErrNoPendingForgeIDChange = errors.New("no pending forge-ID change with that pair for this repository")

// AdoptPendingForgeIDChange adopts exactly the change the admin chose —
// the (oldID, newID) pair the org scan recorded, with the forge's creation
// date it listed — through AdoptForgeID, so the same guard applies: the
// stored ID must still be oldID, or ErrForgeIDNotAsExpected (a superseded
// change is refused, never re-targeted). Keyed by the pair, not by the
// repository (review of v0.29.63: "the newest pending row" adopted a
// change the admin had not clicked). The admin page's Adopt button calls
// it; `aveloxis adopt-forge-id` instead asks the forge live.
func (s *PostgresStore) AdoptPendingForgeIDChange(ctx context.Context, repoID int64, oldID, newID, adoptedBy, note string) error {
	var created *time.Time
	var private bool
	err := s.pool.QueryRow(ctx, `
		SELECT forge_created_at, observed_private
		  FROM aveloxis_data.repo_forge_id_changes
		 WHERE repo_id = $1 AND old_forge_id = $2 AND new_forge_id = $3 AND adopted_at IS NULL`,
		repoID, oldID, newID).Scan(&created, &private)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("repo %d %s→%s: %w", repoID, oldID, newID, ErrNoPendingForgeIDChange)
	}
	if err != nil {
		return fmt.Errorf("repo %d pending forge-ID change: %w", repoID, err)
	}
	return s.AdoptForgeID(ctx, repoID, oldID, newID, created, adoptedBy, note, !private)
}

// AdoptForgeID is the operator's approval of a forge-ID change: in one
// transaction it moves repos.platform_repo_id from oldID to newID — only
// if it still holds oldID (ErrForgeIDNotAsExpected otherwise) — and
// records the change as adopted, creating the record when the scan never
// observed it. forgeCreatedAt (nil = unknown) is the forge's creation date
// of the new repository, the date the page shows. publiclyReachable says
// the re-created repository was seen PUBLIC (the listing's visibility the
// scan recorded; the forge's answer to the CLI): only then does the adoption
// resurrect a gone-stamped row — a private or internal re-creation is
// adopted as the continuation but stays gone, since an unauthenticated probe
// cannot see it (Copilot PR #252, second review).
func (s *PostgresStore) AdoptForgeID(ctx context.Context, repoID int64, oldID, newID string, forgeCreatedAt *time.Time, adoptedBy, note string, publiclyReachable bool) error {
	if oldID == "" || newID == "" || oldID == newID {
		return fmt.Errorf("adopt forge ID for repo %d: old %q and new %q must be two different IDs", repoID, oldID, newID)
	}
	// withRetry and the daily table's per-repository lock FIRST (the fresh
	// review of the PR #252 round): the restore below writes repo_commit_daily,
	// whose every other writer goes lock → repos row, and an adoption that
	// went repos row → lock deadlocked with a trimming walk of the same
	// repository — a repository with a pending change keeps collecting — with
	// no retry on its side. Lock first, and the adoption simply waits for a
	// running walk; a 40P01 victim retries like the other daily writers.
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit
		if _, err := tx.Exec(ctx, repoCommitDailyLockSQL, repoID); err != nil {
			return fmt.Errorf("lock repo_commit_daily for repo %d: %w", repoID, err)
		}
		return s.adoptForgeIDInTx(ctx, tx, repoID, oldID, newID, forgeCreatedAt, adoptedBy, note, publiclyReachable)
	})
}

// adoptForgeIDInTx is AdoptForgeID's body once the transaction holds the
// daily table's lock: the id move, the record, the resurrection of a
// gone-stamped row, the earlier era's restore, the commit.
func (s *PostgresStore) adoptForgeIDInTx(ctx context.Context, tx pgx.Tx, repoID int64, oldID, newID string, forgeCreatedAt *time.Time, adoptedBy, note string, publiclyReachable bool) error {
	tag, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.repos SET platform_repo_id = $3
		WHERE repo_id = $1 AND platform_repo_id = $2`, repoID, oldID, newID)
	if err != nil {
		return fmt.Errorf("adopt forge ID for repo %d: %w", repoID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("repo %d: %w (expected %s)", repoID, ErrForgeIDNotAsExpected, oldID)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_forge_id_changes
			(repo_id, old_forge_id, new_forge_id, forge_created_at, adopted_at, adopted_by, note, observed_private)
		VALUES ($1, $2, $3, $4, NOW(), $5, $6, NOT $7::boolean)
		ON CONFLICT (repo_id, old_forge_id, new_forge_id) DO UPDATE SET
			forge_created_at = COALESCE(EXCLUDED.forge_created_at, repo_forge_id_changes.forge_created_at),
			adopted_at = NOW(), adopted_by = EXCLUDED.adopted_by, note = EXCLUDED.note,
			observed_private = EXCLUDED.observed_private`,
		repoID, oldID, newID, forgeCreatedAt, adoptedBy, note, publiclyReachable); err != nil {
		return fmt.Errorf("record adopted forge ID for repo %d: %w", repoID, err)
	}
	// 0.29.140: the adoption says the re-created repository is this row's
	// continuation — and it exists, so a gone stamp on the row is stale
	// (bobsql: adopted at 15:49, still "no longer publicly available" and
	// absent from the monitor until reconcile-repos happened to re-enqueue
	// it). Cleared, its episode closed and re-enqueued in this transaction;
	// a row that is not gone-stamped is left as it is.
	if publiclyReachable {
		if err := resurrectInTx(ctx, tx, repoID, resurrectPriority, GoneSourceAdoption); err != nil {
			return fmt.Errorf("resurrect repo %d on adoption: %w", repoID, err)
		}
	}
	// Copilot PR #252: the earlier era's daily buckets a walk trimmed before
	// this adoption come back from the commits table (restoreEarlierErasInTx),
	// under the daily table's per-repository lock the transaction took first;
	// the charts change, so the cache state is stamped when any bucket was
	// written. Documented residual: the restore is a fold of the repository's
	// commit rows (minutes for a kernel-sized repository), inside the request
	// that adopts — the GUI and the API adopt under the request's deadline,
	// so a very large re-created repository is adopted with the CLI.
	restored, err := restoreEarlierErasInTx(ctx, tx, repoID)
	if err != nil {
		return fmt.Errorf("repo %d: %w", repoID, err)
	}
	if restored > 0 {
		if _, err := tx.Exec(ctx, stampRepoCacheStateSQL+` WHERE repo_id = $1`, repoID); err != nil {
			return fmt.Errorf("stamp repository %d data changed: %w", repoID, err)
		}
	}
	return tx.Commit(ctx)
}

// ListForgeIDChanges lists the recorded changes with each repository's
// stored URL. pendingOnly lists the ADOPTABLE ones: not adopted, and
// starting from the ID the repository stores now. The full list orders
// adoptable first, then superseded (marked), then adopted, newest
// observation first within each.
func (s *PostgresStore) ListForgeIDChanges(ctx context.Context, pendingOnly bool) ([]ForgeIDChange, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.repo_id, c.old_forge_id, c.new_forge_id, c.first_observed_at, c.last_observed_at,
		       c.forge_created_at, c.adopted_at, c.adopted_by, c.note, COALESCE(r.repo_git, ''),
		       (c.adopted_at IS NULL AND c.old_forge_id IS DISTINCT FROM r.platform_repo_id) AS superseded,
		       c.observed_private
		  FROM aveloxis_data.repo_forge_id_changes c
		  JOIN aveloxis_data.repos r USING (repo_id)
		 WHERE NOT $1 OR (c.adopted_at IS NULL AND c.old_forge_id = r.platform_repo_id)
		 ORDER BY (c.adopted_at IS NOT NULL), (c.old_forge_id IS DISTINCT FROM r.platform_repo_id),
		          c.last_observed_at DESC, c.change_id DESC`, pendingOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForgeIDChange
	for rows.Next() {
		var c ForgeIDChange
		if err := rows.Scan(&c.RepoID, &c.OldForgeID, &c.NewForgeID, &c.FirstObservedAt, &c.LastObservedAt,
			&c.ForgeCreatedAt, &c.AdoptedAt, &c.AdoptedBy, &c.Note, &c.RepoGit, &c.Superseded, &c.ObservedPrivate); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// repoForgeIDChangesAdoptedBatch is the one reader of the adopted changes
// (stage 1b: the stats batch carries them for every id; the single-id
// reader it replaced had no other caller): per repository, in adoption
// order. An id without changes is absent from the map.
func (s *PostgresStore) repoForgeIDChangesAdoptedBatch(ctx context.Context, repoIDs []int64) (map[int64][]ForgeIDChange, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, old_forge_id, new_forge_id, first_observed_at, last_observed_at,
		       forge_created_at, adopted_at, adopted_by, note, observed_private
		  FROM aveloxis_data.repo_forge_id_changes
		 WHERE repo_id = ANY($1) AND adopted_at IS NOT NULL
		 ORDER BY repo_id, adopted_at, change_id`, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]ForgeIDChange{}
	for rows.Next() {
		var c ForgeIDChange
		if err := rows.Scan(&c.RepoID, &c.OldForgeID, &c.NewForgeID, &c.FirstObservedAt, &c.LastObservedAt,
			&c.ForgeCreatedAt, &c.AdoptedAt, &c.AdoptedBy, &c.Note, &c.ObservedPrivate); err != nil {
			return nil, err
		}
		out[c.RepoID] = append(out[c.RepoID], c)
	}
	return out, rows.Err()
}

// GetRepoForgeID returns the stored forge ID ("" when never captured). A
// missing repository is pgx.ErrNoRows, never "" (SR-5).
func (s *PostgresStore) GetRepoForgeID(ctx context.Context, repoID int64) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(platform_repo_id, '') FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&id)
	return id, err
}

// UserLabel names a user for an audit field: their login. A missing user
// is pgx.ErrNoRows (SR-5), never an empty label.
func (s *PostgresStore) UserLabel(ctx context.Context, userID int) (string, error) {
	var login string
	err := s.pool.QueryRow(ctx, `SELECT login_name FROM aveloxis_ops.users WHERE user_id = $1`, userID).Scan(&login)
	return login, err
}
