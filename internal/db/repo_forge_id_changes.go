// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"
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
	// Listing fields (ListForgeIDChanges): the repository as stored.
	RepoGit string `json:"repo_git,omitempty"`
}

// ErrForgeIDNotAsExpected — AdoptForgeID's guard: the stored forge ID is
// not the old ID the operator adopted over (someone else changed it, or
// the wrong repository was named). Nothing is written.
var ErrForgeIDNotAsExpected = errors.New("stored forge ID is not the expected old ID")

// recordForgeIDObservation writes what the org scan saw: stored ≠
// observed. A repeat observation only moves last_observed_at. It never
// touches repos (the observation-only rule, SR-7).
func (s *PostgresStore) recordForgeIDObservation(ctx context.Context, repoID int64, stored, observed string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_forge_id_changes (repo_id, old_forge_id, new_forge_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (repo_id, old_forge_id, new_forge_id) DO UPDATE SET last_observed_at = NOW()`,
		repoID, stored, observed)
	return err
}

// AdoptForgeID is the operator's approval of a forge-ID change: in one
// transaction it moves repos.platform_repo_id from oldID to newID — only
// if it still holds oldID (ErrForgeIDNotAsExpected otherwise) — and
// records the change as adopted, creating the record when the scan never
// observed it. forgeCreatedAt (nil = unknown) is the forge's creation date
// of the new repository, the date the page shows.
func (s *PostgresStore) AdoptForgeID(ctx context.Context, repoID int64, oldID, newID string, forgeCreatedAt *time.Time, adoptedBy, note string) error {
	if oldID == "" || newID == "" || oldID == newID {
		return fmt.Errorf("adopt forge ID for repo %d: old %q and new %q must be two different IDs", repoID, oldID, newID)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit
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
			(repo_id, old_forge_id, new_forge_id, forge_created_at, adopted_at, adopted_by, note)
		VALUES ($1, $2, $3, $4, NOW(), $5, $6)
		ON CONFLICT (repo_id, old_forge_id, new_forge_id) DO UPDATE SET
			forge_created_at = COALESCE(EXCLUDED.forge_created_at, repo_forge_id_changes.forge_created_at),
			adopted_at = NOW(), adopted_by = EXCLUDED.adopted_by, note = EXCLUDED.note`,
		repoID, oldID, newID, forgeCreatedAt, adoptedBy, note); err != nil {
		return fmt.Errorf("record adopted forge ID for repo %d: %w", repoID, err)
	}
	return tx.Commit(ctx)
}

// ListForgeIDChanges lists the recorded changes, pending only (observed,
// not adopted) or all — pending first, then newest observation first —
// with each repository's stored URL.
func (s *PostgresStore) ListForgeIDChanges(ctx context.Context, pendingOnly bool) ([]ForgeIDChange, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.repo_id, c.old_forge_id, c.new_forge_id, c.first_observed_at, c.last_observed_at,
		       c.forge_created_at, c.adopted_at, c.adopted_by, c.note, COALESCE(r.repo_git, '')
		  FROM aveloxis_data.repo_forge_id_changes c
		  JOIN aveloxis_data.repos r USING (repo_id)
		 WHERE NOT $1 OR c.adopted_at IS NULL
		 ORDER BY (c.adopted_at IS NOT NULL), c.last_observed_at DESC, c.change_id DESC`, pendingOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForgeIDChange
	for rows.Next() {
		var c ForgeIDChange
		if err := rows.Scan(&c.RepoID, &c.OldForgeID, &c.NewForgeID, &c.FirstObservedAt, &c.LastObservedAt,
			&c.ForgeCreatedAt, &c.AdoptedAt, &c.AdoptedBy, &c.Note, &c.RepoGit); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// repoForgeIDChangesAdopted returns one repository's ADOPTED changes,
// oldest first — what the repository page shows.
func (s *PostgresStore) repoForgeIDChangesAdopted(ctx context.Context, repoID int64) ([]ForgeIDChange, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, old_forge_id, new_forge_id, first_observed_at, last_observed_at,
		       forge_created_at, adopted_at, adopted_by, note
		  FROM aveloxis_data.repo_forge_id_changes
		 WHERE repo_id = $1 AND adopted_at IS NOT NULL
		 ORDER BY adopted_at, change_id`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForgeIDChange
	for rows.Next() {
		var c ForgeIDChange
		if err := rows.Scan(&c.RepoID, &c.OldForgeID, &c.NewForgeID, &c.FirstObservedAt, &c.LastObservedAt,
			&c.ForgeCreatedAt, &c.AdoptedAt, &c.AdoptedBy, &c.Note); err != nil {
			return nil, err
		}
		out = append(out, c)
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
