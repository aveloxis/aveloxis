// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// shared_with_me.go — the v0.27.82 shared-link flow. Operator
// decision (2026-08-04): a signed-in user opening a link to a repo
// outside their groups gets the repo auto-added to an implicit
// "Shared with Me" group — the third leg of the Starred (v0.27.4) /
// Comparisons (v0.27.14) pattern — instead of a dead-end 403.
//
// SAFETY CONTRACT (the v0.27.20 approval principle, tripwired by
// TestSharedWithMeNeverTouchesCollectionMachinery): this flow LINKS
// existing repo rows only. Approval gates collection, never
// visibility — so nothing here may enqueue collection, register org
// tracking, or create add-requests, and this file must never
// reference that machinery.

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// SharedWithMeGroupName is the reserved implicit-group name for the
// shared-link flow (peer of StarredGroupName / ComparisonsGroupName).
const SharedWithMeGroupName = "Shared with Me"

// ErrSharedRepoNotFound marks a share attempt against a repo_id with
// no repos row. Callers fail closed on it (the structured 403 stays).
var ErrSharedRepoNotFound = errors.New("shared repo not found")

// EnsureRepoSharedWithUser links an EXISTING repo into the user's
// "Shared with Me" group, creating the group on first use. Returns
// added=true only when this call created the link — the signal for
// the one-time GUI notice; re-shares and races report false.
//
// Existence is checked BEFORE group creation so a garbage repo id
// can't leave an empty group behind. The link insert is idempotent
// (ON CONFLICT DO NOTHING) because a repo page fires several data
// calls in parallel and every one races through this path; the group
// creation is race-safe via CreateUserGroup's ON CONFLICT ... DO
// UPDATE ... RETURNING. A repo deleted between the existence check
// and the insert surfaces as SQLSTATE 23503 at the statement's
// implicit commit (the user_repos FK is DEFERRABLE, v0.22.7) and maps
// to ErrSharedRepoNotFound.
func (s *PostgresStore) EnsureRepoSharedWithUser(ctx context.Context, userID int, repoID int64) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1)`, repoID).Scan(&exists); err != nil {
		return false, fmt.Errorf("shared-with-me repo check: %w", err)
	}
	if !exists {
		return false, ErrSharedRepoNotFound
	}
	// v0.27.85: fresh any-group membership check. authorizeRepo gates
	// on a scope map cached for up to 60s, so a repo the user JUST
	// gained through another group (the compare page's Comparisons
	// auto-add, a star's Starred auto-add, an org scan landing) still
	// looks out-of-scope here for the cache window — and pre-fix every
	// repo-scoped request in that window minted a spurious
	// Shared-with-Me link (the 2026-08-05 compare double-group report).
	// The DB is the truth: already in ANY of the user's groups means
	// allow without linking (added=false, no notice).
	var inGroup bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_ops.user_groups g USING (group_id)
			WHERE g.user_id = $1 AND ur.repo_id = $2)`, userID, repoID).Scan(&inGroup); err != nil {
		return false, fmt.Errorf("shared-with-me membership check: %w", err)
	}
	if inGroup {
		return false, nil
	}
	groupID, err := s.findOrCreateNamedGroup(ctx, userID, SharedWithMeGroupName)
	if err != nil {
		return false, fmt.Errorf("shared-with-me group: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, groupID, repoID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return false, ErrSharedRepoNotFound
		}
		return false, fmt.Errorf("shared-with-me link: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
