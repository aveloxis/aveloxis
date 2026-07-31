// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Collections (v0.27.63) are admin-curated groups-of-groups for the
// GUI home page: "Apache Graduated", "CNCF Sandbox", … Each collection
// joins to LIVE user_groups through collection_groups — never a frozen
// repo list — so admin org-groups that auto-grow via org scans keep
// their collections fresh for free.
//
// Two invariants enforced here:
//   - Only ADMIN-owned groups may join a collection (link-time check):
//     collections are fleet-curation surface, and linking an arbitrary
//     user's group would let that user mutate what everyone sees.
//   - Copying a collection into a user's group NEVER enqueues
//     collection and never creates add-requests (v0.27.20 interplay):
//     collection repos are already tracked, so the copy is a pure
//     user_repos INSERT…SELECT — approval only ever gates NEW
//     collection. Pinned by a comment-stripped negative test.

// ErrGroupNotAdminOwned is returned by AddGroupToCollection when the
// candidate member group is not owned by an admin user.
var ErrGroupNotAdminOwned = errors.New("collection member groups must be admin-owned")

// ErrNotGroupOwner is returned by CopyCollectionToGroup when the
// target group does not belong to the calling user.
var ErrNotGroupOwner = errors.New("target group is not owned by the calling user")

// CollectionSummary is one row in the collections list: the
// collection plus its live group/repo cardinality.
type CollectionSummary struct {
	CollectionID int64     `json:"collection_id"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	Position     int       `json:"position"`
	GroupCount   int       `json:"groups"`
	RepoCount    int       `json:"repos"`
	CreatedAt    time.Time `json:"created_at"`
}

// CollectionGroup is one member group in a collection detail view.
type CollectionGroup struct {
	GroupID   int64  `json:"group_id"`
	Name      string `json:"name"`
	RepoCount int    `json:"repos"`
}

// CollectionRepo is one deduped repo row in a collection detail view.
type CollectionRepo struct {
	RepoID int64  `json:"repo_id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	GitURL string `json:"git_url"`
}

// CreateCollection inserts a new collection. Name is UNIQUE — a
// duplicate surfaces as the constraint error for the handler to map.
func (s *PostgresStore) CreateCollection(ctx context.Context, name, description string, position int, createdBy int) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collections (name, description, position, created_by)
		VALUES ($1, $2, $3, $4) RETURNING collection_id`,
		name, description, position, createdBy).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("CreateCollection: %w", err)
	}
	return id, nil
}

// UpdateCollection updates name/description/position in place.
func (s *PostgresStore) UpdateCollection(ctx context.Context, collectionID int64, name, description string, position int) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collections
		SET name = $2, description = $3, position = $4
		WHERE collection_id = $1`, collectionID, name, description, position)
	if err != nil {
		return fmt.Errorf("UpdateCollection: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("UpdateCollection: collection %d not found", collectionID)
	}
	return nil
}

// DeleteCollection removes the collection; collection_groups rows
// cascade (ON DELETE CASCADE). Member user_groups are untouched.
func (s *PostgresStore) DeleteCollection(ctx context.Context, collectionID int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collections WHERE collection_id = $1`, collectionID)
	if err != nil {
		return fmt.Errorf("DeleteCollection: %w", err)
	}
	return nil
}

// AddGroupToCollection links an ADMIN-OWNED group into the
// collection. The owner check happens in the same statement as the
// insert (INSERT…SELECT gated on u.admin) so there is no
// check-then-act race; zero rows inserted with a non-admin owner
// surfaces ErrGroupNotAdminOwned.
func (s *PostgresStore) AddGroupToCollection(ctx context.Context, collectionID, groupID int64) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_groups (collection_id, group_id)
		SELECT $1, g.group_id
		FROM aveloxis_ops.user_groups g
		JOIN aveloxis_ops.users u ON u.user_id = g.user_id
		WHERE g.group_id = $2 AND u.admin
		ON CONFLICT DO NOTHING`, collectionID, groupID)
	if err != nil {
		return fmt.Errorf("AddGroupToCollection: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Either the group isn't admin-owned or the link already
		// exists. Disambiguate for a useful error.
		var isAdminOwned bool
		if err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(u.admin, FALSE)
			FROM aveloxis_ops.user_groups g
			JOIN aveloxis_ops.users u ON u.user_id = g.user_id
			WHERE g.group_id = $1`, groupID).Scan(&isAdminOwned); err != nil {
			return fmt.Errorf("AddGroupToCollection: group %d not found", groupID)
		}
		if !isAdminOwned {
			return ErrGroupNotAdminOwned
		}
		// Already linked — idempotent success.
	}
	return nil
}

// RemoveGroupFromCollection unlinks a member group. The group itself
// (and its repos) are untouched.
func (s *PostgresStore) RemoveGroupFromCollection(ctx context.Context, collectionID, groupID int64) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.collection_groups
		WHERE collection_id = $1 AND group_id = $2`, collectionID, groupID)
	if err != nil {
		return fmt.Errorf("RemoveGroupFromCollection: %w", err)
	}
	return nil
}

// ListCollections returns every collection ordered by position (then
// name), each with its live group count and DEDUPED repo count.
func (s *PostgresStore) ListCollections(ctx context.Context) ([]CollectionSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.collection_id, c.name, c.description, c.position, c.created_at,
		       COUNT(DISTINCT cg.group_id)::int  AS groups,
		       COUNT(DISTINCT ur.repo_id)::int   AS repos
		FROM aveloxis_ops.collections c
		LEFT JOIN aveloxis_ops.collection_groups cg USING (collection_id)
		LEFT JOIN aveloxis_ops.user_repos ur ON ur.group_id = cg.group_id
		GROUP BY c.collection_id
		ORDER BY c.position, c.name`)
	if err != nil {
		return nil, fmt.Errorf("ListCollections: %w", err)
	}
	defer rows.Close()

	out := make([]CollectionSummary, 0, 16)
	for rows.Next() {
		var cs CollectionSummary
		if err := rows.Scan(&cs.CollectionID, &cs.Name, &cs.Description, &cs.Position, &cs.CreatedAt,
			&cs.GroupCount, &cs.RepoCount); err != nil {
			return nil, fmt.Errorf("ListCollections scan: %w", err)
		}
		out = append(out, cs)
	}
	return out, rows.Err()
}

// GetCollectionGroups returns the member groups of a collection in
// link position order, each with its live repo count.
func (s *PostgresStore) GetCollectionGroups(ctx context.Context, collectionID int64) ([]CollectionGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT g.group_id, g.name, COUNT(ur.repo_id)::int AS repos
		FROM aveloxis_ops.collection_groups cg
		JOIN aveloxis_ops.user_groups g USING (group_id)
		LEFT JOIN aveloxis_ops.user_repos ur ON ur.group_id = g.group_id
		WHERE cg.collection_id = $1
		GROUP BY g.group_id, g.name, cg.position
		ORDER BY cg.position, g.name`, collectionID)
	if err != nil {
		return nil, fmt.Errorf("GetCollectionGroups: %w", err)
	}
	defer rows.Close()

	out := make([]CollectionGroup, 0, 8)
	for rows.Next() {
		var g CollectionGroup
		if err := rows.Scan(&g.GroupID, &g.Name, &g.RepoCount); err != nil {
			return nil, fmt.Errorf("GetCollectionGroups scan: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GetCollectionRepos returns one page of the collection's DEDUPED
// repo set (a repo in two member groups appears once), ordered by
// owner/name with repo_id as the stable tiebreaker (the v0.18.7
// pagination lesson), plus the total deduped count.
func (s *PostgresStore) GetCollectionRepos(ctx context.Context, collectionID int64, page, pageSize int) ([]CollectionRepo, int, error) {
	if page < 1 {
		page = 1
	}
	// Two single-comparison clamps rather than one compound
	// condition (CodeQL go/uncontrolled-allocation-size,
	// 2026-07-31): semantically identical, but the bare
	// `pageSize > 100` guard is the barrier shape the scanner's
	// upper-bound analysis recognizes — the OR-combined
	// reassignment was flagged as an uncontrolled allocation size
	// even though it bounded the value.
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 100 {
		pageSize = 100
	}

	var total int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT ur.repo_id)
		FROM aveloxis_ops.collection_groups cg
		JOIN aveloxis_ops.user_repos ur USING (group_id)
		WHERE cg.collection_id = $1`, collectionID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("GetCollectionRepos count: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT r.repo_id, r.repo_owner, r.repo_name, r.repo_git
		FROM aveloxis_ops.collection_groups cg
		JOIN aveloxis_ops.user_repos ur USING (group_id)
		JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
		WHERE cg.collection_id = $1
		ORDER BY r.repo_owner, r.repo_name, r.repo_id
		LIMIT $2 OFFSET $3`, collectionID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("GetCollectionRepos: %w", err)
	}
	defer rows.Close()

	out := make([]CollectionRepo, 0, pageSize)
	for rows.Next() {
		var cr CollectionRepo
		if err := rows.Scan(&cr.RepoID, &cr.Owner, &cr.Name, &cr.GitURL); err != nil {
			return nil, 0, fmt.Errorf("GetCollectionRepos scan: %w", err)
		}
		out = append(out, cr)
	}
	return out, total, rows.Err()
}

// CopyCollectionToGroup links every repo of the collection into
// targetGroupID, which must be owned by userID. Returns the number of
// NEW links (repos already in the group no-op via the user_repos PK).
//
// Deliberately a pure user_repos INSERT…SELECT: collection repos are
// already tracked, so no queue rows and no add-requests are created —
// approval gates NEW collection, never access to collected data
// (v0.27.20 / v0.27.4 scope semantics).
func (s *PostgresStore) CopyCollectionToGroup(ctx context.Context, collectionID int64, userID int, targetGroupID int64) (int64, error) {
	if err := s.verifyGroupOwned(ctx, userID, targetGroupID); err != nil {
		return 0, ErrNotGroupOwner
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
		SELECT DISTINCT $2::bigint, ur.repo_id
		FROM aveloxis_ops.collection_groups cg
		JOIN aveloxis_ops.user_repos ur USING (group_id)
		WHERE cg.collection_id = $1
		ON CONFLICT DO NOTHING`, collectionID, targetGroupID)
	if err != nil {
		return 0, fmt.Errorf("CopyCollectionToGroup: %w", err)
	}
	return tag.RowsAffected(), nil
}
