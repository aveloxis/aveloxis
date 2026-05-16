// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// CntrbIDMigrationCounts is the result of a candidate-set count.
// total = all migration candidates; safe = no collision (target
// deterministic cntrb_id doesn't already exist on a different row);
// collisions = candidates whose target is taken (rename-merge case).
type CntrbIDMigrationCounts struct {
	Total      int
	Safe       int
	Collisions int
}

// candidateSetSQL is the load-bearing CTE shared by the count, the
// dry-run sample, and the live UPDATE. PlatformUUID layout per
// internal/db/github_uuid.go for userID <= uint32 max:
//
//	byte 0: platform_id
//	bytes 1-4: big-endian uint32 of userID
//	bytes 5-15: zero
//
// SQL construction: hex-pack as 32-char string, cast to uuid.
const candidateSetSQL = `
WITH candidates AS (
  SELECT c.cntrb_id AS old_id,
         c.cntrb_login,
         ci.platform_id,
         ci.platform_user_id,
         (lpad(to_hex(ci.platform_id::int), 2, '0') ||
          lpad(to_hex(ci.platform_user_id::bigint::int), 8, '0') ||
          '0000000000000000000000')::uuid AS new_id
  FROM aveloxis_data.contributors c
  JOIN aveloxis_data.contributor_identities ci ON c.cntrb_id = ci.cntrb_id
  WHERE c.cntrb_login != ''
    AND NOT (c.cntrb_id::text LIKE '01%' AND c.cntrb_id::text LIKE '%000000000000')
    AND ci.platform_user_id > 0
    AND ci.platform_user_id <= 4294967295
    AND COALESCE(c.cntrb_deleted, 0) = 0
)`

// CountCntrbIDMigrationCandidates returns the count breakdown for
// the v0.22.2 cntrb_id migration. No writes. Cheap on a 1.7M-
// contributor production DB (one indexed scan + correlated EXISTS).
func CountCntrbIDMigrationCandidates(ctx context.Context, store *PostgresStore) (CntrbIDMigrationCounts, error) {
	var out CntrbIDMigrationCounts
	err := store.pool.QueryRow(ctx, candidateSetSQL+`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE NOT EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)) AS safe,
			COUNT(*) FILTER (WHERE EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)) AS collision_count
		FROM candidates
	`).Scan(&out.Total, &out.Safe, &out.Collisions)
	return out, err
}

// CntrbIDMigrationCandidate is one row in the dry-run sample.
type CntrbIDMigrationCandidate struct {
	Login          string
	OldID          string
	NewID          string
	PlatformUserID int64
}

// SampleCntrbIDMigrationCandidates returns up to `limit` non-
// collision candidate rows for dry-run display.
func SampleCntrbIDMigrationCandidates(ctx context.Context, store *PostgresStore, limit int) ([]CntrbIDMigrationCandidate, error) {
	rows, err := store.pool.Query(ctx, candidateSetSQL+`
		SELECT cntrb_login, old_id::text, new_id::text, platform_user_id
		FROM candidates
		WHERE NOT EXISTS (
			SELECT 1 FROM aveloxis_data.contributors c2
			WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
		)
		ORDER BY cntrb_login
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CntrbIDMigrationCandidate
	for rows.Next() {
		var c CntrbIDMigrationCandidate
		if err := rows.Scan(&c.Login, &c.OldID, &c.NewID, &c.PlatformUserID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// MigrateCntrbIDsBatch runs one batched UPDATE of up to batchSize
// non-collision candidate rows, swapping random cntrb_id values to
// deterministic PlatformUUID form. ON UPDATE CASCADE (v0.22.1)
// propagates the change through every child FK column atomically
// in the same transaction.
//
// Returns the number of contributor rows updated this batch. Zero
// means there are no more eligible candidates (either drained, or
// only collisions remain).
func MigrateCntrbIDsBatch(ctx context.Context, store *PostgresStore, batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("batchSize must be > 0")
	}
	tag, err := store.pool.Exec(ctx, candidateSetSQL+`,
		batch AS (
			SELECT old_id, new_id
			FROM candidates
			WHERE NOT EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)
			LIMIT $1
		)
		UPDATE aveloxis_data.contributors c
		SET cntrb_id = b.new_id,
		    data_collection_date = NOW()
		FROM batch b
		WHERE c.cntrb_id = b.old_id`, batchSize)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// PrecheckCntrbIDCascade verifies every cntrb_id FK has ON UPDATE
// CASCADE. Returns the names of any constraints missing it. The
// v0.22.2 migration refuses to run when this list is non-empty
// (override available via --skip-precheck).
func PrecheckCntrbIDCascade(ctx context.Context, store *PostgresStore, logger *slog.Logger) ([]string, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT tc.constraint_name, rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.referential_constraints rc
		  ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu
		  ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND ccu.table_schema = 'aveloxis_data'
		  AND ccu.table_name = 'contributors'
		  AND ccu.column_name = 'cntrb_id'
	`)
	if err != nil {
		return nil, fmt.Errorf("query referential_constraints: %w", err)
	}
	defer rows.Close()
	var missing []string
	for rows.Next() {
		var name, rule string
		if err := rows.Scan(&name, &rule); err != nil {
			return nil, err
		}
		if rule != "CASCADE" {
			missing = append(missing, fmt.Sprintf("%s (update_rule=%s)", name, rule))
		}
	}
	return missing, rows.Err()
}
