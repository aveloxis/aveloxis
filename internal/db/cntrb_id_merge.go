// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// CntrbIDCollision is one rename-merge case left over from the
// v0.22.2 cntrb_id migration: a contributor with a random cntrb_id
// (the "loser") whose target deterministic PlatformUUID is already
// owned by a different contributor row (the "winner"), typically
// because the GitHub user renamed between the two observations.
type CntrbIDCollision struct {
	LoserID        string
	LoserLogin     string
	WinnerID       string
	WinnerLogin    string
	PlatformID     int16
	PlatformUserID int64
}

// collisionsSQL is the candidate-pair query shared by the count
// and the merge loop. Same exclusion semantics as the v0.22.2
// migration's candidateSetSQL, but selects the PAIRED case
// (winner_id already on a different row) instead of the safe-to-
// migrate case.
const collisionsSQL = `
WITH candidates AS (
  SELECT c.cntrb_id AS loser_id,
         c.cntrb_login AS loser_login,
         ci.platform_id,
         ci.platform_user_id,
         (lpad(to_hex(ci.platform_id::int), 2, '0') ||
          lpad(to_hex(ci.platform_user_id::bigint::int), 8, '0') ||
          '0000000000000000000000')::uuid AS winner_id
  FROM aveloxis_data.contributors c
  JOIN aveloxis_data.contributor_identities ci ON c.cntrb_id = ci.cntrb_id
  WHERE c.cntrb_login != ''
    AND NOT (c.cntrb_id::text LIKE '01%' AND c.cntrb_id::text LIKE '%000000000000')
    AND ci.platform_user_id > 0
    AND ci.platform_user_id <= 4294967295
    AND COALESCE(c.cntrb_deleted, 0) = 0
)`

// CountCntrbIDCollisions returns the number of unresolved
// collision pairs. A pair is "unresolved" when both rows exist
// and the loser is not yet cntrb_deleted.
func CountCntrbIDCollisions(ctx context.Context, store *PostgresStore) (int, error) {
	var n int
	err := store.pool.QueryRow(ctx, collisionsSQL+`
		SELECT COUNT(*)
		FROM candidates
		WHERE EXISTS (
			SELECT 1 FROM aveloxis_data.contributors c2
			WHERE c2.cntrb_id = candidates.winner_id
			  AND c2.cntrb_id <> candidates.loser_id
			  AND COALESCE(c2.cntrb_deleted, 0) = 0
		)
	`).Scan(&n)
	return n, err
}

// SampleCntrbIDCollisions returns up to limit pairs for dry-run
// display. Joins back to aveloxis_data.contributors so the caller
// gets both logins in one query.
func SampleCntrbIDCollisions(ctx context.Context, store *PostgresStore, limit int) ([]CntrbIDCollision, error) {
	rows, err := store.pool.Query(ctx, collisionsSQL+`
		SELECT candidates.loser_id::text,
		       candidates.loser_login,
		       candidates.winner_id::text,
		       w.cntrb_login,
		       candidates.platform_id,
		       candidates.platform_user_id
		FROM candidates
		JOIN aveloxis_data.contributors w ON w.cntrb_id = candidates.winner_id
		WHERE w.cntrb_id <> candidates.loser_id
		  AND COALESCE(w.cntrb_deleted, 0) = 0
		ORDER BY candidates.loser_login
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CntrbIDCollision
	for rows.Next() {
		var c CntrbIDCollision
		if err := rows.Scan(&c.LoserID, &c.LoserLogin, &c.WinnerID, &c.WinnerLogin, &c.PlatformID, &c.PlatformUserID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// MergeCntrbIDCollisionsBatch consolidates up to batchSize
// rename-merge collisions. For each pair (loser=random-UUID row,
// winner=deterministic-UUID row), runs the v0.20.2 phase D soft-
// merge pattern in one transaction:
//
//  1. Move every contributor_identities row from loser to winner.
//     Production audit showed zero collisions where winner already
//     has the same (platform_id, platform_user_id) identity as
//     loser, so a plain UPDATE moves cleanly. If a future
//     production state introduces such conflicts, the UPDATE
//     errors with SQLSTATE 23505 and that batch rolls back —
//     operator gets a clear error pointing at the colliding pair.
//
//  2. Merge non-empty fields from loser into winner via
//     COALESCE(NULLIF(winner.field, ""), loser.field). Winner
//     values are preserved when present.
//
//  3. Insert a contributors_aliases row mapping loser.cntrb_email
//     to winner so commit-email resolution still routes to the
//     active contributor.
//
//  4. Mark loser cntrb_deleted = 1. The loser row stays in place
//     — every child FK referencing loser.cntrb_id keeps working;
//     read-path queries filter on COALESCE(cntrb_deleted, 0) = 0.
//
// Returns the number of pairs merged. Zero means the loop has
// drained — re-running is idempotent.
func MergeCntrbIDCollisionsBatch(ctx context.Context, store *PostgresStore, batchSize int) (int, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("batchSize must be > 0")
	}
	rows, err := SampleCntrbIDCollisions(ctx, store, batchSize)
	if err != nil {
		return 0, fmt.Errorf("sample collisions: %w", err)
	}

	merged := 0
	for _, pair := range rows {
		if err := mergeOnePair(ctx, store, pair); err != nil {
			return merged, fmt.Errorf("merge pair (loser=%s, winner=%s): %w",
				pair.LoserLogin, pair.WinnerLogin, err)
		}
		merged++
	}
	return merged, nil
}

func mergeOnePair(ctx context.Context, store *PostgresStore, pair CntrbIDCollision) error {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Step 1: move loser's identity rows to winner. Production
	// audit guarantees no UNIQUE conflict, but the UPDATE will
	// fail loudly with SQLSTATE 23505 if that assumption ever
	// breaks — the transaction rolls back, operator sees the
	// bad pair.
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.contributor_identities
		SET cntrb_id = $1::uuid
		WHERE cntrb_id = $2::uuid
	`, pair.WinnerID, pair.LoserID); err != nil {
		return fmt.Errorf("move identities: %w", err)
	}

	// Step 2: merge non-empty fields from loser into winner with
	// prefer-existing semantics. Mirrors v0.20.2's pickMergeWinner
	// COALESCE pattern.
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.contributors w
		SET cntrb_email = COALESCE(NULLIF(w.cntrb_email, ''), l.cntrb_email),
		    cntrb_canonical = COALESCE(NULLIF(w.cntrb_canonical, ''), l.cntrb_canonical),
		    cntrb_company = COALESCE(NULLIF(w.cntrb_company, ''), l.cntrb_company),
		    cntrb_location = COALESCE(NULLIF(w.cntrb_location, ''), l.cntrb_location),
		    cntrb_full_name = COALESCE(NULLIF(w.cntrb_full_name, ''), l.cntrb_full_name),
		    data_collection_date = NOW()
		FROM aveloxis_data.contributors l
		WHERE w.cntrb_id = $1::uuid AND l.cntrb_id = $2::uuid
	`, pair.WinnerID, pair.LoserID); err != nil {
		return fmt.Errorf("merge fields: %w", err)
	}

	// Step 3: insert loser's email as an alias pointing at winner.
	// Skip when loser has no email (no alias is meaningful in
	// that case). ON CONFLICT skip — the email may already map to
	// winner from a prior merge or via a normal contributor flow.
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		SELECT $1::uuid,
		       COALESCE(NULLIF(w.cntrb_canonical, ''), w.cntrb_email),
		       l.cntrb_email
		FROM aveloxis_data.contributors w, aveloxis_data.contributors l
		WHERE w.cntrb_id = $1::uuid AND l.cntrb_id = $2::uuid
		  AND l.cntrb_email IS NOT NULL AND l.cntrb_email <> ''
		ON CONFLICT (alias_email) DO NOTHING
	`, pair.WinnerID, pair.LoserID); err != nil {
		return fmt.Errorf("insert alias: %w", err)
	}

	// Step 4: soft-delete the loser. Row stays in place; every FK
	// pointing at loser.cntrb_id remains valid; read-path queries
	// filter via COALESCE(cntrb_deleted, 0) = 0.
	if _, err := tx.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_deleted = 1, data_collection_date = NOW()
		WHERE cntrb_id = $1::uuid
	`, pair.LoserID); err != nil {
		return fmt.Errorf("soft-delete loser: %w", err)
	}

	return tx.Commit(ctx)
}
