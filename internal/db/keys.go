// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadAPIKeys loads API tokens for the given platform. It reads from
// aveloxis_ops.worker_oauth first, then optionally falls back to
// augur_operations.worker_oauth if fallbackToAugur is true.
//
// The platform parameter should be "github" or "gitlab".
func LoadAPIKeys(ctx context.Context, pool *pgxpool.Pool, platform string, fallbackToAugur bool) ([]string, error) {
	keys, err := loadKeysFromTable(ctx, pool, "aveloxis_ops.worker_oauth", platform)
	if err != nil {
		// Table might not exist yet (pre-migration); not fatal.
		keys = nil
	}

	if len(keys) == 0 && fallbackToAugur {
		augurKeys, err := loadKeysFromTable(ctx, pool, "augur_operations.worker_oauth", platform)
		if err != nil {
			return keys, nil // Augur table might not exist; not fatal.
		}
		keys = append(keys, augurKeys...)
	}

	return keys, nil
}

// SaveAPIKey stores an API token in aveloxis_ops.worker_oauth.
//
// instanceURL tags a GitLab token with the normalized web base of the ONE
// instance that issued it (v0.30.0, multi-instance GitLab); "" is the main
// instance, which is what every row stored before v0.30.0 means. A token
// keeps one row: re-saving it under another instance moves it. existed
// reports whether the token was already stored and previousInstanceURL the
// tag it had then, so the caller can say it moved.
func SaveAPIKey(ctx context.Context, pool *pgxpool.Pool, name, token, platform, instanceURL string) (previousInstanceURL string, existed bool, err error) {
	var prev *string
	err = pool.QueryRow(ctx, `
		WITH prev AS (
			SELECT instance_url FROM aveloxis_ops.worker_oauth
			WHERE access_token = $2 AND platform = $3
		)
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform, instance_url)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (access_token, platform) DO UPDATE SET
			name = EXCLUDED.name,
			instance_url = EXCLUDED.instance_url
		RETURNING (SELECT instance_url FROM prev)`,
		name, token, platform, instanceURL).Scan(&prev)
	if err != nil {
		return "", false, err
	}
	if prev != nil {
		return *prev, true, nil
	}
	return "", false, nil
}

// LoadAPIKeysByInstance loads a platform's tokens from
// aveloxis_ops.worker_oauth grouped by instance_url ("" = the main
// instance). With fallbackToAugur and no stored tokens at all, Augur's keys
// are added under "" — Augur has no instances, and its GitLab keys are
// gitlab.com keys. A read error is returned, never an empty map (SR-5); an
// absent Augur schema is not an error (there is nothing to fall back to).
func LoadAPIKeysByInstance(ctx context.Context, pool *pgxpool.Pool, platform string, fallbackToAugur bool) (map[string][]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT instance_url, access_token FROM aveloxis_ops.worker_oauth
		WHERE platform = $1 AND access_token != ''
		ORDER BY oauth_id`, platform)
	if err != nil {
		return nil, fmt.Errorf("load %s keys: %w", platform, err)
	}
	defer rows.Close()
	out := map[string][]string{}
	n := 0
	for rows.Next() {
		var inst, token string
		if err := rows.Scan(&inst, &token); err != nil {
			return nil, fmt.Errorf("load %s keys: %w", platform, err)
		}
		out[inst] = append(out[inst], token)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load %s keys: %w", platform, err)
	}
	if n == 0 && fallbackToAugur {
		augurKeys, err := loadKeysFromTable(ctx, pool, "augur_operations.worker_oauth", platform)
		var pgErr *pgconn.PgError
		switch {
		case errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000"):
			// No Augur tables in this database.
		case err != nil:
			return nil, fmt.Errorf("load %s keys from augur_operations.worker_oauth: %w", platform, err)
		default:
			out[""] = append(out[""], augurKeys...)
		}
	}
	return out, nil
}

func loadKeysFromTable(ctx context.Context, pool *pgxpool.Pool, table, platform string) ([]string, error) {
	// Can't parameterize table names, but these are hardcoded internal values.
	query := fmt.Sprintf(`
		SELECT access_token FROM %s
		WHERE platform = $1 AND access_token != ''
		ORDER BY oauth_id`, table)

	rows, err := pool.Query(ctx, query, platform)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			return nil, err
		}
		keys = append(keys, token)
	}
	return keys, rows.Err()
}

// ImportKeysFromAugur copies all keys from augur_operations.worker_oauth into
// aveloxis_ops.worker_oauth. Duplicates (same token+platform) are skipped.
// Imported keys carry instance_url's default "" — the main instance (Augur
// has no GitLab instances; its GitLab keys are gitlab.com keys).
// Returns the number of keys imported.
func ImportKeysFromAugur(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	tag, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform)
		SELECT name, access_token, platform
		FROM augur_operations.worker_oauth
		WHERE access_token != ''
		ON CONFLICT (access_token, platform) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("copying keys from augur_operations.worker_oauth: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Pool exposes the connection pool for key loading and other direct queries.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}
