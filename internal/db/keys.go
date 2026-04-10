package db

import (
	"context"
	"fmt"

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
func SaveAPIKey(ctx context.Context, pool *pgxpool.Pool, name, token, platform string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform)
		VALUES ($1, $2, $3)
		ON CONFLICT (access_token, platform) DO UPDATE SET
			name = EXCLUDED.name`,
		name, token, platform)
	return err
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
