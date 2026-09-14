// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/platform"
)

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

// StoredAPIKey is one aveloxis_ops.worker_oauth row as key loading sees it.
// InstanceURL tags a GitLab token with the normalized web base of the
// instance that issued it; "" is the main instance.
type StoredAPIKey struct {
	OAuthID     int64
	Token       string
	InstanceURL string
}

// LoadStoredAPIKeys loads a platform's stored tokens from
// aveloxis_ops.worker_oauth in oauth_id order. A read error is returned,
// never an empty result (SR-5): since v0.30.0 Phase C a running process
// reconciles its pools against this read, and an empty answer would remove
// every stored key.
func LoadStoredAPIKeys(ctx context.Context, pool *pgxpool.Pool, platform string) ([]StoredAPIKey, error) {
	rows, err := pool.Query(ctx, `
		SELECT oauth_id, access_token, instance_url FROM aveloxis_ops.worker_oauth
		WHERE platform = $1 AND access_token != ''
		ORDER BY oauth_id`, platform)
	if err != nil {
		return nil, fmt.Errorf("load %s keys: %w", platform, err)
	}
	defer rows.Close()
	var out []StoredAPIKey
	for rows.Next() {
		var k StoredAPIKey
		if err := rows.Scan(&k.OAuthID, &k.Token, &k.InstanceURL); err != nil {
			return nil, fmt.Errorf("load %s keys: %w", platform, err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load %s keys: %w", platform, err)
	}
	return out, nil
}

// LoadAugurAPIKeys loads a platform's tokens from Augur's
// augur_operations.worker_oauth. A database without Augur's tables is not an
// error (nothing to load: nil, nil); any other read error is returned.
// Augur has no GitLab instances — its GitLab keys are gitlab.com keys.
func LoadAugurAPIKeys(ctx context.Context, pool *pgxpool.Pool, platform string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT access_token FROM augur_operations.worker_oauth
		WHERE platform = $1 AND access_token != ''
		ORDER BY oauth_id`, platform)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000") {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load %s keys from augur_operations.worker_oauth: %w", platform, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			return nil, fmt.Errorf("load %s keys from augur_operations.worker_oauth: %w", platform, err)
		}
		keys = append(keys, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load %s keys from augur_operations.worker_oauth: %w", platform, err)
	}
	return keys, nil
}

// ── v0.30.0 Phase C: API-key administration ─────────────────────────────
//
// The admin API (aveloxis api) stores tokens and lists or removes them by
// oauth_id, but never reads a token back: every admin read returns key_id
// and key_mask computed in SQL. Both expressions are built from the
// platform package's constants, and TestAdminAPIKeysInsertListDelete pins
// them byte-equal to platform.KeyID and platform.MaskToken (SR-17), so a
// stored row and the key a running process reports match by key_id. The
// mask compares octet_length like Go's len; the admin API accepts only
// printable ASCII tokens, where left/right characters are bytes.

var (
	keyIDSQL   = fmt.Sprintf(`substr(encode(sha256(convert_to('%s' || access_token, 'UTF8')), 'hex'), 1, 16)`, platform.KeyIDDomain)
	keyMaskSQL = fmt.Sprintf(`CASE WHEN octet_length(access_token) < %d THEN '(hidden)' ELSE left(access_token, 4) || '...' || right(access_token, 4) END`, platform.MaskMinLen)
)

// AdminAPIKey is a stored key as the admin API sees it — no token.
type AdminAPIKey struct {
	OAuthID     int64     `json:"oauth_id"`
	Name        string    `json:"name"`
	Platform    string    `json:"platform"`
	InstanceURL string    `json:"instance_url"`
	KeyID       string    `json:"key_id"`
	KeyMask     string    `json:"key_mask"`
	CreatedAt   time.Time `json:"created_at"`
}

// ErrAPIKeyExists is InsertAPIKey finding the token already stored for the
// platform. The concrete error is *APIKeyExistsError.
var ErrAPIKeyExists = errors.New("API key already stored")

// ErrAPIKeyNotFound is DeleteAPIKey finding no row with that oauth_id.
var ErrAPIKeyNotFound = errors.New("API key not found")

// APIKeyExistsError names where an already-stored token lives.
type APIKeyExistsError struct {
	OAuthID     int64
	InstanceURL string
}

func (e *APIKeyExistsError) Error() string {
	if e.InstanceURL == "" {
		return fmt.Sprintf("%v (oauth_id %d, main instance)", ErrAPIKeyExists, e.OAuthID)
	}
	return fmt.Sprintf("%v (oauth_id %d, instance %s)", ErrAPIKeyExists, e.OAuthID, e.InstanceURL)
}

func (e *APIKeyExistsError) Unwrap() error { return ErrAPIKeyExists }

// InsertAPIKey stores a new token for platform under instanceURL and returns
// its oauth_id. Unlike SaveAPIKey (the CLI's upsert, which moves a re-added
// token) it never changes an existing row: a token already stored for the
// platform returns *APIKeyExistsError naming its row and instance — the
// admin page's "remove it first".
func (s *PostgresStore) InsertAPIKey(ctx context.Context, name, token, platformName, instanceURL string) (int64, error) {
	pool := s.pool
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform, instance_url)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (access_token, platform) DO NOTHING
		RETURNING oauth_id`, name, token, platformName, instanceURL).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("store %s key: %w", platformName, err)
	}
	exists := &APIKeyExistsError{}
	if err := pool.QueryRow(ctx, `
		SELECT oauth_id, instance_url FROM aveloxis_ops.worker_oauth
		WHERE access_token = $1 AND platform = $2`, token, platformName).Scan(&exists.OAuthID, &exists.InstanceURL); err != nil {
		return 0, fmt.Errorf("store %s key: the token is already stored, and reading its row failed: %w", platformName, err)
	}
	return 0, exists
}

// ListAdminAPIKeys lists every stored key (both platforms) without tokens,
// in oauth_id order.
func (s *PostgresStore) ListAdminAPIKeys(ctx context.Context) ([]AdminAPIKey, error) {
	pool := s.pool
	rows, err := pool.Query(ctx, `
		SELECT oauth_id, name, platform, instance_url, `+keyIDSQL+`, `+keyMaskSQL+`, COALESCE(created_at, 'epoch'::timestamptz)
		FROM aveloxis_ops.worker_oauth
		WHERE access_token != ''
		ORDER BY oauth_id`)
	if err != nil {
		return nil, fmt.Errorf("list API keys: %w", err)
	}
	defer rows.Close()
	var out []AdminAPIKey
	for rows.Next() {
		var k AdminAPIKey
		if err := rows.Scan(&k.OAuthID, &k.Name, &k.Platform, &k.InstanceURL, &k.KeyID, &k.KeyMask, &k.CreatedAt); err != nil {
			return nil, fmt.Errorf("list API keys: %w", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list API keys: %w", err)
	}
	return out, nil
}

// DeleteAPIKey removes the stored key oauthID and returns what it was (no
// token). A missing row is ErrAPIKeyNotFound.
func (s *PostgresStore) DeleteAPIKey(ctx context.Context, oauthID int64) (AdminAPIKey, error) {
	pool := s.pool
	var k AdminAPIKey
	err := pool.QueryRow(ctx, `
		DELETE FROM aveloxis_ops.worker_oauth WHERE oauth_id = $1
		RETURNING oauth_id, name, platform, instance_url, `+keyIDSQL+`, `+keyMaskSQL+`, COALESCE(created_at, 'epoch'::timestamptz)`,
		oauthID).Scan(&k.OAuthID, &k.Name, &k.Platform, &k.InstanceURL, &k.KeyID, &k.KeyMask, &k.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdminAPIKey{}, ErrAPIKeyNotFound
	}
	if err != nil {
		return AdminAPIKey{}, fmt.Errorf("delete API key %d: %w", oauthID, err)
	}
	return k, nil
}

// ForgeKeyReportRow is one running process's latest key report. AgeSeconds
// is judged by the DATABASE clock (now() - reported_at), so a reader on
// another host never compares against its own clock.
type ForgeKeyReportRow struct {
	Reporter   string
	ReportedAt time.Time
	AgeSeconds float64
	Report     []byte
}

// SaveForgeKeyReport upserts reporter's key report with reported_at = now()
// and, in the same statement, drops reports from OTHER reporters that are
// more than a day old (a process that stopped reporting — its host gone or
// its boot id replaced by a restart).
func (s *PostgresStore) SaveForgeKeyReport(ctx context.Context, reporter string, report []byte) error {
	_, err := s.pool.Exec(ctx, `
		WITH stale AS (
			DELETE FROM aveloxis_ops.forge_key_reports
			WHERE reporter <> $1 AND reported_at < now() - interval '1 day'
		)
		INSERT INTO aveloxis_ops.forge_key_reports (reporter, reported_at, report)
		VALUES ($1, now(), $2)
		ON CONFLICT (reporter) DO UPDATE SET reported_at = EXCLUDED.reported_at, report = EXCLUDED.report`,
		reporter, report)
	if err != nil {
		return fmt.Errorf("save key report: %w", err)
	}
	return nil
}

// LoadForgeKeyReports returns every stored key report, newest first.
func (s *PostgresStore) LoadForgeKeyReports(ctx context.Context) ([]ForgeKeyReportRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT reporter, reported_at, EXTRACT(EPOCH FROM now() - reported_at)::float8, report
		FROM aveloxis_ops.forge_key_reports
		ORDER BY reported_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("load key reports: %w", err)
	}
	defer rows.Close()
	var out []ForgeKeyReportRow
	for rows.Next() {
		var r ForgeKeyReportRow
		if err := rows.Scan(&r.Reporter, &r.ReportedAt, &r.AgeSeconds, &r.Report); err != nil {
			return nil, fmt.Errorf("load key reports: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load key reports: %w", err)
	}
	return out, nil
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
