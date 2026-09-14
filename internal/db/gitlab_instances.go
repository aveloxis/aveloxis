// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/model"
)

// GitLab instance registry (v0.30.0, multi-instance GitLab).
//
// Every GitLab instance has its own platform_id, so each identity key that
// includes platform_id — contributor_identities, messages,
// contributor_login_history, the PlatformUUID byte — is scoped to its
// instance without rewriting a row. The id lives on aveloxis_data.platforms
// under the instance's normalized web base (platform_instance_url):
//
//   - 2 is the historical instance. Its web base is stamped once, from the
//     main instance's web base at the first sync (production: gitlab.com —
//     every row already stored under 2 was fetched from it), and never moves
//     to another host or prefix; only its scheme may be re-spelled.
//   - Every other instance gets the next free id in
//     [model.GitLabInstanceIDMin, model.GitLabInstanceIDMax].
//
// Rows are never deleted and ids never reused; config order does not matter
// (the web base is the key); an instance's api_url is config only and never
// stored here. Registering happens only in serve startup and the migrate
// command, after Migrate; everything else reads.

// GitLabRegistryAdvisoryLockID serializes registry syncs (two shards starting
// at once must not hand one id to two instances). An xact lock: no
// CONCURRENTLY DDL runs under it.
const GitLabRegistryAdvisoryLockID int64 = 0x41564C58474C4953 // "AVLXGLIS"

// ErrGitLabInstanceIDsExhausted means no id is free in
// [GitLabInstanceIDMin, GitLabInstanceIDMax] for a new instance.
var ErrGitLabInstanceIDsExhausted = errors.New("no free GitLab instance platform_id")

// GitLabInstanceRef is what the registry needs of a configured instance: its
// normalized web base and whether it is the main instance.
type GitLabInstanceRef struct {
	WebBase string
	Primary bool
}

// ensureGitLabInstanceRegistry adds the registry column, its uniqueness and
// its id-range CHECK. platforms is a five-row reference table, so the index
// is a plain build (SR-2 covers fleet-scale tables); the column is added
// before the index and CHECK that use it (SR-8).
func ensureGitLabInstanceRegistry(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	// Stored GitLab keys name the instance that issued them; '' is the main
	// instance, which every pre-v0.30.0 row is.
	addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_ops.worker_oauth", "instance_url", "TEXT NOT NULL DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_data.platforms", "platform_instance_url", "TEXT")
	execMigrationStep(ctx, pg, logger, errs,
		"v0.30.0 unique GitLab instance web base on platforms",
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_platforms_instance_url
		 ON aveloxis_data.platforms (platform_instance_url)
		 WHERE platform_instance_url IS NOT NULL`)
	execMigrationStep(ctx, pg, logger, errs,
		"v0.30.0 GitLab instance web base only on GitLab ids",
		fmt.Sprintf(`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = 'aveloxis_data.platforms'::regclass
				  AND conname = 'platforms_instance_url_gitlab_ids'
			) THEN
				ALTER TABLE aveloxis_data.platforms
				  ADD CONSTRAINT platforms_instance_url_gitlab_ids
				  CHECK (platform_instance_url IS NULL OR platform_id = %d OR platform_id BETWEEN %d AND %d);
			END IF;
		END $$`, model.PlatformGitLab, model.GitLabInstanceIDMin, model.GitLabInstanceIDMax))
}

// SyncGitLabInstances registers every configured GitLab instance and returns
// web base → platform_id for all of them. See the registry comment above for
// the id rules. An instance past the id range is left unregistered and the
// error wraps ErrGitLabInstanceIDsExhausted; nothing is committed then.
func (s *PostgresStore) SyncGitLabInstances(ctx context.Context, instances []GitLabInstanceRef) (map[string]model.Platform, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("gitlab instance registry: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, GitLabRegistryAdvisoryLockID); err != nil {
		return nil, fmt.Errorf("gitlab instance registry: lock: %w", err)
	}
	registered, row2, err := loadGitLabRegistry(ctx, tx)
	if err != nil {
		return nil, err
	}

	for _, in := range instances {
		if !in.Primary || row2 != "" {
			continue
		}
		if _, taken := registered[in.WebBase]; taken {
			break // the main web base is already another instance: row 2 stays unstamped
		}
		if _, err := tx.Exec(ctx, `
			UPDATE aveloxis_data.platforms SET platform_instance_url = $1
			WHERE platform_id = $2 AND platform_instance_url IS NULL`, in.WebBase, int16(model.PlatformGitLab)); err != nil {
			return nil, fmt.Errorf("gitlab instance registry: stamp platform_id 2 with %s: %w", in.WebBase, err)
		}
		registered[in.WebBase] = model.PlatformGitLab
		row2 = in.WebBase
		s.logger.Info("gitlab instance registry: platform_id 2 is the historical GitLab instance", "web_url", in.WebBase)
	}

	for _, in := range instances {
		if _, ok := registered[in.WebBase]; ok {
			continue
		}
		// http:// and https:// of one web URL are one instance (config
		// refuses both at once): a scheme change keeps the platform_id and
		// the registry takes the configured spelling.
		if old, id, ok := schemeTwin(registered, in.WebBase); ok {
			name := "GitLab (" + in.WebBase + ")"
			if id == model.PlatformGitLab {
				name = "GitLab" // the seeded name of the historical instance stays
			}
			if _, err := tx.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1, platform_name = $2 WHERE platform_id = $3`,
				in.WebBase, name, int16(id)); err != nil {
				return nil, fmt.Errorf("gitlab instance registry: re-spell %s as %s: %w", old, in.WebBase, err)
			}
			delete(registered, old)
			registered[in.WebBase] = id
			if row2 == old {
				row2 = in.WebBase
			}
			s.logger.Info("gitlab instance registry: instance web URL changed scheme — same platform_id", "from", old, "to", in.WebBase, "platform_id", id)
			continue
		}
		var next int
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(platform_id), $1 - 1) + 1 FROM aveloxis_data.platforms
			WHERE platform_id BETWEEN $1 AND $2`, int16(model.GitLabInstanceIDMin), int16(model.GitLabInstanceIDMax)).Scan(&next); err != nil {
			return nil, fmt.Errorf("gitlab instance registry: next id: %w", err)
		}
		if next > int(model.GitLabInstanceIDMax) {
			return nil, fmt.Errorf("gitlab instance registry: %s: %w (ids %d–%d are all taken)", in.WebBase, ErrGitLabInstanceIDsExhausted, model.GitLabInstanceIDMin, model.GitLabInstanceIDMax)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.platforms (platform_id, platform_name, platform_instance_url)
			VALUES ($1, $2, $3)`, int16(next), "GitLab ("+in.WebBase+")", in.WebBase); err != nil {
			return nil, fmt.Errorf("gitlab instance registry: register %s as platform_id %d: %w", in.WebBase, next, err)
		}
		registered[in.WebBase] = model.Platform(next)
		s.logger.Info("gitlab instance registry: registered GitLab instance", "web_url", in.WebBase, "platform_id", next)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("gitlab instance registry: commit: %w", err)
	}

	out := make(map[string]model.Platform, len(instances))
	for _, in := range instances {
		out[in.WebBase] = registered[in.WebBase]
		if in.Primary && row2 != "" && row2 != in.WebBase {
			s.logger.Warn("gitlab instance registry: the main GitLab instance is not the historical instance — platform_id 2 keeps its web URL, the main instance has its own id",
				"platform_id_2_web_url", row2, "main_web_url", in.WebBase, "main_platform_id", registered[in.WebBase])
		}
	}
	return out, nil
}

// schemeTwin finds a registered web base that differs from webBase only by
// scheme.
func schemeTwin(registered map[string]model.Platform, webBase string) (string, model.Platform, bool) {
	want := model.SchemelessWebBase(webBase)
	for base, id := range registered {
		if model.SchemelessWebBase(base) == want && base != webBase {
			return base, id, true
		}
	}
	return "", 0, false
}

// LoadGitLabInstanceRegistry returns every registered GitLab instance, web
// base → platform_id. Platform 2 appears only once stamped.
func (s *PostgresStore) LoadGitLabInstanceRegistry(ctx context.Context) (map[string]model.Platform, error) {
	registered, _, err := loadGitLabRegistry(ctx, s.pool)
	return registered, err
}

type registryQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadGitLabRegistry(ctx context.Context, q registryQuerier) (map[string]model.Platform, string, error) {
	rows, err := q.Query(ctx, `
		SELECT platform_id, platform_instance_url FROM aveloxis_data.platforms
		WHERE platform_instance_url IS NOT NULL`)
	if err != nil {
		return nil, "", fmt.Errorf("gitlab instance registry: read: %w", err)
	}
	defer rows.Close()
	registered := map[string]model.Platform{}
	row2 := ""
	for rows.Next() {
		var id int16
		var base string
		if err := rows.Scan(&id, &base); err != nil {
			return nil, "", fmt.Errorf("gitlab instance registry: scan: %w", err)
		}
		registered[base] = model.Platform(id)
		if model.Platform(id) == model.PlatformGitLab {
			row2 = base
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("gitlab instance registry: read: %w", err)
	}
	return registered, row2, nil
}
