package db

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
)

//go:embed schema.sql
var schemaSQL string

// RunMigrations executes the embedded schema DDL and data cleanup fixes.
// All statements use IF NOT EXISTS / ON CONFLICT DO NOTHING, so it is safe
// to run repeatedly.
func RunMigrations(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	logger.Info("running schema migrations")
	_, err := pg.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("schema migration failed: %w", err)
	}

	// Run data cleanup for any garbage timestamps from prior versions.
	if err := cleanupBadTimestamps(ctx, pg, logger); err != nil {
		logger.Warn("timestamp cleanup had errors", "error", err)
		// Non-fatal — don't block startup.
	}

	// Set tool_version column defaults to the current version so new inserts
	// automatically get the right value without every INSERT needing to specify it.
	setToolVersionDefaults(ctx, pg)

	// Backfill tool_version on rows that were inserted before defaults were set.
	// After the first run this is a no-op (zero rows matched).
	backfillToolVersion(ctx, pg, logger)

	// Add columns that may not exist on older schemas.
	addColumnIfMissing(ctx, pg, "aveloxis_data.repo_deps_libyear", "license", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_data.repo_deps_libyear", "purl", "TEXT DEFAULT ''")

	// Relax users.email constraint for OAuth users who may not have a public email.
	pg.pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users ALTER COLUMN email DROP NOT NULL`)
	pg.pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users DROP CONSTRAINT IF EXISTS "user-unique-email"`)
	pg.pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users ALTER COLUMN text_phone DROP NOT NULL`)
	pg.pool.Exec(ctx, `ALTER TABLE aveloxis_ops.users DROP CONSTRAINT IF EXISTS "user-unique-phone"`)

	// Collection queue: commits column (added in v0.5.4).
	addColumnIfMissing(ctx, pg, "aveloxis_ops.collection_queue", "last_commits", "INT DEFAULT 0")

	// SBOM storage: format and timestamp columns (added in v0.5.4).
	addColumnIfMissing(ctx, pg, "aveloxis_data.repo_sbom_scans", "sbom_format", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_data.repo_sbom_scans", "sbom_version", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_data.repo_sbom_scans", "created_at", "TIMESTAMPTZ DEFAULT NOW()")

	// Contributors: enrichment tracking column (added in v0.14.4).
	// Prevents infinite re-enrichment of users with genuinely empty profiles.
	addColumnIfMissing(ctx, pg, "aveloxis_data.contributors", "cntrb_last_enriched_at", "TIMESTAMPTZ")

	// Commits: deduplicate and add unique index (added in v0.7.5).
	// Previous versions had no ON CONFLICT on commits INSERT, so re-collection
	// created duplicate rows. Clean up first, then create the unique index.
	deduplicateCommits(ctx, pg, logger)

	// Repos: strip legacy ".git" suffixes from repo_name (added in v0.11.3).
	// Repos added via Augur import / org listing before the normalize fix
	// stored names like "naturf.git", which 404s every API call (/releases,
	// /issues, /pulls). One-time cleanup; idempotent.
	cleanupRepoNameGitSuffix(ctx, pg, logger)

	// pull_request_repo: add unique constraint for ON CONFLICT support (v0.12.0).
	pg.pool.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_repo_meta_head_base
		ON aveloxis_data.pull_request_repo (pr_repo_meta_id, pr_repo_head_or_base)`)

	// Users table OAuth columns (added in v0.5.0).
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "avatar_url", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "gh_user_id", "BIGINT")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "gh_login", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "gl_user_id", "BIGINT")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "gl_username", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "oauth_provider", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, "aveloxis_ops.users", "oauth_token", "TEXT DEFAULT ''")

	// Create/update materialized views for 8Knot and analytics.
	// Skipped by default on startup (can take minutes on large databases).
	// Set collection.matview_rebuild_on_startup=true in aveloxis.json to enable,
	// or run `aveloxis refresh-views` manually. The scheduler rebuilds them
	// weekly on the configured day (default: Saturday).
	if pg.matviewOnStartup {
		if err := CreateMaterializedViews(ctx, pg, logger); err != nil {
			logger.Warn("materialized view creation had errors", "error", err)
		}
	} else {
		// Still create views if they don't exist (first run), but don't refresh existing ones.
		if err := CreateMaterializedViewsIfNotExist(ctx, pg, logger); err != nil {
			logger.Warn("materialized view creation had errors", "error", err)
		}
	}

	// Stamp schema version so non-migrating commands (web, api) can detect
	// when the schema is behind the binary and warn the operator.
	stampSchemaVersion(ctx, pg, logger)

	logger.Info("schema migrations complete", "schema_version", ToolVersion)
	return nil
}

// stampSchemaVersion writes the current ToolVersion into schema_meta.
// Called at the end of RunMigrations so the version reflects the latest
// successful migration, not just a binary update.
func stampSchemaVersion(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	_, err := pg.pool.Exec(ctx, `
		UPDATE aveloxis_ops.schema_meta
		SET schema_version = $1, migrated_at = NOW()
		WHERE id = TRUE`, ToolVersion)
	if err != nil {
		logger.Warn("failed to stamp schema version", "error", err)
	}
}

// GetSchemaVersion reads the schema version from the database. Returns an
// empty string if the schema_meta table doesn't exist yet (pre-v0.14.5 DB).
func (s *PostgresStore) GetSchemaVersion(ctx context.Context) string {
	var version string
	err := s.pool.QueryRow(ctx,
		`SELECT schema_version FROM aveloxis_ops.schema_meta WHERE id = TRUE`,
	).Scan(&version)
	if err != nil {
		return ""
	}
	return version
}

// CheckSchemaVersion compares the database schema version against the running
// binary's ToolVersion and logs a warning if they don't match. Intended for
// non-migrating commands (web, api) so operators get a clear signal to run
// `aveloxis migrate` or restart `aveloxis serve`.
func (s *PostgresStore) CheckSchemaVersion(ctx context.Context, logger *slog.Logger) {
	dbVersion := s.GetSchemaVersion(ctx)
	if dbVersion == "" {
		logger.Warn("schema version unknown — run 'aveloxis migrate' or restart 'aveloxis serve' to initialize schema tracking")
		return
	}
	if dbVersion != ToolVersion {
		logger.Warn("schema version mismatch: database schema is behind the binary",
			"db_schema_version", dbVersion,
			"binary_version", ToolVersion,
			"action", "run 'aveloxis migrate' or restart 'aveloxis serve'")
	}
}

// setToolVersionDefaults updates the DEFAULT for every tool_version column to
// the current ToolVersion. This way new INSERTs that omit tool_version
// automatically get the correct value without needing it in every INSERT list.
// Only alters tables whose default doesn't already match, so on most startups
// this is a no-op.
func setToolVersionDefaults(ctx context.Context, pg *PostgresStore) {
	expectedDefault := fmt.Sprintf("'%s'::text", ToolVersion)
	rows, err := pg.pool.Query(ctx, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.columns
		WHERE column_name = 'tool_version'
		  AND table_schema IN ('aveloxis_data', 'aveloxis_ops')
		  AND (column_default IS NULL OR column_default != $1)`,
		expectedDefault)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if rows.Scan(&table) == nil {
			pg.pool.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE %s ALTER COLUMN tool_version SET DEFAULT '%s'`,
				table, ToolVersion))
		}
	}
}

// backfillToolVersion sets tool_version on rows where it's empty.
// After setToolVersionDefaults has run and collection uses the new defaults,
// this becomes a no-op on subsequent startups.
func backfillToolVersion(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	tables := []string{
		"aveloxis_data.repo_groups",
		"aveloxis_data.repos",
		"aveloxis_data.contributors",
		"aveloxis_data.contributors_aliases",
		"aveloxis_data.issues",
		"aveloxis_data.issue_labels",
		"aveloxis_data.issue_assignees",
		"aveloxis_data.issue_events",
		"aveloxis_data.pull_requests",
		"aveloxis_data.pull_request_labels",
		"aveloxis_data.pull_request_assignees",
		"aveloxis_data.pull_request_reviewers",
		"aveloxis_data.pull_request_reviews",
		"aveloxis_data.pull_request_commits",
		"aveloxis_data.pull_request_files",
		"aveloxis_data.pull_request_meta",
		"aveloxis_data.pull_request_events",
		"aveloxis_data.messages",
		"aveloxis_data.issue_message_ref",
		"aveloxis_data.pull_request_message_ref",
		"aveloxis_data.releases",
		"aveloxis_data.commits",
		"aveloxis_data.commit_messages",
		"aveloxis_data.commit_parents",
		"aveloxis_data.repo_info",
		"aveloxis_data.repo_clones",
		"aveloxis_data.repo_labor",
		"aveloxis_data.repo_dependencies",
		"aveloxis_data.repo_deps_libyear",
		"aveloxis_data.contributor_repo",
		"aveloxis_data.unresolved_commit_emails",
	}
	totalFixed := 0
	for _, table := range tables {
		tag, err := pg.pool.Exec(ctx, fmt.Sprintf(
			`UPDATE %s SET tool_version = $1 WHERE tool_version IS NULL OR tool_version = ''`,
			table), ToolVersion)
		if err != nil {
			continue
		}
		if n := tag.RowsAffected(); n > 0 {
			totalFixed += int(n)
			logger.Debug("backfilled tool_version", "table", table, "rows", n)
		}
	}
	if totalFixed > 0 {
		logger.Info("backfilled tool_version on rows missing it", "total_rows", totalFixed)
	}
}

// addColumnIfMissing adds a column to a table if it doesn't exist.
// deduplicateCommits removes duplicate rows in the commits table and creates
// a unique index to prevent future duplicates. Previous versions had no
// ON CONFLICT clause on commit inserts, so re-collection runs created
// duplicate (repo_id, cmt_commit_hash, cmt_filename) rows.
func deduplicateCommits(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	// Check if the unique index already exists — if so, dedup was already done.
	var exists bool
	pg.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE schemaname = 'aveloxis_data' AND indexname = 'idx_commits_repo_hash_file'
		)`).Scan(&exists)
	if exists {
		return // already cleaned up
	}

	// Count duplicates to decide if we need to clean up.
	var dupCount int
	pg.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT cmt_commit_hash, cmt_filename, repo_id
			FROM aveloxis_data.commits
			GROUP BY cmt_commit_hash, cmt_filename, repo_id
			HAVING COUNT(*) > 1
			LIMIT 1
		) sub`).Scan(&dupCount)

	if dupCount > 0 {
		logger.Info("deduplicating commits table (one-time migration)")
		// Delete duplicates, keeping the row with the lowest cmt_id.
		tag, err := pg.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.commits
			WHERE cmt_id NOT IN (
				SELECT MIN(cmt_id)
				FROM aveloxis_data.commits
				GROUP BY repo_id, cmt_commit_hash, cmt_filename
			)`)
		if err != nil {
			logger.Warn("failed to deduplicate commits", "error", err)
			return
		}
		logger.Info("deduplicated commits", "rows_removed", tag.RowsAffected())
	}

	// Create the unique index now that duplicates are gone.
	_, err := pg.pool.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_commits_repo_hash_file
		ON aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename)`)
	if err != nil {
		logger.Warn("failed to create commits unique index", "error", err)
	}
}

func addColumnIfMissing(ctx context.Context, pg *PostgresStore, table, column, colType string) {
	_, _ = pg.pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`, table, column, colType))
}

// cleanupRepoNameGitSuffix strips a trailing ".git" from aveloxis_data.repos.repo_name.
// Repos added before the write-side normalization fix (and Augur imports) could
// store slugs like "naturf.git", which 404s every API endpoint that embeds the
// slug (/repos/{owner}/{name}/releases, /issues, /pulls). Idempotent: after the
// first run this matches zero rows.
func cleanupRepoNameGitSuffix(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	tag, err := pg.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_name = regexp_replace(repo_name, '\.git$', '')
		WHERE repo_name LIKE '%.git'`)
	if err != nil {
		logger.Warn("repo_name .git cleanup failed", "error", err)
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		logger.Info("stripped .git suffix from repo_name", "rows_updated", n)
	}
}

// cleanupBadTimestamps nullifies any timestamp columns that have garbage values
// (e.g., year 0001 BC from Go zero time.Time). These occur when a struct's
// time fields were not populated before being passed to an INSERT.
//
// A timestamp is considered garbage if its year is before 1970.
func cleanupBadTimestamps(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	// Each entry: table, column, nullable (true = SET NULL, false = SET to NOW()).
	fixes := []struct {
		table  string
		column string
		useNow bool // if true, replace with NOW() instead of NULL (for NOT NULL columns)
	}{
		// repos
		{"aveloxis_data.repos", "created_at", false},
		{"aveloxis_data.repos", "updated_at", false},

		// issues
		{"aveloxis_data.issues", "created_at", false},
		{"aveloxis_data.issues", "updated_at", false},
		{"aveloxis_data.issues", "closed_at", false},

		// pull_requests
		{"aveloxis_data.pull_requests", "created_at", false},
		{"aveloxis_data.pull_requests", "updated_at", false},
		{"aveloxis_data.pull_requests", "closed_at", false},
		{"aveloxis_data.pull_requests", "merged_at", false},

		// messages
		{"aveloxis_data.messages", "msg_timestamp", false},

		// issue_events
		{"aveloxis_data.issue_events", "created_at", false},

		// pull_request_events
		{"aveloxis_data.pull_request_events", "created_at", false},

		// pull_request_reviews
		{"aveloxis_data.pull_request_reviews", "submitted_at", false},

		// pull_request_commits
		{"aveloxis_data.pull_request_commits", "pr_cmt_timestamp", false},

		// releases
		{"aveloxis_data.releases", "created_at", false},
		{"aveloxis_data.releases", "published_at", false},
		{"aveloxis_data.releases", "updated_at", false},

		// repo_info
		{"aveloxis_data.repo_info", "last_updated", false},

		// commits
		{"aveloxis_data.commits", "cmt_committer_timestamp", false},
		{"aveloxis_data.commits", "cmt_author_timestamp", false},
		{"aveloxis_data.commits", "cmt_date_attempted", true},

		// contributors
		{"aveloxis_data.contributors", "cntrb_created_at", false},

		// collection_status
		{"aveloxis_ops.collection_status", "core_data_last_collected", false},
		{"aveloxis_ops.collection_status", "secondary_data_last_collected", false},
		{"aveloxis_ops.collection_status", "facade_data_last_collected", false},
		{"aveloxis_ops.collection_status", "ml_data_last_collected", false},
	}

	totalFixed := 0
	for _, f := range fixes {
		replacement := "NULL"
		if f.useNow {
			replacement = "NOW()"
		}
		query := fmt.Sprintf(
			`UPDATE %s SET "%s" = %s WHERE "%s" IS NOT NULL AND EXTRACT(YEAR FROM "%s") < 1970`,
			f.table, f.column, replacement, f.column, f.column,
		)
		tag, err := pg.pool.Exec(ctx, query)
		if err != nil {
			// Table or column may not exist yet — skip silently.
			continue
		}
		n := tag.RowsAffected()
		if n > 0 {
			logger.Debug("cleaned up garbage timestamps",
				"table", f.table, "column", f.column, "rows", n)
			totalFixed += int(n)
		}
	}

	if totalFixed > 0 {
		logger.Info("timestamp cleanup complete", "total_rows_fixed", totalFixed)
	}
	return nil
}
