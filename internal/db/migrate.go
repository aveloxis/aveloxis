package db

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
)

//go:embed schema.sql
var schemaSQL string

// RunMigrations executes the embedded schema DDL and data cleanup fixes.
// All statements use IF NOT EXISTS / ON CONFLICT DO NOTHING, so it is safe
// to run repeatedly.
//
// Error semantics (v0.19.4): every schema-changing step routes through
// execMigrationStep or addColumnIfMissing, both of which log at ERROR
// and append the error to a collector. The function returns
// errors.Join of every collected error so `aveloxis serve` and
// `aveloxis migrate` print the FULL list of failures and exit
// non-zero — operators can fix everything in one pass instead of
// chasing failures one at a time. Materialized view rebuild and
// pg_trgm extension creation remain warn-only (they're derived
// data / performance optimizations, not schema integrity).
func RunMigrations(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	logger.Info("running schema migrations")

	// errs collects every schema-integrity failure across the run. We
	// fail closed: serve refuses to start when this slice is non-empty
	// at the end, even if individual steps wrote successfully.
	var errs []error

	if _, err := pg.pool.Exec(ctx, schemaSQL); err != nil {
		// The base DDL block is the foundation — if it fails, every
		// subsequent step is operating against an unknown schema. We
		// still keep going to surface as many follow-up errors as
		// possible, but record this one first.
		logger.Error("schema migration error", "step", "base schema DDL", "error", err)
		errs = append(errs, fmt.Errorf("base schema DDL: %w", err))
	}

	// Run data cleanup for any garbage timestamps from prior versions.
	if err := cleanupBadTimestamps(ctx, pg, logger); err != nil {
		logger.Error("schema migration error", "step", "cleanupBadTimestamps", "error", err)
		errs = append(errs, fmt.Errorf("cleanupBadTimestamps: %w", err))
	}

	// Set tool_version column defaults to the current version so new inserts
	// automatically get the right value without every INSERT needing to specify it.
	setToolVersionDefaults(ctx, pg)

	// Backfill tool_version on rows that were inserted before defaults were set.
	// After the first run this is a no-op (zero rows matched).
	backfillToolVersion(ctx, pg, logger)

	// Add columns that may not exist on older schemas.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_deps_libyear", "license", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_deps_libyear", "purl", "TEXT DEFAULT ''")

	// Relax users.email constraint for OAuth users who may not have a public email.
	execMigrationStep(ctx, pg, logger, &errs, "users.email DROP NOT NULL",
		`ALTER TABLE aveloxis_ops.users ALTER COLUMN email DROP NOT NULL`)
	execMigrationStep(ctx, pg, logger, &errs, "users drop user-unique-email",
		`ALTER TABLE aveloxis_ops.users DROP CONSTRAINT IF EXISTS "user-unique-email"`)
	execMigrationStep(ctx, pg, logger, &errs, "users.text_phone DROP NOT NULL",
		`ALTER TABLE aveloxis_ops.users ALTER COLUMN text_phone DROP NOT NULL`)
	execMigrationStep(ctx, pg, logger, &errs, "users drop user-unique-phone",
		`ALTER TABLE aveloxis_ops.users DROP CONSTRAINT IF EXISTS "user-unique-phone"`)

	// Collection queue: commits column (added in v0.5.4).
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.collection_queue", "last_commits", "INT DEFAULT 0")

	// Collection queue: force-full-recollect flag (added in v0.18.24).
	// Set automatically when a job ends with a GraphQL PR batch error
	// class that leaves PR child data incomplete; set manually via
	// `aveloxis recollect <url>`. CompleteJob clears it on success.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.collection_queue", "force_full_collect", "BOOLEAN NOT NULL DEFAULT FALSE")

	// SBOM storage: format and timestamp columns (added in v0.5.4).
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_sbom_scans", "sbom_format", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_sbom_scans", "sbom_version", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_sbom_scans", "created_at", "TIMESTAMPTZ DEFAULT NOW()")

	// Contributors: enrichment tracking column (added in v0.14.4).
	// Prevents infinite re-enrichment of users with genuinely empty profiles.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "cntrb_last_enriched_at", "TIMESTAMPTZ")

	// Contributors: search-resolve tracking column (v0.19.2). The
	// scheduler's runSearchResolve background task takes contributors
	// with email but no gh_user_id, calls /search/users?q=email, and
	// stamps this column on every attempt (success or no-hit). Used
	// by GetContributorsNeedingSearch as the cooldown filter so the
	// same emails aren't re-searched every cycle, wasting the
	// 30/min/token search-API quota.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "cntrb_last_search_attempted_at", "TIMESTAMPTZ")

	// Commits: deduplicate and add unique index (added in v0.7.5).
	// Previous versions had no ON CONFLICT on commits INSERT, so re-collection
	// created duplicate rows. Clean up first, then create the unique index.
	deduplicateCommits(ctx, pg, logger)

	// Repos: strip legacy ".git" suffixes from repo_name (added in v0.11.3).
	// Repos added via Augur import / org listing before the normalize fix
	// stored names like "naturf.git", which 404s every API call (/releases,
	// /issues, /pulls). One-time cleanup; idempotent.
	cleanupRepoNameGitSuffix(ctx, pg, logger)

	// pg_trgm extension + GIN index on repos for monitor search
	// (v0.18.30). The dashboard's `?q=foo/bar` ILIKE search at v0.18.29
	// was unindexable (leading wildcard). With pg_trgm + a GIN index on
	// (repo_owner || '/' || repo_name), the planner uses the index even
	// for `ILIKE '%foo/bar%'` patterns. Turns the search from O(n) into
	// O(log n + matches). CREATE EXTENSION is idempotent and a no-op if
	// the extension already exists; CREATE INDEX IF NOT EXISTS is safe
	// to run on every startup.
	//
	// pg_trgm is the only step that stays warn-only — the extension
	// requires superuser/pg_create_extensions and is a perf optimization,
	// not data integrity. The follow-up index creation is fatal because
	// once the extension exists, the index DDL failing means a real
	// schema problem.
	if _, err := pg.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
		logger.Warn("failed to create pg_trgm extension; monitor search will use sequential scans",
			"error", err,
			"hint", "the extension requires superuser or membership in pg_create_extensions; check your role grants")
	} else {
		execMigrationStep(ctx, pg, logger, &errs, "create idx_repos_owner_name_trgm GIN index",
			`CREATE INDEX IF NOT EXISTS idx_repos_owner_name_trgm
				ON aveloxis_data.repos
				USING GIN ((repo_owner || '/' || repo_name) gin_trgm_ops)`)
	}

	// pull_request_repo: add unique constraint for ON CONFLICT support (v0.12.0).
	execMigrationStep(ctx, pg, logger, &errs, "create idx_pr_repo_meta_head_base",
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_repo_meta_head_base
		ON aveloxis_data.pull_request_repo (pr_repo_meta_id, pr_repo_head_or_base)`)

	// Users table: dedupe + enforce PK/UNIQUE (v0.18.9).
	// Older installs used CREATE TABLE IF NOT EXISTS with inline UNIQUE, which
	// silently skipped on pre-existing tables created without the constraint —
	// duplicate rows accumulated, and pg_restore to a fresh server failed
	// applying users_pkey / users_login_name_key after the data load.
	dedupeUsers(ctx, pg, logger)

	// Users table OAuth columns (added in v0.5.0).
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "avatar_url", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "gh_user_id", "BIGINT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "gh_login", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "gl_user_id", "BIGINT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "gl_username", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "oauth_provider", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "oauth_token", "TEXT DEFAULT ''")

	// v0.19.0 public-access feature: email confirmation timestamp +
	// group approval workflow. Set email_confirmed_at to NOW() at
	// signup since GitHub OAuth has already verified the address —
	// the column is for audit only, not gating. Group approval
	// columns track admin review of non-admin submissions: status
	// flips between 'pending' (the default for new groups created by
	// non-admins), 'approved', and 'rejected'.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "email_confirmed_at", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "status", "TEXT NOT NULL DEFAULT 'approved'")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "approved_by", "INT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "approved_at", "TIMESTAMPTZ")
	// Existing rows from pre-v0.19.0 deployments default to
	// 'approved' so the upgrade doesn't suddenly hide groups that
	// already exist. New rows from non-admins go to 'pending' via
	// CreateUserGroup's branch.

	// Create/update materialized views for 8Knot and analytics.
	// Skipped by default on startup (can take minutes on large databases).
	// Set collection.matview_rebuild_on_startup=true in aveloxis.json to enable,
	// or run `aveloxis refresh-views` manually. The scheduler rebuilds them
	// weekly on the configured day (default: Saturday).
	//
	// Matview refresh is warn-only — these are derived data, refreshable
	// via `aveloxis refresh-views` or the next scheduler tick, and
	// failing them shouldn't block serve startup.
	//
	// matviewSkip (set by `aveloxis migrate --skip-views`) bypasses both
	// branches so an operator iterating on schema-error fixes doesn't
	// pay the rebuild cost on every retry. The user can run
	// `aveloxis refresh-views` separately when ready, or let the
	// scheduler's weekly rebuild handle it.
	switch {
	case pg.matviewSkip:
		logger.Info("matview block skipped (--skip-views); run `aveloxis refresh-views` separately to materialize")
	case pg.matviewOnStartup:
		if err := CreateMaterializedViews(ctx, pg, logger); err != nil {
			logger.Warn("materialized view creation had errors", "error", err)
		}
	default:
		// Still create views if they don't exist (first run), but don't refresh existing ones.
		if err := CreateMaterializedViewsIfNotExist(ctx, pg, logger); err != nil {
			logger.Warn("materialized view creation had errors", "error", err)
		}
	}

	// Stamp schema version so non-migrating commands (web, api) can detect
	// when the schema is behind the binary and warn the operator.
	stampSchemaVersion(ctx, pg, logger)

	if len(errs) > 0 {
		// Fail closed: surface every collected error so the operator
		// sees the FULL list and can fix them all before retrying.
		// errors.Join produces a multi-line error string by default,
		// which prints cleanly to stderr from cobra.
		logger.Error("schema migrations completed with errors — aveloxis serve will refuse to start until these are resolved",
			"count", len(errs))
		return fmt.Errorf("schema migration had %d error(s):\n%w", len(errs), errors.Join(errs...))
	}

	logger.Info("schema migrations complete", "schema_version", ToolVersion)
	return nil
}

// execMigrationStep runs a schema-changing SQL statement, logging the
// step at INFO before and recording any error in the collector. Used
// by RunMigrations for ALTER TABLE / CREATE INDEX / etc. statements
// where pre-v0.19.4 the err was discarded entirely.
func execMigrationStep(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error, label, sql string) {
	if _, err := pg.pool.Exec(ctx, sql); err != nil {
		logger.Error("schema migration error", "step", label, "error", err)
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
	}
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

// addColumnIfMissing runs ALTER TABLE ... ADD COLUMN IF NOT EXISTS for
// the given table/column/type. Pre-v0.19.4 this helper used the
// `_, _ = pg.pool.Exec(...)` discard-everything pattern, which made
// every failure silent — that's how the v0.19.0 user_groups status/
// approved_by/approved_at columns went missing on chaoss.tv even
// though `aveloxis migrate` had completed successfully. The fixed
// helper logs at ERROR and appends to the collector so the run
// surfaces every failure, and so RunMigrations can fail closed.
func addColumnIfMissing(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error, table, column, colType string) {
	stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`, table, column, colType)
	if _, err := pg.pool.Exec(ctx, stmt); err != nil {
		label := fmt.Sprintf("add column %s.%s (%s)", table, column, colType)
		logger.Error("schema migration error", "step", label, "error", err)
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
	}
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

// dedupeUsers removes duplicate rows from aveloxis_ops.users and ensures the
// primary key + UNIQUE(login_name) constraints actually exist. Tables created
// by early versions of schema.sql via `CREATE TABLE IF NOT EXISTS` escaped the
// later addition of inline UNIQUE/PRIMARY KEY because IF NOT EXISTS silently
// skips. Duplicate rows then accumulated through OAuth logins, and pg_restore
// to a fresh server failed when applying users_pkey / users_login_name_key
// after the data load. Idempotent: after the first run, matches zero rows.
func dedupeUsers(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	// 1. Drop rows that duplicate an existing user_id. Keep the row with the
	//    smallest ctid (physical position — stable within a single transaction
	//    and usable before a PRIMARY KEY exists).
	tag, err := pg.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.users a
		USING aveloxis_ops.users b
		WHERE a.ctid > b.ctid
		  AND a.user_id = b.user_id`)
	if err != nil {
		logger.Warn("users user_id dedup failed", "error", err)
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		logger.Info("deduped users by user_id", "rows_removed", n)
	}

	// 2. Drop rows that duplicate login_name across distinct user_ids. Keep
	//    the lowest user_id so FKs pointing at the older row stay valid.
	tag, err = pg.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.users
		WHERE user_id NOT IN (
			SELECT MIN(user_id)
			FROM aveloxis_ops.users
			GROUP BY login_name
		)`)
	if err != nil {
		logger.Warn("users login_name dedup failed", "error", err)
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		logger.Info("deduped users by login_name", "rows_removed", n)
	}

	// 3. Ensure the PRIMARY KEY exists. Postgres has no ADD CONSTRAINT IF NOT
	//    EXISTS, so we check pg_constraint first.
	_, err = pg.pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = 'aveloxis_ops.users'::regclass
				  AND contype = 'p'
			) THEN
				ALTER TABLE aveloxis_ops.users ADD PRIMARY KEY (user_id);
			END IF;
		END $$`)
	if err != nil {
		logger.Warn("users PRIMARY KEY add failed", "error", err)
	}

	// 4. Ensure UNIQUE(login_name) exists.
	_, err = pg.pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = 'aveloxis_ops.users'::regclass
				  AND contype = 'u'
				  AND conkey = ARRAY[
				      (SELECT attnum FROM pg_attribute
				       WHERE attrelid = 'aveloxis_ops.users'::regclass
				         AND attname = 'login_name')
				  ]
			) THEN
				ALTER TABLE aveloxis_ops.users
				  ADD CONSTRAINT users_login_name_key UNIQUE (login_name);
			END IF;
		END $$`)
	if err != nil {
		logger.Warn("users UNIQUE(login_name) add failed", "error", err)
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
