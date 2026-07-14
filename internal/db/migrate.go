// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
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

	// v0.20.1: acquire a postgres advisory lock so two `aveloxis
	// migrate` processes (or migrate + serve's startup-migrate) can't
	// race on table-level locks. The lock is held on a dedicated pool
	// connection for the entire migration; released via defer.
	//
	// Without this, two concurrent migrations contend on schema-level
	// locks and produce the kind of confusion the 2026-05-08 incident
	// surfaced (orphan UPDATE blocking a CREATE INDEX, with no clean
	// way to tell which migrate is doing what).
	lockConn, err := pg.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration advisory-lock connection: %w", err)
	}
	defer lockConn.Release()

	if pg.migrateNoWait {
		var ok bool
		if err := lockConn.QueryRow(ctx,
			`SELECT pg_try_advisory_lock($1)`, MigrateAdvisoryLockID).Scan(&ok); err != nil {
			return fmt.Errorf("try advisory lock: %w", err)
		}
		if !ok {
			return fmt.Errorf("another aveloxis migration is in progress (advisory lock held); use without --no-wait to wait, or check pg_stat_activity for the holder")
		}
	} else {
		// Blocking acquire. Log if it takes more than 5 seconds so
		// the operator knows we're waiting on someone else.
		acquireDone := make(chan struct{})
		go func() {
			t := time.NewTimer(5 * time.Second)
			defer t.Stop()
			select {
			case <-acquireDone:
			case <-t.C:
				logger.Info("waiting for migration advisory lock — another aveloxis migration is in progress")
			}
		}()
		if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, MigrateAdvisoryLockID); err != nil {
			close(acquireDone)
			return fmt.Errorf("acquire advisory lock: %w", err)
		}
		close(acquireDone)
	}
	defer func() {
		// Best-effort release. If this fails, the lock release happens
		// on connection close (lockConn.Release returns to pool, where
		// pgxpool's reset handlers eventually close idle connections).
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, MigrateAdvisoryLockID)
	}()

	// errs collects every schema-integrity failure across the run. We
	// fail closed: serve refuses to start when this slice is non-empty
	// at the end, even if individual steps wrote successfully.
	var errs []error

	// v0.20.0: spawn a watcher goroutine that surfaces blocked-DDL with
	// PID hints. Pre-v0.20.0, when migrate was blocked on a held lock
	// (the 2026-05-08 incident: orphan PID 10323 holding RowExclusiveLock
	// on commits while the new serve's startup DDL waited 14+ minutes),
	// the only operator-visible signal was migrate sitting silent. The
	// watcher polls pg_stat_activity every 60 seconds for backends that
	// are blocked AND running schema/migration-style queries (filtered
	// by application_name + wait_event_type='Lock'); when blocked, it
	// uses pg_blocking_pids() to surface the holder with a
	// pg_terminate_backend(N) recipe.
	migrateDone := make(chan struct{})
	go watchBlockers(ctx, pg, logger, migrateDone)
	defer close(migrateDone)

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

	// Contributors: breadth tracking column (v0.20.17). The
	// scheduler's runBreadth ticker takes contributors past the
	// configured cooldown, calls /users/{login}/events, and stamps
	// this column on every attempt — even when the response is
	// empty. Pre-v0.20.17 the worker used
	// MAX(contributor_repo.data_collection_date) NULLS FIRST as
	// the "processed" signal, which left contributors with zero
	// public events permanently at the front of the queue. With
	// 1.4M contributors observed on the live aveloxis_large
	// fleet, only 225 (0.016%) ever got events recorded because
	// the worker kept reselecting the same dead-end users.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "cntrb_last_breadth_at", "TIMESTAMPTZ")

	// v0.25.7 — mailing-list ingestion. New email_message + email_message_ref
	// tables are created by schema.sql's CREATE TABLE IF NOT EXISTS on every
	// migrate. These add the columns that land on EXISTING tables, plus the
	// idempotency / dedup indexes (the v0.20.1 CONCURRENTLY convention).
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.issues", "external_key", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_system", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_last_month", "TEXT DEFAULT ''")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_scan_complete", "BOOLEAN DEFAULT FALSE")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_failed_attempts", "INTEGER DEFAULT 0")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_last_failed_at", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_last_run", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_locked_at", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_locked_pid", "INTEGER")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repo_groups_list_serve", "mlls_locked_boot_id", "TEXT DEFAULT ''")
	// Phase 3 (summary/12 §10a): projection columns on email_message. NEW since
	// v0.25.7. linked_pr_review_id completes the link triad (issue/pr/review);
	// projected_kind records what the email was projected onto so it's queryable
	// without joins.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.email_message", "linked_pr_review_id",
		"BIGINT REFERENCES aveloxis_data.pull_request_reviews(pr_review_id) DEFERRABLE INITIALLY DEFERRED")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.email_message", "projected_kind", "TEXT DEFAULT ''")
	// Partial unique on (repo_id, external_key): one issue per external key
	// per repo. (issues has no platform_id column — issues are scoped by
	// repo_id, which carries the platform.) Empty external_key (native
	// GitHub/GitLab rows) excluded so they never collide. Mirrors
	// idx_contributors_login's partial shape. external_key holds 'LUCENE-1'
	// parsed from Pattern-A imported titles, giving mail cross-references a
	// stable key to join on.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_issues_external_key",
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_issues_external_key
		 ON aveloxis_data.issues (repo_id, external_key) WHERE external_key <> ''`)
	// Idempotent list registration: one row per (repo_group, list address).
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_rgls_group_email",
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_rgls_group_email
		 ON aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email)`)
	// v0.25.20 — indexes for the mailing-list projection backfill's per-row
	// lookups. Without these, backfill-mailing-list-projection (and the live
	// projection path) sequential-scan messages / email_message per row — the
	// cause of the ~500-rows-per-30-min crawl observed 2026-06-04. node_id is
	// the body-row join key (UpsertMailingListMessageBody sets node_id =
	// Message-ID); thread_root_id is FindIssueForThread's lookup key.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_messages_node_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_node_id
		 ON aveloxis_data.messages (node_id) WHERE node_id <> ''`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_email_message_thread_root",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_email_message_thread_root
		 ON aveloxis_data.email_message (thread_root_id) WHERE thread_root_id <> ''`)
	// v0.25.22 — candidate-batch indexes for the projection backfill: each batch
	// becomes an index scan over only the still-unprojected rows instead of a
	// full seq scan + sort of email_message (~4.4s/batch on prod).
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_em_proj_pending_keyed",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_em_proj_pending_keyed
		 ON aveloxis_data.email_message (email_message_id)
		 WHERE msg_class = 'issue_event' AND COALESCE(projected_kind, '') = '' AND linked_external_key <> ''`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_em_proj_pending_threaded",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_em_proj_pending_threaded
		 ON aveloxis_data.email_message (email_message_id)
		 WHERE thread_root_id <> '' AND COALESCE(projected_kind, '') = ''`)

	// v0.21.0 — ScancodeWorker state on aveloxis_data.repos.
	//
	// Decouples the scancode per-file license + copyright + package
	// scan from the per-repo collection pipeline. Pre-v0.21.0, scancode
	// ran inside AnalysisCollector.AnalyzeRepo gated by a 2-slot
	// package-level semaphore. The 2026-05-14 production incident
	// (177 of 180 worker goroutines parked at scancode.go:114 for
	// 7+ hours, queue depth growing monotonically) showed that semaphore
	// shape doesn't survive fleet-scale operation. The ScancodeWorker
	// pool runs in its own goroutines, claims eligible repos via the
	// claim SQL below, and never blocks the main collection cycle.
	//
	// Six new columns. scancode_last_run + scancode_version capture
	// completion state. scancode_locked_at + scancode_locked_pid +
	// scancode_locked_boot_id capture in-flight state across restarts
	// — the boot_id field is the kernel boot UUID from
	// /proc/sys/kernel/random/boot_id, paired with the OS PID so the
	// worker's recovery logic can distinguish "scan still running as
	// orphan after aveloxis crashed" from "scan died, PID was reused
	// by an unrelated process after reboot". scancode_output_path
	// records where the subprocess writes results.json so the
	// recovery monitor knows where to look when it observes the
	// orphan exit.
	//
	// Full lock state machine — including the four post-restart states
	// (reboot survivor, live orphan, recoverable corpse, lost run) —
	// is documented in docs/architecture/scancode.md.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_last_run", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_version", "TEXT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_locked_at", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_locked_pid", "INTEGER")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_locked_boot_id", "TEXT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_output_path", "TEXT")

	// v0.21.4: failure tracking + exponential backoff.
	//
	// 2026-05-14 production diagnostic: the v0.21.0 ScancodeWorker
	// cleared the lock columns on failure but kept scancode_last_run
	// NULL. The claim query orders NULLS FIRST on scancode_last_run,
	// so failed repos became the highest-priority candidates for the
	// next dispatcher tick. With ~45s failure scans and ~3-min
	// healthy scans, 10 doomed repos dominated visible activity on
	// a 7-worker pool.
	//
	// v0.21.4 adds a counter + last-failure-time column so the claim
	// query can apply per-row exponential backoff (1h, 4h, 9h, ...,
	// 7d cap). After ScancodeMaxFailures consecutive failures the
	// RecordScancodeFailure helper also stamps scancode_last_run =
	// NOW(), letting the cadence gate (default 180 days) shoulder
	// the long-tail "this repo will never scan" cases out of the
	// queue.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_failed_attempts", "INTEGER DEFAULT 0")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_last_failed_at", "TIMESTAMPTZ")
	// v0.23.8: separate counter for wall-clock-timeout failures
	// (subprocess SIGKILL'd by cmd.Cancel). Drives the per-repo
	// adaptive timeout `min(base * 2^attempts, cap)`. Distinct
	// from scancode_failed_attempts so timeout-class failures
	// don't trigger the v0.21.4 10-strike sideline on kernel-class
	// repos.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "scancode_timeout_attempts", "INTEGER DEFAULT 0")

	// v0.24.0 — DistributionWorker columns. Mirror the v0.21.0
	// scancode_* triple but for the new package-distribution-evidence
	// subsystem. Off by default (distribution_tracking_enabled in
	// aveloxis.json) but the columns exist on every install so the
	// claim query doesn't fail on a fresh-from-config opt-in.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "distribution_last_run", "TIMESTAMPTZ")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "distribution_failed_attempts", "INTEGER DEFAULT 0")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "distribution_last_failed_at", "TIMESTAMPTZ")

	// v0.25.0: distribution_scan_complete tracks whether the most-
	// recent distribution scan was complete (all external sources
	// consulted successfully) or partial (some external source had
	// a transient error or was skipped due to an open circuit
	// breaker). The claim query treats scan_complete=FALSE as
	// immediately re-eligible (bypassing the cadence gate) so the
	// ~10 repos that trip the ecosyste.ms breaker get fully
	// re-collected on the next dispatch cycle after the breaker
	// closes — rotating their partial-scan rows to history.
	//
	// Default TRUE so existing rows (pre-v0.25.0 installs that have
	// already scanned) are not treated as incomplete on first
	// startup. Only NEW scans under v0.25.0+ can mark this false.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "distribution_scan_complete", "BOOLEAN DEFAULT TRUE")

	// Commits: deduplicate and add unique index (added in v0.7.5).
	// Previous versions had no ON CONFLICT on commits INSERT, so re-collection
	// created duplicate rows. Clean up first, then create the unique index.
	deduplicateCommits(ctx, pg, logger)

	// Repos: strip legacy ".git" suffixes from repo_name (added in v0.11.3).
	// Repos added via Augur import / org listing before the normalize fix
	// stored names like "naturf.git", which 404s every API call (/releases,
	// /issues, /pulls). One-time cleanup; idempotent.
	cleanupRepoNameGitSuffix(ctx, pg, logger)

	// repos.repo_git case-insensitive matching (v0.25.32). GitHub and
	// GitLab treat owner/repo paths case-insensitively, so case-variant
	// URLs are the SAME repository — byte-exact matching let the same
	// repo in twice under different casing (1,220 duplicate pairs on the
	// production fleet, each collected in full twice). Two indexes with
	// different contracts:
	//
	// idx_repos_repo_git_lower (non-unique, FAIL-CLOSED) serves the
	// LOWER(repo_git) lookups in FindRepoByURL / resolveCaseVariantURL —
	// without it each lookup sequential-scans the repos table.
	execCreateIndexConcurrently(ctx, pg, logger, &errs,
		"aveloxis_data", "idx_repos_repo_git_lower",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_repo_git_lower
		ON aveloxis_data.repos (LOWER(repo_git))`)

	// uq_repos_repo_git_ci (UNIQUE partial, WARN-ONLY) is the hard
	// backstop against future case-variant duplicates. It can only be
	// created once the fleet has zero case-dup groups — operators run
	// `aveloxis dedup-repos` first, then re-run migrate. Per the
	// CLAUDE.md schema-DDL-ordering rule the index is NOT declared in
	// schema.sql (CREATE UNIQUE INDEX fails on existing duplicates).
	ensureRepoGitCaseInsensitiveUnique(ctx, pg, logger)

	// pg_trgm extension + GIN index on repos for monitor search
	// (v0.18.30). The dashboard's `?q=foo/bar` ILIKE search at v0.18.29
	// was unindexable (leading wildcard). With pg_trgm + a GIN index on
	// (repo_owner || '/' || repo_name), the planner uses the index even
	// for `ILIKE '%foo/bar%'` patterns. Turns the search from O(n) into
	// O(log n + matches). CREATE EXTENSION is idempotent and a no-op if
	// the extension already exists; CREATE INDEX IF NOT EXISTS is safe
	// to run on every startup.
	//
	// pg_trgm extension creation stays warn-only — it requires superuser/
	// pg_create_extensions and is a perf optimization, not data integrity.
	//
	// The dependent GIN index is built with a SCHEMA-QUALIFIED operator
	// class (v0.25.30). The earlier code emitted an unqualified
	// `gin_trgm_ops`, whose resolution depends on the session search_path.
	// The extension can be registered in pg_extension while its operator
	// class lives in a schema (typically public) that is NOT on the
	// connecting role's search_path — `CREATE EXTENSION IF NOT EXISTS`
	// returns nil (no-op) but the index DDL then fails with SQLSTATE 42704
	// ("operator class gin_trgm_ops does not exist for access method gin").
	// Observed on kate 2026-06-13 with a search_path that excluded public.
	// We discover the schema that actually contains gin_trgm_ops and
	// qualify the reference (`<schema>.gin_trgm_ops`), so the critical
	// monitor-search index builds on the same migrate run regardless of
	// search_path — no ALTER ROLE, no extra privileges, no global side
	// effects. When the opclass is found, an index-DDL failure is still
	// surfaced as a fatal error (a genuine schema problem). When it is
	// absent entirely (extension not actually installed), we warn-and-skip
	// rather than block serve startup over a perf index.
	if _, err := pg.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
		logger.Warn("failed to create pg_trgm extension; monitor search will use sequential scans",
			"error", err,
			"hint", "the extension requires superuser or membership in pg_create_extensions; check your role grants")
	} else if schema := ginTrgmOpsSchema(ctx, pg); schema == "" {
		logger.Warn("pg_trgm operator class gin_trgm_ops not found; skipping idx_repos_owner_name_trgm (monitor search will use sequential scans)",
			"hint", "CREATE EXTENSION reported success but no gin_trgm_ops opclass exists in any schema; "+
				"verify pg_trgm is actually installed in this database (\\dx in psql), then re-run migrate")
	} else {
		execCreateIndexConcurrently(ctx, pg, logger, &errs,
			"aveloxis_data", "idx_repos_owner_name_trgm",
			fmt.Sprintf(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_owner_name_trgm
				ON aveloxis_data.repos
				USING GIN ((repo_owner || '/' || repo_name) %s.gin_trgm_ops)`,
				quoteIdent(schema)))
	}
	// NOTE: the mailing-list projection's LINK-by-title fallback
	// (`issue_title LIKE '%[KEY-N]%'`) does NOT need a trigram index — the query
	// filters `repo_id` first (idx_issues_repo_id), so the LIKE only scans one
	// repo's issues, not the whole table. The exact external_key match is tried
	// first (idx_issues_external_key) and the fallback runs only on a miss.

	// pull_request_repo: add unique constraint for ON CONFLICT support (v0.12.0).
	execCreateIndexConcurrently(ctx, pg, logger, &errs,
		"aveloxis_data", "idx_pr_repo_meta_head_base",
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_pr_repo_meta_head_base
		ON aveloxis_data.pull_request_repo (pr_repo_meta_id, pr_repo_head_or_base)`)

	// contributors.gh_login partial index (v0.19.9). The 2026-05-08
	// pg_stat_activity diagnostic on a fleet-scale DB caught 25+
	// concurrent backends running
	// `SELECT cntrb_id FROM contributors WHERE gh_login = $1 LIMIT 1`
	// (FindContributorIDByLogin, called once per resolved commit by
	// CommitResolver.ensureAlias) — each one a sequential scan of the
	// ~5M-row contributors table because cntrb_login was indexed but
	// gh_login wasn't. The same missing index made
	// BackfillCommitAuthorIDs's join probe a hash join over the entire
	// contributors table, producing 2:30-minute UPDATE durations.
	// Partial — `WHERE gh_login != ''` excludes the email-only
	// contributor cohort, mirroring the idx_contributors_login pattern.
	execCreateIndexConcurrently(ctx, pg, logger, &errs,
		"aveloxis_data", "idx_contributors_gh_login",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_gh_login
		ON aveloxis_data.contributors (gh_login) WHERE gh_login != ''`)

	// v0.21.0 — ScancodeWorker claim-query index.
	//
	// The worker runs a claim query roughly every
	// collection.scancode_start_interval_s seconds (default 90),
	// against ALL repos. Without an index the planner falls back to a
	// sequential scan of repos every claim — at fleet scale that's
	// hundreds of thousands of rows examined per minute, all
	// returning the same eligible head.
	//
	// Partial: WHERE COALESCE(repo_archived, FALSE) = FALSE excludes
	// repos whose owning org marked them as archived (we never scan
	// those). NULLS FIRST on scancode_last_run sorts never-scanned
	// repos to the front, mirroring the claim query's ORDER BY.
	execCreateIndexConcurrently(ctx, pg, logger, &errs,
		"aveloxis_data", "idx_repos_scancode_due",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_scancode_due
		ON aveloxis_data.repos (scancode_last_run NULLS FIRST)
		WHERE COALESCE(repo_archived, FALSE) = FALSE`)

	// v0.24.0 — DistributionWorker claim-query index.
	//
	// Same shape as idx_repos_scancode_due. The DistributionWorker's
	// claim query orders by distribution_last_run NULLS FIRST so
	// never-scanned repos move to the front; the partial filter
	// excludes archived repos which we never scan. Without the index
	// the dispatcher's tick scans the entire repos table on every
	// claim attempt.
	execCreateIndexConcurrently(ctx, pg, logger, &errs,
		"aveloxis_data", "idx_repos_distribution_due",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repos_distribution_due
		ON aveloxis_data.repos (distribution_last_run NULLS FIRST)
		WHERE COALESCE(repo_archived, FALSE) = FALSE`)

	// v0.24.1 — one-shot reset of distribution_last_run for fleets
	// affected by the v0.24.0 deps.dev URL-encoding bug.
	//
	// v0.24.0 built the deps.dev project_id path parameter by URL-
	// encoding owner and repo separately and joining with raw slashes:
	//   /v3/projects/github.com/owner/repo:packageversions   ← 404
	// deps.dev v3 is gRPC-transcoded REST, so the project_id is a
	// single path segment whose internal slashes must be percent-
	// encoded:
	//   /v3/projects/github.com%2Fowner%2Frepo:packageversions ← 200
	// Every v0.24.0 deps.dev call hit 404, the 404 branch returned
	// (nil, nil) silently, and the worker fleet produced zero
	// deps.dev rows fleet-wide while ecosyste.ms / github-* sources
	// kept working — masking the bug behind a partial-success scan.
	//
	// The cadence gate (default 180 days) means an affected repo
	// would not be re-scanned until its last_run elapsed, so we
	// override the gate once here by clearing the timestamp for
	// every scanned repo IFF the fleet has zero deps.dev rows.
	// Self-disabling: once any deps.dev row exists, the NOT EXISTS
	// guard short-circuits and subsequent migrate runs are no-ops.
	// Fresh installs with no scanned repos yet are unaffected
	// because the WHERE clause filters on distribution_last_run IS
	// NOT NULL. Operators who want to force re-scan after this
	// point use the documented manual workflow:
	//   UPDATE aveloxis_data.repos SET distribution_last_run = NULL ...
	execMigrationStep(ctx, pg, logger, &errs, "v0.24.1 reset distribution_last_run for fleets affected by v0.24.0 deps.dev URL bug",
		`UPDATE aveloxis_data.repos
		SET distribution_last_run = NULL
		WHERE distribution_last_run IS NOT NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.repo_distribution
		      WHERE source = 'deps.dev'
		  )`)

	// v0.25.0 — one-shot reset for the Julia/R/conda cohort and any
	// repo sidelined by the pre-v0.25.0 strict scanner contract.
	//
	// Two compounding issues drove this on chaoss.tv 2026-05-22/23:
	//
	//   1. The pre-v0.25.0 GitHub manifest classifier didn't recognize
	//      Project.toml (Julia), DESCRIPTION (R/CRAN/Bioconductor),
	//      meta.yaml / recipe.yaml (conda). For these ecosystems the
	//      ONLY working distribution source was ecosyste.ms; when it
	//      went into a 500-storm, every Julia/R/conda repo accumulated
	//      failures under the strict-contract "any error + zero data
	//      = failure" rule, eventually hitting the 10-strike sideline
	//      that stamps distribution_last_run = NOW() to lock the row
	//      out for 180 days.
	//
	//   2. The v0.25.0 loosened scanner contract treats empty-but-
	//      clean responses as success (not failure). Without resetting
	//      the sidelined cohort, those repos stay out of rotation
	//      until their artificial last_run elapses ~6 months later.
	//
	// Reset criterion: distribution_failed_attempts > 0. Specific
	// (only touches repos with failure history), idempotent (a
	// successful subsequent scan zeros the counter; re-running the
	// migration finds nothing to reset), and fresh-install-safe (no
	// repos have failures yet on a clean install).
	//
	// Operators who want to also re-scan repos that scanned to "zero
	// evidence" under the old contract — common for Julia/R repos
	// where ecosyste.ms was the only viable source AND they got NO
	// failure record because the scan technically succeeded — use the
	// documented manual workflow:
	//   UPDATE aveloxis_data.repos SET distribution_last_run = NULL
	//   WHERE repo_id IN (...);
	execMigrationStep(ctx, pg, logger, &errs, "v0.25.0 reset distribution state for cohort sidelined under pre-v0.25.0 strict scanner contract",
		`UPDATE aveloxis_data.repos
		SET distribution_last_run = NULL,
		    distribution_failed_attempts = 0,
		    distribution_last_failed_at = NULL
		WHERE distribution_failed_attempts > 0`)

	// v0.25.1 — drop the inherited natural-key UNIQUE constraints
	// on the two distribution history tables. The pre-v0.25.1 schema
	// declared them via `LIKE parent INCLUDING ALL`, which carried the
	// parent's UNIQUE constraints into history. History tables are
	// supposed to hold many snapshots over time per logical key, but
	// the inherited UNIQUE prevented that — the second rotation of any
	// repo tripped 23505 in MarkDistributionComplete and rolled back.
	//
	// In v0.24.0 the bug was rare (cadence = 180 days). In v0.25.0
	// distribution_scan_complete=FALSE makes partial-scan repos
	// immediately re-eligible, so the failure mode became a tight
	// dispatcher loop: 1 ERROR + 1 burned distribution scan every ~30s
	// per stuck repo. Operators saw this on 2026-05-23 with repo_id=70:
	//
	//   level=ERROR msg="distribution: mark complete failed" repo_id=70
	//   error="rotate repo_distribution_manifest to history: ERROR:
	//   duplicate key value violates unique constraint
	//   \"repo_distribution_manifest_history_repo_id_manifest_path_key\""
	//
	// PRIMARY KEY on distribution_id / manifest_id is NOT dropped —
	// the schema still declares LIKE INCLUDING ALL so the PK survives;
	// only the natural-key UNIQUE constraints by their auto-generated
	// names are dropped. IF EXISTS makes the step idempotent for
	// fresh-install DBs (where schema.sql's own DROP already fired
	// during the base DDL exec) and for re-runs after the constraints
	// are gone.
	execMigrationStep(ctx, pg, logger, &errs, "v0.25.1 drop inherited natural-key UNIQUEs on distribution history tables",
		`ALTER TABLE aveloxis_data.repo_distribution_history
		    DROP CONSTRAINT IF EXISTS repo_distribution_history_repo_id_ecosystem_package_name_so_key;
		ALTER TABLE aveloxis_data.repo_distribution_manifest_history
		    DROP CONSTRAINT IF EXISTS repo_distribution_manifest_history_repo_id_manifest_path_key;`)

	// v0.25.3 — repair distribution_last_run for repos whose
	// v0.25.0/v0.25.1-window scans hit the 23505 rotation bug.
	// Every MarkDistributionComplete rolled back, so the function's
	// final `distribution_last_run = NOW()` stamp never landed. The
	// work was done (data was gathered, rows are in
	// repo_distribution / repo_distribution_manifest from earlier
	// successful v0.24.x scans), but the most-recent attempt's
	// commit was discarded.
	//
	// Post-v0.25.1 deploy without this repair, the worker treats
	// these repos as "never scanned" (NULL distribution_last_run
	// is the first WHERE-clause branch of the claim query) and
	// re-runs all the scans — burning API budget on work whose
	// results are already in the DB.
	//
	// The repair: stamp distribution_last_run to MAX(data_collection_date)
	// across the existing rows in both distribution tables.
	// Reflects when the data was *actually* gathered (typically
	// the v0.24.x scan months ago, before the rotation bug),
	// NOT NOW() which would falsely promise "fresh as of today"
	// for stale data and prevent natural re-scan when cadence
	// genuinely elapses.
	//
	// Repos with zero rows in either table (genuinely never had a
	// successful scan) stay NULL and get scanned on first dispatch
	// after deploy. The `WHERE distribution_last_run IS NULL`
	// guard makes the step self-disabling: once stamped, the row
	// no longer matches the WHERE and subsequent migrate runs are
	// no-ops.
	//
	// Inner UNION ALL + outer GROUP BY repo_id: each table
	// contributes its own MAX, then the outer aggregate picks the
	// later of the two. The shape matters — a flat MAX across the
	// union would still pick the correct max but loses the
	// per-table-per-repo grouping intermediate.
	//
	// v0.25.x-era escape hatch. Documented for deprecation when
	// v0.24.x support ends (target 2027). See
	// docs/architecture/distribution.md §12.
	execMigrationStep(ctx, pg, logger, &errs, "v0.25.3 repair distribution_last_run for cohort whose v0.25.0/v0.25.1 rotations rolled back",
		`UPDATE aveloxis_data.repos r
		SET distribution_last_run = sub.last_observed
		FROM (
		    SELECT repo_id, MAX(observed) AS last_observed
		    FROM (
		        SELECT repo_id, MAX(data_collection_date) AS observed
		        FROM aveloxis_data.repo_distribution
		        GROUP BY repo_id
		        UNION ALL
		        SELECT repo_id, MAX(data_collection_date) AS observed
		        FROM aveloxis_data.repo_distribution_manifest
		        GROUP BY repo_id
		    ) inner_sub
		    GROUP BY repo_id
		) sub
		WHERE r.repo_id = sub.repo_id
		  AND r.distribution_last_run IS NULL`)

	// collection_queue.last_commits backfill (v0.19.11). Pre-v0.19.11
	// the FacadeCollector incremented result.Commits once per inserted
	// ROW rather than once per distinct commit. Since the commits table
	// stores one row per file per commit, the cached last_commits value
	// on collection_queue ended up inflated by the average
	// files-per-commit (typically 5×–50×). That bogus value flowed
	// into GetRepoStatsBatch and from there to every dashboard.
	// v0.19.11 fixes the increment at the source AND runs this one-time
	// backfill so existing rows pick up the correct count without
	// waiting for natural re-collection.
	//
	// `WHERE last_commits IS DISTINCT FROM sub.cnt` ensures the UPDATE
	// only touches mismatched rows — after the first run completes,
	// subsequent migrate runs are effectively no-ops. The
	// COUNT(DISTINCT) subquery itself is the cost; on a 100K-repo
	// fleet with hundreds of millions of commit rows this takes a few
	// minutes once, then never matters again.
	execMigrationStep(ctx, pg, logger, &errs, "backfill collection_queue.last_commits with distinct counts",
		`UPDATE aveloxis_ops.collection_queue q
		SET last_commits = sub.cnt
		FROM (
		    SELECT repo_id, COUNT(DISTINCT cmt_commit_hash) AS cnt
		    FROM aveloxis_data.commits
		    GROUP BY repo_id
		) sub
		WHERE q.repo_id = sub.repo_id
		  AND q.last_commits IS DISTINCT FROM sub.cnt`)

	// v0.20.5 backfill: set force_full_collect=TRUE on queue rows whose
	// last_prs is materially below the latest repo_info.pr_count. These
	// are repos affected by the pre-v0.20.5 gap-fill silent-failure bug:
	// gap fill aborted with a transient GitHub error, the WARN was
	// logged, the error never reached outcome.errMsg, force_full_collect
	// stayed FALSE, and the repo went back into the incremental cycle
	// where it would never close the historical PR gap. The 5% threshold
	// matches the live gap detector in gap_fill.go (PRGapPctThreshold).
	//
	// Idempotent in two ways:
	//   1. Subsequent migrate runs skip rows whose flag is already TRUE.
	//   2. After a successful full re-collection, CompleteJob clears
	//      force_full_collect AND last_prs catches up to pr_count, so
	//      neither filter matches on the next migrate.
	//
	// DISTINCT ON (repo_id) ... ORDER BY repo_id, data_collection_date
	// DESC picks the latest repo_info row per repo — the previous
	// snapshot rotated to repo_info_history so the live table may hold
	// just one row per repo on a steady-state install, but the
	// DISTINCT ON is correct either way.
	execMigrationStep(ctx, pg, logger, &errs, "v0.20.5 backfill force_full_collect for repos with PR gap",
		`UPDATE aveloxis_ops.collection_queue q
		SET force_full_collect = TRUE
		FROM (
		    SELECT DISTINCT ON (repo_id) repo_id, pr_count
		    FROM aveloxis_data.repo_info
		    ORDER BY repo_id, data_collection_date DESC
		) ri
		WHERE q.repo_id = ri.repo_id
		  AND ri.pr_count > 0
		  AND q.last_prs::float / ri.pr_count::float < 0.95
		  AND q.force_full_collect = FALSE`)

	// v0.20.12 (Fix I): gh_state column mirrors gl_state (added in
	// v0.20.3). Used to mark placeholder contributor rows that were
	// inserted from commit metadata when the API returned 404 for
	// the login. Value 'unresolved' means "we observed this login
	// in commit data but couldn't get a profile from the API at
	// observation time" — NOT a claim about GitHub's internal state
	// (the login may have been deleted, suspended, or renamed; we
	// can't tell the cases apart from a single 404).
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "gh_state", "TEXT DEFAULT ''")

	// v0.20.12 backfill placeholder contributors for unresolvable logins
	// in commits. Closes the 22,499-commit gap observed on live
	// aveloxis_large: commits where cmt_author_platform_username is
	// set but no matching contributor row exists (case-insensitively)
	// today. Without this backfill, BackfillCommitAuthorIDs has
	// nothing to JOIN against and the commits stay perpetually
	// unresolved. The placeholder row uses cntrb_id =
	// uuid_generate_v4() (random — we don't have a GitHub user_id
	// to derive a deterministic UUID, per the v0.20.2 R1 rule), sets
	// cntrb_login = gh_login = the observed username (case preserved
	// from commits), and flags gh_state = 'unresolved' so analysts
	// can filter these out of contributor counts.
	//
	// Idempotent: the NOT EXISTS filter is case-insensitive, so on
	// subsequent runs after Fix H has matched commits to the new
	// placeholders, every distinct unresolvable username already has
	// a contributor row and the filter excludes them all.
	//
	// Composes with Fix H: this migration runs BEFORE the next
	// scheduler cycle calls BackfillCommitAuthorIDs, so the new
	// placeholder rows are available when the case-insensitive JOIN
	// runs.
	// tool_version is omitted from the column list — the per-table
	// DEFAULT set by setToolVersionDefaults fills it in. Same for
	// the other tool_version-bearing tables.
	execMigrationStep(ctx, pg, logger, &errs, "v0.20.12 backfill placeholder contributors for unresolvable logins",
		`INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, gh_login, gh_state,
			 tool_source, data_source, data_collection_date)
		SELECT
			gen_random_uuid(),
			MIN(cmt_author_platform_username),
			MIN(cmt_author_platform_username),
			'unresolved',
			'aveloxis-placeholder-backfill',
			'commits.cmt_author_platform_username',
			NOW()
		FROM aveloxis_data.commits c
		WHERE c.cmt_author_platform_username IS NOT NULL
		  AND c.cmt_author_platform_username != ''
		  AND c.cmt_ght_author_id IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.contributors cn
		      WHERE LOWER(cn.gh_login) = LOWER(c.cmt_author_platform_username)
		  )
		GROUP BY LOWER(c.cmt_author_platform_username)
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`,
	)

	// v0.20.7 backfill: clear last_error and force_full_collect for
	// rows that were wrongly flagged by the pre-v0.20.7 "no data
	// collected" heuristic. Affected repos have last_commits > 0
	// (proves facade succeeded) AND last_error contains the
	// specific "no data collected" string the heuristic emitted.
	// Idempotent — once last_error is cleared, the row no longer
	// matches the WHERE clause.
	//
	// We DON'T need to flip success state — the queue's notion of
	// success is implicit (last_error IS NULL). Clearing the error
	// AND any inappropriately-set force_full_collect lets the next
	// cycle run incrementally, which is the correct cadence for a
	// healthy small repo with no API activity.
	execMigrationStep(ctx, pg, logger, &errs, "v0.20.7 clear false-positive 'no data collected' errors for repos with real commits",
		`UPDATE aveloxis_ops.collection_queue
		SET last_error = NULL,
		    force_full_collect = FALSE
		WHERE last_commits > 0
		  AND last_error LIKE 'no data collected%'`)

	// v0.21.0 backfill scancode_last_run from aveloxis_scan.scancode_scans.
	//
	// Pre-v0.21.0 scancode results were tracked exclusively via the
	// aveloxis_scan.scancode_scans rows (one per scan run, with a
	// data_collection_date timestamp — the aveloxis-wide convention
	// for "when did we record this"). The v0.21.0 ScancodeWorker
	// reads its cadence gate off aveloxis_data.repos.scancode_last_run
	// instead; this backfill seeds that column from the existing
	// scancode_scans history so repos already scanned in the past 6
	// months don't all re-scan on first v0.21.0 startup. Repos with
	// no prior scan stay at NULL and sort to the front of the
	// worker's claim queue naturally.
	//
	// The ARRAY_AGG(... ORDER BY data_collection_date DESC)[1] picks
	// the scancode_version from the most-recent scan in case
	// different versions ran historically.
	//
	// Idempotent: WHERE r.scancode_last_run IS NULL on the outer
	// UPDATE means a second migrate run is a no-op once backfill
	// completes. Wrapped in execMigrationStep per the v0.19.4
	// fail-closed contract.
	execMigrationStep(ctx, pg, logger, &errs, "v0.21.0 backfill scancode_last_run from aveloxis_scan.scancode_scans",
		`UPDATE aveloxis_data.repos r
		SET scancode_last_run = sub.last_at,
		    scancode_version = sub.last_version
		FROM (
		    SELECT repo_id,
		           MAX(data_collection_date) AS last_at,
		           (ARRAY_AGG(scancode_version ORDER BY data_collection_date DESC))[1] AS last_version
		    FROM aveloxis_scan.scancode_scans
		    GROUP BY repo_id
		) sub
		WHERE r.repo_id = sub.repo_id
		  AND r.scancode_last_run IS NULL`)

	// v0.21.2 backfill collection_queue.last_issues with cumulative counts.
	//
	// Pre-v0.21.2 the column was the since-filtered per-cycle delta
	// from the most recent collection. v0.21.2 changes CompleteJob
	// to write the cumulative count instead (mirroring v0.19.11's
	// commit-count treatment). This one-shot backfill brings
	// existing rows into the new world without waiting for each
	// repo's next collection cycle — operators on a 40K-repo fleet
	// would otherwise see the misleading "Gathered: 0 / Meta: N"
	// reads on the dashboard for days while the queue rolls.
	//
	// Idempotent in two ways:
	//   1. `IS DISTINCT FROM` filter means once a row's
	//      last_issues matches the cumulative count, subsequent
	//      migrate runs skip it.
	//   2. On a fresh deploy (no historical issues rows), the
	//      subquery returns 0 and the UPDATE skips rows that are
	//      already 0.
	//
	// The COUNT(*) is fast because idx_issues_repo_id exists; the
	// outer subquery scans the full collection_queue but joins
	// each row with a single indexed lookup. On a 40K-repo / 5M-
	// row issues fleet this is a few seconds.
	execMigrationStep(ctx, pg, logger, &errs, "v0.21.2 backfill collection_queue.last_issues with cumulative counts",
		`UPDATE aveloxis_ops.collection_queue q
		SET last_issues = sub.cnt
		FROM (
		    SELECT repo_id, COUNT(*) AS cnt
		    FROM aveloxis_data.issues
		    GROUP BY repo_id
		) sub
		WHERE q.repo_id = sub.repo_id
		  AND q.last_issues IS DISTINCT FROM sub.cnt`)

	// v0.21.2 backfill collection_queue.last_prs with cumulative counts.
	// See last_issues backfill above for the rationale; same shape
	// against aveloxis_data.pull_requests via idx_pull_requests_repo_id.
	execMigrationStep(ctx, pg, logger, &errs, "v0.21.2 backfill collection_queue.last_prs with cumulative counts",
		`UPDATE aveloxis_ops.collection_queue q
		SET last_prs = sub.cnt
		FROM (
		    SELECT repo_id, COUNT(*) AS cnt
		    FROM aveloxis_data.pull_requests
		    GROUP BY repo_id
		) sub
		WHERE q.repo_id = sub.repo_id
		  AND q.last_prs IS DISTINCT FROM sub.cnt`)

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

	// v0.20.4 email-verification: email_pending column + tokens table.
	// Manual-entry emails at /account/email are written to email_pending
	// (not directly to users.email) so they can be confirmed via
	// click-through before becoming canonical. OAuth-callback emails
	// (already provider-verified) bypass this flow.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.users", "email_pending", "TEXT")
	execMigrationStep(ctx, pg, logger, &errs, "create email_confirmations table",
		`CREATE TABLE IF NOT EXISTS aveloxis_ops.email_confirmations (
			token TEXT PRIMARY KEY,
			user_id INT NOT NULL REFERENCES aveloxis_ops.users(user_id) ON DELETE CASCADE,
			email TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL
		)`)
	execMigrationStep(ctx, pg, logger, &errs, "create idx_email_confirmations_user",
		`CREATE INDEX IF NOT EXISTS idx_email_confirmations_user
		 ON aveloxis_ops.email_confirmations (user_id)`)
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "status", "TEXT NOT NULL DEFAULT 'approved'")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "approved_by", "INT")
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_ops.user_groups", "approved_at", "TIMESTAMPTZ")
	// Existing rows from pre-v0.19.0 deployments default to
	// 'approved' so the upgrade doesn't suddenly hide groups that
	// already exist. New rows from non-admins go to 'pending' via
	// CreateUserGroup's branch.

	// v0.22.1: ensure every FK pointing at contributors(cntrb_id)
	// has ON UPDATE CASCADE. Required for v0.22.2's
	// `aveloxis migrate-cntrb-ids` data migration to work — that
	// command UPDATEs random-UUID cntrb_id values to deterministic
	// PlatformUUID form, and Postgres cascades the change to all
	// child FK columns automatically when CASCADE is declared. The
	// pre-v0.22.1 default of NO ACTION would reject any such
	// UPDATE the moment a child row references the old cntrb_id.
	//
	// Idempotent via ensureOnUpdateCascadeOnCntrbIDFKs which
	// checks information_schema.referential_constraints first;
	// already-CASCADE constraints are skipped. ADD CONSTRAINT
	// uses NOT VALID + VALIDATE so the validation scan takes
	// SHARE UPDATE EXCLUSIVE (concurrent reads + writes permitted)
	// instead of ACCESS EXCLUSIVE for the full validation window
	// — important on production where messages and
	// pull_request_commits routinely exceed 50M rows.
	ensureOnUpdateCascadeOnCntrbIDFKs(ctx, pg, logger, &errs)

	// v0.22.6: btree indexes on every unindexed FK column pointing
	// at aveloxis_data.contributors(cntrb_id). Cascade (v0.22.1)
	// adds the BEHAVIOR; these indexes make the behavior TRACTABLE.
	// Without them, the cascade fan-out triggered by
	// `aveloxis migrate-cntrb-ids` seq-scans every child table for
	// every cntrb_id being rewritten — observed on the production
	// aveloxis_large DB as a 17-hour stall on batch 1 (0 rows
	// committed) on 2026-05-17 because 15 of 16 cntrb_id child FKs
	// were unindexed.
	//
	// CONCURRENTLY so the build doesn't block running collection
	// workers' INSERTs on the child tables. Build duration varies
	// by table size; on production, messages and
	// pull_request_commits dominate the wall clock.
	ensureCntrbIDFKIndexes(ctx, pg, logger, &errs)

	// v0.22.7: btree indexes on the 50 child FK columns identified
	// by the 2026-05-17 schema audit. Covers all unindexed children
	// of pull_requests, issues, messages, pull_request_reviews, and
	// 30 unindexed children of repos. Built CONCURRENTLY so
	// production keeps accepting writes during the build, which is
	// hours on a fleet-scale DB (messages and pull_request_commits
	// dominate). Must run BEFORE ensureExtraFKConstraints below so
	// the new RESTRICT/CASCADE checks run against indexed lookups
	// from minute one.
	ensureExtraFKIndexes(ctx, pg, logger, &errs)

	// v0.22.7: apply ON UPDATE CASCADE ON DELETE RESTRICT
	// DEFERRABLE INITIALLY DEFERRED to the same 50 FKs. RESTRICT
	// prevents orphaned-child rows by blocking parent DELETEs;
	// DEFERRABLE INITIALLY DEFERRED lets transactions insert
	// children before parents within a single TX (per-operator
	// design choice on 2026-05-17 for insertion-order robustness).
	// DROP+ADD NOT VALID then VALIDATE so the SHARE UPDATE
	// EXCLUSIVE validation scan permits concurrent reads/writes.
	ensureExtraFKConstraints(ctx, pg, logger, &errs)

	// v0.22.7: bring the 16 v0.22.1 cntrb_id FKs to the same full
	// behavior. They previously had only ON UPDATE CASCADE;
	// v0.22.7 adds ON DELETE RESTRICT and DEFERRABLE INITIALLY
	// DEFERRED for consistency across all v0.22.7-touched FKs.
	// Idempotent on fresh installs where schema.sql declared the
	// full clause inline.
	ensureCntrbIDFKsFullBehavior(ctx, pg, logger, &errs)

	// v0.22.7: catch-all DEFERRABLE flip for any aveloxis FK still
	// non-deferrable. Covers the ~39 small-parent FKs (platforms,
	// users, repo_groups, libraries, lstm_anomaly_models, the
	// already-indexed "main" repos children like issues.repo_id,
	// pull_requests.repo_id, etc.) that aren't in scope for
	// CASCADE/RESTRICT but should still be DEFERRABLE for the
	// consistency the operator wanted. ALTER CONSTRAINT is a
	// metadata-only flip — no scan, no lock contention.
	// Self-discovering via pg_constraint so future FKs added
	// without DEFERRABLE in their declaration auto-fix on next
	// migrate.
	ensureAllFKsDeferrable(ctx, pg, logger, &errs)

	// v0.23.0: repos.languages JSONB column for the full language
	// breakdown (top entry by value is mirrored in primary_language).
	// Existing rows default to '{}'::jsonb; the staged collector and
	// the startup backfill task populate them on next FetchRepoInfo.
	addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "languages", "JSONB DEFAULT '{}'::jsonb")

	// v0.23.2: idx_staging_repo_id supports the v0.22.4 long-jobs
	// watchdog's per-repo staging COUNT(*) query. Without this index
	// the watchdog falls through to a parallel sequential scan of
	// the entire staging table (~9M rows / 112 GB on production),
	// burning ~4.5s and 64 GB of buffer reads per 30-second poll —
	// 4,754 such scans were cancelled in 5 days of production log.
	// The index is non-partial because the watchdog query has no
	// `WHERE processed` predicate the planner could match against a
	// partial index. CONCURRENTLY because the staging table can be
	// large enough that a blocking CREATE INDEX would lock writes
	// for multi-hour windows on existing fleets.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_ops", "idx_staging_repo_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_repo_id ON aveloxis_ops.staging (repo_id)`)

	// v0.25.34: FK-side indexes on email_message's projection links
	// (linked_issue_id / linked_pull_request_id / linked_pr_review_id).
	// The columns arrived in v0.25.7, after the v0.22.6/v0.22.7 FK-index
	// audits, and shipped unindexed. Their FKs are NO-ACTION DEFERRABLE:
	// bulk deletes of issues/PRs/reviews (`aveloxis dedup-repos`) queue
	// one deferred check per deleted parent row and run them AT COMMIT —
	// each check a sequential scan of email_message without these.
	// Observed on the 2026-07-08 production dedup run: 18+ minutes inside
	// a single `commit` statement. Partial (IS NOT NULL) — the RI check
	// predicate implies IS NOT NULL, and most emails carry no link.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_email_message_linked_issue",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_email_message_linked_issue
		ON aveloxis_data.email_message (linked_issue_id) WHERE linked_issue_id IS NOT NULL`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_email_message_linked_pr",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_email_message_linked_pr
		ON aveloxis_data.email_message (linked_pull_request_id) WHERE linked_pull_request_id IS NOT NULL`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_email_message_linked_review",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_email_message_linked_review
		ON aveloxis_data.email_message (linked_pr_review_id) WHERE linked_pr_review_id IS NOT NULL`)

	// v0.23.0: contributor_login_history table + backfill. Closes the
	// rename-audit gap documented as a v0.22.13 limitation
	// ("Intermediate login history is NOT stored"). The CREATE TABLE
	// in schema.sql handles fresh installs idempotently; the backfill
	// here populates existing installations from contributor_identities
	// (the only place that stores (cntrb_id, platform_id, login)
	// triples in pre-v0.23.0 data) plus the historical cntrb_login on
	// every contributor (which differs from any current identity.login
	// when a rename happened before v0.23.0 shipped).
	execMigrationStep(ctx, pg, logger, &errs, "v0.23.0 create contributor_login_history table",
		`CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_login_history (
			history_id   BIGSERIAL PRIMARY KEY,
			cntrb_id     UUID NOT NULL REFERENCES aveloxis_data.contributors(cntrb_id) ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
			platform_id  SMALLINT NOT NULL REFERENCES aveloxis_data.platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
			login        TEXT NOT NULL,
			first_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			source       TEXT NOT NULL DEFAULT 'observation',
			tool_source  TEXT NOT NULL DEFAULT 'aveloxis',
			tool_version TEXT NOT NULL DEFAULT '',
			CONSTRAINT contributor_login_history_unique UNIQUE (cntrb_id, platform_id, login)
		)`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_clh_cntrb",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clh_cntrb ON aveloxis_data.contributor_login_history (cntrb_id)`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_clh_login",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clh_login ON aveloxis_data.contributor_login_history (login)`)

	// v0.23.0 backfill: seed from contributor_identities (the
	// current state) AND from contributors.cntrb_login per platform
	// (the historical-original observation, which differs from any
	// identity row's current login when a rename predated v0.23.0).
	// Idempotent: ON CONFLICT DO NOTHING means re-running the
	// migrate is a no-op once seeded.
	// Inline ToolVersion via fmt.Sprintf — ToolVersion is a binary
	// constant, not user input, so there's no injection vector.
	// execMigrationStep doesn't take SQL parameters; we inline the
	// literal directly into the SQL string.
	execMigrationStep(ctx, pg, logger, &errs, "v0.23.0 backfill contributor_login_history from identities",
		fmt.Sprintf(`INSERT INTO aveloxis_data.contributor_login_history
			(cntrb_id, platform_id, login, source, tool_version)
		 SELECT i.cntrb_id, i.platform_id, i.login, 'backfill', '%s'
		 FROM aveloxis_data.contributor_identities i
		 JOIN aveloxis_data.contributors c USING (cntrb_id)
		 WHERE COALESCE(i.login, '') != ''
		   AND COALESCE(c.cntrb_deleted, 0) = 0
		 ON CONFLICT (cntrb_id, platform_id, login) DO NOTHING`, ToolVersion))
	execMigrationStep(ctx, pg, logger, &errs, "v0.23.0 backfill contributor_login_history from contributors.cntrb_login",
		fmt.Sprintf(`INSERT INTO aveloxis_data.contributor_login_history
			(cntrb_id, platform_id, login, source, tool_version)
		 SELECT DISTINCT c.cntrb_id, i.platform_id, c.cntrb_login, 'backfill', '%s'
		 FROM aveloxis_data.contributors c
		 JOIN aveloxis_data.contributor_identities i USING (cntrb_id)
		 WHERE COALESCE(c.cntrb_login, '') != ''
		   AND COALESCE(c.cntrb_deleted, 0) = 0
		 ON CONFLICT (cntrb_id, platform_id, login) DO NOTHING`, ToolVersion))

	// v0.25.5 — index pair (idx_contributors_cntrb_canonical,
	// idx_contributors_canonical_eq_email) was added here to accelerate
	// the canonical_full_names CTE in the pre-v0.25.6 matview. Both
	// indexes are dropped in the v0.25.6 block below — the matview no
	// longer uses cntrb_canonical for any JOIN.
	//
	// The other two v0.25.5 indexes stay (still serve other matviews):
	//
	//   idx_commits_author_timestamp_recent
	//     Partial since 2024-01-01. Lets the 13-month time filter in
	//     explorer_contributor_recent_actions short-circuit the commit
	//     scan to an index-range-scan rather than a full 474M-row
	//     sequential scan. Refresh the predicate every 2 years
	//     (create new index, drop old) to keep it aligned.
	//
	//   idx_repo_labor_repo_id_analysis_date
	//     Serves both explorer_repo_files and explorer_repo_languages
	//     "latest scan per repo" lookups. Lets the DISTINCT ON walk
	//     the index in O(repos) instead of sorting 56M rows.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_commits_author_timestamp_recent",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_commits_author_timestamp_recent
		    ON aveloxis_data.commits (cmt_author_timestamp)
		    WHERE cmt_author_timestamp >= '2024-01-01'`)
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_repo_labor_repo_id_analysis_date",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repo_labor_repo_id_analysis_date
		    ON aveloxis_data.repo_labor (repo_id, rl_analysis_date DESC)`)

	// v0.25.6 — three one-shot operations that pair with the
	// explorer_new_contributors structural rewrite:
	//
	//   1. Backfill cntrb_canonical from contributors_aliases. The
	//      v0.25.6 commit_resolver.ensureAlias fix populates canonical
	//      on every NEW alias insert going forward, but historical
	//      rows with an alias but no canonical (created via
	//      resolution strategies 2 + 4 pre-v0.25.6) stay empty
	//      without this. Picks MIN(alias_email) per cntrb_id for
	//      determinism. COALESCE on cntrb_canonical = '' so we
	//      never overwrite an existing real canonical. Soft-deleted
	//      contributors are skipped per the v0.20.2 contract.
	//
	//   2. Backfill cmt_author_platform_username from
	//      cmt_ght_author_id where the UUID is populated but the
	//      login text isn't. 91.95% of commits have cmt_ght_author_id
	//      but only 77.58% have the login text — the asymmetry comes
	//      from API paths that stamp the UUID without updating the
	//      login on every matching commit row (noreply email parsing,
	//      Commits API resolution, email-based backfill). This step
	//      brings login coverage up to ~92% as well, which makes
	//      legacy queries that JOIN on platform_username work
	//      correctly without code changes.
	//
	//   3. Drop idx_contributors_cntrb_canonical and
	//      idx_contributors_canonical_eq_email. Added in v0.25.5 to
	//      accelerate the canonical_full_names CTE in
	//      explorer_new_contributors; obsolete in v0.25.6 because
	//      the CTE is gone. Keeping them would cost write
	//      amplification on every contributors INSERT/UPDATE with
	//      zero read-side benefit.
	execMigrationStep(ctx, pg, logger, &errs,
		"v0.25.6 backfill cntrb_canonical from contributors_aliases",
		`UPDATE aveloxis_data.contributors c
		    SET cntrb_canonical = sub.alias_email
		   FROM (
		       SELECT cntrb_id, MIN(alias_email) AS alias_email
		         FROM aveloxis_data.contributors_aliases
		        WHERE COALESCE(alias_email, '') != ''
		        GROUP BY cntrb_id
		   ) sub
		  WHERE c.cntrb_id = sub.cntrb_id
		    AND COALESCE(c.cntrb_canonical, '') = ''
		    AND COALESCE(c.cntrb_deleted, 0) = 0`)

	execMigrationStep(ctx, pg, logger, &errs,
		"v0.25.6 backfill cmt_author_platform_username from cmt_ght_author_id",
		`UPDATE aveloxis_data.commits co
		    SET cmt_author_platform_username = c.gh_login
		   FROM aveloxis_data.contributors c
		  WHERE c.cntrb_id = co.cmt_ght_author_id
		    AND COALESCE(co.cmt_author_platform_username, '') = ''
		    AND COALESCE(c.gh_login, '') != ''
		    AND COALESCE(c.cntrb_deleted, 0) = 0`)

	execMigrationStep(ctx, pg, logger, &errs,
		"v0.25.6 drop idx_contributors_cntrb_canonical (obsoleted by matview rewrite)",
		`DROP INDEX IF EXISTS aveloxis_data.idx_contributors_cntrb_canonical`)

	execMigrationStep(ctx, pg, logger, &errs,
		"v0.25.6 drop idx_contributors_canonical_eq_email (obsoleted by matview rewrite)",
		`DROP INDEX IF EXISTS aveloxis_data.idx_contributors_canonical_eq_email`)

	// v0.26.4 — backfill the two SQL-derivable GraphQL parity columns.
	//
	// The GraphQL collection path never populated pr_diff_url or
	// meta_label (surfaced by the 2026-07-09 data-test column-fill
	// diff; dark on production too). The forward fix synthesizes both
	// at collection time, but incremental cycles are since-filtered —
	// only items UPDATED after the fix get re-upserted, so historical
	// rows would stay dark forever without this backfill. Both values
	// are pure derivations of data already in the DB (no API calls):
	// diff_url = html_url + ".diff"; meta_label = "owner:branch" from
	// the paired pull_request_repo row. The other two parity columns
	// (pr_cmt_node_id, issue_assignees.platform_node_id) need API
	// values and heal only via full re-collection.
	//
	// Idempotent: the COALESCE(col, '') = '' predicates stop matching
	// once filled.
	execMigrationStep(ctx, pg, logger, &errs,
		"v0.26.4 backfill pull_requests.pr_diff_url from pr_html_url",
		`UPDATE aveloxis_data.pull_requests
		 SET pr_diff_url = pr_html_url || '.diff'
		 WHERE COALESCE(pr_diff_url, '') = ''
		   AND COALESCE(pr_html_url, '') <> ''`)

	execMigrationStep(ctx, pg, logger, &errs,
		"v0.26.4 backfill pull_request_meta.meta_label from pull_request_repo",
		`UPDATE aveloxis_data.pull_request_meta m
		 SET meta_label = split_part(r.pr_repo_full_name, '/', 1) || ':' || m.meta_ref
		 FROM aveloxis_data.pull_request_repo r
		 WHERE r.pr_repo_meta_id = m.pr_meta_id
		   AND r.pr_repo_head_or_base = m.head_or_base
		   AND COALESCE(m.meta_label, '') = ''
		   AND COALESCE(m.meta_ref, '') <> ''
		   AND split_part(COALESCE(r.pr_repo_full_name, ''), '/', 1) <> ''`)

	// v0.25.8 — index on commits.cmt_ght_author_id.
	//
	// cmt_ght_author_id is a plain UUID column with no REFERENCES clause
	// in schema.sql, so it was invisible to the v0.22.6 FK-index audit
	// which queried pg_constraint for formal FK child columns. Every
	// index-building pass since v0.22.6 built on that list, leaving
	// this column unindexed on the 474M-row commits table.
	//
	// v0.25.6 switched the explorer_new_contributors commit branch to
	// join contributors ON c.cntrb_id = co.cmt_ght_author_id. Without
	// an index on the commits side, the planner seqscans 381 GB and
	// hash-aggregates ~434M rows before touching contributors — observed
	// as a 2+ day stalled rebuild on aveloxis_large (2026-06-02).
	//
	// Partial predicate mirrors the WHERE clause in the matview's commit
	// branch: only the ~92% of commit rows with a resolved author UUID
	// are included, keeping the index ~8% smaller than a full index.
	execCreateIndexConcurrently(ctx, pg, logger, &errs, "aveloxis_data", "idx_commits_cmt_ght_author_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_commits_cmt_ght_author_id
		 ON aveloxis_data.commits (cmt_ght_author_id)
		 WHERE cmt_ght_author_id IS NOT NULL`)

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

	if len(errs) > 0 {
		// Fail closed: surface every collected error so the operator
		// sees the FULL list and can fix them all before retrying.
		// errors.Join produces a multi-line error string by default,
		// which prints cleanly to stderr from cobra.
		//
		// stampSchemaVersion is intentionally NOT called here. A partial
		// migration must not stamp the schema as up-to-date — that would
		// cause CheckSchemaVersion (used by `aveloxis web` and `aveloxis
		// api` at startup) to suppress its "schema behind binary" warning,
		// hiding the incomplete migration from the operator. Pre-v0.25.8
		// stampSchemaVersion was called unconditionally before this check,
		// which is how a lock-blocked addColumnIfMissing could result in a
		// partial schema stamped as complete (observed 2026-06-02 on
		// aveloxis_large when the ALTER TABLE on contributors waited 1+
		// day for a lock held by the running matview refresh).
		logger.Error("schema migrations completed with errors — aveloxis serve will refuse to start until these are resolved",
			"count", len(errs))
		return fmt.Errorf("schema migration had %d error(s):\n%w", len(errs), errors.Join(errs...))
	}

	// Stamp schema version so non-migrating commands (web, api) can detect
	// when the schema is behind the binary and warn the operator.
	// Only reached when ALL migration steps succeeded — see comment above.
	stampSchemaVersion(ctx, pg, logger)

	logger.Info("schema migrations complete", "schema_version", ToolVersion)
	return nil
}

// MigrateAdvisoryLockID is the postgres advisory-lock id used by
// RunMigrations to coordinate with concurrent migrate processes (and
// `aveloxis serve`'s startup migrate). Stable constant — chosen once
// in v0.20.1, must not change across versions or different aveloxis
// instances pointing at the same DB will fail to coordinate.
//
// Value chosen as ASCII "AVELOXIS" packed into 64 bits (just memorable;
// any stable int64 would do).
const MigrateAdvisoryLockID int64 = 0x4156454C4F584953

// execCreateIndexConcurrently wraps a CREATE INDEX CONCURRENTLY
// statement with self-healing INVALID-index cleanup. CONCURRENTLY's
// failure mode (interrupt mid-build, network blip, OOM) leaves an
// INVALID index — `CREATE INDEX CONCURRENTLY IF NOT EXISTS` then
// fails with "relation already exists" forever. This helper detects
// the INVALID index via pg_index.indisvalid = false and DROPs it
// before retrying the create, so operators don't have to manually
// intervene.
//
// The schema and indexName are passed separately (rather than parsing
// from the SQL) because the helper needs them for the indisvalid
// query.
// ginTrgmOpsSchema returns the name of the schema that contains the
// gin_trgm_ops operator class (for access method gin), or "" if no such
// opclass exists in any schema. The idx_repos_owner_name_trgm DDL
// schema-qualifies the operator class with this value so it resolves
// regardless of the session search_path — pg_trgm's objects frequently
// live in a schema (e.g. public) that isn't on the connecting role's
// path (observed on kate 2026-06-13, SQLSTATE 42704). Returns "" on
// query error or when the opclass is absent (extension not actually
// installed), which the caller treats as warn-and-skip.
func ginTrgmOpsSchema(ctx context.Context, pg *PostgresStore) string {
	var schema string
	err := pg.pool.QueryRow(ctx, `
		SELECT n.nspname
		FROM pg_opclass oc
		JOIN pg_am am ON am.oid = oc.opcmethod
		JOIN pg_namespace n ON n.oid = oc.opcnamespace
		WHERE oc.opcname = 'gin_trgm_ops'
		  AND am.amname = 'gin'
		LIMIT 1`).Scan(&schema)
	if err != nil {
		return ""
	}
	return schema
}

// quoteIdent wraps a PostgreSQL identifier in double quotes, escaping any
// embedded double quotes, so it is safe to interpolate into DDL. Used to
// qualify the gin_trgm_ops operator class with its discovered schema.
func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func execCreateIndexConcurrently(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error, schema, indexName, sql string) {
	var isInvalid bool
	err := pg.pool.QueryRow(ctx, `
		SELECT NOT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`,
		schema, indexName).Scan(&isInvalid)
	if err == nil && isInvalid {
		logger.Warn("dropping invalid index from prior interrupted CONCURRENT build",
			"index", schema+"."+indexName)
		if _, derr := pg.pool.Exec(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s.%s`, schema, indexName)); derr != nil {
			logger.Error("schema migration error", "step", "drop invalid "+indexName, "error", derr)
			*errs = append(*errs, fmt.Errorf("drop invalid %s: %w", indexName, derr))
			return
		}
	}
	if _, err := pg.pool.Exec(ctx, sql); err != nil {
		label := "create index " + indexName
		logger.Error("schema migration error", "step", label, "error", err)
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
	}
}

// watchBlockers periodically polls pg_stat_activity for aveloxis
// backends that are blocked on a lock and surfaces the holder PID(s)
// to the operator log with a pg_terminate_backend(N) recipe.
//
// v0.20.0 introduced this to close the silence-during-blocked-migrate
// gap from the 2026-05-08 incident: when migrate was waiting 14+
// minutes for an orphan to release a lock, the only signal was
// "aveloxis migrate" sitting quiet. The watcher now logs an actionable
// hint within ~60 seconds of the block starting.
//
// Filters on application_name LIKE 'aveloxis-%' AND wait_event_type =
// 'Lock' to scope to OUR backends only — third-party tools holding
// locks elsewhere don't trigger the warning. The first poll fires at
// 30 seconds (ignoring fast migrations) and subsequent polls every
// 60 seconds.
func watchBlockers(ctx context.Context, pg *PostgresStore, logger *slog.Logger, done <-chan struct{}) {
	first := time.NewTimer(30 * time.Second)
	defer first.Stop()
	select {
	case <-done:
		return
	case <-ctx.Done():
		return
	case <-first.C:
	}
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		checkBlockers(ctx, pg, logger)
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// checkBlockers runs one poll cycle: find blocked aveloxis backends,
// resolve their blockers via pg_blocking_pids, log holder + recipe.
func checkBlockers(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	rows, err := pg.pool.Query(ctx, `
		SELECT a.pid,
		       LEFT(a.query, 200)                  AS waiter_query,
		       pg_blocking_pids(a.pid)              AS blockers
		FROM pg_stat_activity a
		WHERE a.application_name LIKE 'aveloxis-%'
		  AND a.wait_event_type = 'Lock'
		  AND a.state = 'active'`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var waiterPid int
		var waiterQuery string
		var blockers []int32
		if err := rows.Scan(&waiterPid, &waiterQuery, &blockers); err != nil {
			continue
		}
		if len(blockers) == 0 {
			continue
		}
		logger.Warn("migration blocked on lock — investigate the holder PID(s)",
			"waiter_pid", waiterPid,
			"holder_pids", blockers,
			"waiter_query_prefix", waiterQuery,
			"hint", "if no aveloxis-side process matches a holder PID, it's an orphan; run `SELECT pg_terminate_backend(<pid>)` to release the lock")
	}
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
		// schema_meta is empty — either migrate has never run or
		// the row was deleted. Either way the binary is about to
		// query columns/tables that may not exist. v0.20.15
		// bumped this from WARN to ERROR after a production
		// incident where the WARN was missed and the next hour
		// produced repeated `column "email_pending" does not
		// exist` runtime errors.
		logger.Error("schema version unknown — `aveloxis migrate` has not run against this database. Run `aveloxis migrate --skip-views` then restart this process. Without it, queries against columns added by recent migrations (e.g. users.email_pending from v0.20.4) will fail at runtime.")
		return
	}
	if dbVersion != ToolVersion {
		// v0.20.15: ERROR not WARN. The binary is about to
		// query columns/tables that the deployed schema may
		// not have. The recovery action is in the message so
		// operators reading the log don't have to dig through
		// docs.
		logger.Error("schema version mismatch — `aveloxis migrate` is required before this process can function correctly. Run `aveloxis migrate --skip-views` then restart. Until then, queries against columns added by intervening migrations will fail at runtime (e.g. `column \"email_pending\" does not exist` was the 2026-05-13 production symptom).",
			"db_schema_version", dbVersion,
			"binary_version", ToolVersion,
			"action", "aveloxis migrate --skip-views")
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

	// Create the unique index now that duplicates are gone. v0.20.1
	// uses CONCURRENTLY so the index build doesn't ShareLock the
	// commits table while the scheduler is running. deduplicateCommits
	// itself is warn-only (this isn't through the err-collector), so
	// keep that behavior here too.
	var existsInvalid bool
	pg.pool.QueryRow(ctx, `
		SELECT NOT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'aveloxis_data' AND c.relname = 'idx_commits_repo_hash_file'`).Scan(&existsInvalid)
	if existsInvalid {
		logger.Warn("dropping invalid idx_commits_repo_hash_file from prior interrupted CONCURRENT build")
		pg.pool.Exec(ctx, `DROP INDEX IF EXISTS aveloxis_data.idx_commits_repo_hash_file`)
	}
	_, err := pg.pool.Exec(ctx, `
		CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_commits_repo_hash_file
		ON aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename)`)
	if err != nil {
		logger.Warn("failed to create commits unique index", "error", err)
	}
}

// ensureRepoGitCaseInsensitiveUnique creates the UNIQUE partial index
// uq_repos_repo_git_ci ON aveloxis_data.repos (LOWER(repo_git)) WHERE
// platform_id IN (1, 2) — the hard backstop that prevents case-variant
// duplicate repositories (GitHub/GitLab treat owner/repo paths
// case-insensitively; generic git hosts on platform 3 may legitimately
// be case-sensitive, so they are excluded and stay byte-exact-unique via
// the existing repos.repo_git UNIQUE constraint).
//
// Create-after-cleanup contract (mirrors deduplicateCommits, per the
// CLAUDE.md schema-DDL-ordering rule): the index is NOT in schema.sql
// and is only created once the fleet has ZERO case-dup groups. Unlike
// deduplicateCommits this function does NOT delete the duplicates itself
// — a duplicate repo pair carries full child-data trees whose merge is
// the operator-invoked `aveloxis dedup-repos` command's job. While
// duplicates remain, this function WARNs (naming the command) and skips.
//
// Deliberately warn-only (no *errs collector): a fleet with pending
// duplicates must still be able to start serve. The application-level
// resolution in FindRepoByURL/resolveCaseVariantURL keeps prevention
// best-effort until the index lands; the next migrate run after
// dedup-repos drains creates it.
func ensureRepoGitCaseInsensitiveUnique(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	// Fast path: a VALID index already exists — nothing to do.
	var existsValid bool
	pg.pool.QueryRow(ctx, `
		SELECT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'aveloxis_data' AND c.relname = 'uq_repos_repo_git_ci'`).Scan(&existsValid)
	if existsValid {
		return
	}

	// Gate on remaining case-variant duplicates: attempting the CREATE
	// with duplicates present would fail and leave an INVALID index.
	var dupGroups int
	if err := pg.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT 1
			FROM aveloxis_data.repos
			WHERE platform_id IN (1, 2)
			GROUP BY LOWER(repo_git)
			HAVING COUNT(*) > 1
		) dup`).Scan(&dupGroups); err != nil {
		logger.Warn("could not count case-variant duplicate repos; skipping uq_repos_repo_git_ci", "error", err)
		return
	}
	if dupGroups > 0 {
		logger.Warn("case-variant duplicate repos present; skipping unique index uq_repos_repo_git_ci",
			"duplicate_groups", dupGroups,
			"hint", "run `aveloxis dedup-repos` to merge them, then re-run `aveloxis migrate`")
		return
	}

	// Drop an INVALID leftover from a prior interrupted CONCURRENTLY
	// build, then create. Same recovery shape as deduplicateCommits.
	var existsInvalid bool
	pg.pool.QueryRow(ctx, `
		SELECT NOT i.indisvalid
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'aveloxis_data' AND c.relname = 'uq_repos_repo_git_ci'`).Scan(&existsInvalid)
	if existsInvalid {
		logger.Warn("dropping invalid uq_repos_repo_git_ci from prior interrupted CONCURRENT build")
		pg.pool.Exec(ctx, `DROP INDEX IF EXISTS aveloxis_data.uq_repos_repo_git_ci`)
	}
	if _, err := pg.pool.Exec(ctx, `
		CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_repos_repo_git_ci
		ON aveloxis_data.repos (LOWER(repo_git))
		WHERE platform_id IN (1, 2)`); err != nil {
		logger.Warn("failed to create uq_repos_repo_git_ci", "error", err)
		return
	}
	logger.Info("created case-insensitive unique index uq_repos_repo_git_ci — case-variant duplicate repos are now blocked at the database level")
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
