// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// PostgresStore implements Store using pgx connection pool.
type PostgresStore struct {
	pool             *pgxpool.Pool
	logger           *slog.Logger
	matviewOnStartup bool // whether to refresh materialized views during migration
	matviewSkip      bool // whether to skip the matview block entirely (--skip-views on migrate)
	migrateNoWait    bool // whether to fail fast on advisory-lock contention (--no-wait on migrate)
	migrateFastPath  bool // F13: skip RunMigrations entirely when the stamp matches (serve startup only)

	// backendPIDs are the server-side PIDs of THIS process's pool
	// connections (v0.28.18), maintained by the pool's AfterConnect /
	// BeforeClose hooks. The mailing-list lock liveness probe excludes them
	// from pg_stat_activity exactly — no pool counters, no client_addr —
	// so `aveloxis serve`'s own startup migrate never counts itself as a
	// running worker host. Under a transaction pooler the reported PIDs
	// are the pooler's, so exclusion silently fails toward "live" (safe).
	backendMu   sync.Mutex
	backendPIDs map[uint32]struct{}
}

func (s *PostgresStore) trackBackend(pid uint32, alive bool) {
	s.backendMu.Lock()
	defer s.backendMu.Unlock()
	if s.backendPIDs == nil {
		s.backendPIDs = map[uint32]struct{}{}
	}
	if alive {
		s.backendPIDs[pid] = struct{}{}
	} else {
		delete(s.backendPIDs, pid)
	}
}

// ownBackendPIDs snapshots this process's pool backend PIDs.
func (s *PostgresStore) ownBackendPIDs() []int32 {
	s.backendMu.Lock()
	defer s.backendMu.Unlock()
	out := make([]int32, 0, len(s.backendPIDs))
	for pid := range s.backendPIDs {
		out = append(out, int32(pid))
	}
	return out
}

// NewPostgresStore connects to PostgreSQL and returns a Store.
// Optional maxConns parameter scales the connection pool (default 20).
// For scheduler use, pass workers+15 so collection workers don't starve
// each other for database connections.
func NewPostgresStore(ctx context.Context, connString string, logger *slog.Logger, maxConns ...int32) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}
	// Install TCP keepalives via a custom DialFunc so every pooled
	// socket detects dead peers in ~2 minutes instead of the OS
	// default 2 hours. See installKeepaliveDialer for why this is
	// not done via conn-string params.
	installKeepaliveDialer(cfg)
	cfg.MaxConns = 20
	if len(maxConns) > 0 && maxConns[0] > 0 {
		cfg.MaxConns = maxConns[0]
	}
	cfg.MinConns = 2

	// Cache server-side prepared statements on each pooled
	// connection. Named statements ("stmtcache_<hash>") let repeat
	// queries skip Parse+plan on the server — the real cost on the
	// hot INSERT/SELECT paths when the DB is on a LAN rather than a
	// loopback socket.
	//
	// The correctness hazard of this mode is SQLSTATE 26000
	// "prepared statement does not exist" when a TCP connection is
	// silently replaced out from under pgx. v0.18.14 adds two
	// defenses that together keep this path safe for direct-Postgres
	// LAN deployments:
	//
	//   1. installKeepaliveDialer (prepared_stmt_retry.go) sets a
	//      custom pgconn DialFunc that builds every socket with
	//      net.KeepAliveConfig — TCP_KEEPIDLE/INTVL/CNT tuned for
	//      ~2-minute dead-peer detection instead of the OS default
	//      2 hours. pgxpool evicts the broken connection before
	//      the cache can fire many queries at a swapped backend.
	//      (Libpq-style conn-string keepalive params do NOT work —
	//      pgx v5 forwards them to Postgres as RuntimeParams and
	//      the startup fails with FATAL 42704.)
	//
	//   2. sendBatchWithRetry (prepared_stmt_retry.go) wraps
	//      pool.SendBatch to retry once on SQLSTATE 26000. The
	//      retry picks up a fresh connection from the pool and the
	//      batch succeeds. Residual races during the keepalive
	//      window become single transparent retries instead of
	//      500-row batch data loss.
	//
	// Full incident record leading to the v0.18.14 configuration:
	//
	//   - v0.18.10  QueryExecModeExec. Safe everywhere (no cache),
	//               but Parse+plan on every query dominated cost
	//               once the DB moved off loopback.
	//   - v0.18.11  Flipped to CacheStatement. Hit SQLSTATE 26000
	//               within hours — client load was stressing TCP
	//               faster than MaxConnIdleTime could defend.
	//   - v0.18.12  Retreated to CacheDescribe. Safe, but server-
	//               side Parse+plan still ran per query — most of
	//               the CacheStatement speedup never materialized.
	//   - v0.18.14  Back to CacheStatement with keepalive + retry.
	//
	// Reversion triggers: sustained 26000s surviving the retry, or
	// pgbouncer landing in front of the DB in txn/statement pooling
	// mode. In either case, swap to QueryExecModeCacheDescribe (or
	// QueryExecModeExec for absolute safety).
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

	// Cycle idle connections before network gear does. The common NAT
	// idle timeout is 5 minutes; we cycle at 4 so pgx opens a fresh
	// TCP connection rather than discover a silently-dropped one at
	// the next SendBatch. MaxConnLifetime caps total age so credentials
	// rotation / failover eventually reaches every connection.
	cfg.MaxConnIdleTime = 4 * time.Minute
	cfg.MaxConnLifetime = 1 * time.Hour

	// application_name is passed via the connection string by callers
	// (cmd/aveloxis uses cfg.Database.ConnectionStringWithAppName).
	// pgxpool.ParseConfig propagates it as a connection parameter, so
	// every backend tags itself with e.g. "aveloxis-serve" /
	// "aveloxis-web" / "aveloxis-api" — pg_stat_activity then filters
	// cleanly per process. v0.20.0 stop-verification depends on this.

	// v0.23.5: install the utf8ScrubTracer on every pooled conn. The
	// tracer mutates string and *string parameters in place inside
	// TraceQueryStart / TraceBatchStart, before pgx encodes them
	// onto the wire. Net effect: PostgreSQL's SQLSTATE 22021
	// ("invalid byte sequence for encoding UTF8") can no longer kill
	// an INSERT/UPDATE because some upstream source handed us a
	// Latin-1 author name or a contributor bio with a PNG signature.
	// See utf8_tracer.go for the full rationale + production-
	// incident history.
	cfg.ConnConfig.Tracer = utf8ScrubTracer{}

	store := &PostgresStore{logger: logger}
	cfg.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		store.trackBackend(conn.PgConn().PID(), true)
		return nil
	}
	cfg.BeforeClose = func(conn *pgx.Conn) {
		store.trackBackend(conn.PgConn().PID(), false)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	store.pool = pool
	return store, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

// PidsByAppName returns the postgres backend PIDs currently identified
// by the given application_name. Used by `aveloxis stop` post-SIGTERM
// to verify all aveloxis-component backends have disconnected before
// returning. v0.20.0.
func (s *PostgresStore) PidsByAppName(ctx context.Context, appName string) ([]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1`, appName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pids []int
	for rows.Next() {
		var pid int
		if err := rows.Scan(&pid); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids, rows.Err()
}

// SetMatviewOnStartup controls whether materialized views are refreshed during migration.
func (s *PostgresStore) SetMatviewOnStartup(enabled bool) {
	s.matviewOnStartup = enabled
}

// SetMatviewSkip controls whether the matview block in RunMigrations is
// skipped entirely. Used by `aveloxis migrate --skip-views` so an
// operator iterating on schema-error fixes doesn't pay the matview
// rebuild cost on every retry. Wins over SetMatviewOnStartup when both
// are set — skip is the stronger signal.
func (s *PostgresStore) SetMatviewSkip(skip bool) {
	s.matviewSkip = skip
}

// SetMigrateNoWait controls how RunMigrations handles advisory-lock
// contention with another in-flight migration (or serve's startup
// migrate). When false (default), the advisory-lock acquire blocks
// until the holder releases. When true, RunMigrations fails fast with
// a clear error if the lock is held. Set by `aveloxis migrate
// --no-wait`.
func (s *PostgresStore) SetMigrateNoWait(noWait bool) {
	s.migrateNoWait = noWait
}

// SetMigrateFastPath — v0.27.131, the F13 fix. When enabled and the
// schema_meta stamp equals ToolVersion, RunMigrations returns
// immediately: the stamp is written ONLY after every migration step
// succeeded, so a match proves this binary's schema is fully applied.
// Enabled by `aveloxis serve` startup ONLY — the production
// observation behind it: serve sat inside RunMigrations for 1h42m
// after a restart (one-shot keyset backfills re-walking every PK
// window to find nothing to do) with 141,799 repos queued and zero
// collection. `aveloxis migrate` NEVER enables it: the explicit
// command is the operator's full-run self-heal path (hand-edited
// schema, hand-dropped view/index). A fast-pathed startup also skips
// views.sql and matview creation — both only change with a version
// bump, which changes ToolVersion and misses the stamp.
func (s *PostgresStore) SetMigrateFastPath(enabled bool) {
	s.migrateFastPath = enabled
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	return RunMigrations(ctx, s, s.logger)
}

// maxDeadlockRetries and retry logic mirrors Augur's DatabaseSession.
const maxDeadlockRetries = 10

// withRetry executes fn, retrying on deadlock (40P01) with exponential backoff.
func (s *PostgresStore) withRetry(ctx context.Context, fn func(ctx context.Context) error) error {
	for attempt := range maxDeadlockRetries {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			wait := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
			jitter := time.Duration(rand.IntN(100)) * time.Millisecond
			s.logger.Warn("deadlock detected, retrying", "attempt", attempt+1, "wait", wait+jitter)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait + jitter):
			}
			continue
		}
		return err
	}
	return fmt.Errorf("exhausted %d deadlock retries", maxDeadlockRetries)
}

// ============================================================
// Repos
// ============================================================

// UpsertRepoGroup creates or finds a repo group by name and type.
// Returns the repo_group_id.
func (s *PostgresStore) UpsertRepoGroup(ctx context.Context, name, rgType, website string) (int64, error) {
	var id int64
	// Try to find existing group by name and type.
	err := s.pool.QueryRow(ctx,
		`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = $1 AND rg_type = $2`,
		name, rgType).Scan(&id)
	if err == nil {
		return id, nil
	}
	// Create new group. rg_name is the identity key (v0.27.17 —
	// uq_repo_groups_rg_name); on a concurrent create the arbiter
	// fires, RETURNING yields no row, and we return the existing
	// group by name (rg_type is metadata, not identity).
	err = s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name, rg_type, rg_website, rg_description)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (rg_name) DO NOTHING
		RETURNING repo_group_id`,
		name, rgType, website, fmt.Sprintf("Auto-created from %s", website),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = $1`, name).Scan(&id)
	}
	return id, err
}

func (s *PostgresStore) UpsertRepo(ctx context.Context, r *model.Repo) (int64, error) {
	// Normalize the repo slug at the write boundary so a ".git" suffix never
	// reaches the DB. API URLs built from repo_name (/repos/{owner}/{name}/...)
	// 404 when the slug has a ".git" suffix.
	r.Name = model.NormalizeRepoName(r.Name)

	// Store repo_git without trailing "/" or ".git" (v0.25.32 hardening) —
	// suffix variants would otherwise slip past both ON CONFLICT (repo_git)
	// and the case-insensitive unique index and create duplicate rows.
	r.GitURL = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(r.GitURL), "/"), ".git")

	// Case-variant resolution (v0.25.32): GitHub and GitLab treat
	// owner/repo paths case-insensitively, so a URL differing from a
	// stored row only by case is the SAME repository. Substitute the
	// stored spelling so the ON CONFLICT below updates that row instead
	// of inserting a duplicate. Lookup errors are deliberately ignored —
	// the INSERT below surfaces any real connectivity problem.
	if r.Platform == model.PlatformGitHub || r.Platform == model.PlatformGitLab {
		if stored, rerr := s.resolveCaseVariantURL(ctx, r.GitURL); rerr == nil && stored != "" {
			r.GitURL = stored
		}
	}

	// Rename/transfer dedup (v0.27.102): when the caller supplies the
	// forge's numeric repository ID (org scans decode it from the
	// listing JSON) and the URL is untracked, a forge-ID hit means the
	// repo was RENAMED or TRANSFERRED upstream — the existing row IS
	// this repository under its old URL. Heal that row's URL via the
	// established UpdateRepoURL writer (same authority class as the
	// v0.25.32 HealRepoCaseDrift carve-out: the forge itself is telling
	// us the canonical current URL for this numeric identity) instead
	// of minting a data-splitting duplicate. The 2026-08-19 audit
	// proved all 12 reconcile-repos consolidation pairs were exactly
	// this shape (dio/eaigw → dio/ai-gateway, 18F/api.data.gov →
	// GSA/api.data.gov, ...). URL-tracked rows never reach this branch
	// — the ON CONFLICT (repo_git) DO UPDATE below owns those.
	if r.PlatformID != "" && (r.Platform == model.PlatformGitHub || r.Platform == model.PlatformGitLab) {
		var urlTracked int64
		// v0.27.112 (wrongly-suppressed Copilot finding): only ErrNoRows
		// means "untracked" — a transient probe failure must not steer
		// us into the heal path on bad information.
		if perr := s.pool.QueryRow(ctx,
			`SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1`,
			r.GitURL).Scan(&urlTracked); perr != nil && !errors.Is(perr, pgx.ErrNoRows) {
			return 0, fmt.Errorf("rename-heal URL probe: %w", perr)
		}
		if urlTracked == 0 {
			// v0.27.111: a LOOKUP error is not "not found" (the v0.27.36
			// rule) — falling through to the insert on a transient DB
			// error could mint the exact rename duplicate this branch
			// prevents. Propagate; the caller retries next scan.
			// v0.27.150 (round 29): host-scoped — a same-numbered
			// project on a DIFFERENT GitLab instance is not a rename.
			existing, ferr := s.FindRepoByPlatformRepoID(ctx, r.Platform, r.PlatformID, ForgeHostOf(r.GitURL))
			if ferr != nil {
				return 0, fmt.Errorf("rename-heal forge-id lookup: %w", ferr)
			}
			if existing > 0 {
				// v0.27.106 (PR #184 review): heal via UpdateRepoURLs
				// (PLURAL) — the renamed repo's child rows (issue/PR/
				// release URLs) carry the OLD owner/repo path and must
				// heal alongside repo_git, matching the prelim rename
				// path. Needs the old URL for the find-and-replace.
				var oldURL string
				// v0.27.111: an ignored error here passed oldURL="" to
				// UpdateRepoURLs, whose repo-only fallback then skipped
				// every child-URL rewrite while reporting success.
				if serr := s.pool.QueryRow(ctx,
					`SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`,
					existing).Scan(&oldURL); serr != nil {
					return 0, fmt.Errorf("rename-heal old-URL lookup for repo %d: %w", existing, serr)
				}
				uerr := s.UpdateRepoURLs(ctx, existing, oldURL, r.GitURL)
				if uerr == nil {
					s.logger.Info("rename detected at add time — healed existing repo URL instead of creating a duplicate",
						"repo_id", existing, "old_url", oldURL, "new_url", r.GitURL, "platform_repo_id", r.PlatformID)
					return existing, nil
				}
				// ONLY a genuine uniqueness race (another writer landed
				// the new URL between our probe and the UPDATE — 23505
				// on repo_git's unique) may fall through to the INSERT
				// below, which then routes to that row via ON CONFLICT
				// (repo_git) DO UPDATE. Any OTHER failure (transient DB
				// error) must propagate — falling through would mint
				// the very duplicate this branch exists to prevent.
				var healPgErr *pgconn.PgError
				if !errors.As(uerr, &healPgErr) || healPgErr.Code != "23505" {
					return 0, fmt.Errorf("rename-heal URL update for repo %d: %w", existing, uerr)
				}
			}
		}
	}

	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		// Ensure a default repo group exists if no group is specified.
		groupID := r.GroupID
		if groupID == 0 {
			// v0.27.17: the arbiter is NAMED. The previous bare
			// ON CONFLICT had no unique to arbitrate against, so this
			// INSERT succeeded on EVERY call — production accumulated
			// 93,912 'Default' groups (one per repo), shattering every
			// repo_group_id rollup. With uq_repo_groups_rg_name in
			// place the conflict fires and the lookup below (dead code
			// until now) finally runs.
			err := s.pool.QueryRow(ctx, `
				INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
				VALUES ('Default', 'Auto-created default repo group')
				ON CONFLICT (rg_name) DO NOTHING
				RETURNING repo_group_id`).Scan(&groupID)
			if err != nil {
				// ON CONFLICT DO NOTHING returns no rows — look it up.
				_ = s.pool.QueryRow(ctx,
					`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = 'Default'`,
				).Scan(&groupID)
			}
			if groupID == 0 {
				return fmt.Errorf("failed to resolve default repo group")
			}
		}

		// Use NULL for zero timestamps — they'll be populated by FetchRepoInfo during collection.
		var createdAt, updatedAt any
		if !r.CreatedAt.IsZero() {
			createdAt = r.CreatedAt
		}
		if !r.UpdatedAt.IsZero() {
			updatedAt = r.UpdatedAt
		}

		insert := func() error {
			return s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos
				(repo_group_id, platform_id, repo_git, repo_name, repo_owner,
				 repo_description, primary_language, forked_from, repo_archived,
				 platform_repo_id, created_at, updated_at, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (repo_git) DO UPDATE SET
				repo_name = EXCLUDED.repo_name,
				repo_owner = EXCLUDED.repo_owner,
				repo_description = EXCLUDED.repo_description,
				primary_language = EXCLUDED.primary_language,
				-- v0.27.78: prefer-nonempty — Phase 0 (UpdateRepoMetadata) is
				-- the authoritative forked_from writer; UpsertRepo callers
				-- (org scans, add-repo) carry a zero-valued model.Repo and a
				-- bare EXCLUDED overwrite wiped the captured value on every
				-- scan tick. Phase 0 still clears it when a repo un-forks.
				forked_from = COALESCE(NULLIF(EXCLUDED.forked_from, ''), repos.forked_from),
				-- v0.27.116/117: FILL-EMPTY-ONLY (prefer the STORED value) —
				-- the forge numeric ID never changes for a given repo, so a
				-- DIFFERENT incoming ID means an upstream delete-and-recreate
				-- under the same URL, and overwriting the stored ID would
				-- destroy the mismatch signal SetPlatformRepoIDIfEmpty and
				-- Phase 0 (UpdateRepoMetadata) now surface. An id-less
				-- re-upsert still can't wipe a captured value, and the first
				-- observed ID still fills an empty column.
				platform_repo_id = COALESCE(NULLIF(repos.platform_repo_id, ''), EXCLUDED.platform_repo_id),
				repo_archived = EXCLUDED.repo_archived,
				-- v0.27.122 (Copilot round 14, suppressed): GREATEST, not
				-- prefer-incoming — overlapping refreshes can finish out of
				-- order, and an older forge response landing last must not
				-- regress the forge's last-update time (it never decreases).
				updated_at = GREATEST(COALESCE(EXCLUDED.updated_at, repos.updated_at), COALESCE(repos.updated_at, EXCLUDED.updated_at)),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()
			RETURNING repo_id`,
				groupID, int16(r.Platform), r.GitURL, r.Name, r.Owner,
				r.Description, r.PrimaryLanguage, r.ForkedFrom, r.Archived,
				r.PlatformID, createdAt, updatedAt, r.Platform.String()+" API",
			).Scan(&id)
		}

		err := insert()
		// Unique-index race (v0.25.32): the pre-insert case resolution and
		// the INSERT are not atomic. When uq_repos_repo_git_ci exists and a
		// concurrent writer lands the other case variant between our resolve
		// and our INSERT, the partial unique index rejects us with 23505 —
		// re-resolve to the now-stored spelling and retry once, which routes
		// the statement through ON CONFLICT (repo_git) DO UPDATE instead.
		var pgErr *pgconn.PgError
		if err != nil && errors.As(err, &pgErr) &&
			pgErr.Code == "23505" && pgErr.ConstraintName == "uq_repos_repo_git_ci" {
			if stored, rerr := s.resolveCaseVariantURL(ctx, r.GitURL); rerr == nil && stored != "" {
				r.GitURL = stored
				err = insert()
			}
		}
		return err
	})
	return id, err
}

// GetRepoByID looks up a repo by its database ID.
func (s *PostgresStore) GetRepoByID(ctx context.Context, repoID int64) (*model.Repo, error) {
	r := &model.Repo{ID: repoID}
	var platID int16
	err := s.pool.QueryRow(ctx, `
		SELECT platform_id, repo_git, repo_name, repo_owner
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID,
	).Scan(&platID, &r.GitURL, &r.Name, &r.Owner)
	if err != nil {
		return nil, err
	}
	r.Platform = model.Platform(platID)
	return r, nil
}

// OrgGroup represents a repo group that tracks a GitHub org or GitLab group.
type OrgGroup struct {
	ID      int64
	Name    string // org/group name
	Type    string // "github_org" or "gitlab_group"
	Website string // original URL
}

// GetOrgRepoGroups returns all repo groups that represent GitHub orgs or GitLab groups.
func (s *PostgresStore) GetOrgRepoGroups(ctx context.Context) ([]OrgGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_group_id, rg_name, rg_type, COALESCE(rg_website,'')
		FROM aveloxis_data.repo_groups
		WHERE rg_type IN ('github_org', 'gitlab_group')
		ORDER BY rg_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []OrgGroup
	for rows.Next() {
		var g OrgGroup
		if err := rows.Scan(&g.ID, &g.Name, &g.Type, &g.Website); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// GetUserGroupIDsForOrgURL returns every user_group whose user_org_requests
// row points at the given org URL. Used by the scan-time / refresh paths
// to bridge legacy repo_groups discovery into modern aveloxis_ops.user_repos
// linkage so every repo (including forks) discovered while scanning a
// tracked org lands in user_repos for the operator's group view.
//
// Returns an empty slice (not an error) when nothing is tracking the org —
// callers can range over the result unconditionally.
func (s *PostgresStore) GetUserGroupIDsForOrgURL(ctx context.Context, orgURL string) ([]int64, error) {
	if orgURL == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT group_id FROM aveloxis_ops.user_org_requests WHERE org_url = $1`,
		orgURL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// GetReposForRenameCheck returns repos that should be checked for renames.
// Picks repos not collected recently, limited to n repos per check cycle.
func (s *PostgresStore) GetReposForRenameCheck(ctx context.Context, limit int) ([]model.Repo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.platform_id, r.repo_git, r.repo_name, r.repo_owner
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE q.status IS DISTINCT FROM 'collecting'
		ORDER BY r.data_collection_date ASC NULLS FIRST
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var repos []model.Repo
	for rows.Next() {
		var r model.Repo
		var platID int16
		if err := rows.Scan(&r.ID, &platID, &r.GitURL, &r.Name, &r.Owner); err != nil {
			return nil, err
		}
		r.Platform = model.Platform(platID)
		repos = append(repos, r)
	}
	return repos, rows.Err()
}

// FindReviewDBID looks up the aveloxis DB pr_review_id for a platform
// review ID within one repo.
//
// v0.25.33: repo-scoped. The pre-v0.25.33 global lookup returned an
// ARBITRARY copy whenever the same repository existed under two
// repo_ids (case-variant duplicates, pre-dedup), silently creating
// cross-repo bridge rows — winner-owned review_comments pointing at
// loser-owned reviews — which then broke `aveloxis dedup-repos` with
// SQLSTATE 23503. A review comment's parent review MUST belong to the
// same repo as the comment.
func (s *PostgresStore) FindReviewDBID(ctx context.Context, repoID, platformReviewID int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT pr_review_id FROM aveloxis_data.pull_request_reviews
		 WHERE platform_review_id = $1 AND repo_id = $2`,
		platformReviewID, repoID).Scan(&id)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

// FindIssueDBID looks up the aveloxis DB issue_id from an issue number (human-readable #N).
func (s *PostgresStore) FindIssueDBID(ctx context.Context, repoID, issueNumber int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT issue_id FROM aveloxis_data.issues WHERE repo_id = $1 AND issue_number = $2`,
		repoID, issueNumber).Scan(&id)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

// FindPRDBID looks up the aveloxis DB pull_request_id from a PR number (human-readable #N).
func (s *PostgresStore) FindPRDBID(ctx context.Context, repoID, prNumber int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE repo_id = $1 AND pr_number = $2`,
		repoID, prNumber).Scan(&id)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

// FindRepoByURL returns the repo_id for a given git URL, or 0 if not found.
//
// v0.25.32: matching is case-insensitive for GitHub/GitLab (platform 1/2)
// because those forges treat owner/repo paths case-insensitively — a URL
// differing from a stored row only by case IS the same repository. A
// byte-exact match is preferred when both exist (a pre-dedup DB can hold
// both case variants; the exact row is the caller's literal intent).
// Generic git (platform 3) stays byte-exact on purpose: unknown hosts may
// legitimately be case-sensitive.
func (s *PostgresStore) FindRepoByURL(ctx context.Context, gitURL string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE repo_git = $1
		   OR (LOWER(repo_git) = LOWER($1) AND platform_id IN (1, 2))
		ORDER BY (repo_git = $1) DESC, repo_id
		LIMIT 1`, gitURL,
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return id, nil
}

// resolveCaseVariantURL returns the stored repo_git spelling of an
// existing GitHub/GitLab row whose URL matches gitURL case-insensitively,
// or "" when no such row exists. UpsertRepo substitutes the stored
// spelling before its INSERT so ON CONFLICT (repo_git) targets the
// existing row instead of creating a case-variant duplicate. The
// platform_id IN (1, 2) gate keeps generic-git hosts byte-exact.
func (s *PostgresStore) resolveCaseVariantURL(ctx context.Context, gitURL string) (string, error) {
	var stored string
	err := s.pool.QueryRow(ctx, `
		SELECT repo_git FROM aveloxis_data.repos
		WHERE LOWER(repo_git) = LOWER($1) AND platform_id IN (1, 2)
		ORDER BY (repo_git = $1) DESC, repo_id
		LIMIT 1`, gitURL,
	).Scan(&stored)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return stored, nil
}

// ArchiveRepo marks a repo as archived/dead. Data is kept, but the repo
// will not be collected again unless manually un-archived and re-queued.
func (s *PostgresStore) ArchiveRepo(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET repo_archived = TRUE, data_collection_date = NOW() WHERE repo_id = $1`,
		repoID)
	return err
}

// UpdateRepoURLs updates the git URL of a repo and fixes all stored URLs
// (issue html_urls, PR html_urls, etc.) that contain the old org/repo path.
// This handles GitHub/GitLab repo renames/transfers where all URLs change.
func (s *PostgresStore) UpdateRepoURLs(ctx context.Context, repoID int64, oldURL, newURL string) error {
	// v0.27.113 (Copilot round 9): normalize the stored URL exactly like
	// UpdateRepoURL does — prelim passes the RAW redirect target, so a
	// redirect to ".../name.git" (or a trailing slash) would otherwise
	// persist a noncanonical repo_git and undermine the URL-dedup
	// invariant. extractRepoPath already trims internally, so only the
	// direct repo_git write was exposed.
	newURL = strings.TrimSuffix(strings.TrimSuffix(newURL, "/"), ".git")

	// Extract the path portions for find-and-replace.
	// e.g., "https://github.com/old-org/old-repo" -> "old-org/old-repo"
	oldPath := extractRepoPath(oldURL)
	newPath := extractRepoPath(newURL)

	if oldPath == "" || newPath == "" || oldPath == newPath {
		// Just update the repo_git URL.
		return s.UpdateRepoURL(ctx, repoID, newURL)
	}

	// v0.27.111 (Copilot PR #184 round 6, active finding): the repo_git
	// update and the child-URL rewrites run in ONE transaction. The old
	// shape updated repo_git first and swallowed every child-update
	// error (the "no matching rows" comment was wrong — a zero-row
	// UPDATE succeeds; an ERROR is a real failure), so a mid-child
	// failure returned success with repo_git already changed — later
	// scans saw the new URL and never retried, leaving issue/PR/release
	// URLs permanently stale. Now any failure rolls the whole rename
	// back and the caller retries next cycle.
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		owner, name := parseRepoURLOwnerName(newURL)
		if _, err := tx.Exec(ctx,
			`UPDATE aveloxis_data.repos
			 SET repo_git = $2, repo_owner = $3, repo_name = $4, data_collection_date = NOW()
			 WHERE repo_id = $1`,
			repoID, newURL, owner, name); err != nil {
			return fmt.Errorf("rename repos row: %w", err)
		}

		// Bulk-update all URL columns that contain the old path.
		updates := []struct {
			table  string
			column string
		}{
			{"aveloxis_data.issues", "issue_url"},
			{"aveloxis_data.issues", "html_url"},
			{"aveloxis_data.pull_requests", "pr_url"},
			{"aveloxis_data.pull_requests", "pr_html_url"},
			{"aveloxis_data.pull_requests", "pr_diff_url"},
			{"aveloxis_data.pull_request_reviews", "html_url"},
			{"aveloxis_data.review_comments", "html_url"},
			{"aveloxis_data.releases", "release_url"},
		}
		for _, u := range updates {
			if _, err := tx.Exec(ctx, fmt.Sprintf(
				`UPDATE %s SET %s = REPLACE(%s, $1, $2) WHERE repo_id = $3 AND %s LIKE '%%' || $1 || '%%'`,
				u.table, u.column, u.column, u.column),
				oldPath, newPath, repoID); err != nil {
				return fmt.Errorf("rename %s.%s: %w", u.table, u.column, err)
			}
		}
		return tx.Commit(ctx)
	})
}

// parseRepoURLOwnerName extracts the normalized owner/name pair for a
// repos-row URL update — the single parse both UpdateRepoURL and the
// transactional UpdateRepoURLs use (v0.27.111).
func parseRepoURLOwnerName(newURL string) (owner, name string) {
	if ru, perr := platform.ParseAnyRepoURL(newURL); perr == nil {
		owner = ru.Owner
		name = ru.Repo
	}
	return owner, model.NormalizeRepoName(name)
}

// extractRepoPath extracts "owner/repo" from a URL like "https://github.com/owner/repo".
func extractRepoPath(u string) string {
	for _, prefix := range []string{"https://", "http://"} {
		u = strings.TrimPrefix(u, prefix)
	}
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	// Remove host: "github.com/owner/repo" -> "owner/repo"
	if _, after, ok := strings.Cut(u, "/"); ok {
		return after
	}
	return ""
}

// UpdateRepoURL changes the git URL, owner, and name of a repo (e.g., after a redirect).
// Extracts the new owner/name from the URL so the dashboard and API show correct values.
func (s *PostgresStore) UpdateRepoURL(ctx context.Context, repoID int64, newURL string) error {
	// Parse owner/name from the new URL via the shared parser (v0.25.32
	// consolidation; unparseable URLs keep empty owner/name — the URL
	// column still updates, matching the historical permissiveness).
	newURL = strings.TrimSuffix(strings.TrimSuffix(newURL, "/"), ".git")
	owner, name := parseRepoURLOwnerName(newURL)

	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos
		 SET repo_git = $2, repo_owner = $3, repo_name = $4, data_collection_date = NOW()
		 WHERE repo_id = $1`,
		repoID, newURL, owner, name,
	)
	return err
}

// DequeueRepo removes a repo from the collection queue.
func (s *PostgresStore) DequeueRepo(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID,
	)
	return err
}

// ============================================================
// Issues
// ============================================================

func (s *PostgresStore) UpsertIssue(ctx context.Context, issue *model.Issue) (int64, error) {
	// Sanitize text fields to remove null bytes and invalid UTF-8.
	issue.Title = SanitizeText(issue.Title)
	issue.Body = SanitizeText(issue.Body)

	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.issues
				(repo_id, platform_issue_id, issue_number, node_id,
				 issue_title, issue_body, issue_state, issue_url, html_url,
				 reporter_id, closed_by_id,
				 created_at, updated_at, closed_at, comment_count,
				 data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
			ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET
				issue_title = EXCLUDED.issue_title,
				issue_body = EXCLUDED.issue_body,
				issue_state = EXCLUDED.issue_state,
				reporter_id = COALESCE(EXCLUDED.reporter_id, issues.reporter_id),
				closed_by_id = COALESCE(EXCLUDED.closed_by_id, issues.closed_by_id),
				updated_at = EXCLUDED.updated_at,
				closed_at = EXCLUDED.closed_at,
				comment_count = EXCLUDED.comment_count,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()
			RETURNING issue_id`,
			issue.RepoID, issue.PlatformID, issue.Number, issue.NodeID,
			issue.Title, issue.Body, issue.State, issue.URL, issue.HTMLURL,
			issue.ReporterID, issue.ClosedByID,
			NullTime(issue.CreatedAt), NullTime(issue.UpdatedAt), issue.ClosedAt, issue.CommentCount,
			issue.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
}

func (s *PostgresStore) UpsertIssueLabels(ctx context.Context, issueID, repoID int64, labels []model.IssueLabel) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, l := range labels {
			batch.Queue(`
				INSERT INTO aveloxis_data.issue_labels
					(issue_id, repo_id, platform_label_id, node_id,
					 label_text, label_description, label_color, data_source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
				ON CONFLICT (issue_id, label_text) DO UPDATE SET
					label_description = EXCLUDED.label_description,
					label_color = EXCLUDED.label_color,
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()`,
				issueID, repoID, l.PlatformID, l.NodeID,
				l.Text, l.Description, l.Color, l.Origin.DataSource,
			)
		}
		if err := s.pool.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		// v0.27.39 (summary/18 Phase 2): reconcile removals. The caller
		// delivers the item's COMPLETE current label set (GraphQL
		// first:100 + pagination; REST full iterators), so anything
		// absent from it was removed upstream. Callers skip empty sets
		// (a failed child fetch must never delete), so removal-to-ZERO
		// is a documented residual until a completeness flag exists.
		names := make([]string, 0, len(labels))
		for _, l := range labels {
			names = append(names, l.Text)
		}
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.issue_labels
			WHERE issue_id = $1 AND NOT (label_text = ANY($2::text[]))`, issueID, names)
		return err
	})
}

func (s *PostgresStore) UpsertIssueAssignees(ctx context.Context, issueID, repoID int64, assignees []model.IssueAssignee) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, a := range assignees {
			batch.Queue(`
				INSERT INTO aveloxis_data.issue_assignees
					(issue_id, repo_id, cntrb_id, platform_assignee_id, platform_node_id, data_source)
				VALUES ($1,$2,$3,$4,$5,$6)
				ON CONFLICT (issue_id, platform_assignee_id) DO UPDATE SET
					cntrb_id = COALESCE(issue_assignees.cntrb_id, EXCLUDED.cntrb_id)`,
				issueID, repoID, a.ContributorID, a.PlatformSrcID, a.PlatformNodeID, a.Origin.DataSource,
			)
		}
		if err := s.pool.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		// v0.27.39: reconcile removed assignees (see UpsertIssueLabels).
		ids := make([]int64, 0, len(assignees))
		for _, a := range assignees {
			ids = append(ids, a.PlatformSrcID)
		}
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.issue_assignees
			WHERE issue_id = $1 AND NOT (platform_assignee_id = ANY($2::bigint[]))`, issueID, ids)
		return err
	})
}

// ============================================================
// Pull Requests
// ============================================================

func (s *PostgresStore) UpsertPullRequest(ctx context.Context, pr *model.PullRequest) (int64, error) {
	pr.Title = SanitizeText(pr.Title)
	pr.Body = SanitizeText(pr.Body)

	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.pull_requests
				(repo_id, platform_pr_id, node_id, pr_number,
				 pr_url, pr_html_url, pr_diff_url, pr_title, pr_body,
				 pr_state, pr_locked, author_id,
				 created_at, updated_at, closed_at, merged_at,
				 merge_commit_sha, author_association, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
			ON CONFLICT (repo_id, platform_pr_id) DO UPDATE SET
				pr_title = EXCLUDED.pr_title,
				pr_body = EXCLUDED.pr_body,
				pr_state = EXCLUDED.pr_state,
				pr_locked = EXCLUDED.pr_locked,
				author_id = COALESCE(EXCLUDED.author_id, pull_requests.author_id),
				updated_at = EXCLUDED.updated_at,
				closed_at = EXCLUDED.closed_at,
				merged_at = EXCLUDED.merged_at,
				merge_commit_sha = EXCLUDED.merge_commit_sha,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()
			RETURNING pull_request_id`,
			pr.RepoID, pr.PlatformSrcID, pr.NodeID, pr.Number,
			pr.URL, pr.HTMLURL, pr.DiffURL, pr.Title, pr.Body,
			pr.State, pr.Locked, pr.AuthorID,
			NullTime(pr.CreatedAt), NullTime(pr.UpdatedAt), pr.ClosedAt, pr.MergedAt,
			pr.MergeCommitSHA, pr.AuthorAssociation, pr.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
}

func (s *PostgresStore) UpsertPRLabels(ctx context.Context, prID, repoID int64, labels []model.PullRequestLabel) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, l := range labels {
			batch.Queue(`
				INSERT INTO aveloxis_data.pull_request_labels
					(pull_request_id, repo_id, platform_label_id, node_id,
					 label_name, label_description, label_color, is_default, data_source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
				ON CONFLICT (pull_request_id, label_name) DO UPDATE SET
					label_description = EXCLUDED.label_description,
					label_color = EXCLUDED.label_color,
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()`,
				prID, repoID, l.PlatformID, l.NodeID,
				l.Name, l.Description, l.Color, l.IsDefault, l.Origin.DataSource,
			)
		}
		if err := s.pool.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		// v0.27.39: reconcile removed labels (see UpsertIssueLabels).
		names := make([]string, 0, len(labels))
		for _, l := range labels {
			names = append(names, l.Name)
		}
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.pull_request_labels
			WHERE pull_request_id = $1 AND NOT (label_name = ANY($2::text[]))`, prID, names)
		return err
	})
}

func (s *PostgresStore) UpsertPRAssignees(ctx context.Context, prID, repoID int64, assignees []model.PullRequestAssignee) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, a := range assignees {
			batch.Queue(`
				INSERT INTO aveloxis_data.pull_request_assignees
					(pull_request_id, repo_id, cntrb_id, platform_assignee_id, data_source)
				VALUES ($1,$2,$3,$4,$5)
				ON CONFLICT (pull_request_id, platform_assignee_id) DO UPDATE SET
					cntrb_id = COALESCE(pull_request_assignees.cntrb_id, EXCLUDED.cntrb_id)`,
				prID, repoID, a.ContributorID, a.PlatformSrcID, a.Origin.DataSource,
			)
		}
		if err := s.pool.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		// v0.27.39: reconcile unassigned assignees (see UpsertIssueLabels).
		ids := make([]int64, 0, len(assignees))
		for _, a := range assignees {
			ids = append(ids, a.PlatformSrcID)
		}
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.pull_request_assignees
			WHERE pull_request_id = $1 AND NOT (platform_assignee_id = ANY($2::bigint[]))`, prID, ids)
		return err
	})
}

func (s *PostgresStore) UpsertPRReviewers(ctx context.Context, prID, repoID int64, reviewers []model.PullRequestReviewer) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, r := range reviewers {
			batch.Queue(`
				INSERT INTO aveloxis_data.pull_request_reviewers
					(pull_request_id, repo_id, cntrb_id, platform_reviewer_id, data_source)
				VALUES ($1,$2,$3,$4,$5)
				ON CONFLICT (pull_request_id, platform_reviewer_id) DO UPDATE SET
					cntrb_id = COALESCE(pull_request_reviewers.cntrb_id, EXCLUDED.cntrb_id)`,
				prID, repoID, r.ContributorID, r.PlatformSrcID, r.Origin.DataSource,
			)
		}
		if err := s.pool.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		// v0.27.39: reconcile withdrawn review requests (see
		// UpsertIssueLabels).
		ids := make([]int64, 0, len(reviewers))
		for _, r := range reviewers {
			ids = append(ids, r.PlatformSrcID)
		}
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.pull_request_reviewers
			WHERE pull_request_id = $1 AND NOT (platform_reviewer_id = ANY($2::bigint[]))`, prID, ids)
		return err
	})
}

func (s *PostgresStore) UpsertPRReview(ctx context.Context, review *model.PullRequestReview) error {
	review.Body = SanitizeText(review.Body)
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Upsert the review itself.
		var reviewID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO aveloxis_data.pull_request_reviews
				(pull_request_id, repo_id, cntrb_id, platform_id, platform_review_id, node_id,
				 review_state, review_body, submitted_at, author_association,
				 commit_id, html_url, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (pull_request_id, platform_review_id) DO UPDATE SET
				review_state = EXCLUDED.review_state,
				review_body = EXCLUDED.review_body,
				cntrb_id = COALESCE(EXCLUDED.cntrb_id, pull_request_reviews.cntrb_id),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()
			RETURNING pr_review_id`,
			review.PRID, review.RepoID, review.ContributorID, int16(review.PlatformID), review.PlatformReviewID,
			review.NodeID, review.State, review.Body, NullTime(review.SubmittedAt),
			review.AuthorAssociation, review.CommitID, review.HTMLURL,
			review.Origin.DataSource,
		).Scan(&reviewID)
		if err != nil {
			return err
		}

		// Store the review body as a message (same pattern as issue/PR comments).
		// Only create a message if the review body is non-empty — many reviews
		// are "APPROVED" or "CHANGES_REQUESTED" with no body text.
		if review.Body != "" {
			var msgID int64
			// v0.27.38 (summary/18 Phase 1a): platform_review_id is a
			// SEPARATE GitHub id sequence from comment ids — the old
			// comment here planned an offset that was never built, and
			// production accumulated 198,237 cross-kind collisions.
			// msg_kind in the arbiter is the real fix.
			err = tx.QueryRow(ctx, upsertMessageSQL,
				review.RepoID, review.PlatformReviewID, int16(review.PlatformID),
				MsgKindReviewBody, review.NodeID, review.ContributorID, review.Body,
				NullTime(review.SubmittedAt), review.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			// Create bridge row linking review to message.
			//
			// v0.27.15: the arbiter (pr_review_id, msg_id) is NAMED. The
			// previous bare ON CONFLICT DO NOTHING had no unique constraint
			// to arbitrate against — dead code, exactly the v0.27.7
			// repo_labor lesson — and every re-collection cycle duplicated
			// this row (5.26M duplicate rows on production). The unique
			// index uq_pr_review_msg_ref is created by the v0.27.15
			// migration after dedup (schema-DDL-ordering rule: NOT in
			// schema.sql). Review-BODY rows deliberately carry no line
			// metadata — a review submission has no line anchor; inline
			// comment rows (which do) are written by the review-comment
			// upserts below.
			_, err = tx.Exec(ctx, `
				INSERT INTO aveloxis_data.pull_request_review_message_ref
					(pr_review_id, repo_id, msg_id, pr_review_src_id, pr_review_msg_node_id, data_source)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (pr_review_id, msg_id) DO NOTHING`,
				reviewID, review.RepoID, msgID, review.PlatformReviewID, review.NodeID,
				review.Origin.DataSource)
			if err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	})
}

func (s *PostgresStore) UpsertPRCommit(ctx context.Context, commit *model.PullRequestCommit) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.pull_request_commits
				(pull_request_id, repo_id, pr_cmt_sha, pr_cmt_node_id,
				 pr_cmt_message, pr_cmt_author_email, author_cntrb_id, pr_cmt_timestamp, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (pull_request_id, pr_cmt_sha) DO UPDATE SET
				pr_cmt_message = EXCLUDED.pr_cmt_message,
				author_cntrb_id = COALESCE(EXCLUDED.author_cntrb_id, pull_request_commits.author_cntrb_id),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			commit.PRID, commit.RepoID, commit.SHA, commit.NodeID,
			commit.Message, commit.AuthorEmail, commit.AuthorID, NullTime(commit.Timestamp),
			commit.Origin.DataSource,
		)
		return err
	})
}

func (s *PostgresStore) UpsertPRFile(ctx context.Context, file *model.PullRequestFile) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.pull_request_files
				(pull_request_id, repo_id, pr_file_path, pr_file_additions, pr_file_deletions, data_source)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (pull_request_id, pr_file_path) DO UPDATE SET
				pr_file_additions = EXCLUDED.pr_file_additions,
				pr_file_deletions = EXCLUDED.pr_file_deletions,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			file.PRID, file.RepoID, file.Path, file.Additions, file.Deletions,
			file.Origin.DataSource,
		)
		return err
	})
}

func (s *PostgresStore) UpsertPRMeta(ctx context.Context, meta *model.PullRequestMeta) (int64, error) {
	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.pull_request_meta
				(pull_request_id, repo_id, cntrb_id, head_or_base, meta_label, meta_ref, meta_sha, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (pull_request_id, head_or_base) DO UPDATE SET
				cntrb_id = COALESCE(pull_request_meta.cntrb_id, EXCLUDED.cntrb_id),
				meta_label = EXCLUDED.meta_label,
				meta_ref = EXCLUDED.meta_ref,
				meta_sha = EXCLUDED.meta_sha,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()
			RETURNING pr_meta_id`,
			meta.PRID, meta.RepoID, meta.AuthorID, meta.HeadOrBase, meta.Label, meta.Ref, meta.SHA,
			meta.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
}

// SetPRMetaLinks stamps pull_requests.meta_head_id/meta_base_id from the
// upserted meta rows' PKs (v0.27.104 — processStagedPR held both ids
// since inception and discarded them; the columns were 100% dark).
// Zero-valued ids leave the column untouched; a real id may replace a
// stale one (meta row recreated under a new PK). The IS DISTINCT FROM
// guard makes steady-state re-collection a no-op read: UpsertPRMeta's
// ON CONFLICT RETURNING yields the same PK every cycle.
func (s *PostgresStore) SetPRMetaLinks(ctx context.Context, prID, headMetaID, baseMetaID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.pull_requests
			SET meta_head_id = COALESCE(NULLIF($2::bigint, 0), meta_head_id),
			    meta_base_id = COALESCE(NULLIF($3::bigint, 0), meta_base_id)
			WHERE pull_request_id = $1
			  AND (meta_head_id IS DISTINCT FROM COALESCE(NULLIF($2::bigint, 0), meta_head_id)
			    OR meta_base_id IS DISTINCT FROM COALESCE(NULLIF($3::bigint, 0), meta_base_id))`,
			prID, headMetaID, baseMetaID)
		return err
	})
}

// UpsertPRRepo inserts or updates a pull_request_repo row, storing fork/upstream
// repo details for a PR's head or base branch. Links to pull_request_meta via
// pr_repo_meta_id. The same PR may have both a head repo (fork) and base repo
// (upstream), so each is stored separately with pr_repo_head_or_base distinguishing them.
func (s *PostgresStore) UpsertPRRepo(ctx context.Context, repo *model.PullRequestRepo) error {
	if repo == nil || repo.MetaID == 0 {
		return nil
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.pull_request_repo
				(pr_repo_meta_id, pr_repo_head_or_base, pr_src_repo_id, pr_src_node_id,
				 pr_repo_name, pr_repo_full_name, pr_repo_private_bool, pr_cntrb_id,
				 data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
			ON CONFLICT (pr_repo_meta_id, pr_repo_head_or_base) DO UPDATE SET
				pr_src_repo_id = EXCLUDED.pr_src_repo_id,
				pr_src_node_id = EXCLUDED.pr_src_node_id,
				pr_repo_name = EXCLUDED.pr_repo_name,
				pr_repo_full_name = EXCLUDED.pr_repo_full_name,
				pr_repo_private_bool = EXCLUDED.pr_repo_private_bool,
				pr_cntrb_id = COALESCE(EXCLUDED.pr_cntrb_id, pull_request_repo.pr_cntrb_id),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			repo.MetaID, repo.HeadOrBase, repo.SrcRepoID, repo.SrcNodeID,
			repo.RepoName, repo.RepoFullName, repo.Private, repo.ContribID,
			repo.Origin.DataSource,
		)
		return err
	})
}

// ============================================================
// Events
// ============================================================

func (s *PostgresStore) UpsertIssueEvent(ctx context.Context, event *model.IssueEvent) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issue_events
				(issue_id, repo_id, cntrb_id, platform_id, platform_event_id, node_id,
				 action, action_commit_hash, created_at, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (repo_id, platform_event_id) DO UPDATE SET
				action = EXCLUDED.action,
				cntrb_id = COALESCE(EXCLUDED.cntrb_id, issue_events.cntrb_id),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			event.IssueID, event.RepoID, event.ContributorID, int16(event.PlatformID), event.PlatformEventID,
			event.NodeID, event.Action, event.ActionCommitHash, NullTime(event.CreatedAt),
			event.Origin.DataSource,
		)
		return err
	})
}

func (s *PostgresStore) UpsertPREvent(ctx context.Context, event *model.PullRequestEvent) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.pull_request_events
				(pull_request_id, repo_id, cntrb_id, platform_id, platform_event_id, node_id,
				 action, action_commit_hash, created_at, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (repo_id, platform_event_id) DO UPDATE SET
				action = EXCLUDED.action,
				cntrb_id = COALESCE(EXCLUDED.cntrb_id, pull_request_events.cntrb_id),
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			event.PRID, event.RepoID, event.ContributorID, int16(event.PlatformID), event.PlatformEventID,
			event.NodeID, event.Action, event.ActionCommitHash, NullTime(event.CreatedAt),
			event.Origin.DataSource,
		)
		return err
	})
}

// ============================================================
// Messages
// ============================================================

func (s *PostgresStore) UpsertMessage(ctx context.Context, msg *model.Message) (int64, error) {
	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, upsertMessageSQL,
			msg.RepoID, msg.PlatformMsgID, int16(msg.PlatformID), MsgKindComment,
			msg.NodeID, msg.ContributorID, msg.Text, NullTime(msg.Timestamp),
			msg.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
}

func (s *PostgresStore) UpsertIssueMessageRef(ctx context.Context, ref *model.IssueMessageRef) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issue_message_ref
				(issue_id, repo_id, msg_id, platform_src_id, platform_node_id, data_source)
			VALUES ($1,$2,$3,$4,$5,
			        COALESCE((SELECT m.data_source FROM aveloxis_data.messages m WHERE m.msg_id = $3), ''))
			ON CONFLICT (issue_id, msg_id) DO NOTHING`,
			ref.IssueID, ref.RepoID, ref.MsgID, ref.PlatformSrcID, ref.PlatformNodeID,
		)
		return err
	})
}

func (s *PostgresStore) UpsertPRMessageRef(ctx context.Context, ref *model.PullRequestMessageRef) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.pull_request_message_ref
				(pull_request_id, repo_id, msg_id, platform_src_id, platform_node_id, data_source)
			VALUES ($1,$2,$3,$4,$5,
			        COALESCE((SELECT m.data_source FROM aveloxis_data.messages m WHERE m.msg_id = $3), ''))
			ON CONFLICT (pull_request_id, msg_id) DO NOTHING`,
			ref.PRID, ref.RepoID, ref.MsgID, ref.PlatformSrcID, ref.PlatformNodeID,
		)
		return err
	})
}

// prReviewMsgRefFromCommentSQL writes the Augur-compat
// pull_request_review_message_ref row for an INLINE review comment,
// carrying the full line-anchoring metadata (v0.27.15 — these columns
// were 100%% dark on production while review_comments held all the
// data). $21 is the message's data_source. Arbiter (pr_review_id,
// msg_id) is the uq_pr_review_msg_ref unique index created by the
// v0.27.15 migration after dedup.
const prReviewMsgRefFromCommentSQL = `
	INSERT INTO aveloxis_data.pull_request_review_message_ref
		(pr_review_id, repo_id, msg_id, pr_review_msg_src_id, pr_review_msg_node_id,
		 pr_review_msg_diff_hunk, pr_review_msg_path, pr_review_msg_position,
		 pr_review_msg_original_position, pr_review_msg_commit_id,
		 pr_review_msg_original_commit_id, pr_review_msg_updated_at,
		 pr_review_msg_html_url, pr_review_msg_author_association,
		 pr_review_msg_start_line, pr_review_msg_original_start_line,
		 pr_review_msg_start_side, pr_review_msg_line, pr_review_msg_original_line,
		 pr_review_msg_side, data_source)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
	ON CONFLICT (pr_review_id, msg_id) DO UPDATE SET
		pr_review_msg_diff_hunk = EXCLUDED.pr_review_msg_diff_hunk,
		pr_review_msg_updated_at = EXCLUDED.pr_review_msg_updated_at`

func (s *PostgresStore) UpsertReviewComment(ctx context.Context, c *model.ReviewComment) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.review_comments
				(pr_review_id, repo_id, msg_id, platform_src_id, node_id,
				 diff_hunk, file_path, position, original_position,
				 commit_id, original_commit_id, line, original_line,
				 side, start_line, original_start_line, start_side,
				 author_association, html_url, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
			ON CONFLICT (repo_id, platform_src_id) DO UPDATE SET
				diff_hunk = EXCLUDED.diff_hunk,
				updated_at = EXCLUDED.updated_at`,
			c.ReviewID, c.RepoID, c.MsgID, c.PlatformSrcID, c.NodeID,
			c.DiffHunk, c.Path, c.Position, c.OriginalPosition,
			c.CommitID, c.OriginalCommitID, c.Line, c.OriginalLine,
			c.Side, c.StartLine, c.OriginalStartLine, c.StartSide,
			c.AuthorAssociation, c.HTMLURL, NullTime(c.UpdatedAt),
		)
		if err != nil {
			return err
		}
		// v0.27.15: mirror the comment into the Augur-compat ref table
		// with its full line metadata. Comments whose review is not in
		// the DB (ReviewID 0) are skipped — the ref's pr_review_id is
		// NOT NULL; the v0.27.15 backfill picks them up if the review
		// arrives later. data_source comes from the message row (exact
		// provenance). Both statements are idempotent, so a failure
		// between them is healed by the caller's retry.
		if c.ReviewID != 0 {
			var ds string
			if err := s.pool.QueryRow(ctx,
				`SELECT COALESCE(data_source, '') FROM aveloxis_data.messages WHERE msg_id = $1`,
				c.MsgID).Scan(&ds); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if _, err := s.pool.Exec(ctx, prReviewMsgRefFromCommentSQL,
				c.ReviewID, c.RepoID, c.MsgID, c.PlatformSrcID, c.NodeID,
				c.DiffHunk, c.Path, c.Position, c.OriginalPosition,
				c.CommitID, c.OriginalCommitID, NullTime(c.UpdatedAt),
				c.HTMLURL, c.AuthorAssociation,
				c.StartLine, c.OriginalStartLine, c.StartSide,
				c.Line, c.OriginalLine, c.Side, ds,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// ============================================================
// Batch message upserts (transaction)
// ============================================================

func (s *PostgresStore) UpsertMessageBatch(ctx context.Context, msgs []platform.MessageWithRef) error {
	// Sanitize message text.
	for i := range msgs {
		msgs[i].Message.Text = SanitizeText(msgs[i].Message.Text)
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		for _, m := range msgs {
			var msgID int64
			err := tx.QueryRow(ctx, upsertMessageSQL,
				m.Message.RepoID, m.Message.PlatformMsgID, int16(m.Message.PlatformID),
				MsgKindComment, m.Message.NodeID, m.Message.ContributorID, m.Message.Text,
				NullTime(m.Message.Timestamp), m.Message.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			if m.IssueRef != nil {
				m.IssueRef.MsgID = msgID
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_data.issue_message_ref
						(issue_id, repo_id, msg_id, platform_src_id, platform_node_id, data_source)
					VALUES ($1,$2,$3,$4,$5,$6)
					ON CONFLICT (issue_id, msg_id) DO NOTHING`,
					m.IssueRef.IssueID, m.IssueRef.RepoID, msgID,
					m.IssueRef.PlatformSrcID, m.IssueRef.PlatformNodeID,
					m.Message.Origin.DataSource,
				)
				if err != nil {
					return err
				}
			}
			if m.PRRef != nil {
				m.PRRef.MsgID = msgID
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_data.pull_request_message_ref
						(pull_request_id, repo_id, msg_id, platform_src_id, platform_node_id, data_source)
					VALUES ($1,$2,$3,$4,$5,$6)
					ON CONFLICT (pull_request_id, msg_id) DO NOTHING`,
					m.PRRef.PRID, m.PRRef.RepoID, msgID,
					m.PRRef.PlatformSrcID, m.PRRef.PlatformNodeID,
					m.Message.Origin.DataSource,
				)
				if err != nil {
					return err
				}
			}
		}

		return tx.Commit(ctx)
	})
}

func (s *PostgresStore) UpsertReviewCommentBatch(ctx context.Context, comments []platform.ReviewCommentWithRef) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		for _, rc := range comments {
			var msgID int64
			err := tx.QueryRow(ctx, upsertMessageSQL,
				rc.Message.RepoID, rc.Message.PlatformMsgID, int16(rc.Message.PlatformID),
				MsgKindReviewComment, rc.Message.NodeID, rc.Message.ContributorID,
				rc.Message.Text, NullTime(rc.Message.Timestamp), rc.Message.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			rc.Comment.MsgID = msgID
			// Use NULL for pr_review_id when it's zero (review not yet in DB).
			var reviewID any
			if rc.Comment.ReviewID != 0 {
				reviewID = rc.Comment.ReviewID
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO aveloxis_data.review_comments
					(pr_review_id, repo_id, msg_id, platform_src_id, node_id,
					 diff_hunk, file_path, position, original_position,
					 commit_id, original_commit_id, line, original_line,
					 side, start_line, original_start_line, start_side,
					 author_association, html_url, updated_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
				ON CONFLICT (repo_id, platform_src_id) DO UPDATE SET
					diff_hunk = EXCLUDED.diff_hunk,
					pr_review_id = COALESCE(EXCLUDED.pr_review_id, review_comments.pr_review_id),
					updated_at = EXCLUDED.updated_at`,
				reviewID, rc.Comment.RepoID, msgID, rc.Comment.PlatformSrcID,
				rc.Comment.NodeID, rc.Comment.DiffHunk, rc.Comment.Path,
				rc.Comment.Position, rc.Comment.OriginalPosition,
				rc.Comment.CommitID, rc.Comment.OriginalCommitID,
				rc.Comment.Line, rc.Comment.OriginalLine,
				rc.Comment.Side, rc.Comment.StartLine, rc.Comment.OriginalStartLine,
				rc.Comment.StartSide, rc.Comment.AuthorAssociation,
				rc.Comment.HTMLURL, NullTime(rc.Comment.UpdatedAt),
			)
			if err != nil {
				return err
			}

			// v0.27.15: Augur-compat ref row with full line metadata
			// (skipped when the review is not yet in the DB — the
			// backfill migration catches those once it is).
			if reviewID != nil {
				if _, err := tx.Exec(ctx, prReviewMsgRefFromCommentSQL,
					reviewID, rc.Comment.RepoID, msgID, rc.Comment.PlatformSrcID, rc.Comment.NodeID,
					rc.Comment.DiffHunk, rc.Comment.Path, rc.Comment.Position, rc.Comment.OriginalPosition,
					rc.Comment.CommitID, rc.Comment.OriginalCommitID, NullTime(rc.Comment.UpdatedAt),
					rc.Comment.HTMLURL, rc.Comment.AuthorAssociation,
					rc.Comment.StartLine, rc.Comment.OriginalStartLine, rc.Comment.StartSide,
					rc.Comment.Line, rc.Comment.OriginalLine, rc.Comment.Side,
					rc.Message.Origin.DataSource,
				); err != nil {
					return err
				}
			}
		}

		return tx.Commit(ctx)
	})
}

// ============================================================
// Releases
// ============================================================

func (s *PostgresStore) UpsertRelease(ctx context.Context, r *model.Release) error {
	r.Name = SanitizeText(r.Name)
	r.Description = SanitizeText(r.Description)
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.releases
				(release_id, repo_id, release_name, release_description,
				 release_author, release_tag_name, release_url,
				 created_at, published_at, updated_at,
				 is_draft, is_prerelease, tag_only, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			ON CONFLICT (repo_id, release_id) DO UPDATE SET
				release_name = EXCLUDED.release_name,
				release_description = EXCLUDED.release_description,
				updated_at = EXCLUDED.updated_at,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			r.ID, r.RepoID, r.Name, r.Description,
			r.Author, r.TagName, r.URL,
			NullTime(r.CreatedAt), r.PublishedAt, NullTime(r.UpdatedAt),
			r.IsDraft, r.IsPrerelease, r.TagOnly, r.Origin.DataSource,
		)
		return err
	})
}

// ============================================================
// Contributors
// ============================================================

// UpsertContributor upserts a single contributor. For bulk operations,
// prefer UpsertContributorBatch which deduplicates in memory first.
func (s *PostgresStore) UpsertContributor(ctx context.Context, contrib *model.Contributor) error {
	return s.UpsertContributorBatch(ctx, []model.Contributor{*contrib})
}

// UpsertContributorBatch upserts a batch of contributors in a single transaction.
// Duplicates within the batch are merged in memory (richest data wins) before
// touching the database, eliminating contention on the contributors table.
func (s *PostgresStore) UpsertContributorBatch(ctx context.Context, contribs []model.Contributor) error {
	if len(contribs) == 0 {
		return nil
	}

	// In-memory dedup: merge contributors with the same login.
	// Keep the richest data (longest non-empty fields win).
	merged := make(map[string]*model.Contributor)
	var identMap = make(map[string][]model.ContributorIdentity)

	for i := range contribs {
		c := &contribs[i]
		if c.Login == "" {
			continue
		}
		existing, ok := merged[c.Login]
		if !ok {
			merged[c.Login] = c
			identMap[c.Login] = c.Identities
		} else {
			// Merge: prefer non-empty fields.
			if c.Email != "" && (existing.Email == "" || len(c.Email) > len(existing.Email)) {
				existing.Email = c.Email
			}
			if c.FullName != "" && existing.FullName == "" {
				existing.FullName = c.FullName
			}
			if c.Company != "" && existing.Company == "" {
				existing.Company = c.Company
			}
			if c.Location != "" && existing.Location == "" {
				existing.Location = c.Location
			}
			if c.Canonical != "" && existing.Canonical == "" {
				existing.Canonical = c.Canonical
			}
			// Merge identities (dedup by platform+user_id).
			seen := make(map[string]bool)
			for _, id := range identMap[c.Login] {
				key := fmt.Sprintf("%d:%d", id.Platform, id.UserID)
				seen[key] = true
			}
			for _, id := range c.Identities {
				key := fmt.Sprintf("%d:%d", id.Platform, id.UserID)
				if !seen[key] {
					identMap[c.Login] = append(identMap[c.Login], id)
				}
			}
		}
	}

	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// v0.20.11 (Fix G): capture the first underlying SQL error
		// across the per-contributor INSERT and per-identity INSERT/
		// UPDATE inside this tx. Pre-v0.20.11 these were swallowed
		// (Debug-level log, then `continue` for the contributor row;
		// `_, _ = tx.Exec(...)` for the identity rows), so when
		// tx.Commit eventually returned "commit unexpectedly resulted
		// in rollback" we had no way to see WHICH statement poisoned
		// the transaction. 70 such rollback events appeared in the
		// May 9–12 production log; the diagnostic capture below
		// surfaces the SQLSTATE and offending login so the next run
		// gives us data to plan a real fix.
		var firstFailedLogin string
		var firstFailedSQLState string
		var firstFailedKind string

		captureErr := func(kind, login string, e error) {
			if firstFailedLogin != "" {
				return
			}
			firstFailedLogin = login
			firstFailedKind = kind
			var pgErr *pgconn.PgError
			if errors.As(e, &pgErr) {
				firstFailedSQLState = pgErr.Code
				s.logger.Warn("contributor batch sub-statement failed",
					"kind", kind, "login", login,
					"sqlstate", pgErr.Code, "constraint", pgErr.ConstraintName,
					"detail", pgErr.Detail, "message", pgErr.Message)
			} else {
				s.logger.Warn("contributor batch sub-statement failed",
					"kind", kind, "login", login, "error", e)
			}
		}

		// v0.22.13: monotonic counter for SAVEPOINT names. SQL
		// identifiers can't contain quotes or arbitrary user input
		// (login strings might contain '[', ']', '-', etc. as in
		// "openshift-merge-bot[bot]"), so we never embed login text
		// in the savepoint name.
		var spCounter int

		// Acquire contributor locks in a DETERMINISTIC order (sorted by login)
		// across every concurrent worker. Without this, the map iteration order
		// is random, so two workers whose batches both touch popular shared
		// contributors (e.g. regro-cf-autotick-bot, conda-forge-admin — bots
		// that appear in thousands of repos) lock those rows in different orders
		// and deadlock (SQLSTATE 40P01). Consistent ordering makes a lock cycle
		// impossible. (2026-06-09: fixes the contributor-deadlock storm on
		// aveloxis_large; same class as Augur's old contributor deadlocks.)
		logins := make([]string, 0, len(merged))
		for login := range merged {
			logins = append(logins, login)
		}
		sort.Strings(logins)

		for _, login := range logins {
			if err := s.upsertOneContributor(ctx, tx, login, merged[login], identMap[login], captureErr, &spCounter); err != nil {
				return err
			}
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			// v0.20.11 (Fix G): annotate the rollback error with the
			// first-failed context captured above. Pre-fix the
			// upstream caller saw only "commit unexpectedly resulted
			// in rollback" with no clue which contributor or which
			// statement poisoned the tx. With this annotation the
			// next run's logs will include the SQLSTATE and the
			// offending login alongside the bare commit error.
			if firstFailedLogin != "" {
				s.logger.Warn("contributor batch commit failed — root cause was earlier sub-statement",
					"kind", firstFailedKind,
					"first_failed_login", firstFailedLogin,
					"first_failed_sqlstate", firstFailedSQLState,
					"commit_error", commitErr)
				return fmt.Errorf("contributor batch commit failed (first sub-statement failure was %s on login %q, SQLSTATE %s): %w",
					firstFailedKind, firstFailedLogin, firstFailedSQLState, commitErr)
			}
			return commitErr
		}
		return nil
	})
}

// ============================================================
// upsertOneContributor performs the full per-contributor unit of the
// batch upsert — savepoint-bracketed row INSERT with the v0.22.13
// rename-recovery branch, then the per-identity savepoint loop with
// login-history recording and the denormalized gh_*/gl_* backfill.
// Extracted verbatim from the former 438-line UpsertContributorBatch
// (v0.27.42, summary/18 Phase 4); behavior identical. A nil return
// means "done or skipped" (skips are already captured via captureErr);
// a non-nil return aborts the whole batch (savepoint machinery itself
// failed, so the transaction is unusable).
func (s *PostgresStore) upsertOneContributor(ctx context.Context, tx pgx.Tx, login string, contrib *model.Contributor, idents []model.ContributorIdentity, captureErr func(kind, login string, e error), spCounter *int) error {
	var cntrb_id string
	// v0.23.0: track whether this contributor's row was
	// rename-recovered so the contributor_login_history rows
	// for its identities get the correct source tag.
	var wasRenameRecovery bool

	// Idempotent upsert: INSERT with ON CONFLICT on the partial unique index.
	// If the login already exists, backfill empty fields and update tool_version.
	// This replaces the previous savepoint pattern — ON CONFLICT on partial
	// unique indexes works in PostgreSQL when the WHERE clause matches exactly.
	var createdAt any
	if !contrib.CreatedAt.IsZero() {
		createdAt = contrib.CreatedAt
	}

	// v0.22.0 deterministic-cntrb_id fix: compute PlatformUUID
	// from the contributor's first identity with a non-zero
	// platform user ID and pass it as $1. If no such identity
	// exists (email-only commit-author contributor), pass NULL
	// — the SQL falls back to gen_random_uuid() via COALESCE.
	//
	// ON CONFLICT (cntrb_login) DO UPDATE deliberately does NOT
	// SET cntrb_id, so existing rows with random UUIDs (from
	// pre-v0.22.0 collections) keep their cntrb_id and all FK
	// references stay valid. New contributors get deterministic
	// UUIDs going forward. Per CLAUDE.md v0.20.2 precedent
	// (rejection of the 16-table FK rewrite), aveloxis does NOT
	// migrate existing random cntrb_id values.
	var desiredCntrbID any
	for _, ident := range idents {
		if ident.UserID > 0 {
			desiredCntrbID = PlatformUUID(int(ident.Platform), ident.UserID).String()
			break
		}
	}

	// v0.22.13 (Fix for production WARN flood on 2026-05-18):
	// wrap each contributor's INSERT in a SAVEPOINT so a
	// single failure does not poison the rest of the batch
	// transaction. Without this, the very first 23505 marks
	// the entire tx as aborted and every subsequent statement
	// (including OTHER contributors' INSERTs and the final
	// Commit) fails with "current transaction is aborted."
	// Pre-fix the operator saw "count=420" batches drop
	// ~419 innocent contributors after a single rename
	// collision on the head of the batch.
	*spCounter++
	cntrbSP := fmt.Sprintf("cntrb_sp_%d", *spCounter)
	if _, spErr := tx.Exec(ctx, "SAVEPOINT "+cntrbSP); spErr != nil {
		return spErr
	}

	err := tx.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, cntrb_email, cntrb_full_name,
			 cntrb_company, cntrb_location, cntrb_canonical, cntrb_created_at,
			 tool_source, tool_version, data_source)
		VALUES (COALESCE($1::uuid, gen_random_uuid()),
		        $2,$3,$4,$5,$6,$7,$8,'aveloxis',$9,'GitHub API')
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO UPDATE SET
			cntrb_email = COALESCE(NULLIF(EXCLUDED.cntrb_email, ''), contributors.cntrb_email),
			cntrb_full_name = COALESCE(NULLIF(EXCLUDED.cntrb_full_name, ''), contributors.cntrb_full_name),
			cntrb_company = COALESCE(NULLIF(EXCLUDED.cntrb_company, ''), contributors.cntrb_company),
			cntrb_location = COALESCE(NULLIF(EXCLUDED.cntrb_location, ''), contributors.cntrb_location),
			cntrb_canonical = COALESCE(NULLIF(EXCLUDED.cntrb_canonical, ''), contributors.cntrb_canonical),
			cntrb_created_at = COALESCE(contributors.cntrb_created_at, EXCLUDED.cntrb_created_at),
			tool_version = EXCLUDED.tool_version,
			data_collection_date = NOW()
		RETURNING cntrb_id`,
		desiredCntrbID,
		contrib.Login, contrib.Email, contrib.FullName,
		contrib.Company, contrib.Location, contrib.Canonical, createdAt,
		ToolVersion,
	).Scan(&cntrb_id)
	if err != nil {
		// v0.22.13: classify the failure to decide between
		// rename-recovery (recoverable, this person already
		// exists under a different login) and skip (any
		// other 23505 or any other SQLSTATE).
		var pgErr *pgconn.PgError
		isRenameCollision := errors.As(err, &pgErr) &&
			pgErr.Code == "23505" &&
			pgErr.ConstraintName == "contributors_pkey" &&
			desiredCntrbID != nil

		// Always roll back to savepoint first to clear the
		// aborted-tx state — required before any further
		// statement in this tx (including the recovery
		// UPDATE) can run.
		if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+cntrbSP); rbErr != nil {
			return rbErr
		}

		if isRenameCollision {
			// v0.22.13 rename recovery: the deterministic UUID
			// already exists on a different row — same person,
			// renamed on GitHub (gh_user_id stable → cntrb_id
			// stable, but cntrb_login on the existing row
			// holds the OLDER observation). Update gh_login
			// on the existing row (the "current display name"
			// mirror) and backfill any empty profile fields.
			// cntrb_login is deliberately NOT modified — R2
			// (per docs/architecture/contributor-resolution.md):
			// cntrb_login is the durable audit trail of the
			// first observation. Same shape as v0.22.12's
			// RenameContributorGhLogin, inlined here so it
			// runs inside the batch tx.
			existingID := desiredCntrbID.(string)
			if _, updErr := tx.Exec(ctx, `
				UPDATE aveloxis_data.contributors
				SET gh_login        = $2,
				    cntrb_email     = COALESCE(NULLIF(cntrb_email, ''),     $3),
				    cntrb_full_name = COALESCE(NULLIF(cntrb_full_name, ''), $4),
				    cntrb_company   = COALESCE(NULLIF(cntrb_company, ''),   $5),
				    cntrb_location  = COALESCE(NULLIF(cntrb_location, ''),  $6),
				    cntrb_canonical = COALESCE(NULLIF(cntrb_canonical, ''), $7),
				    tool_version    = $8,
				    data_collection_date = NOW()
				WHERE cntrb_id = $1::uuid`,
				existingID, contrib.Login, contrib.Email,
				contrib.FullName, contrib.Company, contrib.Location,
				contrib.Canonical, ToolVersion,
			); updErr != nil {
				// Recovery UPDATE itself failed (rare —
				// would require some other constraint
				// violation on this row). Roll back, capture
				// diagnostic, skip this contributor.
				if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+cntrbSP); rbErr != nil {
					return rbErr
				}
				captureErr("contributors_rename_update", login, updErr)
				if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+cntrbSP); relErr != nil {
					return relErr
				}
				return nil
			}
			cntrb_id = existingID
			wasRenameRecovery = true
			s.logger.Info("contributor rename recovered in batch upsert",
				"cntrb_id", existingID,
				"new_gh_login", contrib.Login,
				"cause", "contributors_pkey collision on deterministic UUID — same gh_user_id, different login")
			// Fall through to identity-row inserts below.
		} else {
			// Non-rename failure (e.g. 23505 on
			// idx_contributors_login with cntrb_login='',
			// which ON CONFLICT WHERE cntrb_login!='' would
			// not catch; or any other SQLSTATE). Capture the
			// diagnostic and skip this contributor. Tx is
			// alive — savepoint rollback restored a clean
			// state, so the next contributor in the for loop
			// can still commit.
			captureErr("contributors_insert", login, err)
			if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+cntrbSP); relErr != nil {
				return relErr
			}
			return nil
		}
	}

	// Contributor row is now either freshly inserted,
	// upserted via ON CONFLICT (cntrb_login match), or
	// rename-recovered via UPDATE. Release the outer
	// savepoint before processing identities.
	if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+cntrbSP); relErr != nil {
		return relErr
	}

	// Upsert platform identities and backfill gh_*/gl_* columns.
	// v0.22.13: each identity is bracketed in its own SAVEPOINT so a
	// stale identity row (rare — would require a (platform_id,
	// platform_user_id) collision the ON CONFLICT clause somehow
	// missed, or any other constraint violation) doesn't poison
	// the rest of the batch tx. Pre-v0.22.13 a single bad
	// identity caused `break` and the gh_*/gl_* backfill UPDATE
	// to be skipped for ALL subsequent identities of this
	// contributor AND every later contributor in the batch.
	for _, ident := range idents {
		*spCounter++
		identSP := fmt.Sprintf("ident_sp_%d", *spCounter)
		if _, spErr := tx.Exec(ctx, "SAVEPOINT "+identSP); spErr != nil {
			return spErr
		}

		_, identErr := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email,
				 avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (platform_id, platform_user_id) DO UPDATE SET
				login = EXCLUDED.login,
				name = EXCLUDED.name,
				email = COALESCE(NULLIF(EXCLUDED.email,''), contributor_identities.email),
				avatar_url = EXCLUDED.avatar_url,
				profile_url = EXCLUDED.profile_url`,
			cntrb_id, int16(ident.Platform), ident.UserID, ident.Login, ident.Name,
			ident.Email, ident.AvatarURL, ident.URL, ident.NodeID, ident.Type, ident.IsAdmin,
		)
		if identErr != nil {
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+identSP); rbErr != nil {
				return rbErr
			}
			captureErr("contributor_identities_insert", login, identErr)
			if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+identSP); relErr != nil {
				return relErr
			}
			// Try the next identity — savepoint isolation
			// means one bad identity doesn't block the others.
			continue
		}

		// v0.23.0: record the (cntrb_id, platform_id, login)
		// observation into contributor_login_history. Source
		// reflects whether this iteration was a rename
		// recovery or a steady-state write — operators can
		// SQL-filter the history table on source to audit
		// rename events specifically.
		historySource := LoginSourceObservation
		if wasRenameRecovery {
			historySource = LoginSourceRenameRecovery
		}
		if histErr := recordLoginObservation(ctx, tx, cntrb_id, int16(ident.Platform), ident.Login, historySource); histErr != nil {
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+identSP); rbErr != nil {
				return rbErr
			}
			captureErr("contributor_login_history_insert", login, histErr)
			if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+identSP); relErr != nil {
				return relErr
			}
			continue
		}

		// Backfill denormalized gh_*/gl_* columns on the contributors row
		// from the identity data. This keeps the old Augur columns populated
		// for backward-compatible queries.
		// Mirror the identity onto the legacy denormalized columns;
		// errors are contained in this identity's savepoint below.
		backfillErr := backfillDenormalizedIdentity(ctx, tx, cntrb_id, ident)
		if backfillErr != nil {
			if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+identSP); rbErr != nil {
				return rbErr
			}
			captureErr("identity_denorm_backfill", login, backfillErr)
			if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+identSP); relErr != nil {
				return relErr
			}
			continue
		}

		// v0.22.13: identity savepoint released after the
		// gh_*/gl_* backfill (still under the same savepoint
		// scope so any unforeseen failure in the backfill
		// UPDATE is also contained).
		if _, relErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+identSP); relErr != nil {
			return relErr
		}
	}
	return nil
}

// backfillDenormalizedIdentity mirrors an identity's gh_*/gl_* fields
// onto the contributors row (the legacy Augur compatibility columns) —
// extracted from upsertOneContributor (v0.27.42, summary/18 Phase 4).
// Prefer-existing COALESCE semantics throughout. v0.27.36 history: the
// pre-fix Execs here were `_, _ =` discards (the v0.20.11 regression);
// the caller contains any returned error in the identity's savepoint
// scope.
func backfillDenormalizedIdentity(ctx context.Context, tx pgx.Tx, cntrbID string, ident model.ContributorIdentity) error {
	var backfillErr error
	if ident.Platform == model.PlatformGitHub && ident.UserID > 0 {
		_, backfillErr = tx.Exec(ctx, `
			UPDATE aveloxis_data.contributors SET
				gh_user_id = COALESCE(gh_user_id, $2),
				gh_login = COALESCE(NULLIF(gh_login,''), $3),
				gh_node_id = COALESCE(NULLIF(gh_node_id,''), $4),
				gh_avatar_url = COALESCE(NULLIF(gh_avatar_url,''), $5),
				gh_url = COALESCE(NULLIF(gh_url,''), $6),
				gh_html_url = COALESCE(NULLIF(gh_html_url,''), $6),
				gh_type = COALESCE(NULLIF(gh_type,''), $7),
				gh_site_admin = COALESCE(NULLIF(gh_site_admin,''), $8),
				gh_gravatar_id = COALESCE(NULLIF(gh_gravatar_id,''), $9),
				gh_followers_url = COALESCE(NULLIF(gh_followers_url,''), $10),
				gh_following_url = COALESCE(NULLIF(gh_following_url,''), $11),
				gh_gists_url = COALESCE(NULLIF(gh_gists_url,''), $12),
				gh_starred_url = COALESCE(NULLIF(gh_starred_url,''), $13),
				gh_subscriptions_url = COALESCE(NULLIF(gh_subscriptions_url,''), $14),
				gh_organizations_url = COALESCE(NULLIF(gh_organizations_url,''), $15),
				gh_repos_url = COALESCE(NULLIF(gh_repos_url,''), $16),
				gh_events_url = COALESCE(NULLIF(gh_events_url,''), $17),
				gh_received_events_url = COALESCE(NULLIF(gh_received_events_url,''), $18)
			WHERE cntrb_id = $1::uuid`,
			cntrbID, ident.UserID, ident.Login, ident.NodeID,
			ident.AvatarURL, ident.URL,
			ident.Type, strconv.FormatBool(ident.IsAdmin),
			ident.GravatarID, ident.FollowersURL, ident.FollowingURL,
			ident.GistsURL, ident.StarredURL, ident.SubscriptionsURL,
			ident.OrganizationsURL, ident.ReposURL, ident.EventsURL,
			ident.ReceivedEventsURL,
		)
	} else if ident.Platform == model.PlatformGitLab && ident.UserID > 0 {
		// gl_state added in v0.20.3 — Phase F closable gap.
		// GitLab's user state ("active", "blocked", "banned",
		// "deactivated") was previously parsed from JSON in
		// glUser.State / glMember.State but never plumbed
		// through to contributors.gl_state.
		_, backfillErr = tx.Exec(ctx, `
			UPDATE aveloxis_data.contributors SET
				gl_id = COALESCE(gl_id, $2),
				gl_username = COALESCE(NULLIF(gl_username,''), $3),
				gl_avatar_url = COALESCE(NULLIF(gl_avatar_url,''), $4),
				gl_web_url = COALESCE(NULLIF(gl_web_url,''), $5),
				gl_full_name = COALESCE(NULLIF(gl_full_name,''), $6),
				gl_state = COALESCE(NULLIF(gl_state,''), $7)
			WHERE cntrb_id = $1::uuid`,
			cntrbID, ident.UserID, ident.Login, ident.AvatarURL,
			ident.URL, ident.Name, ident.State,
		)
	}
	return backfillErr
}

// ============================================================
// Commits (facade/git)
// ============================================================

func (s *PostgresStore) UpsertCommit(ctx context.Context, commit *model.Commit) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commits
				(repo_id, cmt_commit_hash, cmt_author_name, cmt_author_raw_email,
				 cmt_author_email, cmt_author_date, cmt_author_affiliation,
				 cmt_committer_name, cmt_committer_raw_email, cmt_committer_email,
				 cmt_committer_date, cmt_committer_affiliation,
				 cmt_added, cmt_removed, cmt_whitespace, cmt_filename,
				 cmt_date_attempted, cmt_committer_timestamp, cmt_author_timestamp,
				 cmt_author_platform_username,
				 tool_source, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
			ON CONFLICT (repo_id, cmt_commit_hash, cmt_filename) DO NOTHING`,
			commit.RepoID, commit.Hash, commit.AuthorName, commit.AuthorRawEmail,
			commit.AuthorEmail, commit.AuthorDate, commit.AuthorAffiliation,
			commit.CommitterName, commit.CommitterRawEmail, commit.CommitterEmail,
			commit.CommitterDate, commit.CommitterAffiliation,
			commit.LinesAdded, commit.LinesRemoved, commit.LinesWhitespace, commit.Filename,
			time.Now(), commit.CommitterTimestamp, commit.AuthorTimestamp,
			commit.AuthorPlatformLogin,
			commit.Origin.ToolSource, commit.Origin.DataSource,
		)
		return err
	})
}

func (s *PostgresStore) InsertCommitParent(ctx context.Context, repoID int64, commitHash, parentHash string) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commit_parents (cmt_id, parent_id, tool_source, data_source)
			SELECT c.cmt_id, p.cmt_id, 'aveloxis-facade', 'git'
			FROM aveloxis_data.commits c, aveloxis_data.commits p
			WHERE c.repo_id = $1 AND c.cmt_commit_hash = $2
			  AND p.repo_id = $1 AND p.cmt_commit_hash = $3
			LIMIT 1
			ON CONFLICT (cmt_id, parent_id) DO NOTHING`,
			repoID, commitHash, parentHash,
		)
		return err
	})
}

func (s *PostgresStore) UpsertCommitMessage(ctx context.Context, msg *model.CommitMessage) error {
	msg.Message = SanitizeText(msg.Message)
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commit_messages
				(repo_id, cmt_msg, cmt_hash, tool_source, data_source)
			VALUES ($1,$2,$3,'aveloxis-facade','git')
			ON CONFLICT (repo_id, cmt_hash) DO UPDATE SET
				cmt_msg = EXCLUDED.cmt_msg,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			msg.RepoID, msg.Message, msg.Hash,
		)
		return err
	})
}

// Repo metadata
// ============================================================

func (s *PostgresStore) InsertRepoInfo(ctx context.Context, info *model.RepoInfo) error {
	// v0.28.18: counts the fetcher could not determine carry forward from
	// the latest prior snapshot instead of being stored as a fabricated 0.
	// The processor rotates the current row to repo_info_history BEFORE
	// this insert, so the prior snapshot may live in either table. A
	// lookup ERROR is not "no prior snapshot" (SR-5): it fails the insert
	// rather than storing zeros on bad information.
	if info.PRCountUnknown || info.IssuesCountUnknown {
		var prPRs, prOpen, prClosed, prMerged, prIssues, prIssuesClosed, prIssuesOpen int
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(pr_count, 0), COALESCE(prs_open, 0), COALESCE(prs_closed, 0), COALESCE(prs_merged, 0),
			       COALESCE(issues_count, 0), COALESCE(issues_closed, 0), COALESCE(open_issues, 0)
			FROM (
				SELECT pr_count, prs_open, prs_closed, prs_merged, issues_count, issues_closed, open_issues,
				       data_collection_date, repo_info_id
				FROM aveloxis_data.repo_info WHERE repo_id = $1
				UNION ALL
				SELECT pr_count, prs_open, prs_closed, prs_merged, issues_count, issues_closed, open_issues,
				       data_collection_date, repo_info_id
				FROM aveloxis_data.repo_info_history WHERE repo_id = $1
			) prior
			ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC
			LIMIT 1`, info.RepoID,
		).Scan(&prPRs, &prOpen, &prClosed, &prMerged, &prIssues, &prIssuesClosed, &prIssuesOpen)
		switch {
		case err == nil:
			if info.PRCountUnknown {
				info.PRCount, info.PRsOpen, info.PRsClosed, info.PRsMerged = prPRs, prOpen, prClosed, prMerged
			}
			if info.IssuesCountUnknown {
				// The whole issue triple travels together — open_issues
				// too, or the snapshot reads total=N/closed=M/open=0.
				info.IssuesCount, info.IssuesClosed, info.OpenIssues = prIssues, prIssuesClosed, prIssuesOpen
			}
			s.logger.Info("repo_info counts unavailable from the forge — prior snapshot's counts carried forward",
				"repo_id", info.RepoID, "pr_count_unknown", info.PRCountUnknown, "issues_count_unknown", info.IssuesCountUnknown,
				"pr_count", info.PRCount, "issues_count", info.IssuesCount)
		case errors.Is(err, pgx.ErrNoRows):
			// The store enforces the coherent zero set itself (SR-18) —
			// whatever the fetcher left in the fields.
			if info.PRCountUnknown {
				info.PRCount, info.PRsOpen, info.PRsClosed, info.PRsMerged = 0, 0, 0, 0
			}
			if info.IssuesCountUnknown {
				// The fetcher's OpenIssues is the project payload's count
				// on this path; without the totals it would store the
				// incoherent 0/0/N triple. The whole triple is 0.
				info.IssuesCount, info.IssuesClosed, info.OpenIssues = 0, 0, 0
			}
			s.logger.Warn("repo_info counts unavailable from the forge and no prior snapshot exists — counts stored as 0 until a fetch succeeds",
				"repo_id", info.RepoID, "pr_count_unknown", info.PRCountUnknown, "issues_count_unknown", info.IssuesCountUnknown)
		default:
			return fmt.Errorf("repo_info prior-snapshot counts for repo %d: %w", info.RepoID, err)
		}
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		// Schema uses TEXT for boolean fields (matching Augur's varchar), so convert.
		boolStr := func(b bool) string {
			if b {
				return "true"
			}
			return "false"
		}
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_info
				(repo_id, last_updated, issues_enabled, prs_enabled, wiki_enabled, pages_enabled,
				 fork_count, star_count, watcher_count, open_issues, committer_count,
				 commit_count, issues_count, issues_closed, pr_count, prs_open, prs_closed, prs_merged,
				 default_branch, license,
				 issue_contributors_count, changelog_file, contributing_file, license_file,
				 code_of_conduct_file, security_issue_file, security_audit_file,
				 status, keywords, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
				$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)`,
			info.RepoID, info.LastUpdated,
			boolStr(info.IssuesEnabled), boolStr(info.PRsEnabled),
			boolStr(info.WikiEnabled), boolStr(info.PagesEnabled),
			info.ForkCount, info.StarCount, info.WatcherCount, info.OpenIssues,
			info.CommitterCount, info.CommitCount, info.IssuesCount, info.IssuesClosed,
			info.PRCount, info.PRsOpen, info.PRsClosed, info.PRsMerged,
			info.DefaultBranch, info.License,
			info.IssueContributorsCount, info.ChangelogFile, info.ContributingFile, info.LicenseFile,
			info.CodeOfConductFile, info.SecurityIssueFile, info.SecurityAuditFile,
			info.Status, info.Keywords, info.Origin.DataSource,
		)
		return err
	})
}

func (s *PostgresStore) UpsertRepoClone(ctx context.Context, clone *model.RepoClone) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_clones
				(repo_id, clone_timestamp, total_clones, unique_clones, data_source)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (repo_id, clone_timestamp) DO UPDATE SET
				total_clones = EXCLUDED.total_clones,
				unique_clones = EXCLUDED.unique_clones,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`,
			clone.RepoID, clone.Timestamp, clone.TotalClones, clone.UniqueClones,
			clone.Origin.DataSource,
		)
		return err
	})
}

// ============================================================
// Collection status
// ============================================================

func (s *PostgresStore) GetCollectionStatus(ctx context.Context, repoID int64) (*CollectionState, error) {
	state := &CollectionState{RepoID: repoID}
	err := s.pool.QueryRow(ctx, `
		SELECT core_status, COALESCE(core_task_id,''),
		       core_data_last_collected::text,
		       secondary_status, COALESCE(secondary_task_id,''),
		       secondary_data_last_collected::text,
		       facade_status, COALESCE(facade_task_id,''),
		       facade_data_last_collected::text,
		       COALESCE(ml_status,'Pending'), COALESCE(ml_task_id,''),
		       ml_data_last_collected::text
		FROM aveloxis_ops.collection_status
		WHERE repo_id = $1`, repoID,
	).Scan(
		&state.CoreStatus, &state.CoreTaskID,
		&state.CoreDataLastCollected,
		&state.SecondaryStatus, &state.SecondaryTaskID,
		&state.SecondaryDataLastCollected,
		&state.FacadeStatus, &state.FacadeTaskID,
		&state.FacadeDataLastCollected,
		&state.MLStatus, &state.MLTaskID,
		&state.MLDataLastCollected,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		state.CoreStatus = "Pending"
		state.SecondaryStatus = "Pending"
		state.FacadeStatus = "Pending"
		state.MLStatus = "Pending"
		return state, nil
	}
	return state, err
}

func (s *PostgresStore) UpdateCollectionStatus(ctx context.Context, state *CollectionState) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_status
				(repo_id, core_status, secondary_status, facade_status, ml_status,
				 core_data_last_collected, secondary_data_last_collected,
				 facade_data_last_collected, ml_data_last_collected, updated_at)
			VALUES ($1, $2, $3, $4, $5,
				CASE WHEN $6::text IS NOT NULL THEN $6::timestamptz ELSE NULL END,
				CASE WHEN $7::text IS NOT NULL THEN $7::timestamptz ELSE NULL END,
				CASE WHEN $8::text IS NOT NULL THEN $8::timestamptz ELSE NULL END,
				CASE WHEN $9::text IS NOT NULL THEN $9::timestamptz ELSE NULL END,
				NOW())
			ON CONFLICT (repo_id) DO UPDATE SET
				core_status = COALESCE(NULLIF(EXCLUDED.core_status,''), collection_status.core_status),
				secondary_status = COALESCE(NULLIF(EXCLUDED.secondary_status,''), collection_status.secondary_status),
				facade_status = COALESCE(NULLIF(EXCLUDED.facade_status,''), collection_status.facade_status),
				ml_status = COALESCE(NULLIF(EXCLUDED.ml_status,''), collection_status.ml_status),
				core_data_last_collected = COALESCE(EXCLUDED.core_data_last_collected, collection_status.core_data_last_collected),
				secondary_data_last_collected = COALESCE(EXCLUDED.secondary_data_last_collected, collection_status.secondary_data_last_collected),
				facade_data_last_collected = COALESCE(EXCLUDED.facade_data_last_collected, collection_status.facade_data_last_collected),
				ml_data_last_collected = COALESCE(EXCLUDED.ml_data_last_collected, collection_status.ml_data_last_collected),
				updated_at = NOW()`,
			state.RepoID, state.CoreStatus, state.SecondaryStatus,
			state.FacadeStatus, state.MLStatus,
			state.CoreDataLastCollected, state.SecondaryDataLastCollected,
			state.FacadeDataLastCollected, state.MLDataLastCollected,
		)
		return err
	})
}
