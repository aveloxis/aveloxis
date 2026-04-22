package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// PostgresStore implements Store using pgx connection pool.
type PostgresStore struct {
	pool             *pgxpool.Pool
	logger           *slog.Logger
	matviewOnStartup bool // whether to refresh materialized views during migration
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

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &PostgresStore{pool: pool, logger: logger}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

// SetMatviewOnStartup controls whether materialized views are refreshed during migration.
func (s *PostgresStore) SetMatviewOnStartup(enabled bool) {
	s.matviewOnStartup = enabled
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
	// Create new group.
	err = s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name, rg_type, rg_website, rg_description)
		VALUES ($1, $2, $3, $4)
		RETURNING repo_group_id`,
		name, rgType, website, fmt.Sprintf("Auto-created from %s", website),
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) UpsertRepo(ctx context.Context, r *model.Repo) (int64, error) {
	// Normalize the repo slug at the write boundary so a ".git" suffix never
	// reaches the DB. API URLs built from repo_name (/repos/{owner}/{name}/...)
	// 404 when the slug has a ".git" suffix.
	r.Name = model.NormalizeRepoName(r.Name)
	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		// Ensure a default repo group exists if no group is specified.
		groupID := r.GroupID
		if groupID == 0 {
			err := s.pool.QueryRow(ctx, `
				INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
				VALUES ('Default', 'Auto-created default repo group')
				ON CONFLICT DO NOTHING
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
		var createdAt, updatedAt interface{}
		if !r.CreatedAt.IsZero() {
			createdAt = r.CreatedAt
		}
		if !r.UpdatedAt.IsZero() {
			updatedAt = r.UpdatedAt
		}

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
				forked_from = EXCLUDED.forked_from,
				repo_archived = EXCLUDED.repo_archived,
				updated_at = COALESCE(EXCLUDED.updated_at, repos.updated_at),
				data_collection_date = NOW()
			RETURNING repo_id`,
			groupID, int16(r.Platform), r.GitURL, r.Name, r.Owner,
			r.Description, r.PrimaryLanguage, r.ForkedFrom, r.Archived,
			r.PlatformID, createdAt, updatedAt, r.Platform.String()+" API",
		).Scan(&id)
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

// FindReviewDBID looks up the aveloxis DB pr_review_id from a platform review ID.
func (s *PostgresStore) FindReviewDBID(ctx context.Context, platformReviewID int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT pr_review_id FROM aveloxis_data.pull_request_reviews WHERE platform_review_id = $1`,
		platformReviewID).Scan(&id)
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
func (s *PostgresStore) FindRepoByURL(ctx context.Context, gitURL string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1`, gitURL,
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return id, nil
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
	// Extract the path portions for find-and-replace.
	// e.g., "https://github.com/old-org/old-repo" -> "old-org/old-repo"
	oldPath := extractRepoPath(oldURL)
	newPath := extractRepoPath(newURL)

	if oldPath == "" || newPath == "" || oldPath == newPath {
		// Just update the repo_git URL.
		return s.UpdateRepoURL(ctx, repoID, newURL)
	}

	// Update repo_git first.
	if err := s.UpdateRepoURL(ctx, repoID, newURL); err != nil {
		return err
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
		_, err := s.pool.Exec(ctx, fmt.Sprintf(
			`UPDATE %s SET %s = REPLACE(%s, $1, $2) WHERE repo_id = $3 AND %s LIKE '%%' || $1 || '%%'`,
			u.table, u.column, u.column, u.column),
			oldPath, newPath, repoID)
		if err != nil {
			// Non-fatal — some tables may not have matching rows.
			continue
		}
	}

	return nil
}

// extractRepoPath extracts "owner/repo" from a URL like "https://github.com/owner/repo".
func extractRepoPath(u string) string {
	for _, prefix := range []string{"https://", "http://"} {
		u = strings.TrimPrefix(u, prefix)
	}
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	// Remove host: "github.com/owner/repo" -> "owner/repo"
	if idx := strings.Index(u, "/"); idx >= 0 {
		return u[idx+1:]
	}
	return ""
}

// UpdateRepoURL changes the git URL, owner, and name of a repo (e.g., after a redirect).
// Extracts the new owner/name from the URL so the dashboard and API show correct values.
func (s *PostgresStore) UpdateRepoURL(ctx context.Context, repoID int64, newURL string) error {
	// Parse owner/name from the new URL.
	newURL = strings.TrimSuffix(strings.TrimSuffix(newURL, "/"), ".git")
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(newURL, "https://"), "http://"), "/")
	owner := ""
	name := ""
	if len(parts) >= 3 {
		name = parts[len(parts)-1]
		owner = strings.Join(parts[1:len(parts)-1], "/")
	}
	// Defense in depth: even after TrimSuffix above, normalize the extracted
	// slug so every write path produces the same canonical form.
	name = model.NormalizeRepoName(name)

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
					data_collection_date = NOW()`,
				issueID, repoID, l.PlatformID, l.NodeID,
				l.Text, l.Description, l.Color, l.Origin.DataSource,
			)
		}
		return s.pool.SendBatch(ctx, batch).Close()
	})
}

func (s *PostgresStore) UpsertIssueAssignees(ctx context.Context, issueID, repoID int64, assignees []model.IssueAssignee) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, a := range assignees {
			batch.Queue(`
				INSERT INTO aveloxis_data.issue_assignees
					(issue_id, repo_id, platform_assignee_id, platform_node_id, data_source)
				VALUES ($1,$2,$3,$4,$5)
				ON CONFLICT (issue_id, platform_assignee_id) DO NOTHING`,
				issueID, repoID, a.PlatformSrcID, a.PlatformNodeID, a.Origin.DataSource,
			)
		}
		return s.pool.SendBatch(ctx, batch).Close()
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
					data_collection_date = NOW()`,
				prID, repoID, l.PlatformID, l.NodeID,
				l.Name, l.Description, l.Color, l.IsDefault, l.Origin.DataSource,
			)
		}
		return s.pool.SendBatch(ctx, batch).Close()
	})
}

func (s *PostgresStore) UpsertPRAssignees(ctx context.Context, prID, repoID int64, assignees []model.PullRequestAssignee) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, a := range assignees {
			batch.Queue(`
				INSERT INTO aveloxis_data.pull_request_assignees
					(pull_request_id, repo_id, platform_assignee_id, data_source)
				VALUES ($1,$2,$3,$4)
				ON CONFLICT (pull_request_id, platform_assignee_id) DO NOTHING`,
				prID, repoID, a.PlatformSrcID, a.Origin.DataSource,
			)
		}
		return s.pool.SendBatch(ctx, batch).Close()
	})
}

func (s *PostgresStore) UpsertPRReviewers(ctx context.Context, prID, repoID int64, reviewers []model.PullRequestReviewer) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		for _, r := range reviewers {
			batch.Queue(`
				INSERT INTO aveloxis_data.pull_request_reviewers
					(pull_request_id, repo_id, platform_reviewer_id, data_source)
				VALUES ($1,$2,$3,$4)
				ON CONFLICT (pull_request_id, platform_reviewer_id) DO NOTHING`,
				prID, repoID, r.PlatformSrcID, r.Origin.DataSource,
			)
		}
		return s.pool.SendBatch(ctx, batch).Close()
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
			// Use the platform_review_id as the platform_msg_id with a review-specific
			// offset to avoid collisions with issue/PR comment message IDs.
			// Assumption: review IDs don't overlap with comment IDs on the same platform.
			err = tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.messages
					(repo_id, platform_msg_id, platform_id, node_id,
					 cntrb_id, msg_text, msg_timestamp, data_source)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (platform_msg_id, platform_id) DO UPDATE SET
					msg_text = EXCLUDED.msg_text,
					cntrb_id = COALESCE(EXCLUDED.cntrb_id, messages.cntrb_id),
					data_collection_date = NOW()
				RETURNING msg_id`,
				review.RepoID, review.PlatformReviewID, int16(review.PlatformID),
				review.NodeID, review.ContributorID, review.Body,
				NullTime(review.SubmittedAt), review.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			// Create bridge row linking review to message.
			_, err = tx.Exec(ctx, `
				INSERT INTO aveloxis_data.pull_request_review_message_ref
					(pr_review_id, repo_id, msg_id, pr_review_src_id, pr_review_msg_node_id)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT DO NOTHING`,
				reviewID, review.RepoID, msgID, review.PlatformReviewID, review.NodeID)
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
				(pull_request_id, repo_id, head_or_base, meta_label, meta_ref, meta_sha, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (pull_request_id, head_or_base) DO UPDATE SET
				meta_label = EXCLUDED.meta_label,
				meta_ref = EXCLUDED.meta_ref,
				meta_sha = EXCLUDED.meta_sha,
				data_collection_date = NOW()
			RETURNING pr_meta_id`,
			meta.PRID, meta.RepoID, meta.HeadOrBase, meta.Label, meta.Ref, meta.SHA,
			meta.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
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
		return s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.messages
				(repo_id, platform_msg_id, platform_id, node_id,
				 cntrb_id, msg_text, msg_timestamp, data_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (platform_msg_id, platform_id) DO UPDATE SET
				msg_text = EXCLUDED.msg_text,
				cntrb_id = COALESCE(EXCLUDED.cntrb_id, messages.cntrb_id),
				data_collection_date = NOW()
			RETURNING msg_id`,
			msg.RepoID, msg.PlatformMsgID, int16(msg.PlatformID), msg.NodeID,
			msg.ContributorID, msg.Text, NullTime(msg.Timestamp), msg.Origin.DataSource,
		).Scan(&id)
	})
	return id, err
}

func (s *PostgresStore) UpsertIssueMessageRef(ctx context.Context, ref *model.IssueMessageRef) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issue_message_ref
				(issue_id, repo_id, msg_id, platform_src_id, platform_node_id)
			VALUES ($1,$2,$3,$4,$5)
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
				(pull_request_id, repo_id, msg_id, platform_src_id, platform_node_id)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (pull_request_id, msg_id) DO NOTHING`,
			ref.PRID, ref.RepoID, ref.MsgID, ref.PlatformSrcID, ref.PlatformNodeID,
		)
		return err
	})
}

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
		return err
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
			err := tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.messages
					(repo_id, platform_msg_id, platform_id, node_id,
					 cntrb_id, msg_text, msg_timestamp, data_source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
				ON CONFLICT (platform_msg_id, platform_id) DO UPDATE SET
					msg_text = EXCLUDED.msg_text,
					cntrb_id = COALESCE(EXCLUDED.cntrb_id, messages.cntrb_id),
					data_collection_date = NOW()
				RETURNING msg_id`,
				m.Message.RepoID, m.Message.PlatformMsgID, int16(m.Message.PlatformID),
				m.Message.NodeID, m.Message.ContributorID, m.Message.Text, NullTime(m.Message.Timestamp),
				m.Message.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			if m.IssueRef != nil {
				m.IssueRef.MsgID = msgID
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_data.issue_message_ref
						(issue_id, repo_id, msg_id, platform_src_id, platform_node_id)
					VALUES ($1,$2,$3,$4,$5)
					ON CONFLICT (issue_id, msg_id) DO NOTHING`,
					m.IssueRef.IssueID, m.IssueRef.RepoID, msgID,
					m.IssueRef.PlatformSrcID, m.IssueRef.PlatformNodeID,
				)
				if err != nil {
					return err
				}
			}
			if m.PRRef != nil {
				m.PRRef.MsgID = msgID
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_data.pull_request_message_ref
						(pull_request_id, repo_id, msg_id, platform_src_id, platform_node_id)
					VALUES ($1,$2,$3,$4,$5)
					ON CONFLICT (pull_request_id, msg_id) DO NOTHING`,
					m.PRRef.PRID, m.PRRef.RepoID, msgID,
					m.PRRef.PlatformSrcID, m.PRRef.PlatformNodeID,
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
			err := tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.messages
					(repo_id, platform_msg_id, platform_id, node_id,
					 cntrb_id, msg_text, msg_timestamp, data_source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
				ON CONFLICT (platform_msg_id, platform_id) DO UPDATE SET
					msg_text = EXCLUDED.msg_text,
					cntrb_id = COALESCE(EXCLUDED.cntrb_id, messages.cntrb_id),
					data_collection_date = NOW()
				RETURNING msg_id`,
				rc.Message.RepoID, rc.Message.PlatformMsgID, int16(rc.Message.PlatformID),
				rc.Message.NodeID, rc.Message.ContributorID, rc.Message.Text, NullTime(rc.Message.Timestamp),
				rc.Message.Origin.DataSource,
			).Scan(&msgID)
			if err != nil {
				return err
			}

			rc.Comment.MsgID = msgID
			// Use NULL for pr_review_id when it's zero (review not yet in DB).
			var reviewID interface{}
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

		for login, contrib := range merged {
			var cntrb_id string

			// Idempotent upsert: INSERT with ON CONFLICT on the partial unique index.
			// If the login already exists, backfill empty fields and update tool_version.
			// This replaces the previous savepoint pattern — ON CONFLICT on partial
			// unique indexes works in PostgreSQL when the WHERE clause matches exactly.
			var createdAt interface{}
			if !contrib.CreatedAt.IsZero() {
				createdAt = contrib.CreatedAt
			}

			err := tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.contributors
					(cntrb_login, cntrb_email, cntrb_full_name,
					 cntrb_company, cntrb_location, cntrb_canonical, cntrb_created_at,
					 tool_source, tool_version, data_source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,'aveloxis',$8,'GitHub API')
				ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO UPDATE SET
					cntrb_email = COALESCE(NULLIF(EXCLUDED.cntrb_email, ''), contributors.cntrb_email),
					cntrb_full_name = COALESCE(NULLIF(EXCLUDED.cntrb_full_name, ''), contributors.cntrb_full_name),
					cntrb_company = COALESCE(NULLIF(EXCLUDED.cntrb_company, ''), contributors.cntrb_company),
					cntrb_location = COALESCE(NULLIF(EXCLUDED.cntrb_location, ''), contributors.cntrb_location),
					cntrb_created_at = COALESCE(contributors.cntrb_created_at, EXCLUDED.cntrb_created_at),
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()
				RETURNING cntrb_id`,
				contrib.Login, contrib.Email, contrib.FullName,
				contrib.Company, contrib.Location, contrib.Canonical, createdAt,
				ToolVersion,
			).Scan(&cntrb_id)
			if err != nil {
				// Duplicate key (23505) is a normal race condition between concurrent
				// workers — the contributor exists either way. Log at Debug, not Warn.
				s.logger.Debug("contributor upsert failed", "login", login, "error", err)
				continue
			}

			// Upsert platform identities and backfill gh_*/gl_* columns.
			for _, ident := range identMap[login] {
				_, _ = tx.Exec(ctx, `
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

				// Backfill denormalized gh_*/gl_* columns on the contributors row
				// from the identity data. This keeps the old Augur columns populated
				// for backward-compatible queries.
				if ident.Platform == model.PlatformGitHub && ident.UserID > 0 {
					_, _ = tx.Exec(ctx, `
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
						cntrb_id, ident.UserID, ident.Login, ident.NodeID,
						ident.AvatarURL, ident.URL,
						ident.Type, fmt.Sprintf("%v", ident.IsAdmin),
						ident.GravatarID, ident.FollowersURL, ident.FollowingURL,
						ident.GistsURL, ident.StarredURL, ident.SubscriptionsURL,
						ident.OrganizationsURL, ident.ReposURL, ident.EventsURL,
						ident.ReceivedEventsURL,
					)
				} else if ident.Platform == model.PlatformGitLab && ident.UserID > 0 {
					_, _ = tx.Exec(ctx, `
						UPDATE aveloxis_data.contributors SET
							gl_id = COALESCE(gl_id, $2),
							gl_username = COALESCE(NULLIF(gl_username,''), $3),
							gl_avatar_url = COALESCE(NULLIF(gl_avatar_url,''), $4),
							gl_web_url = COALESCE(NULLIF(gl_web_url,''), $5),
							gl_full_name = COALESCE(NULLIF(gl_full_name,''), $6)
						WHERE cntrb_id = $1::uuid`,
						cntrb_id, ident.UserID, ident.Login, ident.AvatarURL,
						ident.URL, ident.Name,
					)
				}
			}
		}

		return tx.Commit(ctx)
	})
}

// ============================================================
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
				data_collection_date = NOW()`,
			msg.RepoID, msg.Message, msg.Hash,
		)
		return err
	})
}

// Repo metadata
// ============================================================

func (s *PostgresStore) InsertRepoInfo(ctx context.Context, info *model.RepoInfo) error {
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
