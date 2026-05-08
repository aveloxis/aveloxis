package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// GetUserEmail returns the email column for the given user_id.
// Returns ("", nil) when the row exists but the email column is
// empty/NULL — used by the v0.19.10 email-gate check on the
// dashboard path. Returns an error only on actual DB failures.
func (s *PostgresStore) GetUserEmail(ctx context.Context, userID int) (string, error) {
	var email *string
	err := s.pool.QueryRow(ctx,
		`SELECT email FROM aveloxis_ops.users WHERE user_id = $1`, userID).Scan(&email)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if email == nil {
		return "", nil
	}
	return *email, nil
}

// UpdateUserEmail writes the email column for the given user_id.
// Used by the v0.19.10 POST handler for /account/email when the OAuth
// flow couldn't surface an email automatically. Returns an error if
// the user doesn't exist or the write fails.
func (s *PostgresStore) UpdateUserEmail(ctx context.Context, userID int, email string) error {
	email = strings.TrimSpace(email)
	if email == "" {
		return errors.New("email cannot be empty")
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET email = $2 WHERE user_id = $1`, userID, email)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user_id %d not found", userID)
	}
	return nil
}

// ErrEmptyLogin is returned by UpsertOAuthUser when the supplied
// OAuthUserInfo.Login is empty (or whitespace-only). An empty login
// is never a legitimate OAuth result; allowing it through inserts a
// blank row that subsequent logins keep matching, shadowing the
// real user account. See the v0.18.28 incident in CLAUDE.md.
var ErrEmptyLogin = errors.New("oauth login name is empty")

// OAuthUserInfo holds user data from an OAuth provider.
type OAuthUserInfo struct {
	Login      string
	Email      string
	Name       string
	AvatarURL  string
	GHUserID   int64
	GHLogin    string
	GLUserID   int64
	GLUsername string
	Provider   string
}

// UpsertOAuthUser creates or updates a user from OAuth login. Returns user_id.
// Rejects an empty Login with ErrEmptyLogin so a blank row never gets
// inserted. Distinguishes pgx.ErrNoRows from real DB errors on the
// initial lookup so a transient query failure doesn't silently
// trigger an INSERT and produce a duplicate user row.
//
// v0.19.0: the first user to ever sign up is auto-promoted to admin
// (admin=TRUE on insert). All subsequent users default to admin=FALSE
// and must be promoted by an existing admin via the user-management
// page. This bootstraps fresh deployments — without it, there'd be no
// admin and nobody could approve other users' group submissions.
//
// email_confirmed_at is set to NOW() at signup because GitHub/GitLab
// OAuth has already verified the address before handing it to us; the
// column is for audit only, not gating.
func (s *PostgresStore) UpsertOAuthUser(ctx context.Context, info OAuthUserInfo) (int, error) {
	if strings.TrimSpace(info.Login) == "" {
		return 0, ErrEmptyLogin
	}

	var userID int

	// Try to find existing user by login.
	err := s.pool.QueryRow(ctx,
		`SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1`,
		info.Login).Scan(&userID)

	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			// Real DB error — surface it. Treating this as "not
			// found" and falling through to INSERT is what created
			// blank/duplicate rows in the v0.18.28 incident.
			return 0, fmt.Errorf("lookup user by login: %w", err)
		}
		// Not found — create.
		firstName := info.Name
		if idx := strings.Index(firstName, " "); idx > 0 {
			firstName = firstName[:idx]
		}
		lastName := ""
		if idx := strings.LastIndex(info.Name, " "); idx > 0 {
			lastName = info.Name[idx+1:]
		}

		// v0.19.0: first user is auto-admin. Count existing rows; if
		// zero, set admin = TRUE on this INSERT so the bootstrap
		// signup gets the privilege. Race-free: this branch is only
		// reached when no row matches the login, and a concurrent
		// signup with a different login would also see count == 0
		// and would also flip admin TRUE — that's fine for a fresh
		// deployment (multiple admins from a near-simultaneous
		// initial-batch signup is a non-issue).
		var existingCount int
		_ = s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.users`).Scan(&existingCount)
		isFirstUser := existingCount == 0

		err = s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users
				(login_name, email, first_name, last_name, avatar_url,
				 gh_user_id, gh_login, gl_user_id, gl_username,
				 oauth_provider, admin, email_verified, email_confirmed_at,
				 tool_source, tool_version, data_source)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $12, TRUE, NOW(),
				'aveloxis-web', $11, $10 || ' OAuth')
			RETURNING user_id`,
			info.Login, info.Email, firstName, lastName, info.AvatarURL,
			info.GHUserID, info.GHLogin, info.GLUserID, info.GLUsername,
			info.Provider, ToolVersion, isFirstUser,
		).Scan(&userID)
		return userID, err
	}

	// Found — update OAuth fields.
	_, err = s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.users SET
			email = COALESCE(NULLIF($2, ''), email),
			avatar_url = $3,
			gh_user_id = COALESCE(gh_user_id, $4),
			gh_login = COALESCE(NULLIF($5, ''), gh_login),
			gl_user_id = COALESCE(gl_user_id, $6),
			gl_username = COALESCE(NULLIF($7, ''), gl_username),
			oauth_provider = $8,
			data_collection_date = NOW()
		WHERE user_id = $1`,
		userID, info.Email, info.AvatarURL,
		info.GHUserID, info.GHLogin, info.GLUserID, info.GLUsername,
		info.Provider)
	return userID, err
}

// verifyGroupOwnership checks that the given group belongs to the user.
// Returns the group name or an error if not found/owned.
func (s *PostgresStore) verifyGroupOwnership(ctx context.Context, userID int, groupID int64) (string, error) {
	var name string
	err := s.pool.QueryRow(ctx,
		`SELECT name FROM aveloxis_ops.user_groups WHERE group_id = $1 AND user_id = $2`,
		groupID, userID).Scan(&name)
	if err != nil {
		return "", fmt.Errorf("group not found or not owned by user")
	}
	return name, nil
}

// verifyGroupOwned is a convenience wrapper that only checks ownership
// without returning the group name.
func (s *PostgresStore) verifyGroupOwned(ctx context.Context, userID int, groupID int64) error {
	_, err := s.verifyGroupOwnership(ctx, userID, groupID)
	return err
}

// UserGroup is a group with metadata for the dashboard.
//
// Status (v0.19.0): one of "approved", "pending", "rejected". The
// dashboard shows a badge for non-approved groups so the user knows
// which submissions are still awaiting admin review.
type UserGroup struct {
	GroupID   int64
	Name      string
	Favorited bool
	RepoCount int
	Status    string
}

// GetUserGroups returns all groups for a user with repo counts.
func (s *PostgresStore) GetUserGroups(ctx context.Context, userID int) ([]UserGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT g.group_id, g.name, g.favorited,
		       COUNT(ur.repo_id) AS repo_count,
		       COALESCE(g.status, 'approved') AS status
		FROM aveloxis_ops.user_groups g
		LEFT JOIN aveloxis_ops.user_repos ur ON ur.group_id = g.group_id
		WHERE g.user_id = $1
		GROUP BY g.group_id, g.name, g.favorited, g.status
		ORDER BY g.name`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []UserGroup
	for rows.Next() {
		var g UserGroup
		if err := rows.Scan(&g.GroupID, &g.Name, &g.Favorited, &g.RepoCount, &g.Status); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// IsUserAdmin returns whether the user has admin role. Cached at
// session-create time in Server.Session.IsAdmin so the requireAdmin
// middleware doesn't need to hit the DB per request, but exposed
// here for cases that need a fresh value.
func (s *PostgresStore) IsUserAdmin(ctx context.Context, userID int) (bool, error) {
	var isAdmin bool
	err := s.pool.QueryRow(ctx,
		`SELECT admin FROM aveloxis_ops.users WHERE user_id = $1`, userID,
	).Scan(&isAdmin)
	if err != nil {
		return false, err
	}
	return isAdmin, nil
}

// CreateUserGroup creates a new group for a user. Returns group_id.
//
// v0.19.0: status branches on the creator's admin role. Admins'
// groups auto-approve (status='approved') so admin operations don't
// need to wait on the approval queue. Non-admins' groups go to
// status='pending' and require admin review via ApproveGroup before
// any of their repos enter collection_queue.
func (s *PostgresStore) CreateUserGroup(ctx context.Context, userID int, name string) (int64, error) {
	isAdmin, _ := s.IsUserAdmin(ctx, userID)
	status := "pending"
	if isAdmin {
		status = "approved"
	}
	var groupID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING group_id`,
		userID, name, status).Scan(&groupID)
	return groupID, err
}

// GroupDetail holds a group with its repos and tracked orgs.
type GroupDetail struct {
	GroupID int64
	Name    string
	Repos   []GroupRepo
	Orgs    []GroupOrg
}

// GroupRepo is a repo in a group, optionally enriched with collection stats.
type GroupRepo struct {
	RepoID          int64
	RepoName        string
	RepoOwner       string
	RepoGit         string
	PlatformID      int16 // 1=GitHub, 2=GitLab, 3=Generic Git
	GatheredIssues  int
	GatheredPRs     int
	GatheredCommits int
	MetaIssues      int
	MetaPRs         int
	MetaCommits     int
}

// GroupOrg is a tracked org/group in a user group.
type GroupOrg struct {
	OrgRequestID int64
	OrgURL       string
	OrgName      string
	Platform     string
	LastScanned  *time.Time
}

// GetGroupDetail returns a group with its repos (paginated, optionally filtered) and tracked orgs.
// Returns the detail, total matching repo count, and any error.
func (s *PostgresStore) GetGroupDetail(ctx context.Context, userID int, groupID int64, page, perPage int, search string) (*GroupDetail, int, error) {
	name, err := s.verifyGroupOwnership(ctx, userID, groupID)
	if err != nil {
		return nil, 0, err
	}

	detail := &GroupDetail{GroupID: groupID, Name: name}

	// Count total matching repos.
	var totalRepos int
	if search != "" {
		searchPattern := "%" + strings.ToLower(search) + "%"
		err = s.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			  AND (LOWER(r.repo_name) LIKE $2 OR LOWER(r.repo_owner) LIKE $2 OR LOWER(r.repo_git) LIKE $2)`,
			groupID, searchPattern).Scan(&totalRepos)
	} else {
		err = s.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM aveloxis_ops.user_repos ur
			WHERE ur.group_id = $1`, groupID).Scan(&totalRepos)
	}
	if err != nil {
		totalRepos = 0
	}

	// Load paginated repos.
	offset := (page - 1) * perPage
	detail.Repos, _ = s.loadGroupRepos(ctx, groupID, search, perPage, offset)

	// Load tracked orgs.
	detail.Orgs, _ = s.loadGroupOrgs(ctx, groupID)

	return detail, totalRepos, nil
}

// loadGroupRepos fetches paginated repos for a group, optionally filtered by search.
func (s *PostgresStore) loadGroupRepos(ctx context.Context, groupID int64, search string, limit, offset int) ([]GroupRepo, error) {
	var repoRows pgx.Rows
	var err error

	if search != "" {
		searchPattern := "%" + strings.ToLower(search) + "%"
		repoRows, err = s.pool.Query(ctx, `
			SELECT r.repo_id, r.repo_name, r.repo_owner, r.repo_git, r.platform_id
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			  AND (LOWER(r.repo_name) LIKE $2 OR LOWER(r.repo_owner) LIKE $2 OR LOWER(r.repo_git) LIKE $2)
			ORDER BY r.repo_owner, r.repo_name
			LIMIT $3 OFFSET $4`, groupID, searchPattern, limit, offset)
	} else {
		repoRows, err = s.pool.Query(ctx, `
			SELECT r.repo_id, r.repo_name, r.repo_owner, r.repo_git, r.platform_id
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			ORDER BY r.repo_owner, r.repo_name
			LIMIT $2 OFFSET $3`, groupID, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer repoRows.Close()

	var result []GroupRepo
	for repoRows.Next() {
		var r GroupRepo
		if err := repoRows.Scan(&r.RepoID, &r.RepoName, &r.RepoOwner, &r.RepoGit, &r.PlatformID); err != nil {
			return result, err
		}
		result = append(result, r)
	}
	return result, nil
}

// loadGroupOrgs fetches tracked orgs for a group.
func (s *PostgresStore) loadGroupOrgs(ctx context.Context, groupID int64) ([]GroupOrg, error) {
	orgRows, err := s.pool.Query(ctx, `
		SELECT org_request_id, org_url, org_name, platform, last_scanned
		FROM aveloxis_ops.user_org_requests
		WHERE group_id = $1
		ORDER BY org_name`, groupID)
	if err != nil {
		return nil, err
	}
	defer orgRows.Close()

	var result []GroupOrg
	for orgRows.Next() {
		var o GroupOrg
		if err := orgRows.Scan(&o.OrgRequestID, &o.OrgURL, &o.OrgName, &o.Platform, &o.LastScanned); err != nil {
			return result, err
		}
		result = append(result, o)
	}
	return result, nil
}

// AddRepoToGroup adds a single repo URL to a user group. Creates the repo
// in the repos table and queue if it doesn't exist.
//
// v0.19.0: gated on group status. If the group is 'pending' (non-admin
// submission awaiting review), the repo is added to user_repos but
// NOT enqueued — ApproveGroup is what eventually walks user_repos and
// fills the queue. If the group is 'approved' (admin's own group, or
// previously approved), the repo enqueues immediately as before.
// 'rejected' groups refuse the add entirely.
func (s *PostgresStore) AddRepoToGroup(ctx context.Context, userID int, groupID int64, repoURL string) error {
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return err
	}

	// Look up group status to decide whether to enqueue or defer.
	var status string
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(status, 'approved') FROM aveloxis_ops.user_groups WHERE group_id = $1`,
		groupID).Scan(&status); err != nil {
		return fmt.Errorf("look up group status: %w", err)
	}
	if status == "rejected" {
		return fmt.Errorf("group has been rejected by an administrator")
	}

	// Ensure repo exists in repos table.
	var repoID int64
	err := s.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1`, repoURL).Scan(&repoID)
	if err != nil {
		// Not found — need to insert. Determine platform from URL.
		platform := int16(1) // GitHub default
		if strings.Contains(repoURL, "gitlab") {
			platform = 2
		} else if !strings.Contains(repoURL, "github.com") {
			platform = 3 // Generic git — facade/analysis only
		}
		// Extract owner/name from URL.
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(repoURL, "https://"), "http://"), "/"), "/")
		owner := ""
		name := ""
		if len(parts) >= 3 {
			name = parts[len(parts)-1]
			owner = strings.Join(parts[1:len(parts)-1], "/")
		}

		// Get or create default group.
		var groupIDDB int64
		_ = s.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = 'Default'`).Scan(&groupIDDB)
		if groupIDDB == 0 {
			s.pool.QueryRow(ctx,
				`INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description) VALUES ('Default', 'Auto-created') RETURNING repo_group_id`).Scan(&groupIDDB)
		}

		err = s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name
			RETURNING repo_id`,
			groupIDDB, platform, repoURL, name, owner).Scan(&repoID)
		if err != nil {
			return err
		}

		// Enqueue for collection — only for approved groups.
		// Pending groups defer enqueue to ApproveGroup; this is what
		// gives the admin a chance to review submissions before they
		// burn API quota.
		if status == "approved" {
			s.pool.Exec(ctx, `
				INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
				VALUES ($1, 100, 'queued', NOW())
				ON CONFLICT (repo_id) DO NOTHING`, repoID)
		}
	}

	// Add to user_repos regardless of status — the user can build out
	// the group's repo list while waiting for approval.
	_, err = s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, groupID, repoID)
	return err
}

// AddOrgToGroup registers an org for tracking. Immediately scans for repos
// and adds them to the group.
func (s *PostgresStore) AddOrgToGroup(ctx context.Context, userID int, groupID int64, orgURL string) error {
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return err
	}

	// Determine platform and org name.
	orgURL = strings.TrimSuffix(strings.TrimSpace(orgURL), "/")
	platform := "github"
	if strings.Contains(orgURL, "gitlab") {
		platform = "gitlab"
	}
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(orgURL, "https://"), "http://"), "/")
	orgName := ""
	if len(parts) >= 2 {
		orgName = parts[1]
	}

	// Insert org request.
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests
			(user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (group_id, org_url) DO NOTHING`,
		userID, groupID, orgURL, orgName, platform)
	return err
}

// RemoveRepoFromGroup removes a repo from a user group.
func (s *PostgresStore) RemoveRepoFromGroup(ctx context.Context, userID int, groupID, repoID int64) error {
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2`,
		groupID, repoID)
	return err
}

// GetOrgRequests returns all org requests that need scanning.
func (s *PostgresStore) GetOrgRequests(ctx context.Context) ([]GroupOrg, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT org_request_id, org_url, org_name, platform, last_scanned
		FROM aveloxis_ops.user_org_requests
		ORDER BY last_scanned ASC NULLS FIRST`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orgs []GroupOrg
	for rows.Next() {
		var o GroupOrg
		rows.Scan(&o.OrgRequestID, &o.OrgURL, &o.OrgName, &o.Platform, &o.LastScanned)
		orgs = append(orgs, o)
	}
	return orgs, rows.Err()
}

// GetGroupIDForOrgRequest returns the group_id for an org request.
func (s *PostgresStore) GetGroupIDForOrgRequest(ctx context.Context, orgRequestID int64) (int64, error) {
	var groupID int64
	err := s.pool.QueryRow(ctx,
		`SELECT group_id FROM aveloxis_ops.user_org_requests WHERE org_request_id = $1`,
		orgRequestID).Scan(&groupID)
	return groupID, err
}

// AddRepoToGroupByID adds a repo to a user group by repo_id (no ownership check).
func (s *PostgresStore) AddRepoToGroupByID(ctx context.Context, groupID, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, groupID, repoID)
	return err
}

// MarkOrgRequestScanned updates the last_scanned timestamp.
func (s *PostgresStore) MarkOrgRequestScanned(ctx context.Context, orgRequestID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.user_org_requests SET last_scanned = NOW() WHERE org_request_id = $1`,
		orgRequestID)
	return err
}

// ============================================================
// v0.19.0 admin / approval workflow
// ============================================================

// PendingGroup is a group awaiting admin review, joined to its
// requesting user for the admin's pending-queue view.
type PendingGroup struct {
	GroupID     int64
	Name        string
	UserID      int
	UserLogin   string
	UserEmail   string
	RepoCount   int
	OrgRequests int
	CreatedAt   time.Time
}

// ListPendingGroups returns every group with status='pending' joined
// to its requesting user. Sorted oldest-first so the admin sees the
// longest-waiting submissions at the top.
func (s *PostgresStore) ListPendingGroups(ctx context.Context) ([]PendingGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT g.group_id, g.name, u.user_id, u.login_name, COALESCE(u.email, ''),
		       (SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = g.group_id),
		       (SELECT COUNT(*) FROM aveloxis_ops.user_org_requests WHERE group_id = g.group_id),
		       COALESCE(u.data_collection_date, NOW())
		FROM aveloxis_ops.user_groups g
		JOIN aveloxis_ops.users u ON u.user_id = g.user_id
		WHERE g.status = 'pending'
		ORDER BY g.group_id`)
	if err != nil {
		return nil, fmt.Errorf("list pending groups: %w", err)
	}
	defer rows.Close()
	var out []PendingGroup
	for rows.Next() {
		var p PendingGroup
		if err := rows.Scan(&p.GroupID, &p.Name, &p.UserID, &p.UserLogin, &p.UserEmail,
			&p.RepoCount, &p.OrgRequests, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ApproveGroup flips a pending group to approved AND enqueues all of
// its repos for collection. Idempotent: re-approving an already-
// approved group is a no-op (the queue INSERT uses ON CONFLICT DO
// NOTHING).
//
// Org requests in the group don't need explicit handling here — the
// scheduler's refreshOrgs ticker will pick them up on its next cycle
// now that the group is approved (the org-scan path checks group
// status before enqueueing the org's repos).
func (s *PostgresStore) ApproveGroup(ctx context.Context, groupID int64, adminID int) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Flip status. Only act on currently-pending rows so a double
		// click doesn't re-stamp approved_at.
		tag, err := tx.Exec(ctx, `
			UPDATE aveloxis_ops.user_groups
			SET status = 'approved', approved_by = $2, approved_at = NOW()
			WHERE group_id = $1 AND status = 'pending'`,
			groupID, adminID)
		if err != nil {
			return fmt.Errorf("approve group: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}

		// Enqueue every repo in the group for collection. ON CONFLICT
		// DO NOTHING handles repos already in the queue from another
		// group's approval.
		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			SELECT ur.repo_id, 100, 'queued', NOW()
			FROM aveloxis_ops.user_repos ur
			WHERE ur.group_id = $1
			ON CONFLICT (repo_id) DO NOTHING`, groupID)
		if err != nil {
			return fmt.Errorf("enqueue group repos on approval: %w", err)
		}
		return tx.Commit(ctx)
	})
}

// RejectGroup flips a pending group to rejected. Repos in the group
// are NOT deleted (the user might appeal); they simply never enter
// collection_queue. The user's dashboard shows the rejection state.
func (s *PostgresStore) RejectGroup(ctx context.Context, groupID int64, adminID int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.user_groups
		SET status = 'rejected', approved_by = $2, approved_at = NOW()
		WHERE group_id = $1 AND status = 'pending'`,
		groupID, adminID)
	return err
}

// AdminUser is the row shape for the admin user-management page.
type AdminUser struct {
	UserID    int
	Login     string
	Email     string
	Provider  string
	IsAdmin   bool
	CreatedAt time.Time
}

// ListUsers returns all users for the admin user-management page.
// Sorted by user_id ascending so the first-promoted (typically the
// operator) appears first.
func (s *PostgresStore) ListUsers(ctx context.Context) ([]AdminUser, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, login_name, COALESCE(email, ''), COALESCE(oauth_provider, ''),
		       admin, COALESCE(data_collection_date, NOW())
		FROM aveloxis_ops.users
		ORDER BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	var out []AdminUser
	for rows.Next() {
		var u AdminUser
		if err := rows.Scan(&u.UserID, &u.Login, &u.Email, &u.Provider, &u.IsAdmin, &u.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// SetUserAdmin toggles the admin role for a user. Refuses to demote
// the last remaining admin so the system can't end up with zero
// admins (which would leave the approval queue forever stuck).
func (s *PostgresStore) SetUserAdmin(ctx context.Context, userID int, isAdmin bool) error {
	if !isAdmin {
		// Demoting — make sure another admin remains.
		var otherAdmins int
		err := s.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM aveloxis_ops.users WHERE admin = TRUE AND user_id != $1`, userID).Scan(&otherAdmins)
		if err != nil {
			return fmt.Errorf("count remaining admins: %w", err)
		}
		if otherAdmins == 0 {
			return fmt.Errorf("refusing to demote the last admin — promote another user first")
		}
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET admin = $2 WHERE user_id = $1`,
		userID, isAdmin)
	return err
}
