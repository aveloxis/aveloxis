package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

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
func (s *PostgresStore) UpsertOAuthUser(ctx context.Context, info OAuthUserInfo) (int, error) {
	var userID int

	// Try to find existing user by login.
	err := s.pool.QueryRow(ctx,
		`SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1`,
		info.Login).Scan(&userID)

	if err != nil {
		// Not found — create.
		firstName := info.Name
		if idx := strings.Index(firstName, " "); idx > 0 {
			firstName = firstName[:idx]
		}
		lastName := ""
		if idx := strings.LastIndex(info.Name, " "); idx > 0 {
			lastName = info.Name[idx+1:]
		}

		err = s.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users
				(login_name, email, first_name, last_name, avatar_url,
				 gh_user_id, gh_login, gl_user_id, gl_username,
				 oauth_provider, admin, email_verified,
				 tool_source, tool_version, data_source)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE, TRUE,
				'aveloxis-web', $11, $10 || ' OAuth')
			RETURNING user_id`,
			info.Login, info.Email, firstName, lastName, info.AvatarURL,
			info.GHUserID, info.GHLogin, info.GLUserID, info.GLUsername,
			info.Provider, ToolVersion,
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
type UserGroup struct {
	GroupID   int64
	Name      string
	Favorited bool
	RepoCount int
}

// GetUserGroups returns all groups for a user with repo counts.
func (s *PostgresStore) GetUserGroups(ctx context.Context, userID int) ([]UserGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT g.group_id, g.name, g.favorited, COUNT(ur.repo_id) AS repo_count
		FROM aveloxis_ops.user_groups g
		LEFT JOIN aveloxis_ops.user_repos ur ON ur.group_id = g.group_id
		WHERE g.user_id = $1
		GROUP BY g.group_id, g.name, g.favorited
		ORDER BY g.name`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []UserGroup
	for rows.Next() {
		var g UserGroup
		if err := rows.Scan(&g.GroupID, &g.Name, &g.Favorited, &g.RepoCount); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// CreateUserGroup creates a new group for a user. Returns group_id.
func (s *PostgresStore) CreateUserGroup(ctx context.Context, userID int, name string) (int64, error) {
	var groupID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name) VALUES ($1, $2)
		ON CONFLICT (user_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING group_id`,
		userID, name).Scan(&groupID)
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
func (s *PostgresStore) AddRepoToGroup(ctx context.Context, userID int, groupID int64, repoURL string) error {
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return err
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

		// Enqueue for collection.
		s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			VALUES ($1, 100, 'queued', NOW())
			ON CONFLICT (repo_id) DO NOTHING`, repoID)
	}

	// Add to user_repos.
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
