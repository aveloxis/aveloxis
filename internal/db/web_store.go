// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/platform"
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

// ErrLoginNameTaken refuses an OAuth login whose name belongs to an account
// with a different forge identity (final whole-tree review F1, 2026-09-28):
// another provider's user, another numeric ID on the same provider, or a row
// the login cannot prove it owns. A name is not an identity (SR-6); merging
// on it handed a GitLab user who registered a GitHub admin's username that
// admin's account.
var ErrLoginNameTaken = errors.New("oauth login name belongs to another forge identity")

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
	GLHost     string // the GitLab instance that answered (web.gitlab_base_url); see GitLabOAuthHost
	Provider   string
	// SignupAddr is the browser's address on the callback (web.trusted_proxy
	// decides whether X-Forwarded-For is believed); a creation from it counts
	// toward signups_per_address_per_day (v0.29.89). Zero: not known, not
	// counted.
	SignupAddr netip.Addr
}

// GitLabOAuthHost is the one spelling of the GitLab instance a login came
// from (SR-17): the callback's base URL trimmed, lowercased, without a
// trailing slash; empty is gitlab.com, the callback's default. A GitLab user
// ID is an identity only on its own instance (final review round 2 F4: two
// self-hosted instances number their users from 1, so moving
// web.gitlab_base_url handed instance B's user 7 instance A's user 7).
func GitLabOAuthHost(base string) string {
	h := strings.TrimRight(strings.ToLower(strings.TrimSpace(base)), "/")
	if h == "" {
		return "https://gitlab.com"
	}
	return h
}

// SignInOAuthUser creates or updates a user from OAuth login. Returns the
// user_id and whether this sign-in created the account (the web login's
// welcome mail keys on it; final review round 3 — a second, name-keyed
// probe in the handler disagreed with the identity rule below).
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
func (s *PostgresStore) SignInOAuthUser(ctx context.Context, info OAuthUserInfo) (int, bool, error) {
	if strings.TrimSpace(info.Login) == "" {
		return 0, false, ErrEmptyLogin
	}

	var userID int

	// The forge's numeric user ID is the identity; the login name is a
	// label a forge lets anyone register once it is free (final whole-tree
	// review F1, 2026-09-28: matching on login_name alone handed a GitLab
	// user with a GitHub admin's username that admin's account). An account
	// is found by its ID first; the name is claimed only by a row of the
	// same provider that carries no ID to contradict the login (a legacy or
	// ID-less row, stamped by the claim), and any other collision is refused.
	// A row from before oauth_provider existed is a GitHub row: GitLab
	// sign-in arrived with that column.
	var ownID int64
	switch info.Provider {
	case "github":
		ownID = info.GHUserID
		info.GLHost = "" // a GitHub login never stamps a GitLab instance
	case "gitlab":
		ownID = info.GLUserID
		info.GLHost = GitLabOAuthHost(info.GLHost)
	default:
		return 0, false, fmt.Errorf("oauth login %q: unknown provider %q", info.Login, info.Provider)
	}
	if ownID > 0 {
		// Through 0.29.68 a rename inserted a second row with the same ID,
		// so more than one can match: the most recently used one signs in,
		// and the duplicate is logged for an administrator (round 2 F3). A
		// GitLab ID matches only on its own instance; a row from before the
		// instance was recorded matches none until StampLegacyGitLabHost
		// stamps it at web start (round 3).
		byID := `SELECT user_id, count(*) OVER () FROM aveloxis_ops.users WHERE gh_user_id = $1 AND $2 = ''
			ORDER BY data_collection_date DESC NULLS LAST, user_id DESC LIMIT 1`
		if info.Provider == "gitlab" {
			byID = `SELECT user_id, count(*) OVER () FROM aveloxis_ops.users WHERE gl_user_id = $1 AND gl_oauth_host = $2
			ORDER BY data_collection_date DESC NULLS LAST, user_id DESC LIMIT 1`
		}
		var matches int
		err := s.pool.QueryRow(ctx, byID, ownID, info.GLHost).Scan(&userID, &matches)
		if err == nil {
			if matches > 1 && s.logger != nil {
				s.logger.Warn("oauth login: more than one account carries this forge user ID — the most recently used one signed in; merge or remove the others",
					"provider", info.Provider, "forge_user_id", ownID, "accounts", matches, "user_id", userID)
			}
			return userID, false, s.updateOAuthUser(ctx, userID, info)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, fmt.Errorf("lookup user by %s id: %w", info.Provider, err)
		}
	}

	var rowGH, rowGL int64
	var rowProvider string
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, COALESCE(gh_user_id, 0), COALESCE(gl_user_id, 0), `+effectiveProviderSQL("")+`
		 FROM aveloxis_ops.users WHERE login_name = $1`,
		info.Login).Scan(&userID, &rowGH, &rowGL, &rowProvider)

	if err == nil && (rowProvider != info.Provider || rowGH != 0 || rowGL != 0) {
		return 0, false, fmt.Errorf("oauth login %q (%s id %d): %w", info.Login, info.Provider, ownID, ErrLoginNameTaken)
	}
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			// Real DB error — surface it. Treating this as "not
			// found" and falling through to INSERT is what created
			// blank/duplicate rows in the v0.18.28 incident.
			return 0, false, fmt.Errorf("lookup user by login: %w", err)
		}
		// Not found — create.
		firstName, lastName := splitOAuthName(info.Name)

		// v0.19.0: first user is auto-admin. Count existing rows; if
		// zero, set admin = TRUE on this INSERT so the bootstrap
		// signup gets the privilege. Race-free: this branch is only
		// reached when no row matches the login, and a concurrent
		// signup with a different login would also see count == 0
		// and would also flip admin TRUE — that's fine for a fresh
		// deployment (multiple admins from a near-simultaneous
		// initial-batch signup is a non-issue).
		// v0.27.36 (summary/18 Phase 0c): FAIL CLOSED. The pre-fix
		// discarded this error, so a transient DB failure left
		// existingCount == 0 and granted admin to an arbitrary later
		// signup. On error we refuse the signup entirely — the user
		// retries once the DB is healthy and gets the correct answer.
		var existingCount int
		if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.users`).Scan(&existingCount); err != nil {
			return 0, false, fmt.Errorf("counting users for first-admin bootstrap: %w", err)
		}
		isFirstUser := existingCount == 0

		// v0.29.89 (summary/53): the account and its sign-up record are one
		// transaction, decided by the sign-ups-per-address quota under a
		// per-address lock (SR-18: where the account is created).
		pol, err := s.readSignupPolicy(ctx) // before Begin: no pool read inside the transaction
		if err != nil {
			return 0, false, fmt.Errorf("create account: sign-up quota: %w", err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("create account: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		key, err := s.admitSignup(ctx, tx, info.SignupAddr, isFirstUser, pol)
		if err != nil {
			return 0, false, err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users
				(login_name, email, first_name, last_name, avatar_url,
				 gh_user_id, gh_login, gl_user_id, gl_username,
				 oauth_provider, admin, email_verified, email_confirmed_at,
				 tool_source, tool_version, data_source, gl_oauth_host)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6::bigint, 0), $7, NULLIF($8::bigint, 0), $9, $10, $12, TRUE, NOW(),
				'aveloxis-web', $11, $10 || ' OAuth', NULLIF($13, ''))
			RETURNING user_id`,
			info.Login, info.Email, firstName, lastName, info.AvatarURL,
			info.GHUserID, info.GHLogin, info.GLUserID, info.GLUsername,
			info.Provider, ToolVersion, isFirstUser, info.GLHost,
		).Scan(&userID)
		if err != nil {
			return 0, false, err
		}
		if err := recordSignup(ctx, tx, key, s.sealSignupAddr(info.SignupAddr), userID); err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, fmt.Errorf("create account: %w", err)
		}
		s.logAccountCreated(ctx, userID)
		return userID, true, nil
	}

	// Found and owned (an ID-less row of this provider): the claim stamps
	// the login's ID on it.
	return userID, false, s.updateOAuthUser(ctx, userID, info)
}

// UpsertOAuthUser is SignInOAuthUser without the created flag, for callers
// that only need the account (fixtures, the test deployment's bootstrap
// admin).
func (s *PostgresStore) UpsertOAuthUser(ctx context.Context, info OAuthUserInfo) (int, error) {
	id, _, err := s.SignInOAuthUser(ctx, info)
	return id, err
}

// StampLegacyGitLabHost records base (spelled by GitLabOAuthHost) as the
// instance of every GitLab account from before the instance was recorded
// (final review round 3, decided as a class): such a row matches no
// instance at sign-in, because matching any let a later instance's user with
// the same ID take it. `aveloxis web` runs it at start for its configured
// instance, so those accounts sign in as before; a recorded host is never
// replaced. Idempotent.
func (s *PostgresStore) StampLegacyGitLabHost(ctx context.Context, base string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE aveloxis_ops.users SET gl_oauth_host = $1
		WHERE COALESCE(gl_user_id, 0) <> 0 AND COALESCE(gl_oauth_host, '') = ''`, GitLabOAuthHost(base))
	if err != nil {
		return 0, fmt.Errorf("stamp legacy GitLab accounts with %s: %w", GitLabOAuthHost(base), err)
	}
	return tag.RowsAffected(), nil
}

// updateOAuthUser refreshes an owned account's OAuth fields. v0.27.84: the
// display name is refreshed on every login too (it used to be written only
// at first signup, going stale after provider-side renames); an empty
// provider name preserves the stored one. A stored ID is never replaced,
// and a zero ID is never stored (0 read as "has an identity" would lock the
// row against its own provider's claim). The account's login_name follows
// the forge's current name unless another account holds it (round 2 F2: a
// renamed user kept the old name, which then refused whoever registered it
// next and made the first-signup probe read the user as new at every
// login), and a GitLab row records the instance that answered.
func (s *PostgresStore) updateOAuthUser(ctx context.Context, userID int, info OAuthUserInfo) error {
	freshFirst, freshLast := splitOAuthName(info.Name)
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.users SET
			email = COALESCE(NULLIF($2, ''), email),
			avatar_url = $3,
			gh_user_id = COALESCE(NULLIF(gh_user_id, 0), NULLIF($4::bigint, 0)),
			gh_login = COALESCE(NULLIF($5, ''), gh_login),
			gl_user_id = COALESCE(NULLIF(gl_user_id, 0), NULLIF($6::bigint, 0)),
			gl_username = COALESCE(NULLIF($7, ''), gl_username),
			oauth_provider = $8,
			first_name = CASE WHEN $9 = '' THEN first_name ELSE $9 END,
			last_name = CASE WHEN $9 = '' THEN last_name ELSE $10 END,
			gl_oauth_host = COALESCE(NULLIF(gl_oauth_host, ''), NULLIF($11, '')),
			login_name = CASE WHEN NOT EXISTS (
					SELECT 1 FROM aveloxis_ops.users o WHERE o.login_name = $12 AND o.user_id <> $1)
				THEN $12 ELSE login_name END,
			data_collection_date = NOW()
		WHERE user_id = $1`,
		userID, info.Email, info.AvatarURL,
		info.GHUserID, info.GHLogin, info.GLUserID, info.GLUsername,
		info.Provider, freshFirst, freshLast, info.GLHost, info.Login)
	return err
}

// CrossProviderUserAuditSQL counts accounts linked to BOTH a GitHub and a
// GitLab user. Through 0.29.68 a login was matched on login_name alone,
// so a GitLab user who signed in with a GitHub user's name was linked to
// that account (final whole-tree review F1); such a row is either a person
// who really uses both forges under one name or a takeover. The 0.29.69
// deploy checklist runs it as an observation-only audit.
//
// The old code wrote the other provider's ID as 0 and a takeover kept it,
// so such a row is found by its name trail — a GitHub login AND a GitLab
// user name on one account — as well as by two IDs (round 2 F1).
func CrossProviderUserAuditSQL() string {
	return `SELECT count(*) FROM aveloxis_ops.users WHERE (COALESCE(gh_user_id, 0) <> 0 AND COALESCE(gl_user_id, 0) <> 0) OR (COALESCE(gh_login, '') <> '' AND COALESCE(gl_username, '') <> '')`
}

// DuplicateForgeIDUserAuditSQL counts forge user IDs that more than one
// account carries. Through 0.29.68 a user who renamed on the forge got a
// second account at the next sign-in; from 0.29.69 the most recently used one
// signs in and the rest are logged (round 2 F3). Observation only.
func DuplicateForgeIDUserAuditSQL() string {
	return `SELECT (SELECT count(*) FROM (SELECT gh_user_id FROM aveloxis_ops.users WHERE COALESCE(gh_user_id, 0) <> 0 GROUP BY 1 HAVING count(*) > 1) g) + (SELECT count(*) FROM (SELECT gl_user_id, COALESCE(gl_oauth_host, '') FROM aveloxis_ops.users WHERE COALESCE(gl_user_id, 0) <> 0 GROUP BY 1, 2 HAVING count(*) > 1) l)`
}

// verifyGroupOwnership checks that the given group belongs to the user.
// Returns the group name or an error if not found/owned.
func (s *PostgresStore) verifyGroupOwnership(ctx context.Context, userID int, groupID int64) (string, error) {
	var name string
	err := s.pool.QueryRow(ctx,
		`SELECT name FROM aveloxis_ops.user_groups WHERE group_id = $1 AND user_id = $2`,
		groupID, userID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrGroupNotOwned
	}
	if err != nil {
		// A failed lookup is not "not owned" (SR-5; Copilot review of PR #207
		// on d436880: the API reported it as a client error).
		return "", fmt.Errorf("look up group ownership: %w", err)
	}
	return name, nil
}

// ErrGroupNotOwned means the group does not exist or belongs to another user.
var ErrGroupNotOwned = errors.New("group not found or not owned by user")

// ErrGroupRejected means an administrator rejected the group, so it takes no
// additions.
var ErrGroupRejected = errors.New("group has been rejected by an administrator")

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
	// PendingAdds (v0.27.84) counts the group's pending
	// collection_add_requests so the GUI can render the REAL state:
	// the group itself is approved (v0.27.20 — the approval unit is
	// the ADDITION), but its additions may await review.
	PendingAdds int
}

// GetUserGroups returns all groups for a user with repo counts and
// pending-addition counts (the subquery rides idx_add_requests_group).
func (s *PostgresStore) GetUserGroups(ctx context.Context, userID int) ([]UserGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT g.group_id, g.name, g.favorited,
		       COUNT(ur.repo_id) AS repo_count,
		       COALESCE(g.status, 'approved') AS status,
		       (SELECT COUNT(*) FROM aveloxis_ops.collection_add_requests ar
		        WHERE ar.group_id = g.group_id AND ar.status = 'pending') AS pending_adds
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
		if err := rows.Scan(&g.GroupID, &g.Name, &g.Favorited, &g.RepoCount, &g.Status, &g.PendingAdds); err != nil {
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
	if AdminPrivilegeDropped(ctx) {
		return false, nil // the request runs without admin privilege (an API token)
	}
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
// v0.27.20 (per-add approval, summary/15 Option A): every group is
// created with status='approved'. Groups are just containers —
// visibility was never gated (v0.27.4 scope rule), and the approval
// unit moved from the group to the ADDITION of not-yet-tracked
// content (AddReposToGroup / AddOrgToGroup create pending
// collection_add_requests for non-admins). 'rejected' remains the
// group-level abuse lever (RejectGroup blocks all future adds).
// The v0.19.0 pending-group flow is retired; pre-existing pending
// groups are converted to add-requests by migrateLegacyPendingGroups.
func (s *PostgresStore) CreateUserGroup(ctx context.Context, userID int, name string) (int64, error) {
	var groupID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, 'approved')
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
	// A failed read after the ownership check is returned, never rendered as
	// an empty group (PR #218 review C17, SR-5): the web handlers turn it
	// into a logged 500.
	if err != nil {
		return nil, 0, fmt.Errorf("count repos of group %d: %w", groupID, err)
	}

	// Load paginated repos.
	offset := (page - 1) * perPage
	if detail.Repos, err = s.loadGroupRepos(ctx, groupID, search, perPage, offset); err != nil {
		return nil, 0, fmt.Errorf("load repos of group %d: %w", groupID, err)
	}

	// Load tracked orgs.
	if detail.Orgs, err = s.loadGroupOrgs(ctx, groupID); err != nil {
		return nil, 0, fmt.Errorf("load orgs of group %d: %w", groupID, err)
	}

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
	// An error ending the rows is the query's failure, not the end of the
	// page (PR #218 review C17).
	return result, repoRows.Err()
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
	return result, orgRows.Err() // PR #218 review C17, as in loadGroupRepos
}

// AddRepoToGroup adds a single repo URL to a user group under the
// v0.27.20 per-add approval rule — a thin single-URL wrapper over
// AddReposToGroup with auto-approval disabled. Kept for the CLI
// importers (which run as the bootstrap admin and therefore take the
// direct path) and any caller that doesn't need the outcome counts;
// web/portal handlers call AddReposToGroup directly for the
// linked/pending split and the configured auto-approve limit.
func (s *PostgresStore) AddRepoToGroup(ctx context.Context, userID int, groupID int64, repoURL string) error {
	_, err := s.AddReposToGroup(ctx, userID, groupID, []string{repoURL}, 0)
	return err
}

// splitOAuthName splits a provider display name into the users
// table's first/last columns: first token before the first space,
// last token after the last space (middle names fall away — the
// original v0.19.0 rule, now shared by the INSERT and UPDATE paths).
func splitOAuthName(name string) (first, last string) {
	// v0.27.87 (Copilot round): trim BEFORE splitting. A
	// whitespace-only provider name used to pass the UPDATE path's
	// `$9 = ''` preserve-guard and overwrite stored names with junk
	// (" " / ''); a trailing space clobbered last_name the same way.
	name = strings.TrimSpace(name)
	first = name
	if idx := strings.Index(first, " "); idx > 0 {
		first = first[:idx]
	}
	if idx := strings.LastIndex(name, " "); idx > 0 {
		last = name[idx+1:]
	}
	return first, last
}

// IsOrgRegisteredAnywhere reports whether an org URL is already
// registered for scanning in ANY group (case-insensitive — org URL rows
// written before v0.29.68, and any request pended before it, keep the
// registrant's case; a URL pasted since is stored in CanonicalOrgURL's
// form; and GitHub logins are case-insensitive).
// This is the v0.27.84 auto-approve criterion: a duplicate
// registration of an already-registered org adds ZERO new collection
// (the v0.27.83 scan dedup enumerates each distinct org once, and the
// org's future repos already auto-enqueue via the existing
// registration), so a non-admin adding one needs no review.
//
// A registration in a REJECTED group does not count: it is never scanned,
// so it is no existing collection, and counting it let a non-admin whose
// group was rejected re-track the same org from a fresh group with no
// review (round-12 review, v0.29.38).
func (s *PostgresStore) IsOrgRegisteredAnywhere(ctx context.Context, orgURL string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM aveloxis_ops.user_org_requests o
		JOIN aveloxis_ops.user_groups g ON g.group_id = o.group_id
		WHERE LOWER(org_url) = LOWER($1) AND g.status IS DISTINCT FROM 'rejected')`, orgURL).Scan(&exists)
	return exists, err
}

// CanonicalOrgURL is the ONE stored spelling of an org URL: trimmed, no
// trailing "/", https:// added to schemeless input, and lowercased. The
// lowercase is worklist follow-up 3: GitHub and GitLab paths are
// case-insensitive, but the registration's unique key (group_id, org_url) is
// not, so `github.com/CHAOSS` beside `github.com/chaoss` made a second
// registration (and a second audit row) while IsOrgRegisteredAnywhere's
// LOWER() said it was already there. Rows written before v0.29.68 keep
// their case; the LOWER() checks still find them. The https:// step is
// v0.27.94 (Copilot finding on PR #179): platform.ParseOrgURL tolerates a
// schemeless URL, so such a registration WORKED while the raw stored
// org_url defeated every exact/prefix matcher (ReconcileOrgRepoLinks,
// GetUserGroupIDsForOrgURL, the dedup). AddOrgToGroup is the choke point
// every caller routes through, so this runs there.
func CanonicalOrgURL(orgURL string) string {
	return strings.ToLower(orgURLTrimmedSchemed(orgURL))
}

// orgURLTrimmedSchemed is CanonicalOrgURL before the lowercase: the form the
// entry limit measures (MaxAddURLBytes budgets for lower() lengthening some
// characters, so the check runs on the input, not on the grown form).
func orgURLTrimmedSchemed(orgURL string) string {
	orgURL = strings.TrimSuffix(strings.TrimSpace(orgURL), "/")
	if orgURL != "" && !strings.Contains(orgURL, "://") {
		orgURL = "https://" + orgURL
	}
	return orgURL
}

// AddOrgToGroup registers an org for tracking under the v0.27.20
// per-add approval rule: an ADMIN's registration lands in
// user_org_requests immediately (presence there = approved to scan;
// the caller may fire an immediate scan); a non-admin's registration
// of a NEW org creates a pending collection_add_requests row
// (kind='org') that an admin must approve — a new org is an unbounded
// future-repo commitment, so it pends regardless of
// auto_approve_add_limit.
//
// v0.27.84 refinement (operator decision, 2026-08-05): a non-admin's
// registration of an org that is ALREADY registered in a group that is not
// rejected (IsOrgRegisteredAnywhere) auto-approves — it leaves an
// auto-approved audit row (decided_by=0, the auto_approve_add_limit
// pattern) and registers immediately, both in one transaction. This
// preserves the "no EnqueueRepo reachable from a non-admin handler
// without approval" invariant structurally: the org's repos are
// already tracked and its future repos already auto-enqueue via the
// existing registration, so this branch adds no new enqueue
// reachability.
func (s *PostgresStore) AddOrgToGroup(ctx context.Context, userID int, groupID int64, orgURL, ghAPIBase string) (OrgAddOutcome, error) {
	var out OrgAddOutcome
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return out, err
	}
	status, err := s.GetGroupStatus(ctx, groupID)
	if err != nil {
		return out, fmt.Errorf("look up group status: %w", err)
	}
	if status == "rejected" {
		return out, ErrGroupRejected
	}

	if len(orgURLTrimmedSchemed(orgURL)) > MaxAddURLBytes {
		return out, ErrURLTooLong
	}
	orgURL = CanonicalOrgURL(orgURL)
	// An org URL is stored, shown and enumerated; credentials in it are
	// refused like a repo URL's (v0.29.57, review 5261384568).
	if err := platform.RefuseURLUserinfo(orgURL); err != nil {
		return out, err
	}
	// The host gate, before the admin/non-admin split so neither path can
	// register or pend an org the deployment cannot enumerate; the same
	// label registerApprovedOrg will store decides it (SR-17/SR-18).
	if _, platformName := parseOrgURLMeta(orgURL); true {
		if err := orgRegistrable(platformName, orgURL, ghAPIBase); err != nil {
			return out, err
		}
	}
	// A lookup ERROR is not "not an admin" (SR-5; worklist follow-up 6).
	isAdmin, err := s.IsUserAdmin(ctx, userID)
	if err != nil {
		return out, fmt.Errorf("look up admin flag: %w", err)
	}
	if !isAdmin {
		registered, regErr := s.IsOrgRegisteredAnywhere(ctx, orgURL)
		if regErr != nil {
			return out, fmt.Errorf("check org registration: %w", regErr)
		}
		if !registered {
			reqID, err := s.createAddRequest(ctx, userID, groupID, "org", orgURL, nil, "pending")
			if err != nil {
				return out, err
			}
			out.RequestID = reqID
			return out, nil
		}
		// Already registered → auto-approve: the audit request and the
		// user_org_requests registration commit together, so a failure
		// leaves neither — not an approved request with nothing tracked, and
		// no leftover audit row for the user's retry after a failure
		// (round-13 review). Two successful adds still record two audit rows,
		// as before. The registration check above runs outside this
		// transaction; a RejectGroup that commits in between leaves the same
		// rows as this add committing just before it (declined, Copilot review
		// of PR #207 on eb248eb).
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return out, err
		}
		defer tx.Rollback(ctx)
		reqID, err := insertAddRequest(ctx, tx, userID, groupID, "org", orgURL, nil, "approved")
		if err != nil {
			return out, err
		}
		if _, err := registerApprovedOrg(ctx, tx, AddRequest{UserID: userID, GroupID: groupID, OrgURL: orgURL}, ghAPIBase); err != nil {
			return out, err
		}
		if err := tx.Commit(ctx); err != nil {
			return out, fmt.Errorf("auto-approve org: %w", err)
		}
		out.RequestID = reqID
		out.Registered = true
		return out, nil
	}

	// An admin's add registers directly (the user_org_requests row), in a
	// transaction of its own.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	if _, err := registerApprovedOrg(ctx, tx, AddRequest{UserID: userID, GroupID: groupID, OrgURL: orgURL}, ghAPIBase); err != nil {
		return out, err
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("register org: %w", err)
	}
	out.Registered = true
	return out, nil
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
		if err := rows.Scan(&o.OrgRequestID, &o.OrgURL, &o.OrgName, &o.Platform, &o.LastScanned); err != nil {
			return nil, err
		}
		orgs = append(orgs, o)
	}
	return orgs, rows.Err()
}

// HasNeverScannedOrgs reports whether any tracked org has never been
// enumerated (last_scanned IS NULL). This is the scheduler's demand
// signal for an immediate org scan (v0.27.52): every registration path —
// an admin adding an org directly and a non-admin's auto-approved add of an
// already-registered org (both AddOrgToGroup), and an admin approving a
// pending org request (DecideAddRequest) — inserts the user_org_requests row
// with a NULL last_scanned, so the row itself carries "scan me now" across
// processes with no RPC. A re-add of an org the group already registers (in
// any letter case since v0.29.68) inserts nothing and so fires nothing.
//
// Orgs whose owning group is 'rejected' are EXCLUDED: the scan's
// rejected gate skips them without ever stamping last_scanned, so
// counting them here would re-fire the demand probe on every poll
// tick forever. For the same reason only rows the scan will enumerate
// count (OrgScanEligible: a "github" row on this deployment's GitHub host,
// ghAPIBase): a row the host gate skips stays NULL forever too, and a
// GitLab group — which the full pass stamps but nothing enumerates — is
// not worth a demand scan that would only stamp it (v0.29.57 rounds 2–3).
func (s *PostgresStore) HasNeverScannedOrgs(ctx context.Context, ghAPIBase string) (bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.platform, o.org_url
		FROM aveloxis_ops.user_org_requests o
		JOIN aveloxis_ops.user_groups g USING (group_id)
		WHERE o.last_scanned IS NULL
		  AND COALESCE(g.status, 'approved') <> 'rejected'`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var platformName, orgURL string
		if err := rows.Scan(&platformName, &orgURL); err != nil {
			return false, err
		}
		if OrgScanEligible(platformName, orgURL, ghAPIBase) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// GetGroupIDForOrgRequest returns the group_id for an org request.
func (s *PostgresStore) GetGroupIDForOrgRequest(ctx context.Context, orgRequestID int64) (int64, error) {
	var groupID int64
	err := s.pool.QueryRow(ctx,
		`SELECT group_id FROM aveloxis_ops.user_org_requests WHERE org_request_id = $1`,
		orgRequestID).Scan(&groupID)
	return groupID, err
}

// AddRepoToGroupByID adds a repo to a user group by repo_id (no ownership
// check). The returned bool reports whether a NEW user_repos link was
// inserted — the INSERT is ON CONFLICT DO NOTHING, so a nil error alone
// cannot distinguish "new link" from "already linked". Callers that count
// or log "new" links must gate on the bool, not the error (v0.27.91: the
// org scan counted every no-op re-link as new, claiming 9.3M new repos in
// one 8.8-day production run).
func (s *PostgresStore) AddRepoToGroupByID(ctx context.Context, groupID, repoID int64) (bool, error) {
	// Within the owner's repository allocation (v0.29.89, summary/53): an
	// enforced quota refuses with a *capacity.Exceeded.
	n, err := s.linkWithinCap(ctx, groupID, []int64{repoID}, linkAll)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// AddOrgRepoToGroupByID is AddRepoToGroupByID for a repository an
// organization registration brings (the scans, the CLI loaders): within the
// owner's repository allocation, but not counted as the account's daily
// additions — an administrator approved the organization (v0.29.89).
func (s *PostgresStore) AddOrgRepoToGroupByID(ctx context.Context, groupID, repoID int64) (bool, error) {
	n, err := s.linkWithinCap(ctx, groupID, []int64{repoID}, linkOrg)
	if err != nil {
		return false, err
	}
	return n > 0, nil
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

// GroupApproval is what ApproveGroup reports about a group it approved:
// who asked for it — RequesterEmail is "" when their account has no
// address — and the group's name, for the approval email.
type GroupApproval struct {
	RequesterEmail string
	RequesterLogin string
	GroupName      string
}

// ApproveGroup flips a pending group to approved AND enqueues all of
// its repos for collection, in one transaction. Idempotent: re-approving
// an already-approved group is a no-op (the UPDATE matches only pending
// rows; the queue INSERT's ON CONFLICT DO NOTHING covers repos already
// queued by another group).
//
// It returns the requester, read by the same statement that approves, and
// whether THIS call approved the group: false (with a zero GroupApproval)
// for a group that is not pending or does not exist, so a second click
// mails nobody. Before v0.29.36 the web and API handlers each looked the
// requester up themselves, discarded that lookup's error and mailed on
// every call (round-10 review).
//
// LEGACY (v0.19.0 → v0.27.20): new groups are always created
// 'approved' now and non-admin additions pend on
// collection_add_requests instead — see add_requests.go. This method
// stays for the migration window (deciding any pre-conversion pending
// group) and remains harmless afterwards. NOTE: org scans were NEVER
// status-gated (the earlier claim here that they were was wrong —
// audited 2026-07-16); under the per-add rule the gate is structural:
// unapproved org registrations never reach user_org_requests at all.
func (s *PostgresStore) ApproveGroup(ctx context.Context, groupID int64, adminID int) (GroupApproval, bool, error) {
	var approval GroupApproval
	var approved bool
	err := s.withRetry(ctx, func(ctx context.Context) error {
		approval, approved = GroupApproval{}, false
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Flip status. Only act on currently-pending rows so a double
		// click doesn't re-stamp approved_at. user_groups.user_id is a
		// NOT NULL foreign key, so the join always finds the requester.
		var a GroupApproval
		err = tx.QueryRow(ctx, `
			UPDATE aveloxis_ops.user_groups g
			SET status = 'approved', approved_by = $2, approved_at = NOW()
			FROM aveloxis_ops.users u
			WHERE g.group_id = $1 AND g.status = 'pending' AND u.user_id = g.user_id
			RETURNING COALESCE(u.email, ''), u.login_name, g.name`,
			groupID, adminID).Scan(&a.RequesterEmail, &a.RequesterLogin, &a.GroupName)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("approve group: %w", err)
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
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		approval, approved = a, true
		return nil
	})
	if err != nil {
		return GroupApproval{}, false, err
	}
	return approval, approved, nil
}

// RejectGroup flips a group to rejected. Repos in the group are NOT
// deleted (the user might appeal); the group simply refuses all
// future additions (AddReposToGroup / AddOrgToGroup error) and its
// tracked orgs stop scanning (the v0.27.20 rejected-group gate in
// scanOrgRepos / refreshUserOrgs).
//
// v0.27.20: applies to ANY non-rejected group, not just 'pending' —
// with groups now created 'approved', this is the group-level abuse
// lever, so it must work on approved groups too.
func (s *PostgresStore) RejectGroup(ctx context.Context, groupID int64, adminID int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.user_groups
		SET status = 'rejected', approved_by = $2, approved_at = NOW()
		WHERE group_id = $1 AND status IS DISTINCT FROM 'rejected'`,
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
	CreatedAt time.Time // real join date (users.created_at, INSERT-only)
	LastSeen  time.Time // data_collection_date — re-stamped on every login
}

// ListUsers returns all users for the admin user-management page.
// Sorted by user_id ascending so the first-promoted (typically the
// operator) appears first.
func (s *PostgresStore) ListUsers(ctx context.Context) ([]AdminUser, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, login_name, COALESCE(email, ''), COALESCE(oauth_provider, ''),
		       admin,
		       COALESCE(created_at, data_collection_date, NOW()),
		       COALESCE(data_collection_date, NOW())
		FROM aveloxis_ops.users
		ORDER BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	var out []AdminUser
	for rows.Next() {
		var u AdminUser
		if err := rows.Scan(&u.UserID, &u.Login, &u.Email, &u.Provider, &u.IsAdmin, &u.CreatedAt, &u.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ErrLastAdmin is SetUserAdmin's refusal to demote the only remaining
// administrator: the store declining the operation, not failing (PR #218
// review, follow-up to C6 — untyped, it read as a server error once store
// failures became logged generic 500s). Handlers answer it 409.
var ErrLastAdmin = errors.New("refusing to demote the last admin — promote another user first")

// SetUserAdmin toggles the admin role for a user. Refuses to demote
// the last remaining admin (ErrLastAdmin) so the system can't end up with
// zero admins (which would leave the approval queue forever stuck).
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
			return ErrLastAdmin
		}
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_ops.users SET admin = $2 WHERE user_id = $1`,
		userID, isAdmin)
	return err
}
