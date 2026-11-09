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
// It is NOT model.NormalizeInstanceWebBase (which drops www., a default
// port and collapses the path): the stored key is frozen on every account
// row since 0.29.69, and a respelling of web.gitlab_base_url is documented
// as a move to another instance (configuration.md); the two rules meet on
// one input only at config.Load, which admits by the model's rule and
// stores by this one (0.29.101 round 1, declined: a new spelling would
// re-key every GitLab account).
func GitLabOAuthHost(base string) string {
	h := strings.TrimRight(strings.ToLower(strings.TrimSpace(base)), "/")
	if h == "" {
		return platform.PublicGitLabWebBase
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
	if errors.Is(err, pgx.ErrNoRows) {
		// No such account is a definitive answer (a deleted account with a
		// live session), not a lookup failure: never an administrator.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("admin flag for user %d: %w", userID, err)
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
		searchPattern := LikeContains(strings.ToLower(search)) // % and _ are literal (stage 1b review, the class sweep)
		err = s.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			  AND (LOWER(r.repo_name) LIKE $2 ESCAPE '\' OR LOWER(r.repo_owner) LIKE $2 ESCAPE '\' OR LOWER(r.repo_git) LIKE $2 ESCAPE '\')`,
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
	if detail.Orgs, err = s.GetGroupOrgs(ctx, groupID); err != nil {
		return nil, 0, fmt.Errorf("load orgs of group %d: %w", groupID, err)
	}

	return detail, totalRepos, nil
}

// loadGroupRepos fetches paginated repos for a group, optionally filtered by search.
func (s *PostgresStore) loadGroupRepos(ctx context.Context, groupID int64, search string, limit, offset int) ([]GroupRepo, error) {
	var repoRows pgx.Rows
	var err error

	if search != "" {
		searchPattern := LikeContains(strings.ToLower(search))
		repoRows, err = s.pool.Query(ctx, `
			SELECT r.repo_id, r.repo_name, r.repo_owner, r.repo_git, r.platform_id
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			  AND (LOWER(r.repo_name) LIKE $2 ESCAPE '\' OR LOWER(r.repo_owner) LIKE $2 ESCAPE '\' OR LOWER(r.repo_git) LIKE $2 ESCAPE '\')
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

// GetGroupOrgs lists a group's tracked organizations
// (aveloxis_ops.user_org_requests — presence there means approved to scan,
// per the v0.27.20 rule): one loader for the web GUI's group page and the
// API's /groups/{id}/orgs, so the two surfaces cannot disagree on what a
// "tracked org" is. Who may read the group is decided before this is
// called (admin.Service.GroupAccess).
func (s *PostgresStore) GetGroupOrgs(ctx context.Context, groupID int64) ([]GroupOrg, error) {
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
	return platform.WithDefaultScheme(strings.TrimSuffix(strings.TrimSpace(orgURL), "/"))
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
func (s *PostgresStore) AddOrgToGroup(ctx context.Context, userID int, groupID int64, orgURL, ghAPIBase, glWebBase string) (OrgAddOutcome, error) {
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
	if _, platformName := parseOrgURLMeta(orgURL, glWebBase); true {
		if err := orgRegistrable(platformName, orgURL, ghAPIBase, glWebBase); err != nil {
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
		if _, err := registerApprovedOrg(ctx, tx, AddRequest{UserID: userID, GroupID: groupID, OrgURL: orgURL}, ghAPIBase, glWebBase); err != nil {
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
	if _, err := registerApprovedOrg(ctx, tx, AddRequest{UserID: userID, GroupID: groupID, OrgURL: orgURL}, ghAPIBase, glWebBase); err != nil {
		return out, err
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("register org: %w", err)
	}
	out.Registered = true
	return out, nil
}

// RemoveRepoFromGroup removes a repo from a user group and reports
// whether a link was removed (false: the repository was not in the group;
// v0.29.91 — the api answered "removed" either way).
func (s *PostgresStore) RemoveRepoFromGroup(ctx context.Context, userID int, groupID, repoID int64) (bool, error) {
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2`,
		groupID, repoID)
	if err != nil {
		return false, fmt.Errorf("remove repository from group: %w", err)
	}
	return tag.RowsAffected() > 0, nil
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
// ghAPIBase, or a "gitlab" row on the configured instance, glWebBase — ""
// while no GitLab key exists): a row a gate skips stays NULL forever too
// (v0.29.57 rounds 2–3; the GitLab arm since stage 1e of summary/59).
func (s *PostgresStore) HasNeverScannedOrgs(ctx context.Context, ghAPIBase, glWebBase string) (bool, error) {
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
		if OrgScanEligible(platformName, orgURL, ghAPIBase, glWebBase) {
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
	if err := s.goneIfUnlinked(ctx, n, repoID); err != nil {
		return false, err
	}
	return n > 0, nil
}

// AddRepoIDsToGroup links repoIDs into groupID with ONE decision of the
// owner's quotas: all of the new repositories or none (v0.29.91, whole-PR
// review: the Comparisons auto-add of an organization linked item by item,
// partial on a refusal, ~11 round trips a repository). It returns the
// links inserted; a refusal is the *capacity.Exceeded.
func (s *PostgresStore) AddRepoIDsToGroup(ctx context.Context, groupID int64, repoIDs []int64) (int, error) {
	return s.linkWithinCap(ctx, groupID, repoIDs, linkAll)
}

// AddOrgRepoToGroupByID is AddRepoToGroupByID for a repository an
// organization registration brings (the scans, add-repo's org expansion):
// within the owner's repository allocation, and counted as the account's
// daily additions unless an administrator decided the registration that
// brings it (orgLinksCounted; v0.29.91 — an auto-approved registration is
// the account's own decision).
func (s *PostgresStore) AddOrgRepoToGroupByID(ctx context.Context, groupID, repoID int64) (bool, error) {
	n, err := s.linkWithinCap(ctx, groupID, []int64{repoID}, linkOrg)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// AddOperatorRepoToGroupByID is AddRepoToGroupByID for a link the
// operator's own command makes (the foundation loader's dashboard groups):
// within the owner's repository allocation, not counted as the account's
// daily additions (the operator decided it).
func (s *PostgresStore) AddOperatorRepoToGroupByID(ctx context.Context, groupID, repoID int64) (bool, error) {
	n, err := s.linkWithinCap(ctx, groupID, []int64{repoID}, linkOperator)
	if err != nil {
		return false, err
	}
	if err := s.goneIfUnlinked(ctx, n, repoID); err != nil {
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

// RejectGroup flips a group to rejected. Repos in the group are NOT
// deleted (the user might appeal); the group simply refuses all
// future additions (AddReposToGroup / AddOrgToGroup error) and its
// tracked orgs stop scanning (the v0.27.20 rejected-group gate in
// refreshUserOrgs).
//
// v0.27.20: applies to ANY non-rejected group, not just 'pending' —
// with groups now created 'approved', this is the group-level abuse
// lever, so it must work on approved groups too. The legacy pending-group
// approval (ListPendingGroups / ApproveGroup, v0.19.0) is gone since
// 0.29.100 (N9, stage 1g of summary/59): the migrate converted every
// pending group into add requests at v0.27.20, and the only INSERT writes
// 'approved'. A group that does not exist touches no row: the admin
// service answers NotFound ahead of this.
func (s *PostgresStore) RejectGroup(ctx context.Context, groupID int64, adminID int) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.user_groups
		SET status = 'rejected', approved_by = $2, approved_at = NOW()
		WHERE group_id = $1 AND status IS DISTINCT FROM 'rejected'`,
		groupID, adminID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
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
		SELECT user_id, login_name, COALESCE(email, ''), `+effectiveProviderSQL("")+`,
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

// ErrUserNotFound is the ONE answer for an account that does not exist:
// SetUserAdmin's target (the UPDATE touched no row), a capacity account
// read by id or login, an API token grant's owner. The caller's input,
// not a failure (0.29.106, whole-branch review DB-F5: three sentinels for
// one condition, each classified at a different layer).
var ErrUserNotFound = errors.New("no such account")

// adminRoleLockClass is the two-argument advisory lock space SetUserAdmin
// serialises on (key 0): the "another administrator remains" rule is a
// count-then-write, and two demotions deciding at once each saw the other
// as the remaining administrator (DBS-10, stage 1 of summary/59).
const adminRoleLockClass = 0x61767A // "avz"

// SetUserAdmin sets the admin role for a user, atomically: one transaction
// holds the admin-role lock, counts the OTHER administrators, refuses to
// demote the last one (ErrLastAdmin — the approval queue would be stuck
// for good), and writes. ErrUserNotFound when no such user. Self-demotion
// is the service's refusal (admin.ErrSelfDemotion), ahead of this.
func (s *PostgresStore) SetUserAdmin(ctx context.Context, userID int, isAdmin bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("set admin: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(adminRoleLockClass), int32(0)); err != nil {
		return fmt.Errorf("set admin: lock: %w", err)
	}
	if !isAdmin {
		var otherAdmins int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM aveloxis_ops.users WHERE admin = TRUE AND user_id != $1`, userID).Scan(&otherAdmins); err != nil {
			return fmt.Errorf("count remaining admins: %w", err)
		}
		if otherAdmins == 0 {
			return ErrLastAdmin
		}
	}
	tag, err := tx.Exec(ctx,
		`UPDATE aveloxis_ops.users SET admin = $2 WHERE user_id = $1`,
		userID, isAdmin)
	if err != nil {
		return fmt.Errorf("set admin: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user %d: %w", userID, ErrUserNotFound)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("set admin: %w", err)
	}
	return nil
}
