// Package collector — commit_resolver.go resolves git commit authors to
// GitHub/GitLab users, populating cmt_author_platform_username and enriching
// the contributors table with full platform profile data.
//
// This is the Go implementation of augur-contributor-resolver scripts 2 and 3.
// It runs as a post-facade phase, after commits are inserted from git log.
//
// Resolution strategy (in priority order, cheapest first):
//  1. Noreply email parse — zero API calls, extracts login+user_id from
//     GitHub noreply addresses like 12345+user@users.noreply.github.com
//  2. Contributor DB lookup — check if we already know this email
//  3. GitHub Commits API — GET /repos/{owner}/{repo}/commits/{sha} returns
//     the linked GitHub user with all profile fields
//  4. GitHub Search API — search/users?q=email (for non-noreply emails)
//
// The resolver also:
//  - Backfills all gh_* columns on the contributor row
//  - Handles login renames (same gh_user_id, different login)
//  - Creates contributor aliases for commit emails
//  - Uses deterministic GithubUUID for contributor IDs (Augur-compatible)
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// CommitResolver resolves commit authors to platform users.
type CommitResolver struct {
	store  *db.PostgresStore
	http   *platform.HTTPClient
	logger *slog.Logger

	// Caches to avoid repeated lookups within a run.
	emailCache map[string]string // email -> gh_login (or "" for not found)
	hashCache  map[string]string // commit_hash -> gh_login
}

// NewCommitResolver creates a resolver using the GitHub API via the given key pool.
func NewCommitResolver(store *db.PostgresStore, keys *platform.KeyPool, logger *slog.Logger) *CommitResolver {
	return &CommitResolver{
		store:      store,
		http:       platform.NewHTTPClient("https://api.github.com", keys, logger),
		logger:     logger,
		emailCache: make(map[string]string),
		hashCache:  make(map[string]string),
	}
}

// unresolvedCommit is a commit needing author resolution (local mirror of db.UnresolvedCommit).
type unresolvedCommit struct {
	Hash  string
	Email string
}

// ResolveResult tracks resolver statistics.
type ResolveResult struct {
	TotalCommits    int
	ResolvedNoreply int
	ResolvedDBHit   int
	ResolvedAPI     int
	ResolvedSearch  int
	Unresolved      int
	ContribsCreated int
	ContribsUpdated int
	AliasesCreated  int
	Errors          int
}

// ResolveCommits resolves all unresolved commits for a repo.
// Only works for GitHub repos (GitLab commit resolution uses a different API).
func (r *CommitResolver) ResolveCommits(ctx context.Context, repoID int64, owner, repo string) (*ResolveResult, error) {
	result := &ResolveResult{}

	dbCommits, err := r.store.GetUnresolvedCommits(ctx, repoID)
	if err != nil {
		return result, fmt.Errorf("querying unresolved commits: %w", err)
	}
	// Convert to local type.
	commits := make([]unresolvedCommit, len(dbCommits))
	for i, c := range dbCommits {
		commits[i] = unresolvedCommit{Hash: c.Hash, Email: c.Email}
	}
	result.TotalCommits = len(commits)

	if len(commits) == 0 {
		return result, nil
	}

	r.logger.Info("resolving commit authors",
		"repo_id", repoID, "owner", owner, "repo", repo,
		"unresolved", len(commits))

	for _, cmt := range commits {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		login, ghUserID, err := r.resolveOne(ctx, repoID, owner, repo, cmt, result)
		if err != nil {
			r.logger.Warn("failed to resolve commit", "hash", cmt.Hash[:8], "error", err)
			result.Errors++
			continue
		}
		if login == "" {
			result.Unresolved++
			// Store unresolved email for future resolution attempts.
			if cmt.Email != "" && strings.Contains(cmt.Email, "@") {
				r.store.InsertUnresolvedEmail(ctx, cmt.Email)
			}
			continue
		}

		// Update commit rows with the resolved login.
		if err := r.store.SetCommitAuthorLogin(ctx, repoID, cmt.Hash, login); err != nil {
			r.logger.Warn("failed to set commit author login", "hash", cmt.Hash[:8], "error", err)
			result.Errors++
			continue
		}

		// Ensure contributor exists with full gh_* fields and create alias.
		if ghUserID > 0 {
			r.ensureContributor(ctx, login, ghUserID, cmt.Email, result)
		} else {
			// Resolved via DB or Search (no user ID). Still create alias.
			r.ensureAlias(ctx, login, cmt.Email, result)
		}
	}

	// Bulk backfill: connect commits to contributors via cmt_ght_author_id.
	if n, err := r.store.BackfillCommitAuthorIDs(ctx, repoID); err != nil {
		r.logger.Warn("backfill cmt_ght_author_id failed", "error", err)
	} else if n > 0 {
		r.logger.Info("backfilled cmt_ght_author_id", "rows", n)
	}

	r.logger.Info("commit resolution complete",
		"total", result.TotalCommits,
		"noreply", result.ResolvedNoreply,
		"db_hit", result.ResolvedDBHit,
		"api", result.ResolvedAPI,
		"search", result.ResolvedSearch,
		"unresolved", result.Unresolved,
		"contribs_created", result.ContribsCreated,
		"contribs_updated", result.ContribsUpdated,
		"aliases", result.AliasesCreated)

	return result, nil
}

// resolveOne tries all strategies to resolve a single commit's author.
// Returns (login, gh_user_id, error).
func (r *CommitResolver) resolveOne(ctx context.Context, repoID int64, owner, repo string, cmt unresolvedCommit, result *ResolveResult) (string, int64, error) {
	email := cmt.Email

	// Check hash cache first.
	if login, ok := r.hashCache[cmt.Hash]; ok {
		if login != "" {
			result.ResolvedDBHit++
		}
		return login, 0, nil
	}

	// Check email cache.
	if login, ok := r.emailCache[email]; ok {
		if login != "" {
			result.ResolvedDBHit++
			r.hashCache[cmt.Hash] = login
		}
		return login, 0, nil
	}

	// Strategy 1: Parse noreply email (free, no API call).
	if info := ParseNoreplyEmail(email); info != nil {
		r.emailCache[email] = info.Login
		r.hashCache[cmt.Hash] = info.Login
		result.ResolvedNoreply++
		return info.Login, info.UserID, nil
	}

	// Skip bot/junk emails and non-email strings (names, etc.)
	if IsBotEmail(email) || email == "" || !strings.Contains(email, "@") {
		r.emailCache[email] = ""
		r.hashCache[cmt.Hash] = ""
		return "", 0, nil
	}

	// Strategy 2: DB lookup by email.
	if login, err := r.store.FindLoginByEmail(ctx, email); err == nil && login != "" {
		r.emailCache[email] = login
		r.hashCache[cmt.Hash] = login
		result.ResolvedDBHit++
		return login, 0, nil
	}

	// Strategy 3: GitHub Commits API.
	info, err := r.githubCommitLookup(ctx, owner, repo, cmt.Hash)
	if err != nil {
		return "", 0, err
	}
	if info != nil {
		r.emailCache[email] = info.Login
		r.hashCache[cmt.Hash] = info.Login
		result.ResolvedAPI++
		return info.Login, info.UserID, nil
	}

	// Strategy 4: GitHub Search API (email search).
	login, err := r.githubEmailSearch(ctx, email)
	if err != nil {
		return "", 0, err
	}
	if login != "" {
		r.emailCache[email] = login
		r.hashCache[cmt.Hash] = login
		result.ResolvedSearch++
		return login, 0, nil
	}

	// Not found.
	r.emailCache[email] = ""
	r.hashCache[cmt.Hash] = ""
	return "", 0, nil
}

// ghCommitAuthor holds the fields we extract from the GitHub Commits API.
type ghCommitAuthor struct {
	Login  string
	UserID int64
	NodeID string
	// All the gh_* profile fields.
	AvatarURL        string
	HTMLURL          string
	GravatarID       string
	URL              string
	FollowersURL     string
	FollowingURL     string
	GistsURL         string
	StarredURL       string
	SubscriptionsURL string
	OrganizationsURL string
	ReposURL         string
	EventsURL        string
	ReceivedEventsURL string
	Type             string
	SiteAdmin        bool
	// From commit.author (git-level).
	Name  string
	Email string
}

// githubCommitLookup calls GET /repos/{owner}/{repo}/commits/{sha}.
func (r *CommitResolver) githubCommitLookup(ctx context.Context, owner, repo, sha string) (*ghCommitAuthor, error) {
	path := fmt.Sprintf("/repos/%s/%s/commits/%s", owner, repo, sha)

	resp, err := r.http.Get(ctx, path)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, nil // 404 — commit not on GitHub
		}
		return nil, err
	}
	defer resp.Body.Close()

	var data struct {
		Author *struct {
			Login             string `json:"login"`
			ID                int64  `json:"id"`
			NodeID            string `json:"node_id"`
			AvatarURL         string `json:"avatar_url"`
			GravatarID        string `json:"gravatar_id"`
			URL               string `json:"url"`
			HTMLURL           string `json:"html_url"`
			FollowersURL      string `json:"followers_url"`
			FollowingURL      string `json:"following_url"`
			GistsURL          string `json:"gists_url"`
			StarredURL        string `json:"starred_url"`
			SubscriptionsURL  string `json:"subscriptions_url"`
			OrganizationsURL  string `json:"organizations_url"`
			ReposURL          string `json:"repos_url"`
			EventsURL         string `json:"events_url"`
			ReceivedEventsURL string `json:"received_events_url"`
			Type              string `json:"type"`
			SiteAdmin         bool   `json:"site_admin"`
		} `json:"author"`
		Committer *struct {
			Login string `json:"login"`
			ID    int64  `json:"id"`
		} `json:"committer"`
		Commit struct {
			Author struct {
				Name  string `json:"name"`
				Email string `json:"email"`
			} `json:"author"`
		} `json:"commit"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("decoding commit API response: %w", err)
	}

	// Try author first, fall back to committer.
	if data.Author != nil && data.Author.Login != "" {
		return &ghCommitAuthor{
			Login:             data.Author.Login,
			UserID:            data.Author.ID,
			NodeID:            data.Author.NodeID,
			AvatarURL:         data.Author.AvatarURL,
			HTMLURL:           data.Author.HTMLURL,
			GravatarID:        data.Author.GravatarID,
			URL:               data.Author.URL,
			FollowersURL:      data.Author.FollowersURL,
			FollowingURL:      data.Author.FollowingURL,
			GistsURL:          data.Author.GistsURL,
			StarredURL:        data.Author.StarredURL,
			SubscriptionsURL:  data.Author.SubscriptionsURL,
			OrganizationsURL:  data.Author.OrganizationsURL,
			ReposURL:          data.Author.ReposURL,
			EventsURL:         data.Author.EventsURL,
			ReceivedEventsURL: data.Author.ReceivedEventsURL,
			Type:              data.Author.Type,
			SiteAdmin:         data.Author.SiteAdmin,
			Name:              data.Commit.Author.Name,
			Email:             data.Commit.Author.Email,
		}, nil
	}

	if data.Committer != nil && data.Committer.Login != "" {
		return &ghCommitAuthor{
			Login:  data.Committer.Login,
			UserID: data.Committer.ID,
			Name:   data.Commit.Author.Name,
			Email:  data.Commit.Author.Email,
		}, nil
	}

	return nil, nil // no GitHub user linked
}

// githubEmailSearch uses the Search API to find a login by email.
func (r *CommitResolver) githubEmailSearch(ctx context.Context, email string) (string, error) {
	// Strip surrounding quotes that sometimes appear in git log author emails
	// (e.g., `"steve@example.com"`). GitHub rejects quoted search queries with 400.
	email = strings.Trim(email, `"' `)
	if email == "" || !strings.Contains(email, "@") {
		return "", nil
	}
	path := fmt.Sprintf("/search/users?q=%s+in:email&per_page=1", url.QueryEscape(email))

	resp, err := r.http.Get(ctx, path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var data struct {
		TotalCount int `json:"total_count"`
		Items      []struct {
			Login string `json:"login"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", nil // don't fail on decode error for search
	}

	if data.TotalCount > 0 && len(data.Items) > 0 {
		return data.Items[0].Login, nil
	}
	return "", nil
}

// ensureContributor creates or updates a contributor with full gh_* fields.
// Uses the deterministic GithubUUID for the cntrb_id.
func (r *CommitResolver) ensureContributor(ctx context.Context, login string, ghUserID int64, commitEmail string, result *ResolveResult) {
	desiredID := db.GithubUUID(ghUserID).String()

	created, actualID, err := r.store.UpsertContributorFull(ctx, desiredID, login, ghUserID, commitEmail)
	if err != nil {
		r.logger.Warn("failed to upsert contributor", "login", login, "error", err)
		result.Errors++
		return
	}
	if created {
		result.ContribsCreated++
	} else {
		result.ContribsUpdated++
	}

	// Create alias using the actual cntrb_id (may differ from desiredID
	// if the login already existed under a different UUID).
	if commitEmail != "" && !IsNoreplyEmail(commitEmail) && !IsBotEmail(commitEmail) {
		if err := r.store.EnsureContributorAlias(ctx, actualID, commitEmail); err != nil {
			r.logger.Warn("failed to create alias", "email", commitEmail, "error", err)
		} else {
			result.AliasesCreated++
		}
	}
}

// ensureAlias creates an alias for a commit email when we resolved the login
// but don't have a gh_user_id (resolved via DB lookup or Search API).
func (r *CommitResolver) ensureAlias(ctx context.Context, login, commitEmail string, result *ResolveResult) {
	if commitEmail == "" || IsNoreplyEmail(commitEmail) || IsBotEmail(commitEmail) {
		return
	}
	// Look up the contributor by login to get their cntrb_id.
	cntrbID, err := r.store.FindContributorIDByLogin(ctx, login)
	if err != nil || cntrbID == "" {
		return
	}
	if err := r.store.EnsureContributorAlias(ctx, cntrbID, commitEmail); err != nil {
		r.logger.Warn("failed to create alias", "email", commitEmail, "error", err)
	} else {
		result.AliasesCreated++
	}
}

// ResolveEmailsToCanonical enriches contributors that have gh_login but
// no cntrb_canonical by calling the GitHub Users API to get their profile email.
func (r *CommitResolver) ResolveEmailsToCanonical(ctx context.Context) (int, error) {
	contribs, err := r.store.GetContributorsMissingCanonical(ctx)
	if err != nil {
		return 0, err
	}
	if len(contribs) == 0 {
		return 0, nil
	}

	r.logger.Info("enriching contributor canonical emails", "count", len(contribs))
	updated := 0

	for _, c := range contribs {
		if err := ctx.Err(); err != nil {
			return updated, err
		}

		path := fmt.Sprintf("/users/%s", c.Login)
		resp, err := r.http.Get(ctx, path)
		if err != nil {
			continue
		}

		var user struct {
			Email string `json:"email"`
		}
		json.NewDecoder(resp.Body).Decode(&user)
		resp.Body.Close()

		if user.Email != "" && strings.Contains(user.Email, "@") &&
			!strings.Contains(strings.ToLower(user.Email), "noreply") {
			if err := r.store.SetContributorCanonical(ctx, c.ID, user.Email); err == nil {
				updated++
			}
		}

		// Small delay to be respectful of rate limits on the Users API.
		time.Sleep(100 * time.Millisecond)
	}

	r.logger.Info("canonical email enrichment complete", "updated", updated)
	return updated, nil
}
