// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
//   - Backfills all gh_* columns on the contributor row
//   - Handles login renames (same gh_user_id, different login)
//   - Creates contributor aliases for commit emails
//   - Uses deterministic GithubUUID for contributor IDs (Augur-compatible)
package collector

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// CommitResolver resolves commit authors to platform users.
type CommitResolver struct {
	store *db.PostgresStore
	http  *platform.HTTPClient
	// searchClient is the shared email→identity API surface (Search API +
	// global commit-search). An interface so tests can fake it; in
	// production it's a *github.Client built from the same key pool.
	searchClient emailSearchClient
	logger       *slog.Logger

	// Caches to avoid repeated lookups within a run.
	emailCache map[string]string // email -> gh_login (or "" for not found)
	hashCache  map[string]string // commit_hash -> gh_login
	// transient records emails whose API SEARCH (Strategy 4) failed WITHOUT
	// an answer this run (worklist item 16, review round 1): not a miss —
	// nothing is cached or stamped — but the next commits by the same author
	// skip the search instead of re-spending the budget and logging a WARN
	// each. Decided as a class (review round 2): the memo covers exactly
	// what failed, the search; the per-commit SHA lookup (Strategy 3) still
	// runs, since it can answer for a commit the email search never could.
	// Never persisted; the next run retries.
	transient map[string]bool

	// bareClone is the repository's bare clone (the facade's, whose HEAD is
	// the default branch it walks); defaultBranch is that branch's commit
	// set, listed at most once per run and only after the first 422 — see
	// loadDefaultBranch. nil until listed; listed says an attempt was made.
	bareClone     string
	defaultBranch map[string]bool
	listed        bool
}

// WithBareClone gives the resolver the repository's bare clone, so a run
// that meets commits no longer on the default branch (history rewritten
// upstream) can recognise them locally instead of spending a SHA lookup on
// each and aborting on the 422s (worklist 73). Without it the resolver
// behaves as before.
func (r *CommitResolver) WithBareClone(path string) *CommitResolver {
	r.bareClone = path
	return r
}

// loadDefaultBranch lists the bare clone's default-branch commits once per
// run (`git rev-list HEAD`; the facade walks the same branch) and reports
// whether a list is available. Called only after a 422, so a repository
// whose commits all resolve never pays for the listing. A failure is
// logged and leaves the old behaviour (the 50-in-a-row abort) in place.
func (r *CommitResolver) loadDefaultBranch(ctx context.Context, repoID int64, unresolved []unresolvedCommit) bool {
	if r.listed {
		return r.defaultBranch != nil
	}
	r.listed = true
	if r.bareClone == "" {
		return false
	}
	// Streamed, and only this run's unresolved hashes are kept: the run
	// asks about nothing else, and holding the whole listing plus a map of
	// every branch commit cost 100+ MB on a kernel-size repository (PR #218
	// review A9).
	want := make(map[string]struct{}, len(unresolved))
	for _, c := range unresolved {
		want[c.Hash] = struct{}{}
	}
	cmd := exec.CommandContext(ctx, "git", "-C", r.bareClone, "rev-list", "HEAD")
	stderr := &stderrCapture{}
	cmd.Stderr = stderr
	set := make(map[string]bool)
	err := func() error {
		// startSweptCommand, not cmd.StdoutPipe: it owns the process group
		// and the pipe, so a child that outlives git cannot wedge the read
		// (PR #207).
		swept, err := startSweptCommand(cmd)
		if err != nil {
			return err
		}
		defer swept.Close()
		sc := bufio.NewScanner(swept.Stdout)
		for sc.Scan() {
			if h := strings.TrimSpace(sc.Text()); h != "" {
				if _, ok := want[h]; ok {
					set[h] = true
				}
			}
		}
		scanErr := sc.Err()
		if scanErr != nil {
			// Drain so git is not blocked on a full pipe before Wait.
			_, _ = io.Copy(io.Discard, swept.Stdout)
		}
		if waitErr := swept.Wait(); waitErr != nil {
			return waitErr
		}
		return scanErr
	}()
	if err != nil && ctx.Err() != nil {
		return false // a stop, not a failed listing: the loop's ctx check ends the run
	}
	if err != nil {
		r.logger.Warn("commit resolution: listing the default branch failed — commits not on it cannot be told apart this run",
			"repo_id", repoID, "clone", r.bareClone, "error", execErr(ctx, err), "stderr", stderr.String())
		return false
	}
	r.defaultBranch = set
	return true
}

// errTransientMemo is resolveOne's answer for a commit whose email is in
// transient once the SHA lookup has missed: the loop counts it
// (TransientSkipped) and moves on, without a WARN, without Errors++ and
// without recording the email as unresolved.
var errTransientMemo = errors.New("search for this email already failed without an answer this run")

// errNotOnDefaultBranch is resolveOne's answer for a commit that needs an API
// lookup but is not on the listed default branch (worklist 73): the loop
// counts it (NotOnDefaultBranch) and moves on, with no WARN and no 422 count.
var errNotOnDefaultBranch = errors.New("commit is not on the default branch")

// NewCommitResolver creates a resolver using the GitHub API via the given key pool.
// NewCommitResolver builds a resolver against the GitHub API at baseURL, or
// public GitHub when baseURL is empty. The base is a parameter for the same
// reason the breadth worker takes one (v0.29.57): these are the deployment's
// keys, and a hardcoded api.github.com sends an Enterprise token to a third
// party. BOTH clients here are built on it — the REST client and the search
// client — because either one carries the key.
func NewCommitResolver(store *db.PostgresStore, keys *platform.KeyPool, baseURL string, logger *slog.Logger) *CommitResolver {
	baseURL = platform.GitHubAPIBaseOrPublic(baseURL)
	return &CommitResolver{
		store:        store,
		http:         platform.NewHTTPClient(baseURL, keys, logger, platform.AuthGitHub),
		searchClient: github.New(baseURL, keys, logger),
		logger:       logger,
		emailCache:   make(map[string]string),
		hashCache:    make(map[string]string),
		transient:    make(map[string]bool),
	}
}

// unresolvedCommit is a commit needing author resolution (local mirror of db.UnresolvedCommit).
type unresolvedCommit struct {
	Hash  string
	Email string
}

// ResolveResult tracks resolver statistics.
type ResolveResult struct {
	TotalCommits         int
	ResolvedNoreply      int
	ResolvedDBHit        int
	ResolvedAPI          int
	ResolvedSearch       int
	ResolvedCommitSearch int // resolved via global commit-search (shared resolver)
	Unresolved           int
	KeyExhausted         int // commits that failed because no API keys were available
	TransientSkipped     int // commits skipped because an earlier commit's search for the same email failed without an answer this run
	WriteFailed          int // commits resolved by a strategy whose write then failed (counted in Errors, not as resolved)
	Consecutive422       int // consecutive 422 "No commit found" errors from GitHub API
	NotOnDefaultBranch   int // unresolved commits no longer on the default branch (history rewritten upstream): skipped, rows kept
	ContribsCreated      int
	ContribsUpdated      int
	AliasesCreated       int
	Errors               int
	// SearchAttempts and SearchTime are the email searches (the API tail)
	// this run made and the wall time spent in them, rate-limit waits
	// included (worklist item 76).
	SearchAttempts int
	SearchTime     time.Duration
}

// resolved is every commit a strategy resolved AND whose write landed: a
// commit whose author was found but whose SetCommitAuthorLogin or
// contributor upsert failed is an error, not a resolution (WriteFailed;
// batch-3 review round 3 — counting it under both sent accounted() past
// the commits visited and KeyExhausted negative).
func (r *ResolveResult) resolved() int {
	return r.ResolvedNoreply + r.ResolvedDBHit + r.ResolvedAPI + r.ResolvedSearch + r.ResolvedCommitSearch - r.WriteFailed
}

// accounted is every commit the run has an outcome for: resolved,
// unresolved, errored, or skipped on a memoised search failure — each
// commit once. The two "remaining" formulas (key exhaustion, the 422 abort)
// subtract it from TotalCommits — one spelling (SR-17; batch-3 review round
// 2: both omitted TransientSkipped and ResolvedCommitSearch, so a key
// refusal after a memoised author counted the skipped commits as
// key-exhausted and flipped the run to FAILED).
func (r *ResolveResult) accounted() int {
	return r.resolved() + r.Unresolved + r.Errors + r.TransientSkipped + r.NotOnDefaultBranch
}

// IsSuccess returns true if the resolution completed meaningfully —
// i.e., most commits were resolved or legitimately unresolvable, not
// failed due to key exhaustion. It reads KeyExhausted alone; Errors and
// TransientSkipped reach it only through accounted() at a refusal.
//
// v0.20.10 (Fix F) fixes an integer-division bug in the original
// formulation `r.KeyExhausted < r.TotalCommits/2`. With TotalCommits=1,
// the threshold collapsed to 0, and `KeyExhausted < 0` is impossible
// for non-negative counters — so EVERY single-commit job was falsely
// reported as a failure even when its one commit resolved via the DB
// cache and no API call was made. 569 such false-positive ERRORs
// appeared in the May 9–12 production log, drowning out real
// key-exhaustion events.
//
// Correct form: failure if MORE than 50% of commits failed due to key
// exhaustion. Integer-arithmetic equivalent: success iff
// `KeyExhausted * 2 <= TotalCommits`. This treats 50%-exhausted as
// the inclusive success boundary (matches the original "more than
// 50%" docstring), and correctly identifies zero key exhaustion as
// success at any TotalCommits including 1.
func (r *ResolveResult) IsSuccess() bool {
	if r.TotalCommits == 0 {
		return true
	}
	return r.KeyExhausted*2 <= r.TotalCommits
}

// ShouldAbort422 returns true when the resolver should stop making API calls
// because too many consecutive 422 "No commit found" errors indicate the
// commits in the database don't belong to this repo (e.g., stale clone data
// from a previous repo_id assignment).
func (r *ResolveResult) ShouldAbort422() bool {
	return r.Consecutive422 >= 50
}

// ResolveCommits resolves all unresolved commits for a repo.
// Only works for GitHub repos (GitLab commit resolution uses a different API).
func (r *CommitResolver) ResolveCommits(ctx context.Context, repoID int64, owner, repo string) (*ResolveResult, error) {
	result := &ResolveResult{}
	runStart := time.Now()

	dbCommits, err := r.store.GetUnresolvedCommits(ctx, repoID)
	unresolvedQuery := time.Since(runStart)
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
		"unresolved", len(commits),
		"unresolved_query", unresolvedQuery.Round(time.Millisecond)) // worklist item 75

	for _, cmt := range commits {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		login, ghUserID, err := r.resolveOne(ctx, repoID, owner, repo, cmt, result)
		if errors.Is(err, context.Canceled) {
			return result, err // shutdown, not a failure (pass 35)
		}
		if errors.Is(err, errNotOnDefaultBranch) {
			result.NotOnDefaultBranch++
			continue
		}
		if errors.Is(err, errTransientMemo) {
			result.TransientSkipped++
			continue
		}
		if err != nil {
			// Distinguish key exhaustion from other errors — key exhaustion means
			// we should stop trying (all subsequent calls will fail too).
			// Typed (v0.29.70): the error text carries the request URL, so a
			// text match read a repository named "…invalidated…" as exhaustion.
			if isKeyExhaustion(err) {
				result.KeyExhausted = result.TotalCommits - result.accounted()
				r.logger.Error("commit resolution aborted: no API keys available",
					"repo_id", repoID,
					"resolved_so_far", result.resolved(),
					"remaining", result.KeyExhausted)
				break
			}
			// Track consecutive 422 "No commit found" errors. These mean the
			// commit SHAs in the database don't exist in this repo (usually caused
			// by a stale bare clone that belonged to a different repo). After 50
			// consecutive 422s, abort — continuing would just waste API calls.
			if errors.Is(err, platform.ErrUnprocessableEntity) {
				// A 422 on a commit the default branch no longer has is
				// rewritten history, not a stale clone (worklist 73: an
				// upstream rewrite left 359 unresolved rows and the run
				// aborted at every cycle). List the branch once and skip.
				if r.loadDefaultBranch(ctx, repoID, commits) && !r.defaultBranch[cmt.Hash] {
					result.NotOnDefaultBranch++
					continue
				}
				result.Consecutive422++
				if result.ShouldAbort422() {
					remaining := result.TotalCommits - result.accounted()
					r.logger.Error("commit resolution aborted: commits do not belong to this repo",
						"repo_id", repoID, "owner", owner, "repo", repo,
						"consecutive_422", result.Consecutive422,
						"remaining", remaining,
						"hint", "the bare clone may be stale — delete the clone dir and re-collect")
					result.Errors += remaining
					break
				}
			} else {
				result.Consecutive422 = 0 // reset on non-422 error
			}
			r.logger.Warn("failed to resolve commit", "hash", cmt.Hash[:8], "error", err)
			result.Errors++
			continue
		}
		// Successful resolution (or clean skip) — reset 422 counter.
		result.Consecutive422 = 0

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
			// Round-8 class sweep: warn-and-continue per commit, and
			// result.Errors reaches the key-exhaustion accounting — so a
			// shutdown both flooded the log and could report "commit
			// resolution FAILED" for a clean stop.
			if errors.Is(err, context.Canceled) {
				return result, err
			}
			r.logger.Warn("failed to set commit author login", "hash", cmt.Hash[:8], "error", err)
			result.Errors++
			result.WriteFailed++ // resolved, not recorded: counted once, as an error
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

	if result.NotOnDefaultBranch > 0 {
		r.logger.Info("commit resolution: unresolved commits no longer on the default branch were skipped (history rewritten upstream) — their rows are kept",
			"repo_id", repoID, "owner", owner, "repo", repo, "skipped", result.NotOnDefaultBranch)
	}

	// Bulk backfill: connect commits to contributors via cmt_ght_author_id.
	backfillStart := time.Now()
	n, err := r.store.BackfillCommitAuthorIDs(ctx, repoID)
	backfill := time.Since(backfillStart) // worklist item 75: up to 4,034 s on kate
	if err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("backfill cmt_ght_author_id failed", "repo_id", repoID, "duration", backfill.Round(time.Millisecond), "error", err)
		}
	} else if n > 0 {
		r.logger.Info("backfilled cmt_ght_author_id", "repo_id", repoID, "rows", n, "duration", backfill.Round(time.Millisecond))
	}

	logLevel := slog.LevelInfo
	status := "complete"
	if !result.IsSuccess() {
		logLevel = slog.LevelError
		status = "FAILED (no API keys available — most commits unresolved)"
	}
	r.logger.Log(ctx, logLevel, "commit resolution "+status,
		"repo_id", repoID, // worklist items 75/76
		"duration", time.Since(runStart).Round(time.Millisecond),
		"backfill_duration", backfill.Round(time.Millisecond),
		"search_attempts", result.SearchAttempts,
		"search_time", result.SearchTime.Round(time.Millisecond),
		"total", result.TotalCommits,
		"noreply", result.ResolvedNoreply,
		"db_hit", result.ResolvedDBHit,
		"api", result.ResolvedAPI,
		"search", result.ResolvedSearch,
		"commit_search", result.ResolvedCommitSearch,
		"unresolved", result.Unresolved,
		"key_exhausted", result.KeyExhausted,
		"transient_skipped", result.TransientSkipped,
		"not_on_default_branch", result.NotOnDefaultBranch,
		"write_failed", result.WriteFailed,
		"errors", result.Errors,
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

	// Skip automation/junk emails and non-email strings (names, etc.)
	// (IsAutomationEmail ⊇ IsBotEmail — review 2026-08-30 #12: a relay
	// address as a commit author must never acquire an alias/identity.)
	if IsAutomationEmail(email) || email == "" || !strings.Contains(email, "@") {
		r.emailCache[email] = ""
		r.hashCache[cmt.Hash] = ""
		return "", 0, nil
	}

	// Strategy 2: DB lookup by email. A DB error is returned, not read as
	// "not in the DB" (SR-5; worklist item 17): the API strategies would
	// otherwise spend calls, and resolve, for an email the store knew.
	login, err := r.store.FindLoginByEmail(ctx, email)
	if err != nil {
		return "", 0, fmt.Errorf("look up email in the store: %w", err)
	}
	if login != "" {
		r.emailCache[email] = login
		r.hashCache[cmt.Hash] = login
		result.ResolvedDBHit++
		return login, 0, nil
	}

	// Once the default branch is listed, a commit not on it gets no API
	// lookup (worklist 73): the SHA lookup answers 422 for it, and the
	// email search was never reached for a 422. The free strategies above
	// still ran (final review F1 of v0.29.69).
	if r.defaultBranch != nil && !r.defaultBranch[cmt.Hash] {
		return "", 0, errNotOnDefaultBranch
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

	// Strategy 4: shared API tail — GitHub Search API, then global
	// commit-search (the shared resolver, summary/12 §5g). Skipped for an
	// email whose search already failed without an answer this run (the
	// memo; the SHA lookup above still had its chance). commit-search
	// resolves private-profile-email authors that user-search misses, and
	// it now yields the gh_user_id too (the old githubEmailSearch returned
	// login only).
	if r.transient[email] {
		return "", 0, errTransientMemo
	}
	searchStart := time.Now()
	login, ghUserID, source, err := ResolveEmailViaAPI(ctx, r.searchClient, email)
	// Worklist item 76: what the email searches cost this repository (the
	// time includes the search API's rate-limit waits).
	result.SearchAttempts++
	result.SearchTime += time.Since(searchStart)
	if err != nil && !platform.IsDefinitiveAnswer(err) {
		if !errors.Is(err, context.Canceled) {
			r.transient[email] = true // the rest of the run skips this email's search
		}
		return "", 0, err
	}
	if err != nil {
		// The search gave a DEFINITIVE answer that is not a hit
		// (platform.IsDefinitiveAnswer: a rejected request such as the 422
		// on a malformed address "m - @ - halle.us", or a ClassSkip answer
		// like not-found): a no-match, the answer the scheduler's search
		// sweep already stamps. It falls through to
		// the not-found path so the miss is cached for the run — it used
		// to be an error per commit, re-searched every time, and fed
		// Consecutive422, the stale-clone abort meant for commit SHAs
		// (2026-09-23 log review).
		r.logger.Debug("commit author email search gave a definitive no-match", "email", email, "error", err)
		login = ""
	}
	if login != "" {
		r.emailCache[email] = login
		r.hashCache[cmt.Hash] = login
		if source == EmailSourceCommitSearch {
			result.ResolvedCommitSearch++
		} else {
			result.ResolvedSearch++
		}
		return login, ghUserID, nil
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
	AvatarURL         string
	HTMLURL           string
	GravatarID        string
	URL               string
	FollowersURL      string
	FollowingURL      string
	GistsURL          string
	StarredURL        string
	SubscriptionsURL  string
	OrganizationsURL  string
	ReposURL          string
	EventsURL         string
	ReceivedEventsURL string
	Type              string
	SiteAdmin         bool
	// From commit.author (git-level).
	Name  string
	Email string
}

// githubCommitLookup calls GET /repos/{owner}/{repo}/commits/{sha}.
func (r *CommitResolver) githubCommitLookup(ctx context.Context, owner, repo, sha string) (*ghCommitAuthor, error) {
	path := fmt.Sprintf("/repos/%s/%s/commits/%s", owner, repo, sha)

	// v0.28.18: ETag-free — a body-decoding reader cannot use a 304.
	resp, err := r.http.Get(platform.WithoutETag(ctx), path)
	if err != nil {
		// The typed sentinel, never the error's text (old problem O4, SR-5:
		// a 422 whose URL contained "not found" read as a definitive 404).
		if errors.Is(err, platform.ErrNotFound) {
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

// (githubEmailSearch removed v0.25.x — its Search-API logic now lives in the
// shared github.Client.SearchUserByEmail, invoked via ResolveEmailViaAPI in
// Strategy 4, which also adds the global commit-search fallback.)

// ensureContributor creates or updates a contributor with full gh_* fields.
// Uses the deterministic GithubUUID for the cntrb_id.
func (r *CommitResolver) ensureContributor(ctx context.Context, login string, ghUserID int64, commitEmail string, result *ResolveResult) {
	desiredID := db.GithubUUID(ghUserID).String()

	created, actualID, err := r.store.UpsertContributorFull(ctx, desiredID, login, ghUserID, commitEmail)
	if err != nil {
		// Round-8 class sweep: a shutdown must not count as a resolve
		// error — result.Errors reaches the key-exhaustion accounting.
		// The caller's loop-top ctx guard ends the pass on the next
		// iteration.
		if errors.Is(err, context.Canceled) {
			return
		}
		r.logger.Warn("failed to upsert contributor", "login", login, "error", err)
		result.Errors++
		result.WriteFailed++ // resolved, not recorded: counted once, as an error
		return
	}
	if created {
		result.ContribsCreated++
	} else {
		result.ContribsUpdated++
	}

	// Create alias using the actual cntrb_id (may differ from desiredID
	// if the login already existed under a different UUID).
	if commitEmail != "" && !IsNoreplyEmail(commitEmail) && !IsAutomationEmail(commitEmail) {
		if err := r.store.EnsureContributorAlias(ctx, actualID, commitEmail, "aveloxis-commit-resolver", "GitHub API"); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				r.logger.Warn("failed to create alias", "email", commitEmail, "error", err)
			}
		} else {
			result.AliasesCreated++
		}
	}
}

// ensureAlias creates an alias for a commit email when we resolved the login
// but don't have a gh_user_id (resolved via DB lookup or Search API).
func (r *CommitResolver) ensureAlias(ctx context.Context, login, commitEmail string, result *ResolveResult) {
	if commitEmail == "" || IsNoreplyEmail(commitEmail) || IsAutomationEmail(commitEmail) {
		return
	}
	// Look up the contributor by login to get their cntrb_id.
	cntrbID, err := r.store.FindContributorIDByLogin(ctx, login)
	if err != nil {
		// Logged (everything that errors is): the login already landed on the
		// commit, so nothing revisits it — the alias is what this run loses.
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("failed to look the contributor up for its alias", "login", login, "error", err)
		}
		return
	}
	if cntrbID == "" {
		return
	}
	if err := r.store.EnsureContributorAlias(ctx, cntrbID, commitEmail, "aveloxis-commit-resolver", "GitHub API"); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("failed to create alias", "email", commitEmail, "error", err)
		}
	} else {
		result.AliasesCreated++
	}
	// v0.25.6: also backfill cntrb_canonical from the commit email when
	// the contributor row doesn't have one yet. SetContributorCanonical
	// uses COALESCE(NULLIF(cntrb_canonical, ''), $2) so an existing
	// non-empty value is preserved — we never overwrite a real canonical
	// with a commit author email. Closes a long-standing gap where
	// strategies 2 (DB lookup) and 4 (Search API) populated the alias
	// table but left cntrb_canonical empty on the parent contributor
	// row, suppressing the email-canonical join path downstream.
	if err := r.store.SetContributorCanonical(ctx, cntrbID, commitEmail); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("failed to backfill canonical", "cntrb_id", cntrbID, "email", commitEmail, "error", err)
		}
	}
}

// isKeyExhaustion reports whether err means the key pool can serve no
// request: empty (ErrNoKeys) or every key invalidated. Decided on the
// pool's typed errors, never on text (SR-5).
func isKeyExhaustion(err error) bool {
	return errors.Is(err, platform.ErrNoKeys) || errors.Is(err, platform.ErrAllKeysInvalidated)
}
