<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Adding a new platform

This chapter walks through adding a new data source to Aveloxis. It uses **Bugzilla** as the worked example because Bugzilla forces honesty about what does and doesn't fit Aveloxis's existing platform model — it's an issue tracker with no git component, no pull requests, no releases. If you can add Bugzilla, you can add Gitea, Forgejo, SourceHut, Jira, Phabricator, or anything else.

The chapter is long because the work is real. Read it once end-to-end before touching code, then use it as a checklist.

## What "a platform" means in Aveloxis

A platform is a data source that produces structured records about software development activity. Aveloxis already supports three:

| `Platform` enum | ID | What it produces |
|---|---|---|
| `PlatformGitHub` | 1 | Repos, issues, PRs, reviews, comments, contributors, releases, commits (via git) |
| `PlatformGitLab` | 2 | Repos, issues, merge requests, reviews, comments, contributors, releases, commits (via git) |
| `PlatformGenericGit` | 3 | Git-only — facade walks commits, no API collection |

The existing `platform.Client` interface (defined in `internal/platform/platform.go`) is heavily shaped around "git forge with REST/GraphQL API." It assumes the platform has issues, PRs, contributors, etc. A platform that's missing some of these (Bugzilla has no PRs; Generic Git has no API) must:

1. Implement the interface anyway.
2. Return empty iterators / nil for the methods that don't apply.
3. Make sure the scheduler's pipeline skips the phases that depend on those methods returning useful data.

The third point is where the architectural design call lives. We'll get to it.

## Bugzilla — the worked example

[Bugzilla](https://www.bugzilla.org/) is the bug tracker that birthed the open-source bug-tracker genre (1998). It's still actively used by Mozilla, Red Hat, Apache, and a long tail of older projects. Its data model:

| Bugzilla concept | Closest Aveloxis concept | Notes |
|---|---|---|
| Product | Repo (or RepoGroup) | A Bugzilla product = a logical project. Like a GitHub repo but no git. |
| Component | Sub-repo (no clean fit) | A product's sub-area. Aveloxis has no native concept. |
| Bug | Issue | Maps cleanly. Bug status (NEW/ASSIGNED/RESOLVED) → issue state. |
| Comment | Message | Maps cleanly. |
| Bug history event | IssueEvent | Bugzilla logs every field change; map to `issue_events`. |
| User | Contributor | Bugzilla user_id → contributors. Email primary. |
| Keyword | IssueLabel | Bugzilla calls them keywords; map to labels. |
| Assignee | IssueAssignee | Singular in Bugzilla (one assignee per bug); map to a one-element list. |
| Attachment | (no fit) | Skipped; Aveloxis doesn't store attachments. |
| Tracking flag | (no fit) | Skipped. |
| Milestone | Release (loose fit) | Could map; the cleaner choice is "skip releases for Bugzilla." |

**What Bugzilla does NOT have:** git repositories, pull requests, file changes, commits, branches, releases (in the GitHub sense), SBOMs, vulnerabilities (dependency-derived), scancode results, OpenSSF Scorecard.

This means a Bugzilla collector implements roughly 40% of `platform.Client` meaningfully and returns empty results for the rest. That's fine — the scheduler must gate the dependent phases.

## API choice: REST vs XML-RPC

Bugzilla exposes two APIs:

- **XML-RPC** (`/xmlrpc.cgi`) — supported by every Bugzilla 3.4+. The old standard.
- **REST** (`/rest/bug`, `/rest/user`, etc.) — supported by Bugzilla 5.0+ (2015). JSON, modern.

Go has solid `encoding/xml` but no clean XML-RPC client; you'd want the REST endpoint. The walkthrough below assumes Bugzilla 5.0+. For older Bugzilla instances, you'd need to add XML-RPC encoding/decoding — out of scope here.

Auth: API keys (Bugzilla 5.0+ supports them) or username+password. The Bugzilla key goes in the `X-BUGZILLA-API-KEY` header.

## The end-to-end inventory: what you'll touch

This is the complete list of files a new platform needs. Some are mandatory; some are optional depending on what the platform supports.

### Mandatory

1. **`internal/model/repo.go`** — add `PlatformBugzilla Platform = 4` to the enum + update the `String()` switch + decide on capability predicates (`IsGitOnly` etc.).
2. **`internal/db/schema.sql`** — add `(4, 'Bugzilla')` to the `aveloxis_data.platforms` seed INSERT.
3. **`internal/platform/bugzilla/`** — new package implementing `platform.Client`. At minimum:
   - `client.go` — the `Client` struct, constructor, all interface methods (most return empty iterators).
   - `repos.go` — `FetchRepoInfo` and `FetchCloneStats` (stubbed; Bugzilla products don't have clone stats).
   - `issues.go` — `ListIssues`, `ListIssueLabels`, `ListIssueAssignees`, `FetchIssueByNumber`, plus the unified `ListIssuesAndPRs`.
   - `messages.go` — comment-related methods.
   - `events.go` — `ListIssueEvents` from bug history.
   - `users.go` — `ListContributors`, `EnrichContributor`, `SearchUserByEmail`.
   - `urlparse.go` — `ParseRepoURL` (parses a Bugzilla product URL).
   - `errors.go` — mapping Bugzilla error codes to `platform.Class*` classifications.
4. **`internal/scheduler/scheduler.go`** — add `case model.PlatformBugzilla:` to the `selectClient` switch (~line 899); add the relevant capability gate around facade/analysis/SBOM phases (~line 756).
5. **`cmd/aveloxis/main.go`** — load Bugzilla API keys into a `bugzillaKeys` `*platform.KeyPool`, construct the `bugzilla.Client`, pass it to the scheduler config.
6. **`internal/db/keys.go`** — already supports arbitrary platform strings via `loadKeysFromTable(table, platform)`. Pass `"bugzilla"` as the platform string. No code change needed unless you want explicit constants.
7. **`internal/config/config.go`** — add a `BugzillaConfig` block to `Config` (base URL, optionally a list of self-hosted Bugzilla hosts).
8. **`internal/db/version.go`** — bump.
9. **`CLAUDE.md`** — `### Changes in vX.Y.0` section documenting the new platform.

### Likely

10. **`internal/web/server.go`** — bulk-paste-URL parsing. If you want operators to be able to paste a Bugzilla product URL into the web GUI and have it routed correctly, the URL parser dispatch needs to recognize Bugzilla URLs.
11. **`cmd/aveloxis/main.go`** — `add-repo` command needs URL parsing that recognizes Bugzilla.
12. **`internal/collector/prelim.go`** — Bugzilla products don't 301-redirect on rename; you may need to add a Bugzilla-specific liveness check OR accept that prelim is a no-op for Bugzilla. The latter is fine.

### Optional

13. **OAuth login for the web GUI.** If you want operators to be able to sign in via Bugzilla (most Bugzilla instances don't expose OAuth, so this is rarely useful — but if your target instance does, follow the GitHub/GitLab pattern in `handleGitHubLogin` / `handleGitHubCallback`).
14. **Source-contract tests** for each piece.
15. **Integration tests** that exercise a mocked Bugzilla via `httptest.NewServer`.

That's 9 mandatory files, 3 likely, 3 optional. Allocate ~2–3 days for the mandatory work if you've never done it before, ~1 day if you have.

## The capability design call

Aveloxis's existing capability predicate is:

```go
// internal/model/repo.go
func (p Platform) IsGitOnly() bool {
    return p == PlatformGenericGit
}
```

The scheduler uses it to gate API collection:

```go
// internal/scheduler/scheduler.go (~line 756)
if !repo.Platform.IsGitOnly() {
    // staged collection (API)
    client, err := s.selectClient(repo.Platform)
    // ...
}
```

And the facade phase runs unconditionally (every platform gets facade), then the analysis phase runs on every platform with a git clone.

For Bugzilla, you need the inverse gate: "this platform has API data, but no git side." The cleanest extension is a parallel predicate:

```go
// internal/model/repo.go
// HasGit returns true if this platform has a git repository to clone and walk.
// False for API-only platforms like Bugzilla, Jira, Phabricator.
func (p Platform) HasGit() bool {
    switch p {
    case PlatformGitHub, PlatformGitLab, PlatformGenericGit:
        return true
    case PlatformBugzilla:
        return false
    default:
        return false  // unknown platforms are safest treated as API-only
    }
}
```

Then the scheduler gates facade + analysis + SBOM + scancode + scorecard + vulnerability scanning behind `if repo.Platform.HasGit()`. Add this gate AROUND each phase's call site. About 6 sites in `scheduler.go` and `collector/collector.go`.

The existing `IsGitOnly` stays; it's specifically "this platform ONLY has git, no API." Bugzilla is the mirror: API only, no git.

If you anticipate more platforms with mixed capabilities (a platform that has issues but no PRs, say), consider extending `Platform` to a `Capabilities()` method returning a struct. For just Bugzilla, the parallel-predicate approach is fine — don't refactor for a hypothetical future.

## Step-by-step walkthrough

### Step 1 — declare the platform

```go
// internal/model/repo.go
const (
    PlatformGitHub     Platform = 1
    PlatformGitLab     Platform = 2
    PlatformGenericGit Platform = 3
    PlatformBugzilla   Platform = 4   // new
)

func (p Platform) String() string {
    switch p {
    case PlatformGitHub:
        return "GitHub"
    case PlatformGitLab:
        return "GitLab"
    case PlatformGenericGit:
        return "Git"
    case PlatformBugzilla:
        return "Bugzilla"
    default:
        return "Unknown"
    }
}

// HasGit returns true if this platform has a git repository.
// False for API-only platforms (Bugzilla, future Jira, etc.).
func (p Platform) HasGit() bool {
    switch p {
    case PlatformGitHub, PlatformGitLab, PlatformGenericGit:
        return true
    default:
        return false
    }
}
```

### Step 2 — seed the database

```sql
-- internal/db/schema.sql
INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
VALUES (1, 'GitHub'), (2, 'GitLab'), (3, 'Git'), (4, 'Bugzilla')
ON CONFLICT DO NOTHING;
```

The migration code applies this on every `aveloxis migrate` because of `ON CONFLICT DO NOTHING`. Operators with existing databases get the row inserted on their next migrate.

If you want to be belt-and-suspenders explicit, add an `execMigrationStep`:

```go
// internal/db/migrate.go — inside RunMigrations
execMigrationStep(ctx, pg, logger, &errs,
    "v0.24.0 seed Bugzilla platform row",
    `INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
     VALUES (4, 'Bugzilla') ON CONFLICT DO NOTHING`)
```

Idempotent via `ON CONFLICT DO NOTHING`.

### Step 3 — package skeleton

```
internal/platform/bugzilla/
├── client.go           # Client struct + constructor + Platform() + interface satisfaction
├── repos.go            # FetchRepoInfo (Bugzilla product metadata)
├── issues.go           # ListIssues, ListIssueLabels, ListIssueAssignees, FetchIssueByNumber, ListIssuesAndPRs
├── messages.go         # ListIssueComments + the per-issue variants
├── events.go           # ListIssueEvents from Bugzilla bug history
├── prs_noop.go         # No-op implementations of every PR-related method
├── releases_noop.go    # No-op ListReleases
├── users.go            # ListContributors, EnrichContributor, SearchUserByEmail
├── urlparse.go         # ParseRepoURL("https://bugzilla.example.com/show_bug.cgi?id=X" or product URL)
├── errors.go           # ClassifyError mapping for Bugzilla-specific responses
├── client_test.go      # Source-contract tests pinning interface satisfaction
└── issues_test.go      # httptest-driven behavioral tests
```

### Step 4 — Client struct + interface satisfaction

```go
// internal/platform/bugzilla/client.go
// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package bugzilla implements platform.Client for Bugzilla bug tracker
// instances. Targets Bugzilla 5.0+ REST API.
//
// Capabilities supported:
//   - Bugs (-> issues), bug history (-> issue events), comments (-> messages),
//     users (-> contributors), keywords (-> issue labels), assignees.
//
// Capabilities NOT supported (return empty / nil):
//   - Pull requests, releases, file changes, contributor identity from git,
//     OAuth login. Bugzilla has none of these.
//
// The scheduler skips facade, analysis, scancode, SBOM, vulnerability
// scanning, and scorecard for repos with PlatformBugzilla, gated by
// model.Platform.HasGit() == false.
package bugzilla

import (
    "context"
    "iter"
    "log/slog"
    "time"

    "github.com/aveloxis/aveloxis/internal/model"
    "github.com/aveloxis/aveloxis/internal/platform"
)

// Client implements platform.Client for Bugzilla.
type Client struct {
    baseURL string                // e.g. "https://bugzilla.mozilla.org"
    keys    *platform.KeyPool     // API keys (Bugzilla 5.0+); use nil for unauthenticated read-only access
    http    *platform.HTTPClient
    logger  *slog.Logger
}

// New constructs a Bugzilla client. baseURL is the Bugzilla instance root
// (no trailing slash). If keys is nil, requests are unauthenticated —
// works for public bugs on public instances, rate-limited harshly.
func New(baseURL string, keys *platform.KeyPool, logger *slog.Logger) *Client {
    return &Client{
        baseURL: baseURL,
        keys:    keys,
        http:    platform.NewHTTPClient(keys, logger, platform.AuthBugzilla),
        logger:  logger,
    }
}

func (c *Client) Platform() model.Platform {
    return model.PlatformBugzilla
}

// OnPermanentRedirect is a no-op for Bugzilla — Bugzilla products don't
// 301-redirect on rename; admins update DNS at the host level if needed.
// The scheduler installs the hook unconditionally on every platform.Client;
// we accept the call and ignore.
func (c *Client) OnPermanentRedirect(_ func(from, to string)) {}

// Compile-time assertion that *Client satisfies platform.Client.
var _ platform.Client = (*Client)(nil)
```

### Step 5 — auth style

The existing `platform.AuthStyle` enum is GitHub-only and GitLab-only. Add a Bugzilla variant:

```go
// internal/platform/auth.go (or wherever AuthStyle lives)
type AuthStyle int
const (
    AuthGitHub AuthStyle = iota
    AuthGitLab
    AuthBugzilla   // sends X-BUGZILLA-API-KEY: <key>
)
```

And extend the HTTPClient's request preparation:

```go
// internal/platform/httpclient.go — wherever the auth header is set
switch c.authStyle {
case AuthGitHub:
    req.Header.Set("Authorization", "token "+key)
case AuthGitLab:
    req.Header.Set("PRIVATE-TOKEN", key)
case AuthBugzilla:
    req.Header.Set("X-BUGZILLA-API-KEY", key)
}
```

### Step 6 — URL parsing

```go
// internal/platform/bugzilla/urlparse.go
import "fmt"
import "net/url"
import "strings"

// ParseRepoURL converts a Bugzilla product URL into ("", productName, nil).
// Bugzilla has no owner concept at the product level — the instance is
// the owner.
//
//   "https://bugzilla.mozilla.org/buglist.cgi?product=Firefox" -> ("", "Firefox")
//   "https://bugzilla.mozilla.org/describecomponents.cgi?product=Toolkit" -> ("", "Toolkit")
func (c *Client) ParseRepoURL(rawURL string) (owner, repo string, err error) {
    u, err := url.Parse(rawURL)
    if err != nil {
        return "", "", fmt.Errorf("parsing URL: %w", err)
    }
    q := u.Query()
    product := q.Get("product")
    if product == "" {
        return "", "", fmt.Errorf("URL has no product parameter: %s", rawURL)
    }
    // owner is empty for Bugzilla; we use just the product name.
    return "", strings.TrimSpace(product), nil
}
```

The scheduler dispatches on URL → platform; you'll need to wire that in `internal/web/server.go`'s URL-router and `cmd/aveloxis/main.go`'s `add-repo` command. Pattern:

```go
// internal/web/server.go (and add-repo) — URL dispatch
func detectPlatform(url string) model.Platform {
    switch {
    case strings.Contains(url, "github.com"):
        return model.PlatformGitHub
    case strings.Contains(url, "gitlab.com"):
        return model.PlatformGitLab
    case strings.Contains(url, "bugzilla"):  // very loose; tighten for production
        return model.PlatformBugzilla
    default:
        return model.PlatformGenericGit
    }
}
```

In practice, operators will configure a list of Bugzilla host patterns in `aveloxis.json` (similar to `gitlab_hosts`) so the dispatch is deterministic.

### Step 7 — implement the meaningful methods

The methods that DO apply to Bugzilla:

#### `FetchRepoInfo` — Bugzilla product metadata

```go
// internal/platform/bugzilla/repos.go
func (c *Client) FetchRepoInfo(ctx context.Context, _, product string) (*model.RepoInfo, error) {
    // GET /rest/product?names=<product>&include_fields=name,description,is_active,bug_count,...
    var resp struct {
        Products []struct {
            Name        string `json:"name"`
            Description string `json:"description"`
            IsActive    bool   `json:"is_active"`
            // Bug count needs a separate /rest/bug?product=X&count_only=true call.
        } `json:"products"`
    }
    url := fmt.Sprintf("%s/rest/product?names=%s&include_fields=name,description,is_active",
        c.baseURL, url.QueryEscape(product))
    if err := c.http.GetJSON(ctx, url, &resp); err != nil {
        return nil, fmt.Errorf("fetch product: %w", err)
    }
    if len(resp.Products) == 0 {
        return nil, platform.ErrNotFound
    }
    p := resp.Products[0]

    // Fetch open bug count via a separate /rest/bug count query.
    bugCount, _ := c.fetchOpenBugCount(ctx, product)

    return &model.RepoInfo{
        Name:        p.Name,
        Description: p.Description,
        Archived:    !p.IsActive,
        IssuesCount: bugCount,
        // Bugzilla doesn't expose stars/watchers/forks/contributors-count.
        // Leave at zero; the README documents the gap.
    }, nil
}
```

#### `ListIssues` — bugs as issues

```go
// internal/platform/bugzilla/issues.go
func (c *Client) ListIssues(ctx context.Context, _, product string, since time.Time) iter.Seq2[model.Issue, error] {
    return func(yield func(model.Issue, error) bool) {
        // GET /rest/bug?product=<product>&last_change_time=<since>&include_fields=...
        offset := 0
        for {
            params := url.Values{}
            params.Set("product", product)
            params.Set("limit", "500")
            params.Set("offset", strconv.Itoa(offset))
            if !since.IsZero() {
                params.Set("last_change_time", since.Format(time.RFC3339))
            }
            params.Set("include_fields", "id,summary,status,resolution,assigned_to,reporter,creation_time,last_change_time,keywords")

            var resp struct {
                Bugs []bugzillaBug `json:"bugs"`
            }
            apiURL := fmt.Sprintf("%s/rest/bug?%s", c.baseURL, params.Encode())
            if err := c.http.GetJSON(ctx, apiURL, &resp); err != nil {
                yield(model.Issue{}, fmt.Errorf("list bugs at offset %d: %w", offset, err))
                return
            }
            if len(resp.Bugs) == 0 {
                return  // pagination complete
            }
            for _, b := range resp.Bugs {
                if !yield(b.toIssue(product), nil) {
                    return
                }
            }
            if len(resp.Bugs) < 500 {
                return
            }
            offset += 500
        }
    }
}

type bugzillaBug struct {
    ID               int       `json:"id"`
    Summary          string    `json:"summary"`
    Status           string    `json:"status"`
    Resolution       string    `json:"resolution"`
    AssignedTo       string    `json:"assigned_to"`
    Reporter         string    `json:"reporter"`
    CreationTime     time.Time `json:"creation_time"`
    LastChangeTime   time.Time `json:"last_change_time"`
    Keywords         []string  `json:"keywords"`
}

func (b bugzillaBug) toIssue(product string) model.Issue {
    state := "open"
    if b.Status == "RESOLVED" || b.Status == "VERIFIED" || b.Status == "CLOSED" {
        state = "closed"
    }
    return model.Issue{
        Number:     b.ID,
        Title:      b.Summary,
        State:      state,
        ReporterLogin: b.Reporter,
        // ... rest of the mapping
        CreatedAt:  b.CreationTime,
        UpdatedAt:  b.LastChangeTime,
    }
}
```

Patterns to follow:

- Streaming via `iter.Seq2[T, error]`. Yield one item at a time so the staged collector can flush in batches without buffering the whole result set.
- Pagination via `offset` + fixed `limit`. Bugzilla's REST API supports it natively. Stop when the response is shorter than the limit.
- `since` filter via `last_change_time`. Pass `time.Time` formatted as RFC3339.
- Map Bugzilla statuses to Aveloxis's `open`/`closed` state. `RESOLVED`, `VERIFIED`, `CLOSED` are closed; everything else is open.
- Error wrapping with context (offset number) so failures are actionable.

#### `ListIssuesAndPRs` — the unified phase-2 enumerator

```go
func (c *Client) ListIssuesAndPRs(ctx context.Context, owner, product string, since time.Time) (*platform.IssueAndPRBatch, error) {
    batch := &platform.IssueAndPRBatch{
        Issues:         []model.Issue{},
        PullRequests:   []model.PullRequest{},  // always empty for Bugzilla
        IssueComments:  []platform.MessageWithRef{},  // populate if you choose inline-comment mode
        IssueLabels:    nil,  // skip the per-issue label fetch optimization
        IssueAssignees: nil,
    }
    for issue, err := range c.ListIssues(ctx, owner, product, since) {
        if err != nil {
            return batch, err  // return partial + error so the staged collector can stage what we got
        }
        batch.Issues = append(batch.Issues, issue)
    }
    return batch, nil
}
```

Returning the batch with an error lets the staged collector stage partial results (the v0.20.9 pattern — see [`docs/contributing/adding-a-collection-phase.md`](adding-a-collection-phase.md)).

#### `ListIssueComments` — Bugzilla bug comments

Bugzilla's REST API for comments: `GET /rest/bug/comment?ids=<bug_id>&new_since=<since>`. There's no fleet-wide "all comments since X" endpoint — you have to enumerate bugs first.

The pattern: implement `ListCommentsForIssue` (per-issue), and have `ListIssueComments` (fleet-wide) iterate over `ListIssues` and yield comments per bug. Less efficient than GitHub's repo-wide `/issues/comments` endpoint, but Bugzilla has no equivalent.

#### `ListContributors` — Bugzilla users

```go
func (c *Client) ListContributors(ctx context.Context, _, product string) iter.Seq2[model.Contributor, error] {
    return func(yield func(model.Contributor, error) bool) {
        // Bugzilla has no "list users who touched this product" endpoint.
        // The pragmatic approach: enumerate the most recent N bugs, harvest
        // unique reporter + assigned_to fields, then call /rest/user?names=
        // for each to fetch profile data.
        seen := make(map[string]bool)
        for issue, err := range c.ListIssues(ctx, "", product, time.Time{}) {
            if err != nil {
                yield(model.Contributor{}, err)
                return
            }
            for _, login := range []string{issue.ReporterLogin, issue.AssigneeLogin} {
                if login == "" || seen[login] {
                    continue
                }
                seen[login] = true
                contrib, enrichErr := c.EnrichContributor(ctx, login)
                if enrichErr != nil {
                    continue  // skip; not fatal
                }
                if !yield(*contrib, nil) {
                    return
                }
            }
        }
    }
}

func (c *Client) EnrichContributor(ctx context.Context, login string) (*model.Contributor, error) {
    var resp struct {
        Users []struct {
            ID       int64  `json:"id"`
            Name     string `json:"name"`
            Email    string `json:"email"`
            RealName string `json:"real_name"`
        } `json:"users"`
    }
    apiURL := fmt.Sprintf("%s/rest/user?names=%s", c.baseURL, url.QueryEscape(login))
    if err := c.http.GetJSON(ctx, apiURL, &resp); err != nil {
        return nil, fmt.Errorf("enrich user: %w", err)
    }
    if len(resp.Users) == 0 {
        return nil, platform.ErrNotFound
    }
    u := resp.Users[0]
    return &model.Contributor{
        Login:    u.Name,
        Email:    u.Email,
        FullName: u.RealName,
        Identities: []model.ContributorIdentity{{
            Platform: model.PlatformBugzilla,
            UserID:   u.ID,
            Login:    u.Name,
            Email:    u.Email,
        }},
    }, nil
}
```

The deterministic UUID: `PlatformUUID(int(model.PlatformBugzilla), u.ID)` works as-is because the helper accepts any platform ID byte. No code change needed.

#### `SearchUserByEmail` — Bugzilla user lookup

```go
func (c *Client) SearchUserByEmail(ctx context.Context, email string) (login string, userID int64, err error) {
    var resp struct {
        Users []struct {
            ID    int64  `json:"id"`
            Name  string `json:"name"`
            Email string `json:"email"`
        } `json:"users"`
    }
    apiURL := fmt.Sprintf("%s/rest/user?match=%s", c.baseURL, url.QueryEscape(email))
    if err := c.http.GetJSON(ctx, apiURL, &resp); err != nil {
        return "", 0, err
    }
    if len(resp.Users) == 0 {
        return "", 0, nil  // not found is not an error — the contract is ("", 0, nil)
    }
    // Find the user whose email matches exactly (Bugzilla's match= is a substring match).
    for _, u := range resp.Users {
        if strings.EqualFold(u.Email, email) {
            return u.Name, u.ID, nil
        }
    }
    return "", 0, nil
}
```

### Step 8 — implement the no-op methods

Every PR / release / file / review method returns an empty iterator:

```go
// internal/platform/bugzilla/prs_noop.go
func (c *Client) ListPullRequests(_ context.Context, _, _ string, _ time.Time) iter.Seq2[model.PullRequest, error] {
    return func(yield func(model.PullRequest, error) bool) { /* empty */ }
}

func (c *Client) ListPRLabels(_ context.Context, _, _ string, _ int) iter.Seq2[model.PullRequestLabel, error] {
    return func(yield func(model.PullRequestLabel, error) bool) { /* empty */ }
}

// ... all other PR methods follow the same pattern: empty iter.Seq2 ...

func (c *Client) FetchPRMeta(_ context.Context, _, _ string, _ int) (head, base *model.PullRequestMeta, err error) {
    return nil, nil, platform.ErrNotFound  // Bugzilla has no PRs at all
}

func (c *Client) FetchPRRepos(_ context.Context, _, _ string, _ int) (headRepo, baseRepo *model.PullRequestRepo, err error) {
    return nil, nil, platform.ErrNotFound
}

func (c *Client) FetchPRByNumber(_ context.Context, _, _ string, _ int) (*model.PullRequest, error) {
    return nil, platform.ErrNotFound
}

func (c *Client) FetchPRBatch(_ context.Context, _, _ string, _ []int) ([]platform.StagedPR, error) {
    return []platform.StagedPR{}, nil  // empty success — staged collector will see no PRs to stage
}
```

```go
// internal/platform/bugzilla/releases_noop.go
func (c *Client) ListReleases(_ context.Context, _, _ string) iter.Seq2[model.Release, error] {
    return func(yield func(model.Release, error) bool) { /* empty */ }
}

func (c *Client) FetchCloneStats(_ context.Context, _, _ string) ([]model.RepoClone, error) {
    return nil, platform.ErrNotFound  // Bugzilla has no clone stats
}
```

### Step 9 — wire into the scheduler

```go
// internal/scheduler/scheduler.go around line 899
func (s *Scheduler) selectClient(p model.Platform) (platform.Client, error) {
    switch p {
    case model.PlatformGitHub:
        return s.ghClient, nil
    case model.PlatformGitLab:
        return s.glClient, nil
    case model.PlatformBugzilla:
        return s.bzClient, nil
    default:
        return nil, fmt.Errorf("unknown platform: %d", p)
    }
}
```

Add a `bzClient platform.Client` field to the `Scheduler` struct. Update the constructor `NewWithKeys` to accept it.

Around line 756, the gate that controls "API collection vs git-only":

```go
if !repo.Platform.IsGitOnly() {
    client, clientErr := s.selectClient(repo.Platform)
    // ... existing code does staged collection
}
```

This already works for Bugzilla — `IsGitOnly()` returns false for Bugzilla, so the API collection branch runs.

The NEW gate is around facade / analysis / SBOM / scancode / scorecard. Find each call site and wrap:

```go
if repo.Platform.HasGit() {
    // facade phase
    facadeResult, facadeErr := s.runFacadeAndAnalysis(ctx, ...)
    // ...
}
```

Audit `internal/scheduler/scheduler.go` for `runFacadeAndAnalysis`, `analysisCollector.AnalyzeRepo`, scorecard invocation, SBOM generation, vulnerability scanning, scancode worker pool entries. Wrap each in the `HasGit()` gate.

### Step 10 — wire into `main.go`

```go
// cmd/aveloxis/main.go — runServe and the keys loader

func loadKeys(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys bool, logger *slog.Logger) (
    ghKeys, glKeys, bzKeys *platform.KeyPool, err error,
) {
    // ... existing ghKeys + glKeys loading

    bzKeysData, err := store.LoadAPIKeys(ctx, "bugzilla", useAugurKeys)
    if err != nil {
        logger.Warn("loading Bugzilla keys", "error", err)
    }
    bzKeys = platform.NewKeyPool(bzKeysData, ...)

    return ghKeys, glKeys, bzKeys, nil
}
```

Then:

```go
ghKeys, glKeys, bzKeys, err := loadKeys(ctx, cfg, store, useAugurKeys, logger)
ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)
bzClient := bugzilla.New(cfg.Bugzilla.BaseURL, bzKeys, logger)

sched := scheduler.NewWithKeys(store, ghClient, glClient, bzClient, ghKeys, logger, scheduler.Config{...})
```

Update `scheduler.NewWithKeys` to accept the bzClient param.

### Step 11 — config

```go
// internal/config/config.go
type Config struct {
    Database   DatabaseConfig    `json:"database"`
    GitHub     GitHubConfig      `json:"github"`
    GitLab     GitLabConfig      `json:"gitlab"`
    Bugzilla   BugzillaConfig    `json:"bugzilla"`   // new
    // ...
}

type BugzillaConfig struct {
    BaseURL       string   `json:"base_url"`        // e.g. "https://bugzilla.mozilla.org"
    APIKeys       []string `json:"api_keys,omitempty"`
    BugzillaHosts []string `json:"bugzilla_hosts,omitempty"`  // for multi-instance dispatch
}
```

`aveloxis.example.json` gains a `bugzilla` block. The existing `TestExampleConfigIncludesEveryJSONField` tripwire will fire CI if you forget.

Document the field in `docs/getting-started/configuration.md`.

### Step 12 — tests

Source-contract:

```go
// internal/platform/bugzilla/client_test.go
func TestBugzillaClientSatisfiesPlatformInterface(t *testing.T) {
    var _ platform.Client = (*Client)(nil)  // compile-time check
}

func TestBugzillaClientPlatformReturnsBugzilla(t *testing.T) {
    c := New("https://example.invalid", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
    if c.Platform() != model.PlatformBugzilla {
        t.Errorf("expected PlatformBugzilla, got %v", c.Platform())
    }
}
```

Behavioral, via `httptest.NewServer`:

```go
func TestBugzillaListIssues(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if strings.HasPrefix(r.URL.Path, "/rest/bug") {
            w.Header().Set("Content-Type", "application/json")
            w.Write([]byte(`{
                "bugs": [
                    {"id": 12345, "summary": "Test bug", "status": "NEW", "reporter": "alice@example.invalid"},
                    {"id": 12346, "summary": "Resolved bug", "status": "RESOLVED", "reporter": "bob@example.invalid"}
                ]
            }`))
            return
        }
        http.NotFound(w, r)
    }))
    defer srv.Close()

    c := New(srv.URL, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
    var issues []model.Issue
    for issue, err := range c.ListIssues(context.Background(), "", "Firefox", time.Time{}) {
        if err != nil { t.Fatal(err) }
        issues = append(issues, issue)
    }
    if len(issues) != 2 {
        t.Errorf("expected 2 issues, got %d", len(issues))
    }
    if issues[0].State != "open" {
        t.Errorf("NEW bug should map to open state, got %q", issues[0].State)
    }
    if issues[1].State != "closed" {
        t.Errorf("RESOLVED bug should map to closed state, got %q", issues[1].State)
    }
}
```

### Step 13 — version bump, CLAUDE.md entry

```go
// internal/db/version.go
var ToolVersion = "0.24.0"  // minor bump for new platform
```

Add a `### Changes in v0.24.0 — Bugzilla platform support` section to `CLAUDE.md` documenting:

- What's collected (bugs, comments, users, events, keywords as labels).
- What's NOT collected (PRs, releases, files, commits, SBOMs, vulnerabilities — Bugzilla has none of these).
- The capability-gate pattern (`HasGit()`) introduced for it.
- How operators configure it (`aveloxis.json` `bugzilla` block, `aveloxis add-key <token> --platform bugzilla`).
- Anything operators should know (rate limits, auth requirements, parity gaps).

## What you'll discover during implementation

Some things the walkthrough doesn't cover that you'll bump into:

1. **`SetMatviewSkip` interaction.** The dm_repo_* aggregates JOIN on commits, which Bugzilla doesn't have. Those views will still build but return zero rows for Bugzilla repos. Fine — they just don't apply.

2. **`repo_info` columns** that don't apply to Bugzilla (star_count, fork_count, watcher_count, clone_count, default_branch). Leave them at zero / empty. Don't fabricate values. Document the parity gap in the architecture docs (mirroring how `docs/architecture/contributor-resolution.md` documents the GitHub/GitLab gaps).

3. **`contributor_identities` rows** for Bugzilla users have an `email` field but no `name` (Bugzilla returns `real_name`), no `avatar_url`, no GraphQL `node_id`, etc. Leave those empty. The `contributors_aliases` mechanism for commit-email resolution doesn't apply (no commits).

4. **`monitor` dashboard** filters by platform. The existing UI doesn't know what to render for Bugzilla. You'll want to update `internal/monitor/monitor.go` and the template to handle the case (or display Bugzilla repos in the same table with the "Commits" column empty).

5. **Web GUI repo detail page.** The Chart.js panels for "Weekly commits" / "Weekly PRs" don't apply. Decide: hide them for Bugzilla repos, or show "no data."

6. **Gap fill and open-item refresh.** These exist for issues + PRs. For Bugzilla, PR refresh is a no-op (no PRs). Issue gap fill should work if `ListIssues` accepts a `since=zero` to fetch everything. Verify.

## Don'ts

- **Don't fabricate fields.** If Bugzilla doesn't have stars, leave `star_count` at zero. Don't substitute bug_count or anything else.
- **Don't extend `platform.Client` for Bugzilla-specific concepts** (attachments, tracking flags). Either map to existing concepts or leave un-collected. Adding interface methods that only one platform implements ramps up complexity for everyone.
- **Don't add a `case PlatformBugzilla` to every existing platform switch** if a default works. Several call sites use `selectClient(repo.Platform)` which already routes by platform; you don't need parallel switches everywhere.
- **Don't skip tests.** The walkthrough above has source-contract + behavioral test examples for a reason. Without tests, refactors will break Bugzilla silently because the test suite won't exercise the non-default code paths.

## A reasonable order of operations

1. Read this chapter end-to-end. Skim the existing `internal/platform/github/` and `internal/platform/gitlab/` packages.
2. Add the enum entry + DB seed (steps 1–2). 1 hour. Commit.
3. Scaffold the package with no-op methods + interface satisfaction tests (steps 3–4, 7's no-ops). 2–4 hours. Tests should compile + pass.
4. Implement `ListIssues` + `FetchRepoInfo` + URL parsing (steps 6–7). 1 day. Add behavioral tests via httptest.
5. Implement contributors + comments + events (step 7). 1 day.
6. Wire into scheduler + main.go + config (steps 9–11). Half day.
7. End-to-end test against a real (or local-Docker'd) Bugzilla. 1 day.
8. Write CLAUDE.md entry + bump version + open PR.

Total: ~1 week for someone familiar with Go and Aveloxis. Longer if you're new to either.

## Final note

The existing platform layer is the second iteration of this design — the first attempt (early 2024) tried to share too much between GitHub and GitLab and resulted in a leaky abstraction. The current shape works because each platform owns its package with its own types, sharing only the model and the platform.Client interface. Resist the urge to refactor toward "shared collectors" until you have THREE platforms in production — then you'll know what's actually shareable.

For Bugzilla specifically, the package will look mostly distinct from github/ and gitlab/, which is fine. Distinct packages stay readable; over-shared code becomes a maintenance burden when one platform's API changes.

Good luck. File issues if any of this is unclear.
