# Platform Abstraction Layer

The `internal/platform` package is the HTTP client, rate limiting, and API abstraction layer that enables Aveloxis to collect from GitHub and GitLab with equal completeness through a single interface.

---

## Interface hierarchy

```
platform.Client
  |-- Platform()           -> model.Platform
  |-- ParseRepoURL()       -> owner, repo
  |-- RepoCollector
  |     |-- FetchRepoInfo
  |     |-- FetchCloneStats
  |-- IssueCollector
  |     |-- ListIssues
  |     |-- ListIssueLabels
  |     |-- ListIssueAssignees
  |-- PullRequestCollector
  |     |-- ListPullRequests
  |     |-- ListPRLabels, ListPRAssignees, ListPRReviewers
  |     |-- ListPRReviews, ListPRCommits, ListPRFiles
  |     |-- FetchPRMeta
  |-- EventCollector
  |     |-- ListIssueEvents
  |     |-- ListPREvents
  |-- MessageCollector
  |     |-- ListIssueComments
  |     |-- ListPRComments
  |     |-- ListReviewComments
  |-- ReleaseCollector
  |     |-- ListReleases
  |-- ContributorCollector
        |-- ListContributors
        |-- EnrichContributor
```

All list methods return `iter.Seq2[T, error]` (Go 1.23 iterators) for memory-efficient streaming pagination. Callers consume results with `for item, err := range client.ListIssues(...)`.

---

## HTTP client (`HTTPClient`)

Shared by both GitHub and GitLab implementations. Features:

- **Platform-aware authentication**: `AuthStyle` parameter controls the auth header format. GitHub uses `Authorization: token <key>` (PATs). GitLab uses `PRIVATE-TOKEN: <key>`. Set at construction via `NewHTTPClient(..., AuthGitHub)` or `NewHTTPClient(..., AuthGitLab)`.
- **Connection pooling**: HTTP/2 enabled, 20 idle connections per host for high-throughput collection.
- **Automatic retries**: Up to 10 retries with exponential backoff for transient errors (502/503/504).
- **Rate limit awareness**: Reads `X-RateLimit-*` (GitHub) and `RateLimit-*` (GitLab) headers, waits for reset when exhausted.
- **Secondary rate limit handling**: Respects `Retry-After` headers from GitHub's secondary rate limits.
- **Conditional requests (ETags)**: Caches ETags from responses and sends `If-None-Match` on subsequent requests. GitHub does not count 304 responses against the rate limit, saving quota on unchanged data during incremental collection.
- **Bad credential detection**: 401 responses permanently invalidate the API key.
- **Explicit redirect handling (v0.16.10+)**: Go's default redirect follower is disabled (`CheckRedirect: http.ErrUseLastResponse`). The switch handles 301, 302, 307, 308 directly by reading the `Location` header and re-issuing against the new URL, capped at `maxRedirectHops = 5` per call. Each hop logs `following redirect from=... to=... status=... hop=N`. Centralizing the logic means there is only one place to reason about auth-header preservation, hop caps, and cross-host edge cases.
- **`ErrGone` sentinel (v0.16.10+)**: Distinct from `ErrNotFound`. Returned for (a) 410 Gone responses, (b) 3xx responses with an empty/missing `Location` header (observed when GitHub cannot determine the redirect target, body `{"url":""}`), and (c) redirect chains exceeding `maxRedirectHops`. Callers use `errors.Is(err, ErrGone)` to treat these as "skip this resource" without failing the job. The staged collector's `isOptionalEndpointSkip` checks `ErrNotFound | ErrForbidden | ErrGone` together.

---

## Key pool (`KeyPool`)

Manages multiple API tokens with round-robin rotation for maximum throughput.

- **Round-robin rotation**: Every key's rate limit is fully utilized before the pool waits.
- **Configurable buffer**: Stops using a key when `remaining` drops to `buffer` (default 15), preventing 403s from concurrent workers that checked out a key before the count was updated.
- **Automatic refill**: Keys are refilled to 5000 when the rate-limit window resets.
- **Resource-aware**: Only core API responses update the key's rate-limit counter. Search and GraphQL responses (which have separate buckets) are ignored to prevent premature key rotation.

---

## Pagination

Both GitHub and GitLab use 100-item pages. The pagination engine is shared, with platform-specific next-page resolution:

| Platform | Primary method | Fallback |
|---|---|---|
| GitHub | `Link` header `rel="next"` | -- |
| GitLab | `X-Next-Page` header | `Link` header `rel="next"` |

The pagination functions (`PaginateGitHub`, `PaginateGitLab`) are generic and work with any JSON-decodable type.

---

## URL parsing (`RepoURL`)

Parses repository URLs and identifies the platform:

- `https://github.com/owner/repo` -> GitHub, owner="owner", repo="repo"
- `https://gitlab.com/group/subgroup/project` -> GitLab, owner="group/subgroup", repo="project"
- Self-hosted instances detected by hostname hints or "gitlab" substring in hostname.

The `APIURL()` method returns the correct API base URL, including GitHub Enterprise (`/api/v3`) and GitLab (`/api/v4`).

---

## Adding a new platform

To add support for a new forge (e.g., Gitea):

1. Create `internal/platform/gitea/` with `types.go` (raw API types) and `client.go`.
2. Implement `platform.Client` -- all 7 sub-interfaces.
3. Add the platform to `model.Platform` constants.
4. Add URL detection in `repourl.go`'s `detectPlatform()`.
5. Wire into `cmd/aveloxis/main.go` client creation.

The `HTTPClient`, `KeyPool`, and pagination engine are reusable across all platforms.

---

## Design notes

- **GitLab API differences**: GitLab lacks bulk endpoints for notes (comments) and requires iterating parent entities. The GitLab client iterates issues/MRs and fetches their notes individually. This is slower but unavoidable given the API design.
- **GitHub events endpoint**: GitHub's `/repos/{owner}/{repo}/issues/events` returns events for both issues and PRs. The GitHub client fetches this once via a shared helper and filters by type for `ListIssueEvents` and `ListPREvents`.
- **GitLab review comments**: GitLab uses "discussions" with positioned notes instead of GitHub's explicit review comments. The `ListReviewComments` method maps positioned discussion notes to the `ReviewComment` model.

## GitHub vs GitLab data gaps

All `platform.Client` interface methods are implemented for both platforms. The following data discrepancies exist due to GitLab API limitations:

| Data | GitHub | GitLab | Impact |
|---|---|---|---|
| Community profile files | GraphQL file detection (CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY) | Not yet implemented (closable via `/repository/tree`) | `repo_info` community fields empty for GitLab |
| Watcher count | `watchers.totalCount` via GraphQL | No public API | `repo_info.watcher_count` is 0 for GitLab |
| Clone stats | `/traffic/clones` | Admin-only API | `repo_clones` table empty for GitLab |
| GraphQL node IDs | Available on all entities | Not applicable (uses numeric IDs) | `pr_src_node_id` empty for GitLab; `pr_src_repo_id` always populated |
| Contributor identity URLs | 10+ per-user URL fields (followers, gists, starred, etc.) | Not available | `gh_*_url` columns empty for GitLab contributors |
| Contributor type | `User`, `Bot`, `Organization` | Not distinguished | `cntrb_type` not populated for GitLab |
| Contributor breadth | `/users/{login}/events` endpoint | No equivalent | `contributor_repo` only populated for GitHub contributors |
