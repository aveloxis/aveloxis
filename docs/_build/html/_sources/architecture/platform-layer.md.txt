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
  |     |-- ListRepoEvents
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
- **Conditional requests (ETags)**: for paginated listings (`Get` via `paginate`), the client caches ETags and sends `If-None-Match` on subsequent requests; a 304 means "nothing new since last time" and ends pagination cleanly, and GitHub does not count 304s against the rate limit. Single-object reads (`GetJSON` — one PR, one issue, one user, one project) are **always ETag-free** (v0.28.17): a body-decoding reader cannot use a 304, and before the change a repeat read of the same URL in one process either errored (`not modified (304)`) or silently returned empty children — the GitLab MR batch and every REST child waterfall were affected. A job that fetched listing pages and then failed forgets its repo's cached ETags (`ForgetRepoETags`, v0.28.18) so the retry re-reads every page; the paginator rebases GitHub's `/repositories/{id}/…` Link-header continuations onto the listing's `/repos/{owner}/{repo}/` namespace so page 2 onward is forgotten with page 1.
- **Bad credential detection**: 401 responses permanently invalidate the API key.
- **Explicit redirect handling (v0.16.10+)**: Go's default redirect follower is disabled (`CheckRedirect: http.ErrUseLastResponse`). The switch handles 301, 302, 307, 308 directly by reading the `Location` header and re-issuing against the new URL, capped at `maxRedirectHops = 5` per call. Each hop logs `following redirect from=... to=... status=... hop=N`. Centralizing the logic means there is only one place to reason about auth-header preservation, hop caps, and cross-host edge cases. Since v0.29.12 a request that carries a pool key never leaves the client base URL's scheme and host (port included, no userinfo): `HTTPClient.Get` checks the final URL before every attempt — whatever built it — and `GraphQLAt` checks its explicit endpoint, both before a key is leased. A redirect is checked before it is followed (and before the permanent-redirect hook runs), and a pagination `Link` continuation is resolved against the request, checked, and made relative to the base URL's path (a GitLab `/api/v4` continuation is no longer doubled). Every refusal logs at ERROR and returns `ErrOffHostRefused` (classified `ClassSkip`). A refusal after page 1 of a listing — a refused continuation, or a redirect refused on a later page — is wrapped in `ErrListingTruncated`, which `ClassifyError` checks first and classifies `ClassFatal`, so the endpoint fails and the window is re-listed rather than skipped with `last_collected` advancing. A URL that does not parse is not an off-host refusal: it fails at request construction as before, and an unparseable `Location` is `ErrGone`. A relative `Location` is resolved against the requested URL (RFC 3986), not appended to the base.
- **`ErrGone` sentinel (v0.16.10+)**: Distinct from `ErrNotFound`. Returned for (a) 410 Gone responses, (b) 3xx responses with an empty/missing `Location` header (observed when GitHub cannot determine the redirect target, body `{"url":""}`), and (c) redirect chains exceeding `maxRedirectHops`. Callers use `errors.Is(err, ErrGone)` to treat these as "skip this resource" without failing the job. The staged collector's `isOptionalEndpointSkip` delegates to `ClassifyError(err) == ClassSkip`, which covers `ErrNotFound`, `ErrForbidden`, `ErrGone`, `ErrOffHostRefused` and the other skip sentinels.
- **Per-item comment endpoints (v0.16.12+)**: `MessageCollector` has three per-item methods alongside the repo-wide since-filtered listings: `ListCommentsForIssue(owner, repo, issueNumber)`, `ListCommentsForPR(owner, repo, prNumber)`, `ListReviewCommentsForPR(owner, repo, prNumber)`. GitHub implementations target `/repos/{o}/{r}/issues/{n}/comments` (tagged as IssueRef or PRRef by the caller's context) and `/repos/{o}/{r}/pulls/{n}/comments`. GitLab implementations target `/projects/:id/issues/:iid/notes`, `/projects/:id/merge_requests/:iid/notes`, and `/projects/:id/merge_requests/:iid/discussions` (filtered to notes carrying a `position`). These power gap fill and open-item refresh, which need comments on historical or prior-cycle-missed items that would otherwise fall outside any repo-wide since window.

---

## Key pool (`KeyPool`)

Manages every API token for one platform, and hands them to every collector.

### Key pool contract

These rules are the contract; tests enforce each one.

1. **One shared budget.** Rate-limit budget is a single resource shared by
   every collector in a process: staged collection, commit resolution,
   enrichment, search resolution, breadth, org scans, distribution, and the
   tokens lent to scorecard subprocesses. All of them lease a platform's keys
   from that platform's one `KeyPool` (one for GitHub, one for GitLab)
   through `Acquire`, the only in-process path to a key; scorecard
   subprocesses borrow through `LendTokens`, which is accounted.
2. **A refusal benches the key for everyone.** When GitHub refuses a key
   (HTTP 403 or 429 with `X-RateLimit-Remaining: 0` and no `Retry-After`), the
   pool benches that key for that budget (`core`, `graphql` or `search`) until
   the refusal's `X-RateLimit-Reset`. If the reset is missing or already past,
   the bench lasts a five-minute probe window. No later response header can
   lift a bench early. The pool keeps no balance for `search` (30 requests a
   minute per user); a search refusal benches the key for search only.
3. **The collector goes back for another key.** On a refusal, the REST and
   GraphQL clients return to `Acquire` straight away and get a different key.
   The rotation does not use up one of the request's retries. Rotations are
   capped at the number of keys, so a request can pass through every key
   before refusals start using up its retries. The exception is a GraphQL
   caller that opts into fast-fail (`WithGraphQLFastFail`, used where batch
   subdivision is the retry): its rotations spend its short retry budget.
   When a REST refusal names a budget the pool did not bench for that request
   (a resource other than `core`, `graphql` or `search`, or a budget that does
   not match the request, such as a `search` refusal on a non-search path),
   there is no key to rotate away from, so the client waits for that refusal's
   reset instead of re-sending at once.
4. **Wait only when nothing has budget.** For a foreground caller, `Acquire`
   blocks for budget only when no key has any. It then wakes at the earliest
   bench end, rest end or window reset. When only the in-flight ceilings are
   full, it waits for a request slot to free. A fast-fail GraphQL caller gets
   `ErrGraphQLBudgetExhausted` instead of waiting, and a background GraphQL
   sweep also waits while the pool is at the foreground reserve line.
   Keys refused for `core` or `graphql`, quarantined or resting are not lent to
   scorecard either.

Other behaviour:

- **Selection**: among eligible keys, fewest in-flight requests first, then the most remaining budget.
- **Per-key and pool-wide in-flight ceilings** for the GitHub pool (`collection.github_max_inflight_per_key`, `collection.github_max_inflight`), and a foreground reserve that background GraphQL sweeps cannot spend into (`collection.github_budget_foreground_reserve_pct`). The GitLab pool has no per-key ceiling.
- **Configurable buffer**: stops using a key when `remaining` drops to `buffer` (default 15).
- **Resource-aware**: `core` and `graphql` balances are tracked separately; `search` is refusal-only (rule 2). Other resources are not tracked.
- **Secondary limits** (`Retry-After`): the key rests in the pool for the `Retry-After`, and the refused request waits it out before retrying. GitHub warns that continuing while secondary-limited can get an integration banned, and tokens owned by the same GitHub account share limits.
- **Not shared across processes**: `aveloxis web` org scans, one-shot CLI commands and scorecard subprocesses spend the same tokens outside `serve`'s pool. `serve` sees that spend in the next response it gets on each key, and at the latest in the first refusal (rule 2).
- **Observability**: the 5-minute `key pool summary` log line reports tracked balances plus `refused_core_now`, `refused_graphql_now`, `refused_search_now` and `refusals_lifetime` (one per refused response).

---

## GraphQL transport (the GitHub default)

The sub-interface tables above describe the REST methods — which remain first-class (GitLab composes them; they are also the `"rest"` escape hatch). But since v0.26.0 the DEFAULT GitHub transport is GraphQL: `ListIssuesAndPRs` enumerates issues + PRs in cursor-paginated GraphQL queries, and `FetchPRBatch` fetches 10–25 PRs with ALL their children (labels, assignees, reviewers, reviews, commits, files, comments) in one aliased query, with automatic batch subdivision on transient failures and a per-PR REST rescue at size 1. Both are `platform.Client` methods; GitLab implements them as REST composition.

### GraphQL errors that arrive with HTTP 200

GitHub reports several conditions in the response body of a 200, so no
status-code arm ever sees them. Each has its own class, because the recovery
differs:

| Body | Class | What the client does |
|---|---|---|
| `RATE_LIMITED` (any spelling) | rate limit | benches the key's GraphQL budget and rotates to another key |
| `RESOURCE_LIMITS_EXCEEDED` | transient, `ErrResourceLimits` | the caller halves the batch — the one condition where a smaller query provably helps |
| `Something went wrong while executing your query …` (no type) | transient, `ErrGraphQLExecutionTimeout` | retried with backoff like a 5xx; an exhausted budget leaves a transient error, so batching callers subdivide (v0.29.56) |
| `NOT_FOUND` / `FORBIDDEN` per path | skip | the other aliases' data is kept; the missing node is absent, never zero |

The execution timeout is GitHub's answer for a query its resolver could not
finish. It used to classify fatal, so nothing retried or subdivided it: on
2026-09-17 it failed all seven contributor-activity ticks and the sweep wrote
nothing for three days. It is intermittent — a probe re-ran the batch that had
failed every tick and all 100 queries succeeded — which is why a plain retry is
the first response.

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
- **GitHub events endpoint**: GitHub's `/repos/{owner}/{repo}/issues/events` returns events for both issues and PRs. `ListRepoEvents` walks it ONCE and yields a tagged union (`RepoEvent.Issue` or `RepoEvent.PR`). The pre-v0.26.3 design walked the endpoint twice (issues, then PRs) and the second pass got a 304 from the first pass's ETag on any quiet repo — silently dropping the entire PR-event history; the single pass makes that impossible.
- **GitLab metadata counts**: issue counts come from `/issues_statistics`; merge-request counts from the `X-Total` header of one-row `/merge_requests?state=…` probes. GitLab omits `X-Total` on any listing above 10,000 records, so above that the client counts through GitLab GraphQL (`mergeRequests(state:) { count }`, one query for all three states). A probe that fails outright marks the count UNKNOWN (never a fabricated 0) and the store carries the prior snapshot's counts forward; disabled features are a definitive zero and are not probed (v0.28.18).
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
