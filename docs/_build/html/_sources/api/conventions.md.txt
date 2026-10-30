# API design conventions

These are the rules the Aveloxis REST API is designed and documented by.
They are modeled on the
[Swift API Design Guidelines](https://www.swift.org/documentation/api-design-guidelines/),
which are about how a call reads where it is used, and they apply those
ideas to HTTP and JSON. The [reference](reference.rst) follows them, and
`scripts/openapi_routes_test.go` checks the ones a program can check (see
the end of this page).

The second half of the page is the contract every operation shares:
access, errors, rate limits, caching and time. The reference does not
repeat it.

## Fundamentals

- **Clarity at the point of use is the most important goal.** A call is
  written once and read many times, in scripts, notebooks and logs. Judge
  a design by reading a whole request in context:
  `GET /api/v1/repos/42/contributions/affiliations?since=2025-01-01`
  should say what it returns without a trip to this documentation.
- **Clarity is more important than brevity.** A path segment is not
  shorter for its own sake: `contributions/identities` rather than
  `contrib-ids`.
- **Every operation is documented.** Writing the entry is part of
  designing the operation. If its behavior cannot be described in a few
  sentences, the design is probably wrong.

## Naming

### Use words that remove ambiguity

- **Include every word needed to avoid ambiguity.** The roster of people
  who contributed in a window is `contributions/identities`; the
  Augur-compatible `contributors` route returns aggregated counts. The
  two names say which is which.
- **Omit needless words.** `/repos/42/stats`, not `/repos/42/repo-stats`:
  the path already says whose statistics they are.
- **Name parameters and fields for their role, not their type.**
  `since` and `until`, not `date1` and `date2`; `retention_threshold`,
  not `n`.
- **Booleans read as assertions about the thing they describe**:
  `scanned`, `starred`, `is_osi`, `is_admin`, `editable`.

### Use terminology well

- **Use a term of art only in its established meaning**: SBOM, purl,
  CycloneDX, SPDX, OSV, libyear, ETag. Do not coin a new word for an
  established idea, and do not use an established word for a new one.
- **Use one term per concept, everywhere**: in paths, field names, the
  reference and the guide.

  | Concept | Term | In paths and fields |
  |---|---|---|
  | A collected repository | repository | `repos`, `repo_id` |
  | A user's set of repositories and organizations | group | `groups`, `group_id` |
  | An administrator-curated set of groups | collection | `collections`, `collection_id` |
  | A GitHub organization or GitLab group | organization | `org` |
  | A person, across identities | contributor | `contributors`, `cntrb_id` |
  | A repository or organization being compared | entity | `entities` (`repo:42`, `org:github.com/chaoss`) |
  | What a caller may read | scope | `repo_out_of_scope` |
  | A signed-in web session's Bearer token | session token | `sessionToken` scheme |
  | An administrator-granted Bearer token | API token | `apiToken` scheme, `avx_…` |
  | A fair-use limit | quota | `quota`, `capacity_limit` |
  | The repositories an account may hold | allocation | `kind: allocation` |

- **Avoid abbreviations**, except where they are the established name:
  `repo` in paths and fields is the precedent of every existing client,
  and `cntrb_id` is the stored column clients already read.
- **Embrace precedent.** The Augur-compatible routes keep Augur's paths
  and parameter names (`begin_date`, `end_date`, `period`), because 8Knot
  and other Augur clients call them as they are. Routes that predate
  these rules keep their names: renaming a path or field breaks every
  client that uses it.

### Spelling

- Paths are lower case, with plural nouns for collections of resources
  (`/groups/{groupID}/repos`) and `-` between words.
- Path parameters are lowerCamelCase and end in `ID` for identifiers
  (`{repoID}`, `{groupID}`), the precedent of the existing routes.
- Query parameters and JSON fields are `snake_case`. Identifiers end in
  `_id`, instants in `_at` (`created_at`, `reset_at`). Two older answers
  use Go field names instead (the mailing-list statistics and the
  single-repository Augur lookup); the reference marks them.

### Name operations by what they do

Each operation has an `operationId`, the name a generated client uses for
it, shown in every reference entry.

- **A read (GET) is a noun phrase naming what it returns**:
  `repositoryStats`, `groupRepositories`, `apiTokens`. A GET has no side
  effects that matter to the caller, so a verb such as `get` or `list`
  adds nothing.
- **A change is an imperative verb phrase**: `createGroup`,
  `addRepositoriesToGroup`, `revokeAPIToken`.
- **Paired operations read as a pair**: `starRepository` (PUT) and
  `unstarRepository` (DELETE).
- **Acronyms are uniformly cased**: lower case at the start of a name
  (`apiTokens`, `sbomDocument`), upper case elsewhere
  (`repositorySBOM`, `grantAPIToken`).
- **Augur-compatible operations** are `augur` followed by Augur's metric
  name (`augurIssuesNew`).

## Documentation

- **Every operation has a summary of one sentence.** A read says what it
  returns ("Returns the weekly activity series of a repository."); a
  change says what it does, in the third person ("Revokes an API
  token.").
- **The description adds what a schema cannot say**: what edge values
  mean, the order of a list, defaults and caps, and what each status
  code means for this operation. It ends with the handler that serves it
  ("Handler: `handleRepoStats`."), so a reader can go from the reference
  to the source.
- **Document the cost of anything that is not cheap.** The Swift
  guidelines ask for the complexity of any property that is not O(1);
  here, an operation that is not a cached or indexed read says what it
  costs and how its input is bounded (at most 500 ids per call, at most
  seven entities, a `limit` capped at 100).
- **Document every failure.** Each response code an operation can return
  is in its entry. The ones many operations share are written once,
  under "Shared responses" at the end of the reference, and each entry
  links there.

## Access

Every operation states who may call it. There are five classes, written in
the spec as the operation's `security` requirements:

| Access | Who may call | `security` in the spec |
|---|---|---|
| Public | Anyone; no token (`/health`, `/public/stats`) | `[]` |
| Read | A session or an API token; anonymous callers too while `api.require_auth` is off, and from an exempt network (`api.exempt_cidrs`) | `sessionToken`, `apiToken`, or none |
| Signed in | A session or an API token, always | `sessionToken`, `apiToken` |
| Session | A signed-in session; an API token is refused (`403`, or `400` on sign-out) | `sessionToken` |
| Administrator | An administrator's signed-in session; an API token is refused even when an administrator owns it | `sessionToken` with role `admin` |

Both token kinds are sent as `Authorization: Bearer <token>`.

**Scope.** A repository-scoped operation (any `/repos/{repoID}/…` read)
answers only for repositories in the caller's groups. An administrator's
session is unscoped, and so is an anonymous caller where the access class
admits one (`api.require_auth` off, or an exempt network): scope belongs
to a token's account, and an anonymous call has none. A signed-in session that follows a link to a
repository outside its groups has the repository added to its "Shared
with Me" group, and the answer carries `X-Aveloxis-Added-To-Group` once.
The addition happens before the operation runs, so the header rides on
whatever that request answers, an error (`400`, `404`, `429`, `500`)
included;
an API token is refused with `403 repo_out_of_scope` and a body that says
how to add the repository. The [guide](../guide/api.md#authentication-getting-an-api-token)
has the details and the caps.

## Errors

A status code means the same thing on every operation:

| Code | Meaning | Body |
|---|---|---|
| `400` | The request is malformed: an id that is not an integer, a value outside its set, a missing parameter or field | One sentence, `text/plain` (a JSON `{"error": …}` where the entry says so) |
| `401` | No valid token where one is needed: missing, unknown, expired or revoked | `{"error": …}` |
| `403` | A valid token that may not do this: out of scope (`repo_out_of_scope`, `entity_out_of_scope`), the wrong token kind or role, or the account's repository allocation is full (`capacity_limit`) | JSON |
| `404` | No such resource | One sentence |
| `409` | A conflict with stored state the entry describes (a setting `aveloxis.json` owns, a superseded change) | One sentence or JSON, per entry |
| `422` | Understood but not acted on: the account e-mail could not be sent a confirmation | JSON naming why |
| `429` | A rate limit: retry after `Retry-After` seconds | `rate limit exceeded`, or the `capacity_limit` body |
| `500` | A server-side failure. The body never carries the cause; it is in the api log | `internal error; try again`, or a fixed phrase |
| `503` | Try again: a presented token could not be checked because the database failed (the token was not rejected), or the request ran past `http_timeout_seconds` and was cut off | `{"error": "session lookup failed; try again"}`, or the text `request exceeded http_timeout_seconds` |

A client should treat `401` as "the token is gone" and `503` as "try
again"; they are never mixed. Every `401` and `403` is sent
`Cache-Control: private, no-store`.

## Rate limits and quotas

Every request passes the rate limiter. A caller without a valid token is
limited per address; a session and an API token are counted against
their own quotas from every address. The limits, their defaults and the
quota modes are described in the
[guide's Rate limits section](../guide/api.md#rate-limits).

An answer counted against an enforced quota carries the IETF `RateLimit`
and `RateLimit-Policy` header fields and the older `X-RateLimit-Limit`,
`X-RateLimit-Remaining` and `X-RateLimit-Reset`, all readable by
cross-origin browser clients. A refusal by a quota is the
`capacity_limit` body: `429` with `Retry-After` for a rate quota, `403`
without one for the repository allocation, which does not free up with
time.

## Caching and validators

A repository's own read operations whose reference entry lists `304` are
served from a response cache that is kept until the repository's state
changes — it is collected or scanned again, or a repair outside a
collection rewrites its data — or, for an answer over a window, until that
window moves (no answer expires on a timer):

- the answer carries an `ETag`; send it back as `If-None-Match` and an
  unchanged answer is `304 Not Modified` with no body;
- `Cache-Control: no-cache` marks an answer any caller may be given (a
  client revalidates each time), and `private, no-store` an answer about
  this caller, which nothing may store;
- `X-Cache: hit` says the answer came from the cache.

The [guide](../guide/api.md#top-contributors-ranked-v02761) describes
when the cache is on and what it keeps.

## Times and windows

- Instants are RFC 3339 strings in UTC (`2026-10-10T14:02:11Z`).
- Days are `YYYY-MM-DD` and mean UTC days. A day parameter that does not
  parse is a `400` on every operation (since 0.29.99; the repository and
  contribution operations fell back to their default before). A window
  whose start is not before its end is a `400` everywhere.
- `until` is inclusive on the repository series and contribution
  operations, and exclusive on the comparison, which also ends at the last
  complete bucket. The [guide](../guide/api.md#window-boundary-semantics-v02739)
  explains why both stay as they are.

## Compatibility

- **One field name for a metric's number.** The comparison series
  (`/compare`) and the single-number Augur-compatible metric routes carry
  the number in a field named `value`. Augur names that field per metric
  (`stars`, `forks`, `commit_count`, …); a client moving from Augur reads
  `value` here instead. The exceptions, each marked in the reference:
  `/repos/{id}/watchers` keeps Augur's `watchers` key (the one series whose
  shape already matched Augur's when this rule was decided);
  `/repos/{id}/code-changes-lines` is a two-number row (`added`, `removed`)
  under Augur's names; and the older native weekly series of
  `/repos/{id}/timeseries` carries `count` (unchanged since it shipped; its
  clients predate the rule). The 0.29.111 release notes record the
  decision.

- Adding an operation, an optional parameter or a field is a compatible
  change. Clients must ignore fields they do not know.
- Removing or renaming an operation, a parameter or a field, or changing
  what a field means, is a breaking change and is called out in the
  release notes.
- Absent and `null` are different where the reference says so: an
  omitted field usually means "not known" (no snapshot yet), never zero.

## What the tests check

`scripts/openapi_routes_test.go` reads the spec and the route
registrations in `internal/api` and fails the build when:

- a registered route is not in the spec, or a spec path is not registered;
- a path parameter is missing, or a declared query parameter is not one
  the handler reads (and the other way round);
- an operation's `security` is not the access its handler enforces;
- an operation leaves out a response its access or its handler implies:
  `429` and `503` everywhere, `401` off the public paths, `403` where the
  handler refuses by role or scope, `500` where it can fail on the server,
  `304` with `ETag` and `If-None-Match` on the response-cached routes;
- an `operationId` is not lowerCamelCase or not unique, a read's name
  starts with a verb such as `get`, a summary is not one sentence ending
  in a period, an operation does not have exactly one tag, a description
  does not cite its handler, or a `$ref` does not resolve.

The design rules above that need judgment (the right words, the cost
notes) are checked by review.
