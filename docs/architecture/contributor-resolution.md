# Contributor Resolution

Contributor resolution is the process of mapping platform user references (login, email, avatar, etc.) to canonical contributor records in the database. Aveloxis has two layers of resolution that operate at different phases of the collection pipeline.

---

## Two layers

### Layer 1: API-phase resolution (during collection)

Operates during issue, PR, event, and message collection. Maps platform user references to `cntrb_id` UUIDs.

### Layer 2: Git-phase resolution (after facade)

Operates after the facade phase. Maps git commit author emails to GitHub user accounts.

---

## API-phase resolution: ContributorResolver

The `ContributorResolver` resolves platform user references to canonical `cntrb_id` UUIDs using a three-tier strategy.

### Tier 1: In-memory cache

The cache maps three lookups:

- **Platform user ID** -> `cntrb_id`
- **Email** -> `cntrb_id`
- **Login** -> `cntrb_id`

On a cache hit, no database query is needed. The cache is write-through: any new contributor inserted into the database is also added to the cache.

The cache persists across repos within the same process lifetime, so contributors who appear in multiple repos are resolved without repeated DB lookups.

### Tier 2: Database lookup

If the cache misses, the resolver queries the `contributor_identities` table:

```sql
SELECT cntrb_id
FROM aveloxis_data.contributor_identities
WHERE platform_id = $1 AND platform_user_id = $2
```

The unique constraint on `(platform_id, platform_user_id)` ensures this lookup returns at most one result.

### Tier 3: Create new

If no existing contributor is found, a new one is created:

1. A deterministic `cntrb_id` UUID is generated via **GithubUUID**
2. A row is inserted into `contributors` with all available profile fields
3. A row is inserted into `contributor_identities` mapping the platform identity to the contributor
4. Both the cache and the database are updated

---

## GithubUUID: Deterministic contributor IDs

Aveloxis generates `cntrb_id` UUIDs deterministically from the platform user ID. The UUID encodes:

| Byte(s) | Content |
|---|---|
| 0 | Platform ID (`1` = GitHub, `2` = GitLab) |
| 1-4 | Platform user ID (big-endian, zero-padded) |
| 5-15 | Zero-filled |

### Properties

- **Deterministic:** The same GitHub/GitLab user always gets the same UUID, regardless of which Aveloxis instance creates it.
- **Augur-compatible:** The encoding scheme matches Augur's `GithubUUID` function, so contributor IDs are byte-compatible between the two systems.
- **Cross-platform safe:** GitHub and GitLab users with the same numeric ID get different UUIDs (different platform byte).
- **Large ID support:** GitHub user IDs up to 2^32 are supported in 4 bytes.

### Example

GitHub user ID `12345` on platform `1` (GitHub):

```
Byte 0: 0x01 (GitHub)
Bytes 1-4: 0x00003039 (12345 big-endian)
Bytes 5-15: 0x00000000000000000000
UUID: 01003039-0000-0000-0000-000000000000
```

---

## Git-phase resolution: Commit resolver

After the facade phase inserts commit rows, the commit resolver maps git commit author emails to GitHub user accounts. This is the Go implementation of the [augur-contributor-resolver](https://github.com/augurlabs/augur-contributor-resolver) scripts.

### Resolution strategy (cheapest first)

The resolver processes unresolved commit emails in order of cost:

#### 1. Noreply email parse (free)

GitHub noreply emails have a predictable format:

```
12345+username@users.noreply.github.com
username@users.noreply.github.com
```

The parser extracts:

- **Login** from the username portion
- **`gh_user_id`** from the numeric prefix (if present)

No API call is needed. The contributor can be resolved entirely from the email string.

#### 2. Database lookup (free)

Checks existing records:

- `contributors.cntrb_email` -- direct email match
- `contributors.cntrb_canonical` -- canonical email match
- `contributors_aliases.alias_email` -- alias email match

#### 3. GitHub Commits API (1 API call)

```
GET /repos/{owner}/{repo}/commits/{sha}
```

The response includes the linked GitHub user object with all profile fields (`gh_user_id`, `gh_node_id`, `gh_avatar_url`, all `gh_*` URLs, etc.).

This is the most reliable method because it uses the commit SHA itself to find the linked user.

#### 4. GitHub Search API (1 API call)

```
GET /search/users?q={email}+in:email
```

For remaining non-noreply emails that were not found by the Commits API. The Search API has a lower rate limit (30 requests/minute), so it is used as a last resort.

### Post-resolution actions

For each resolved commit author:

1. **Update commit rows:** `cmt_author_platform_username` is set on all commit rows with that author's email
2. **Create/update contributor:** A contributor row is created (or updated) with the deterministic GithubUUID and all `gh_*` profile fields
3. **Detect login renames:** If the same `gh_user_id` maps to a different login than what is in the database, the contributor's `gh_login` is updated
4. **Create alias:** An entry in `contributors_aliases` maps the commit email to the contributor's canonical email
5. **Bulk backfill:** After all commits are resolved, a SQL join sets `cmt_ght_author_id` from `cmt_author_platform_username` -> `contributors.gh_login`

---

## Login rename detection

GitHub users can rename their accounts. When this happens:

- The old login still appears in historical API responses and git commits
- The new login appears in current API responses
- The `gh_user_id` remains the same

Aveloxis detects renames by comparing:

```
IF existing_contributor.gh_user_id == resolved_user.gh_user_id
   AND existing_contributor.gh_login != resolved_user.gh_login
THEN update gh_login to the new value
```

This keeps contributor records current without creating duplicate entries.

---

## Canonical email enrichment

After commit resolution, some contributors have `gh_login` but no `cntrb_canonical` email. Phase 5 calls the GitHub Users API:

```
GET /users/{login}
```

The profile response includes the user's public email, which is set as `cntrb_canonical`.

---

## Alias creation

The `contributors_aliases` table maps alternate email addresses to a contributor's canonical email. Aliases are created during commit resolution:

```sql
INSERT INTO aveloxis_data.contributors_aliases
  (cntrb_id, canonical_email, alias_email, cntrb_active)
VALUES ($1, $2, $3, 1)
ON CONFLICT (alias_email) DO NOTHING;
```

The alias table is consulted during database lookups (tier 2 of the API-phase resolver), enabling future resolution of the same email without API calls.

---

## Contributor breadth worker

Every 6 hours, the scheduler runs the breadth worker to discover cross-repo contributor activity:

1. Selects up to 100 contributors, prioritizing those never processed, then oldest
2. For each contributor, calls `GET /users/{login}/events`
3. Each event (PushEvent, PullRequestEvent, IssuesEvent, etc.) is stored in `contributor_repo`

This maps contributors to their activity across repos outside the tracked set, providing a broader picture of contributor engagement.

---

## Resolution flow diagram

```
Commit email from git log
        |
        v
  ┌─────────────────┐
  │ Noreply parse?   │──yes──> Extract login + user_id
  └────────┬─────────┘         Create/update contributor
           │ no
           v
  ┌─────────────────┐
  │ DB lookup?       │──yes──> Return existing cntrb_id
  │ (email, alias)   │
  └────────┬─────────┘
           │ no
           v
  ┌─────────────────┐
  │ Commits API?     │──yes──> Create contributor with GithubUUID
  │ GET /commits/sha │         Create alias
  └────────┬─────────┘
           │ no
           v
  ┌─────────────────┐
  │ Search API?      │──yes──> Create contributor with GithubUUID
  │ GET /search/users│         Create alias
  └────────┬─────────┘
           │ no
           v
  Unresolved (stored in
  unresolved_commit_emails)
```

---

## Next steps

- [Facade Commits](facade-commits.md) -- how git log data is parsed and stored
- [Staged Pipeline](staged-pipeline.md) -- how staging enables bulk contributor resolution
- [Overview](overview.md) -- system architecture overview
