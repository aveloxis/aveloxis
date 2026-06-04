# Contributor Resolution

Contributor resolution is the process of mapping platform user references — login names, numeric platform user IDs, commit-author emails, mailing-list sender emails — to canonical contributor records in the database. It is one of the most heavily-exercised paths in aveloxis: every issue reporter, PR author, review author, message author, event actor, commit author, and mailing-list sender triggers it.

The whole subsystem has three layers worth keeping distinct in your head:

1. **Data origins** — *where* a contributor reference enters the system (API actors, git commit authors, mailing-list senders). Each origin hands off a `login`, a `platform_user_id`, an email, or some subset.
2. **Shared processing** — the resolution logic the origins funnel through. Login/ID references go through `ContributorResolver.Resolve`; **bare emails** (commit authors, mailing-list senders) go through one shared chain, [`ResolveEmailToIdentity`](#shared-emailidentity-resolution).
3. **Identity** — the canonical `contributors` row (`cntrb_id`) every origin ultimately maps to, and the guarantees about it.

This document is the **public contract** for contributor data in aveloxis. The contract has thirteen rules covering identity-key determinism, immutability, deduplication, enrichment, and FK integrity. Operators rely on these guarantees when writing analytics queries against `aveloxis_data.contributors` and its child tables.

If you are looking for the high-level resolution flow, jump to [Resolution flow](#resolution-flow). If you are diagnosing a specific data-quality concern, jump to [Data-quality FAQ](#data-quality-faq) or [Diagnostic queries](#diagnostic-queries).

---

## What a contributor record represents

A row in `aveloxis_data.contributors` represents one **person or bot** that interacts with repositories aveloxis tracks. The same person may have a GitHub identity (`gh_user_id`, `gh_login`), a GitLab identity (`gl_id`, `gl_username`), one or more commit-author emails, or any combination of these. The `cntrb_id` UUID on the row is stable and is referenced from 16+ child tables (commits, issues, PRs, messages, reviews, events).

The schema separates three concerns:

- **`aveloxis_data.contributors`** — one row per individual; carries denormalized `gh_*` / `gl_*` columns for backward-compatible Augur queries and the canonical email (`cntrb_canonical`).
- **`aveloxis_data.contributor_identities`** — one row per `(platform, platform_user_id)` observation; the relational truth for "this person has this account on this platform."
- **`aveloxis_data.contributors_aliases`** — one row per distinct commit-author email; maps alternate emails back to the canonical contributor.

A contributor with both a GitHub and a GitLab identity has one `contributors` row and two `contributor_identities` rows. A contributor with three different commit emails has one `contributors` row and three `contributors_aliases` rows.

---

## Where contributor data comes from

Three independent origins feed contributor data. They differ in *what identifier they carry* and *which resolution path they take*, but all converge on a single `contributors` row.

| Origin | Entry point | Identifier it carries | Resolution path |
|---|---|---|---|
| **Platform API actors** (issue/PR/review/comment authors, event actors) | Staged collector → `ContributorResolver.Resolve` | `login` + `platform_user_id` (+ name, email) | Login/ID lookup → deterministic-UUID INSERT ([Layer 1](#layer-1-api-phase-resolution-during-collection)) |
| **Git commit authors** | Facade → `CommitResolver.ResolveCommits` | a bare commit-author **email** (rarely a noreply login) | Commit-specific SHA lookup, then the [shared email→identity chain](#shared-emailidentity-resolution) ([Layer 2](#layer-2-git-phase-resolution-after-facade)) |
| **Mailing-list senders** | `MailingListProcessor` (drains staged email) + the sender-resolve ticker | a bare sender **email** (+ display name) | The [shared email→identity chain](#shared-emailidentity-resolution) ([Layer 3](#layer-3-background-tasks)) |

The key structural point: **the two email-bearing origins (commits, mailing-list senders) share one resolution chain.** A commit author and a mailing-list sender with the same email resolve to the same `contributors` row, and an email that resolves to a platform identity *later* (as the DB fills) is reconciled by the same background machinery for both. This is deliberate — see [Shared email→identity resolution](#shared-emailidentity-resolution).

---

## Contract rules

The thirteen rules below define what the system promises about contributor data. They are referenced by ID throughout the codebase (search for "R1", "R2", etc. in code comments) and in the private `summary/02-contract.md` design document.

### R1: Identity-key determinism

When `(platform, platform_user_id)` is known with `platform_user_id > 0`, `cntrb_id` MUST equal `PlatformUUID(platform, platform_user_id)`. This produces an Augur-byte-compatible UUID encoding the platform byte and the user ID. Two independent collections of the same GitHub or GitLab user produce the same UUID — cross-database joins on `cntrb_id` work without content drift.

When `platform_user_id == 0` (commit author with no resolved platform user), `cntrb_id` is a random `gen_random_uuid()`. This is the only legal source of nondeterminism, typically affecting <1% of contributors.

### R2: Identity-key immutability

Once `cntrb_id` is written, it MUST NOT change. There is no operation in aveloxis that updates `cntrb_id` on an existing row. If a row was inserted with a random UUID and the contributor later turns out to have a `gh_user_id`, the search-resolve background task adds the `gh_user_id` to the existing row but does NOT migrate the row to the deterministic UUID. The two would differ only in the value of `cntrb_id`, and changing it would orphan every FK reference (commits, issues, PRs, messages — 16+ columns).

This rule is the reason aveloxis does not ship a 16-table merge migration. The cleanup tools in [augur-contributor-resolver](https://github.com/aveloxis/augur-contributor-resolver) exist precisely because Augur did the opposite — letting `cntrb_id` mutate post-hoc and accumulating orphans. Aveloxis prevents the bad state at the write boundary.

### R3: `cntrb_login` partial uniqueness

The partial unique index `idx_contributors_login` enforces that no two rows share the same non-empty `cntrb_login`. Empty logins are unconstrained — multiple email-only contributors can coexist with `cntrb_login = ''`.

**Rename edge case.** When a platform user renames between observations, three outcomes are possible:

1. The new login is unobserved elsewhere — `Resolve` updates the existing row's `cntrb_login` to the new value via the `ON CONFLICT (cntrb_id) DO UPDATE` branch. Clean.
2. The new login is already observed under a different `cntrb_id` AND the partial unique index trips at write time — the `UpsertContributorFull` 23505 fallback UPDATEs the row's other fields without touching `cntrb_login`. Two rows representing the same person coexist with different logins, both visible in lookup queries. This was the steady-state behavior pre-v0.20.2.
3. **(v0.20.2 onward)** When `runSearchResolve` later identifies the duplicate via the search API result and calls `LinkContributorToGitHubUser`, the function now performs a **logical merge** (soft-delete pattern). It picks a winner — preferring the row whose `cntrb_id` matches `PlatformUUID(1, ghUserID)` per R1, falling back to the older row — copies non-empty fields from the loser(s) into the winner, inserts `contributors_aliases` rows so the loser's emails resolve to the winner, and sets `cntrb_deleted = 1` on the loser(s). The loser rows are NOT deleted physically (preserving R2 identity-key immutability and R10 FK integrity); they're filtered out of every lookup query via `COALESCE(cntrb_deleted, 0) = 0`. Analytics/aggregate queries that JOIN on `cntrb_id` continue to see the loser rows so historical activity stays attributed correctly.

The v0.20.2 logical merge means outcome #2 is now self-correcting: the next search-resolve cycle that processes the duplicate's email cleans it up automatically.

**`cntrb_deleted` semantics (v0.20.2):**
- `0` (default) — active row, returned by lookup queries.
- `1` — loser of a rename merge. Filtered out of `Resolve`'s lookup-by-login, `FindLoginByEmail`, `FindContributorIDByLogin`, `GetThinContributorLogins`, `GetContributorsNeedingSearch`, `GetContributorsMissingCanonical`, and `PopulateAffiliations` candidate selection. Still visible to analytics queries that JOIN on `cntrb_id` directly (so historical FK references continue to resolve).
- The column already existed in the schema (legacy Augur compatibility); v0.20.2 repurposes it for the soft-delete merge semantics. Pre-v0.20.2 rows are all `cntrb_deleted = 0` or NULL — both treated as active by `COALESCE(cntrb_deleted, 0) = 0`.

### R4: Identity rows are denormalized truth

Every observation of `(platform_id, platform_user_id)` produces a row in `contributor_identities` (UNIQUE on the pair). The denormalized `gh_*` / `gl_*` columns on the `contributors` row are convenience copies; when they disagree with `contributor_identities`, the latter wins.

Backfill from `contributor_identities` to `contributors.gh_*` happens via `backfillGHColumns` and the inline `UPDATE` inside `UpsertContributorBatch`.

### R5: Email aliasing

Every distinct commit-author email observed for a resolved contributor lands in `contributors_aliases` with `alias_email = email` and `cntrb_id` pointing at the contributor. The `UNIQUE (alias_email)` constraint enforces that one email maps to exactly one contributor.

If an email is observed under two different `cntrb_id`s, that is a duplicate-contributor bug, not a multi-mapping. Use the [Diagnostic queries](#diagnostic-queries) to detect this case.

### R6: Enrichment is best-effort, cooldown-bounded

A "thin" contributor has empty `cntrb_company` AND empty `cntrb_location`. The periodic enrichment ticker calls `client.EnrichContributor(login)` to fill these fields. If the platform profile is genuinely empty (the user has not set company / location), the row's `cntrb_last_enriched_at` is stamped to `NOW()` to suppress retry for the cooldown window (30 days).

Two separate cooldown columns track two distinct background tasks:

- `cntrb_last_enriched_at` — `runEnrichment` and `ResolveEmailsToCanonical` cooldown.
- `cntrb_last_search_attempted_at` — `runSearchResolve` (search-by-email) cooldown.

Without these cooldowns, every periodic tick would re-process the same "genuinely empty" rows forever, wasting GitHub Search API and core API quota.

### R7: Cached resolution on the hot path

Every UserRef observed during staged collection MUST go through `ContributorResolver.Resolve` (defined in `internal/db/contributors.go`), not a direct `UpsertContributor` call. The resolver caches `(platform, user_id) → cntrb_id` for the lifetime of the collection job. Two observations of the same user inside one repo's data hit the cache; across workers (different repos), each worker pays the cache-miss SELECT.

This rule is the input to the planned process-wide cache (Phase C of `summary/04-refactoring-plan.md`), which lifts the cache to fleet scope.

### R8: Bulk operations dedup before flushing

`UpsertContributorBatch` MUST deduplicate by login in memory ("richest data wins") before flushing in a single transaction. Per-row `UpsertContributor` calls in a tight loop are forbidden — that pattern produced 14,000 single-row transactions per enrichment cycle on a 100K-repo fleet (v0.18.28 pre-fix) and induced deadlocks on the contributors hot index.

The batch path is used by:

- `EnrichThinContributors` (after the periodic enrichment ticker)
- The staged processor's `processBatch` (for `EntityContributor` payloads)
- The legacy `Collector.collectContributors`

### R9: Idempotent re-collection

Every contributor INSERT carries an `ON CONFLICT` clause. Re-collecting the same repo produces the same final state — `DO UPDATE` coalesces non-empty incoming fields into existing fields without overwriting non-empty existing values. This is verified by the `TestAllDataInsertTablesHaveOnConflict` source-contract test.

### R10: Foreign-key integrity

Every `cntrb_id` referenced from a child table MUST exist in `contributors`. The resolver creates the parent row before any child row references it. This is what prevents the orphan-FK problem that drove the augur-contributor-resolver post-hoc fix scripts.

### R11: Search-resolve never modifies identity

The `runSearchResolve` background task and its `LinkContributorToGitHubUser` worker function MUST NOT modify `cntrb_id` (R2) or `cntrb_login` (R3). They only backfill `gh_user_id`, `gh_login`, the audit column `cntrb_last_search_attempted_at`, and add a `contributor_identities` row.

### R12: Commit author resolution chain order

`CommitResolver.resolveOne` tries strategies in this exact order, stopping at the first hit:

1. In-memory hash cache — same SHA seen this job
2. In-memory email cache — same email seen this job
3. `ParseNoreplyEmail` — extracts login + user_id from `12345+username@users.noreply.github.com` (free, no API call)
4. Bot/junk email skip
5. `FindLoginByEmail` — DB lookup against existing `cntrb_email` / `cntrb_canonical` / `contributors_aliases`
6. GitHub Commits API (`/repos/{o}/{r}/commits/{sha}`) — 1 token call (commit-specific; uses the SHA, not the email)
7. The **shared API tail** `ResolveEmailViaAPI` (Search API by email → global commit-search by author-email) — 1–2 search-budget calls

Strategies 1–5 are free. Strategies 6–7 cost API quota and fire only on first observation. Step 7 is NOT commit-specific — it is the same chain the mailing-list sender resolver uses (see [Shared email→identity resolution](#shared-emailidentity-resolution)). The commit resolver keeps its SHA lookup (step 6) ahead of it because the SHA is a stronger signal than an email for a commit.

### R13: Documentation as a first-class deliverable

This document IS R13's deliverable. The contract is public. Operator-facing changes to contributor data shape MUST be reflected here in the same release that ships the code change. The source-contract test `TestContributorResolutionDocReferencesCanonicalFunctions` fails the build if any of `ContributorResolver.Resolve`, `UpsertContributorBatch`, or `LinkContributorToGitHubUser` is renamed without updating this doc.

---

## Resolution flow

### Layer 1: API-phase resolution (during collection)

When the staged collector observes a UserRef from issues, PRs, events, or messages:

```
UserRef (login + platform_user_id + name + email + ...)
        |
        v
  ContributorResolver.Resolve
        |
        ├── 1. In-memory cache by (platform, user_id) — hit returns immediately
        |
        ├── 2. SELECT contributor_identities WHERE platform_id=? AND platform_user_id=?
        |       — hit caches and returns
        |
        ├── 3. SELECT contributors WHERE cntrb_login=?
        |       — hit reuses existing row, backfills identities row, caches, returns
        |
        └── 4. INSERT contributors + contributor_identities in one transaction
                 - userID > 0: ON CONFLICT (cntrb_id) DO UPDATE     — R1, R3
                 - userID == 0: ON CONFLICT (cntrb_login) DO UPDATE — R3 partial
```

Step 3 (lookup-by-login before INSERT) was added in v0.19.2 to prevent the partial-unique-index race that produced "duplicate key value violates unique constraint idx_contributors_login" floods in production logs. Without it, two workers observing the same renamed user under the new login both attempt INSERT with different deterministic UUIDs and one trips the constraint.

### Layer 2: Git-phase resolution (after facade)

After the facade phase loads commits, `CommitResolver.ResolveCommits` walks every commit with `cmt_author_platform_username IS NULL` and tries the seven-step chain in [R12](#r12-commit-author-resolution-chain-order). On a hit, it calls `UpsertContributorFull` to create or update the contributor row, then `EnsureContributorAlias` to record the commit email.

```
For each unresolved commit:
        |
        v
  resolveOne(hash, email)
        |
        ├── hash cache → email cache → noreply parse → bot skip → DB lookup → Commits API → shared API tail (Search → global commit-search)
        |
        v
  on hit:
        |
        ├── SetCommitAuthorLogin(repo, hash, login)
        ├── UpsertContributorFull(deterministic-uuid, login, gh_user_id, email)  — R1, R2
        └── EnsureContributorAlias(cntrb_id, email)                              — R5

After all commits resolved:
        |
        └── BackfillCommitAuthorIDs(repo) — bulk UPDATE commits SET cmt_ght_author_id = ...
```

`UpsertContributorFull` carries the most defensive logic in the codebase. The 23505 fallback (v0.19.2) catches the partial-unique-index trip described in [R3](#r3-cntrb_login-partial-uniqueness) and degrades gracefully — the rename edge case becomes a logged Debug line, not a job-killing error.

### Layer 3: Background tasks

Three periodic tickers run inside `aveloxis serve`. Each is rate-limited and cooldown-bounded per [R6](#r6-enrichment-is-best-effort-cooldown-bounded):

| Ticker | Cadence | What it does | Cooldown column |
|---|---|---|---|
| `runEnrichment` | 30 min | ≤14000 thin contributors → `client.EnrichContributor(login)` → `UpsertContributorBatch` | `cntrb_last_enriched_at` |
| `runSearchResolve` | 1 hour | ≤100 contributors with email + no `gh_user_id` → `SearchUserByEmail` → `LinkContributorToGitHubUser` on hit | `cntrb_last_search_attempted_at` |
| `runMailingListSenderResolve` | 1 hour | ≤N mailing-list senders (≥ message threshold) with no resolved `cntrb_id` → [shared email→identity chain](#shared-emailidentity-resolution) → link/create on hit | `sender_last_resolve_attempt` |
| `runBreadth` | configurable | Discovers cross-repo contributor activity via Events API; writes `contributor_repo` only | n/a (per-contributor priority) |

The two email-resolve tickers (`runSearchResolve` for contributor rows, `runMailingListSenderResolve` for mailing-list senders) are the **convergence machinery**: an email observed before its owner's platform identity is known gets re-attempted as the DB fills, so email-only rows acquire a `gh_user_id` over days without re-collection. In addition to the tickers, every per-job collection runs the commit resolver (Layer 2) and a `ResolveEmailsToCanonical` pass that fills `cntrb_canonical` for ≤500 contributors per call.

### Shared email→identity resolution

Two origins carry a bare email rather than a platform login: **git commit authors** and **mailing-list senders**. Both resolve that email through ONE shared function so the two paths can never drift on what "resolve an email" means (operator directive, 2026-06-04: *"one shared piece of code for these two distinct but highly similar use cases"*). It lives in `internal/collector/email_identity_resolver.go`:

```
ResolveEmailToIdentity(store, client, email)
        |
        ├── 1. ParseNoreplyEmail        — free; login+id from a GitHub noreply address
        ├── 2. bot/junk/non-email skip  — free
        ├── 3. FindLoginByEmail (DB)    — existing cntrb_email / canonical / aliases
        └── 4. ResolveEmailViaAPI:
                 ├── SearchUserByEmail            — /search/users?q={email}+in:email   (public-profile email only, ~8–11%)
                 └── SearchCommitByAuthorEmail    — /search/commits?q=author-email:{email}  (GLOBAL; ~35%, the load-bearing step for private-profile emails)
```

- **`ResolveEmailToIdentity`** is the full chain (noreply → bot → DB → API tail). The mailing-list sender resolver and the batch `MailingListProcessor` call this.
- **`ResolveEmailViaAPI`** is just the API tail (Search → global commit-search). The `CommitResolver` calls this directly as its step 7, keeping its commit-specific SHA lookup (step 6) ahead of it — see [R12](#r12-commit-author-resolution-chain-order).
- A no-resolution outcome is `("", 0, "", nil)` — **not** an error. Errors are returned only on transport/5xx failures the caller should retry. This matches the platform search-method contract.
- The returned `source` (`noreply` / `db` / `search` / `commit-search`) is telemetry: it tells operators *how* an email was resolved, which is the signal for tuning the API budget.

Global commit-search is the reason mailing-list senders resolve at all for the common case (a participant who keeps their profile email private but has authored a public commit somewhere on GitHub). It is the single highest-yield step and is why the chain is shared rather than duplicated per origin.

---

## GithubUUID / PlatformUUID

The deterministic UUID encoding is byte-compatible with Augur:

| Byte(s) | Content |
|---|---|
| 0 | Platform ID (1 = GitHub, 2 = GitLab) |
| 1–4 | Platform user ID (big-endian uint32, when `userID ≤ MaxUint32`) |
| 1–8 | Platform user ID (big-endian uint64, when `userID > MaxUint32`) |
| Remaining | Zero-filled |

Example: GitHub user `12345` (platform=1):

```
Byte 0:  0x01
Bytes 1-4: 0x00003039  (12345 big-endian)
Remaining: 0x00000000000000000000
UUID:    01003039-0000-0000-0000-000000000000
```

The 8-byte fallback for IDs above 2³² is non-Augur-compatible by necessity (Augur predates GitHub's overflow concern). Existing rows in either layout are never re-encoded. The function is in `internal/db/github_uuid.go`.

---

## Rename handling: which columns track renames, which don't

GitHub and GitLab both allow users to rename their account. The numeric `user_id` is stable across renames, so the deterministic `cntrb_id = PlatformUUID(platform, user_id)` keeps every cross-table reference valid. But the **login** field has multiple representations in the schema, and they behave differently on a rename. This is the contract.

### Three tables, three rename behaviors

#### `aveloxis_data.contributors` (one row per person)

| Column | Updated on rename? | Purpose |
|---|---|---|
| `cntrb_id` (PK) | **Never** | Stable identity. All FK references (16 columns across 15 tables — see [R10](#r10-foreign-key-integrity)) depend on this. |
| `cntrb_login` | **Never** (R2 invariant) | Durable audit trail — the login as first observed for this contributor. Subject to `idx_contributors_login` partial unique index. Use this when asking "what was this person originally called?" |
| `gh_login` / `gl_username` | **Yes** | The "current display name" mirror. Updated by `RenameContributorGhLogin` (v0.22.12, breadth-worker 404 path) and by `UpsertContributorBatch`'s rename-recovery branch (v0.22.13, batch-upsert pkey-collision path). Indexed by `idx_contributors_gh_login` for case-insensitive lookups. Use this when asking "what is this person called RIGHT NOW?" |
| `gh_user_id` / `gl_id` | Never (write-once via COALESCE) | Stable platform integer ID. Drives the deterministic `cntrb_id`. |

A renamed contributor ends up with `cntrb_login='oldname'` and `gh_login='newname'` on **the same row**. Two different login values, each authoritative for a different question.

#### `aveloxis_data.contributor_identities` (one row per `(platform_id, platform_user_id)`)

The unique key is `(platform_id, platform_user_id)`, so there is **one identity row per platform user** — not one per observation. On every observation:

```sql
INSERT INTO contributor_identities (cntrb_id, platform_id, platform_user_id, login, ...)
VALUES (...)
ON CONFLICT (platform_id, platform_user_id) DO UPDATE SET
    login = EXCLUDED.login,        -- overwritten in place
    name = EXCLUDED.name,
    email = COALESCE(NULLIF(EXCLUDED.email,''), contributor_identities.email),
    avatar_url = EXCLUDED.avatar_url,
    profile_url = EXCLUDED.profile_url;
```

The previous login is **overwritten** on the existing identity row. **We do NOT create a new identity row for a rename.** The identity row is a "current state per platform user" record, not a history. After a rename, the previous login at the identity layer is lost.

#### `aveloxis_data.commits` (one row per file per commit)

- `cmt_author_platform_username TEXT` — a **frozen raw string** set once at commit-resolution time. No FK constraint; just text. It stays at whatever login was current when the commit was attributed. Renames after the fact do not update it.
- `cmt_ght_author_id UUID` — the stable FK to `contributors(cntrb_id)`. **This is what keeps commit attribution correct across renames**, because the cntrb_id is deterministic from gh_user_id and the gh_user_id never changes.

`BackfillCommitAuthorIDs` resolves unresolved commits via `LOWER(cmt_author_platform_username) = LOWER(cn.gh_login)` (v0.20.12 Fix H, case-insensitive). It joins against the **current** `gh_login`, not `cntrb_login`. The function only updates rows with `cmt_ght_author_id IS NULL`, so once a commit is attributed it stays attributed regardless of subsequent renames.

**v0.25.6 coverage asymmetry closure.** Production audit on 2026-05-29 of `aveloxis_large` (474M commit rows) found `cmt_ght_author_id` populated on 91.95% of rows but `cmt_author_platform_username` populated on only 77.58%. The asymmetry comes from API paths that stamp the UUID without updating the login text on every matching commit row (noreply email parsing, Commits API resolution, email-based backfill). v0.25.6 ships a one-shot migration `backfill cmt_author_platform_username from cmt_ght_author_id` that UPDATEs commits SET platform_username = c.gh_login wherever the UUID is populated but the login text isn't. After deploy, login-based queries against commits match the UUID-based queries' coverage. Soft-deleted contributors (R10 / v0.20.2 merge losers) are skipped per `COALESCE(c.cntrb_deleted, 0) = 0`.

### Operational consequences

**"Show me commits by user X (where X is the current login)"** — JOIN commits ON `cmt_ght_author_id`, then filter on `contributors.gh_login = 'X'`. The FK is stable; the gh_login mirror reflects current state. Works.

**"What was this commit's author at write time?"** — Read `commits.cmt_author_platform_username` directly. Frozen string. Useful for audit logs and historical accuracy.

**"What was this user's first observed login on aveloxis?"** — Read `contributors.cntrb_login`. Never mutates.

**"What login has this person been known by between then and now?"** — **Stored as of v0.23.0** in `aveloxis_data.contributor_login_history`. Every observed `(cntrb_id, platform_id, login)` triple gets a row, with `first_seen` preserved across re-observations and `last_seen` advancing. The `source` column tags the row with WHY it was created (`observation`, `rename_recovery`, `rename_breadth`, `backfill`). Pre-v0.23.0 only `cntrb_login` (first observation) and `gh_login` (current state) survived; intermediate renames between those two were lost. See the "Login history table (v0.23.0)" section below for query patterns.

### Login history table (v0.23.0)

`aveloxis_data.contributor_login_history` is the append-only audit trail of every login a contributor has been observed under. Schema:

```sql
CREATE TABLE aveloxis_data.contributor_login_history (
    history_id   BIGSERIAL PRIMARY KEY,
    cntrb_id     UUID NOT NULL REFERENCES contributors(cntrb_id)
                 ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    platform_id  SMALLINT NOT NULL REFERENCES platforms(platform_id) DEFERRABLE INITIALLY DEFERRED,
    login        TEXT NOT NULL,
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source       TEXT NOT NULL DEFAULT 'observation',
    tool_source  TEXT NOT NULL DEFAULT 'aveloxis',
    tool_version TEXT NOT NULL DEFAULT '',
    UNIQUE (cntrb_id, platform_id, login)
);
```

`source` values:

| Value | Written by | Trigger |
|---|---|---|
| `observation` | `UpsertContributorBatch` per-identity loop | Steady-state per-cycle write — most rows have this source. |
| `rename_recovery` | `UpsertContributorBatch` rename-recovery branch (v0.22.13) | Pkey collision recovery: same gh_user_id, new login arrived in a batch. |
| `rename_breadth` | `RenameContributorGhLogin` (v0.22.12) | Breadth worker hit 404, looked up by id, found a different current login. |
| `backfill` | One-shot migration step at v0.23.0 install | Seeded from existing `contributor_identities` + `contributors.cntrb_login` so the history table starts populated. |

Common queries:

```sql
-- Every login this person has ever used, oldest first
SELECT login, first_seen, last_seen, source
FROM aveloxis_data.contributor_login_history
WHERE cntrb_id = $1::uuid
ORDER BY first_seen ASC;

-- Find all rename events (any contributor whose history has more than
-- one login on the same platform)
SELECT cntrb_id, platform_id, count(*) AS login_count,
       array_agg(login ORDER BY first_seen ASC) AS chronological
FROM aveloxis_data.contributor_login_history
GROUP BY cntrb_id, platform_id
HAVING count(*) > 1;

-- Resolve a historical login to a current contributor (e.g. when
-- joining against an external dataset that captured a now-stale login)
SELECT c.cntrb_id, c.gh_login AS current_login
FROM aveloxis_data.contributor_login_history h
JOIN aveloxis_data.contributors c USING (cntrb_id)
WHERE h.login = $1 AND h.platform_id = 1
ORDER BY h.last_seen DESC
LIMIT 1;
```

The Go store method `PostgresStore.GetContributorLoginHistory(ctx, cntrbID)` returns the rows ordered chronologically — useful for the contributor detail page in the web GUI (display planned for a follow-up release).

### Why the two write paths exist (v0.22.12 vs v0.22.13)

Both paths update `gh_login` on the existing contributor row and never touch `cntrb_login`. They differ in *trigger*:

| Path | Trigger | Code |
|---|---|---|
| `RenameContributorGhLogin` (v0.22.12) | Breadth worker hits HTTP 404 on `/users/{stored_login}/events`, falls back to `/user/{stored_id}`, finds a different login | `internal/db/contributor_search_resolve.go` |
| `UpsertContributorBatch` rename-recovery (v0.22.13) | Enrichment ticker / staged collector calls batch upsert with new login; deterministic-UUID INSERT trips `contributors_pkey` | `internal/db/postgres.go` |

The v0.22.13 path is inlined inside the batch transaction (rather than calling out to `RenameContributorGhLogin`) because the batch tx already holds the savepoint scope and a nested transaction would deadlock against itself. The shape of the UPDATE statement is identical between the two paths: `SET gh_login = $newLogin, cntrb_email = COALESCE(...)` — preserving the contract uniformly.

---

## Data-quality FAQ

### Why do I see two rows for the same person?

Most likely cause: a platform-user rename, where the old login still appears in historical observations and a new login already exists in the table from a separate observation. See [R3](#r3-cntrb_login-partial-uniqueness). The two rows have different `cntrb_login` values but represent the same person. To detect:

```sql
-- Two cntrb rows sharing a gh_user_id (definite duplicate)
SELECT gh_user_id, count(*) AS rows, array_agg(cntrb_login)
FROM aveloxis_data.contributors
WHERE gh_user_id IS NOT NULL
GROUP BY gh_user_id
HAVING count(*) > 1;
```

To detect via identities:

```sql
-- Same (platform, platform_user_id) pointing at multiple cntrb_ids (orphan risk)
SELECT platform_id, platform_user_id, count(DISTINCT cntrb_id)
FROM aveloxis_data.contributor_identities
GROUP BY platform_id, platform_user_id
HAVING count(DISTINCT cntrb_id) > 1;
```

A future release will add a logical merge via `cntrb_deleted = 1` (Phase D of the refactor plan); for now, treat duplicates as a known data-quality limitation and coalesce in queries.

### Why is `cntrb_canonical` empty?

Three legal reasons (post-v0.25.6):

1. The user has set their email to private on GitHub. `EnrichContributor` returns no email; `ResolveEmailsToCanonical` cannot help. After the 30-day cooldown the row is retried in case the user changed their setting, but typically stays empty.
2. The contributor is `email-only` (no `gh_user_id`, no `gl_id`) — created from a commit author with a non-noreply email but no resolvable platform account. `cntrb_canonical` may be set to the commit email itself, or empty if `UpsertContributorFull` was called with `commitEmail = ''`.
3. The contributor was created very recently and the enrichment ticker has not yet reached them. They appear in the next enrichment cycle.

**Pre-v0.25.6 there was a fourth cause**: the commit resolver's `ensureAlias` (strategies 2 — DB lookup — and 4 — Search API) populated the `contributors_aliases` table but did NOT update `cntrb_canonical` on the parent contributor row. The forward fix in v0.25.6 calls `SetContributorCanonical` on every alias insert, and the one-shot v0.25.6 migration `backfill cntrb_canonical from contributors_aliases` reads `MIN(alias_email)` per cntrb_id and fills empty canonicals from the alias table. After running `aveloxis migrate` on v0.25.6, this fourth cause is closed; rows that remain empty fall into one of the three reasons above.

### What does `gh_user_id IS NULL` mean?

The contributor was created from a commit-only observation (no GitHub account observed yet), or from a UserRef that did not include a numeric `platform_user_id`. The `runSearchResolve` background task tries to backfill `gh_user_id` by searching GitHub for the email. If found, [R11](#r11-search-resolve-never-modifies-identity) backfills `gh_user_id` and `gh_login` without changing `cntrb_id`. If not found, `cntrb_last_search_attempted_at` is stamped and the row is skipped for the cooldown window (30 days).

GitLab users follow the same pattern with `gl_id`, but the GitLab `/users/search?email=` endpoint is admin-only on gitlab.com, so search-resolve produces no GitLab hits in practice.

### Why is `cntrb_id` random instead of the deterministic UUID?

The contributor was created when no `gh_user_id` (or `gl_id`) was known — typically from a commit author with no platform account observation. Per [R2](#r2-identity-key-immutability), the `cntrb_id` does NOT migrate to the deterministic UUID even if a `gh_user_id` is later discovered (search-resolve backfills `gh_user_id` on the same random-UUID row). Two columns side by side: random `cntrb_id`, populated `gh_user_id`. This is correct.

### Why does `cmt_author_platform_username` differ from `cntrb_login` for the same author?

The commit-author resolver may set `cmt_author_platform_username` to the resolved platform login at the time of resolution. If the platform user later renames, `cntrb_login` updates but `cmt_author_platform_username` stays at the historical value (it represents "who authored the commit at the time of authoring"). Use `cmt_ght_author_id` (the `cntrb_id`) for current-identity joins.

### Why does my query show contributors with no commits?

Contributors are created lazily from any UserRef — issue reporter, PR author, message author, etc. — not just commit authors. A contributor who only opened an issue but never authored a commit is legitimate.

---

## Diagnostic queries

Run these against the production DB (see operator-private docs for connection details). All are read-only.

```sql
-- 1. Counts by data quality bucket
SELECT
  count(*) FILTER (WHERE gh_user_id IS NOT NULL) AS with_gh_user_id,
  count(*) FILTER (WHERE gl_id IS NOT NULL) AS with_gl_id,
  count(*) FILTER (WHERE gh_user_id IS NULL AND gl_id IS NULL) AS email_only,
  count(*) FILTER (WHERE cntrb_canonical = '' AND gh_login != '') AS gh_login_no_canonical,
  count(*) FILTER (WHERE cntrb_company = '' AND cntrb_location = '') AS thin,
  count(*) AS total
FROM aveloxis_data.contributors;
```

```sql
-- 2. Find duplicate-by-gh_user_id (R3 rename casualties or worse)
SELECT gh_user_id, count(*) AS dup_count, array_agg(cntrb_login ORDER BY data_collection_date DESC)
FROM aveloxis_data.contributors
WHERE gh_user_id IS NOT NULL
GROUP BY gh_user_id
HAVING count(*) > 1
ORDER BY dup_count DESC
LIMIT 20;
```

```sql
-- 3. Identity rows pointing at multiple cntrb_ids (FK-orphan risk)
SELECT platform_id, platform_user_id, count(DISTINCT cntrb_id) AS distinct_parents
FROM aveloxis_data.contributor_identities
GROUP BY platform_id, platform_user_id
HAVING count(DISTINCT cntrb_id) > 1;
```

```sql
-- 4. Aliases pointing at multiple cntrb_ids (data-quality bug)
SELECT alias_email, count(DISTINCT cntrb_id)
FROM aveloxis_data.contributors_aliases
GROUP BY alias_email
HAVING count(DISTINCT cntrb_id) > 1;
```

```sql
-- 5. Enrichment progress: how many rows past cooldown still need a pass?
SELECT
  count(*) FILTER (WHERE cntrb_last_enriched_at IS NULL) AS never_enriched,
  count(*) FILTER (WHERE cntrb_last_enriched_at < NOW() - INTERVAL '30 days') AS past_cooldown,
  count(*) FILTER (WHERE cntrb_last_enriched_at >= NOW() - INTERVAL '30 days') AS within_cooldown
FROM aveloxis_data.contributors
WHERE cntrb_login != '' AND cntrb_company = '' AND cntrb_location = '';
```

```sql
-- 6. Search-resolve queue depth
SELECT count(*) AS candidates
FROM aveloxis_data.contributors
WHERE cntrb_email != ''
  AND gh_user_id IS NULL
  AND cntrb_email NOT LIKE '%noreply%'
  AND (cntrb_last_search_attempted_at IS NULL
       OR cntrb_last_search_attempted_at < NOW() - INTERVAL '30 days');
```

```sql
-- 7. Commits with unresolved authors per repo (Layer 2 progress)
SELECT r.repo_owner || '/' || r.repo_name AS repo,
       count(*) AS unresolved
FROM aveloxis_data.commits c
JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id
WHERE c.cmt_author_platform_username IS NULL OR c.cmt_author_platform_username = ''
GROUP BY r.repo_owner, r.repo_name
ORDER BY unresolved DESC
LIMIT 20;
```

```sql
-- 8. FK integrity check: child rows referencing missing contributors
-- (Should always return 0 if R10 holds.)
SELECT 'commits' AS tbl, count(*)
FROM aveloxis_data.commits c
LEFT JOIN aveloxis_data.contributors ct ON ct.cntrb_id = c.cmt_ght_author_id
WHERE c.cmt_ght_author_id IS NOT NULL AND ct.cntrb_id IS NULL
UNION ALL
SELECT 'issues', count(*)
FROM aveloxis_data.issues i
LEFT JOIN aveloxis_data.contributors ct ON ct.cntrb_id = i.reporter_id
WHERE i.reporter_id IS NOT NULL AND ct.cntrb_id IS NULL
UNION ALL
SELECT 'pull_requests', count(*)
FROM aveloxis_data.pull_requests p
LEFT JOIN aveloxis_data.contributors ct ON ct.cntrb_id = p.author_id
WHERE p.author_id IS NOT NULL AND ct.cntrb_id IS NULL;
```

---

## Intentional limitations

These are accepted trade-offs, NOT bugs. Operators should be aware before filing issues.

### The rename edge case (largely closed by v0.20.2)

Per [R3](#r3-cntrb_login-partial-uniqueness), a platform user who renames between observations historically produced two contributor rows when the new login was already observed under a different `cntrb_id`. v0.20.2 added a logical-merge path in `LinkContributorToGitHubUser` that resolves these duplicates the next time `runSearchResolve` processes the relevant email — see R3 for the full description.

The remaining residual case: a duplicate where the loser's email never gets re-observed by search-resolve (e.g., the email is private, or the user has set noreply). Those duplicates persist until either the email surfaces or an operator manually invokes the merge. For this rare case, a coalesce query is still useful:

```sql
-- Coalesce duplicate rows by gh_user_id, picking the most recently collected
SELECT DISTINCT ON (gh_user_id) cntrb_id, gh_user_id, cntrb_login, ...
FROM aveloxis_data.contributors
WHERE gh_user_id IS NOT NULL
ORDER BY gh_user_id, data_collection_date DESC;
```

### Random-UUID rows for email-only contributors

Per [R1](#r1-identity-key-determinism), commit authors with no resolvable platform account get a random `cntrb_id`. Per [R2](#r2-identity-key-immutability), this UUID is not later migrated even if `gh_user_id` is discovered. Operators should not rely on `cntrb_id` byte-equality for joins across databases for the email-only subset.

### GitLab parity gaps

GitLab does not expose all the fields GitHub does. CLAUDE.md's "GitHub/GitLab Parity Gaps — Closure Plan" section enumerates the accepted limitations:

- **Watcher count** — no GitLab equivalent; `star_count` is the closest analog but semantically different.
- **GraphQL node IDs** (`gh_node_id`) — GitLab uses numeric project IDs only; `SrcRepoID` numeric serves the same purpose.
- **Contributor identity URL fields** (`gh_followers_url`, `gh_starred_url`, etc.) — GitHub-specific denormalized fields with no GitLab equivalent.
- **Search-by-email on GitLab** — admin-only on gitlab.com; `runSearchResolve` produces no GitLab hits in practice.

### Cooldown windows are 30 days, not configurable

Per [R6](#r6-enrichment-is-best-effort-cooldown-bounded), `cntrb_last_enriched_at` and `cntrb_last_search_attempted_at` use a hard-coded 30-day cooldown. A user who unlocks their public profile within the cooldown window will not have their `cntrb_company` / `cntrb_location` updated until the window expires. The cooldown was chosen empirically to balance freshness against API quota.

### Search-resolve is GitHub-only, low-yield

`runSearchResolve` only uses GitHub's `/search/users?q=email+in:email` because GitLab's equivalent endpoint is admin-only. Even on GitHub, success rate is moderate (~20–40% depending on the email cohort) because users frequently set email to private. The task is intentionally low-rate (100 candidates/hour) to stay within the 30/min/token search-API budget.

### The cache is per-job, not fleet-wide (today)

Per [R7](#r7-cached-resolution-on-the-hot-path), the `ContributorResolver` cache is scoped to one repo's collection. Two workers processing different repos that share contributors each pay the cache-miss SELECT. Phase C of the refactor plan lifts the cache to fleet scope — track that work in `summary/04-refactoring-plan.md`.

---

## GitLab vs GitHub: column-by-column parity matrix (v0.20.3)

Aveloxis collects contributor data from both GitHub and GitLab and stores them in the same `aveloxis_data.contributors` table. Some columns map cleanly between platforms; others are intentionally GitHub-only or GitLab-only. This matrix is the contract for what to expect when querying contributor data on a mixed-platform fleet.

| GitHub column | GitLab column | API source | Status |
|---|---|---|---|
| `gh_user_id` | `gl_id` | GitHub `/user`, `/users/{login}` ; GitLab `/user`, `/users?username=` | ✓ both populated (`ContributorIdentity.UserID`) |
| `gh_login` | `gl_username` | same | ✓ both populated (`ContributorIdentity.Login`) |
| `gh_url` / `gh_html_url` | `gl_web_url` | same | ✓ both populated (`ContributorIdentity.URL`) |
| `gh_avatar_url` | `gl_avatar_url` | same | ✓ both populated (`ContributorIdentity.AvatarURL`) |
| `cntrb_full_name` | `cntrb_full_name` (also `gl_full_name`) | same | ✓ both populated (`ContributorIdentity.Name`) |
| `cntrb_email` | `cntrb_email` | GitHub `/user.email` (public only) ; GitLab `/users.public_email` | ✓ both populated when available; private-email users get `''` on either platform |
| `cntrb_company`, `cntrb_location` | same | both via the enrichment endpoint | ✓ both populated when set on the user profile |
| `gh_site_admin` | (gl_state implies isAdmin via `access_level >= 50`) | GitHub `/user.site_admin` ; GitLab project member `/access_level` | ≈ approximate. GitLab "Owner" role on a project maps to admin; the deployment-wide "is admin" GitHub field has no GitLab single-flag equivalent. Stored as a stringified bool on `gh_site_admin`; for GitLab the equivalent is implicit in `IsAdmin` at the identity row. |
| `gh_state` | `gl_state` | (aveloxis-internal — set to `'unresolved'` for placeholder rows; otherwise empty) | **Added in v0.20.12 to mirror gl_state.** GitHub's `/user` endpoint doesn't expose account-state lifecycle, so `gh_state` is NOT populated from API data. Its only writer today is the v0.20.12 placeholder backfill, which sets `gh_state = 'unresolved'` for contributor rows synthesized from `commits.cmt_author_platform_username` when no API resolution was possible. Analysts filter these out with `WHERE COALESCE(gh_state,'') NOT IN ('unresolved')`. The `'unresolved'` flag means "we observed this login in commit data but never got a profile from the API" — it's NOT a claim that the user was deleted, suspended, or renamed (we can't tell those apart from a single 404). |
| (none) | `gl_state` | GitLab `/users/.state` ("active", "blocked", "banned", "deactivated") | **GitLab-only field, populated as of v0.20.3.** Useful for filtering blocked/deactivated users out of contributor analytics. GitHub has no equivalent — its `/user` endpoint doesn't expose account-state lifecycle. |
| `gh_node_id` | (none) | GitHub GraphQL global node ID | accepted limitation. GitLab uses numeric project/user IDs; there's no GraphQL globally-unique node ID. `cntrb_id` (deterministic UUID) plays the same role at the aveloxis layer. |
| `gh_type` (User / Bot / Organization) | (none) | GitHub `/user.type` | accepted limitation. GitLab doesn't classify user accounts as Bot vs Organization at this granularity. Bot detection on GitLab data uses email patterns / heuristics. |
| `gh_gravatar_id` | (none — `avatar_url` already returns gravatar) | GitHub `/user.gravatar_id` | accepted limitation. GitLab's `avatar_url` is a complete URL that already includes gravatar where applicable; there's no separate id. |
| `gh_followers_url`, `gh_following_url`, `gh_gists_url`, `gh_starred_url`, `gh_subscriptions_url`, `gh_organizations_url`, `gh_repos_url`, `gh_events_url`, `gh_received_events_url` | (none) | GitHub `/user.{field}` | accepted limitation (8 fields). These are GitHub REST hypermedia links; GitLab's REST API uses path-based URLs derived from `gl_web_url`. Aveloxis stores them denormalized for backward-compatible Augur queries; downstream tools that need GitLab-side equivalents construct them as `gl_web_url + "/<segment>"`. |

### Querying contributor data on mixed-platform fleets

When you need a single value across both platforms, prefer the platform-agnostic `cntrb_*` columns over the platform-specific `gh_*` / `gl_*` ones. The `cntrb_login`, `cntrb_email`, `cntrb_company`, `cntrb_location`, `cntrb_full_name` fields are populated regardless of platform and are the recommended targets for analytics queries.

Use the platform-specific columns when you specifically need the GitHub or GitLab perspective — e.g., `gh_user_id` to join against externally-collected GitHub data, or `gl_state` to filter out blocked GitLab accounts.

### Closable gaps that aren't planned

The following GitHub fields don't have a GitLab equivalent we plan to add. They appear above as "accepted limitation":

- `gh_node_id`: GitLab has no GraphQL globally-unique node ID system. Numeric `gl_id` serves the same purpose at the GitLab side; `cntrb_id` (deterministic UUID per R1) serves the cross-platform role.
- `gh_type`: GitLab user-account taxonomy is binary (User vs not-a-User); the Bot/Organization distinction GitHub exposes doesn't exist there.
- `gh_followers_url` etc.: GitLab's REST URL scheme is path-derivable from `gl_web_url`. Storing duplicates would just be aveloxis copying a hyperlink that's reconstructable.

These are documented for transparency, not blocking issues. Closing them would require fabricating data — which would be worse for analyst trust than leaving the cells empty.

---

## Related code

| Function | File | Role |
|---|---|---|
| `ContributorResolver.Resolve` | `internal/db/contributors.go` | Layer 1: per-UserRef resolution. Three-tier lookup + branched ON CONFLICT INSERT. |
| `UpsertContributorBatch` | `internal/db/postgres.go` | Layer 1: bulk path with in-memory dedup; mandated by R8. |
| `UpsertContributorFull` | `internal/db/commit_resolver_store.go` | Layer 2: commit-resolver path with 23505 fallback. |
| `LinkContributorToGitHubUser` | `internal/db/contributor_search_resolve.go` | Layer 3: search-resolve link path; pinned by R11 not to modify identity. |
| `ResolveEmailToIdentity` / `ResolveEmailViaAPI` | `internal/collector/email_identity_resolver.go` | The shared email→identity chain used by commit + mailing-list resolution. |
| `EnrichThinContributors` | `internal/collector/enrich.go` | Layer 3: periodic enrichment ticker handler. |
| `CommitResolver.ResolveCommits` | `internal/collector/commit_resolver.go` | Layer 2: per-job commit-author chain (calls `ResolveEmailViaAPI` as step 7). |
| `MailingListProcessor` | `internal/collector/mailinglist_processor.go` | Drains staged mailing-list messages; resolves senders via `ResolveEmailToIdentity`. |
| `ParseNoreplyEmail` | `internal/collector/noreply.go` | R12 strategy 3 / shared-chain strategy 1. |
| `PlatformUUID` / `GithubUUID` / `GitLabUUID` | `internal/db/github_uuid.go` | R1 deterministic encoding. |

---

## Next steps

- [Facade Commits](facade-commits.md) — how git log data feeds Layer 2.
- [Staged Pipeline](staged-pipeline.md) — how staging feeds Layer 1.
- [Mailing-list ingestion](mailing-list.md) — how mailing-list senders feed Layer 3 (the third data origin).
- [Overview](overview.md) — system architecture.
