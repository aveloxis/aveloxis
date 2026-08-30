# Mailing-List Ingestion (v0.25.7+)

Mailing-list archives are collected by the MailingListWorker, a dedicated decoupled worker pool inside `aveloxis serve`. It ingests email from project mailing-list archives into the canonical Aveloxis tables, classifies each message, and — where the email corresponds to an issue, pull request, or review — projects it onto those entities. Off by default; opt in via `collection.mailing_list_enabled = true`.

This document explains *why* the system looks the way it does, what the data is good for, and how to interpret it. Operator-facing tuning lives in [`docs/getting-started/configuration.md`](../getting-started/configuration.md); the CLI is in [`docs/guide/commands.md`](../guide/commands.md). The full design rationale (sampling, migration patterns, decision log) is in `summary/10-apache-history-ingestion.md` and `summary/11-apache-mailing-list-implementation-plan.md`.

## 1. The question this subsystem answers

A great deal of open-source project activity never appears in the GitHub/GitLab API: design discussion, release votes, patch review on lists that predate (or replace) pull requests, and the Jira/Bugzilla issue history of projects that migrated to GitHub issues. The MailingListWorker recovers that activity.

Every email becomes a first-class **`email_message`** entity (a peer to `issues`, `pull_requests`, and `pull_request_reviews`), with its body stored in the shared `messages` table via an `email_message_ref` bridge — the same unified-message architecture every other text source uses. Classification then **routes** the message onto the canonical home it belongs in:

- an issue-tracker notification → linked to an `issues` row;
- a patch submission (kernel-style `[PATCH]`) → a pull-request-equivalent;
- a `Reviewed-by:` / review reply → a pull-request-review-equivalent;
- everything else (votes, announcements, discussion, support) → stays a standalone `mailing_list_only` message.

### 1.1 Two orthogonal axes

The design keeps two questions separate (this is the single most important thing to understand about the subsystem):

- **Axis A — message class**: *what kind of email is this?* (issue notification, patch, review, vote, announcement, discussion, …). Stored in `email_message.msg_class`.
- **Axis B — repo association**: *which repo/PMC does it concern?* Stored as the `signaled_repo_url` / `signaled_repo_id` pair (see §6).

A message can be classified (Axis A) without resolving to a known repo (Axis B), and vice-versa. The leftovers on each axis are explicit: `mailing_list_only` / `unclassified` on Axis A, an unresolved `signaled_repo_id` on Axis B. Nothing is silently dropped.

## 2. Why a dedicated worker

The work is independent of every per-repo collection phase, and follows the same decoupled-pool pattern v0.21.0 applied to scancode and v0.24.0 to the DistributionWorker:

- **No API tokens.** Apache Pony Mail is unauthenticated; the kernel public-inbox path is a `git clone`. The optional `polite_email` only sets a contact header.
- **Its own cadence.** Lists are tail-refreshed on a 30-day default cadence — much slower than main collection — and a one-time full-history backfill runs when a list has no checkpoint.
- **Its own claim queue.** Lists are claimed from `aveloxis_data.repo_groups_list_serve` (the `mlls_*` columns), entirely separate from the repo `collection_queue`. Enabling the subsystem does not slow per-repo collection.

## 3. `email_message` — the first-class entity

```
email_message  ──email_message_ref──▶  messages   (body, platform_id = 6)
     │
     ├─ signaled_repo_id      ──▶ repos        (which repo it concerns, Axis B)
     ├─ linked_issue_id       ──▶ issues       (routed: issue-tracker mail)
     ├─ linked_pull_request_id──▶ pull_requests(routed: patch / PR-equivalent)
     └─ thread_root_id                          (threading: In-Reply-To / References)
```

`platform_id = 6` (`'Mailing List'`) tags every message-table row sourced from a list. Metadata convention on each row: `data_source` = the specific list address (e.g. `dev@kafka.apache.org`), `tool_source` = `"Aveloxis Mailing List Collector"`, `tool_version` = the release, `data_collection_date` = load time.

## 4. The two archive backends

Backends implement a common `ArchiveSource` interface (`Name`, `EnumerateLists`, `FirstMonth`, `FetchMonth`), so adding a third archive system is a config + one-file job. The worker spawns one runner pool per registered system.

### 4.1 Apache Pony Mail (`apache_ponymail`)

The Foal API at `lists.apache.org`:

| Endpoint | Use |
|---|---|
| `mbox.lua?list=…&date=YYYY-MM` | bulk monthly mbox download (the data path) |
| `preferences.lua` | per-domain list catalog (enumeration) |
| `stats.lua?list=…&domain=…&d=lte=1d` | list-level `firstYear`/`firstMonth` for the full-history backfill |

The mbox stream is parsed mboxrd, with MIME multipart + quoted-printable/base64 decoding. A 404 is a clean empty-month miss; 429 → rate-limited (feeds the Pacer); 5xx / transport → transient (feeds the Breaker).

> **`stats.lua` window matters.** `firstYear`/`firstMonth` are list-level metadata returned for *any* date window. `FirstMonth` therefore uses the cheapest window (`d=lte=1d`, ~1 s / ~50 KB). An earlier `d=lte=30y` forced Pony Mail to aggregate the list's entire history and stream back every message (~18 MB / ~35 s on a busy list), which timed out the worker. Regression tripwire: `TestPonyMailFirstMonthUsesCheapWindow`.

### 4.2 lore.kernel.org public-inbox (`lore_public_inbox`)

lore's HTTP surface is Anubis-gated, so the sanctioned bulk path is a **bare `git clone`** of the per-list public-inbox archive (`https://lore.kernel.org/<list>/git/0.git`). `FetchMonth` walks the archive's commits within the month window and reads each message from the `m` blob (`git cat-file -p <hash>:m`). Enumeration returns nil (the catalog isn't machine-listable under Anubis); kernel lists are curated via `register-mailing-list`.

## 5. Classification

`internal/mailinglist/systems.yaml` defines per-system ordered rules (subject regex, body URL, sender, List-Id, list-address) compiled at load. `System.Classify(msg)` returns the first match as a `(class, source, captures)` triple. The eleven classes:

| Class | Routes to | Typical source |
|---|---|---|
| `issue_event` | `issues` (via `external_key`) | `jira@`, issue-tracker notification mail |
| `patch_submission` | `pull_requests` | kernel `[PATCH]` submissions |
| `review` | `pull_request_reviews` | `Reviewed-by:` / review replies |
| `github_mirror` | linked issue/PR (mirror) | `github@` notification lists |
| `commit_notify` | (metadata) | `commits@` push notifications |
| `vote` `announce` `result` `discuss` `support` `unclassified` | `mailing_list_only` | human discussion, votes, releases, Q&A |

`captures` carries structured data the rule extracted — e.g. `{external_key: "KAFKA-20167"}` (the Jira key, used to bridge to an issue) or `{repo: "arrow-rs"}` (the repo signal, used for Axis B).

## 6. Sender and signaled-repo resolution

### 6.1 Sender identity (§5d)

Sender resolution happens at **drain time in the `MailingListProcessor`** (not in the fetch worker — see §8.1). For the inline stamp on the `messages` row the Processor does a DB lookup (`ResolveContributorIDByEmail`), cached per-list so a recurring sender is resolved once. Senders that don't resolve from the DB keep their `sender_email` and are retried two ways as the contributors table fills: the existing `BackfillMailingListSenderIDs` ticker (hourly DB re-lookup), and the v0.25.x `runMailingListSenderResolve` ticker, which runs senders with ≥ a message threshold through the **shared `ResolveEmailToIdentity` chain** (noreply → bot → DB → GitHub Search → GitHub **global commit-search**) — the same chain the commit resolver uses. Global commit-search is the load-bearing step: list senders are largely committers, so a sender who keeps their profile email private still resolves via a public commit they authored anywhere on GitHub. See [Contributor Resolution → Shared email→identity resolution](contributor-resolution.md#shared-emailidentity-resolution).

### 6.2 Signaled repo — two columns, never block

Which repo a message concerns is captured as a pair:

- **`signaled_repo_url`** — the canonical repo URL extracted from the message's signal (a bot/mirror `[repo]` bracket, a `github.com/owner/repo` body URL, a `GH-NNNNN` key). Captured **even if that repo isn't in the catalog**.
- **`signaled_repo_id`** — the FK to `repos`, filled in **only once the URL resolves to a repo we hold**.

Resolution is **bidirectional and non-blocking**: mail-side at write time via `FindRepoByURL`; repo-side when a new repo is created, by sweeping `email_message` (`ResolveSignaledRepoForURL`). An unresolved `signaled_repo_id` means "we captured a real signal pointing at a repo we don't have loaded" (e.g. Arrow's `github@` list naming `apache/arrow-rs` when only `apache/arrow` is tracked) — not a defect. Tracking the whole org (`load-foundation-orgs`) drives resolution toward 100%.

## 7. Schema (v0.25.7)

```sql
-- platforms gains row 6
INSERT INTO aveloxis_data.platforms (platform_id, ...) VALUES (6, 'Mailing List', ...);

-- email_message: the first-class entity (declared AFTER issues/pull_requests/
-- messages in schema.sql — it FK-references all three; see the ordering tripwire)
CREATE TABLE aveloxis_data.email_message ( ... );
CREATE TABLE aveloxis_data.email_message_ref ( ... );  -- bridge to messages

-- issues gains an external key for Jira/Bugzilla import correlation
ALTER TABLE aveloxis_data.issues ADD COLUMN external_key TEXT DEFAULT '';
-- partial unique: (repo_id, external_key) WHERE external_key <> ''

-- repo_groups_list_serve gains the claim/checkpoint/lock columns
mlls_system, mlls_last_month, mlls_scan_complete,
mlls_failed_attempts, mlls_last_failed_at, mlls_last_run,
mlls_locked_at, mlls_locked_pid, mlls_locked_boot_id
-- + UNIQUE (repo_group_id, rgls_email)
```

Per-column documentation is in [`docs/schema.md`](../schema.md). See [`docs/contributing/schema-migrations.md`](../contributing/schema-migrations.md) for the table-ordering rule that the v0.25.9 Phase 4 run surfaced (an FK-bearing table must be `CREATE`d after its referenced tables, since `schema.sql` runs as one transaction).

## 8. Worker architecture

```
        aveloxis_data.repo_groups_list_serve.mlls_*
                          ▲
              ┌───────────┼────────────┐
              ▼                        ▼
        ClaimNextList            CheckpointListMonth /
   (FOR UPDATE SKIP LOCKED)      CompleteListScan / RecordListFailure
              │                        ▲
              ▼                        │
        ┌───────────┐  per-system ┌─────────┐
        │ dispatcher │──jobs chan─▶│ runner  │
        │ per system │             │  pool   │
        └───────────┘             │ (N=2)   │
                                  └─────────┘
                                       │  claim→fetch→classify→STAGE→checkpoint
                                       ▼
                              ArchiveSource (Pony Mail | public-inbox)
```

- **Claim**: `ClaimNextList(system, cadence, pid, bootID)` acquires a list with `FOR UPDATE SKIP LOCKED`, gated on cadence and on stale-lock recovery (`MailingListStaleLock = 2h` — a lock older than that is presumed dead; the `(pid, boot_id)` stamp is informational, liveness is age alone). `RecoverStaleListLocks` runs at startup and clears locks past the same window. A scan interrupted by `stop serve` releases its own lock on the way out (`ReleaseListLock`, v0.28.18 — keyed on the claim's own `mlls_locked_at` stamp, which is unique per claim in every topology where `(pid, boot_id)` is not, on a bounded background context the scheduler waits for before closing the pool) so the list is reclaimable on restart; if that release loses (a hung database at shutdown), the stale window is the fallback. The month checkpoint resumes it either way. `CompleteListScan` / `RecordListFailure` predate the ownership predicate: a scan that runs past the 2 h window and is reclaimed elsewhere can still be closed out by its first holder — a documented gap, bounded by the staging table's `ON CONFLICT DO NOTHING`.
- **Checkpoint**: each completed month stamps `mlls_last_month` via `CheckpointListMonth`, so an interrupted scan resumes from where it stopped rather than re-fetching.
- **Months to scan**: from `mlls_last_month` forward to the current month; for a never-scanned list, from `FirstMonth` (full history) when `mailing_list_backfill_months <= 0`, else the recent N-month window.
- **Failure backoff** (v0.21.4 quadratic, base 120s): `RecordListFailure` schedules 2m → 8m → 18m → … and sidelines the list after `MailingListMaxFailures = 10` consecutive failures.

### 8.1 Staging split (v0.25.x)

The pipeline is split into a **fetch** half and a **resolve+write** half across a staging table, for the same reason the API pipeline stages: doing per-message sender-resolution + hot-table writes inline (on every fetched message, across concurrent list runners) reproduces Augur's lock contention on `contributors` / `issues` / `pull_requests`. The split keeps the fetchers off the hot tables.

- **`MailingListWorker`** (fetch half): claim → fetch a month → classify each message (cheap, no DB) → **stage** the classified envelope into `aveloxis_ops.mailing_list_staging` → checkpoint. It never touches the hot tables.
- **`MailingListProcessor`** (resolve+write half): drains `mailing_list_staging` **one list at a time, single-threaded** (`mailing_list_processor_workers`, default 1 — `>1` only fans out across *distinct* lists via an in-process per-list guard). Per drained message it resolves the repo (once per list, from the staged `repo_group_id`), resolves the sender (per-list cached), resolves mirror-links / signaled-repo, and writes `email_message` + (for non-mirror) `messages` + `email_message_ref`.
- **Deferral**: a list whose `repo_group` has no repo yet is **left staged** (`messages.repo_id` is `NOT NULL`); it drains automatically once `load-foundation-orgs` / DOAP-enrichment populates the group. `aveloxis mailing-list-stats` surfaces these stuck lists. The hourly staging sweep is `processed`-gated, so undrained rows are never purged.

## 9. Mirror handling

`collection.mailing_list_mirror_handling` controls what happens to mirror-class mail (`github_mirror`) — notification lists that merely echo GitHub activity Aveloxis already collects via the API:

| Value | Behavior |
|---|---|
| `skip` | drop mirror mail entirely (the API copy is authoritative) |
| `metadata_only` (default) | record the `email_message` row + link, but don't duplicate the body into `messages` |
| `full` | store everything, including the body |

The default avoids wholesale-duplicating GitHub data into a second form while keeping the linkage and timeline.

### How a mirror message is linked (v0.28.20)

The link key is the **GitHub GraphQL node ID**, recovered from the
Message-ID. Apache's GitBox relay uses it as the local part
(`PR_kwDOBCyuKc8AAAABBMldbw@gitbox.apache.org`); replies append a UUID,
which is stripped by its exact 8-4-4-4-12 shape — never by cutting at the
first dash, since node IDs are base64url and may legitimately contain one.
`mailinglist.NodeIDFromMessageID` does the extraction and
`ResolveMirrorLinkByNodeID` joins it to `pull_requests.node_id` /
`issues.node_id` (both indexed since v0.28.20).

The node ID is preferred over the body-URL captures
(`owner`/`repo`/`kind`/`number`) that the systems.yaml body rule can
supply, for three reasons: it is an exact platform identifier rather than
a number matched within a guessed owner; it is available under
`metadata_only`, which stores no body at all; and in practice the body
rule does not fire on Apache mail — a 2026-08-29 production audit found
**0 of 396,809** mirror rows carried its captures, which is why
`linked_issue_id` / `linked_pull_request_id` were NULL on every one of
them before v0.28.20. The body-URL path remains as the fallback for
relays whose mail does carry a canonical URL.

Legacy `MDExO…` node IDs are deliberately not matched: they carry no type
prefix, so accepting them could key a row onto an unrelated entity. A
message whose referenced issue/PR is not in the catalog is left NULL
rather than guessed. Historical rows are repaired by
`scripts/heal_mirror_links.sh`, which uses the same key.

## 10. Operator CLI

```bash
aveloxis load-foundation-core-repos      # one core repo per project (was: import-foundations)
aveloxis load-foundation-orgs --yes      # track the foundation's GitHub org(s) for repo discovery
aveloxis load-apache-lists               # register per-PMC dev@/users@ lists via enumeration
aveloxis register-mailing-list \         # register one list (any system, e.g. the kernel)
    --system lore_public_inbox --list linux-pci@vger.kernel.org --repo https://github.com/torvalds/linux
aveloxis backfill-issue-external-keys    # populate issues.external_key from [KEY-N] title prefixes (conflict-safe)
aveloxis backfill-mailing-list-projection # project existing issue_event mail → issues, in place (Phase 5)
aveloxis mailing-list-stats              # coverage rollup (+ missed-LINK shadow guard)
aveloxis verify-mailing-list [--strict]  # Phase 4 branch-coverage harness (§12)
```

See [`docs/guide/commands.md`](../guide/commands.md) for full flag references. The REST rollup is `GET /api/v1/mailing-list/stats` ([`docs/guide/api.md`](../guide/api.md)).

## 11. Config knobs

All under `collection` in `aveloxis.json`:

```jsonc
{
  "collection": {
    "mailing_list_enabled": false,            // master switch
    "mailing_list_workers": 2,                // concurrent list runners per system
    "mailing_list_cadence_days": 30,          // tail-refresh cadence
    "mailing_list_backfill_months": 6,        // history window when no checkpoint (<=0 = full history)
    "mailing_list_polite_email": "",          // contact in the User-Agent for archive admins
    "mailing_list_mirror_handling": "metadata_only"
  }
}
```

## 11c. Layer 2 projection — mailing-list → canonical entities (Phase 3)

Layer 1 (every email → `email_message` + body + classification + threading) is universal and lossless. **Layer 2** *additionally* projects a message onto a canonical entity (`issues` / `pull_requests` / `pull_request_reviews`) **only where the mail maps cleanly to how that community operates** — gated by the per-system `projection_policy` in `systems.yaml` (`clean_fit` for Apache; `none` for the forge-less kernel). The processor reads the policy via `System.ProjectionClean()`.

**Analytical purpose:** before this subsystem, Apache projects' issue data was *absent* (Apache tracks issues in Jira/Bugzilla, not GitHub Issues). Projected issues land under the **PMC's GitHub `repo_id`** — `issues` has no `platform_id` column, the repo carries the platform — so they appear in that repo's standard per-repo issue analytics exactly like native issues. Provenance lives in `external_key` + `data_source` (`'JIRA'`) + `tool_source`.

**Phase A (shipped) — `issue_event → issues` link-or-create** (`MailingListProcessor`, drain-time):

1. An `issue_event` message with a parsed `external_key` (e.g. `KAFKA-123`): **LINK** if an issue for that key already exists — matched by `external_key` OR by the bracketed `[KEY]` in a native issue's title (the Apache Jira→GitHub import shape). LINK-by-title **prevents the missed-LINK shadow**: without it, projecting before `backfill-issue-external-keys` would mint a synthetic that squats the key (the UNIQUE index then blocks the native issue from getting it). Else → **CREATE** a synthetic issue (negative, deterministic `platform_issue_id`, idempotent on `(repo_id, platform_issue_id)`).
2. **Thread-inheritance (#1):** once any message in a thread is projected onto an issue, the rest of the thread — human discussion, `Re:` replies, `discuss`-class mail that carries no key — inherits that issue (via `thread_root_id`, cache + `FindIssueForThread`). So the **full email history** attaches, not just the Jira-notification stream.
3. Every projected email is bridged as a comment (`issue_message_ref`); `issues.comment_count` is recomputed so threads show in analytics.
4. `reporter_id` is the resolved sender **only when it is not the `jira@`/bot sender**; real-actor-from-body parsing is a follow-up.
5. `email_message.projected_kind` records the outcome (`issue`/`pr`/`review`/`mailing_list_only`).

**Backfill (Phase 5):** `aveloxis backfill-mailing-list-projection` runs the same projection over `email_message` rows collected before the projection code existed — in place, no re-collection. Three idempotent steps to convergence: keyed projection → thread-inheritance → mark-remaining. `aveloxis mailing-list-stats` surfaces any **missed-LINK shadows** (synthetic issue whose key sits in a native issue's title) for remediation; the conflict-safe `backfill-issue-external-keys` no longer errors on them.

**Sender attribution (Phases 2+4):** senders the DB can't resolve are run through the shared email→identity chain; direct-human senders that still don't resolve get an **email-only contributor** (random `cntrb_id`, `cntrb_email` set) so they're counted and ride the convergence ticker. Bot/relay senders (`jira@`, `git@`, CI) never become contributors.

**Phase B (verified, NOT built) — PR/review synthesis** from `github_mirror` mail. Verification (2026-06-04, summary/12 §3) settled it: `pull_requests.platform_pr_id` stores the GitHub PR **`databaseId`**, but mirror mail carries only the PR **number** — a synthesized PR keyed on the number would *duplicate* the API collector's row rather than merge. Decision: **don't synthesize**; the lever for full Apache PR data is **org collection** (`load-foundation-orgs`) + the existing `github_mirror` **LINK** path (repaired in v0.28.20 — see §9; it was dark until then). `linked_pr_review_id` remains in the schema should a future uncollectable-sibling case justify a number→databaseId resolution step.

For `projection_policy: none` (kernel): none of the above runs — a `[PATCH]` is not a PR; Layer 1 is the faithful record.

## 11d. Forge-less PR-equivalents — the special case (Phase C)

Some communities — most notably the **Linux kernel** (lore.kernel.org,
`projection_policy: none`) — do code review **entirely by email**. A `[PATCH]`
thread *is* the pull request; the `Re:` replies *are* the review. There is no
forge, and therefore no `pull_requests` / `pull_request_reviews` entity to
project onto.

**The special case, stated plainly:** Aveloxis deliberately does **NOT**
synthesize `pull_requests` rows for these. Fabricating a "PR" for a community
that doesn't use one misrepresents how it works and would pollute the real PR
tables (the §1 governing principle: project only where it's a clean, faithful
fit). The faithful record is the `email_message` rows themselves —
`msg_class IN ('patch_submission','review')` + `thread_root_id` grouping.

To make that ergonomic without a fake forge entity, Phase C ships a **read-only
VIEW**, `aveloxis_data.mailing_list_pr_equivalents`, that groups those mail
threads and presents each as a PR-equivalent:

| column | meaning |
|---|---|
| `thread_key` | the patch-series identity (cover letter `[PATCH 0/N]` or a standalone `[PATCH]`) |
| `repo_id` / `list_address` | the registered repo + list |
| `title` / `author_email` / `author_cntrb_id` | from the series root patch (author resolved to a contributor when known) |
| `created_at` / `last_activity_at` | first / last message in the thread |
| `patch_count` / `review_count` / `participant_count` | thread aggregates |
| `source` | always `'mailing_list'` — the explicit "this is mail-derived, **not** a forge PR" label |

Two properties worth knowing:

- **It is a plain VIEW** — zero storage, never materialized/refreshed, and
  intentionally absent from the matview refresh list. Querying it always
  reflects current `email_message` data.
- **It is empty until forge-less lists are collected.** The filter
  `msg_class IN ('patch_submission','review')` is itself the forge-less gate:
  the 2026-06-03 survey found Apache produces **zero** of these classes (it is
  GitHub-PR-native), so the view contains only kernel-style mail and stays
  empty until lore/public-inbox lists (`register-mailing-list`) are actually
  collected. That's by design — not a bug.

Analysts who want "PR-like" activity for forge-less projects query this view;
`pull_requests` stays exclusively real forge data.

## 12. Verification (Phase 4) and the collection-ordering caveat

`aveloxis verify-mailing-list` is the branch-coverage harness: it reports, per logic branch, whether the subsystem produced any rows — every `msg_class`, both backends, each routing outcome, threading, signaled-repo resolution, sender resolution, and `external_key` backfill — each marked **PASS / EMPTY / DEFER**. `--strict` exits non-zero if a *required* (mailing-list-native) branch is empty, so it can gate a verification collection.

> **Collection ordering matters.** Five branches are **cross-subsystem** — `bridged-to-issue`, `bridged-to-PR`, `mirror-linked`, `sender-resolved`, and `external_key`. They resolve *inline* only when the linked repos' GitHub issues/PRs/contributors are already present when the mail is written. In a fresh run that collects GitHub and mailing lists *concurrently*, those references stay NULL until the periodic backfills catch up — so the harness reports them as **DEFER** (informational, not gating). To exercise them in a verification run, collect the linked repos' GitHub data **first** (or re-collect the lists afterward), then run `backfill-issue-external-keys` and let the sender-backfill ticker run.

## 13. What the subsystem does NOT do

- **Does NOT re-import data GitHub already has.** Mirror lists default to `metadata_only`; the API copy of a PR/issue is authoritative. The subsystem captures the linkage, not a second body.
- **Does NOT enumerate lore.kernel.org.** The public-inbox catalog is Anubis-gated; kernel lists are curated via `register-mailing-list`.
- **Does NOT classify GitLab list mail specially.** The classifier is system-driven; a GitLab-oriented system definition can be added to `systems.yaml` if needed.
- **Does NOT block on unresolved signals.** An unresolved `signaled_repo_id` or sender is retried later, never an error.
- **Does NOT collect attachments.** Patch *bodies* are parsed for classification (`has_patch`), but binary attachments aren't stored.

## 14. Cross-references

- **Architectural cousins**: [`distribution.md`](distribution.md) and [`scancode.md`](scancode.md) — the same decoupled-pool pattern for different domains.
- **Adding a backend**: [`docs/contributing/adding-a-platform.md`](../contributing/adding-a-platform.md) (the `ArchiveSource` interface follows the platform-extension shape).
- **Source-of-truth files**:
  - `internal/mailinglist/` — `systems.yaml` + classifier, `defensive.go` (Pacer/Breaker), `archive.go` (interface + mbox/RFC822 parse), `ponymail.go`, `publicinbox.go`
  - `internal/collector/mailinglist_worker.go` — fetch→classify→STAGE→checkpoint (the fetch half)
  - `internal/collector/mailinglist_processor.go` — drain staging → resolve sender/mirror/repo → project (Layer 2) → write `email_message`/`messages`/bridges (the resolve+write half)
  - `internal/db/mailinglist_staging_store.go`, `internal/db/mailinglist_projection_store.go`, `internal/db/mailinglist_sender_resolve_store.go`
  - `internal/db/email_message_store.go`, `internal/db/mailinglist_state_store.go`
  - `cmd/aveloxis/{load_foundation_orgs,load_apache_lists,register_mailing_list,backfill_external_keys,mailing_list_stats,verify_mailing_list}.go`
  - `internal/api/server.go` — `handleMailingListStats`
- **Design archive**: `summary/10-apache-history-ingestion.md`, `summary/11-apache-mailing-list-implementation-plan.md`, `summary/12-mailing-list-projection.md`.
