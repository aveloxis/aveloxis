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

The sender email is resolved to a contributor via the same `ResolveContributorIDByEmail` chain the commit resolver uses, and stamped on the `messages` row. Unresolved senders keep their `sender_email` and are retried by a periodic `BackfillMailingListSenderIDs` ticker (hourly) as the contributors table fills from ongoing collection. List senders are largely committers, so resolution improves over time and after the linked repos' GitHub collection completes.

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
                                       │  claim→fetch→classify→resolve→route→checkpoint
                                       ▼
                              ArchiveSource (Pony Mail | public-inbox)
```

- **Claim**: `ClaimNextList(system, cadence, staleLock, pid, bootID)` acquires a list with `FOR UPDATE SKIP LOCKED`, gated on cadence and on stale-lock recovery (`MailingListStaleLock = 2h` — a lock older than that is presumed dead, the v0.21.0 `(pid, boot_id)` recovery shape). `RecoverStaleListLocks` runs at startup.
- **Checkpoint**: each completed month stamps `mlls_last_month` via `CheckpointListMonth`, so an interrupted scan resumes from where it stopped rather than re-fetching.
- **Months to scan**: from `mlls_last_month` forward to the current month; for a never-scanned list, from `FirstMonth` (full history) when `mailing_list_backfill_months <= 0`, else the recent N-month window.
- **Failure backoff** (v0.21.4 quadratic, base 120s): `RecordListFailure` schedules 2m → 8m → 18m → … and sidelines the list after `MailingListMaxFailures = 10` consecutive failures.

## 9. Mirror handling

`collection.mailing_list_mirror_handling` controls what happens to mirror-class mail (`github_mirror`) — notification lists that merely echo GitHub activity Aveloxis already collects via the API:

| Value | Behavior |
|---|---|
| `skip` | drop mirror mail entirely (the API copy is authoritative) |
| `metadata_only` (default) | record the `email_message` row + link, but don't duplicate the body into `messages` |
| `full` | store everything, including the body |

The default avoids wholesale-duplicating GitHub data into a second form while keeping the linkage and timeline.

## 10. Operator CLI

```bash
aveloxis load-foundation-core-repos      # one core repo per project (was: import-foundations)
aveloxis load-foundation-orgs --yes      # track the foundation's GitHub org(s) for repo discovery
aveloxis load-apache-lists               # register per-PMC dev@/users@ lists via enumeration
aveloxis register-mailing-list \         # register one list (any system, e.g. the kernel)
    --system lore_public_inbox --list linux-pci@vger.kernel.org --repo https://github.com/torvalds/linux
aveloxis backfill-issue-external-keys    # populate issues.external_key from [KEY-N] title prefixes
aveloxis mailing-list-stats              # coverage rollup
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
  - `internal/collector/mailinglist_worker.go` — claim→fetch→classify→resolve→route→checkpoint
  - `internal/db/email_message_store.go`, `internal/db/mailinglist_state_store.go`
  - `cmd/aveloxis/{load_foundation_orgs,load_apache_lists,register_mailing_list,backfill_external_keys,mailing_list_stats,verify_mailing_list}.go`
  - `internal/api/server.go` — `handleMailingListStats`
- **Design archive**: `summary/10-apache-history-ingestion.md`, `summary/11-apache-mailing-list-implementation-plan.md`.
