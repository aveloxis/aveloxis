# Human provenance — where identity lives, column by column

Every research claim built on this database ultimately rests on one
question: **who did this, and how do we know?** This page is the
adversarially-reviewed map of every identity-bearing column — who
writes it, from what upstream field, what NULL means, and which heal
advances it. When a column here disagrees with the code, the code wins
and this page is a bug; file it like one.

## The four identity chains

| Chain | Source of truth | Stable key | Written by |
|---|---|---|---|
| Forge (GitHub/GitLab) | The forge's numeric user id | `PlatformUUID(platform, user_id)` → `contributors.cntrb_id` | `ContributorResolver.Resolve`, `UpsertContributorBatch` |
| Git (commits) | Commit author email | email → `contributors_aliases.alias_email` → `cntrb_id` | The commit resolver (4 strategies, R1–R13 contract) |
| Email (mailing lists) | The sender's RFC-822 From address | email/canonical/alias → `cntrb_id` | Drain-time resolve + the sender backfill (`resolve-email-identities`) |
| Jira | The Server-era stable username (`name`) | `jira_identities.jira_name` → `cntrb_id` | `ResolveJiraIdentity` (login → display, unambiguous only) |

Rules that hold everywhere:

- **SR-6 — never fabricate identity.** A link is written only on an
  unambiguous match or a deliberate, labeled mint. Ambiguity stays
  NULL with the raw identity preserved.
- **Automation never gets an identity.** `collector.IsAutomationEmail`
  and its SQL twin `aveloxis_data.is_automation_email` gate every
  attribution path (bots, the Jira/GitBox relays, list addresses).
  Ungated minting once handed 83,746 messages to a `jira@apache.org`
  phantom contributor; the ledgered heal
  (`v0.29.0 heal automation-phantom contributors`) repaired it and the
  gates keep it repaired.
- **Soft-delete only.** A merged/phantom contributor row is marked
  `cntrb_deleted = 1`, never removed (R10 — FK integrity). Every read
  path filters `COALESCE(cntrb_deleted, 0) = 0`.

## Issues — the multi-provider entity

One logical ticket = **one `issues` row keyed `(repo_id,
external_key)`**. Three providers can describe the same ticket, ranked:

| Rank | Provider | May write | data_source |
|---|---|---|---|
| 1 | Forge API | everything on its own rows; untouchable by lower ranks | `GitHub API`, … |
| 2 | Jira API | state, reporter, title, `jira_issue_id` on synthetic rows | `JIRA API` |
| 3 | Mail projection | creates synthetics; state via notification actions, event-time guarded | `JIRA` |

There is **no duplication window by construction**:
`syntheticIssueID(external_key)` is a deterministic negative id, so
mail and the Jira collector minting the same ticket independently
converge on the same row in either arrival order. A real Jira internal
id lives in `issues.jira_issue_id` — synthetics keep the negative
`platform_issue_id` forever (a positive Jira id could collide with a
GitHub `databaseId` under the unique arbiter, and the
projection-duplicate detector keys on the sign).

Identity columns on issues:

| Column | Written by | NULL means |
|---|---|---|
| `reporter_id` | Forge collection (rank 1); Jira backfill/collector (rank 2, synthetic rows); mail projection (rank 3, human senders only — the relay is never a reporter) | Reporter identity not yet resolvable. Jira synthetics: run `backfill-jira-identities`. |
| `closed_by_id` | Forge event derivation (v0.26.5) | No close event observed (or history-capped). |

## Messages — one body table, five origins

`messages.platform_id` + `msg_kind` say what a row IS:

| platform_id | msg_kind | Origin | `cntrb_id` written by | `node_id` holds |
|---|---|---|---|---|
| 1/2 | 1–3 | Forge comments/reviews | The staged processor's resolver | GraphQL node ID |
| 4 | 1 | **Native Jira comment** | `ResolveJiraIdentity` / mint at drain | (empty) |
| 6 | 4 | Mailing-list body | Drain-time resolve + `resolve-email-identities` backfill | **The RFC-822 Message-ID** — the join key to `email_message` |

Email-row specifics:

- `msg_sender_email` is the RAW parsed sender — provenance, not a
  resolution. `cntrb_id` is the resolution; NULL = not yet resolvable
  and the hourly backfill keeps retrying as identities accrue
  (measured: a full pass is ~5–10 minutes; the alias arm alone
  reaches 163K bodies nothing else can).
- `data_source` holds the LIST ADDRESS.
- `msg_text` is raw forever; `msg_text_clean` is the quote-stripped
  variant (email rows only; NULL = read the raw text).

**Duplicate messages across origins are LINKED, not deleted**: a
GitBox mirror mail's native twin is on `email_message.linked_*`
(v0.28.20); a Jira notification's native comment twin is on
`email_message.linked_msg_id` (v0.29.0, stamped at collection time by
the pilot-validated ±2-minute match — in EITHER arrival order: the
Jira collector links the nearest unlinked notification when it stores
a native comment, and the mailing-list projection reverse-links the
nearest unclaimed native comment when a `[Commented]` notification
arrives after Jira collection already ran). Read-time precedence is
always `native > notification`, keyed off those stamps — never off
fuzzy matching at query time. `issues.comment_count` counts LOGICAL
comments: refs whose message is a superseded notification (its
`email_message` row carries `linked_msg_id`) are excluded from the
recount, so a matched pair counts once. The ~7% of notifications
with no native match are the sole record and still count.

## jira_identities — the banked usernames

Every Jira identity observed via the REST API, raw and permanent:
`jira_name` (the Server-era stable username — **absent from Jira
Cloud's API**, which is why this table is collected early),
`jira_user_key` (the internal JIRAUSERnnnn key), `display_name`, and a
nullable `cntrb_id` with `match_method`:

| match_method | Meaning |
|---|---|
| `login` | `jira_name` matched exactly one `gh_login`/`cntrb_login` (pilot: 49.2% of issues) |
| `display` | `display_name` matched exactly one `cntrb_full_name` (+17.6%) |
| `minted` | Jira-only person — a contributor row was minted so networks include them |
| *(empty)* | Unmatched or AMBIGUOUS — preserved raw, linked to nothing (SR-6) |

## contributors_aliases — the email bridge

The one table every email-shaped identity flows through. Four writer
families, each stamping its own `tool_source` (v0.29.0 — previously
every path claimed to be the commit resolver): the commit resolver's
two alias paths, the mailing-list sender-resolve linker, and the
v0.20.2 rename-merge loser insert. `alias_email` is UNIQUE — the
structural bound that keeps attribution fan-out at one contributor per
address.

## Which heal advances what

| Gap | Heal | Cadence |
|---|---|---|
| Unattributed email bodies | `aveloxis resolve-email-identities` (minutes) | plus the hourly ticker (`mailing_list_sender_backfill_interval_minutes`) |
| Unstripped email history | `aveloxis strip-quoted-history` | once; ingest strips forward |
| Permanently-open Jira synthetics | ledgered migrate step (`v0.29.0 backfill synthetic Jira issue state`) | once; the projection + collector keep state forward |
| Jira reporter/comment identity | `aveloxis backfill-jira-identities` | once soon (the Cloud-migration window); the collector keeps it forward |
| Phantom automation contributors | ledgered migrate step (`v0.29.0 heal automation-phantom contributors`) | once; the gates prevent recurrence |
| Mis-drained mailing-list rows (cross-system drain) | ledgered migrate step (`v0.29.0 heal cross-system mis-drained mailing-list rows`) + `aveloxis backfill-mailing-list-projection` | once; the system-scoped drain claim prevents recurrence |

## Validated invariants (Part G, 2026-09-01)

The five-project main-vs-branch validation (tomcat, hbase, kafka, beam,
arrow — full collection + full-history mailing lists + Jira sync on two
scratch databases) closed with these properties holding, now the
documented contract for the identity chains above:

- **I1** — zero duplicate `(repo_id, external_key)` issues in any arrival
  order (mail-then-API and API-then-mail both converge on one row via the
  deterministic negative id).
- **I2** — `platform_issue_id < 0` exactly for mail/Jira-born rows; native
  forge rows never carry a synthetic id.
- **I3** — every Jira-touched issue whose key has conversational mail
  keeps at least one `issue_message_ref` bridge (68,332 of 68,444 API
  issues bridged; the 112 residual are provably mail-less).
- **I4** — `email_message.linked_msg_id` covers 98.8% of comment
  notifications on the overlap window (pilot floor: 94.4%).
- **I5/I6** — no automation-phantom contributors; no message attribution
  points at an automation sender.
- **I7** — mail/Jira state writers never alter a native forge row
  (arrow's 28K native issues byte-stable across sides), and API-owned
  rows only yield to strictly newer mail events
  (`trackerActionEventGuardSQL`).
- **Analytics A/B** — network growth is connective (largest-component
  share 0.97–1.00 on both sides), and text features read the
  quote-stripped bodies via `COALESCE(msg_text_clean, msg_text)`.
  That COALESCE consumer lives in the **aveloxis-analytics**
  repository (`data/sql/features/static/messages_text_stats.sql`,
  pinned by its own tests) — deliberately NOT in this repository:
  no in-repo production surface serves message *text* (the
  Augur-compat message endpoint is count-only, and the strip CLI's
  worklist read is the stripper's raw INPUT). A future in-repo
  text-serving endpoint must read the COALESCE.
