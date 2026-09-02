# PR #193 — "JIRA / Mailing List Updates Plus Plus" (v0.29.0)

**142 files, ~+16K / −0.4K.** Ships the connected email-attribution + Jira
program (Parts A–G of the approved plan), hardened through **18 Copilot review
rounds + 1 fresh-context adversarial round → 69 findings, 68 taken, 1
declined-with-reason**, plus two operator-directed pre-ship additions.

## What it does

**1. Email sender attribution (Part A).** Rewrites the mailing-list sender
backfill from a LIMIT-rescan loop (~31 days) to keyset windows plus a
`contributors_aliases` arm (full pass ≈ 5–10 min), lifting attribution to
~78% of non-relay bodies. New `aveloxis resolve-email-identities` one-shot;
`mailing_list_sender_backfill_interval_minutes` knob with a persistent
cross-tick cursor. Includes the automation-phantom heal — relay addresses
(`jira@apache.org`, `gitbox@`, list addresses) were being minted as
contributors (83,746 messages attributed to the `jira@` phantom).

**2. Quote-stripping (Part B).** New `internal/mailinglist/quotestrip.go`
(rule set `qs-v1` from a 20K-message sweep; median body 4,774 → ~300 chars,
comparable to GitHub comments). Writes `messages.msg_text_clean` at ingest;
`aveloxis strip-quoted-history` walks history.

**3. Jira as a first-class issue provider (Parts C1–C3).** A full subsystem —
minimal Jira client + drift-safe `WalkProjectByUpdated`, three tables
(`jira_project_serve`, `jira_staging`, `jira_identities`), worker + processor +
scheduler wiring (gated behind `jira_enabled`, default off), and CLIs
`register-jira-projects` + `backfill-jira-identities`. The **provider-precedence
contract** is enforced at the writer layer (SR-18): one ticket = one `issues`
row keyed `(repo_id, external_key)`, convergent in either arrival order via a
deterministic negative synthetic id, with **forge > Jira API > mail** field
ownership. Mail-notification actions (`Resolved`/`Reopened`) reach issue state
(zero API calls — Tier-1 ships unconditionally); comment notifications link to
their native twin (`email_message.linked_msg_id`, ±2-min match validated at
94.4% in the pilot).

**4. GitHub-mirror linkage + provenance docs (Parts D–F).**
`scripts/heal_mirror_links.sh` backfills dark `linked_*` columns via the GitBox
Message-ID node ID (now repo-group-scoped, since a Message-ID is
sender-controlled); new `docs/architecture/human-provenance.md`.

Also folded in during hardening: the **CompleteJob shutdown-retry** fix (a 66h
collection that finished during a serve restart was losing its `last_collected`
stamp) and the **GraphQL rate-limit budget** arc from the chaoss.tv pytorch
incident (per-resource key budgets, in-body rotation, background reserve).

## Two operator-directed pre-ship additions

**Deploy-steps gate on `start serve` / `start all`.** This release heals data a
plain restart does not touch, and a missed heal would go unnoticed. New
`aveloxis_ops.deploy_ack` table + a per-version checklist registry: on an
*existing* fleet with un-acknowledged steps, `start serve` / `start all` prints
the checklist and prompts (interactive) or refuses (non-interactive) unless
`--skip-deploy-check`. Fresh installs and already-acknowledged releases start
silently. New commands `aveloxis deploy-checklist` (read-only) and
`aveloxis ack-deploy`.

**Bugzilla-relay automation-email fix.** ASF's `bugzilla@` / `bugzilla-daemon@`
relays send ~193K notifications already in the corpus; the automation predicate
didn't know them, so 4 phantom contributor rows were being minted for them
exactly like the `jira@` phantom. Both twins (Go + SQL) now recognize the
Bugzilla relays, and the ledgered phantom heal catches the live phantoms on the
first v0.29.0 migrate. This is the one pre-release Bugzilla item that had to
land here (the relays are minting phantoms *today*); the full Bugzilla
subsystem is next.

## Schema

3 new tables + `deploy_ack` (147 total). New columns on existing tables:
`issues.jira_issue_id`, `issues.last_mail_event_id`, `messages.msg_text_clean` /
`_rule` / `msg_updated`, `email_message.linked_msg_id`. All idempotent; migrate
stays minutes.

## New config knobs

`jira_enabled` (off) / `jira_workers` / `jira_cadence_hours` /
`jira_polite_email` / `mailing_list_sender_backfill_interval_minutes`.

## Why Jira collection stays OFF on kate (`jira_enabled: false`)

The near-term goal is a coherent mailing-list + attribution pipeline, not
standing up a new high-volume collection subsystem — and turning the Jira API
collector on works *against* that goal for three reasons:

1. **You already get the Jira signal for free from mail.** Tier-1 (state +
   reporter from `[Resolved]`/`[Created]` notifications you're *already*
   collecting) ships unconditionally and needs zero API calls. For the
   analytics you care about, the standing collector adds little on top.
2. **The full subsystem is a large ongoing load for a *decaying* payload.**
   ASF is migrating projects Jira → GitHub; the collector's unique value (real
   comment bodies, the ~358K issues absent from mail) shrinks with every
   migration, while the hourly sync + ~one-polite-day full corpus cost is
   permanent. It's the wrong time to take on that surface.
3. **The one *perishable* thing is captured without the collector.** Jira
   Cloud's API has no stable username (GDPR, 2019), so a Cloud migration makes
   the usernames on the whole 844K-issue corpus unfetchable at once — and those
   usernames are what identity matching runs on. That's banked by the *one-shot*
   `backfill-jira-identities` CLI (~2–3 polite hours), which you can run when
   ready. It is a strict subset of the standing collector, so running it does
   not commit you to turning collection on.

In short: run the identity one-shot if/when you want the username data locked
in; leave `jira_enabled` off — the standing collector is a lot of ongoing cost
for a signal you mostly already have and a payload that's on its way out.

## Bugzilla

Only the pre-ship must-do (the relay automation-email predicate, above) is in
this PR — it stops active phantom-minting. The full generic-tracker subsystem
(mail-primary since ASF Bugzilla is login-walled; `jira_*` → `tracker_*`
generalization; `BZ-<id>` external keys) is planned in `summary/27` and is the
next release. **Decided (2026-09-02):** ship v0.29.0 with `jira_*` naming (keeps the
18-round-hardened surface stable at ship); the `jira_*` → `tracker_*`
generalization and the full Bugzilla subsystem land together in v0.30.0, which
carries the one-time rename migration on the deployed tables.

## Deployment (kate, after merge)

`aveloxis start serve` / `start all` will now *prompt you* with this exact
ladder if the steps aren't acknowledged:

1. `aveloxis stop all`
2. `aveloxis migrate --skip-views` — schema + ledgered backfills (node_id
   indexes build CONCURRENTLY — the long pole; also catches the 4 live Bugzilla
   phantoms on this first run)
3. `scripts/heal_mirror_links.sh` — **skip on `aveloxis_large`** (82,869
   mirrors; see the script's own note)
4. `aveloxis resolve-email-identities`
5. `aveloxis strip-quoted-history --limit 50000` (canary), then without `--limit`
6. `aveloxis backfill-mailing-list-projection`
7. `aveloxis refresh-views`
8. `aveloxis ack-deploy`
9. `aveloxis start all`

Then the `due_at` pull-in for the four stuck monster repos
(4366/7311/16095/94608) — **only after the new binary is serving**. Jira stays
off; `register-jira-projects` / `backfill-jira-identities` are optional there.
No `aveloxis.json` edits required.

_(Note: the accidentally-committed 28 MB `aveloxis` binary has already been
removed from the working tree and is in `.gitignore`.)_
