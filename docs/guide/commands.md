# Commands Reference

Complete reference for every Aveloxis CLI command.

---

## `aveloxis serve`

Starts the long-running scheduler that continuously collects repos from the priority queue, plus a web monitoring dashboard.

```bash
aveloxis serve [flags]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--monitor` | string | `"127.0.0.1:5555"` | Address for the monitoring dashboard (the REST API is a separate process — see `aveloxis api`). Always started with serve. |
| `--workers` | integer | config `collection.workers` (default `12`); flag overrides | Number of concurrent collection workers. Each worker claims one repo at a time from the queue. The cobra flag default is 1, but when the flag is not explicitly set, serve uses `collection.workers` from `aveloxis.json`. |
| `--augur-keys` | boolean | `false` | Also load API keys from Augur's `augur_operations.worker_oauth` table. |

### Behavior

- Uses the **staged collection pipeline** (API -> staging -> processing -> facade -> commit resolution -> analysis).
- The queue is Postgres-backed (`aveloxis_ops.collection_queue`) and uses `SELECT ... FOR UPDATE SKIP LOCKED` for atomic job claiming.
- Safe to stop and restart at any time. On shutdown (`Ctrl-C` / `SIGTERM`), active workers finish their current API call, queue locks are released, and staging data is preserved.
- On startup, automatically processes any leftover staged data from a previous interrupted run.
- Stale locks from crashed instances are recovered after 1 hour.
- Multiple instances can share the same queue for horizontal scaling.

### Periodic tasks

The scheduler also runs these background tasks:

| Task | Interval | Description |
|---|---|---|
| Org refresh | Every 4 hours | Re-fetches org membership lists |
| Contributor breadth | Every 15 minutes (config `breadth_interval_minutes`) | Discovers cross-repo contributor activity via GitHub Events API |
| Materialized view rebuild | Weekly (Saturday) | Pauses collection, refreshes all 20 materialized views, resumes |
| Stale lock recovery | Every 5 minutes | Re-queues jobs locked for more than 1 hour |

### Scancode worker tuning

`aveloxis serve` also runs a decoupled `ScancodeWorker` pool (v0.21.0+) for per-file license + copyright scanning. The pool's behavior is tuned via the `collection` block of `aveloxis.json` — no CLI flag. The most operationally relevant knobs:

| `aveloxis.json` field | Default | What to change it for |
|---|---|---|
| `scancode_workers` | `2` | More concurrent scancode invocations (cap by CPU cores). |
| `scancode_max_in_memory` | `5000` | **Raise on RAM-rich hosts** (e.g. `50000` on a host with hundreds of GB) so scancode keeps more per-file scan state in memory instead of spilling to a tempfile. Linux-kernel / chromium-class monorepos benefit most. Memory cost is roughly `scancode_workers × --processes × scancode_max_in_memory × per-file working set`, so account for the multiplier when sizing. v0.25.2+. |
| `scancode_run_timeout_hours` | `2` | Raise on fleets skewed toward big repos so the per-job timeout starts higher (default is adaptive — every wall-clock timeout doubles the next attempt's budget up to `scancode_run_timeout_cap_hours`). |
| `scancode_cadence_days` | `180` | Lower for testing or when source-license churn is unusually high. |

See [`configuration.md` -> scancode worker](../getting-started/configuration.md) for the full per-field reference and the v0.21.x → v0.25.x context.

### Examples

```bash
# Start with defaults (1 worker, dashboard on :5555)
aveloxis serve

# Start with 4 workers and a custom dashboard port
aveloxis serve --workers 4 --monitor :8082

# Start using Augur's API keys
aveloxis serve --workers 4 --augur-keys
```

---

## `aveloxis web`

Starts the web GUI for OAuth-based group management. Users log in via GitHub or GitLab, create groups, and add repositories or entire organizations for collection.

```bash
aveloxis web
```

### Configuration

The `web` command has no CLI flags. All settings come from the `web` section of `aveloxis.json`:

| Config field | Type | Default | Description |
|---|---|---|---|
| `web.addr` | string | `":8082"` | Listen address for the web server. |
| `web.base_url` | string | `"http://localhost:8082"` | External URL used to construct OAuth callback URLs. |
| `web.session_secret` | string | (required) | Secret key for signing session cookies. |
| `web.github_client_id` | string | `""` | GitHub OAuth app client ID. |
| `web.github_client_secret` | string | `""` | GitHub OAuth app client secret. |
| `web.gitlab_client_id` | string | `""` | GitLab OAuth app client ID (Application ID). |
| `web.gitlab_client_secret` | string | `""` | GitLab OAuth app client secret. |
| `web.gitlab_base_url` | string | `"https://gitlab.com"` | GitLab instance URL for self-hosted instances. |

### OAuth app setup

- **GitHub**: Create an OAuth app at [https://github.com/settings/developers](https://github.com/settings/developers). Set the callback URL to `{web.base_url}/auth/github/callback`.
- **GitLab**: Create an OAuth app at [https://gitlab.com/-/profile/applications](https://gitlab.com/-/profile/applications). Set the redirect URI to `{web.base_url}/auth/gitlab/callback`. Check the `read_user` scope.

### Behavior

- Serves the web GUI on the configured listen address.
- Users authenticate via OAuth, then create groups and add repos or orgs through the browser.
- Breadcrumb navigation shows `Home / {Group Name}` for easy navigation.
- Group detail pages display repos 25 per page with pagination controls. A case-insensitive search box filters by repo name, owner, or URL.
- Repos added through the web GUI are inserted into the same collection queue used by `aveloxis add-repo`.
- When a user adds a GitHub org or GitLab group, a `user_org_requests` row is created. A background task scans tracked orgs every 4 hours to discover and queue new repos.
- Sessions are stored in-memory with a 24-hour expiry. Restarting the process clears all sessions.
- Runs as a separate process from `aveloxis serve`. Both share the database.

### Examples

```bash
# Start the web GUI (uses settings from aveloxis.json)
aveloxis web

# Then open http://localhost:8082 in your browser
```

See the [Web GUI guide](web-gui.md) for detailed setup instructions.

---

## `aveloxis api`

Starts the REST API server (default `:8383`). Serves repo statistics, weekly
time series, dependency licenses, scancode results, SBOM downloads, repo
search, and the Augur-compatible metric endpoints. The web GUI's charts and
the comparison page load their data from this process.

```bash
aveloxis api --addr :8383
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--addr` | string | `":8383"` | Listen address for the API server. |

Run alongside `aveloxis serve` and `aveloxis web` — `aveloxis start all`
starts all three. See the [REST API guide](api.md) for the endpoint
reference.

---

## `aveloxis scancode-worker`

Runs ONLY the ScancodeWorker pool against the configured database (v0.27.6)
— the dedicated-scancode-host deployment, following the `aveloxis api`
single-purpose-process precedent. Scancode is the one subsystem whose
resource profile (multi-GB shallow clones, CPU-pinned Python subprocesses,
24-hour worst-case wall clocks) is nothing like the API-bound main pipeline;
moving it to an adjacent machine isolates its disk/CPU blast radius while
the shared tables (`FOR UPDATE SKIP LOCKED` claims + the
`scancode_locked_host` column) provide all the coordination needed.

```bash
aveloxis scancode-worker -c /etc/aveloxis/aveloxis.json
```

- **The primary server opts out** by setting `"scancode_workers": 0`
  (an EXPLICIT 0 — an absent key keeps the default of 2) in its own
  `aveloxis.json`.
- **No API keys required** — scancode clones anonymously; this command
  starts without any `worker_oauth` rows (unlike `aveloxis serve`).
- **Does NOT run schema migrations** (the v0.21.5 contract: only `serve`
  and `migrate` do). It checks the schema version at startup and logs an
  ERROR pointing at `aveloxis migrate` when the DB is behind.
- Writes its PID to `~/.aveloxis/aveloxis-scancode-worker.pid`.
- All `collection.scancode_*` knobs apply (workers, cadence, clone dir,
  adaptive timeouts, ignore globs, timeout-cap strikes).

Full recipe — Postgres remote access, minimal config template, systemd
unit, libmagic version-lock — in the
[dedicated scancode host guide](dedicated-scancode-host.md).

---

## `aveloxis test-mail`

Sends a test email through the configured Gmail SMTP settings (v0.20.14).
Validates the `mail` block of `aveloxis.json` first (fail-fast on syntactic
mistakes like a bare domain in `gmail_user` or a non-App-Password), then
actually exercises the credentials.

```bash
aveloxis test-mail you@example.org
```

Use at deploy time so the first user signup isn't the first SMTP attempt.

---

## `aveloxis collect`

One-shot collection of specific repos without the scheduler. Uses the **direct collection pipeline** (bypasses staging, writes directly to relational tables). Best for testing or collecting a small number of repos.

```bash
aveloxis collect [flags] <url> [<url> ...]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--full` | boolean | `false` | Full historical collection. Ignores the `days_until_recollect` window and fetches all data from the beginning. |
| `--augur-keys` | boolean | `false` | Also load API keys from Augur's `augur_operations.worker_oauth` table. |

### Examples

```bash
# Incremental collection (only new data since last run)
aveloxis collect https://github.com/chaoss/augur

# Full historical collection
aveloxis collect --full https://github.com/chaoss/augur

# Multiple repos, mixed platforms
aveloxis collect \
  https://github.com/torvalds/linux \
  https://gitlab.com/fdroid/fdroidclient
```

---

## `aveloxis add-repo`

Adds repositories to the collection queue. Platform is auto-detected from the URL.

```bash
aveloxis add-repo [flags] <url> [<url> ...]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--priority` | integer | `100` | Queue priority. Lower numbers are collected first. |
| `--from-augur` | boolean | `false` | Import all repos from `augur_data.repo`. Each URL is verified via HTTP HEAD -- dead repos are skipped. |

### URL formats

```bash
# Single GitHub repo
aveloxis add-repo https://github.com/chaoss/augur

# Single GitLab repo (including nested subgroups)
aveloxis add-repo https://gitlab.com/group/subgroup/project

# GitHub organization (adds all repos in the org)
aveloxis add-repo https://github.com/chaoss

# Multiple repos at once
aveloxis add-repo \
  https://github.com/torvalds/linux \
  https://github.com/chaoss/grimoirelab

# High priority
aveloxis add-repo --priority 10 https://github.com/kubernetes/kubernetes

# Import from Augur
aveloxis add-repo --from-augur
```

### Platform detection

- URLs containing `github.com` are treated as GitHub
- URLs containing `gitlab` in the hostname are treated as GitLab
- Hostnames listed in `gitlab.gitlab_hosts` in the config are treated as GitLab

---

## `aveloxis add-key`

Stores API keys in the database for use during collection.

```bash
aveloxis add-key [flags] [<token>]
```

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--platform` | string | (required unless `--from-augur`) | Platform for the key: `github` or `gitlab`. |
| `--from-augur` | boolean | `false` | Bulk import all keys from `augur_operations.worker_oauth`. Duplicates are skipped. |

### Examples

```bash
# Store a GitHub token
aveloxis add-key ghp_your_github_token --platform github

# Store a GitLab token
aveloxis add-key glpat-your_gitlab_token --platform gitlab

# Bulk import from Augur
aveloxis add-key --from-augur
```

---

## `aveloxis prioritize`

Pushes a repository to the front of the collection queue.

```bash
aveloxis prioritize <url>
```

Sets the repo's priority to 0 and due time to now. The scheduler will collect it next.

```bash
aveloxis prioritize https://github.com/chaoss/augur
```

Also available via the REST API:

```bash
curl -X POST http://localhost:5555/api/prioritize/42
```

Where `42` is the repo's `repo_id`.

---

## `aveloxis recollect`

Flags one or more repositories for a **full** (`since=zero`) re-collection on their next scheduler cycle.

```bash
aveloxis recollect <url>...
```

Sets the `force_full_collect` flag on each named repo's `aveloxis_ops.collection_queue` row. When the scheduler dequeues a flagged repo, `determineSince()` returns zero time, triggering a full re-collection regardless of `last_collected`. The flag clears itself on the next successful completion.

```bash
aveloxis recollect https://github.com/chaoss/augur
aveloxis recollect https://github.com/a/b https://github.com/c/d     # multiple
```

Use this command:

- After a bugfix or schema change that invalidates previously-collected data for a specific repo.
- To manually force a refresh when you suspect the incremental-since window missed something.
- In combination with `aveloxis prioritize <url>` to start the full re-collection immediately rather than at the repo's normal due time.

### Automatic triggering (v0.18.24+)

The scheduler also sets this flag **automatically** when a collection ends with an error that indicates incomplete PR child data — specifically the GraphQL PR batch error classes (stream CANCEL mid-body, "Timeout on validation of query", or retry exhaustion). The next cycle then backfills whatever the failed batch missed. Operators see a `force_full_recollect set` WARN log line when auto-flagging fires.

See the [troubleshooting guide](troubleshooting.md#graphql-pr-batch-errors-on-large-repos) for details on the error classes that trigger auto-flagging.

---

## `aveloxis migrate`

Creates or updates the database schema.

```bash
aveloxis migrate
```

Creates 141 tables and 20 materialized views across three PostgreSQL schemas:

- **`aveloxis_data`** (100 tables + 20 materialized views) -- all collected data
- **`aveloxis_ops`** (37 tables) -- operational state
- **`aveloxis_scan`** (4 tables) -- scancode per-file license/copyright results

Also performs a data cleanup pass that nullifies garbage timestamps (year < 1970) across all tables, preventing BC-era dates from poisoning queries.

Safe to run repeatedly. All DDL uses `CREATE ... IF NOT EXISTS` and inserts use `ON CONFLICT DO NOTHING`. Does not touch Augur schemas if sharing a database.

---

## `aveloxis backfill-identities`

One-off repair for missing identity attribution (v0.26.5 — see
`summary/identity-attribution-audit-2026-07-09.md`). The 2026-07-09
audit found `cntrb_id` 0% populated on `issue_assignees`,
`pull_request_assignees`, `pull_request_reviewers`, and
`pull_request_meta`, and `issues.closed_by_id` at 0.015% — while the
raw identity material (platform user ids) was stored at ~100%.

```bash
aveloxis backfill-identities --dry-run          # candidate counts, no writes
aveloxis backfill-identities                    # all phases
aveloxis backfill-identities --phase 1          # assignment joins + pr_meta owners (SQL only)
aveloxis backfill-identities --phase 2          # closed_by from issue events (SQL only)
aveloxis backfill-identities --phase 3          # closed_by GraphQL timeline sweep (needs API keys)
```

Flags: `--dry-run`, `--batch-size` (primary-key window width per UPDATE batch, default
100000; on fleet-scale tables use 1000000), `--limit` (cap per phase, 0 = unbounded), `--phase`
(`all|1|2|3`), `--sweep-batch` (issues per GraphQL query in phase 3,
default 100).

Phases 1–2 are pure SQL derivation (no API calls; measured production
coverage 99.87–99.98% for assignments). Phase 3 fetches closers the
history-capped events feed cannot reach, via per-issue
`timelineItems(itemTypes:[CLOSED_EVENT])` batched ~100 issues per
GraphQL query (~3 points each). Run phase 2 after the v0.26.3
event-healing cohort completes for best coverage; all phases are
idempotent and resumable. Rows whose identity genuinely isn't
derivable stay NULL — no data is fabricated.

## `aveloxis dedup-repos`

Merges case-variant duplicate repositories (v0.25.32). GitHub and GitLab
treat owner/repo URLs case-insensitively, so
`github.com/azure/azure-sdk-tools` and `github.com/Azure/azure-sdk-tools`
are the **same repository** — but fleets that predate v0.25.32 could
accumulate both as separate rows, each collected in full (doubled API
budget, storage, and double-counted analytics).

```bash
aveloxis dedup-repos --dry-run      # show the plan (first 20 pairs)
aveloxis dedup-repos --limit 50     # canary batch
aveloxis dedup-repos                # full run — re-run until "0 pairs"
```

### Flags

| Flag | Default | Purpose |
|---|---|---|
| `--dry-run` | false | Print the merge plan without writing. Shows winner/loser URLs + repo_ids, per-side last-collected dates, and whether either side is mid-collection. |
| `--batch-size` | 50 | Pairs merged per batch. Each pair is one heavy transaction (repoints + child-data deletes). |
| `--limit` | 0 | Cap total pairs merged this run (0 = no cap). |

### What each merge does

Per pair, winner = the **oldest** `repo_id`, in one transaction:

1. Repoints `user_repos` to the winner — every user group that
   referenced either variant keeps the repo and immediately sees the
   winner's collected data.
2. Repoints shared-copy rows (`messages`, `email_message`,
   `commit_comment_ref`, `foundation_membership`) — these tables are
   globally unique, so the pair shares one copy. (`contributor_repo` is
   deliberately untouched: it is the breadth worker's observational
   record of contributors' GitHub-wide activity, keyed by the numeric
   `gh_repo_id`, not a catalog reference.)
3. Deletes the loser's duplicated child data (issues, PRs, commits,
   releases, dependency scans, ...) leaves-first, then the loser row
   itself. Nothing is lost: both sides collected the same repository.
4. Enqueues the winner if it has never been collected.

Pairs with either side `status='collecting'` are **skipped and
reported** — re-run once those jobs finish. The command is idempotent;
merged pairs drop out of the candidate set.

### After the run

```bash
aveloxis migrate --skip-views   # builds uq_repos_repo_git_ci — the permanent
                                # DB-level backstop (skipped with a WARN while
                                # duplicates remain)
aveloxis refresh-views          # matviews stop double-counting immediately
```

Generic-git repos (platform 3) are never touched: unknown hosts may
legitimately be case-sensitive, so their URLs stay byte-exact.

---

## `aveloxis migrate-cntrb-ids`

One-shot opt-in data migration (v0.22.2): rewrites contributors whose
`cntrb_id` is a legacy random UUID to the deterministic
`PlatformUUID(platform_id, platform_user_id)` form, cascading through all
child tables via the v0.22.1 `ON UPDATE CASCADE` FKs.

```bash
aveloxis migrate-cntrb-ids --dry-run     # show the plan
aveloxis migrate-cntrb-ids               # execute (batched transactions)
aveloxis migrate-cntrb-ids --limit 10000 # incremental run
```

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | `false` | Show the plan and a sample without writing. |
| `--batch-size` | `5000` | Rows per transaction (use 500 on fleet-scale DBs). |
| `--limit` | `0` | Cap rows migrated this run (0 = no cap). |
| `--skip-precheck` | `false` | DANGEROUS: skip the ON UPDATE CASCADE precheck. |

Run `aveloxis refresh-views` afterwards. Collision pairs are left for
`merge-cntrb-collisions`.

---

## `aveloxis merge-cntrb-collisions`

Soft-merges the rename-collision pairs `migrate-cntrb-ids` skips (v0.22.3):
same platform user under two rows. Moves identities to the winner, merges
non-empty fields, inserts an alias, marks the loser `cntrb_deleted = 1`
(row stays in place — no FK rewrites).

```bash
aveloxis merge-cntrb-collisions --dry-run
aveloxis merge-cntrb-collisions
```

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | `false` | Show the merge plan without writing. |
| `--batch-size` | `500` | Pairs merged per batch (one transaction each). |
| `--limit` | `0` | Cap total pairs merged this run. |

---

## `aveloxis refresh-views`

Manually refreshes all 20 materialized views.

```bash
aveloxis refresh-views
```

Uses `REFRESH MATERIALIZED VIEW CONCURRENTLY` where unique indexes exist, so reads are not blocked during the refresh. Views are also rebuilt automatically every Saturday by `aveloxis serve`.

---

## `aveloxis sbom`

Generates a CycloneDX or SPDX Software Bill of Materials from the
dependency data collected for a repository.

```bash
aveloxis sbom 42 --format cyclonedx -o sbom.json
```

| Flag | Default | Description |
|---|---|---|
| `--format` | `cyclonedx` | Output format: `cyclonedx` or `spdx`. |
| `--output`, `-o` | stdout | Write to a file instead of stdout. |
| `--store` | `false` | Also store the SBOM in `repo_sbom_scans`. |

The repo must have been collected with dependency analysis (automatic
under `aveloxis serve`). SBOMs are also downloadable from the web GUI and
the REST API.

---

## `aveloxis install-tools`

Installs optional analysis tools.

```bash
aveloxis install-tools
```

Installs three external analysis tools: [scc](https://github.com/boyter/scc) (per-file code complexity; requires Go), [scorecard](https://github.com/ossf/scorecard) (OpenSSF security posture; release tarball), and [scancode](https://github.com/nexB/scancode-toolkit) (per-file license/copyright scanning; requires **Python 3.10+** and pipx — also injects the `typecode-libmagic` plugin). If the Python prerequisite is missing, scancode installation fails and per-file license scanning never runs; see `aveloxis upgrade-tools` for updating installed tools.

If `scc` is not installed, the code complexity phase is silently skipped during collection.

---

## `aveloxis upgrade-tools`

Upgrades the external analysis tools in place (v0.23.6). Unlike
`install-tools` (which short-circuits when a tool exists), this re-runs
the install path to pull updates: `go install ...@latest` for scc, a fresh
release tarball for scorecard, and `pipx upgrade` for scancode (preserving
venv customizations) plus a `typecode-libmagic` re-injection.

```bash
aveloxis upgrade-tools
```

Exits non-zero if any tool fails to upgrade, so it can run from a
maintenance cron.

---

## `aveloxis start`

Launches aveloxis components as detached background processes with log output redirected to files in `~/.aveloxis/`.

```bash
aveloxis start serve   # scheduler + monitor → ~/.aveloxis/aveloxis.log
aveloxis start web     # web GUI             → ~/.aveloxis/web.log
aveloxis start api     # REST API            → ~/.aveloxis/api.log
aveloxis start all     # all three at once
```

PID files are written to `~/.aveloxis/aveloxis-{serve,web,api}.pid`. If a component is already running, the command reports it and skips the launch.

Log files are opened in append mode — existing content is preserved across restarts.

Note: `start` does NOT survive a reboot. For production hosts that should
come back automatically after a restart, run the three processes under
systemd instead — see [Running Aveloxis as a Service](running-as-a-service.md).
Pick one manager per host: once the systemd units own the processes,
`aveloxis start`/`stop` must not be used there.

---

## `aveloxis stop`

Gracefully stops background aveloxis processes.

```bash
aveloxis stop serve    # stop only the scheduler
aveloxis stop web      # stop only the web GUI
aveloxis stop api      # stop only the REST API
aveloxis stop all      # stop all three
aveloxis stop          # (no args) same as 'all'
```

Sends `SIGTERM` to the specified component(s) using PID files in `~/.aveloxis/`. Active workers finish their current API call, queue locks are released, and staging data is preserved. PID files are cleaned up automatically. Stale PID files (process no longer running) are detected and removed.

```{note}
`aveloxis stop` also works for processes started in the foreground (e.g., `aveloxis serve`), because all foreground processes write PID files on startup.
```

---

## `aveloxis data-test`

Operator-driven shadow-database verification harness for schema
changes. Builds binaries from a tagged release and the local working
tree, provisions two scratch databases (`aveloxis_released` and
`aveloxis_new`), collects the same repo into each, and reports
row-count differences. Catches data-loss regressions before they
ship. Shipped in v0.22.8.

See the full guide: [Schema-change verification](data-test.md).

### Flags

- `--released-tag TAG` — git tag of the released aveloxis version
  to compare against (e.g., `0.22.6`). The tag must exist in the
  local clone; `git fetch --tags` if missing. **Required.**
- `--repo URL` — git URL of the test repo to collect into both
  scratch DBs. `augurlabs/augur` is the canonical choice. **Required.**
- `--keep-dbs` — retain the scratch DBs after the run. Default is
  to drop them. Pass when you want to inspect a failing table via
  `psql` after the report is written.
- `--work-dir PATH` — where to put binaries, logs, and the report.
  Default is `/tmp/aveloxis-data-test-<UTC-timestamp>`.

### Behavior

- Builds the released binary via `git worktree add` (reuses local
  clone's objects — no remote fetch).
- Builds the local binary from the current working tree.
- Connects to the configured PostgreSQL host using the operator's
  `aveloxis.json` credentials. The user must have **CREATEDB
  privilege** because the harness creates and drops scratch
  databases.
- Copies API keys from the operator's primary `aveloxis_ops.api_keys`
  table into both scratch DBs — operator doesn't re-paste tokens.
- Collections run **sequentially** (~30 min each), not parallel,
  because both sides share the API key pool.
- Exit code 0 on PASS or FLAG-only; exit code 1 on any FAIL (row
  loss detected) — suitable for CI gating.

### Examples

```bash
# Validate the current working tree against v0.22.6 using augur
aveloxis data-test --released-tag 0.22.6 \
  --repo https://github.com/augurlabs/augur

# Keep scratch DBs for ad-hoc inspection after a FAIL
aveloxis data-test --released-tag 0.22.6 \
  --repo https://github.com/augurlabs/augur \
  --keep-dbs

# Custom work directory (useful for CI artifact retention)
aveloxis data-test --released-tag 0.22.6 \
  --repo https://github.com/augurlabs/augur \
  --work-dir /var/cache/aveloxis-data-test
```

The full report is written to `<work-dir>/report.md`. See
[Schema-change verification](data-test.md) for guidance on
interpreting PASS / FLAG / FAIL results.

---

### Column-fill diff (v0.26.1)

Row counts can't see a column the new binary stopped populating (the
canonical case: `issue_labels.platform_label_id` is `0` on every row
under the GraphQL path while row counts match exactly). After the
row-count diff, data-test therefore also compares per-column FILL
COUNTS — how many rows carry a meaningful value, type-aware (`<> ''`
for text, `<> 0` for numerics, `IS NOT NULL` otherwise) — across every
column of every base table in all three schemas.

- **FAIL** (exit code 1): a column populated under the released binary
  is *completely* unpopulated under the new one — a dropped mapping or
  renamed JSON tag.
- **FLAG**: fill counts differ — review; small drift is expected since
  the two collections run against a live repo minutes apart.


## `aveloxis shadow-diff`

Semantic equivalence diff of issue/PR collection between two databases
(the harness behind the REST → GraphQL refactor's per-phase validation).
Compares issues, PRs, labels, assignees, reviewers, reviews, commits,
files, messages, and bridge tables by platform-stable keys.

```bash
aveloxis shadow-diff --rest-dsn postgres://.../aveloxis_shadow_rest \
                     --graphql-dsn postgres://.../aveloxis_shadow_graphql
```

| Flag | Default | Description |
|---|---|---|
| `--rest-dsn` | (required) | Postgres DSN for the baseline database. |
| `--graphql-dsn` | (required) | Postgres DSN for the comparison database. |
| `--repo-id` | `0` | Restrict the diff to one repo_id (0 = all). |
| `--json` | `false` | Machine-readable report for CI pipes. |

Exit code 1 on any FAIL-level difference. For a coarser whole-schema
row-count comparison, see `aveloxis data-test`.

---

## `aveloxis reconcile-repos`

Heals the stranded-repo class: non-archived `repos` rows with no
`collection_queue` row — invisible to the scheduler forever. Production
audit (2026-07-21) root-caused the cohort to GitHub renames + prelim's
duplicate skip+dequeue (which dequeues without archiving), plus a
smaller lost-enqueue share. Each stranded repo is classified by a LIVE
redirect check:

- dead upstream (404/410) → archived (the outcome prelim applies)
- redirects to a **tracked** repo → dataless duplicates heal onto the
  winner (`HealRenamedDuplicate`); data-bearing duplicates consolidate
  through the dedup-repos per-pair machinery (repoints + leaves-first
  deletes)
- redirects to an **untracked** URL → re-enqueued (prelim renames it
  in place on the next cycle — rename detection is prelim's job)
- alive at its own URL → re-enqueued (a lost queue row)

```bash
aveloxis reconcile-repos --dry-run   # per-repo classification, no writes
aveloxis reconcile-repos --limit 50  # canary
aveloxis reconcile-repos             # everything
```

The scheduler also logs a startup gauge (`non-archived repos with no
collection_queue row`) pointing here whenever the count is non-zero.
Re-run until stranded = 0; healed repos drop out of the set.

## `aveloxis data-verify`

The standing data-verification program (v0.27.43): runs the read-only
invariant probe battery against the configured database and exits 1 on
any FAIL — suitable for gating releases and cron runs. Probes:
structural invariants (case-duplicate repos, Default-group singleton,
stranded repos, stale >24h locks, cross-kind message corruption with
migration-state awareness), sampled count-integrity (cached queue
counts vs actual rows, gathered vs forge metadata at the gap
detector's 5% line, batch-vs-single stats agreement), and fill rates
(report-only unless floors are set).

```bash
aveloxis data-verify                          # full battery, human report
aveloxis data-verify --json                   # automation form
aveloxis data-verify --sample 500             # wider drift/equality sample
aveloxis data-verify --ground-truth 10        # ALSO re-fetch live GitHub
                                              # metadata for 10 sampled repos
                                              # and compare (needs API keys)
aveloxis data-verify --min-identity-fill 95   # optional floors → WARN below
```

Severities: FAIL = a code-enforced invariant is broken (exit 1);
WARN = known-and-healing state or operator-attention signal (pending
heal worklist, stranded repos, schema behind the binary — each WARN
names its fix command); OK = verified. Safe against production: every
probe is read-only and bounded.

## `aveloxis heal-messages`

Repairs message rows corrupted by the pre-v0.27.38 cross-kind ID
collision (issue/PR conversation comments, inline review comments, and
review bodies share overlapping GitHub ID sequences; under the old
two-column arbiter the later writer silently overwrote the earlier
kind's text — 198,237 rows on the production fleet). The v0.27.38
migration captures the affected rows into
`aveloxis_ops.message_heal_worklist`; this command consumes it.

Each pass refetches the claiming parents' comments per-item
(parent-deduplicated — API cost is per distinct issue/PR, not per
collision), re-creates the correct rows under the kinded arbiter,
deletes the stale cross-kind bridge links, and stamps `healed_at`.
Failed parents leave their rows pending; re-run until "nothing
pending".

```bash
aveloxis heal-messages --dry-run       # plan: pending rows, distinct parents
aveloxis heal-messages --limit 1000    # canary pass
aveloxis heal-messages                 # everything pending
```

Flags: `--limit` (0 = all pending), `--dry-run`, `--augur-keys`.
Requires GitHub API keys (this healer refetches). Safe with serve
stopped; after a fleet restart, run it once the force-full wave
crests so it doesn't compete with collection for the key pool.

## `aveloxis heal-vulnerabilities`

One-shot healer for vulnerability rows stored before v0.27.4's
two-phase OSV fetch (they carry `severity=UNKNOWN` and empty
summaries — the querybatch endpoint only returns id stubs). Re-scans
every repository that has vulnerability rows, filling severity, CVSS,
summary, fix versions, aliases, and references from `/v1/vulns/{id}`.

```bash
aveloxis heal-vulnerabilities              # all repos with findings
aveloxis heal-vulnerabilities --limit 50   # bounded canary run
```

Idempotent (prefer-nonempty upserts never downgrade filled data) and
safe alongside a running `serve`. Without this command the same
healing happens gradually as each repo's next scheduled collection
cycle re-scans it.

## `aveloxis run-scorecard`

Bulk remote-primary OpenSSF Scorecard pass (v0.27.5). Walks every
non-archived, at-least-once-collected **GitHub** repo and runs
scorecard in remote mode (`--repo`, the full ~18-check set including
Code-Review, Maintained, Contributors, Branch-Protection, CI-Tests,
CII-Best-Practices, Signed-Releases), oldest-scorecard-first — repos
that have never been scanned come first. Exists because the per-cycle
collection phase historically ran local mode (~11 checks); this
command upgrades the fleet's scorecard data without waiting for each
repo's recollect cadence.

```bash
aveloxis run-scorecard                        # the whole backlog
aveloxis run-scorecard --limit 50             # canary run
aveloxis run-scorecard --older-than 180       # only stale/never-scanned repos
aveloxis run-scorecard --workers 8            # default NumCPU/2, capped at NumCPU
```

Details:

- **Refuses to start while `aveloxis serve` is running on this host**
  (checked via the serve pidfile + process liveness) — the pass would
  compete with the collection workers for the shared GitHub API
  budget. Stop serve first: `aveloxis stop serve`. It also writes its
  own pidfile (`~/.aveloxis/aveloxis-run-scorecard.pid`) so two bulk
  passes can't overlap.
- Uses the SAME invoke/persist code as the collection phase: the
  comma-separated multi-token `GITHUB_TOKEN` built from the key pool
  (`collection.scorecard_token_count`), the per-attempt wall-clock
  timeout (`collection.scorecard_timeout_minutes`, default 15), and
  the `scorecard_mode` marker on every stored row.
- No analysis clone exists in this pass, so there is no local
  backstop: a failed remote attempt is logged, counted, and skipped —
  the repo's next collection cycle retries.
- Progress prints every 100 repos: done/total, cumulative
  `api_calls_used` (instrumented via `/rate_limit` on the first
  token), elapsed, and ETA. Interrupting with Ctrl-C is safe; the
  oldest-first ordering makes a re-run resume where it left off.
- Requires GitHub API keys (`aveloxis add-key`). Does not run schema
  migrations (v0.21.5 contract) — run `aveloxis migrate` first when
  upgrading to v0.27.5 so the `scorecard_mode` column exists.

## `aveloxis staging-stats`

Read-only view of the JSONB staging table, per repo and entity type
(v0.22.4): row counts, processed/unprocessed split, oldest/newest
timestamps, and approximate bytes.

```bash
aveloxis staging-stats              # top 10 (repo, entity_type) cohorts
aveloxis staging-stats --top 50
aveloxis staging-stats --repo microsoft/vscode
```

Use it to confirm the `staging_retention_hours` cleanup is working and to
spot repos whose staged rows aren't being processed.

---

## `aveloxis distribution-stats`

Read-only rollup of the distribution-tracking subsystem (v0.24.0): which
repos publish to package registries, and which declare packaging manifests
without any registry evidence.

```bash
aveloxis distribution-stats                    # fleet rollup
aveloxis distribution-stats --orphans          # manifest-without-registry list
aveloxis distribution-stats --repo owner/repo  # per-repo drill-down
```

Requires `collection.distribution_tracking_enabled: true`. See the
[distribution architecture doc](../architecture/distribution.md).

---

## Mailing-list commands

These commands register and verify the mailing-list ingestion subsystem
(off by default; set `collection.mailing_list_enabled = true` to collect).
See [Mailing-list ingestion](../architecture/mailing-list.md) for how the
subsystem works.

### `aveloxis load-foundation-core-repos`

Loads one core/primary repository per project across the tracked open-source
foundations (Apache TLPs + podlings). Idempotently registers each project's
flagship repo into a foundation group. Renamed from `import-foundations`
(kept as a hidden alias so existing scripts keep working).

```bash
aveloxis load-foundation-core-repos
aveloxis load-foundation-core-repos --dry-run
```

### `aveloxis load-foundation-orgs`

Registers each foundation's GitHub org(s) as **tracked orgs** under your
user, so the periodic org-refresh ticker continuously discovers new repos.
Tracking the `apache` org pulls all ~3,000 `apache/*` repos (a large
collection-budget commitment — surfaced in the output) so sibling repos
like `arrow-rs` get collected and mailing-list repo signals resolve.

```bash
aveloxis load-foundation-orgs --dry-run
aveloxis load-foundation-orgs --yes
```

| Flag | Description |
|---|---|
| `--user-id` | owning user (default 1) |
| `--dry-run` | print planned registrations without writing |
| `--yes` | proceed without the interactive confirmation |

### `aveloxis load-apache-lists`

For each Apache PMC, ensures a per-PMC `repo_group`, links the PMC's primary
repo, and registers the `dev@` and `users@` lists for collection. Run
`load-foundation-core-repos` first so the primary repos exist; PMCs whose
repo isn't in the catalog are skipped.

```bash
aveloxis load-apache-lists --dry-run
aveloxis load-apache-lists
```

### `aveloxis register-mailing-list`

Registers a single mailing list for collection under any archive system —
used for curated, non-catalog lists like the kernel's lore public-inbox.
The list is attached to the repo's `repo_group` (named after the repo, so
multiple lists for one repo share a group).

```bash
aveloxis register-mailing-list \
    --system lore_public_inbox \
    --list linux-pci@vger.kernel.org \
    --repo https://github.com/torvalds/linux
```

| Flag | Description |
|---|---|
| `--system` | archive system (`apache_ponymail`, `lore_public_inbox`) |
| `--list` | list address |
| `--repo` | repo URL to attach the list's discussion to (must already exist in the catalog) |

### `aveloxis backfill-issue-external-keys`

Populates `issues.external_key` from bracketed `[KEY-N]` title prefixes
(Apache Jira → GitHub issue imports). This is what lets `issue_event`
mailing-list mail bridge to the imported issue by its Jira key.

```bash
aveloxis backfill-issue-external-keys
```

### `aveloxis mailing-list-stats`

Read-only coverage rollup: registered lists, `email_message` counts, mirror
rate, signaled-repo resolution, sender-identity resolution, and the
per-class distribution. Safe to run alongside an active `serve`.

```bash
aveloxis mailing-list-stats
```

The same data is available over HTTP at `GET /api/v1/mailing-list/stats`
(see [REST API](api.md)).

### `aveloxis verify-mailing-list`

The Phase 4 branch-coverage harness. Reads the collected data and prints a
PASS / EMPTY / DEFER table for every logic branch (each `msg_class`, both
backends, each routing outcome, threading, signaled/sender resolution,
`external_key` backfill), plus the contributor-resolution assessment.

```bash
aveloxis verify-mailing-list            # report only (exit 0)
aveloxis verify-mailing-list --strict   # exit non-zero if a required branch is empty
```

| Flag | Description |
|---|---|
| `--strict` | exit non-zero when a *required* (mailing-list-native) branch produced zero rows |

`--strict` gates only the mailing-list-native branches. Cross-subsystem
branches (bridged-to-issue/PR, mirror-linked, sender-resolved,
`external_key`) report as **DEFER** and never gate — they fill in
steady-state operation once the linked repos' GitHub data is collected and
the periodic backfills run. See
[Mailing-list ingestion §12](../architecture/mailing-list.md) for the
collection-ordering caveat.

---

## `aveloxis backfill-mailing-list-projection`

Projects already-ingested mailing-list `issue_event` mail onto issues
(mailing-list Phase 5). In-place and idempotent; batched.

```bash
aveloxis backfill-mailing-list-projection --batch 500
```

| Flag | Default | Description |
|---|---|---|
| `--batch` | `500` | Rows per batch. |

---

## `aveloxis load-numfocus-projects`

Loads the embedded NumFocus project catalog (v0.25.4): creates the
"NumFocus Sponsored" / "NumFocus Affiliated" user groups and adds each
project's flagship repository.

```bash
aveloxis load-numfocus-projects --dry-run
aveloxis load-numfocus-projects
aveloxis load-numfocus-projects --detect-new   # report catalog drift vs numfocus.org
```

| Flag | Default | Description |
|---|---|---|
| `--user-id` | `1` | Owner of the created groups. |
| `--dry-run` | `false` | Preview without writing. |
| `--detect-new` | `false` | Crawl numfocus.org and report projects missing from the embedded catalog. |
| `--catalog-file` | (embedded) | Override the embedded YAML catalog. |

---

## `aveloxis load-numfocus-orgs`

Companion to `load-numfocus-projects`: registers each NumFocus project's
GitHub organization for ongoing repo-discovery refresh ("NumFocus
Sponsored Orgs" / "NumFocus Affiliated Orgs" groups). Same flag surface as
`load-numfocus-projects`.

```bash
aveloxis load-numfocus-orgs --dry-run
aveloxis load-numfocus-orgs
```

---

## `aveloxis version`

Prints the Aveloxis version.

```bash
aveloxis version
```

---

## Global behavior

### Config file

All commands look for `aveloxis.json` in the current working directory. The config file must exist and contain valid database connection parameters.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | General error (invalid arguments, config not found, database connection failed) |

### Signal handling

`aveloxis serve` handles the following signals:

- **`SIGTERM`** / **`SIGINT`** (`Ctrl-C`) -- graceful shutdown. Workers finish current API calls, locks are released, staging data is preserved.
- **`SIGTERM`** sent by `aveloxis stop` -- same graceful shutdown.
