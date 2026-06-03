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
| `--monitor` | string | `":5555"` | Address for the monitoring dashboard and REST API. Set to `":0"` to disable. |
| `--workers` | integer | `1` | Number of concurrent collection workers. Each worker claims one repo at a time from the queue. |
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
| Contributor breadth | Every 6 hours | Discovers cross-repo contributor activity via GitHub Events API |
| Materialized view rebuild | Weekly (Saturday) | Pauses collection, refreshes all 19 matviews, resumes |
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

Creates 108 tables and 19 materialized views across two PostgreSQL schemas:

- **`aveloxis_data`** (84 tables + 19 materialized views) -- all collected data
- **`aveloxis_ops`** (24 tables) -- operational state

Also performs a data cleanup pass that nullifies garbage timestamps (year < 1970) across all tables, preventing BC-era dates from poisoning queries.

Safe to run repeatedly. All DDL uses `CREATE ... IF NOT EXISTS` and inserts use `ON CONFLICT DO NOTHING`. Does not touch Augur schemas if sharing a database.

---

## `aveloxis refresh-views`

Manually refreshes all 19 materialized views.

```bash
aveloxis refresh-views
```

Uses `REFRESH MATERIALIZED VIEW CONCURRENTLY` where unique indexes exist, so reads are not blocked during the refresh. Views are also rebuilt automatically every Saturday by `aveloxis serve`.

---

## `aveloxis install-tools`

Installs optional analysis tools.

```bash
aveloxis install-tools
```

Currently installs [scc](https://github.com/boyter/scc) (Sloc Cloc and Code) for per-file code complexity analysis. Requires Go to be installed.

If `scc` is not installed, the code complexity phase is silently skipped during collection.

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
