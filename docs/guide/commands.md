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
