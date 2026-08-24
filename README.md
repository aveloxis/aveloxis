# Aveloxis

**Open source community health analytics at fleet scale — the Go successor to [Augur](https://github.com/augurlabs/augur), built by CHAOSS co-founders**, as a solution to [CHAOSS governance removing protections for software creators](https://www.seangoggins.net/open-source-software-health-and-sustainability/). CHAOSS metrics, contributor analytics, SBOMs, and vulnerability scanning for GitHub and GitLab. Live at [aveloxis.io](https://aveloxis.io).

[![Tests](https://github.com/aveloxis/aveloxis/actions/workflows/test.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/test.yml)
[![Lint](https://github.com/aveloxis/aveloxis/actions/workflows/lint.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/lint.yml)
[![CodeQL](https://github.com/aveloxis/aveloxis/actions/workflows/codeql.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/codeql.yml) 
[![Container Build](https://github.com/aveloxis/aveloxis/actions/workflows/container-build.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/container-build.yml)
[![Docker Publish](https://github.com/aveloxis/aveloxis/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/aveloxis/aveloxis/actions/workflows/docker-publish.yml)

![Static Badge](https://img.shields.io/badge/DataCollection-tested-green) ![Static Badge](https://img.shields.io/badge/AugurConversion-untested-red) ![Static Badge](https://img.shields.io/badge/Containers-tested-green)  ![Static Badge](https://img.shields.io/badge/8Knot-tested-green)

#### [Documentation can be found on readthedocs.io at aveloxis.readthedocs.io/en/latest](https://aveloxis.readthedocs.io/en/latest/) 

[Read about the way Claude Code was used to create Aveloxis](https://seanpgoggins.substack.com/p/artificial-intelligence-can-erase)

<img width="1200" height="300" alt="aveloxis-banner-1200x300" src="https://github.com/user-attachments/assets/c3276b26-357d-404c-b6d9-d47a4778ca63" />

Copyright © 2026 University of Missouri, Sean Goggins, and Derek Howard. This software is possible through the support of [The Sloan Foundation](sloan.org). Learn more in the [Detailed LCF](#detailed-lcf). $\color{red}{\text{Augur user?}}$ [Compare with Augur](#comparison-with-augur)

## Requirements
**Note: RHEL/CentOS installations are based on internet searches, as we do not have access to a machine with those OS's**
- **Go 1.23+** ([install](https://go.dev/doc/install))
- **PostgreSQL 14+** (local, Docker, or remote)
- **git** (for the facade/commit collection phase)
- **GitHub and/or GitLab API tokens** (personal access tokens with repo/read scope)
- **Python 3.10+** and **libmagic** (optional, for ScanCode license/copyright scanning — installed automatically by `aveloxis install-tools`)
  - macOS: `brew install libmagic`
  - Debian/Ubuntu: `sudo apt-get install libmagic1`
  - RHEL/CentOS: `sudo yum install file-libs`
- **pipx** (You may need to install pipx for scancode's installation to run)
  - macOS: `brew install pipx`
  - Debian/Ubuntu: `sudo apt install pipx`
  - RHEL/CentOS: `sudo yum install pipx`
- **python-setuptools** (Necessary for scancode)
  - macOS: `brew install python-setuptools`
  - Debian/Ubuntu: `sudo apt install python3-setuptools`
  - RHEL/CentOS: `sudo dnf install python3-setuptools`
    
[See Development Mode Notes if you're running locally to evaluate](#development-mode)

## Installation

**Option 1: Install to your PATH** (recommended — lets you run `aveloxis` from anywhere).
Optionally install analysis tools for code complexity scanning:
```bash
git clone https://github.com/aveloxis/aveloxis.git
cd aveloxis
go mod tidy
go install ./cmd/aveloxis

# Verify it works (binary is now in $GOPATH/bin or $HOME/go/bin):
aveloxis version

# Install optional analysis tools (scc for code complexity):
aveloxis install-tools
```

> If `aveloxis: command not found`, add Go's bin directory to your PATH:
> ```bash
> export PATH="$PATH:$(go env GOPATH)/bin"
> ```

**Option 2: Build locally** (binary stays in the repo directory):
```bash
git clone https://github.com/aveloxis/aveloxis.git
cd aveloxis
go mod tidy
go build -o bin/aveloxis ./cmd/aveloxis

# Must use the explicit path — the binary is NOT on your PATH:
./bin/aveloxis version
```

All examples below use `aveloxis` (assumes Option 1). If you used Option 2, replace `aveloxis` with `./bin/aveloxis` everywhere.

### Database Setup

Aveloxis needs a PostgreSQL database. You can use an existing Augur database (Aveloxis creates its own `aveloxis_data` and `aveloxis_ops` schemas and does not touch Augur's schemas) or a fresh one:

**Option A: Use an existing Augur database** — just point `aveloxis.json` at the same host/port/dbname. Aveloxis creates its own schemas and does not touch Augur's.

Create an `aveloxis.json` file by copying [`aveloxis.example.json`](./aveloxis.example.json) and placing your credentials in that file.

**Option B: Create a fresh database** (run in `psql` as a superuser):
```sql
CREATE DATABASE aveloxis;
CREATE USER aveloxis WITH ENCRYPTED PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE aveloxis TO aveloxis;
ALTER DATABASE aveloxis OWNER TO aveloxis;
```

**Option C: Docker** (one command, no psql needed):
```bash
docker run -d --name aveloxis-db -p 5432:5432 \
  -e POSTGRES_DB=aveloxis \
  -e POSTGRES_USER=aveloxis \
  -e POSTGRES_PASSWORD=aveloxis \
  postgres:16
```

Then run migrations:
```bash
aveloxis migrate
```

This creates 143 tables (100 in `aveloxis_data`, 39 in `aveloxis_ops`, 4 in `aveloxis_scan`) with full parity to Augur's schema. All DDL uses `CREATE ... IF NOT EXISTS` and `ON CONFLICT DO NOTHING`, so `migrate` is safe to run repeatedly.

### OAUTH App Setup
You will need a github OAUTH application for login to work on the web view. And there's nothing available without login. You can also use GitLab's OAUTH, or configure both. 

**Example Values for GitHub OAuth**: 
- Homepage URL: https://aveloxis.io (this one is not important)
- Authorization Callback URL: http://localhost:8082/auth/github/callback (This one is important, and if you are running locally this is exactly what you need)

**Example Values for GitLab OAuth:**
- Callback URL: http://localhost:8082/auth/gitlab/callback 

Put those into your `aveloxis.json` file as described in the [Configuration Section](# configuration)

If you are running on bare metal, at this point you are ready to go!
```bash
# Start all three processes in the background (logs to ~/.aveloxis/*.log)
aveloxis start all

# Or start individually
aveloxis start serve   # → ~/.aveloxis/aveloxis.log
aveloxis start web     # → ~/.aveloxis/web.log
aveloxis start api     # → ~/.aveloxis/api.log
```

Then you can open the interfaces:
```bash
open http://localhost:8082          # Web GUI (login, visualizations, comparison)
open http://localhost:8082/monitor  # Collection monitor dashboard (requires login)
open http://localhost:8383/api/v1/health  # REST API
```

## Docker / Podman

Aveloxis runs in containers via Docker Compose or Podman Compose. All instructions below work with either — substitute `podman` for `docker` if you use Podman.

### Step 1: Configure `aveloxis.docker.json`

This file is **required**. It's mounted into all containers as the config file. You must add at least one API key — without keys, the scheduler (`serve`) will refuse to start.

```bash
# An example of the file already exists in the repo — copy, then edit it directly:
cp aveloxis.docker.example.json aveloxis.docker.json
vim aveloxis.docker.json
```

**Minimum changes needed:**

```jsonc
{
  "github": {
    "api_keys": ["ghp_YOUR_GITHUB_TOKEN"],   // ← REQUIRED: at least one PAT
    ...
  },
  "web": {
    "dev_mode": true,                         // ← Required for HTTP (no HTTPS)
    "github_client_id": "Iv1.abc123...",      // ← For OAuth login (see below)
    "github_client_secret": "deadbeef...",    // ← For OAuth login (see below)
    ...
  }
}
```

**GitHub API token:** Go to [github.com/settings/tokens](https://github.com/settings/tokens) and create a Personal Access Token with `repo` (or `public_repo`) scope. Add it to `github.api_keys`.

**GitHub OAuth app (for web GUI login):** Go to [github.com/settings/developers](https://github.com/settings/developers) → New OAuth App:
- **Homepage URL:** `http://localhost:8082`
- **Authorization callback URL:** `http://localhost:8082/auth/github/callback`

Copy the Client ID and Client Secret into `github_client_id` and `github_client_secret`.

**`dev_mode: true`** is needed because the containers run over plain HTTP. Without it, session cookies are marked `Secure` and browsers won't send them over HTTP, causing login to fail silently.

### Step 2: Start everything

```bash
# Docker
docker compose up -d --build

# Podman
podman compose up -d --build
```

This starts 5 containers:
| Container | Purpose | Port |
|---|---|---|
| `postgres` | PostgreSQL 16 database | 5432 |
| `migrate` | Runs schema migrations, then exits | — |
| `serve` | Collection scheduler | — |
| `web` | Web GUI (OAuth login, visualizations, monitor) | **8082** |
| `api` | REST API (stats, charts, SBOMs) | **8383** |

### Step 3: Open the interfaces

```
http://localhost:8082                # Web GUI (login with GitHub, create groups, add repos)
http://localhost:8082/monitor        # Collection monitor (queue status, repo progress)
http://localhost:8383/api/v1/health  # REST API health check
```

### Adding repos

```bash
# Via CLI (run inside a container)
docker compose exec serve aveloxis add-repo https://github.com/chaoss/augur

# Via the web GUI
# Log in at http://localhost:8082, create a group, and add repos through the browser
```

### Managing containers

```bash
# View logs
docker compose logs -f serve       # Follow scheduler logs
docker compose logs -f web         # Follow web GUI logs
docker compose logs migrate        # Check migration output

# Stop (data is preserved in volumes)
docker compose down

# Stop AND delete all data (database + clones — destructive!)
docker compose down -v

# Restart with different worker count
AVELOXIS_WORKERS=40 docker compose up -d

# Set a custom database password
AVELOXIS_DB_PASSWORD=secret docker compose up -d
```

### Persistent volumes

| Volume | Contents | Survives `down`? | Destroyed by `down -v`? |
|---|---|---|---|
| `aveloxis-pgdata` | PostgreSQL database (all collected data) | Yes | **Yes** |
| `aveloxis-repos` | Bare git clones for facade/analysis | Yes | **Yes** |

### Build from source

```bash
# Docker
docker build -t aveloxis .

# Podman
podman build -t aveloxis .

# Then use the local image
docker compose up -d --build
```

### Troubleshooting containers

**`serve` exits immediately:**
Check `docker compose logs serve`. The most common cause is missing API keys — you'll see `"no API keys configured for any platform"`. Add at least one key to `aveloxis.docker.json` and restart.

**Web GUI login fails (no error, just redirects back):**
Set `"dev_mode": true` in the `"web"` section of `aveloxis.docker.json`. Without this, session cookies require HTTPS.

**`migrate` fails:**
Check `docker compose logs migrate`. If it says a relation doesn't exist, you may need to rebuild: `docker compose down -v && docker compose up -d --build`.

**Monitor shows no repos:**
Add repos via `docker compose exec serve aveloxis add-repo <url>` or through the web GUI.

## Quick Start for Existing Augur Users

If you already have Augur running with repos and API keys in its database, you can be collecting in four commands:

```bash
# 1. Point Aveloxis at your existing Augur database.
#    Create a minimal config with just the database connection:
cat > aveloxis.json <<'EOF'
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "augur",
    "password": "your-augur-db-password",
    "dbname": "augur",
    "sslmode": "prefer"
  }
}
EOF

# 2. Create the aveloxis_data and aveloxis_ops schemas in your Augur database.
#    This does NOT touch augur_data or augur_operations.
aveloxis migrate

# 3. Copy your API keys from augur_operations.worker_oauth into aveloxis_ops.worker_oauth.
aveloxis add-key --from-augur

# 4. Import your repos from augur_data.repo.
#    Each URL is verified against the forge via HTTP HEAD — dead repos are skipped.
aveloxis add-repo --from-augur

# 5. Start collecting. Open http://localhost:8082/monitor to watch progress.
aveloxis start all
```

After step 3, your keys live in `aveloxis_ops.worker_oauth` and are loaded automatically — no `--augur-keys` flag needed going forward. After step 4, all your verified Augur repos are in the Aveloxis queue and will be collected on the scheduler's priority order.

## Quick Start (Fresh Install)

```bash
# 1. Create a config file
cp aveloxis.example.json aveloxis.json
# Edit aveloxis.json with your database credentials and API tokens
#
# IMPORTANT for local development: set "dev_mode": true in the "web" section
# so session cookies work over plain HTTP (without HTTPS).
# See the "Development Mode" note below.

# 2. Create the database schemas and tables
aveloxis migrate

# 3. Store your API keys in the database
aveloxis add-key ghp_your_github_token --platform github
aveloxis add-key glpat-your_gitlab_token --platform gitlab

# 4. Add repos to the collection queue (CLI method)
aveloxis add-repo https://github.com/chaoss/augur https://gitlab.com/fdroid/fdroidclient

# -- OR use the web GUI to add repos and orgs via browser --
# Configure OAuth credentials in aveloxis.json (see Configuration),
# then run: aveloxis web
# Open http://localhost:8082, log in with GitHub/GitLab, create a group,
# and add repos or orgs through the UI.

# 5. Start the scheduler
aveloxis start serve

# Open http://localhost:8082/monitor to watch collection progress
```

## Configuration

Create `aveloxis.json` (or copy from `aveloxis.example.json`).

**The canonical reference for every supported `aveloxis.json` field** lives at [`docs/getting-started/configuration.md`](./docs/getting-started/configuration.md) (also rendered at the ReadTheDocs site). That document has the full table — every supported field across every section — including all the v0.18.x REST→GraphQL options (`pr_child_mode`, `listing_mode`, `threading_mode`, `shard_size`), the v0.18.29+ periodic-task cadence knobs (`enrich_interval_minutes`, `search_resolve_interval_minutes`, `affiliation_interval_minutes`), v0.20.0's `shutdown_grace_seconds`, v0.21.0's decoupled scancode worker (`scancode_workers`, `scancode_cadence_days`, `scancode_start_interval_s`, `scancode_clone_dir`, `scancode_shutdown_grace_minutes` — see [`docs/architecture/scancode.md`](./docs/architecture/scancode.md) for the design), and the v0.19.0 Gmail-SMTP `mail` block.

A minimal example to get started; see `aveloxis.example.json` for an all-fields template and the doc above for the full reference:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "aveloxis",
    "password": "your-password",
    "dbname": "aveloxis",
    "sslmode": "prefer"
  },
  "github": {
    "api_keys": ["ghp_your_token_here"]
  },
  "gitlab": {
    "api_keys": ["glpat-your_token_here"]
  },
  "collection": {
    "workers": 12,
    "repo_clone_dir": "/data/aveloxis-repos"
  },
  "web": {
    "addr": ":8082",
    "base_url": "http://localhost:8082",
    "session_secret": "change-me-to-a-random-string",
    "dev_mode": false,
    "github_client_id": "your-github-oauth-app-client-id",
    "github_client_secret": "your-github-oauth-app-client-secret"
  },
  "log_level": "info"
}
```

The settings most operators tune (defaults shown for reference):

| Field | Default | Notes |
|---|---|---|
| `database.*` | localhost:5432 | PostgreSQL connection. |
| `github.api_keys` / `gitlab.api_keys` | `[]` | Round-robin rotated. Production should prefer the `worker_oauth` table via `aveloxis add-key`. |
| `collection.workers` | `12` | Concurrent collection goroutines. pgx pool sizes to `max(workers + 15, 20)`. |
| `collection.days_until_recollect` | `1` | After collection, `due_at = last_collected + days_until_recollect`. |
| `collection.repo_clone_dir` | `$HOME/aveloxis-repos` | Bare clones. Plan TB-scale for large fleets. |
| `collection.pr_child_mode` | `"graphql"` | GraphQL is the default GitHub path (~5× faster, v0.26.0+); set `"rest"` as the escape hatch. |
| `collection.listing_mode` | `"graphql"` | Default since v0.26.0; `"rest"` restores the repo-wide REST issue/PR scans. |
| `collection.threading_mode` | `"single"` | Set to `"sharded"` with `pr_child_mode=graphql` to parallelize large-repo PR batches (v0.18.3+). |
| `collection.matview_rebuild_day` | `"saturday"` | Or `"disabled"` to turn off scheduled rebuilds. |
| `web.dev_mode` | `false` | Set `true` for local HTTP development (see Development Mode below). Never enable in production. |
| `log_level` | `"info"` | `debug` / `info` / `warn` / `error`. |

Every other setting (mail/SMTP, periodic-task cadences, OAuth credentials, shutdown grace, etc.) is documented in [`docs/getting-started/configuration.md`](./docs/getting-started/configuration.md).

### Development Mode

> **If you are developing locally over HTTP** (the typical case), you must set `"dev_mode": true` in the `"web"` section of `aveloxis.json`. Without this, session cookies are marked `Secure` and your browser will refuse to send them over plain `http://localhost`, making login fail silently.
>
> ```json
> "web": {
>   "dev_mode": true,
>   ...
> }
> ```
>
> **Do not enable `dev_mode` in production.** In production, run behind a TLS-terminating reverse proxy (nginx, Caddy) and leave `dev_mode` at its default (`false`) so cookies are only sent over HTTPS. `HttpOnly` is always enabled regardless of this setting.

### API Key Sources and Rotation

Keys are loaded from three sources, merged together:

1. **`aveloxis_ops.worker_oauth`** — always checked. Store keys via `aveloxis add-key`.
2. **`augur_operations.worker_oauth`** — checked when `--augur-keys` flag is set.
3. **`aveloxis.json`** — lowest priority, for standalone deployments without a database pre-populated with keys.

**Startup validation:** `aveloxis serve` and `aveloxis collect` require at least one API key (GitHub or GitLab) to start. If no keys are found in any source, the process exits with a clear error message. If keys are found for only one platform, a warning is logged but the process continues (repos for the unconfigured platform will not be collected). If key loading fails (e.g., database connection error), the error is logged at ERROR level.

**Key rotation:** All keys are rotated via round-robin so every key's rate limit is fully utilized. When a key's remaining requests drop to the buffer threshold (default: 15), it's skipped until its rate-limit window resets. With N tokens at 5000 req/hr each, total throughput is N * ~4985 req/hr. For example, 74 tokens give ~368K lookups/hour. Keys that return 401 (bad credentials) are permanently invalidated.

## Commands

### `aveloxis serve` — Run the collection scheduler

Starts the long-running scheduler that continuously collects repos from the queue. Uses the **staged collection pipeline** (see Architecture). The monitor dashboard is integrated into `aveloxis web` at `/monitor`.

```bash
aveloxis serve [flags]

Flags:
  --workers int      Concurrent collection workers (default 1)
  --augur-keys       Load API keys from Augur's worker_oauth table
```

The scheduler uses a **Postgres-backed priority queue** (`aveloxis_ops.collection_queue`). Jobs are claimed atomically with `SELECT ... FOR UPDATE SKIP LOCKED`, so multiple Aveloxis instances can share the same queue for horizontal scaling. No Redis, no RabbitMQ, no Celery.

**Restart/resume:** Aveloxis is safe to stop and restart at any time. On shutdown (`Ctrl-C` / `SIGTERM` / `pkill aveloxis`), it waits for active API calls to finish, then releases all queue locks so repos go back to `queued` immediately. On startup, it automatically:
- Processes any leftover staged data from the interrupted run into relational tables (so you don't lose what was already fetched from the API)
- Releases any stale locks from a previous instance
- Repos that were mid-collection resume from the beginning of their current collection cycle, but data already in the relational tables is upserted (duplicates are harmless)

### `aveloxis web` — Start the web GUI

Starts the web GUI for group management with OAuth login. Users can log in via GitHub or GitLab, create groups, and add repositories or entire organizations to those groups for collection.

```bash
aveloxis web
```

No flags -- all configuration comes from the `web` section of `aveloxis.json` (see Configuration below). Default listen address is `:8082`.

Requires OAuth app credentials in `aveloxis.json`. Create a GitHub OAuth app at [https://github.com/settings/developers](https://github.com/settings/developers) or a GitLab OAuth app at [https://gitlab.com/-/profile/applications](https://gitlab.com/-/profile/applications). Set the callback URL to `{web.base_url}/auth/github/callback` or `{web.base_url}/auth/gitlab/callback` respectively.

### `aveloxis collect` — One-shot collection (no queue)

For ad-hoc collection of specific repos without the scheduler. Since v0.26.2 this delegates to the same **staged collection pipeline** `serve` uses (staging → processing), so one-shot runs and scheduled runs produce identical data. Best for testing or collecting a handful of repos.

```bash
# Incremental (only data since last collection window)
aveloxis collect https://github.com/chaoss/augur

# Full historical collection
aveloxis collect --full https://github.com/chaoss/augur

# Multiple repos, mixed platforms
aveloxis collect \
  https://github.com/torvalds/linux \
  https://gitlab.com/fdroid/fdroidclient

Flags:
  --full             Full historical collection (ignore recollect window)
  --augur-keys       Load API keys from Augur's worker_oauth table
```

### `aveloxis add-repo` — Add repos to the queue

```bash
# Add at default priority (100)
aveloxis add-repo https://github.com/chaoss/augur

# Add at high priority (lower number = collected sooner)
aveloxis add-repo --priority 10 https://gitlab.com/gitlab-org/gitlab

# Add multiple repos at once
aveloxis add-repo \
  https://github.com/torvalds/linux \
  https://github.com/chaoss/grimoirelab \
  https://gitlab.com/fdroid/fdroidclient

# Import all repos from an existing Augur installation (verifies each URL is alive)
aveloxis add-repo --from-augur
```

Platform is auto-detected from the URL. GitLab nested subgroups are supported:
```
https://gitlab.com/group/subgroup/project  ->  owner="group/subgroup", repo="project"
```

Self-hosted GitLab instances are recognized if the hostname contains "gitlab" or is listed in `gitlab_hosts` in the config.

### `aveloxis add-key` — Store API keys

```bash
# Store a GitHub token
aveloxis add-key ghp_your_github_token --platform github

# Store a GitLab token
aveloxis add-key glpat-your_gitlab_token --platform gitlab

# Bulk import all keys from Augur (duplicates are skipped)
aveloxis add-key --from-augur
```

### `aveloxis prioritize` — Push a repo to the top

```bash
aveloxis prioritize https://github.com/chaoss/augur
```

Sets priority to 0 and due time to now. The scheduler will collect this repo next.

Also available via the monitor dashboard's "Boost" button at `/monitor`, or by clicking "Boost" next to any queued repo.

### `aveloxis recollect` — Flag a repo for full (since=zero) re-collection

```bash
aveloxis recollect https://github.com/chaoss/augur
aveloxis recollect https://github.com/a/b https://github.com/c/d   # batch
```

Sets `force_full_collect` on the named repos' queue rows. On each repo's next scheduler cycle the collector ignores `last_collected` and re-collects from the beginning of time; the flag clears itself on successful completion. Use this after a bugfix that invalidates collected data, or when you want to make sure a specific repo is fully refreshed.

The scheduler also sets this flag **automatically** when a collection ends with a GraphQL PR batch error class (stream CANCEL, validation timeout, retry exhaustion) so that next pass backfills whatever the failed batch missed. See [docs/guide/troubleshooting.md](docs/guide/troubleshooting.md) for details.

Combine with `aveloxis prioritize <url>` if you want the re-collection to start immediately rather than on the repo's normal cycle.

### `aveloxis migrate` — Set up the database schema

```bash
aveloxis migrate
```

Creates 143 tables and 20 materialized views across three PostgreSQL schemas:
- **`aveloxis_data`** (100 tables + 20 materialized views) — all collected data plus analytics views
- **`aveloxis_ops`** (39 tables) — operational tables: collection queue, JSONB staging store, collection status, API credentials, users/auth, config, worker state
- **`aveloxis_scan`** (4 tables) — scancode per-file license/copyright results and history
- **`aveloxis_augur_data`** (6 views) — Augur compatibility layer for 8Knot. Contains views that alias Aveloxis column names to Augur conventions (e.g., `star_count` → `stars_count`, `pr_number` → `pr_src_number`). Only tables with column name differences have views here; identical tables resolve via search_path fallback to `aveloxis_data`.

Safe to run repeatedly. Does not touch Augur schemas if sharing a database. Also creates 20 materialized views for 8Knot/analytics compatibility and runs a data cleanup pass that fixes any garbage timestamps (e.g., year 0001 BC from uninitialized fields) by setting them to NULL.

**8Knot integration:** Set `AUGUR_SCHEMA=aveloxis_augur_data,aveloxis_data` (no space after comma) in 8Knot's `.env`. The two-schema search path resolves Augur-named columns from `aveloxis_augur_data` first, then falls through to `aveloxis_data` for tables with identical schemas. For existing Augur databases, use `AUGUR_SCHEMA=augur_data` as before — the compatibility schema is not needed.

### `aveloxis refresh-views` — Refresh materialized views

```bash
aveloxis refresh-views
```

Manually refreshes all 20 materialized views used by [8Knot](https://github.com/oss-aspen/8Knot) and other analytics tools. Uses `REFRESH MATERIALIZED VIEW CONCURRENTLY` where unique indexes exist (doesn't block reads). Views are also rebuilt automatically on a configurable schedule by `aveloxis serve` (default: Saturday; set `collection.matview_rebuild_day` in `aveloxis.json` to change, or `"disabled"` to turn off).

### `aveloxis install-tools` — Install all optional analysis tools

```bash
aveloxis install-tools
```

Installs all optional third-party tools used by Aveloxis. Each tool is independently optional — if not installed, its analysis phase is silently skipped.

| Tool | Install command | Purpose |
|---|---|---|
| [scc](https://github.com/boyter/scc) | `go install github.com/boyter/scc/v3@latest` | Code complexity analysis — populates `repo_labor` with lines of code, comments, blanks, and complexity per file per language |
| [scorecard](https://github.com/ossf/scorecard) | `go install github.com/ossf/scorecard/v5/cmd/scorecard@latest` | OpenSSF Scorecard — evaluates security practices (Code-Review, Maintained, Vulnerabilities, etc.) and populates `repo_deps_scorecard` |
| [scancode](https://github.com/aboutcode-org/scancode-toolkit) | `pipx install scancode-toolkit-mini` (+ `pipx inject scancode-toolkit-mini typecode-libmagic`) | Per-file license and copyright detection — populates `aveloxis_scan.scancode_file_results` with SPDX license expressions, copyrights, holders, and package data. Runs on its own decoupled worker pool every 180 days per repo (`scancode_cadence_days`). Requires Python 3.10+ and libmagic (`brew install libmagic` on macOS, `apt-get install libmagic1` on Debian/Ubuntu). |

Tools that are already installed are skipped. The command verifies each tool is on PATH after installation.

**Automatic updates:** On scheduler startup, Aveloxis checks if it has been more than 30 days since the last tool update. If so, it updates each installed tool (`go install ...@latest` for scc, the latest release tarball for scorecard, `pipx upgrade` + libmagic re-inject for scancode). Only tools already on PATH are updated — missing tools are not auto-installed. `aveloxis upgrade-tools` runs the same update pass on demand. The check timestamp is stored at `~/.aveloxis-tool-check`.

### `aveloxis start` — Start background processes

```bash
aveloxis start serve   # scheduler + monitor → ~/.aveloxis/aveloxis.log
aveloxis start web     # web GUI             → ~/.aveloxis/web.log
aveloxis start api     # REST API            → ~/.aveloxis/api.log
aveloxis start all     # all three at once
```

Launches the specified component(s) as detached background processes. Output is appended to log files in `~/.aveloxis/`. PID files are written to `~/.aveloxis/aveloxis-{serve,web,api}.pid` for reliable process tracking. If a component is already running, the command reports it and skips the launch.

### `aveloxis stop` — Stop background processes

```bash
aveloxis stop serve    # stop only the scheduler
aveloxis stop web      # stop only the web GUI
aveloxis stop api      # stop only the REST API
aveloxis stop all      # stop all three
aveloxis stop          # (no args) same as 'all'
```

Sends SIGTERM to the specified component(s) using PID files in `~/.aveloxis/`. Active workers finish their current API call, queue locks are released, and staging data is preserved. PID files are cleaned up automatically. Stale PID files (process no longer running) are detected and removed.

### `aveloxis sbom` — Generate Software Bill of Materials

Generates a [CycloneDX](https://cyclonedx.org/) 1.5 or [SPDX](https://spdx.dev/) 2.3 SBOM from the dependency data collected for a repository. The repo must have been collected with dependency/libyear analysis (runs automatically during `aveloxis serve`).

```bash
# Generate CycloneDX JSON to stdout
aveloxis sbom 42

# Generate SPDX JSON
aveloxis sbom 42 --format spdx

# Write to file
aveloxis sbom 42 -o sbom.json

# Store in database (repo_sbom_scans table)
aveloxis sbom 42 --store

# Both file and database
aveloxis sbom 42 -o sbom.json --store

Flags:
  --format string   Output format: cyclonedx or spdx (default "cyclonedx")
  -o, --output      Write to file instead of stdout
  --store           Also store the SBOM in repo_sbom_scans table
```

**What's in the SBOM:**

| Format | Contents |
|---|---|
| **CycloneDX 1.5** | bomFormat, specVersion, tool metadata (aveloxis), root component with `evidence.licenses` (concluded from ScanCode source analysis) and `evidence.copyright` (detected holders), all dependencies as library components with purl, version, license, and a scope that follows the dependency's real role (runtime → `required`, optional/peer → `optional`, dev/test/build → `excluded`) |
| **SPDX 2.3** | CC0-1.0 data license, root package with `licenseConcluded` from ScanCode source analysis (vs. `licenseDeclared` from registry), `copyrightText` from detected holders, all dependencies as packages with purl external refs, and scope-typed relationships (`DEPENDS_ON` for runtime; `DEV_`/`TEST_`/`BUILD_`/`OPTIONAL_DEPENDENCY_OF` and `PROVIDED_DEPENDENCY_OF` for the rest) |

Both formats support `?scope=runtime` (only the shipped surface) and `?vulns=1` (annotate with the repo's current unresolved findings — CycloneDX via its native vulnerabilities array, SPDX via SECURITY/advisory external references).

**License capture** from 12 package registries:

| Registry | Ecosystem | License source |
|---|---|---|
| npm | JavaScript | `license` field from `npm view` JSON |
| PyPI | Python | `info.license` from pypi.org API |
| crates.io | Rust | `license` field from crates.io version data |
| RubyGems | Ruby | `licenses` array from rubygems.org API |
| Go proxy | Go | Not available from proxy (would need pkg.go.dev) |
| Maven Central | Java/Scala | `timestamp` from search API |
| Packagist | PHP | `license` array from repo.packagist.org API |
| Hex.pm | Elixir | `licenses` from hex.pm API |
| NuGet | .NET | `licenseExpression` from nuget.org registration API |
| pub.dev | Dart/Flutter | From pub.dev API |
| Hackage | Haskell | Upload time from hackage.haskell.org |
| GitHub Releases | Swift (SwiftPM) | Via GitHub releases API (no central registry) |

**Package URLs (purl)** are generated for each dependency following the [purl spec](https://github.com/package-url/purl-spec): `pkg:npm/express@4.18.0`, `pkg:pypi/flask@2.3.0`, `pkg:golang/github.com/spf13/cobra@1.8.1`, `pkg:cargo/serde@1.0`, `pkg:gem/rails@7.0`, `pkg:maven/junit/junit@4.13`, `pkg:composer/laravel/framework@10.0`, `pkg:hex/phoenix@1.7.0`, `pkg:nuget/Newtonsoft.Json@13.0.3`, `pkg:pub/http@0.13.6`, `pkg:hackage/aeson@2.0`.

## Monitoring Dashboard

The monitor dashboard is integrated into the web GUI at `/monitor` (requires login). It shows:

- Queue statistics (total, queued, collecting)
- Every repo with: status, priority, due time, last run duration
- **Gathered vs Metadata columns**: Gathered Issues, Meta Issues, Gathered PRs, Meta PRs, Gathered Commits, Meta Commits — so you can see collection completeness at a glance
- A **Boost** button to push any queued repo to the top
- Pagination at 200 repos per page
- Auto-refreshes every 60 seconds by default (configurable via `monitor.refresh_seconds`)

Navigate to the monitor from any page via the "Monitor" link in the top nav bar.

### REST API (`aveloxis api`)

Separate process (default `127.0.0.1:8383`). Start alongside `serve` and `web`.

The API has grown far past this README — the full reference lives at [`docs/guide/api.md`](docs/guide/api.md) (every route is covered there, tripwire-enforced). Families:

| Family | Highlights |
|---|---|
| Repo data | `/repos/{id}/stats` (gathered vs metadata, gone state), batch `/repos/stats?ids=` (≤500 ids), `/timeseries`, `/licenses` (+ drill-down via `/deps?license=`), `/scancode-files`, `/scorecard`, `/vulnerabilities` (kind/scope/resolution labels, `introduced_by` chains, `scanned_at`), `/sbom?format=cyclonedx\|spdx` (`?vulns=1` annotation, real dependency graphs) |
| Contributors | `/repos/{id}/contributors/top`, `/contributors/elsewhere`, `/contributions/{identities,affiliations,coverage}`, `/contributors/{id}/activity` |
| Analytics | `/metrics` catalog (CHAOSS-derived), `/compare` + `/compare/snapshot` (≤7 entities, orgs included), `/entities/search`, ~30 Augur-swagger-compatible metric routes |
| Workspace | `/me`, `/groups`, `/home/repos`, `/home/new-repos`, `/collections`, repo + collection stars, `/admin/*` (users, approvals, monitor) |
| Auth & limits | Bearer session tokens (minted by the web process at `/auth/token`), per-user repo scope, per-IP rate limiting + daily quota, CORS allowlist; `/api/v1/health` and `/api/v1/public/stats` are the only unauthenticated routes |

## Web GUI

The web GUI (`aveloxis web`) provides a browser-based interface for managing repository groups with OAuth authentication.

- **OAuth login flow:** Users authenticate via GitHub or GitLab OAuth apps. The login redirects to the provider's authorization page, then back to the callback URL with an auth code that is exchanged for an access token. The token is used to fetch the user's profile (login, email, avatar).
- **Group management:** Authenticated users create named groups and add repos or entire GitHub orgs / GitLab groups. Repos are automatically queued for collection.
- **Bulk repo paste:** The add-repo form accepts a textarea with line-delimited URLs — paste a list and they're all added at once.
- **Gathered vs metadata stats:** Each repo shows Gathered Issues, Meta Issues, Gathered PRs, Meta PRs, Gathered Commits, Meta Commits — collection completeness at a glance.
- **SBOM download:** CDX and SPDX download buttons per repo, generating SBOMs on-the-fly. Authenticated users only.
- **Breadcrumb navigation:** Home / Group Name hierarchy. 25-per-page pagination with 5-page sliding window and case-insensitive search.
- **Org tracking:** When a user adds an org, a scheduler task scans it every 4 hours, discovers new repos, and queues them automatically.
- **URL validation:** All URLs are validated before adding. GitHub and GitLab URLs require owner/repo path. Other URLs are accepted as git-only repos.
- **Session management:** Sessions are in-memory with 24-hour expiry. Restarting `aveloxis web` clears all sessions.

The web GUI runs as a separate process from `aveloxis serve` — they share the same database but do not need to run on the same host. **Note:** `aveloxis web` does NOT run migrations. Use `aveloxis migrate` or `aveloxis serve` for that.

### Interactive Visualizations

The web GUI includes built-in interactive visualizations powered by [Chart.js](https://www.chartjs.org/), loaded from CDN with no build step. Requires the REST API (`aveloxis api`) to be running alongside `aveloxis web`.

**Repository detail page** — clicking a repo name in a group opens `/groups/{gid}/repos/{rid}` with:
- **4 weekly time-series charts:** Commits/week, PRs Opened/week, PRs Merged/week, Issues/week (last 2 years by default)
- **Summary stat cards:** Issues, PRs, Commits, Vulnerabilities (critical count highlighted)
- **Dependency license table:** All licenses in the project's dependency tree with counts and OSI compliance indicators (checkmark for [OSI-approved](https://opensource.org/licenses/) licenses)
- **Source code license table:** Per-file license detections from ScanCode with SPDX expressions, file counts, OSI compliance, and copyright holders list
- **SBOM download buttons:** CycloneDX 1.5 and SPDX 2.3

**Comparison page** (`/compare`) — accessible from the dashboard home page:
- **Search any repo** in the database via autocomplete
- **Select up to 5 repos** — each shown as a color-coded tag
- **Three comparison modes:**
  - **Raw Counts** — actual weekly values, best for repos of similar size
  - **100%** — each repo normalized so its peak week = 100%, best for comparing trends regardless of size
  - **Z-Score** — values as standard deviations from the mean, best for comparing trends while explicitly controlling for community size differences
- **4 overlaid charts** with mode toggle, one per metric
- **URL-shareable:** `/compare?repos=1,2,3` pre-populates the selection

The design follows the [GHData/CHAOSS visualization principles](https://wiki.linuxfoundation.org/oss-health-metrics/start): temporal context for all data, cross-project comparison with normalization, and rapid iteration.

## Generic Git Repository Support

Aveloxis can collect data from any git-hosted repository, not just GitHub and GitLab. When a user enters a URL that doesn't match `github.com` or `gitlab.com` (e.g., `https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git`), it is accepted as a **git-only** repository.

**What's collected for git-only repos:**
- Git commits (bare clone + `git log --numstat` on the default branch) — full commit history with per-file stats
- Commit messages and parent relationships
- Dependencies (from manifest files in the checkout)
- Libyear (dependency age from package registries)
- Code complexity (scc)
- OpenSSF Scorecard
- SBOM generation (CycloneDX + SPDX)

**What's NOT collected** (requires a forge API):
- Issues, pull requests, events, messages, releases, repo info metadata
- Contributors (from API — git authors are still resolved via email)

**Commit author resolution for git-only repos:** Aveloxis attempts to resolve commit author emails against both the GitHub Search API and GitLab API to find platform identities. This means if a contributor uses the same email on GitHub and on a self-hosted Gitea instance, their identity can still be linked.

**In the web GUI:** Git-only repos are marked with a purple **Git-only** badge in the repository list.

**URL validation:** All URLs entered in the web GUI are validated. GitHub and GitLab URLs must have an owner/repo path. Other URLs are accepted if they have a valid host and path structure — the scheduler will attempt to clone them and report an error if cloning fails.

## Architecture

### Collection Pipelines

Aveloxis has one collection pipeline — the **staged pipeline** — used by both `aveloxis serve` (scheduled) and `aveloxis collect` (one-shot).

#### Staged Pipeline (`serve`)

Designed for 400K+ repos. Eliminates database contention on the contributors table by decoupling API collection from relational persistence:

**Prelim phase:** Before any data collection, each repo's URL is checked with an HTTP HEAD request. If the URL redirects (repo was renamed or transferred):
- If the new URL already exists in our database: the old repo is marked as a duplicate and dequeued. This prevents collecting the same repo twice.
- If the new URL is new: the old repo's URL is updated to the canonical URL, and all stored URLs in issues, PRs, reviews, releases are bulk-updated via `REPLACE()` to reflect the new org/repo path.
- If the URL returns 404/410: the repo is skipped as dead.

**Phase 1 — Collect (fast, no contention):** Raw API responses are written to a JSONB staging table (`aveloxis_ops.staging`). No FK lookups, no contributor resolution. Multiple workers can blast data concurrently with zero contention on any relational table. Issues and PRs are staged as **envelope types** that bundle the parent entity with all its children (labels, assignees, reviewers, reviews, commits, files, head/base metadata) in a single JSONB row. Data is collected in this order:

1. Repo info, releases, clone/traffic stats (collected first for commit count metadata)
2. Contributors (seed from member/contributor lists)
3. Issues + labels + assignees (bundled per issue)
4. Pull requests + all children (bundled per PR)
5. Events (issue + PR)
6. Messages (issue comments, PR comments, inline review comments)

For repos with **>10,000 commits** (detected from repo_info metadata), steps 3-5 run in **parallel** across 3 goroutines (each with its own staging writer), then messages are collected after all three complete.

**Heartbeat locking:** During staged collection, workers send heartbeats every 30 seconds (`HeartbeatJob`) to update `locked_at`. This prevents `RecoverStaleLocks` (1-hour timeout) from stealing active jobs on large repos that take hours to collect. Without heartbeats, the stale lock recovery would repeatedly reclaim the lock and purge accumulated staging data.

**Phase 2 — Process (single-threaded per repo):** Staged data is drained in 500-row batches by entity type, in dependency order (contributors first, then issues, then PRs, then events/messages, then metadata). Contributors are resolved in bulk with an in-memory write-through cache (platform ID -> email -> login deduplication). When an envelope is processed, the parent is upserted first to obtain its database ID, then all bundled children are upserted using that ID.

- **Review messages**: PR review bodies are stored in the `messages` table with a link in `pull_request_review_message_ref` — the same bridge-table pattern used for issue comments (`issue_message_ref`) and PR comments (`pull_request_message_ref`). Only reviews with non-empty bodies get a message row.
- **Repo info rotation**: Before inserting a new `repo_info` snapshot, the previous snapshot is moved to `repo_info_history`, preserving all metadata columns. The main `repo_info` table always has only the latest data per repo.
- **Repo info source**: GitHub uses a GraphQL query that returns PR/issue/commit counts, community profile files (CONTRIBUTING.md, CHANGELOG.md, CODE_OF_CONDUCT.md, SECURITY.md), license, and archive status in one API call. GitLab uses REST (`/projects/{id}?statistics=true`) plus `/issues_statistics` for issue breakdowns and per-state MR counts via `X-Total` headers.

**Phase 2b — Gap fill:** After processing, gathered issue/PR counts are compared against `repo_info` metadata. If the gap exceeds 5%, all issue/PR numbers are listed from the API and diffed against collected numbers in the database. Only the specific missing items are fetched, plus 2 already-collected items on each side of each gap to verify their associated data (comments, events, reviews) is complete. Handles multiple distinct gaps per repo. This catches incomplete collections from any cause — interrupted runs, API errors, rate limit exhaustion.

**Phase 2c — Contributor enrichment:** Thin contributor records (missing company/location from the basic Contributors API or lazy resolution) are enriched by calling `GET /users/{login}` for full profile data (company, location, email, name, created_at). Up to 500 contributors per pass — over multiple collection cycles, all contributors eventually get enriched.

**Phase 3 — Facade (git):** After API data is processed, the repo is cloned as a bare repo (or fetched if a clone already exists). `git log --numstat` is run on the **default branch only** (via `git symbolic-ref HEAD`), so the gathered commit count matches the forge's metadata count. For each commit:
- Per-file rows are inserted into `commits` (one row per file touched per commit, matching Augur's model)
- Parent-child relationships are inserted into `commit_parents`
- Commit messages are inserted into `commit_messages`
- **Whitespace analysis** (Augur-parity): blank-line and reformat-only changes are measured per commit into `cmt_whitespace`, with `cmt_added`/`cmt_removed` adjusted to Augur's semantics
- **Contributor affiliations** are resolved: email domains are matched against the `contributor_affiliations` table to populate `cmt_author_affiliation` and `cmt_committer_affiliation`
- After all commits are inserted, **Facade aggregates** are computed: `dm_repo_annual`, `dm_repo_monthly`, `dm_repo_weekly` (and their repo_group counterparts) are refreshed by aggregating commit data by email, affiliation, and time period

**Phase 4 — Analysis (on-demand full clone):** A temporary full checkout is created from the bare clone (local, no network). Five analysis phases run against it, then the checkout is retained for scorecard before deletion:

1. **Dependency scanning** (`repo_dependencies`): walks the checkout for manifest files across 14 ecosystems — JavaScript (package.json), Python (requirements.txt, pyproject.toml, Pipfile), Go (go.mod), Rust (Cargo.toml), Ruby (Gemfile), Java/Kotlin (pom.xml, build.gradle, build.gradle.kts), PHP (composer.json), Elixir (mix.exs), Swift (Package.swift), Dart (pubspec.yaml), Scala (build.sbt), .NET (packages.config), Haskell (package.yaml), C/C++ (Makefile, CMakeLists.txt). Extracts dependency names and counts. Additionally, 19 lockfile formats (package-lock, yarn incl. berry, pnpm, poetry, Cargo.lock, Gemfile.lock, composer.lock, and more) are parsed for locked versions, the **transitive dependency closure**, and parent→child dependency edges (`vuln_scan_transitive`, default on since v0.27.136) — feeding exact-version vulnerability matching, `introduced_by` chains, and real dependency graphs in SBOMs.
2. **Libyear** (`repo_deps_libyear`): for each versioned dependency, queries its package registry (npm, PyPI, Go proxy, crates.io, RubyGems, Maven Central, Packagist, Hex.pm, NuGet, pub.dev, Hackage, SwiftPM/GitHub) to compare the current version against the latest. Calculates libyear = (latest_release_date - current_release_date) / 365.
3. **Code complexity** (`repo_labor`): if `scc` is installed, runs `scc -f json --by-file` to get per-file metrics — programming language, total lines, code lines, comment lines, blank lines, and complexity. Install via `aveloxis install-tools`.
4. **ScanCode license/copyright detection** (`aveloxis_scan.scancode_file_results`): runs on its **own decoupled worker pool** (v0.21.0), not inside the per-repo pipeline — each repo is scanned from a fresh shallow clone every 180 days (`scancode_cadence_days`), with crash recovery, adaptive timeouts, and failure backoff. Detects per-file licenses (SPDX expressions), copyrights, holders, and packages; results stored in the dedicated `aveloxis_scan` schema with history rotation. Install via `aveloxis install-tools` (requires Python 3.10+).
5. **OpenSSF Scorecard** (`repo_deps_scorecard`): if the `scorecard` binary is installed, GitHub repos run **remote-first** (`--repo`, 18 checks, multi-token round-robin) with automatic fallback to `--local` against the checkout; GitLab and generic-git repos run local-only (11 checks). Each check (Code-Review, Maintained, Vulnerabilities, etc.) is stored with its score, reason, and details as JSONB. Previous results are rotated to `repo_deps_scorecard_history`. Install via `aveloxis install-tools`.

**Phase 5 — Commit Author Resolution (GitHub only):** After facade completes, resolves git commit author emails to GitHub user accounts. This is the Go implementation of the [augur-contributor-resolver](https://github.com/aveloxis/augur-contributor-resolver) scripts. Resolution strategy, cheapest first:

1. **Noreply email parse** (free) — `12345+user@users.noreply.github.com` extracts login and `gh_user_id` directly from the email format
2. **Database lookup** — checks `contributors` (cntrb_email, cntrb_canonical) and `contributors_aliases` (alias_email)
3. **GitHub Commits API** — `GET /repos/{owner}/{repo}/commits/{sha}` returns the linked GitHub user with all profile fields (`gh_user_id`, `gh_node_id`, `gh_avatar_url`, all `gh_*` URLs, etc.)
4. **GitHub Search API** — `GET /search/users?q=email+in:email` for remaining non-noreply emails

For each resolved commit author:
- `cmt_author_platform_username` is set on all commit rows with that hash
- The contributor row is created/updated with the deterministic `GithubUUID` (Augur-compatible) and all `gh_*` profile fields are backfilled
- Login renames are detected (same `gh_user_id`, different login) and the contributor's `gh_login` is updated
- An alias is created in `contributors_aliases` linking the commit email to the contributor
- After all commits are resolved, a bulk SQL backfill sets `cmt_ght_author_id` by joining `cmt_author_platform_username` to `contributors.gh_login`

**Phase 6 — Canonical Email Enrichment:** For contributors that have `gh_login` but no `cntrb_canonical`, calls `GET /users/{login}` to get their profile email and sets `cntrb_canonical`.

**Phase 7 — SBOM Generation:** Both CycloneDX 1.5 and SPDX 2.3 SBOMs are generated from the `repo_deps_libyear` data and stored in `repo_sbom_scans` with format metadata. SBOMs include dependency names, versions, licenses, and package URLs from all 12 registries. Available for download via the web GUI or REST API.

**Phase 8 — Vulnerability Scanning (OSV.dev):** All dependencies with package URLs (purls) are batch-queried against the [OSV.dev](https://osv.dev) API to identify known vulnerabilities. Findings carry three distinguishing labels: `dependency_kind` (a problem in the project's own releases, in a direct dependency, or anywhere in the transitive lockfile closure), `dependency_scope` (shipped runtime code vs dev/test/build tooling), and `version_resolution` (whether the exact affected version is known or only a declared range floor — a finding is never presented as more certain than the evidence supports). OSV aggregates data from NVD (CVEs), GitHub Advisory Database (GHSA), PyPI advisories, RustSec, Go Vulnerability Database, and OSS-Fuzz — providing comprehensive coverage across all supported ecosystems. Results are stored in `repo_deps_vulnerabilities` with:
- Vulnerability ID (GHSA, PYSEC, RUSTSEC, GO, etc.) and CVE cross-reference
- CVSS severity and score (approximated from vector)
- Affected and fixed version ranges
- Summary, details, and reference URLs
- Source attribution

The OSV.dev batch endpoint (`POST /v1/querybatch`) accepts purls natively — no CPE mapping needed. No API key required. NIST NVD is not queried directly because it uses CPE identifiers where the vendor field is unpredictable from package names alone.

**Periodic — Contributor Breadth:** Every 15 minutes (`breadth_interval_minutes`), the scheduler runs the breadth worker which calls `GET /users/{login}/events` for each contributor to discover their activity in repos outside the tracked set. Each event (PushEvent, PullRequestEvent, IssuesEvent, etc.) is stored in `contributor_repo`, mapping contributors to their cross-repo activity. Contributors are prioritized by those never attempted first, then by a 7-day jittered cooldown (`breadth_cooldown_days`); up to 2,000 contributors are processed per cycle (`breadth_batch_size`) through a concurrent fetch pool with a circuit breaker for GitHub 5xx storms.

#### One-shot collection (`collect`)

`aveloxis collect` runs the SAME staged pipeline as `serve` for the named repos (since v0.26.2 — the legacy direct-write path was removed after it was found to silently drop event rows). One-shot and scheduled collections are byte-equivalent; `--full` forces a since-zero historical pass.

### Postgres-Backed Queue

The scheduler queue lives in `aveloxis_ops.collection_queue` and uses `FOR UPDATE SKIP LOCKED` for atomic job claiming:

- **Durability**: Queue survives process restarts — no in-memory state lost
- **Horizontal scaling**: Multiple `aveloxis serve` instances can share the same queue
- **Transparency**: Queue state is queryable with plain SQL
- **Stale lock recovery**: Jobs locked by crashed workers are automatically re-queued after 1 hour
- **Priority override**: Any repo can be pushed to the top at any time via CLI or API
- **No extra infrastructure**: No Redis, RabbitMQ, or Celery

### Platform Abstraction

GitHub and GitLab implement the same `platform.Client` interface with 7 sub-interfaces:

| Sub-interface | Methods | Notes |
|---|---|---|
| `RepoCollector` | `FetchRepoInfo`, `FetchCloneStats` | Clone stats unavailable for GitLab via API |
| `IssueCollector` | `ListIssues`, `ListIssueLabels`, `ListIssueAssignees` | |
| `PullRequestCollector` | `ListPullRequests`, `ListPRLabels`, `ListPRAssignees`, `ListPRReviewers`, `ListPRReviews`, `ListPRCommits`, `ListPRFiles`, `FetchPRMeta` | GitLab MRs mapped to PR model |
| `EventCollector` | `ListIssueEvents`, `ListPREvents` | GitLab uses resource events API |
| `MessageCollector` | `ListIssueComments`, `ListPRComments`, `ListReviewComments` | GitLab review comments use `/merge_requests/:iid/discussions` with diff position filtering |
| `ReleaseCollector` | `ListReleases` | |
| `ContributorCollector` | `ListContributors`, `EnrichContributor` | GitLab combines `/members/all` + `/repository/contributors` |

All methods use Go 1.23 iterators (`iter.Seq2`) for memory-efficient streaming pagination.

### Contributor Resolution

There are two layers of contributor resolution:

**API-phase resolution** (during issue/PR/event collection): Platform user references (login, email, avatar, etc.) are resolved to a canonical `cntrb_id` UUID via a three-tier strategy:

1. **In-memory cache**: Platform ID -> UUID lookup (avoids DB round-trips for repeated contributors within a batch)
2. **Database lookup**: `contributor_identities` table (platform_id + platform_user_id unique key)
3. **Create new**: Insert into `contributors` + `contributor_identities` if no match found

**Git-phase resolution** (after facade commits are inserted): Commit author emails are resolved to GitHub user accounts. This is the equivalent of the [augur-contributor-resolver](https://github.com/aveloxis/augur-contributor-resolver) scripts, implemented natively in Go:

1. **Noreply parse** (free) — extract login + user ID from GitHub noreply email format
2. **DB lookup** — check contributors and aliases tables by email
3. **GitHub Commits API** — `GET /repos/{owner}/{repo}/commits/{sha}` for the linked GitHub user
4. **GitHub Search API** — `GET /search/users?q=email` for remaining emails
5. **Backfill** — bulk SQL join to set `cmt_ght_author_id` from resolved logins

### Deterministic Contributor IDs (GithubUUID)

Aveloxis generates `cntrb_id` UUIDs using Augur's deterministic scheme: the UUID encodes `platform_id` (byte 0) and `gh_user_id` (bytes 1-4, big-endian). This means:
- The same GitHub user always gets the same `cntrb_id` regardless of which system created it
- Aveloxis contributor IDs are byte-compatible with existing Augur data
- GitLab uses the same scheme with platform byte = 2

### Contributor Affiliation Resolution

During the facade phase, commit author/committer emails are matched against the `contributor_affiliations` table to resolve organizational affiliations. The resolver:

- Loads all active affiliation rules on first use (lazy, cached in memory)
- Matches exact domain first (e.g., `user@redhat.com` -> `Red Hat`)
- Falls back to parent domains (e.g., `user@mail.google.com` -> `Google` via `google.com`)
- Populates `cmt_author_affiliation` and `cmt_committer_affiliation` on every commit row

### Text Sanitization

All text fields (issue titles/bodies, PR titles/bodies, message text, release descriptions, review bodies, commit messages) are sanitized before database insertion. This mirrors Augur's `remove_null_characters_from_string()` and UTF-8 encoding cleanup:

- **Null bytes** (`\x00`) — removed (PostgreSQL TEXT cannot store them; these appear in bot-generated content and copy-pasted binary data)
- **Invalid UTF-8 sequences** — replaced with U+FFFD (Unicode replacement character)
- **Control characters** (C0: 0x01-0x1F except tab/newline/CR; C1: 0x7F-0x9F) — stripped
- Clean strings pass through without allocation (fast path)

### Dead Repo Sidelining

When the prelim phase detects a repo that returns 404 or 410 (deleted, made private, or DMCA'd):

- **Data is preserved** — all previously collected issues, PRs, commits, messages, etc. remain in the database
- **Collection stops permanently** — the repo is marked `repo_archived = TRUE`, stamped with the distinct `repo_gone_at` marker (v0.28.1 — "no longer reachable" is a different fact from "forge says archived"), and removed from the queue
- **No wasted API calls** — unlike Augur, which keeps retrying dead repos every cycle, Aveloxis permanently sidelines them
- **Resurrection is automatic**: `aveloxis mark-gone-repos` re-probes the gone cohort — a definitive 200 (org re-publicized) atomically clears the stamp and re-enqueues; prelim's healthy path does the same on its next probe

### Error Handling

- **Gateway error retry**: 502/503/504 responses trigger exponential backoff with jitter (1s, 2s, 4s, 8s... up to 64s base + random jitter), context-aware, up to 10 retries. This handles GitHub/GitLab service degradation gracefully.
- **Timestamp cleanup**: `aveloxis migrate` automatically detects and nullifies garbage timestamps (year < 1970) across all tables, preventing BC-era dates from poisoning queries
- **Deadlock retry**: All database upserts use exponential backoff retry on PostgreSQL deadlock errors (error code `40P01`), up to 10 attempts
- **Stale lock recovery**: The scheduler checks every 5 minutes for jobs that have been locked for more than 1 hour and re-queues them
- **Per-entity error isolation**: A failed upsert for one issue/PR/message logs a warning but does not abort collection for the entire repo
- **Facade resilience**: If `git fetch` fails on an existing clone, the facade re-clones from scratch before giving up

### Materialized Views (8Knot Compatibility)

Aveloxis creates 20 materialized views compatible with [8Knot](https://github.com/oss-aspen/8Knot) and other Augur analytics tools:

| View | Purpose |
|---|---|
| `api_get_all_repo_prs` | Total PR count per repo |
| `api_get_all_repos_commits` | Total distinct commit count per repo |
| `api_get_all_repos_issues` | Total issue count per repo (excluding PRs) |
| `explorer_entry_list` | Repo list with group names |
| `explorer_commits_and_committers_daily_count` | Daily commit/committer counts |
| `explorer_contributor_actions` | All contributor actions (commits, issues, PRs, reviews, comments) with ranking |
| `explorer_new_contributors` | First-time contributor tracking |
| `augur_new_contributors` | 8Knot compat — a plain VIEW alias of `explorer_contributor_actions` (zero refresh cost, v0.25.6) |
| `explorer_pr_assignments` | PR assignment/unassignment events |
| `explorer_pr_response` | PR message response tracking |
| `explorer_pr_response_times` | Comprehensive PR metrics (time to close, response times, line/file/commit counts) |
| `explorer_issue_assignments` | Issue assignment events |
| `explorer_user_repos` | User-to-repo mapping |
| `explorer_repo_languages` | Language breakdown from repo_labor |
| `explorer_libyear_summary` / `_detail` | Dependency age (libyear) metrics (`explorer_libyear_all` is a plain VIEW alias of `_summary`) |
| `explorer_contributor_recent_actions` | Same as `explorer_contributor_actions` but limited to last 13 months |
| `explorer_pr_files` | PR file paths with pull_request_id and repo_id |
| `explorer_cntrb_per_file` | Contributors and reviewers aggregated per file path |
| `explorer_repo_files` | Latest SCC file listing per repo (most recent analysis date) |
| `issue_reporter_created_at` | Legacy issue reporter view |

**Rebuild schedule:** Configurable via `collection.matview_rebuild_day` in `aveloxis.json` (default: `"saturday"`). Set to `"disabled"` to turn off automatic rebuilds. Views are NOT refreshed on every startup (was causing slow starts on large databases). On first run, views are created; subsequent startups skip them. Manual rebuild: `aveloxis refresh-views`. The explicit `aveloxis migrate` command always creates/refreshes views.

### Database Schema

Three schemas in PostgreSQL with full parity to Augur's `augur_data` and `augur_operations`, plus a dedicated schema for ScanCode results:

- **`aveloxis_data`** (100 tables + 20 materialized views) — All collected data: repos, issues, PRs, commits (per-file), commit parents, commit messages, messages, events, releases, contributors, contributor identities/aliases/affiliations, dependencies/SBOM, sentiment/NLP analysis, LSTM anomaly detection, topic modeling, Facade aggregates (dm_repo_annual/monthly/weekly, dm_repo_group_annual/monthly/weekly), repo labor/complexity, DEI badging, CHAOSS metrics, network analysis, repo insights, and more. Plus 20 materialized views for 8Knot compatibility.
- **`aveloxis_ops`** (39 tables) — Operational tables: collection queue, JSONB staging store, collection status (tracks core/secondary/facade/ML phases independently), API credentials, users/auth/sessions, config, worker history/jobs, network weighted tables.
- **`aveloxis_scan`** (4 tables) — ScanCode per-file license and copyright detection: `scancode_scans` (scan metadata), `scancode_file_results` (per-file SPDX license, copyrights, holders, packages as JSONB), plus `_history` tables for both.

Tables omitted from Augur (junk): `_transfer_testing`, `_transfer_training`, `akl;fjlk;a` (renamed to `dei_badging`), `analysis_log`, `all`, `github_users_2`, `worker_oauth_copy1`.

### Column Name Mapping (Augur to Aveloxis)

Aveloxis uses cleaner column names internally but exposes Augur-compatible names in all materialized views for seamless 8Knot integration. The internal schema avoids Augur's `pr_src_*` and `gh_*` prefixes in favor of descriptive names, but view output columns are aliased to match Augur exactly.

**Pull Requests:**

| Augur column | Aveloxis table column | Matview output alias |
|---|---|---|
| `pr_src_id` | `platform_pr_id` | `pr_src_id` |
| `pr_src_number` | `pr_number` | — |
| `pr_src_state` | `pr_state` | `pr_src_state` |
| `pr_src_title` | `pr_title` | — |
| `pr_created_at` | `created_at` | `pr_created_at` |
| `pr_merged_at` | `merged_at` | `pr_merged_at` |
| `pr_closed_at` | `closed_at` | `pr_closed_at` |
| `pr_augur_contributor_id` | `author_id` | `cntrb_id` |
| `pr_src_author_association` | `author_association` | `pr_src_author_association` |
| `pr_merge_commit_sha` | `merge_commit_sha` | — |

**Pull Request Meta:**

| Augur column | Aveloxis table column | Matview output alias |
|---|---|---|
| `pr_repo_meta_id` | `pr_meta_id` | — |
| `pr_head_or_base` | `head_or_base` | `pr_head_or_base` |
| `pr_src_meta_label` | `meta_label` | `pr_src_meta_label` |
| `pr_src_meta_ref` | `meta_ref` | — |
| `pr_sha` | `meta_sha` | — |

**Pull Request Reviews:**

| Augur column | Aveloxis table column |
|---|---|
| `pr_review_state` | `review_state` |
| `pr_review_body` | `review_body` |
| `pr_review_submitted_at` | `submitted_at` |
| `pr_review_src_id` | `platform_review_id` |

**Issues:**

| Augur column | Aveloxis table column |
|---|---|
| `gh_issue_id` | `platform_issue_id` |
| `gh_issue_number` | `issue_number` |

**Repo Info:**

| Augur column | Aveloxis table column |
|---|---|
| `stars_count` | `star_count` |
| `watchers_count` | `watcher_count` |
| `pull_request_count` | `pr_count` |
| `pull_requests_open` | `prs_open` |
| `pull_requests_closed` | `prs_closed` |
| `pull_requests_merged` | `prs_merged` |
| `committers_count` | `committer_count` |

**Table Names:**

| Augur table | Aveloxis table |
|---|---|
| `augur_data.repo` | `aveloxis_data.repos` |
| `augur_data.message` | `aveloxis_data.messages` |
| `augur_data.platform` | `aveloxis_data.platforms` |
| `augur_data.*` (all others) | `aveloxis_data.*` (same name) |
| `augur_operations.*` | `aveloxis_ops.*` |

**Libyear compatibility note:** Augur's `repo_deps_libyear` table has a typo: `current_verion` (missing 's'). Aveloxis fixes this to `current_version` in the table, but the `explorer_libyear_detail` materialized view aliases it back to `current_verion` for 8Knot compatibility.

## What's Collected

Both platforms collect the same data types. Most fields have full parity; known gaps are documented below the table:

| Entity | GitHub Source | GitLab Source | Storage |
|---|---|---|---|
| Issues | `/repos/{o}/{r}/issues` | `/projects/{id}/issues` | `issues` |
| Issue Labels | Embedded in issues | Embedded in issues | `issue_labels` |
| Issue Assignees | Embedded in issues | Embedded in issues | `issue_assignees` |
| Pull Requests / MRs | `/repos/{o}/{r}/pulls` | `/projects/{id}/merge_requests` | `pull_requests` |
| PR/MR Labels | Embedded in PRs | Embedded in MRs | `pull_request_labels` |
| PR/MR Assignees | Embedded in PRs | Embedded in MRs | `pull_request_assignees` |
| PR/MR Reviewers | `/pulls/{n}/requested_reviewers` | Embedded in MRs | `pull_request_reviewers` |
| PR/MR Reviews | `/pulls/{n}/reviews` | `/merge_requests/{n}/approvals` | `pull_request_reviews` + `messages` + `pull_request_review_message_ref` (review body stored as message via bridge table) |
| PR/MR Commits | `/pulls/{n}/commits` | `/merge_requests/{n}/commits` | `pull_request_commits` |
| PR/MR Files | `/pulls/{n}/files` | `/merge_requests/{n}/diffs` | `pull_request_files` |
| PR/MR Head/Base Meta | Embedded in PR response | Source/target branch from MR | `pull_request_meta` |
| Issue Comments | `/issues/comments` | `/issues/{n}/notes` | `messages` + `issue_message_ref` |
| PR/MR Comments | `/issues/comments` (shared endpoint) | `/merge_requests/{n}/notes` | `messages` + `pull_request_message_ref` |
| Review Comments | `/pulls/comments` | `/merge_requests/{n}/discussions` (diff-positioned) | `messages` + `review_comments` |
| Issue Events | `/issues/events` | `/projects/{id}/events` + resource events | `issue_events` |
| PR/MR Events | `/issues/events` (shared endpoint) | `/projects/{id}/events` + resource events | `pull_request_events` |
| Releases | `/repos/{o}/{r}/releases` | `/projects/{id}/releases` | `releases` |
| Repo Info (metadata) | GraphQL API (counts, community profile, license, status) | `/projects/{id}?statistics=true` + `/issues_statistics` + MR counts via `X-Total` | `repo_info` (latest) + `repo_info_history` |
| Contributors | `/repos/{o}/{r}/contributors` | `/projects/{id}/members/all` + `/repository/contributors` | `contributors` + `contributor_identities` |
| Clone Stats | `/traffic/clones` | Not available via API | `repo_clones` |
| Commits (git) | `git clone --bare` + `git log --numstat` (default branch) | Same | `commits` + `commit_parents` + `commit_messages` |
| Facade Aggregates | Computed from commits table | Same | `dm_repo_annual/monthly/weekly` |
| Commit Author Resolution | Noreply parse + Commits API + Search API (GitHub only) | N/A (GitLab identity from API) | `contributors` + `contributor_aliases` |
| Dependencies | File scan: 14 ecosystems (package.json, go.mod, pom.xml, Cargo.toml, etc.) | Same | `repo_dependencies` |
| Libyear | 12 registries (npm, PyPI, Go, Cargo, RubyGems, Maven, Packagist, Hex, NuGet, pub.dev, Hackage, SwiftPM) | Same | `repo_deps_libyear` |
| Code Complexity | `scc --by-file` (if installed) | Same | `repo_labor` |
| OpenSSF Scorecard | remote-first `--repo` (18 checks), `--local` fallback | local mode (11 checks) | `repo_deps_scorecard` (latest) + `repo_deps_scorecard_history` |
| ScanCode License/Copyright | `scancode -clpi` per file (decoupled worker, every 180 days) | Same | `aveloxis_scan.scancode_scans` + `scancode_file_results` + history |
| SBOMs | Generated from libyear data | Same | `repo_sbom_scans` (CycloneDX 1.5 + SPDX 2.3) |
| Vulnerability Scan | OSV.dev batch API (purls; direct + transitive) | Same | `repo_deps_vulnerabilities` (CVE ID, severity, CVSS, fixed version, kind/scope/resolution labels, lifecycle stamps) |
| Lockfiles / Transitive Closure | 19 lockfile formats + `go mod graph` | Same | `repo_lockfiles` + `repo_lockfile_packages` + `repo_lockfile_edges` (dependency graph for SBOMs + `introduced_by` chains) |
| Package Distribution | deps.dev + ecosyste.ms + release assets + GitHub Packages + manifests | ecosyste.ms + manifests | `repo_distribution` (+ manifest + history tables) — "where is this repo published?" |
| PR/MR Fork Repos | `head.repo` / `base.repo` in PR response | `/projects/{id}` per source/target | `pull_request_repo` |
| Contributor Affiliations | Auto-populated from email domains + `cntrb_company` | Same | `contributor_affiliations` |
| Contributor Breadth | `GET /users/{login}/events` (15-min cycles, 7-day cooldown) | — | `contributor_repo` |
| Canonical Email Enrichment | `GET /users/{login}` for profile email | Same | `contributors.cntrb_canonical` |

### GitHub vs GitLab — Known Data Gaps

The following fields are available from GitHub but not from GitLab due to platform API limitations:

| Field | GitHub | GitLab | Notes |
|---|---|---|---|
| Community profile files (CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY) | GraphQL file detection | `/repository/tree` file detection (v0.12.2) | Full parity |
| `repo_info.commit_count` | REST `/repos/{o}/{r}` — accurate | `GET /projects/:id?statistics=true` → often 0 for mirrored or private-low-scope projects | v0.16.9+ backfills from facade's `git log` count after a successful clone so the monitor/web "Metadata commits" column matches reality |
| Watcher count (`repo_info`) | GraphQL `watchers.totalCount` | Not available | GitLab has no public "watchers" API; `star_count` is the closest analog |
| Clone statistics (`repo_clones`) | `/traffic/clones` (requires push access) | Not available | GitLab exposes clone data only via admin-only endpoints |
| GraphQL node IDs (`pr_src_node_id`) | Available on all entities | Not applicable | GitLab uses numeric IDs, not GraphQL node IDs — architectural difference |
| Contributor URL fields (`gh_followers_url`, etc.) | 10+ URL fields per contributor | Not available | GitLab API doesn't expose follower/following/gist/etc. URLs |
| Contributor type (User/Bot/Organization) | `type` field on user objects | Not available | GitLab doesn't distinguish user types the same way |
| Label numeric IDs (`issue_labels.platform_label_id`, `pull_request_labels.platform_label_id`) | REST `id` — populated on the REST child paths only | Populated (REST) | On GitHub's GraphQL child paths these columns stay `0`: GraphQL's `Label` type exposes no `databaseId`. No SELECT/JOIN consumers exist; the trade-off is that label renames within a project cannot be distinguished from delete+create |

### Unified Message Architecture

All text content from conversations — regardless of where it originates — is stored in a single `messages` table. This design enables cross-cutting text analysis (sentiment, response times, contributor communication patterns) without needing to query four separate tables. The semantic origin of each message is preserved via bridge tables:

| Message type | Purpose | Bridge table | Metadata table |
|---|---|---|---|
| Issue comments | Discussion on issues | `issue_message_ref` | — |
| PR/MR comments | Discussion on pull requests | `pull_request_message_ref` | — |
| Inline review comments | Code-level feedback on specific diff lines | `review_comments` (has `msg_id` FK) | `review_comments` (diff_hunk, file_path, line, position) |
| Review bodies | Top-level review text (e.g., "LGTM", "Changes requested because...") | `pull_request_review_message_ref` | `pull_request_reviews` (review_state, submitted_at) |

**Why this matters for analysis:** A query like "all messages by contributor X" joins `messages` once. A query like "all inline code review feedback on file Y" joins through `review_comments`. A query like "average time from PR open to first review body" joins through `pull_request_review_message_ref`. The bridge tables give you the semantic context; the `messages` table gives you the text.

Review bodies are stored in both `pull_request_reviews.review_body` (for quick access with the review metadata) and in `messages` (for unified text analysis). This intentional duplication keeps the review table self-contained while enabling cross-message-type analytics.

## Comparison with Augur

| Aspect | Augur | Aveloxis |
|---|---|---|
| Language | Python | Go |
| Processes | Celery workers + Flask API + Flower + Redis + RabbitMQ | 3 processes: `serve` (scheduler), `web` (GUI+monitor), `api` (REST) |
| Queue | Celery + RabbitMQ + Redis | Postgres `SKIP LOCKED` (no extra infrastructure) |
| Monitoring | Flower (separate service) | Built-in dashboard with gathered vs metadata count columns |
| Testing | Lags development due to long history. | Test first programming from birth. | 
| REST API | Flask/Gunicorn with Beaker cache | Separate `aveloxis api` with repo stats, batch stats, SBOM download |
| GitLab support | Partial (missing releases, repo info, review comments, contributor enrichment) | Full parity with GitHub — all features work on both platforms |
| Repo info | GitHub Only | GitHub: GraphQL for all counts + community profile files. GitLab: REST + `/issues_statistics` + MR counts via `X-Total`. Historical snapshots in `repo_info_history`. |
| Contributor model | `gh_*`/`gl_*` columns mixed on one table | Separate `contributor_identities` table |
| DB write pattern | Individual upserts during collection | JSONB staging → bulk batch processing (`pgx.Batch` for deps, libyear, labor, breadth). Significantly lower database contention for the contributors table. |
| Repo redirect handling | Not proactively handled | Prelim phase detects renames/transfers, deduplicates, updates URLs + bulk-fixes all stored URLs regularly |
| Dependency scanning | Custom Python parsers for 12 languages | Go parsers for 14 ecosystems + 19 lockfile formats (transitive closure + dependency edges), on-demand full clone |
| Libyear | npm + PyPI only | 12 registries: npm, PyPI, Go, Cargo, RubyGems, Maven, Packagist, Hex, NuGet, pub.dev, Hackage, SwiftPM |
| Code complexity (scc) | Requires manual scc install + separate worker | `aveloxis install-tools` + automatic per-repo analysis |
| OpenSSF Scorecard | Runs `scorecard` binary against GitHub repos. | Runs `scorecard` binary against GitHub AND GitLab repos. Results in `repo_deps_scorecard` with history. |
| SBOM generation | Not supported | CycloneDX 1.5 + SPDX 2.3 with license capture from 12 registries. Download via web GUI or REST API. |
| Review messages | Review bodies live only on the review row — no unified text store | Review body stored in `messages` with the `pull_request_review_message_ref` bridge (same pattern as issue/PR comments) — one table for ALL conversation text |
| History tracking | Fills repo_info on each run, grows infinitely. | `repo_info_history` and `repo_deps_scorecard_history` preserve all previous snapshots |
| Scheduling | Celery Beat + collection_status table (opaque) | Priority queue — fills ALL worker slots per tick (not one per tick) |
| Priority override | Not supported | `aveloxis prioritize` / POST API / dashboard button |
| Scaling | Single Celery worker per queue | Multiple instances via `SKIP LOCKED`; 40+ workers on one host doing all collection for each repo before moving on. |
| API key rotation | Sequential drain, single key at a time | Round-robin across all keys, 15-request buffer, full utilization |
| API key source | Keyman service + Redis | Config file and/or Augur's `worker_oauth` table |
| API efficiency | No conditional requests | ETag caching (304 = free), HTTP/2 multiplexing, 20 idle connections per host |
| Commit author resolution | Separate Python scripts, long process with contention. | Built-in post-facade phase: noreply parse, DB lookup, Commits API, Search API |
| Materialized views | 18 views, manual refresh or Celery task | 20 materialized views + 2 alias views, configurable auto-rebuild schedule (default Saturday), not refreshed on startup |
| Contributor breadth | Separate Celery worker, manual scheduling | Built-in 15-minute cycles with cooldown + circuit breaker; plus per-day contributor activity history and activity classification |
| Contributor IDs | Deterministic GithubUUID from gh_user_id | Deterministic GithubUUID from gh_user_id (Augur byte-compatible) |
| Facade aggregates | Post-processing in Python | SQL-based aggregate refresh per repo after git log |
| Affiliation resolution | Python domain matching | In-memory cached resolver with parent domain fallback |
| Text sanitization | Python encode/decode with backslashreplace + null byte removal | Go sanitizer: null bytes, invalid UTF-8, control characters stripped at DB boundary |
| Dead repo handling | Keeps retrying dead repos every cycle | Permanently sidelines (archived flag + dequeued), data preserved |
| Gateway error retry | Basic retry | Exponential backoff with jitter (1s-64s) for 502/503/504 |
| User interface | CLI only, except for Admin. | Web GUI with OAuth, interactive Chart.js visualizations, cross-project comparison (100%/Z-Score), dependency license analysis, SBOM download |
| Visualizations | Requires external tool (8Knot/Dash) | Still 8Knot compatible, 100%! And, built-in weekly time-series charts, comparison page for up to 5 repos with Z-score normalization |
| Vulnerability scanning | Not supported | OSV.dev batch API: direct + transitive findings by purl with `introduced_by` chains, lifecycle (first-detected/resolved) stamps, and honest version-resolution labels |
| SBOM dependency graphs | Not supported | CycloneDX + SPDX carry the REAL parent→child graph from lockfile edges |
| Distribution tracking | Not supported | "Where is this repo published?" — deps.dev, ecosyste.ms, release assets, GitHub Packages, in-repo manifests |
| Public showcase | Not supported | Static, SEO-indexable collection + repo snapshot pages with charts, SBOM downloads, and redacted contributor previews (`aveloxis generate-showcase`) |
| Fuzzing | Not supported | 7 native Go fuzz targets under ClusterFuzzLite |
| Non-GitHub/GitLab repos | Not supported | Git-only mode: facade, analysis, scorecard, SBOM — email resolution against both platforms |
| User org tracking | Static — orgs added once, never rescanned | Dynamic — `user_org_requests` tracked, new repos auto-discovered every 4h |
| Error recovery | Manual restart | Automatic stale lock recovery + deadlock retry |
| Mailing-list ingestion | Separate insight worker, GitHub-archive only | Decoupled `MailingListWorker`: Apache Pony Mail + lore.kernel.org public-inbox, email as a first-class `email_message` entity routed onto issues/PRs/reviews/`mailing_list_only`, Jira/Bugzilla `external_key` correlation. Platform `6` = Mailing List. Off by default; see [docs](docs/architecture/mailing-list.md) |

## Project Structure

The tree is organized as one CLI binary plus focused internal packages (file-level layout shifts too fast to catalog here — the package boundaries are the stable map):

```
aveloxis/
  cmd/aveloxis/       # CLI entry point — ~35 cobra commands (serve, web, api, collect,
                      # migrate, add-repo/-key, data-test, heal-*, rewalk-whitespace,
                      # mark-gone-repos, generate-showcase, load-* importers, ...)
  internal/           # All implementation packages (see table)
  scripts/            # Repo-wide tripwire tests + standing-rules registry + operator SQL
  aveloxis-story/     # Reference: Augur's schemas + artifacts from the road to Aveloxis
  docs/               # Sphinx/ReadTheDocs sources (getting-started, guide, architecture,
                      # contributing)
  Dockerfile, docker-compose.yml, .readthedocs.yaml
  aveloxis.example.json / aveloxis.docker.example.json
```

| Package | Purpose |
|---|---|
| `internal/collector` | Collection orchestration: staged pipeline, facade (git), analysis (deps/libyear/scc), lockfile + `go mod graph` transitive closure, SBOM generation, scorecard, vulnerability scanning, scancode + distribution + mailing-list worker pools, commit resolution, gap fill, prelim redirect/dead detection, whitespace walker, tools management |
| `internal/db` | PostgreSQL store: schema DDL (`schema.sql`), migrations + the completed-backfill ledger, all upserts (idempotent by contract), staging writer/processor, matviews, sanitization + the UTF-8 boundary tracer, history rotation, queue, stats/timeseries/metrics readers, repo dedup |
| `internal/platform` | HTTP client (rate limiting, key rotation, ETag caching, retries, error taxonomy) + `github/` (REST + GraphQL) and `gitlab/` (REST) implementations of the shared `platform.Client` interface |
| `internal/scheduler` | Queue polling, worker slots, heartbeats, stale-lock recovery, periodic tickers (enrichment, breadth, activity classification + history, affiliations, org scans, matview rebuilds), long-jobs watchdog |
| `internal/api` | REST API on :8383 — auth middleware, per-IP rate limiting, analytics/compare, portal + admin endpoints |
| `internal/web` | Server-rendered web GUI on :8082 — OAuth login, groups, visualizations, monitor page, admin approvals |
| `internal/model` | Platform-agnostic data types shared by every layer |
| `internal/config` | `aveloxis.json` loading — every knob's single default layer |
| `internal/showcase` | Static public showcase generator (collection + repo snapshot pages, SVG charts, sitemap) |
| `internal/mailinglist` | Apache Pony Mail + lore public-inbox archive backends, mbox parsing, message classification |
| `internal/monitor` | Standalone monitor dashboard on :5555 |
| `internal/mailer` | Gmail-SMTP transactional mail (welcome, approvals, operator vulnerability digest) |
| `internal/importers` | Curated foundation catalogs (NumFocus, Apache, ...) |
| `internal/safego` / `internal/pidfile` / `internal/srctest` | Panic-safe goroutine helpers; PID-file liveness; the shared source-contract test engine (test-only) |

## Testing

```bash
# Run all unit tests (no database required)
go test ./...

# Run with verbose output
go test -v ./...

# Run a specific package
go test ./internal/platform/...

# Run integration tests (requires live PostgreSQL)
# Set the connection string, then tests with t.Skip guards will run:
AVELOXIS_TEST_DB="postgres://user:pass@localhost:5432/aveloxis_test" go test ./internal/db/...
```

The suite currently stands at **~2,960 test functions across ~670 test files** (plus 7 native Go fuzz targets), organized in tiers:

- **Unit + source-contract tier** — runs on plain `go test ./...` with no database. Includes behavioral tests (parsers for 14 manifest ecosystems + 19 lockfile formats, SBOM generation, OSV response handling, GraphQL batch subdivision, git-log/whitespace walkers) and the repo's signature *source-contract tripwires*: tests that pin load-bearing code shapes so regressions fail the build before review — every INSERT has a real ON CONFLICT arbiter, every schema column has a writer or a documented reason, docs counts match the schema, every registered command is documented, every config key is in the docs and example config, every API route has a smoke recipe, and the standing-rules registry (`scripts/standing_rules.go`) keeps each hard-won rule attached to the tests that enforce it.
- **Integration tier** — gated on `AVELOXIS_TEST_DB`; runs migrations end-to-end against a real PostgreSQL (fresh AND populated), then exercises store methods, the staged processor, queue lifecycle, and end-to-end flows (gap healing to convergence, vulnerability lifecycle, rename dedup, group approvals). CI provisions a Postgres service container for this tier on every push.
- **Network canaries** — gated on `AVELOXIS_TEST_NETWORK`; a weekly CI job hits the real deps.dev, OSV.dev, crates.io, GitHub search, and Pony Mail APIs to catch contract drift that mocks structurally cannot see.
- **Fuzzing** — ClusterFuzzLite builds 7 native `func Fuzz*` targets (mbox parsing, lockfile/manifest parsers, purl helpers, repo-URL parsers); their seed corpora also run under ordinary `go test`.
- **Lint gates** — `staticcheck`, `golangci-lint` (the CI version), `gofmt`, and CodeQL all block merges.

# Build docs
```bash
cd docs
pip install -r requirements.txt
sphinx-build -b html . _build/html
open _build/html/index.html

Or if you prefer a one-liner from the repo root:

pip install sphinx sphinx-rtd-theme myst-parser && sphinx-build -b html docs docs/_build/html && open docs/_build/html/index.html
```

# Turning on Apache Mailing List Collection
Starting Apache mailing-list collection

###### 1. Schema must be at v0.25.9+ (the mailing-list tables + platform 6):
```bash
aveloxis stop all && aveloxis migrate --skip-views && aveloxis start all
```

###### 2. Make sure the repos exist (lists attach to a repo's group):
```bash
aveloxis load-foundation-core-repos        # one primary repo per Apache project
aveloxis load-foundation-orgs --yes        # optional: track the whole apache org so sibling repos resolve
```

###### 3. Register the per-PMC dev@/users@ lists:
```bash
aveloxis load-apache-lists                 # --dry-run first to preview
```

###### 4. Turn the worker on in aveloxis.json, then restart serve:
```json
"collection": {
  "mailing_list_enabled": true,
  "mailing_list_polite_email": "you@example.org"   // sets the contact header
}
```

```bash
aveloxis stop serve && aveloxis start serve        # the MailingListWorker spawns inside serve
```

###### 5. Watch it:
```bash
aveloxis mailing-list-stats                        # coverage rollup
aveloxis verify-mailing-list                       # PASS/EMPTY/DEFER branch table
```

The single thing that actually turns it on is mailing_list_enabled: true + a serve restart — the worker is a decoupled pool inside serve and claims its own list queue. It's off by default.
Full reference: docs/architecture/mailing-list.md.

One reminder from this run: the cross-subsystem branches (bridge-to-issue, sender resolution, external_key) fill in best when a list's linked repo has its GitHub data collected first — otherwise they backfill over time rather than resolving inline.

# Detailed LCF
Aveloxis is free software: you can redistribute it and/or modify it under the terms of the MIT License as published by the Open Source Initiative. See the [LICENSE](LICENSE) file for more details. This work has been funded almost entirely through the Alfred P. Sloan Foundation. Mozilla, The Reynolds Journalism Institute, VMWare, Red Hat Software, Grace Hopper's Open Source Day, GitHub, Microsoft, Twitter, Adobe, the Gluster Project, Open Source Summit (NA/Europe), and the Linux Foundation Compliance Summit have made contributions to the code and in some cases financially supported the development of Aveloxis's predecessoar, Augur, from 2017 to 2026.  Aveloxis collects open source community health data from GitHub and GitLab with equal completeness, storing it in a shared PostgreSQL schema for cross-platform analysis. It is designed as an upgrade to the [Augur](https://github.com/chaoss/augur) collection pipeline. A feature and robustness comparison is available in the [Comparison with Augur section of this readme.](#comparison-with-augur)
