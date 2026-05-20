# Configuration

Aveloxis is configured via a JSON file named `aveloxis.json` in the current working directory.

---

## Creating the config file

Copy the example configuration and edit it with your database credentials and API tokens:

```bash
cp aveloxis.example.json aveloxis.json
```

A minimal configuration only needs the `database` section:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "user": "aveloxis",
    "password": "your-password",
    "dbname": "aveloxis",
    "sslmode": "prefer"
  }
}
```

A full configuration with **every** supported option (current as of v0.20.12):

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
    "api_keys": ["ghp_your_token_here"],
    "base_url": "https://api.github.com"
  },
  "gitlab": {
    "api_keys": ["glpat-your_token_here"],
    "base_url": "https://gitlab.com/api/v4",
    "gitlab_hosts": ["gitlab.freedesktop.org"]
  },
  "mail": {
    "gmail_user": "aveloxis-ops@yourdomain.com",
    "gmail_app_password": "xxxx xxxx xxxx xxxx",
    "from_name": "Aveloxis",
    "site_url": "https://your-host.example"
  },
  "collection": {
    "days_until_recollect": 1,
    "workers": 12,
    "repo_clone_dir": "/data/aveloxis-repos",
    "force_full": false,
    "matview_rebuild_day": "saturday",
    "matview_rebuild_on_startup": false,
    "pr_child_mode": "graphql",
    "listing_mode": "graphql",
    "threading_mode": "sharded",
    "shard_size": 3000,
    "issue_child_mode": "graphql",
    "enrich_interval_minutes": 30,
    "search_resolve_interval_minutes": 60,
    "affiliation_interval_minutes": 60,
    "breadth_interval_minutes": 15,
    "breadth_batch_size": 2000,
    "breadth_cooldown_days": 7,
    "shutdown_grace_seconds": 10,
    "scancode_workers": 2,
    "scancode_start_interval_s": 90,
    "scancode_cadence_days": 180,
    "scancode_clone_dir": "/tmp/aveloxis-scancode",
    "scancode_shutdown_grace_minutes": 30,
    "staging_retention_hours": 1,
    "phase_watchdog_minutes": 75
  },
  "web": {
    "addr": ":8082",
    "session_secret": "generate-a-random-32-byte-string",
    "base_url": "https://aveloxis.example.com",
    "dev_mode": false,
    "github_client_id": "your-github-oauth-app-client-id",
    "github_client_secret": "your-github-oauth-app-client-secret",
    "gitlab_client_id": "your-gitlab-oauth-app-id",
    "gitlab_client_secret": "your-gitlab-oauth-app-secret",
    "gitlab_base_url": "https://gitlab.com",
    "api_internal_url": "http://127.0.0.1:8383"
  },
  "log_level": "info"
}
```

Every field is optional except `database` credentials and at least one API key source (config or `worker_oauth` table). Sections you don't need can be omitted entirely.

---

## Full config reference

### Database

| Field | Type | Default | Description |
|---|---|---|---|
| `database.host` | string | `"localhost"` | PostgreSQL server hostname or IP address. |
| `database.port` | integer | `5432` | PostgreSQL server port. |
| `database.user` | string | (required) | Database username. |
| `database.password` | string | (required) | Database password. |
| `database.dbname` | string | (required) | Database name. |
| `database.sslmode` | string | `"prefer"` | PostgreSQL SSL mode. Options: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. |

### GitHub

| Field | Type | Default | Description |
|---|---|---|---|
| `github.api_keys` | string[] | `[]` | GitHub personal access tokens for API access. Multiple tokens enable round-robin rotation. |
| `github.base_url` | string | `"https://api.github.com"` | GitHub API base URL. Change this for GitHub Enterprise Server installations. |

### GitLab

| Field | Type | Default | Description |
|---|---|---|---|
| `gitlab.api_keys` | string[] | `[]` | GitLab personal access tokens. |
| `gitlab.base_url` | string | `"https://gitlab.com/api/v4"` | GitLab API base URL. Change for self-hosted GitLab instances. |
| `gitlab.gitlab_hosts` | string[] | `[]` | Additional hostnames to recognize as GitLab instances. Use this for self-hosted GitLab servers whose hostnames do not contain "gitlab". |

### Collection

The `collection` block holds every knob for the staged-pipeline scheduler and its periodic background tasks. Group them by category:

**Throughput / scheduling**

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.days_until_recollect` | integer | `1` | Minimum number of days before a repo is re-collected. After a successful job, `due_at = last_collected + days_until_recollect`. Changing this value takes effect on the next `aveloxis serve` restart (v0.16.6's startup-time `RealignDueDates` rewrites queued rows). |
| `collection.workers` | integer | `12` | Number of concurrent collection workers when running `aveloxis serve`. Each worker may make many concurrent DB calls; the pgx pool is sized as `max(workers + 15, 20)`. |
| `collection.repo_clone_dir` | string | `$HOME/aveloxis-repos` | Directory for bare git clones used by the facade phase. Can grow to terabytes for large instances (400K+ repos). |
| `collection.force_full` | boolean | `false` | Fleet-wide: when `true`, every collection pass runs `since=zero` regardless of `last_collected`. Use this once after a systemic bug fix that invalidates collected data, then revert to `false`. For per-repo full re-collection, use `aveloxis recollect <url>` instead (sets a queue flag, doesn't touch this setting). |

**Materialized views**

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.matview_rebuild_day` | string | `"saturday"` | Day of the week the scheduler refreshes the 22 materialized views. Values: `"sunday"`–`"saturday"`, or `"disabled"` / `"none"` / `"off"` to never auto-rebuild. Independent of `aveloxis refresh-views` which always refreshes on demand. |
| `collection.matview_rebuild_on_startup` | boolean | `false` | When `true`, `aveloxis serve` rebuilds the matviews on every startup. Default `false` because the rebuild can take many minutes on large fleets and `migrate` already refreshes them on schema changes. |

**REST → GraphQL refactor (v0.18.x phases)**

These four settings control the staged collector's request shape. The default for all four matches the pre-v0.18.x REST behavior so existing deployments don't shift transport on upgrade. Operators running medium-to-large fleets should opt into the GraphQL path for the ~5× wall-clock speedup observed in benchmarks (augurlabs/augur, 73 keys: 125 min REST → 24 min GraphQL).

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.pr_child_mode` | string | `"rest"` | `"rest"` uses the per-PR REST waterfall (8 calls per PR). `"graphql"` (v0.18.1+) uses `FetchPRBatch` — one GraphQL query per 10 PRs returning all child data inline. GitLab path is REST composition in both modes (column parity preserved). |
| `collection.listing_mode` | string | `"rest"` | `"rest"` uses separate iterators for `/issues` and `/pulls`. `"graphql"` (v0.18.2+) calls `ListIssuesAndPRs` once per repo — a pair of paginated GraphQL queries instead of two REST scans. Setting both this AND `pr_child_mode` to `"graphql"` activates v0.18.5's `fullGraphQLMode` gate: conversation comments are delivered inline, eliminating one repo-wide REST call. |
| `collection.threading_mode` | string | `"single"` | `"single"` fetches PR batches sequentially. `"sharded"` (v0.18.3+) partitions the enumerated PR list and runs each shard in its own goroutine when the PR count exceeds `shard_size`. Only activates when `pr_child_mode=graphql`. |
| `collection.shard_size` | integer | `3000` | Item-count threshold for `threading_mode=sharded`. Number of shards = `ceil(prs / shard_size)`. Smaller values fan out earlier on medium repos. Ignored when `threading_mode != "sharded"`. |
| `collection.issue_child_mode` | string | `"graphql"` | `"graphql"` (v0.22.3+ default, phase 5.2) drains labels and assignees from the inline maps delivered by `ListIssuesAndPRs`, eliminating two per-issue REST calls. ~100× speedup on the issue phase on repos with thousands of issues; ~13–15× speedup on full augur collection (verified by shadow-diff on 2026-05-16). `"rest"` keeps the legacy waterfall (two REST calls per issue) — available as an escape hatch, same posture as `pr_child_mode` after its v0.19.0 default flip. Requires `listing_mode=graphql` to take effect (the inline maps come from that path). GitLab path is REST composition in both modes (column parity preserved at the row level). **Known parity gap**: `issue_labels.platform_label_id` stays 0 on the GraphQL path because GitHub's GraphQL `Label` type has no `databaseId` — same gap as `pull_request_labels.platform_label_id`. The column has no SELECT/JOIN/WHERE consumers anywhere in the codebase (verified by grep over `internal/db/`, `queries/`, `internal/api/`), so this is a known parity gap, not a regression. The only material loss is detection of label renames within a project: renamed labels show as two rows instead of one. |

**Background tasks**

Periodic tickers that run on the scheduler. v0.16.5 / v0.18.29 / v0.19.7 moved each of these out of the per-repo hot path (where they caused fan-out contention) into single-goroutine periodic tasks. Cadence is configurable; defaults are conservative.

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.enrich_interval_minutes` | integer | `30` | Cadence (minutes) of the thin-contributor profile enrichment ticker. Each tick processes one batch of up to 14,000 thin contributors via `GET /users/{login}`. With 14K candidates and 73 keys, even 60 minutes is well under the rate budget. |
| `collection.search_resolve_interval_minutes` | integer | `60` | Cadence (minutes) of the v0.19.2 search-resolve ticker. Each tick takes 100 contributors with email-but-no-`gh_user_id` and calls GitHub's search API to backfill the identity. GitHub search is rate-limited to 30/min/token (separate budget from the 5000/hour core API), so this runs at a deliberately low cadence. |
| `collection.affiliation_interval_minutes` | integer | `60` | Cadence (minutes) of the v0.19.7 affiliation-population ticker. Recomputes the global domain→company map from `contributor_affiliations`. Pre-v0.19.7 this fired from every worker after every repo and caused `UNIQUE (ca_domain)` ShareLock contention. |
| `collection.breadth_interval_minutes` | integer | `15` | Cadence (minutes) of the v0.20.17 contributor breadth ticker. Each tick calls `/users/{login}/events` for up to `breadth_batch_size` contributors past their cooldown window and stamps `contributors.cntrb_last_breadth_at`. Pre-v0.20.17 this was hardcoded to 6 hours / 100 batch / no cooldown — first-pass coverage of a 1.4M-contributor fleet would have taken 9.6 years. At 15-min interval × 2000 batch the new throughput targets ~192K contributors/day → first pass in ~7 days on a 1.4M fleet. |
| `collection.breadth_batch_size` | integer | `2000` | Maximum contributors processed per breadth tick. Each contributor takes 1–3 API calls (most users have ≤300 recent events fitting in one page). |
| `collection.breadth_cooldown_days` | integer | `7` | Minimum interval between successive breadth attempts on the same contributor. After this window the contributor becomes eligible again via the `cntrb_last_breadth_at IS NULL OR < NOW() - interval` filter. Steady-state load with a 7-day cooldown over 1.4M contributors is ~200K/day = 8K/hour ≈ 2% of the 365K/hr budget of a 73-key fleet. |

**Shutdown**

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.shutdown_grace_seconds` | integer | `10` | v0.20.0: ctx-cancel grace window for in-flight workers before `Scheduler.Run` closes the pgx pool. Pre-v0.20.0 the wait was unbounded — a 26-minute `commits` UPDATE blocked shutdown for the full duration. Setting this too low means worker transactions abort mid-flight (Postgres rolls them back safely but logs are noisy); too high means slow shutdown. |

**Scancode worker (v0.21.0)**

The scancode per-file license + copyright + package scan is run by a dedicated `ScancodeWorker` pool, decoupled from the per-repo collection pipeline. Pre-v0.21.0 scancode ran inline in `AnalysisCollector.AnalyzeRepo` gated by a 2-slot package-level semaphore; the 2026-05-14 production incident showed that shape doesn't survive fleet-scale operation (177 of 180 collection workers parked behind the semaphore for 7+ hours). The decoupled pool fixes the structural problem and adds operator-tunable cadence + concurrency. See `docs/architecture/scancode.md` for the full architecture write-up.

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.scancode_workers` | integer | `2` | Maximum concurrent scancode invocations. Pre-v0.21.0 the limit was hardcoded to 2; the default matches that so upgrading operators don't see a sudden change in scancode CPU load. Operators with spare CPU cores should raise this (the user running the fleet that surfaced the 2026-05-14 incident has tested 12 against 64 cores). |
| `collection.scancode_start_interval_s` | integer | `90` | Minimum seconds between *successful* scancode claim starts (v0.21.3+). As of v0.21.3 this is a minimum-gap pacing primitive, not a throughput cap — the dispatcher claims as fast as workers free up, with this interval enforced only between consecutive successful starts. Bounds clone-bandwidth bursts on restart. Pre-v0.21.3 this was a `time.NewTicker` rate cap and limited first-pass throughput to one claim per interval regardless of worker availability; that bug is documented in CLAUDE.md v0.21.3. |
| `collection.scancode_cadence_days` | integer | `180` | Minimum days between successive scancode runs on the same repo. Pre-v0.21.0 was 30 days; the change reflects that per-file license + copyright headers in source files change rarely on the timescale that matters, and the I/O cost of scanning a Linux-kernel-scale mirror doesn't justify monthly re-scans. Dependency-level licenses (which DO change as packages update) still flow through the per-cycle Phase 4 dependency scan + Phase 6 SBOM generation. |
| `collection.scancode_clone_dir` | string | `"/tmp/aveloxis-scancode"` | Parent directory for per-run shallow clones. Each scan creates `<dir>/repo_<id>_<unix_ts>` and removes it on completion (success or failure). Size budget: each clone is the working tree only (`git clone --depth 1`), so ≈ checked-out repo size. With default 2 workers and average ~50 MB clones, ~100 MB peak; raise expectations for big-repo / many-worker installs. |
| `collection.scancode_shutdown_grace_minutes` | integer | `30` | Time the `ScancodeWorker` waits for in-flight scans to finish on `aveloxis stop`. Within the grace window, runners complete naturally (parse JSON output, write DB, clear lock columns). At grace expiry, `cmd.Process.Kill()` is invoked on the still-running scancode subprocess; `cmd.Wait()` returns with an error, the runner's lock-clear path fires, no orphaned scans from graceful shutdown. Separate from `shutdown_grace_seconds` (which paces the main scheduler) because scancode scans are intrinsically long-running. |
| `collection.staging_retention_hours` | integer | `1` | How long processed staging rows are kept before the hourly `PurgeStagedProcessed` sweep deletes them. v0.22.4 cut from the prior hardcoded 7-day window: 2026-05-16 production diagnostics showed JSONB tombstones stacking 3–5× on frequently-re-collected repos (zephyr had 84K issue rows against an actual 28K count). Not a correctness bug (`Processor` reads `WHERE NOT processed`) but real disk waste. Operators who need forensic retention (shadow-diff debugging, post-mortem analysis) can raise this — `24` (one day) is a reasonable middle ground. |
| `collection.phase_watchdog_minutes` | integer | `75` | Stall threshold for the v0.22.4 observation-only long-jobs watchdog. If a repo's staging row count has not grown for this many minutes, the watchdog appends one JSON-lines event to `~/.aveloxis/aveloxis-long-jobs.log` and writes a per-event goroutine dump under `~/.aveloxis/long-jobs/`. The watchdog **NEVER cancels the job, NEVER requeues the repo, NEVER kills anything** — large first-cycle collections (microsoft/vscode-class) may legitimately run for days, and aborting them would prevent them from ever completing. Re-emits every `phase_watchdog_minutes` while the stall persists; emits a `stall_resumed` event when staging row count grows again so analysts can compute total stall duration. Lower this (e.g. `30`) during active incident triage to spot smaller hangs; raise it on installations whose largest-repo collection routinely takes many hours. |

**Force-rerun cookbook** — to invalidate the cadence gate and trigger a fresh scan on the next worker tick, set `scancode_last_run` back to NULL:

```sql
-- Single repo:
UPDATE aveloxis_data.repos SET scancode_last_run = NULL WHERE repo_owner = 'apache' AND repo_name = 'doris';

-- Whole fleet (e.g. after a scancode major-version upgrade):
UPDATE aveloxis_data.repos SET scancode_last_run = NULL;
```

The worker's claim query orders `NULLS FIRST`, so cleared repos move to the front of the queue.

### Web (OAuth + GUI)

The `web` block configures the `aveloxis web` server. Optional — if you only run `serve` (collection scheduler), you can omit this entirely.

| Field | Type | Default | Description |
|---|---|---|---|
| `web.addr` | string | `":8082"` | Listen address for the web GUI. |
| `web.session_secret` | string | (none) | Secret used to sign session cookies. Generate a random 32+ byte string. Without this, sessions don't survive restarts. |
| `web.base_url` | string | (none) | Public-facing external URL of the web GUI (e.g. `https://aveloxis.example.com`). Used to build OAuth callback URLs and outbound email links. |
| `web.dev_mode` | boolean | `false` | When `true`, disables the `Secure` flag on cookies so the GUI works over plain HTTP. **Production must leave this `false`** so browsers only send cookies over HTTPS. `HttpOnly` is always set regardless. |
| `web.github_client_id` | string | (none) | GitHub OAuth App client ID. Create one at <https://github.com/settings/developers>. The callback URL must match `<base_url>/auth/github/callback`. |
| `web.github_client_secret` | string | (none) | GitHub OAuth App client secret. |
| `web.gitlab_client_id` | string | (none) | GitLab OAuth Application ID. Create one at <https://gitlab.com/-/profile/applications> (or your self-hosted instance's `/admin/applications`). |
| `web.gitlab_client_secret` | string | (none) | GitLab OAuth Application secret. |
| `web.gitlab_base_url` | string | `"https://gitlab.com"` | GitLab base URL for OAuth (the HTML site, NOT the API URL). Override for self-hosted GitLab. |
| `web.api_internal_url` | string | `"http://127.0.0.1:8383"` | Server-to-server URL where the web process reaches `aveloxis api`. The web server reverse-proxies `/api/*` requests to this URL so the browser only talks to the web origin. Set this to a remote URL if running the API on a different host. |

### Monitor (dashboard, v0.23.0)

The `monitor` block tunes the `/monitor` dashboard served by `aveloxis serve` on port `:5555`.

| Field | Type | Default | Description |
|---|---|---|---|
| `monitor.refresh_seconds` | int | `60` | Meta-refresh interval emitted in the dashboard HTML (`<meta http-equiv="refresh" content="N">`). Clamped to `[10, 3600]` at consumption — values outside that range fall back to the default. Lower values give snappier updates at the cost of more frequent server-side scans; higher values reduce DB pressure on large fleets. The pre-v0.23.0 hard-coded `60` is the same default. |

Mobile detection (also v0.23.0) is automatic and not configurable: when the dashboard handler observes a known mobile User-Agent (`iPhone`, `iPad`, `Android`, `Mobile`, `Windows Phone`, `BlackBerry`), it emits a `body.is-mobile` class that stacks the queue table into vertical cards. Desktop users at narrow window widths also pick up the same layout via a `@media (max-width: 768px)` block — UA detection is just an extra signal for phones with non-standard viewports.

### Mail (Gmail SMTP, optional)

See the [Email section below](#email-gmail-smtp-optional) for setup details. The `mail` block fields:

| Field | Type | Description |
|---|---|---|
| `mail.gmail_user` | string | Gmail address used for SMTP auth and as the `From` address. Empty disables the mailer (no-op). |
| `mail.gmail_app_password` | string | The 16-character App Password (spaces allowed). Not the account's regular password. |
| `mail.from_name` | string | Display name shown in recipients' inboxes. |
| `mail.site_url` | string | Public-facing URL used in email body links. |

### Logging

| Field | Type | Default | Description |
|---|---|---|---|
| `log_level` | string | `"info"` | Log verbosity level. Options: `debug`, `info`, `warn`, `error`. |

Log level descriptions:

- **`debug`** -- Very verbose. Includes individual API calls, staging writes, and contributor resolution details. Use for troubleshooting.
- **`info`** -- Default. Logs per-repo progress (start/finish, entity counts, phase transitions). Good for production monitoring.
- **`warn`** -- Logs non-fatal issues like individual entity upsert failures, missing contributors, and skipped repos.
- **`error`** -- Logs only fatal errors that prevent collection from continuing.

---

## API key sources

API keys are loaded from three sources, merged together in priority order:

1. **`aveloxis_ops.worker_oauth` table** -- Always checked first. Store keys here via `aveloxis add-key`. This is the recommended approach for production.

2. **`augur_operations.worker_oauth` table** -- Only checked when the `--augur-keys` flag is passed to `serve` or `collect`. Useful during migration before you have copied keys over.

3. **`aveloxis.json` config file** -- Lowest priority. The `github.api_keys` and `gitlab.api_keys` arrays. Convenient for standalone deployments or quick testing.

Keys from all sources are merged and deduplicated. If a key appears in multiple sources, it is used only once.

```{tip}
For production, store keys in the database with `aveloxis add-key` and leave the config file arrays empty. This keeps secrets out of configuration files and allows key management without restarting the service.
```

---

## API key rotation behavior

All loaded keys are rotated via **round-robin** to fully utilize every key's rate limit.

- Each GitHub token provides 5000 requests per hour.
- When a key's remaining requests drop to the **buffer threshold** (default: 15), it is skipped until its rate-limit window resets.
- Keys that return HTTP 401 (bad credentials) are **permanently invalidated** for the lifetime of the process.
- Keys that return HTTP 403 (rate limited) are temporarily skipped until their reset time.

### Throughput math

With N tokens, total throughput is approximately:

```
N * (5000 - 15) = N * 4985 requests/hour
```

| Tokens | Requests/hour | Notes |
|---|---|---|
| 1 | ~4,985 | Minimum viable for small instances |
| 4 | ~19,940 | Good for a few hundred repos |
| 10 | ~49,850 | Good for a few thousand repos |
| 74 | ~368,890 | Large-scale (Augur production) |

---

## Clone directory

The `collection.repo_clone_dir` setting controls where bare git clones are stored. These clones are permanent and used for incremental `git fetch` on subsequent collection cycles.

- **Default:** `$HOME/aveloxis-repos`
- **Sizing:** Each bare clone is typically 10-500 MB. For 400K repos, plan for multiple terabytes.
- **Performance:** Use an SSD or fast local storage. NFS can work but may slow the facade phase.
- **Full clones:** Temporary full checkouts (for analysis) are created inside this directory and deleted after use.

```{warning}
Do not delete this directory while Aveloxis is running. If deleted while stopped, the facade phase will re-clone all repos from scratch on the next run.
```

---

## Email (Gmail SMTP, optional)

Aveloxis can send transactional emails (welcome on first signup, group-approval notifications) via Gmail SMTP. The mailer is **optional** — when not configured, the application works fine without sending email.

### Setup

1. Pick a Gmail account dedicated to the deployment. This can be a personal Gmail account (`something@gmail.com`) or a Google Workspace account on a custom domain (`ops@aveloxis.io`). Either way, the value you put into `gmail_user` must be the **full email address**, not just the domain.
2. Enable **2-Step Verification** on that account: <https://myaccount.google.com/security>. App Passwords cannot be generated without 2SV, and regular account passwords stopped working with SMTP when Google deprecated "less secure app access" in 2022.
3. Generate an **App Password** for "Mail": <https://myaccount.google.com/apppasswords>. Google displays the password as `xxxx xxxx xxxx xxxx` (four groups of four lowercase letters). The actual auth token is the **16 contiguous lowercase letters**; the spaces are display formatting only. Aveloxis strips the spaces on load, so either form in `aveloxis.json` works.
4. Add a `mail` block to `aveloxis.json`:

```json
{
  "mail": {
    "gmail_user": "ops@aveloxis.io",
    "gmail_app_password": "abcd efgh ijkl mnop",
    "from_name": "Aveloxis",
    "site_url": "https://your-host.example"
  }
}
```

| Field | Required format | Purpose |
|---|---|---|
| `gmail_user` | Full email address with `@`. **Not** the bare domain. | Used both as the SMTP auth username and as the `From` address. Leaving this empty (along with `gmail_app_password`) disables the mailer (silent no-op). |
| `gmail_app_password` | Exactly 16 lowercase ASCII letters (display-format spaces fine). **Not** a regular account password. | The App Password generated in step 3. Validation rejects anything else at startup with a clear error message. |
| `from_name` | Free-form string | Display name shown in recipients' inboxes. Defaults to the bare email address when omitted. |
| `site_url` | Full URL | Public-facing URL for your Aveloxis deployment. Used in email body links. |

### Validation at startup

`aveloxis web` runs `mailer.ValidateConfig` against the supplied block when the server boots. If validation fails, the WARN line is emitted before any user can sign up:

- `mail.gmail_user "aveloxis.io" is not an email address` — you set the bare domain. Use the full address (`ops@aveloxis.io`).
- `mail.gmail_app_password is N character(s) after removing display-format spaces but Google App Passwords are exactly 16 lowercase letters` — you pasted a regular password or something else. Generate an actual App Password.
- `mail.gmail_user is empty but mail.gmail_app_password is set` (or vice versa) — partial config. Either fill both fields or empty both.

When validation fails, the mailer falls back to disabled behavior (no email sent, no errors raised by calling code) so the rest of the application keeps working. Fix the config and restart `aveloxis web` to enable the mailer.

### Verifying the setup with `aveloxis test-mail`

After fixing the config, send a one-shot test email without waiting for a user to sign up:

```bash
aveloxis test-mail your-personal-address@example.com
```

The command runs the same `ValidateConfig` check, then calls `mailer.Send` against `smtp.gmail.com:587`. Output:

- **Success**: `test email sent successfully to=...` — credentials are working. The test email arrives within seconds.
- **Validation error**: command exits non-zero with a clear message. Fix `aveloxis.json` and try again. No SMTP attempt is made.
- **SMTP error from Gmail itself** (e.g. `535 5.7.8 Username and Password not accepted`): credentials look syntactically correct but Gmail rejected them. Most likely: App Password generated against a different account, or 2-Step Verification was just disabled on the account that owns the App Password.

### Transport details

The mailer uses Go's stdlib `net/smtp` against `smtp.gmail.com:587` with STARTTLS and PLAIN auth. No third-party email library is required.

### Common failure modes

- **`535 5.7.8 Username and Password not accepted`** — credentials passed `ValidateConfig`'s syntactic check but Gmail rejected them at auth time. Causes: App Password was revoked, 2SV was disabled after the password was generated, or the App Password belongs to a different account than the one named in `gmail_user`.
- **`550 5.7.0 Mail relay denied`** — Gmail considers the recipient address invalid. Re-check the captured email in `aveloxis_ops.users`.
- **No log entry at all** — `gmail_user` is empty (mailer disabled). Add the config block and restart.

### Disabling

Remove or empty BOTH `gmail_user` AND `gmail_app_password`. Setting only one without the other is treated as a misconfiguration. With both empty, the mailer is a silent no-op and the rest of the application continues to work.

---

## Next steps

- [Quick Start](quickstart.md) -- get collecting in 5 steps
- [Commands Reference](../guide/commands.md) -- full CLI reference
