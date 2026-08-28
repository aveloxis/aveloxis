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

A full configuration with **every** supported option.
`TestConfigurationDocSnippetMatchesEffectiveDefaults` enforces that
claim on two axes, and they have different reach:

- **Presence — every block, every key.** Each section of the snippet
  must carry every field of its Go config struct. An omitted key is
  invisible to any value check (absent simply means "use the
  default"), so presence is asserted separately. The one reasoned
  exemption is `github.gitlab_hosts`: the two forge blocks share one
  struct and that field is GitLab-only.
- **Values — every block that has compiled defaults.** `collection`,
  `api`, `monitor` and `mail` are compared field by field against the
  compiled defaults, through the same effective accessors the running
  code uses. The exceptions are named and carry a reason in the test:
  `mail`'s sender address, app password, display name and site URL are
  placeholders with no compiled default. `database`, `github`,
  `gitlab` and `web` are **not** value-checked at all — every field
  there is site-specific (hosts, tokens, secrets), so treat those
  values as illustrative and take their defaults from the reference
  tables below.

The value check exists because the snippet taught
`scancode_shutdown_grace_minutes: 30` for three months after the
default flipped to `0`, and `threading_mode: "sharded"` for three and
a half against its own reference table below:

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
    "site_url": "https://your-host.example",
    "operator_email": "",
    "vuln_digest_min_severity": "HIGH",
    "vuln_digest_interval_hours": 24,
    "vuln_digest_include_transitive": false,
    "vuln_digest_include_dev": false
  },
  "collection": {
    "days_until_recollect": 1,
    "archived_recollect_multiplier": 6,
    "workers": 12,
    "repo_clone_dir": "/data/aveloxis-repos",
    "force_full": false,
    "matview_rebuild_day": "saturday",
    "matview_rebuild_on_startup": false,
    "matview_rebuild_skip_dm_aggregates": false,
    "activity_history_window_days": 180,
    "activity_history_interval_minutes": 1,
    "activity_history_batch": 150,
    "activity_history_concurrency": 8,
    "activity_history_window_concurrency": 4,
    "activity_history_cooldown_days": 90,
    "pr_child_mode": "graphql",
    "listing_mode": "graphql",
    "threading_mode": "single",
    "shard_size": 3000,
    "issue_child_mode": "graphql",
    "enrich_interval_minutes": 30,
    "search_resolve_interval_minutes": 60,
    "affiliation_interval_minutes": 60,
    "breadth_interval_minutes": 15,
    "breadth_batch_size": 2000,
    "breadth_cooldown_days": 7,
    "breadth_fetch_concurrency": 8,
    "shutdown_grace_seconds": 10,
    "scancode_workers": 2,
    "scancode_start_interval_s": 90,
    "scancode_cadence_days": 180,
    "scancode_clone_dir": "/tmp/aveloxis-scancode",
    "scancode_shutdown_grace_minutes": 0,
    "scancode_run_timeout_hours": 2,
    "scancode_run_timeout_cap_hours": 24,
    "scancode_max_in_memory": 5000,
    "vuln_scan_transitive": true,
    "dev_build_deps": false,
    "github_actions_deps": false,
    "skip_largest_percent": 0,
    "scorecard_timeout_minutes": 15,
    "scorecard_token_count": 0,
    "scancode_timeout_cap_strikes": 3,
    "scancode_ignore_globs": [],
    "staging_retention_hours": 1,
    "phase_watchdog_minutes": 75,
    "distribution_tracking_enabled": false,
    "distribution_tracking_interval_days": 180,
    "distribution_tracking_workers": 4,
    "distribution_tracking_start_interval_s": 30,
    "distribution_tracking_polite_email": "",
    "distribution_tracking_user_agent": "",
    "distribution_tracking_cross_check_sources": true,
    "distribution_tracking_immediate_partial_reclaim": true,
    "mailing_list_enabled": false,
    "mailing_list_workers": 2,
    "mailing_list_cadence_days": 30,
    "mailing_list_backfill_months": 6,
    "mailing_list_polite_email": "",
    "mailing_list_mirror_handling": "metadata_only",
    "mailing_list_processor_workers": 1
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
    "api_internal_url": "http://127.0.0.1:8383",
    "spa_url": "",
    "auto_approve_add_limit": 0
  },
  "monitor": {
    "refresh_seconds": 60
  },
  "api": {
    "rate_limit_rps": 1,
    "rate_limit_burst": 10,
    "rate_limit_daily": 1000,
    "exempt_cidrs": ["127.0.0.0/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
    "cors_origins": [],
    "trusted_proxy": "",
    "require_auth": false
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
| `collection.archived_recollect_multiplier` | integer | `6` | v0.28.1: stretches the recollect interval for ARCHIVED repos (`due_at = last_collected + days_until_recollect × multiplier`). Archived repos still recollect — they can be unarchived or gain issues — just less often; on a large fleet this recovers the ~20% of queue churn otherwise spent on mostly-304 cycles over frozen repos. Set `1` to disable the stretch. Applied inside both due_at writers, so config changes take effect fleet-wide on the next serve restart. |
| `collection.workers` | integer | `12` | Number of concurrent collection workers when running `aveloxis serve`. Each worker may make many concurrent DB calls; the pgx pool is sized as `max(workers + 15, 20)`. |
| `collection.repo_clone_dir` | string | `~/aveloxis-repos` (computed at startup) | Directory for bare git clones used by the facade phase. Can grow to terabytes for large instances (400K+ repos). |
| `collection.force_full` | boolean | `false` | Fleet-wide: when `true`, every collection pass runs `since=zero` regardless of `last_collected`. Use this once after a systemic bug fix that invalidates collected data, then revert to `false`. For per-repo full re-collection, use `aveloxis recollect <url>` instead (sets a queue flag, doesn't touch this setting). |

**Materialized views**

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.matview_rebuild_day` | string | `"saturday"` | Day of the week the scheduler refreshes the 20 materialized views. Values: `"sunday"`–`"saturday"`, or `"disabled"` / `"none"` / `"off"` to never auto-rebuild. Independent of `aveloxis refresh-views` which always refreshes on demand. |
| `collection.matview_rebuild_on_startup` | boolean | `false` | When `true`, `aveloxis serve` rebuilds the matviews on every startup. Default `false` because the rebuild can take many minutes on large fleets and `migrate` already refreshes them on schema changes. |
| `collection.activity_history_window_days` | integer | `180` | Span of each GitHub `contributionsCollection` window the daily contributor-history backfill queries (v0.27.58). Clamped to 365 (GitHub's hard 1-year window limit); non-positive falls back to 180. This is the STARTING span — when a window hits the 100-repositories-per-type cap or a contribution page cap, the worker halves the window recursively and logs the cap hit at INFO so loss rate is trackable. |
| `collection.activity_history_interval_minutes` | integer | `1` | v0.28.3: the history sweep's tick interval. Ticks arriving while a cycle is still running are dropped (single-flight), so a shorter interval never overlaps cycles. |
| `collection.activity_history_batch` | integer | `150` | v0.28.3: contributors claimed per sweep cycle. |
| `collection.activity_history_concurrency` | integer | `8` | v0.28.3: contributors fetched concurrently within a cycle (the worker pool). The pre-v0.28.3 sweep was fully serial — ~1,170 contributors/day against a multi-million pool. |
| `collection.activity_history_window_concurrency` | integer | `4` | v0.28.3: history windows fetched concurrently per contributor (a 10-year account is ~20 windows; the serial chain was the dominant per-contributor cost). |
| `collection.activity_history_cooldown_days` | integer | `90` | v0.28.3: the re-audit cooldown — how long a backfilled contributor stays out of the claim pool (jittered like the breadth cooldown). |
| `collection.matview_rebuild_skip_dm_aggregates` | boolean | `false` | When `true`, the weekly scheduler rebuild refreshes ONLY the materialized views and skips the `dm_` aggregate table pass (a per-repo loop that can run for days on fleet-scale databases — while it runs, new collection claims are paused). With the skip on, `dm_repo_*` / `dm_repo_group_*` tables update only when an operator runs `aveloxis refresh-views --aggregates` (v0.28.18; plain `refresh-views` and `aveloxis migrate` refresh the materialized views only). To disable the weekly rebuild entirely (matviews AND aggregates), set `matview_rebuild_day` to `"disabled"`. |

**REST → GraphQL refactor (v0.18.x phases)**

These four settings control the staged collector's request shape. **GraphQL is the default** for `pr_child_mode`, `listing_mode`, and `issue_child_mode` (flipped in v0.26.0 after shadow-diff validation) — the ~5× wall-clock speedup observed in benchmarks (augurlabs/augur, 73 keys: 125 min REST → 24 min GraphQL) is what a fresh deployment gets out of the box. Set any of them to "rest" as the escape hatch for a systemic GraphQL outage; the REST path remains first-class.

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.pr_child_mode` | string | `"graphql"` (v0.26.0; was `"rest"`) | `"rest"` uses the per-PR REST waterfall (8 calls per PR). `"graphql"` (v0.18.1+) uses `FetchPRBatch` — one GraphQL query per 10 PRs returning all child data inline. GitLab path is REST composition in both modes (column parity preserved). |
| `collection.listing_mode` | string | `"graphql"` (v0.26.0; was `"rest"`) | `"rest"` uses separate iterators for `/issues` and `/pulls`. `"graphql"` (v0.18.2+) calls `ListIssuesAndPRs` once per repo — a pair of paginated GraphQL queries instead of two REST scans. Setting both this AND `pr_child_mode` to `"graphql"` activates v0.18.5's `fullGraphQLMode` gate: conversation comments are delivered inline, eliminating one repo-wide REST call. |
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
| `collection.breadth_interval_minutes` | integer | `15` | Cadence (minutes) of the v0.20.17 contributor breadth ticker. Each tick calls `/users/{login}/events` for up to `breadth_batch_size` contributors past their cooldown window and stamps `contributors.cntrb_last_breadth_at`. Pre-v0.20.17 this was hardcoded to 6 hours / 100 batch / no cooldown (400 contributors/day — 9.6 years for one pass over a 1.4M fleet). At 15-min interval × 2000 batch a cycle-per-tick cadence targets ~192K contributors/day → first pass over a 2.3M-contributor fleet in ~12 days, provided each cycle actually finishes within the interval — which is what `breadth_fetch_concurrency` controls (a sequential cycle over 2000 contributors at HTTP-RTT speed takes ~15–30 min; 8 concurrent fetchers bring it to ~2–4 min). |
| `collection.breadth_batch_size` | integer | `2000` | Maximum contributors processed per breadth tick. Each contributor takes 1–3 API calls (most users have ≤300 recent events fitting in one page). Operators raising this toward fleet scale (e.g. 18000) should raise `breadth_fetch_concurrency` in step so the cycle completes within `breadth_interval_minutes`. |
| `collection.breadth_cooldown_days` | integer | `7` | Minimum interval between successive breadth attempts on the same contributor. After this window the contributor becomes eligible again via the `cntrb_last_breadth_at IS NULL OR < NOW() - interval` filter. Steady-state load with a 7-day cooldown over a 2.3M-contributor fleet is ~330K/day ≈ 14K/hour ≈ 4% of the 365K/hr budget of a 73-key fleet. |
| `collection.breadth_fetch_concurrency` | integer | `8` | Number of fetcher goroutines the breadth worker runs per cycle (v0.27.8). Pre-v0.27.8 the worker was strictly sequential — one contributor, one HTTP request in flight — so throughput was bounded by HTTP round-trip time (~1–2 contributors/sec) no matter how large the key pool. Fetches run concurrently; persistence (event inserts + `cntrb_last_breadth_at` stamps) stays on a single coordinator that writes in batches. The cycle-complete log line reports `contributors_per_sec` and `fetch_concurrency` so you can measure the effect of changes. Raising it burns the shared API budget faster; 8 keeps one breadth cycle from dominating the pool. |

**Shutdown**

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.shutdown_grace_seconds` | integer | `10` | v0.20.0: ctx-cancel grace window for in-flight workers before `Scheduler.Run` closes the pgx pool. Pre-v0.20.0 the wait was unbounded — a 26-minute `commits` UPDATE blocked shutdown for the full duration. Setting this too low means worker transactions abort mid-flight (Postgres rolls them back safely but logs are noisy); too high means slow shutdown. |

#### Scancode worker (v0.21.0)

The scancode per-file license + copyright + package scan is run by a dedicated `ScancodeWorker` pool, decoupled from the per-repo collection pipeline. Pre-v0.21.0 scancode ran inline in `AnalysisCollector.AnalyzeRepo` gated by a 2-slot package-level semaphore; the 2026-05-14 production incident showed that shape doesn't survive fleet-scale operation (177 of 180 collection workers parked behind the semaphore for 7+ hours). The decoupled pool fixes the structural problem and adds operator-tunable cadence + concurrency. See `docs/architecture/scancode.md` for the full architecture write-up.

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.scancode_workers` | integer | `2` | Maximum concurrent scancode invocations. Pre-v0.21.0 the limit was hardcoded to 2; the default matches that so upgrading operators don't see a sudden change in scancode CPU load. Operators with spare CPU cores should raise this (the user running the fleet that surfaced the 2026-05-14 incident has tested 12 against 64 cores). v0.27.6: an EXPLICIT `0` disables the scancode pool on `aveloxis serve` entirely — the dedicated-scancode-host recipe (an adjacent machine runs `aveloxis scancode-worker` against the shared DB; see the [dedicated scancode host guide](../guide/dedicated-scancode-host.md)). An absent key keeps the default of 2; pre-v0.27.6 an explicit 0 was silently clamped to 2. |
| `collection.scancode_start_interval_s` | integer | `90` | Minimum seconds between *successful* scancode claim starts (v0.21.3+). As of v0.21.3 this is a minimum-gap pacing primitive, not a throughput cap — the dispatcher claims as fast as workers free up, with this interval enforced only between consecutive successful starts. Bounds clone-bandwidth bursts on restart. Pre-v0.21.3 this was a `time.NewTicker` rate cap and limited first-pass throughput to one claim per interval regardless of worker availability; that bug is documented in the v0.21.3 release notes. |
| `collection.scancode_cadence_days` | integer | `180` | Minimum days between successive scancode runs on the same repo. Pre-v0.21.0 was 30 days; the change reflects that per-file license + copyright headers in source files change rarely on the timescale that matters, and the I/O cost of scanning a Linux-kernel-scale mirror doesn't justify monthly re-scans. Dependency-level licenses (which DO change as packages update) still flow through the per-cycle Phase 4 dependency scan + Phase 6 SBOM generation. |
| `collection.scancode_clone_dir` | string | `"/tmp/aveloxis-scancode"` | Parent directory for per-run shallow clones. Each scan creates `<dir>/repo_<id>_<unix_ts>` and removes it on completion (success or failure). Size budget: each clone is the working tree only (`git clone --depth 1`), so ≈ checked-out repo size. With default 2 workers and average ~50 MB clones, ~100 MB peak; raise expectations for big-repo / many-worker installs. |
| `collection.scancode_shutdown_grace_minutes` | integer | `0` | Extra time the `ScancodeWorker` waits, on top of the fixed bookkeeping allowance, for the runners' POST-kill DB bookkeeping on `aveloxis stop` — it cannot let a scan finish (every scan is killed at cancel since v0.23.3). v0.23.7 flipped the default from 30 minutes to 0. Rationale: a scancode subprocess that outlives `aveloxis stop` cannot deliver its output back — the JSON file is read by Go code inside aveloxis. The v0.21.0 `recoverOrphans` path on next startup notices orphaned lock rows and either ingests from disk if a usable file exists or clears the lock; either way, lingering past stop buys nothing. Since v0.23.3 the scan subprocess is killed at cancel regardless of this value — it cannot let a scan finish; it only lengthens the wait for the runners' post-kill bookkeeping (lock clear / completion-stamp retry), and `aveloxis stop`, the `serve` process join and the documented systemd `TimeoutStopSec` size their budgets from it (v0.28.18). |
| `collection.scancode_run_timeout_hours` | integer | `2` | v0.23.8. BASE wall-clock timeout for a single scancode subprocess. Default 2h matches the pre-v0.23.8 hardcoded constant. The effective per-job timeout is `min(base * 2^scancode_timeout_attempts, scancode_run_timeout_cap_hours)`: every time a scan exits with `signal: killed` (cmd.Cancel signature when the wall-clock fires), the row's `scancode_timeout_attempts` counter increments and the next attempt gets twice the timeout. Kernel-class repos (~80K files, ~3h scan minimum) discover their natural runtime over a few cycles. Operators with a fleet skewed toward big repos can raise the base directly (e.g., `8`) rather than waiting for adaptive scaling. Timeout-class failures do NOT increment `scancode_failed_attempts` (the v0.21.4 10-strike sideline counter) — kernel-class repos legitimately need long timeouts and shouldn't be sidelined. |
| `collection.scancode_run_timeout_cap_hours` | integer | `24` | v0.23.8. Upper bound on the adaptive per-job scancode timeout. Even kernel-class repos shouldn't need a single scan slot for more than a day; rows that genuinely take longer are more likely broken than legitimately big. Combined with the cap, a repo's effective timeout is `min(base * 2^attempts, cap)` — so attempts 0/1/2/3/4 with base=2h compute to 2h/4h/8h/16h/24h-capped. Operators with extremely large fleets can raise the cap; the v0.21.4 ScancodeMaxFailures (10-strike sideline on the SEPARATE `scancode_failed_attempts` counter) still bounds genuine-failure risk. |
| `collection.scancode_max_in_memory` | integer | `5000` | v0.25.2. Caps how many file scan results scancode keeps in RAM before spilling intermediate state to a tempfile. The value flows verbatim to scancode's `--max-in-memory N` argument. Default 5000 matches the pre-v0.25.2 hardcoded value and is conservative — appropriate for low-memory dev hosts. Production hosts with hundreds of GB of RAM can safely raise this (e.g. `50000` or higher) to speed up monorepo scans where the default forces an early disk spill; the linux kernel and chromium-class repos benefit most. Memory cost is roughly per-process: `--processes N` × `--max-in-memory M` × per-file working set, so account for the multiplier when sizing on RAM-rich hosts. Zero or negative values fall back to the default; the value never reaches the scancode CLI unchecked. |
| `collection.vuln_scan_transitive` | bool | `true` (since v0.27.136) | Scan the FULL transitive closure each committed lockfile enumerates, in addition to declared direct dependencies, and store the dependency edges that power `introduced_by` chain attribution (v0.27.133) and the real SBOM dependency graphs (v0.27.134). Transitive findings carry `dependency_kind='transitive'` and render behind a toggle in the GUI (direct view is the default). The default flipped from `false` on 2026-08-21 canary evidence: the knob both ADDS transitive findings and REMOVES range-floor false positives (lockfile-resolved versions scan as `locked`). Set explicitly to `false` to opt out — turning it off resolves the transitive findings on each repo's next scan (rows remain as historical record). |
| `collection.dev_build_deps` | bool | `false` | v0.27.45 (summary/19 P2): expand Python dependency collection to dev/test/build/optional manifest families — requirements-variant files (`requirements-dev.txt`, `test_requirements.txt`, `requirements/*.txt`), pyproject `[build-system].requires` / `[project.optional-dependencies]` / PEP 735 `[dependency-groups]` / poetry groups, Pipfile `[dev-packages]`, setup.py `tests_require`/`extras_require`, setup.cfg `[options.extras_require]`. OPT-IN: this is the findings-volume driver — Python dev tooling is stale-pin-dense, so vulnerability counts jump when it flips on. Canary on a small database first. The Go test-only relabel and the per-ecosystem scope relabels (v0.27.44) are always on — they relabel existing rows without adding any. |
| `collection.github_actions_deps` | bool | `false` | v0.27.47 (summary/19 P4): inventory GitHub Actions workflow `uses:` references as build-scope dependencies (`manager='githubactions'`, libyear NULL) and scan them against OSV's "GitHub Actions" ecosystem. OSV carries no purl and no version comparator for Actions, so the query is versionless and the pinned ref is evaluated client-side; commit-SHA pins produce no findings (cannot be compared — and are the recommended mitigation posture anyway). Its own knob — a new ecosystem, not a scope refinement. Canary before fleet enablement. |
| `collection.skip_largest_percent` | float | `0` | TEMPORARY throughput lever (v0.27.35): exclude the fleet's top-N% repos — by forge-reported commit count OR pull-request count, either qualifies — from collection claims, so a handful of kernel/pytorch-class monsters can't occupy every worker slot and starve the rest of the fleet. `0.5` skips the top 0.5%. Skipped repos stay queued and untouched; remove the knob and restart serve and they collect normally (they'll be immediately due). The excluded set refreshes every 6 hours and its effective thresholds are logged (`large-repo skip ACTIVE`). While enabled the gate is absolute — a monitor "Boost" cannot override it. Repos never measured (no repo_info yet) are never skipped. |
| `collection.scorecard_timeout_minutes` | integer | `15` | v0.27.5. Per-ATTEMPT wall-clock cap for a single OpenSSF Scorecard invocation. The scorecard phase is remote-primary for GitHub repos (`--repo`, ~18 checks) with a local backstop against the retained analysis clone (`--local`, ~11 checks) — the remote attempt gets this timeout, and the local fallback gets a fresh window, so a repo holds the phase for at most 2× this value. Motivation: the measured multi-DAY remote hangs on production (scorecard with one shared token sleeps through rate-limit resets). Zero or negative falls back to 15. |
| `collection.scorecard_token_count` | integer | `0` | v0.27.5. How many key-pool tokens to hand scorecard as a comma-separated `GITHUB_TOKEN` (scorecard round-robins the list per request). `0` (the default) = ALL non-invalidated pool tokens; `N>0` = the first N. Multi-token is what makes remote-primary mode viable: a moderate repo costs ~40 API calls in remote mode, and spreading them across the pool prevents any single token from being drained by scorecard while collection workers share it. Negative values collapse to 0. |
| `collection.scancode_timeout_cap_strikes` | integer | `3` | v0.27.6. Number of CONSECUTIVE wall-clock timeouts **at the adaptive-timeout cap** (`scancode_run_timeout_cap_hours`) after which the repo is sidelined exactly like the v0.21.4 10-strike failure path (`scancode_last_run` is stamped so the cadence gate excludes it for the full cadence window, default 180 days). The v0.23.8 design deliberately never sidelined timeout-class failures — correct while the timeout can still grow, but once a repo times out AT the cap no bigger timeout is coming. June 2026 production logs showed the consequence: pytorch/docs and WHO/smart-html (multi-GB generated-HTML repos) were claimed 27× each, every cycle burning a 24-hour worker slot forever. Below-cap timeouts still never count toward the sideline; the diagnostic trail stays in `scancode_timeout_attempts` (not reset by the sideline). Zero or negative values fall back to 3 — there is deliberately no "unlimited retries" setting. |
| `collection.scancode_ignore_globs` | string array | `[]` | v0.27.6. Optional path globs passed to the scancode subprocess as repeated `--ignore <glob>` flags (e.g. `["*.min.js", "*/node_modules/*", "*/docs/_build/*"]`). Empty by default — no flags are added and scan behavior is byte-identical to pre-v0.27.6. Use it to exclude generated or vendored trees fleet-wide; per-repo skips of overwhelmingly-generated repos are handled automatically by the v0.27.6 generated-content skip policy (`scancode_skip_reason = 'generated-content'`). |
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

#### DistributionWorker (v0.24.0)

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.distribution_tracking_enabled` | bool | `false` | v0.24.0. Master switch for the DistributionWorker — the periodic worker pool that records evidence of where each repo is *published* (deps.dev, ecosyste.ms, GitHub Packages, GitHub release assets) and which manifests it carries (intent). **Off by default**: the subsystem makes outbound calls to deps.dev + ecosyste.ms and operators should explicitly opt in. Independent of every other collection setting; flipping this on does not affect the per-repo collection pipeline. |
| `collection.distribution_tracking_interval_days` | integer | `180` | v0.24.0. Per-repo cadence (in days) between successive distribution scans. Default 180 (6 months) — package-distribution mappings are stable on this timescale; re-scanning more frequently buys little signal at the cost of registry API load. The next scan picks up: new ecosystems the repo was published to, deprecated packages no longer in the registry, and updated `latest_published_at` timestamps. The prior snapshot rotates into `repo_distribution_history` so analysts can observe drift over time. |
| `collection.distribution_tracking_workers` | integer | `4` | v0.24.0. Concurrent runner goroutines fetching against deps.dev / ecosyste.ms / GitHub. Each runner performs ~5 cheap HTTP calls per claimed repo; concurrency is bounded primarily to keep total outbound traffic predictable. Raise this only if first-pass coverage timing matters and outbound bandwidth is not a concern. |
| `collection.distribution_tracking_start_interval_s` | integer | `30` | v0.24.0. Minimum seconds between successful CLAIM operations. With default 4 workers and a 30s ticker, steady-state throughput is ~120 repos/hour — comfortably under any known external rate limit. Same minimum-gap pacing primitive as scancode (post-v0.21.3); not a throughput cap. |
| `collection.distribution_tracking_polite_email` | string | `""` | v0.24.0. Value sent in the `From:` HTTP request header to ecosyste.ms so the operator's traffic lands in their "polite pool" priority queue. Optional but recommended: ecosyste.ms documents the polite-pool contract at https://ecosyste.ms — provide a real email address so they can contact you should rate-limit discussions be needed. Missing value falls back to the lower-priority "common pool". |
| `collection.distribution_tracking_user_agent` | string | `""` | v0.24.0. Overrides the User-Agent header sent to deps.dev / ecosyste.ms / GitHub. When empty the client uses `aveloxis/<tool_version>`. Operators behind shared egress IPs may want a more identifying string so registry operators can route diagnostics. |
| `collection.distribution_tracking_cross_check_sources` | bool | `true` | v0.25.0. When true (the default), guarantees BOTH deps.dev AND ecosyste.ms are queried for every repo even when one returns non-empty data. Each source persists its own rows into `repo_distribution` (UNIQUE constraint includes the `source` column so two rows for the same package coexist). The trade-off is ~2× external-registry API calls per scan, but at 180-day cadence the absolute budget is tiny (~5K calls/hour on a 100K-repo fleet). Operator-mandated lock-in for v0.25.0 — set to false only when you explicitly want to halve registry traffic at the cost of single-source-of-truth dependence. The field is a JSON boolean; when omitted from `aveloxis.json`, the v0.25.0 default of `true` applies (pointer-to-bool internally so the decoder distinguishes "absent" from "explicit false"). **v0.25.x-era escape hatch** — see [v0.25.x distribution-tracking knobs](#v025x-distribution-tracking-knobs) for the planned deprecation horizon. |
| `collection.distribution_tracking_immediate_partial_reclaim` | bool | `true` | v0.25.3. When true (the default), keeps the v0.25.0 behavior: a repo whose last scan was partial (`distribution_scan_complete = FALSE` — typically because the ecosyste.ms circuit breaker was open during the scan) is immediately re-eligible on the next dispatcher cycle, bypassing the cadence gate. The `ClaimNextDistributionRepo` WHERE clause includes `OR COALESCE(scan_complete, TRUE) = FALSE` in this mode. **Set to `false`** to suppress that behavior: partial-scan rows then wait for normal cadence like everything else. The `ORDER BY scan_complete ASC` tiebreaker stays in both modes — among cadence-elapsed rows, partial scans still get priority, just don't bypass the gate. Operator framing: the immediate-reclaim design is correct *during* a v0.24.x → v0.25.x transition when partial-scan repos legitimately need urgent re-collection; once a fleet is through that cohort and steady-state cadence resumes, the mechanism becomes operational churn rather than a recovery tool. This knob is the explicit off-switch. Pointer-to-bool internally — when omitted from `aveloxis.json`, the v0.25.3 default of `true` applies, preserving v0.25.0/v0.25.1 behavior on existing fleets. **v0.25.x-era escape hatch** — see [v0.25.x distribution-tracking knobs](#v025x-distribution-tracking-knobs) for the planned deprecation horizon. |

#### Mailing-list worker (v0.25.7)

| Field | Type | Default | Description |
|---|---|---|---|
| `collection.mailing_list_enabled` | bool | `false` | v0.25.7. Master switch for the MailingListWorker — the decoupled pool that ingests mailing-list archives (Apache Pony Mail today; lore.kernel.org public-inbox planned) into `email_message` + `messages`. **Off by default**: makes outbound calls to archive hosts and depends on a populated per-PMC `repo_group` (run `load-foundation-orgs` first). Independent of the per-repo collection pipeline. |
| `collection.mailing_list_workers` | integer | `2` | v0.25.7. Concurrent list-runner goroutines. Each claims one list and scans it month-by-month with adaptive (AIMD) pacing, so modest concurrency is intentional — these are community archive servers, not a CDN. |
| `collection.mailing_list_cadence_days` | integer | `30` | v0.25.7. Per-list tail-refresh cadence in days. A list re-scans (from its `mlls_last_month` checkpoint forward) once this elapses. Deep history is collected once on the first pass; subsequent passes only fetch new months. |
| `collection.mailing_list_backfill_months` | integer | `6` | v0.25.7. How many months of history to scan when a list has no checkpoint yet. Bounds the first-pass cost — full-archive backfill would be enormous on high-volume lists, so the default is a recent window. Raise it to deepen history, or set it to **`0` (or negative) for full history** from each list's first month (v0.25.12 — `0` was previously coerced to 6, so full history was unreachable and lists only collected the recent window). **Applies only to un-checkpointed lists:** once scanned, a list resumes forward from its checkpoint, so changing this value does NOT re-backfill already-scanned lists. To re-scan from the beginning, reset the checkpoint: `UPDATE aveloxis_data.repo_groups_list_serve SET mlls_last_month='', mlls_scan_complete=FALSE, mlls_last_run=NULL WHERE mlls_system <> ''` then restart serve. |
| `collection.mailing_list_polite_email` | string | `""` | v0.25.7. Contact address embedded in the `User-Agent` sent to archive hosts, so admins can reach the operator instead of blocking (Apache/lore actively gate scrapers). Recommended whenever `mailing_list_enabled` is true. |
| `collection.mailing_list_mirror_handling` | string | `"metadata_only"` | v0.25.7. How to handle messages that mirror data we already collect from GitHub (`github_mirror`/`commit_notify` classes). `metadata_only` (default): record the `email_message` provenance row + link, but do NOT re-copy the body into `messages`. `skip`: drop mirrors entirely. `full`: keep everything (belt-and-suspenders completeness). §5 of the design — awareness, not zero-overlap zealotry. |
| `collection.mailing_list_processor_workers` | integer | `1` | v0.25.x. Drain goroutines **per mailing-list system** for the resolve+write half of the pipeline. The fetch+classify worker stages classified messages into `aveloxis_ops.mailing_list_staging`; the `MailingListProcessor` drains that staging table and does the DB-dependent work (sender→contributor resolution, mirror-link, signaled-repo, and the `email_message` / `messages` / `email_message_ref` writes). This staging→batch boundary is what keeps the mailing-list pipeline off the per-message direct-upsert path that reproduced Augur's lock contention on the hot tables. Draining is **single-threaded per list** (summary/12 §11); `1` (default) drains one list at a time. `>1` fans out across **distinct** lists only — an in-process per-list guard keeps two goroutines off the same list. Keep at `1` unless a deep per-list backlog needs cross-list parallelism. |

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
| `web.spa_url` | string | `""` | Trusted origin of the separate-repo SPA (aveloxis-gui), e.g. `https://gui.example.org` (or `http://localhost:8000` in dev). When set, the OAuth flow honors a `?next=` URL under this origin so signing in from the SPA returns the user to the SPA instead of the server-rendered `/dashboard`. Relative `next` paths are always honored; anything else is rejected (open-redirect protection). |
| `web.auto_approve_add_limit` | int | `0` | Per-add approval (v0.27.20): when > 0, a non-admin batch of NOT-yet-tracked repo URLs at or under this size is collected immediately (with an auto-approved audit request); larger batches — and ALL org registrations — wait for admin approval on the Approvals page. `0` (default) = every non-admin addition of new repos requires approval. Already-collected repos always link instantly for everyone; approval gates collection load, never visibility. |

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
| `mail.operator_email` | string | Where fleet-level operator notifications go (v0.27.12) — currently the new-vulnerabilities digest. Empty (default) disables operator notifications entirely. |
| `mail.vuln_digest_min_severity` | string | Severity floor for the vulnerability digest: `CRITICAL`, `HIGH` (default — admits CRITICAL+HIGH), `MEDIUM`, `LOW`, or `ALL`. Unrecognized values fall back to `HIGH`. |
| `mail.vuln_digest_interval_hours` | int | Minimum gap between digest emails (default 24). The scheduler checks hourly; a digest is sent only when the interval has elapsed AND new findings exist — quiet windows produce no email. |
| `mail.vuln_digest_include_transitive` | bool | `false` | Include `dependency_kind='transitive'` findings in the operator vulnerability digest (v0.27.21). Default off so the first transitive-enabled cycles don't flood the digest with utility-package findings. |
| `mail.vuln_digest_include_dev` | bool | `false` | v0.27.46 (summary/19 P3): include findings on non-runtime-scope dependencies (dev/test/build/optional/peer) in the operator digest. Default off so the `dev_build_deps` Python expansion never floods the email; runtime-scope findings always digest. |
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

- **Default:** `~/aveloxis-repos` — computed from the process's home directory at startup when the key is absent.
- **No environment expansion:** values in `aveloxis.json` are used LITERALLY — `$HOME` or `~` in the JSON becomes a relative directory named `$HOME`/`~` under the working directory. Use a real absolute path, or omit the key to get the computed default.
- **Sizing:** Each bare clone is typically 10-500 MB. For 400K repos, plan for multiple terabytes.
- **Performance:** Use an SSD or fast local storage. NFS can work but may slow the facade phase.
- **Full clones:** Temporary full checkouts (for analysis) are created inside this directory and deleted after use.

```{warning}
Do not delete this directory while Aveloxis is running. If deleted while stopped, the facade phase will re-clone all repos from scratch on the next run.
```

---


## Public analytics API (`api`)

v0.27.0: rate limiting + CORS for the `aveloxis api` process. Limits
apply ONLY to clients whose resolved IP falls outside `exempt_cidrs` —
same-box and same-LAN traffic is never limited.

| Field | Default | Meaning |
|---|---|---|
| `rate_limit_rps` | `1` | Sustained per-IP requests/second (token bucket). |
| `rate_limit_burst` | `10` | Per-IP burst capacity. |
| `rate_limit_daily` | `1000` | Per-IP daily request quota — the anti-bulk-crawl control. Exceeding returns 429 with `Retry-After: 86400`. |
| `exempt_cidrs` | loopback + RFC1918 + `::1/128` | Client networks that bypass limiting entirely. |
| `cors_origins` | `[]` | Browser origins allowed to call the API (the separate-repo aveloxis-gui). Empty = no cross-origin access. |
| `trusted_proxy` | `""` | Peer IP whose `X-Forwarded-For` is believed when resolving the client address. Set this to your nginx host when proxying — otherwise every request appears to come from the proxy and the exemption/limits misapply. Empty = XFF ignored (spoof-safe default). |
| `require_auth` | `false` | Gate every data endpoint (all but `/health`) behind Bearer session tokens minted by the web process's `/auth/token`. Flip on once the aveloxis-gui token flow is deployed. Exempt-CIDR clients bypass auth even when enabled. Scoped users receive structured 403s for repos outside their approved groups. |

For the full public-GUI deployment runbook (nginx layout, OAuth
callbacks, when to flip `require_auth`), see the aveloxis-gui
repository's README "Production deployment" section.

```jsonc
"api": {
  "rate_limit_rps": 1,
  "rate_limit_burst": 10,
  "rate_limit_daily": 1000,
  "exempt_cidrs": ["127.0.0.0/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
  "cors_origins": ["https://your-gui.example.org"],
  "trusted_proxy": "127.0.0.1"
}
```

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

## v0.25.x distribution-tracking knobs

Several settings in the `collection` block exist specifically to give operators control over edge-case behavior introduced during the v0.24.0 → v0.25.x transition of the DistributionWorker subsystem. They are documented here as a coherent group because they share the same lifecycle: useful now during the transition cohort, scheduled for removal once v0.24.x support ends.

### The settings in this group

| JSON key | Introduced | Purpose |
|---|---|---|
| `collection.distribution_tracking_cross_check_sources` | v0.25.0 | When `true` (default), always queries BOTH deps.dev AND ecosyste.ms even when one returns data. Locks in cross-source verification. Operator-mandated lock-in flag. |
| `collection.distribution_tracking_immediate_partial_reclaim` | v0.25.3 | When `true` (default), partial-scan repos (`distribution_scan_complete = FALSE`) bypass the cadence gate and re-collect on the next dispatcher tick. When `false`, they wait for normal cadence. |

In addition, three one-shot migrations run on every `aveloxis migrate` (all self-disabling via WHERE clauses, so re-runs are no-ops once the cohort they target has been processed):

- **v0.24.1 reset** — `distribution_last_run = NULL` for fleets with zero deps.dev rows, fixing the v0.24.0 URL-encoding bug's silent-data-loss cohort.
- **v0.25.0 reset** — clears the failure-tracking columns for repos that hit the 10-strike sideline under the pre-v0.25.0 strict scanner contract.
- **v0.25.3 repair** — stamps `distribution_last_run = MAX(data_collection_date)` for repos whose v0.25.0/v0.25.1-window scans were thrown away by the 23505 rotation bug fixed in v0.25.1, so the post-v0.25.1 worker doesn't redo their work.

### Why they exist

Each one corresponds to a specific operational incident from the v0.24.0–v0.25.x evolution of the DistributionWorker:

- The DistributionWorker shipped in v0.24.0 had a deps.dev URL-encoding bug and a strict scanner contract that surfaced as silent data loss on Julia/R/conda repos.
- v0.25.0 loosened the contract, added cross-source lock-in, added a per-source circuit breaker, added a `distribution_scan_complete` column, and added immediate-reclaim for partial scans.
- v0.25.1 fixed a downstream history-table UNIQUE constraint bug that v0.25.0's immediate-reclaim exposed as a tight dispatcher loop.
- v0.25.3 added the explicit off-switch for immediate-reclaim (this section) plus the repair migration for the cohort whose work v0.25.1 indirectly rescued.

The knobs and migrations represent operator control over a *transitional* problem. Fleets that started on v0.25.1+ never experienced the underlying bugs and don't need the migrations to fire (the WHERE clauses make them no-ops automatically). Fleets that crossed the transition lean on these knobs to recover gracefully.

### Lifecycle and deprecation horizon

These settings are **explicitly ephemeral**:

- They have no value for new deployments started on v0.25.1 or later. The defaults preserve the intended v0.25.x behavior; operators don't need to set or change them.
- They have transient value for operators who upgraded *through* the v0.24.x → v0.25.x transition. The `_reclaim` knob lets them turn off the urgent-re-collection mechanism once their transition cohort is processed; the migrations heal the residual data state from the bug window.
- They will be **removed when v0.24.x support officially ends (target: 2027)**. By then, no operator should still be running a fleet that was first collected under v0.24.0–v0.25.0, and the only purpose of these knobs and migrations will have been served.

The removal will be staged:

1. **v0.26.x or v0.27.x (when v0.24.x ends mainstream support):** the knobs are marked deprecated in `aveloxis.json` schema validation. Aveloxis logs a WARN at startup if either knob is present in `aveloxis.json`. The defaults stay the same; behavior is unchanged.
2. **Two minor versions later:** the JSON fields are removed from the config struct. Operators with the keys still in their `aveloxis.json` get a fatal "unknown config key" startup error. The reset/repair migrations stay (they're idempotent and cost nothing to keep) but their docs get pruned.
3. **Reset and repair migrations stay indefinitely** as cheap historical scaffolding — they don't fire on healthy data and document the v0.25.x-era recovery story for any operator who finds an extremely old DB.

The intent is operator clarity: when you read `aveloxis.json` and see these keys, you know they're not part of the stable long-term surface. When you stop seeing them in the example config (post-deprecation), they've fully aged out.

### What operators on fresh installs should do

Nothing. Leave both knobs absent from `aveloxis.json` and the defaults handle the rest. The migrations are no-ops on a fresh DB because there are no rows matching the WHERE clauses.

### What operators upgrading through v0.25.x should do

1. Deploy v0.25.1 to fix the rotation bug.
2. Deploy v0.25.3 — the v0.25.3 repair migration runs on next `aveloxis migrate`, stamping `distribution_last_run` for the lost-completion cohort.
3. Watch the worker for a cycle to confirm new partial scans are still re-claimable (the v0.25.0 immediate-reclaim is still on by default).
4. Once the fleet is steady-state and the urgent-re-collection cohort is empty, optionally set `"distribution_tracking_immediate_partial_reclaim": false` in `aveloxis.json` to switch to cadence-only operation. This is the steady-state stable mode.

See `docs/architecture/distribution.md` §12 for the full design rationale.

---

## Next steps

- [Quick Start](quickstart.md) -- get collecting in 5 steps
- [Commands Reference](../guide/commands.md) -- full CLI reference


## Operator vulnerability digest (v0.27.12)

When `mail.operator_email` is set (and the Gmail mailer is configured),
`aveloxis serve` emails a digest of **newly detected, unresolved
vulnerability findings** at or above `mail.vuln_digest_min_severity`.

Semantics worth knowing:

- The scheduler checks hourly, but sends at most one digest per
  `vuln_digest_interval_hours` (default 24). Quiet windows (no new
  findings) produce **no email** — the window still advances.
- The very first digest after enabling covers only one interval back,
  NOT all history — enabling the feature on an established fleet does
  not dump the entire findings table into one email.
- A failed send is retried with the SAME window on the next hourly
  check (nothing is dropped); the window only advances after a
  successful send or a quiet evaluation.
- The email body itemizes up to 50 findings (most severe first) and
  always states the total; the state lives in
  `~/.aveloxis/vuln-digest-last`.
- Findings enter the digest by `first_detected_at`, so a finding is
  reported once when first seen — re-scans of the same vulnerability
  do not re-notify.
