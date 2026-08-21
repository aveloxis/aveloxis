// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package config handles Aveloxis configuration.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func defaultCloneDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/aveloxis-repos"
	}
	return filepath.Join(home, "aveloxis-repos")
}

// Config is the top-level Aveloxis configuration.
type Config struct {
	Database DatabaseConfig `json:"database"`
	GitHub   PlatformConfig `json:"github"`
	GitLab   PlatformConfig `json:"gitlab"`
	Mail     MailConfig     `json:"mail"` // v0.19.0: Gmail-backed transactional mailer

	// Collection controls how repositories are collected.
	Collection CollectionConfig `json:"collection"`

	// Web GUI settings.
	Web WebConfig `json:"web"`

	// v0.23.0: Monitor dashboard settings.
	Monitor MonitorConfig `json:"monitor"`

	// v0.27.0: public analytics API settings (rate limiting + CORS).
	API APIConfig `json:"api"`

	// LogLevel sets the minimum log level: "debug", "info", "warn", or "error".
	LogLevel string `json:"log_level"`
}

// MonitorConfig governs the /monitor dashboard.
//
// v0.23.0 — added so operators can tune the meta-refresh cadence per
// deployment. The pre-v0.23.0 60-second hard-coded value (v0.18.30
// raised from 10s after the per-render scans started hammering the
// DB) is now just the default; large fleets that want even less
// pressure can set refresh_seconds=120 or higher. Phone-pinned
// operator dashboards that want faster feedback can set 30.
type MonitorConfig struct {
	// RefreshSeconds is the HTML meta-refresh interval emitted on
	// the dashboard. Clamped to [10, 3600] at consumption time so a
	// fat-fingered 0 doesn't produce a refresh storm and a fat-
	// fingered 99999 doesn't make the dashboard appear frozen.
	RefreshSeconds int `json:"refresh_seconds"`
}

// MonitorRefreshSecondsOrDefault returns the configured refresh
// cadence, falling back to 60 (the v0.18.30 default) when the
// configured value is zero or out of safe bounds.
func (m MonitorConfig) MonitorRefreshSecondsOrDefault() int {
	if m.RefreshSeconds < 10 || m.RefreshSeconds > 3600 {
		return 60
	}
	return m.RefreshSeconds
}

// DatabaseConfig holds PostgreSQL connection details.
type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	SSLMode  string `json:"sslmode"`
}

// ConnectionString returns a PostgreSQL DSN.
func (d DatabaseConfig) ConnectionString() string {
	sslmode := d.SSLMode
	if sslmode == "" {
		sslmode = "prefer"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.DBName, sslmode)
}

// ConnectionStringWithAppName returns a PostgreSQL DSN with an
// `application_name` parameter set, so pg_stat_activity rows from
// long-running queries can be filtered down to a specific aveloxis
// process (serve / web / api). v0.20.0 introduced this so
// `aveloxis stop` can verify backend disconnection post-SIGTERM.
func (d DatabaseConfig) ConnectionStringWithAppName(name string) string {
	return d.ConnectionString() + "&application_name=" + name
}

// PlatformConfig holds API keys and settings for a forge platform.
type PlatformConfig struct {
	APIKeys []string `json:"api_keys"`
	BaseURL string   `json:"base_url,omitempty"` // override for self-hosted instances

	// GitLabHosts lists additional hostnames that should be recognized as
	// GitLab instances (for self-hosted). Only relevant for GitLab config.
	GitLabHosts []string `json:"gitlab_hosts,omitempty"`
}

// WebConfig configures the web GUI and OAuth.
type WebConfig struct {
	// Addr is the listen address for the web GUI (default ":8082").
	Addr string `json:"addr"`

	// SessionSecret is used to sign session cookies (generate a random string).
	SessionSecret string `json:"session_secret"`

	// BaseURL is the external URL for OAuth callbacks (e.g., "https://aveloxis.example.com").
	BaseURL string `json:"base_url"`

	// DevMode disables the Secure flag on cookies, allowing the web GUI to work
	// over plain HTTP during local development. In production (the default),
	// cookies are always marked Secure so browsers only send them over HTTPS.
	// HttpOnly is always set regardless of this flag.
	DevMode bool `json:"dev_mode"`

	// GitHub OAuth app credentials (from https://github.com/settings/developers).
	GitHubClientID     string `json:"github_client_id"`
	GitHubClientSecret string `json:"github_client_secret"`

	// GitLab OAuth app credentials (from https://gitlab.com/-/profile/applications).
	GitLabClientID     string `json:"gitlab_client_id"`
	GitLabClientSecret string `json:"gitlab_client_secret"`
	GitLabBaseURL      string `json:"gitlab_base_url"` // default "https://gitlab.com"

	// APIInternalURL is where the web server reaches the `aveloxis api` process
	// server-to-server. The web server reverse-proxies /api/* requests to this
	// URL so the browser talks only to the web origin — eliminating the old
	// hardcoded `http://localhost:8383` JS fetch that broke for any
	// non-localhost browser and was further broken by CORS tightening on
	// 2026-04-14. Default assumes the api process runs on the same host as
	// the web process, which matches `aveloxis start all`. Override to point
	// at a remote API instance.
	APIInternalURL string `json:"api_internal_url"`

	// SPAURL is the trusted origin of the separate-repo SPA
	// (aveloxis-gui). When set, OAuth callbacks honor a ?next=
	// parameter pointing there (or any relative path), so signing in
	// from the SPA returns the user TO the SPA instead of the
	// server-rendered /dashboard. Empty (default) = only relative
	// next paths are honored. v0.27.4.
	SPAURL string `json:"spa_url"`

	// AutoApproveAddLimit (v0.27.20 per-add approval, summary/15):
	// when > 0, a non-admin batch of NOT-yet-tracked repo URLs whose
	// size is at or under this limit is added and enqueued
	// immediately (with an auto-approved audit request row) instead
	// of waiting on an admin decision. 0 (the default) means every
	// non-admin addition of new repos requires approval. Org
	// registrations ALWAYS require approval regardless of this knob —
	// an org is an unbounded mass add by definition. Already-tracked
	// repos never need approval (they link instantly for everyone).
	AutoApproveAddLimit int `json:"auto_approve_add_limit"`
}

// AutoApproveAddLimitValue returns the effective auto-approve limit:
// the configured value, with negatives collapsed to 0 (fully gated).
func (w WebConfig) AutoApproveAddLimitValue() int {
	if w.AutoApproveAddLimit < 0 {
		return 0
	}
	return w.AutoApproveAddLimit
}

// CollectionConfig controls collection behavior.
//
// Note (v0.20.18): the former BatchSize / "batch_size" field was
// removed here. It defaulted to 1000 but no production code
// ever read it — operators tuning it saw no behavior change.
// The actual batch sizes in the system are hardcoded constants
// (stagingFlushSize = 500 in internal/db/staging.go, prBatchSize
// = 10 in internal/platform/github/graphql_pr_batch.go, etc.).
// If a tunable knob is genuinely needed later, add it with a
// name that says what it controls (e.g. "staging_flush_size")
// and wire it through to the actual consumer.
type CollectionConfig struct {
	// DaysUntilRecollect is how many days before re-collecting a repo.
	DaysUntilRecollect int `json:"days_until_recollect"`

	// Workers is the number of concurrent collection goroutines.
	Workers int `json:"workers"`

	// RepoCloneDir is the directory where repos are cloned for facade/commit
	// analysis. Can be terabytes for large instances. Defaults to $HOME/aveloxis-repos.
	RepoCloneDir string `json:"repo_clone_dir"`

	// ForceFullCollection when true makes every collection pass a full collection
	// (since=zero) regardless of when the repo was last collected. Use this to
	// re-collect all data after a bug fix (e.g., fixing contributor resolution).
	// Set to false after the full pass completes.
	ForceFullCollection bool `json:"force_full"`

	// MatviewRebuildDay is the day of the week to rebuild materialized views.
	// Valid values: "monday" through "sunday", or "disabled" to never auto-rebuild.
	// Default: "saturday". Views are rebuilt once per week on this day.
	MatviewRebuildDay string `json:"matview_rebuild_day"`

	// MatviewRebuildSkipDMAggregates skips the dm_ aggregate table
	// refresh (RefreshAllRepoAggregates) inside the weekly scheduler
	// rebuild, keeping only the materialized-view step. v0.27.56 —
	// added after the 2026-07-27→30 incident where the dm_ step (a
	// 93K-repo × two-pass per-repo loop) ran 3+ days holding
	// MatviewRebuildActive, silently pausing all collection claims.
	// Deliberately does NOT affect `aveloxis refresh-views` or
	// `aveloxis migrate` — those are explicit operator commands.
	// The FULL weekly-rebuild off-switch is matview_rebuild_day:
	// "disabled".
	MatviewRebuildSkipDMAggregates bool `json:"matview_rebuild_skip_dm_aggregates"`

	// ActivityHistoryWindowDays is the span of each GitHub
	// contributionsCollection window the v0.27.58 daily-history
	// backfill queries (operator decision 2026-07-30: parameterizable,
	// default 180). GitHub validates from/to at max one year, so the
	// accessor clamps at 365; the subdivision-on-cap logic halves
	// windows below this at runtime when a window hits the 100-repo or
	// page caps, so this is the STARTING span, not a guarantee.
	ActivityHistoryWindowDays int `json:"activity_history_window_days"`

	// MatviewRebuildOnStartup controls whether materialized views are created/refreshed
	// during schema migration (startup). For large databases this can take minutes.
	// Default: false — views are created on first migrate but not refreshed on every startup.
	MatviewRebuildOnStartup bool `json:"matview_rebuild_on_startup"`

	// PRChildMode selects between the REST per-PR child waterfall
	// ("rest", default) and the batched GraphQL fetcher ("graphql").
	// When "graphql", the staged collector, open-item refresh, and gap
	// filler all use platform.Client.FetchPRBatch — one query for up
	// to 25 PRs and all their children. GitLab's FetchPRBatch falls
	// back to REST composition because GitLab's GraphQL API is weaker
	// on merge_request fields; parity is preserved at the column level.
	//
	// Default "rest" so existing deployments pick up v0.18.1 without a
	// behavior change until operators explicitly opt in.
	PRChildMode string `json:"pr_child_mode"`

	// ListingMode selects between two separate REST iterators for
	// issues and PRs ("rest", default) and the unified GraphQL
	// listing ("graphql") added in phase 2 of the REST→GraphQL
	// refactor. When "graphql", the staged collector calls
	// platform.Client.ListIssuesAndPRs once per repo instead of
	// iterating ListIssues and ListPullRequests separately. On GitHub
	// this is a pair of paginated GraphQL queries; on GitLab it
	// composes the existing REST iterators (GitLab's GraphQL MR
	// surface is too limited to use directly). Column parity is
	// preserved in both modes.
	//
	// Default "rest" so existing deployments pick up v0.18.2 without
	// a behavior change until operators explicitly opt in.
	ListingMode string `json:"listing_mode"`

	// ThreadingMode selects between single-goroutine PR batch fetching
	// ("single", default — pre-phase-3 behavior) and sharded multi-
	// goroutine fetching ("sharded"). In sharded mode, when the PR
	// count exceeds ShardSize, the enumerated PR list is partitioned
	// and each shard runs in its own goroutine with its own GraphQL
	// batch calls. Added in phase 3 of the REST→GraphQL refactor.
	// Uses ParallelSlots to coordinate with the scheduler's worker
	// pool so the total goroutine count stays within the configured
	// workers budget.
	//
	// Default "single" so v0.18.3 is a no-op for operators who don't
	// opt in. Sharding only activates when threading_mode=sharded
	// AND pr_child_mode=graphql (the REST child path is per-PR
	// sequential and doesn't benefit from shard-level fan-out).
	ThreadingMode string `json:"threading_mode"`

	// ShardSize is the item-count threshold above which sharded mode
	// fans out. Default 3000 per the refactor plan ("1 additional
	// worker per 3000 issues and PRs"). Operators running very large
	// fleets may want a smaller value to exercise sharding on medium
	// repos; the equivalence-test harness overrides it to 500 to
	// trigger sharding on augur (2,623 PRs).
	//
	// Ignored when ThreadingMode != "sharded".
	ShardSize int `json:"shard_size"`

	// IssueChildMode selects between per-issue REST label+assignee
	// fetching (the legacy waterfall, ~2 REST calls per issue) and
	// inline GraphQL delivery ("graphql" — labels and assignees arrive
	// in the issue listing). When "graphql", the staged collector
	// skips ListIssueLabels and ListIssueAssignees per-issue REST
	// iterators; data comes from the IssueLabels / IssueAssignees
	// maps on IssueAndPRBatch. GitLab repos keep running the REST
	// path regardless of mode (GitLab's REST-composition
	// ListIssuesAndPRs leaves the maps nil).
	//
	// Phase 5 of the REST → GraphQL refactor. See
	// summary/06-issue-graphql-children.md for the full plan.
	//
	// Default flipped from "rest" to "graphql" in v0.22.3 after
	// shadow-diff equivalence testing on augur confirmed zero
	// regressions on the phase-5 target tables (issue_labels,
	// issue_assignees, issue_message_ref). REST mode stays
	// available as an escape hatch — same posture as PRChildMode
	// after its v0.19.0 default flip. The known parity gap
	// (issue_labels.platform_label_id stays 0 on GraphQL because
	// GitHub's GraphQL Label type has no databaseId) is documented
	// in CLAUDE.md v0.22.0; the column has no SELECT/JOIN/WHERE
	// consumers anywhere in the codebase.
	IssueChildMode string `json:"issue_child_mode"`

	// EnrichIntervalMinutes controls how often thin-contributor profile
	// enrichment runs as a periodic scheduler task. v0.18.29 moved
	// enrichment off the per-job hot path because every worker calling
	// EnrichThinContributors(14000) after its own repo finished
	// exhausted the GitHub key pool in ~11 minutes on a 120-worker
	// fleet. The periodic task runs once per interval on a single
	// goroutine, well under the rate-limit budget.
	//
	// Default 30 (minutes) when unset. Faster (10) catches up enrichment
	// sooner; slower (60) leaves more REST headroom. With 14K thin
	// contributors and 73 keys, even 60 minutes is comfortably within
	// the 5K/key/hour budget.
	EnrichIntervalMinutes int `json:"enrich_interval_minutes"`

	// SearchResolveIntervalMinutes controls how often the v0.19.2
	// search-resolve background task runs. The task takes
	// contributors with email but no gh_user_id and calls GitHub's
	// search API to backfill the platform identity. GitHub search
	// is rate-limited to 30/min/token (separate budget from the
	// 5000/hour core API), so this runs at a deliberately low
	// cadence. Default 60 (minutes) when unset.
	SearchResolveIntervalMinutes int `json:"search_resolve_interval_minutes"`

	// AffiliationIntervalMinutes controls how often the v0.19.7
	// PopulateAffiliations task runs as a periodic singleton.
	// Pre-v0.19.7 this fired from every worker after every repo
	// completed (Phase 5b in runJob), producing fan-out contention on
	// UNIQUE (ca_domain) and the ShareLock pile-up the operator
	// caught on 2026-05-08. The domain→company map is global state;
	// recomputing it from contributor data once an hour is sufficient
	// because the source data (cntrb_company) is itself bounded by
	// the 30-day enrichment cooldown. Default 60 (minutes) when unset.
	AffiliationIntervalMinutes int `json:"affiliation_interval_minutes"`

	// BreadthIntervalMinutes controls how often the contributor
	// breadth worker ticks. v0.20.17: pre-fix the ticker was
	// hardcoded to 6 hours and batch=100, capping throughput to
	// 400 contributors/day — 9.6 years to cover a 1.4M-contributor
	// fleet once. Combined with BreadthBatchSize=2000 the new
	// 15-minute default targets ~192K contributors/day, putting
	// first-pass coverage of a 1.4M fleet at about 7 days.
	BreadthIntervalMinutes int `json:"breadth_interval_minutes"`

	// BreadthBatchSize is the maximum number of contributors the
	// breadth worker processes per tick. v0.20.17 default 2000.
	// Each contributor takes 1–3 API calls (most users have ≤300
	// recent events fitting in one page) so 2000 × 3 = 6000 API
	// calls/tick is well under the 73-key budget at typical
	// scheduler intervals.
	BreadthBatchSize int `json:"breadth_batch_size"`

	// BreadthCooldownDays is the minimum interval between
	// successive attempts on the same contributor. v0.20.17
	// default 7. Steady-state load with this cooldown over 1.4M
	// contributors is 200K/day = 8K/hour ≈ 2% of the 365K/hr
	// budget for a 73-key fleet.
	BreadthCooldownDays int `json:"breadth_cooldown_days"`

	// BreadthFetchConcurrency is the number of fetcher goroutines the
	// contributor breadth worker runs per cycle (v0.27.8). Pre-v0.27.8
	// the Run loop was strictly sequential — one contributor, one HTTP
	// request in flight — so cycle throughput was bounded by HTTP RTT
	// (~1–2 contributors/sec) regardless of API budget. With N fetchers
	// the cycle scales ~N× until the key pool's rate limits pace it.
	// Default 8 when unset. Raising this trades API-budget burn rate
	// for wall-clock; the 73-key production fleet has headroom for far
	// more, but 8 keeps a single breadth cycle from dominating the
	// shared pool.
	BreadthFetchConcurrency int `json:"breadth_fetch_concurrency"`

	// ShutdownGraceSeconds caps how long Scheduler.Run's ctx-cancel
	// branch waits for in-flight workers to finish before closing the
	// pgx pool. Default 10 (seconds) when unset. Pre-v0.20.0 the wait
	// was unbounded — a 26-minute commits UPDATE blocked shutdown for
	// the full duration. Setting this too low means workers' transactions
	// abort mid-flight (Postgres rolls them back; safe but log-noisy);
	// too high means a slow shutdown. 10 seconds matches the pollInterval.
	ShutdownGraceSeconds int `json:"shutdown_grace_seconds"`

	// v0.21.0 — Scancode is now run by a dedicated ScancodeWorker pool
	// rather than inline in AnalysisCollector.AnalyzeRepo. The 2026-05-14
	// production incident showed that gating scancode behind a 2-slot
	// package-level semaphore at fleet scale parked 177 of 180 collection
	// workers behind the queue for 7+ hours. The decoupled pool runs in
	// its own goroutines, never blocks the main collection cycle, and
	// re-clones repos shallowly on demand so scancode runs are
	// independent of facade/analysis state.
	//
	// ScancodeWorkers is the maximum concurrent scancode invocations.
	// Default 2 when unset — matches pre-v0.21.0 concurrency so
	// operators upgrading don't see a sudden change in scancode CPU
	// load. Operators on machines with spare CPU cores should raise
	// this (operator running aveloxis_large at 64 cores has tested 12
	// without issues).
	//
	// v0.27.6: an EXPLICIT `"scancode_workers": 0` in aveloxis.json
	// disables the scancode pool on `aveloxis serve` entirely — the
	// dedicated-scancode-host recipe (the adjacent machine runs
	// `aveloxis scancode-worker` against the shared DB; see
	// docs/guide/dedicated-scancode-host.md). An ABSENT key keeps the
	// default of 2 (config.Load overlays aveloxis.json onto
	// DefaultConfig, so only a written 0 reaches the scheduler's
	// disable gate). Pre-v0.27.6 an explicit 0 was silently clamped
	// to 2.
	ScancodeWorkers int `json:"scancode_workers"`

	// ScancodeStartIntervalSec is the minimum time between consecutive
	// scancode CLAIM operations. Default 90 seconds when unset.
	// Different from the inter-completion time: the ticker fires every
	// 90s and the dispatcher attempts a new claim. If no slot is free
	// the tick is a no-op; if one is free the claim happens.
	//
	// Pacing reasoning: each scancode start triggers a shallow git
	// clone followed by a scancode invocation. The 90s gate prevents
	// new clones from bursting the network/disk when the operator
	// wakes a freshly-restarted aveloxis serve with hundreds of
	// eligible repos.
	ScancodeStartIntervalSec int `json:"scancode_start_interval_s"`

	// ScancodeCadenceDays is the minimum interval between successive
	// scancode runs on the same repo. Default 180 days (6 months) when
	// unset. Pre-v0.21.0 was 30 days; the change reflects that
	// per-file license + copyright headers in source files change
	// rarely on the timescale we care about, and the I/O cost of
	// scanning a Linux-kernel-scale mirror doesn't justify monthly
	// re-scans.
	//
	// Dependency-level licenses (which DO change as packages are
	// updated) still flow through the per-cycle Phase 4 dependency
	// scan + Phase 6 SBOM generation; scancode is specifically the
	// per-source-file scan and benefits from a longer cadence.
	ScancodeCadenceDays int `json:"scancode_cadence_days"`

	// ScancodeCloneDir is the parent directory for the per-run
	// shallow clones the ScancodeWorker creates. Each scan creates
	// <ScancodeCloneDir>/repo_<id>_<unix_ts> and removes it on
	// completion (success or failure). Default
	// "/tmp/aveloxis-scancode" when unset.
	//
	// Disk-space budget: each clone is the working tree only
	// (`git clone --depth 1`), so size ≈ checked-out repo size. With
	// the default 2 workers and average ~50 MB clones, ~100 MB peak.
	// On a fleet with 12 workers and Linux-kernel-scale repos, peak
	// can reach several GB; size /tmp accordingly or set
	// scancode_clone_dir to a dedicated mount.
	ScancodeCloneDir string `json:"scancode_clone_dir"`

	// ScancodeShutdownGraceMinutes caps how long the ScancodeWorker
	// waits for in-flight scans to finish on aveloxis stop. Default
	// 0 (immediate kill) as of v0.23.7.
	//
	// Why 0 by default: a scancode subprocess that outlives aveloxis
	// can't deliver its output back — the JSON file is read by Go
	// code inside aveloxis. A scan finishing after `aveloxis stop`
	// produces a file no one will ever ingest. The v0.21.0
	// recoverOrphans path on next startup notices the orphaned lock
	// row, attempts to ingest from disk if a usable file exists,
	// otherwise clears the lock and the row goes back to the active
	// queue. Either way, lingering past stop buys nothing — it just
	// delays shutdown AND increases ghost-process risk.
	//
	// Operators who genuinely want the old behavior (let in-flight
	// scans finish if they're close) set this explicitly to a
	// positive minute count. Within the grace window: runners
	// complete their scans naturally and ingest results. At grace
	// expiry: the worker's ctx.Done() fires cmd.Cancel which kills
	// the process group.
	//
	// Separate from collection.shutdown_grace_seconds (which paces
	// the main scheduler's stop).
	ScancodeShutdownGraceMinutes int `json:"scancode_shutdown_grace_minutes"`

	// ScancodeRunTimeoutHours is the BASE wall-clock timeout for a
	// single scancode subprocess invocation. Default 2 hours
	// (matches the pre-v0.23.8 hardcoded constant).
	//
	// v0.23.8 also adds per-repo adaptive scaling: when a scan exits
	// with `signal: killed` (the cmd.Cancel signature when scanCtx
	// times out), the row's scancode_timeout_attempts counter
	// increments, and the next attempt uses
	// `min(base * 2^attempts, cap_hours)`. Kernel-class repos
	// (~80K files, ~3h scan minimum) discover their natural runtime
	// over a few cycles without operator intervention.
	//
	// Operators with a fleet skewed toward big repos can raise this
	// directly (e.g., 8 hours) instead of waiting for adaptive
	// scaling to converge.
	ScancodeRunTimeoutHours int `json:"scancode_run_timeout_hours"`

	// ScancodeRunTimeoutCapHours bounds the adaptive timeout's
	// upper limit. Default 24 hours. Even kernel-class repos
	// shouldn't need a single scan slot for more than a day; a row
	// that genuinely takes longer than this is more likely broken
	// than legitimately big.
	//
	// Together with the v0.21.4 ScancodeMaxFailures (10-strike
	// sideline on the SEPARATE scancode_failed_attempts counter),
	// the cap ensures no single repo monopolizes worker capacity
	// indefinitely. Note that timeout-class failures do NOT
	// increment scancode_failed_attempts — kernel-class repos
	// hitting the cap repeatedly stay in the active queue, they
	// just won't get any bigger timeout.
	ScancodeRunTimeoutCapHours int `json:"scancode_run_timeout_cap_hours"`

	// ScancodeMaxInMemory caps how many file scan results scancode
	// keeps in RAM before spilling intermediate state to a tempfile
	// on disk. Default 5000 — matches the pre-v0.25.2 hardcoded
	// constant so legacy aveloxis.json files keep working unchanged.
	//
	// The default is conservative for low-memory dev machines. On
	// production hosts with hundreds of GB of RAM, raising this
	// reduces tempfile I/O on monorepos (linux kernel, chromium,
	// etc.) where the default forces spill early in the scan.
	// scancode passes the value through to its
	// `--max-in-memory N` flag verbatim. Set 0 or negative to
	// fall back to the default; the accessor clamps so a bogus
	// value can't reach the scancode subprocess.
	ScancodeMaxInMemory int `json:"scancode_max_in_memory"`

	// VulnScanTransitive (v0.27.21 Phase C1, summary/14): scan the
	// FULL transitive closure each committed lockfile enumerates, not
	// just declared direct dependencies — and (v0.27.133 C2) store
	// the dependency EDGES that power introduced_by attribution and
	// the SBOM graphs. DEFAULT FLIPPED TO TRUE in v0.27.136 on the
	// 2026-08-21 canary evidence (operator decision): +242 transitive
	// findings on the 22-repo canary AND -81 range-floor false
	// positives on junit5 alone (lockfile-locked resolution deflates
	// advisory-history matches), 93.5% chain-attribution floor,
	// +45% scan targets well inside the C0 cache's dedup envelope.
	// Pointer-to-bool (the v0.25.0 cross_check_sources pattern) so
	// the decoder distinguishes "absent" (use the default = true)
	// from an explicit false — the escape hatch for fleets that want
	// declared-direct-only scanning. Read via
	// VulnScanTransitiveValue(), never the raw field. Transitive
	// findings carry dependency_kind='transitive' and are excluded
	// from the operator digest unless
	// mail.vuln_digest_include_transitive is set.
	VulnScanTransitive *bool `json:"vuln_scan_transitive,omitempty"`

	// DevBuildDeps (v0.27.45, summary/19 P2): expand Python dependency
	// collection to the dev/test/build/optional manifest families —
	// requirements-variant files (requirements-dev.txt,
	// test_requirements.txt, requirements/*.txt), pyproject
	// [build-system].requires / [project.optional-dependencies] /
	// PEP 735 [dependency-groups] / poetry groups, Pipfile
	// [dev-packages], setup.py tests_require + extras_require, and
	// setup.cfg [options.extras_require]. Default false (opt-in): this
	// is the findings-volume driver — Python dev tooling is
	// stale-pin-dense, so vulnerability counts jump when it flips on.
	// Canary on a small database before fleet enablement (the
	// v0.27.19 npm/cargo first-wave lesson). The Go test-only relabel
	// and the P1 scope relabels are NOT behind this knob (they add no
	// rows).
	DevBuildDeps bool `json:"dev_build_deps"`

	// GitHubActionsDeps (v0.27.47, summary/19 P4): inventory workflow
	// `uses:` action references as build-scope dependencies
	// (manager='githubactions', libyear NULL) and scan them against
	// OSV's "GitHub Actions" ecosystem (versionless query +
	// client-side ref evaluation — OSV's Actions advisories carry no
	// purl and no version comparator). Its own knob rather than
	// riding dev_build_deps: a new ecosystem, not a scope refinement.
	// Default false until the small-DB canary sizes the wave.
	GitHubActionsDeps bool `json:"github_actions_deps"`

	// SkipLargestPercent (v0.27.35): TEMPORARY throughput lever —
	// exclude the fleet's top-N% repos (by forge-reported commit count
	// OR PR count, either qualifies) from collection claims so a
	// handful of kernel/pytorch-class monsters can't occupy every
	// worker slot and hold back the rest of the fleet. 0 (default) =
	// disabled. Skipped repos stay 'queued', overdue, and untouched —
	// remove the knob (and restart serve) and they collect normally.
	// NOTE: this is an absolute gate while enabled: a monitor "Boost"
	// cannot override it for a skipped repo.
	SkipLargestPercent float64 `json:"skip_largest_percent"`

	// ScorecardTimeoutMinutes is the per-ATTEMPT wall-clock cap for a
	// single OpenSSF Scorecard invocation (v0.27.5). Default 15
	// minutes when unset. The remote-primary phase applies it to the
	// remote attempt AND, when a fallback fires, again to the local
	// attempt — so a repo can hold a scorecard slot for at most
	// 2×timeout. Motivated by the production multi-DAY hang class:
	// remote scorecard with a single shared token sleeps through
	// rate-limit resets; the timeout converts "hung for days" into
	// "fell back to local mode after 15 minutes" (11 checks beat
	// none).
	ScorecardTimeoutMinutes int `json:"scorecard_timeout_minutes"`

	// ScorecardTokenCount caps how many key-pool tokens are handed to
	// scorecard as a comma-separated GITHUB_TOKEN (scorecard
	// round-robins the list per request). Default 0 = ALL non-invalid
	// pool tokens; N>0 = the first N. Multi-token is what makes remote
	// mode viable at fleet scale — a single token shared with 40
	// collection workers is the measured cause of the multi-day remote
	// hangs (scorecard sleeps until the token's reset). Negative
	// values collapse to 0 (all tokens).
	ScorecardTokenCount int `json:"scorecard_token_count"`
	// ScancodeTimeoutCapStrikes (v0.27.6) is the number of CONSECUTIVE
	// wall-clock timeouts AT the adaptive-timeout cap
	// (scancode_run_timeout_cap_hours) after which the repo is
	// sidelined exactly like the v0.21.4 10-strike failure path
	// (scancode_last_run is stamped so the cadence gate excludes it
	// for the full cadence window). Default 3 when unset or
	// non-positive.
	//
	// Why this exists: the v0.23.8 adaptive-timeout design
	// deliberately never sidelined timeout-class failures — a repo
	// that needs more time is big, not broken. That held until the
	// June 2026 production logs showed pytorch/docs and
	// WHO/smart-html claimed 27× EACH: stretched timeout reaches the
	// 24h cap → "signal: killed" → RecordScancodeTimeout (grows the
	// next timeout, which is already capped) → re-claimed → repeat
	// forever. Once a repo times out at the cap, no bigger timeout is
	// coming; N consecutive at-cap timeouts is proof the repo cannot
	// be scanned within the operator's budget. The diagnostic trail
	// stays in scancode_timeout_attempts (never reset by the
	// sideline).
	ScancodeTimeoutCapStrikes int `json:"scancode_timeout_cap_strikes"`

	// ScancodeIgnoreGlobs (v0.27.6) is an optional list of path globs
	// passed to the scancode subprocess as repeated `--ignore <glob>`
	// flags. Empty by default (no flags added — identical behavior to
	// pre-v0.27.6). Operators use it to exclude generated or vendored
	// trees fleet-wide (e.g. "*.min.js", "*/node_modules/*",
	// "*/docs/_build/*") without waiting for the per-repo
	// generated-content skip policy to trigger.
	ScancodeIgnoreGlobs []string `json:"scancode_ignore_globs"`

	// PhaseWatchdogMinutes controls the v0.22.4 observation watchdog's
	// stall threshold. If staging row count for a repo does not grow
	// for this many minutes, the watchdog appends an event to
	// ~/.aveloxis/aveloxis-long-jobs.log plus a goroutine dump under
	// ~/.aveloxis/long-jobs/. Default 75 minutes — chosen so that
	// large first-cycle collections (microsoft/vscode-class) are
	// observed for prevalence but never killed. The watchdog NEVER
	// cancels the job or requeues the repo. Operators tune up to
	// reduce noise on very-slow-but-legitimate workloads, or down
	// (e.g. 30) to spot smaller hangs during incident triage.
	PhaseWatchdogMinutes int `json:"phase_watchdog_minutes"`

	// StagingRetentionHours is how long processed staging rows are kept
	// before the hourly PurgeStagedProcessed sweep deletes them. Default
	// 1 hour when unset. v0.22.4: pre-v0.22.4 the window was hardcoded
	// to 7 days, which stacked 3–5 cycles of processed JSONB tombstones
	// per frequently-re-collected repo and wasted multiple GB of disk
	// across the fleet. Operators who need forensic retention (e.g. for
	// shadow-diff debugging) can raise this to a value of their choice;
	// 24 (one day) is a reasonable middle ground.
	StagingRetentionHours int `json:"staging_retention_hours"`

	// v0.24.0 — DistributionWorker (package-distribution evidence).
	//
	// A periodic worker pool that examines each repo against deps.dev,
	// ecosyste.ms, GitHub Packages, GitHub release assets, and the
	// GitHub Contents API to determine whether/where the repo is
	// distributed via package managers. Captures intent signals
	// (manifests in the repo) alongside registry-side facts so
	// operators can answer "which repos declare packaging intent but
	// were never published?".
	//
	// OFF BY DEFAULT. Operators opt in by setting
	// distribution_tracking_enabled = true and restarting serve.
	DistributionTrackingEnabled bool `json:"distribution_tracking_enabled"`

	// DistributionTrackingIntervalDays is the per-repo cadence between
	// successive scans. Default 180 (6 months) when unset. Package-
	// distribution mappings are stable on this timescale; re-scanning
	// more frequently buys little signal at the cost of registry API
	// load.
	DistributionTrackingIntervalDays int `json:"distribution_tracking_interval_days"`

	// DistributionTrackingWorkers is the number of concurrent runners
	// fetching against the external HTTP services. Default 4 when
	// unset. Each runner performs a sequence of ~5 cheap HTTP calls per
	// claimed repo; concurrency is bounded primarily to keep total
	// outbound traffic predictable, not because any individual call is
	// expensive.
	DistributionTrackingWorkers int `json:"distribution_tracking_workers"`

	// DistributionTrackingStartIntervalSec is the minimum time between
	// consecutive CLAIM operations. Default 30 seconds when unset. With
	// the default 4 workers and 30s ticker, steady-state throughput is
	// ~120 repos/hour — comfortably under any known external rate
	// limit and well below the GitHub key pool's budget.
	DistributionTrackingStartIntervalSec int `json:"distribution_tracking_start_interval_s"`

	// DistributionTrackingPoliteEmail is the value passed in the
	// `From:` HTTP request header to ecosyste.ms so the operator's
	// traffic lands in their "polite pool" priority queue. Optional;
	// missing values fall back to the lower-priority "common pool".
	// ecosyste.ms documents the polite-pool contract at
	// https://ecosyste.ms/.
	DistributionTrackingPoliteEmail string `json:"distribution_tracking_polite_email"`

	// DistributionTrackingUserAgent overrides the User-Agent header
	// sent to deps.dev / ecosyste.ms / GitHub. When empty the client
	// uses `aveloxis/<tool_version>`. Operators behind shared egress
	// IPs may want a more identifying string so registry operators
	// can route diagnostics to the right person.
	DistributionTrackingUserAgent string `json:"distribution_tracking_user_agent"`

	// DistributionTrackingCrossCheckSources, when true (the default
	// in v0.25.0), guarantees BOTH deps.dev AND ecosyste.ms are
	// queried for every repo even when one returns non-empty data.
	// Each source persists its own rows into repo_distribution; the
	// UNIQUE constraint includes the source column so two-source
	// rows for the same package coexist.
	//
	// Rationale (operator direction, 2026-05-23): operators want
	// explicit lock-in that we don't optimize one external registry
	// away. The trade-off is roughly 2× external-registry API calls
	// per scan; at 180-day cadence the absolute budget is tiny
	// (~5K calls/hour fleet-wide on a 100K-repo cohort), and the
	// signal value of cross-checking — being able to see when
	// deps.dev and ecosyste.ms disagree about which packages are
	// indexed for a repo — is worth it.
	//
	// Set to false only when an operator needs to halve registry
	// traffic at the cost of single-source-of-truth dependence.
	// Pointer-to-bool (not bare bool) so the JSON decoder can
	// distinguish "absent" (use v0.25.0 default = true) from
	// "explicitly false." A bare bool would default to false on
	// missing-key, silently breaking the lock-in guarantee for
	// every aveloxis.json that pre-dates v0.25.0.
	DistributionTrackingCrossCheckSources *bool `json:"distribution_tracking_cross_check_sources,omitempty"`

	// DistributionTrackingImmediatePartialReclaim, when true (the
	// default in v0.25.3), keeps the v0.25.0 behavior: a repo whose
	// last scan was partial (distribution_scan_complete = FALSE) is
	// immediately re-eligible on the next dispatcher cycle,
	// bypassing the cadence gate. The ClaimNextDistributionRepo
	// WHERE clause includes `OR COALESCE(scan_complete, TRUE) = FALSE`.
	//
	// Set to false to suppress that re-claim behavior. Partial-scan
	// repos then wait for normal cadence like everything else — the
	// claim WHERE drops the scan_complete branch. The ORDER BY's
	// `scan_complete ASC` tiebreaker stays in both modes, so among
	// cadence-elapsed rows, partial scans still get priority.
	//
	// Operator framing (2026-05-24): the v0.25.0 immediate-reclaim
	// design is correct *during* a v0.24.x → v0.25.x transition
	// when partial-scan repos legitimately need urgent re-collection.
	// Once a fleet is through that cohort and steady-state cadence
	// resumes, the immediate-reclaim mechanism becomes operational
	// churn rather than a recovery tool. This knob is the explicit
	// off-switch.
	//
	// v0.25.x-era escape hatch. See docs/architecture/distribution.md
	// §12 for the planned deprecation horizon when v0.24.x support
	// ends in 2027.
	//
	// Pointer-to-bool (not bare bool) so the JSON decoder distinguishes
	// "absent" (use v0.25.3 default = true, preserving v0.25.0
	// behavior on aveloxis.json files that pre-date v0.25.3) from
	// "explicitly false." Same pattern as DistributionTrackingCrossCheckSources.
	DistributionTrackingImmediatePartialReclaim *bool `json:"distribution_tracking_immediate_partial_reclaim,omitempty"`

	// v0.25.7 — MailingListWorker (Apache Pony Mail + lore public-inbox).
	// Off by default. Collects dev@/users@ discussion + governance into
	// email_message + messages; jira@/commits@ are mirror-aware.
	MailingListEnabled        bool   `json:"mailing_list_enabled"`
	MailingListWorkers        int    `json:"mailing_list_workers"`                   // concurrent list runners (default 2)
	MailingListCadenceDays    int    `json:"mailing_list_cadence_days"`              // tail-refresh cadence (default 30)
	MailingListBackfillMonths *int   `json:"mailing_list_backfill_months,omitempty"` // history window when a list has no checkpoint (absent → 6; explicit 0 or negative → full history from the list's first month)
	MailingListPoliteEmail    string `json:"mailing_list_polite_email"`              // contact in the User-Agent so archive admins can reach us
	MailingListMirrorHandling string `json:"mailing_list_mirror_handling"`           // skip | metadata_only (default) | full
	// MailingListProcessorWorkers is how many drain goroutines per system pull
	// staged messages through the resolve+write half (summary/12 §11). Default
	// 1 — draining is single-threaded PER LIST; >1 only fans out across
	// DISTINCT lists (an in-process per-list guard keeps two goroutines off the
	// same list). Keep at 1 unless a deep per-list backlog needs cross-list
	// parallelism.
	MailingListProcessorWorkers int `json:"mailing_list_processor_workers"` // drain goroutines per system (default 1)
}

// MailingListProcessorWorkersOrDefault falls back to 1 drain goroutine per
// system (single-threaded per list).
func (c *CollectionConfig) MailingListProcessorWorkersOrDefault() int {
	if c.MailingListProcessorWorkers <= 0 {
		return 1
	}
	return c.MailingListProcessorWorkers
}

// MailingListCadenceDuration returns the per-list tail-refresh cadence.
// Falls back to 30 days when unset.
func (c *CollectionConfig) MailingListCadenceDuration() time.Duration {
	if c.MailingListCadenceDays <= 0 {
		return 30 * 24 * time.Hour
	}
	return time.Duration(c.MailingListCadenceDays) * 24 * time.Hour
}

// MailingListWorkersOrDefault falls back to 2 concurrent runners.
func (c *CollectionConfig) MailingListWorkersOrDefault() int {
	if c.MailingListWorkers <= 0 {
		return 2
	}
	return c.MailingListWorkers
}

// MailingListBackfillMonthsOrDefault returns the history window for a list
// with no checkpoint. A nil field (absent from aveloxis.json) → the bounded
// default of 6 months. An explicit value is passed through unchanged,
// INCLUDING 0 or negative, which the worker interprets as "full history from
// the list's first month". Coercing <= 0 to 6 (the pre-v0.25.12 bug) made
// full-history mode unreachable, so lists only collected the recent ~6 months.
func (c *CollectionConfig) MailingListBackfillMonthsOrDefault() int {
	if c.MailingListBackfillMonths == nil {
		return 6
	}
	return *c.MailingListBackfillMonths
}

// MailingListMirrorHandlingOrDefault falls back to "metadata_only".
func (c *CollectionConfig) MailingListMirrorHandlingOrDefault() string {
	switch c.MailingListMirrorHandling {
	case "skip", "metadata_only", "full":
		return c.MailingListMirrorHandling
	default:
		return "metadata_only"
	}
}

// PhaseWatchdogDuration returns the v0.22.4 long-jobs watchdog
// threshold. Falls back to 75 minutes when unset.
func (c *CollectionConfig) PhaseWatchdogDuration() time.Duration {
	if c.PhaseWatchdogMinutes <= 0 {
		return 75 * time.Minute
	}
	return time.Duration(c.PhaseWatchdogMinutes) * time.Minute
}

// StagingRetentionDuration converts StagingRetentionHours to a
// time.Duration. Falls back to 1 hour when unset. v0.22.4 default.
func (c *CollectionConfig) StagingRetentionDuration() time.Duration {
	if c.StagingRetentionHours <= 0 {
		return time.Hour
	}
	return time.Duration(c.StagingRetentionHours) * time.Hour
}

// EnrichIntervalDuration converts EnrichIntervalMinutes to a time.Duration.
// Falls back to 30 minutes when unset (zero) so existing aveloxis.json
// files without the new key keep the documented default.
// RecollectAfterDuration converts days_until_recollect to a duration.
// Defaults to 24h when unset/non-positive — the single place this
// default lives (v0.25.37; the scheduler's mirror fallback is gone).
func (c *CollectionConfig) RecollectAfterDuration() time.Duration {
	if c.DaysUntilRecollect <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(c.DaysUntilRecollect) * 24 * time.Hour
}

func (c *CollectionConfig) EnrichIntervalDuration() time.Duration {
	if c.EnrichIntervalMinutes <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.EnrichIntervalMinutes) * time.Minute
}

// SearchResolveIntervalDuration converts SearchResolveIntervalMinutes to
// a time.Duration. Falls back to 60 minutes when unset.
func (c *CollectionConfig) SearchResolveIntervalDuration() time.Duration {
	if c.SearchResolveIntervalMinutes <= 0 {
		return 60 * time.Minute
	}
	return time.Duration(c.SearchResolveIntervalMinutes) * time.Minute
}

// AffiliationIntervalDuration converts AffiliationIntervalMinutes to
// a time.Duration. Falls back to 60 minutes when unset.
func (c *CollectionConfig) AffiliationIntervalDuration() time.Duration {
	if c.AffiliationIntervalMinutes <= 0 {
		return 60 * time.Minute
	}
	return time.Duration(c.AffiliationIntervalMinutes) * time.Minute
}

// ShutdownGraceDuration converts ShutdownGraceSeconds to a
// time.Duration. Falls back to 10 seconds when unset.
func (c *CollectionConfig) ShutdownGraceDuration() time.Duration {
	if c.ShutdownGraceSeconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(c.ShutdownGraceSeconds) * time.Second
}

// BreadthIntervalDuration converts BreadthIntervalMinutes to a
// time.Duration. Falls back to 15 minutes when unset — see field
// comment for the throughput math behind that default.
func (c *CollectionConfig) BreadthIntervalDuration() time.Duration {
	if c.BreadthIntervalMinutes <= 0 {
		return 15 * time.Minute
	}
	return time.Duration(c.BreadthIntervalMinutes) * time.Minute
}

// BreadthBatchSizeOrDefault returns BreadthBatchSize or 2000 when
// unset.
func (c *CollectionConfig) BreadthBatchSizeOrDefault() int {
	if c.BreadthBatchSize <= 0 {
		return 2000
	}
	return c.BreadthBatchSize
}

// BreadthCooldownDuration converts BreadthCooldownDays to a
// time.Duration. Falls back to 7 days when unset.
func (c *CollectionConfig) BreadthCooldownDuration() time.Duration {
	if c.BreadthCooldownDays <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(c.BreadthCooldownDays) * 24 * time.Hour
}

// BreadthFetchConcurrencyOrDefault returns BreadthFetchConcurrency or
// 8 when unset/invalid — the v0.27.8 fetcher-pool size for the
// contributor breadth worker.
func (c *CollectionConfig) BreadthFetchConcurrencyOrDefault() int {
	if c.BreadthFetchConcurrency <= 0 {
		return 8
	}
	return c.BreadthFetchConcurrency
}

// DistributionTrackingInterval converts DistributionTrackingIntervalDays
// to a time.Duration. Falls back to 180 days (6 months) when unset —
// the v0.24.0 design default.
func (c *CollectionConfig) DistributionTrackingInterval() time.Duration {
	if c.DistributionTrackingIntervalDays <= 0 {
		return 180 * 24 * time.Hour
	}
	return time.Duration(c.DistributionTrackingIntervalDays) * 24 * time.Hour
}

// DistributionTrackingWorkersOrDefault returns
// DistributionTrackingWorkers or 4 when unset.
func (c *CollectionConfig) DistributionTrackingWorkersOrDefault() int {
	if c.DistributionTrackingWorkers <= 0 {
		return 4
	}
	return c.DistributionTrackingWorkers
}

// DistributionTrackingStartInterval converts
// DistributionTrackingStartIntervalSec to a time.Duration. Falls back
// to 30 seconds when unset.
func (c *CollectionConfig) DistributionTrackingStartInterval() time.Duration {
	if c.DistributionTrackingStartIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.DistributionTrackingStartIntervalSec) * time.Second
}

// DistributionTrackingCrossCheckSourcesValue returns the effective
// cross-check setting, defaulting to true when the JSON field is
// absent. v0.25.0 default. See the field doc on
// DistributionTrackingCrossCheckSources for the rationale.
// VulnScanTransitiveValue returns the effective transitive-scan
// setting. nil (absent from aveloxis.json) means the v0.27.136
// default: TRUE. An explicit false is the opt-out escape hatch.
// This accessor is the SINGLE default layer (SR-10) — consumers must
// never read the raw pointer.
func (c *CollectionConfig) VulnScanTransitiveValue() bool {
	if c.VulnScanTransitive == nil {
		return true
	}
	return *c.VulnScanTransitive
}

func (c *CollectionConfig) DistributionTrackingCrossCheckSourcesValue() bool {
	if c.DistributionTrackingCrossCheckSources == nil {
		return true
	}
	return *c.DistributionTrackingCrossCheckSources
}

// DistributionTrackingImmediatePartialReclaimValue returns the
// effective immediate-reclaim setting (v0.25.3). Default true
// preserves v0.25.0/v0.25.1 behavior when the JSON field is
// absent — pointer-to-bool so we can tell "absent" from
// "explicitly false." See the field comment on
// CollectionConfig.DistributionTrackingImmediatePartialReclaim
// for the operator-framing rationale.
func (c *CollectionConfig) DistributionTrackingImmediatePartialReclaimValue() bool {
	if c.DistributionTrackingImmediatePartialReclaim == nil {
		return true
	}
	return *c.DistributionTrackingImmediatePartialReclaim
}

// defaultScancodeCloneDir returns the default scancode clone parent
// directory. Kept as a function (not a constant) so unit tests can
// adjust on OSes where /tmp is unconventional. Mirrors the pattern in
// defaultCloneDir for the main collection clone directory.
func defaultScancodeCloneDir() string {
	return "/tmp/aveloxis-scancode"
}

// ScancodeWorkersOrDefault returns ScancodeWorkers or 2 when unset
// (v0.21.0). Default 2 matches pre-v0.21.0 hardcoded concurrency. See
// CollectionConfig.ScancodeWorkers for the tuning rationale.
func (c *CollectionConfig) ScancodeWorkersOrDefault() int {
	if c.ScancodeWorkers <= 0 {
		return 2
	}
	return c.ScancodeWorkers
}

// ScancodeStartInterval converts ScancodeStartIntervalSec to a
// time.Duration. Falls back to 90 seconds when unset.
func (c *CollectionConfig) ScancodeStartInterval() time.Duration {
	if c.ScancodeStartIntervalSec <= 0 {
		return 90 * time.Second
	}
	return time.Duration(c.ScancodeStartIntervalSec) * time.Second
}

// ScancodeCadence converts ScancodeCadenceDays to a time.Duration.
// Falls back to 180 days (6 months) when unset.
func (c *CollectionConfig) ScancodeCadence() time.Duration {
	if c.ScancodeCadenceDays <= 0 {
		return 180 * 24 * time.Hour
	}
	return time.Duration(c.ScancodeCadenceDays) * 24 * time.Hour
}

// ScancodeCloneDirOrDefault returns ScancodeCloneDir or the default
// "/tmp/aveloxis-scancode" when unset.
func (c *CollectionConfig) ScancodeCloneDirOrDefault() string {
	if c.ScancodeCloneDir == "" {
		return defaultScancodeCloneDir()
	}
	return c.ScancodeCloneDir
}

// ScancodeShutdownGrace converts ScancodeShutdownGraceMinutes to a
// time.Duration.
//
// v0.23.7: zero (the default) maps to zero — immediate kill on stop.
// Pre-v0.23.7 the accessor returned 30 min on a zero input. The
// behavior change was driven by the operator observation that
// subprocesses surviving `aveloxis stop` can't deliver their output
// anyway. See the field docstring above for the full rationale.
//
// Operators who set a positive value explicitly in aveloxis.json
// keep getting the old "let in-flight scans finish" behavior.
func (c *CollectionConfig) ScancodeShutdownGrace() time.Duration {
	if c.ScancodeShutdownGraceMinutes <= 0 {
		return 0
	}
	return time.Duration(c.ScancodeShutdownGraceMinutes) * time.Minute
}

// ScancodeRunTimeout returns the BASE wall-clock timeout for a
// single scancode subprocess. v0.23.8. Defaults to 2 hours when
// unset (matching the pre-v0.23.8 hardcoded constant). The
// effective per-job timeout is computed by runOne as
// `min(base * 2^attempts, cap)` where attempts is the row's
// scancode_timeout_attempts counter.
func (c *CollectionConfig) ScancodeRunTimeout() time.Duration {
	if c.ScancodeRunTimeoutHours <= 0 {
		return 2 * time.Hour
	}
	return time.Duration(c.ScancodeRunTimeoutHours) * time.Hour
}

// ScancodeRunTimeoutCap returns the upper bound on the per-job
// adaptive scancode timeout. v0.23.8. Defaults to 24 hours.
func (c *CollectionConfig) ScancodeRunTimeoutCap() time.Duration {
	if c.ScancodeRunTimeoutCapHours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(c.ScancodeRunTimeoutCapHours) * time.Hour
}

// ScancodeMaxInMemoryOrDefault returns the scancode --max-in-memory
// cap (v0.25.2). Defaults to 5000, matching the pre-v0.25.2
// hardcoded constant so legacy configs are unaffected. Negative or
// zero inputs collapse to the default — the value flows directly to
// a scancode CLI argument so a bogus number must never reach the
// subprocess.
func (c *CollectionConfig) ScancodeMaxInMemoryOrDefault() int {
	if c.ScancodeMaxInMemory <= 0 {
		return 5000
	}
	return c.ScancodeMaxInMemory
}

// ScorecardTimeout returns the per-attempt wall-clock cap for a
// single scorecard invocation (v0.27.5). Defaults to 15 minutes when
// unset or non-positive. Applied per ATTEMPT: the remote attempt gets
// the full window, and the local fallback (when one fires) gets a
// fresh window of its own.
func (c *CollectionConfig) ScorecardTimeout() time.Duration {
	if c.ScorecardTimeoutMinutes <= 0 {
		return 15 * time.Minute
	}
	return time.Duration(c.ScorecardTimeoutMinutes) * time.Minute
}

// ScorecardTokenCountOrDefault returns how many pool tokens to hand
// scorecard (v0.27.5). 0 means "all tokens" — the default. Negative
// inputs collapse to 0 so a bogus config value can't panic a slice
// bound downstream.
func (c *CollectionConfig) ScorecardTokenCountOrDefault() int {
	if c.ScorecardTokenCount < 0 {
		return 0
	}
	return c.ScorecardTokenCount
}

// ScancodeTimeoutCapStrikesOrDefault returns the consecutive at-cap
// timeout count after which a repo is sidelined (v0.27.6). Defaults
// to 3 when unset or non-positive — there is deliberately no "0 =
// disabled" escape hatch, because an at-cap timeout can never be
// cured by another attempt (the timeout is already at its maximum)
// and unlimited retries are exactly the pytorch/docs 27-claim spin
// loop this knob exists to end.
func (c *CollectionConfig) ScancodeTimeoutCapStrikesOrDefault() int {
	if c.ScancodeTimeoutCapStrikes <= 0 {
		return 3
	}
	return c.ScancodeTimeoutCapStrikes
}

// ScancodeIgnoreGlobsOrDefault returns the operator's scancode
// --ignore globs, normalizing "absent" and "empty list" to nil so
// the two JSON spellings produce identical effective behavior
// (no --ignore flags at all). v0.27.6.
func (c *CollectionConfig) ScancodeIgnoreGlobsOrDefault() []string {
	if len(c.ScancodeIgnoreGlobs) == 0 {
		return nil
	}
	return c.ScancodeIgnoreGlobs
}

// MailConfig configures the Gmail-backed transactional mailer
// (v0.19.0). When GmailUser is empty the mailer is a no-op — the rest
// of the application works without email enabled.
//
// Setup:
//  1. Enable 2-Step Verification on the Gmail account
//  2. Generate an App Password (https://myaccount.google.com/apppasswords)
//  3. Paste the 16-character password into GmailAppPassword
//
// SiteURL goes into outbound email bodies as the link target. Set it
// to the public-facing URL operators land on (e.g.
// https://your-host.example).
type MailConfig struct {
	GmailUser        string `json:"gmail_user"`
	GmailAppPassword string `json:"gmail_app_password"`
	FromName         string `json:"from_name"`
	SiteURL          string `json:"site_url"`

	// OperatorEmail (v0.27.12) is where fleet-level operator
	// notifications go — currently the new-vulnerabilities digest.
	// Empty (the default) disables operator notifications entirely;
	// the digest ticker never starts.
	OperatorEmail string `json:"operator_email"`

	// VulnDigestIncludeTransitive (v0.27.21): include
	// dependency_kind='transitive' findings in the operator digest.
	// Default false — the first transitive-enabled cycles would
	// otherwise blast a 50-item email of utility-package findings.
	VulnDigestIncludeTransitive bool `json:"vuln_digest_include_transitive"`

	// VulnDigestIncludeDev (v0.27.46, summary/19 P3 — decision #1):
	// include findings on non-runtime-scope dependencies
	// (dev/test/build/optional/peer) in the operator digest. Default
	// false so the P2 Python dev-tooling expansion never floods the
	// email; runtime-scope findings always digest.
	VulnDigestIncludeDev bool `json:"vuln_digest_include_dev"`

	// VulnDigestMinSeverity is the severity floor for the digest:
	// CRITICAL, HIGH (default — admits CRITICAL+HIGH), MEDIUM, LOW,
	// or ALL (everything incl. UNKNOWN). Unrecognized values fall
	// back to HIGH.
	VulnDigestMinSeverity string `json:"vuln_digest_min_severity"`

	// VulnDigestIntervalHours is the minimum gap between digest
	// emails (default 24). The ticker checks hourly; a digest is
	// sent only when the interval has elapsed AND new findings
	// exist — quiet windows produce no email.
	VulnDigestIntervalHours int `json:"vuln_digest_interval_hours"`
}

// VulnDigestMinSeverityOrDefault returns the configured digest floor,
// defaulting to HIGH when unset.
func (m MailConfig) VulnDigestMinSeverityOrDefault() string {
	if strings.TrimSpace(m.VulnDigestMinSeverity) == "" {
		return "HIGH"
	}
	return m.VulnDigestMinSeverity
}

// VulnDigestInterval returns the configured digest interval,
// defaulting to 24h when unset or non-positive.
func (m MailConfig) VulnDigestInterval() time.Duration {
	if m.VulnDigestIntervalHours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(m.VulnDigestIntervalHours) * time.Hour
}

// Load reads configuration from a JSON file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
}

// SlogLevel returns the slog.Level corresponding to the LogLevel string.
func (c *Config) SlogLevel() slog.Level {
	switch strings.ToLower(c.LogLevel) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// MatviewRebuildWeekday returns the time.Weekday for the configured matview
// rebuild day, or -1 if disabled.
// ActivityHistoryWindowDaysOrDefault returns the configured history
// window span, clamped to GitHub's 1-year contributionsCollection
// limit (365); non-positive values fall back to the 180-day default.
func (c *CollectionConfig) ActivityHistoryWindowDaysOrDefault() int {
	switch {
	case c.ActivityHistoryWindowDays <= 0:
		return 180
	case c.ActivityHistoryWindowDays > 365:
		return 365
	default:
		return c.ActivityHistoryWindowDays
	}
}

func (c *CollectionConfig) MatviewRebuildWeekday() int {
	switch strings.ToLower(c.MatviewRebuildDay) {
	case "sunday":
		return int(time.Sunday)
	case "monday":
		return int(time.Monday)
	case "tuesday":
		return int(time.Tuesday)
	case "wednesday":
		return int(time.Wednesday)
	case "thursday":
		return int(time.Thursday)
	case "friday":
		return int(time.Friday)
	case "saturday":
		return int(time.Saturday)
	// v0.27.96: "disable" added to the alias list. The operator set it on
	// production 2026-08-18 believing rebuilds were off; the silent
	// Saturday fallback kept the 11-hour weekly rebuild firing. Be liberal
	// in what disable spellings we accept, and see
	// MatviewRebuildDayRecognized for the WARN-on-typo companion.
	case "disabled", "disable", "none", "off":
		return -1
	default:
		return int(time.Saturday) // default
	}
}

// MatviewRebuildDayRecognized reports whether MatviewRebuildDay maps to a
// deliberate schedule choice — a real weekday, a disable alias, or empty
// (the documented default). False means the value is a typo and
// MatviewRebuildWeekday is silently falling back to Saturday; the scheduler
// WARNs at startup so the fallback is never invisible (v0.27.96,
// log-the-effective-value rule).
func (c *CollectionConfig) MatviewRebuildDayRecognized() bool {
	switch strings.ToLower(c.MatviewRebuildDay) {
	case "", "sunday", "monday", "tuesday", "wednesday", "thursday",
		"friday", "saturday", "disabled", "disable", "none", "off":
		return true
	default:
		return false
	}
}

// DefaultConfig returns configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Database: DatabaseConfig{
			Host:    "localhost",
			Port:    5432,
			User:    "augur",
			DBName:  "augur",
			SSLMode: "prefer",
		},
		GitHub: PlatformConfig{
			BaseURL: "https://api.github.com",
		},
		GitLab: PlatformConfig{
			BaseURL: "https://gitlab.com/api/v4",
		},
		Web: WebConfig{
			Addr:           ":8082",
			GitLabBaseURL:  "https://gitlab.com",
			APIInternalURL: "http://127.0.0.1:8383",
		},
		Collection: CollectionConfig{
			DaysUntilRecollect:        1,
			Workers:                   12,
			RepoCloneDir:              defaultCloneDir(),
			MatviewRebuildDay:         "saturday",
			ActivityHistoryWindowDays: 180,
			MatviewRebuildOnStartup:   false,
			// v0.26.0 (tech-debt Action 3, phase A): GraphQL is the
			// default for GitHub PR-child fetch and issue+PR listing —
			// the flip the v0.19.0 sunset plan scheduled but never
			// executed. Shadow-diffed to column equivalence across the
			// v0.18–v0.22 phases; ~5× faster wall-clock on the reference
			// repo. REST remains a first-class escape hatch: set
			// "pr_child_mode"/"listing_mode" to "rest" in aveloxis.json.
			// Path DELETION (phase B) is a separate operator go/no-go.
			PRChildMode:    "graphql",
			ListingMode:    "graphql",
			ThreadingMode:  "single",
			ShardSize:      3000,
			IssueChildMode: "graphql",
			// v0.21.0 ScancodeWorker defaults — see CollectionConfig
			// field docs for the full rationale.
			ScancodeWorkers:              2,
			ScancodeStartIntervalSec:     90,
			ScancodeCadenceDays:          180,
			ScancodeCloneDir:             defaultScancodeCloneDir(),
			ScancodeShutdownGraceMinutes: 0,    // v0.23.7: immediate kill on stop
			ScancodeRunTimeoutHours:      2,    // v0.23.8: base wall-clock per scan
			ScancodeRunTimeoutCapHours:   24,   // v0.23.8: upper bound on adaptive timeout
			ScancodeMaxInMemory:          5000, // v0.25.2: matches pre-v0.25.2 hardcoded value; bump on RAM-rich production hosts
			ScancodeTimeoutCapStrikes:    3,    // v0.27.6: sideline after 3 consecutive at-cap timeouts (the pytorch/docs 27× spin loop)
			// v0.24.0 DistributionWorker defaults. Off by default;
			// 6-month cadence; modest concurrency. See CollectionConfig
			// field docs for the full rationale.
			DistributionTrackingEnabled:          false,
			DistributionTrackingIntervalDays:     180,
			DistributionTrackingWorkers:          4,
			DistributionTrackingStartIntervalSec: 30,
			// v0.25.7 MailingListWorker. Off by default.
			MailingListEnabled:     false,
			MailingListWorkers:     2,
			MailingListCadenceDays: 30,
			// MailingListBackfillMonths left nil → MailingListBackfillMonthsOrDefault()
			// returns 6. Set it explicitly to 0 (or negative) in aveloxis.json for
			// full-history collection from each list's first month.
			MailingListMirrorHandling:   "metadata_only",
			MailingListProcessorWorkers: 1, // single-threaded per list (summary/12 §11)
		},
		LogLevel: "info",
	}
}

// APIConfig configures the public analytics API (v0.27.0). Rate
// limits apply ONLY to clients whose resolved IP is outside
// ExemptCIDRs — same-box / same-LAN traffic is never limited.
type APIConfig struct {
	// RateLimitRPS is the sustained per-IP request rate. Default 1.
	RateLimitRPS float64 `json:"rate_limit_rps,omitempty"`
	// RateLimitBurst is the per-IP burst capacity. Default 10.
	RateLimitBurst int `json:"rate_limit_burst,omitempty"`
	// RateLimitDaily is the per-IP daily request quota — the actual
	// anti-bulk-crawl control. Default 1000.
	RateLimitDaily int `json:"rate_limit_daily,omitempty"`
	// ExemptCIDRs lists client networks that bypass limiting
	// entirely. Default: loopback + RFC1918 (+ ::1).
	ExemptCIDRs []string `json:"exempt_cidrs,omitempty"`
	// CORSOrigins lists browser origins allowed to call the API
	// (the separate-repo GUI). Empty = no cross-origin access.
	CORSOrigins []string `json:"cors_origins,omitempty"`
	// TrustedProxy is the peer IP whose X-Forwarded-For header is
	// believed when resolving the client address (the nginx-on-
	// same-box layout). Empty = XFF ignored.
	TrustedProxy string `json:"trusted_proxy,omitempty"`
	// RequireAuth gates every data endpoint (all but /health) behind
	// Bearer session tokens. Default FALSE: flip it on once the
	// aveloxis-gui token flow is deployed — enabling it earlier
	// breaks the server-rendered GUI's browser-side chart fetches.
	// Exempt-CIDR clients bypass auth even when enabled.
	RequireAuth bool `json:"require_auth,omitempty"`
}

// RateLimitRPSOrDefault returns the configured sustained rate, or 1.
func (a APIConfig) RateLimitRPSOrDefault() float64 {
	if a.RateLimitRPS <= 0 {
		return 1
	}
	return a.RateLimitRPS
}

// RateLimitBurstOrDefault returns the configured burst, or 10.
func (a APIConfig) RateLimitBurstOrDefault() int {
	if a.RateLimitBurst <= 0 {
		return 10
	}
	return a.RateLimitBurst
}

// RateLimitDailyOrDefault returns the configured daily quota, or 1000.
func (a APIConfig) RateLimitDailyOrDefault() int {
	if a.RateLimitDaily <= 0 {
		return 1000
	}
	return a.RateLimitDaily
}

// ExemptCIDRsOrDefault returns the configured exempt networks, or the
// loopback + RFC1918 default set.
func (a APIConfig) ExemptCIDRsOrDefault() []string {
	if len(a.ExemptCIDRs) == 0 {
		return []string{"127.0.0.0/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	}
	return a.ExemptCIDRs
}

// SkipLargestFraction converts collection.skip_largest_percent into
// the (0,1) fraction LargestRepoIDs consumes. Out-of-range values
// (negative, or >= 100 — certainly a misconfiguration) disable the
// skip entirely; 0 = disabled. The scheduler logs the EFFECTIVE
// threshold values at refresh time, per the effective-value rule.
func (c CollectionConfig) SkipLargestFraction() float64 {
	if c.SkipLargestPercent <= 0 || c.SkipLargestPercent >= 100 {
		return 0
	}
	return c.SkipLargestPercent / 100
}
