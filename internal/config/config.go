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

	// LogLevel sets the minimum log level: "debug", "info", "warn", or "error".
	LogLevel string `json:"log_level"`
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
	// 30 minutes when unset.
	//
	// Within the grace window: runners complete their scans naturally
	// (parsing scancode JSON output, writing results to the DB,
	// clearing lock columns). At grace expiry: the worker calls
	// cmd.Process.Kill() on the still-running scancode subprocess,
	// which causes cmd.Wait() to return with an error, which triggers
	// the runner's lock-clear path. No orphaned scans from graceful
	// shutdown.
	//
	// Separate from collection.shutdown_grace_seconds (which paces
	// the main scheduler's stop). 30 min is much longer than 10s
	// because scancode runs are intrinsically long-running on big
	// repos and you usually want them to finish if they're close.
	ScancodeShutdownGraceMinutes int `json:"scancode_shutdown_grace_minutes"`
}

// EnrichIntervalDuration converts EnrichIntervalMinutes to a time.Duration.
// Falls back to 30 minutes when unset (zero) so existing aveloxis.json
// files without the new key keep the documented default.
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
// time.Duration. Falls back to 30 minutes when unset.
func (c *CollectionConfig) ScancodeShutdownGrace() time.Duration {
	if c.ScancodeShutdownGraceMinutes <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.ScancodeShutdownGraceMinutes) * time.Minute
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
	case "disabled", "none", "off":
		return -1
	default:
		return int(time.Saturday) // default
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
			DaysUntilRecollect:      1,
			Workers:                 12,
			RepoCloneDir:            defaultCloneDir(),
			MatviewRebuildDay:       "saturday",
			MatviewRebuildOnStartup: false,
			PRChildMode:             "rest",
			ListingMode:             "rest",
			ThreadingMode:           "single",
			ShardSize:               3000,
			IssueChildMode:          "graphql",
			// v0.21.0 ScancodeWorker defaults — see CollectionConfig
			// field docs for the full rationale.
			ScancodeWorkers:              2,
			ScancodeStartIntervalSec:     90,
			ScancodeCadenceDays:          180,
			ScancodeCloneDir:             defaultScancodeCloneDir(),
			ScancodeShutdownGraceMinutes: 30,
		},
		LogLevel: "info",
	}
}
