// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Aveloxis is a data collection tool for open source software community health metrics.
// It collects data from GitHub and GitLab with equal completeness, storing results
// in a shared schema for cross-platform analysis.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/api"
	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/monitor"
	"github.com/aveloxis/aveloxis/internal/pidfile"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/aveloxis/aveloxis/internal/scheduler"
	"github.com/aveloxis/aveloxis/internal/web"
	"github.com/spf13/cobra"
)

// Version is the current Aveloxis version. Single source of truth is db.ToolVersion.
var Version = db.ToolVersion

func main() {
	root := &cobra.Command{
		Use:   "aveloxis",
		Short: "Open source community health data collection",
	}

	var cfgPath string
	root.PersistentFlags().StringVarP(&cfgPath, "config", "c", "aveloxis.json", "path to config file")

	root.AddCommand(
		collectCmd(&cfgPath),
		serveCmd(&cfgPath),
		apiCmd(&cfgPath),
		scancodeWorkerCmd(&cfgPath),
		webCmd(&cfgPath),
		startCmd(&cfgPath),
		stopCmd(&cfgPath),
		addRepoCmd(&cfgPath),
		loadFoundationCoreReposCmd(&cfgPath),
		loadFoundationOrgsCmd(&cfgPath),
		loadApacheListsCmd(&cfgPath),
		backfillExternalKeysCmd(&cfgPath),
		backfillMailingListProjectionCmd(&cfgPath),
		resolveEmailIdentitiesCmd(&cfgPath),
		stripQuotedHistoryCmd(&cfgPath),
		registerJiraProjectsCmd(&cfgPath),
		backfillJiraIdentitiesCmd(&cfgPath),
		deployChecklistCmd(),
		ackDeployCmd(&cfgPath),
		mailingListStatsCmd(&cfgPath),
		verifyMailingListCmd(&cfgPath),
		registerMailingListCmd(&cfgPath),
		loadNumfocusProjectsCmd(&cfgPath),
		loadNumfocusOrgsCmd(&cfgPath),
		addKeyCmd(&cfgPath),
		prioritizeCmd(&cfgPath),
		recollectCmd(&cfgPath),
		migrateCmd(&cfgPath),
		migrateCntrbIDsCmd(&cfgPath),
		mergeCntrbCollisionsCmd(&cfgPath),
		dedupReposCmd(&cfgPath),
		backfillIdentitiesCmd(&cfgPath),
		refreshViewsCmd(&cfgPath),
		installToolsCmd(),
		upgradeToolsCmd(),
		sbomCmd(&cfgPath),
		shadowDiffCmd(),
		dataTestCmd(&cfgPath),
		testMailCmd(&cfgPath),
		stagingStatsCmd(&cfgPath),
		healVulnerabilitiesCmd(&cfgPath),
		healCollectionGapsCmd(&cfgPath),
		markGoneReposCmd(&cfgPath),
		runScorecardCmd(&cfgPath),
		distributionStatsCmd(&cfgPath),
		versionCmd(),
		healMessagesCmd(&cfgPath),
		reconcileReposCmd(&cfgPath),
		dataVerifyCmd(&cfgPath),
		generateShowcaseCmd(&cfgPath),
		backfillRepoMetadataCmd(&cfgPath),
		rewalkWhitespaceCmd(&cfgPath),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// --- serve: long-running scheduler + monitor ---

func serveCmd(cfgPath *string) *cobra.Command {
	var (
		monitorAddr      string
		workers          int
		useAugurKeys     bool
		allowSecondServe bool
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the collection scheduler and monitoring dashboard",
		Long: `Starts the scheduler, which continuously collects data for all repos
in the queue. Also starts the web monitor (like Flower for Celery).

The queue is stored in Postgres. Multiple aveloxis instances can share
the same queue — each claims jobs via SELECT ... FOR UPDATE SKIP LOCKED.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// If --workers wasn't explicitly set on the CLI, use the config
			// file value. This lets users set workers in aveloxis.json without
			// needing to pass it on the command line every time.
			if !cmd.Flags().Changed("workers") {
				cfg := loadConfig(*cfgPath, slog.New(slog.NewTextHandler(os.Stderr, nil)))
				if cfg.Collection.Workers > 0 {
					workers = cfg.Collection.Workers
				}
			}
			return runServe(*cfgPath, monitorAddr, workers, useAugurKeys, allowSecondServe)
		},
	}

	cmd.Flags().StringVar(&monitorAddr, "monitor", "127.0.0.1:5555", "address for the monitoring dashboard")
	cmd.Flags().IntVar(&workers, "workers", 1, "number of concurrent collection workers")
	cmd.Flags().BoolVar(&useAugurKeys, "augur-keys", false, "load API keys from Augur's augur_operations.worker_oauth table")
	// 2026-09-11 (F3). Serve REFUSES to start when another
	// aveloxis-serve is already connected to the same database; this is
	// the deliberate override. A CLI flag rather than a config key on
	// purpose: a config key persists silently and would make the second
	// serve invisible again, which is the failure this refusal exists to
	// prevent. `aveloxis start serve` does not forward it — a deliberate
	// second serve is the foreground/systemd shape (see
	// docs/guide/dedicated-scancode-host.md).
	cmd.Flags().BoolVar(&allowSecondServe, "allow-second-serve", false,
		"start even though another aveloxis-serve is connected to this database (competes for the same queue and API keys)")

	return cmd
}

func runServe(cfgPath, monitorAddr string, workers int, useAugurKeys, allowSecondServe bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	// Write PID file so 'aveloxis stop serve' can find us.
	pidPath := pidfile.Path("serve")
	if err := pidfile.Write(pidPath, os.Getpid()); err != nil {
		logger.Warn("failed to write PID file — 'aveloxis stop' will fall back to pgrep", "path", pidPath, "error", err)
	}
	defer pidfile.Remove(pidPath)

	// v0.22.4 item 8 — register SIGUSR1 handler so operators can
	// snapshot the goroutine state of a running serve without killing
	// it. Operator workflow:
	//   kill -USR1 $(cat ~/.aveloxis/aveloxis-serve.pid)
	//   ls -lh ~/.aveloxis/serve-goroutines-*.txt
	uninstallDump := installGoroutineDumpHandler(logger, defaultGoroutineDumpDir())
	defer uninstallDump()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Scale the database connection pool to the worker count so collection
	// workers don't starve each other for connections. Each worker makes many
	// concurrent DB calls (inserts, queries) during collection phases.
	poolSize := max(int32(workers+15), 20)
	// application_name = "aveloxis-serve" so post-stop verification
	// (and operators reading pg_stat_activity) can filter per-process.
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName(db.AppNameForHost(db.ServeApplicationName)), logger, poolSize)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// F13 (v0.27.131): serve startup trusts a current schema stamp and
	// skips the migration re-walk (observed 1h42m of backfill-window
	// re-walking on kate with zero collection). `aveloxis migrate`
	// remains the full-run self-heal path and never fast-paths.
	store.SetMigrateFastPath(true)
	store.SetAllowSecondServe(allowSecondServe)
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrating database: %w", err)
	}

	ghKeys, glKeys, err := loadKeys(ctx, cfg, store, useAugurKeys, logger)
	if err != nil {
		return fmt.Errorf("loading API keys: %w", err)
	}
	ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
	glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

	// Start scheduler.
	store.SetMatviewOnStartup(cfg.Collection.MatviewRebuildOnStartup)

	sched := scheduler.NewWithKeys(store, ghClient, glClient, ghKeys, logger, scheduler.Config{
		Workers: workers,
		// The whole aveloxis.json collection block, consumed directly —
		// the scheduler reads knobs through the CollectionConfig
		// accessors, so a new knob needs NO wiring here (v0.25.37).
		Collection: &cfg.Collection,
		// The mail block rides the same single-source pattern
		// (v0.27.12 operator vulnerability digest).
		Mail: &cfg.Mail,
	})
	// v0.27.12: operator vulnerability digest. Must be injected
	// BEFORE Run starts (the ticker gate is evaluated at startup).
	if cfg.Mail.OperatorEmail != "" {
		sched.SetDigestMailer(digestMailerAdapter{mailer.New(mailerConfigFrom(cfg), logger)})
	}
	// v0.27.36 (summary/18 Part 3b): the scheduler goroutine is
	// JOINED on shutdown. Pre-fix, runServe returned as soon as
	// ctx cancelled and the deferred store.Close() raced the
	// scheduler's own drain → releaseOurLocks → Close sequence —
	// silently undoing the v0.20.0/v0.27.25 graceful-shutdown work
	// (the residual "stuck in 'collecting' after stop" source).
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		sched.Run(ctx)
	}()

	// Start monitor.
	// v0.23.0: refresh cadence is operator-configurable via
	// monitor.refresh_seconds in aveloxis.json. Falls back to the
	// package default (60s) when unset / out of bounds.
	mon := monitor.NewWithOptions(store, logger, monitor.Options{
		RefreshSeconds: cfg.Monitor.MonitorRefreshSecondsOrDefault(),
	})
	srv := &http.Server{Addr: monitorAddr, Handler: mon.Handler()}
	go func() {
		logger.Info("monitor listening", "addr", monitorAddr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("monitor server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("monitor server shutdown", "error", err)
	}
	// Wait for the scheduler's graceful shutdown (worker drain, lock
	// release, its own pool close) before the deferred store.Close()
	// runs. Bounded: the scheduler's drain is capped by
	// shutdown_grace_seconds and its wait for the background pools'
	// bookkeeping by the scancode grace + allowance (shutdownBudget);
	// the margin here is a backstop against a wedged DB call in the
	// drain path itself. Expiring here skips releaseOurLocks (the next
	// start's RecoverOtherWorkerLocks covers) and closes the pool under
	// any bookkeeping still in flight — so this bound must exceed the
	// scheduler's (pass 39).
	select {
	case <-schedDone:
	case <-time.After(shutdownBudget(cfg) + 30*time.Second):
		logger.Warn("scheduler did not finish shutdown within the shutdown budget + margin — exiting anyway")
	}
	return nil
}

// shutdownBudget is the longest a graceful serve shutdown takes before
// its pool close: the collection drain grace, plus the operator's
// scancode grace and the runners' bookkeeping allowance the scheduler
// waits for. Every outer bound (the runServe join, `aveloxis stop`'s
// backend poll, the documented systemd TimeoutStopSec) derives from it
// (pass 39).
func shutdownBudget(cfg *config.Config) time.Duration {
	return cfg.Collection.ShutdownGraceDuration() + collector.ScancodeShutdownBound(cfg.Collection.ScancodeShutdownGrace())
}

// --- api: REST API server ---

func apiCmd(cfgPath *string) *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Start the Aveloxis REST API server",
		Long: `Starts a REST API server for data access. Used by the monitoring
dashboard and web GUI to fetch repo statistics and SBOMs.

Run alongside 'aveloxis serve' and 'aveloxis web'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAPI(*cfgPath, addr)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "listen address for the API server (overrides api.addr in aveloxis.json; default 127.0.0.1:8383)")
	return cmd
}

// warnAPIPortMismatch surfaces the one mistake api.addr makes easy:
// move the API to another port and the web GUI keeps proxying /api/*
// to the old one, so every chart silently 502s while both processes
// look healthy.
//
// Deliberately narrow. api_internal_url pointing at a DIFFERENT HOST
// is a legitimate split deployment (or an nginx hop) and says nothing
// about this machine's api.addr, so only a LOOPBACK target with a
// mismatched port is reported — that combination cannot be right.
// A warning, not an error: the operator may be mid-migration, and web
// serves the GUI fine without the proxy.
func warnAPIPortMismatch(cfg *config.Config, logger *slog.Logger) {
	target, err := url.Parse(strings.TrimSpace(cfg.Web.APIInternalURL))
	if err != nil || target.Host == "" {
		return // web.New already warns about an unparseable value
	}
	host, port, err := net.SplitHostPort(target.Host)
	if err != nil {
		return // no explicit port; nothing to compare
	}
	// "localhost" is loopback but not an IP literal — and it is the
	// spelling docs/guide/api.md teaches, so keying on ParseIP alone
	// made the warning silent for the project's own documented value.
	isLoopback := strings.EqualFold(host, "localhost")
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		isLoopback = true
	}
	if !isLoopback {
		return
	}
	_, apiPort, err := net.SplitHostPort(cfg.API.AddrOrDefault())
	if err != nil || apiPort == port {
		return
	}
	logger.Warn("web.api_internal_url points at a loopback port the API is not listening on — /api/* will 502 and every chart in the GUI will be empty",
		"api_internal_url", cfg.Web.APIInternalURL,
		"api_addr", cfg.API.AddrOrDefault(),
		"fix", "set web.api_internal_url to http://127.0.0.1:"+apiPort)
}

func runAPI(cfgPath, addr string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	pidPath := pidfile.Path("api")
	if err := pidfile.Write(pidPath, os.Getpid()); err != nil {
		logger.Warn("failed to write PID file — 'aveloxis stop' will fall back to pgrep", "path", pidPath, "error", err)
	}
	defer pidfile.Remove(pidPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Flag beats config beats default. `aveloxis start api` passes only
	// --config, so the config value is the one that governs a
	// backgrounded API process (v0.28.19).
	if addr == "" {
		addr = cfg.API.AddrOrDefault()
	}

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName(componentAppName("api")), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// api does not run migrations — check if schema is current.
	store.CheckSchemaVersion(ctx, logger)

	apiServer, err := api.NewWithOptions(store, logger, api.Options{
		RateLimitRPS:   cfg.API.RateLimitRPSOrDefault(),
		RateLimitBurst: cfg.API.RateLimitBurstOrDefault(),
		RateLimitDaily: cfg.API.RateLimitDailyOrDefault(),
		ExemptCIDRs:    cfg.API.ExemptCIDRsOrDefault(),
		CORSOrigins:    cfg.API.CORSOrigins,
		TrustedProxy:   cfg.API.TrustedProxy,
		RequireAuth:    cfg.API.RequireAuth,
		// v0.27.20 per-add approval: add-request notifications +
		// the auto-approve limit for the portal repo-add endpoint.
		Mailer:              mailer.New(mailerConfigFrom(cfg), logger),
		AutoApproveAddLimit: cfg.Web.AutoApproveAddLimitValue(),
	})
	if err != nil {
		return fmt.Errorf("api middleware config: %w", err)
	}
	srv := &http.Server{Addr: addr, Handler: apiServer.Handler()}

	go func() {
		logger.Info("API server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("API server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("server shutdown", "error", err)
	}
	return nil
}

// --- collect: one-shot collection ---

func collectCmd(cfgPath *string) *cobra.Command {
	var (
		full         bool
		useAugurKeys bool
	)

	cmd := &cobra.Command{
		Use:   "collect [repo-urls...]",
		Short: "One-shot collection for specific repos (does not use the queue)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCollect(*cfgPath, args, full, useAugurKeys)
		},
	}

	cmd.Flags().BoolVar(&full, "full", false, "full historical collection")
	cmd.Flags().BoolVar(&useAugurKeys, "augur-keys", false, "load API keys from Augur's worker_oauth table")

	return cmd
}

func runCollect(cfgPath string, repoURLs []string, full, useAugurKeys bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
	// Schema migrations only run from `aveloxis serve` startup and
	// the dedicated `aveloxis migrate` subcommand. Operators run
	// migrate explicitly before kicking off one-off collections.

	ghKeys, glKeys, err := loadKeys(ctx, cfg, store, useAugurKeys, logger)
	if err != nil {
		return fmt.Errorf("loading API keys: %w", err)
	}
	ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
	glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

	for _, repoURL := range repoURLs {
		client, owner, repo, err := collector.ClientForRepo(repoURL, ghClient, glClient)
		if err != nil {
			logger.Error("skipping repo", "url", repoURL, "error", err)
			continue
		}

		repoID, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: client.Platform(),
			GitURL:   repoURL,
			Name:     repo,
			Owner:    owner,
		})
		if err != nil {
			logger.Error("failed to upsert repo", "url", repoURL, "error", err)
			continue
		}

		// v0.27.139: the incremental lower bound is the queue row's
		// last_collected (start-anchored by CompleteJob), NEVER
		// now−days_until_recollect — the old expression skipped
		// everything last-updated between the previous run and now−D
		// (the podman-desktop blind-window class, CLI edition). A
		// never-collected or untracked repo — and any lookup ERROR
		// (SR-5: an error is not "never collected"; full is the safe
		// direction) — collects from zero.
		var since time.Time
		if !full {
			if lc, lcErr := store.GetRepoLastCollected(ctx, repoID); lcErr != nil {
				logger.Warn("last_collected lookup failed — collecting FULL", "url", repoURL, "error", lcErr)
			} else if lc != nil {
				since = *lc
			}
		}

		coll := collector.NewWithOptions(client, store, logger, ghKeys, cfg.Collection.RepoCloneDir).
			WithCollectionModes(cfg.Collection.PRChildMode, cfg.Collection.ListingMode,
				cfg.Collection.ThreadingMode, cfg.Collection.ShardSize, cfg.Collection.IssueChildMode)
		result, err := coll.CollectRepo(ctx, repoID, owner, repo, since)
		if err != nil {
			logger.Error("collection failed", "url", repoURL, "error", err)
			continue
		}

		logger.Info("done", "url", repoURL,
			"issues", result.Issues, "prs", result.PullRequests,
			"messages", result.Messages, "events", result.Events,
			"releases", result.Releases, "contributors", result.Contributors,
			"errors", len(result.Errors))
	}
	return nil
}

// --- add-repo: add repos to the collection queue ---

func addRepoCmd(cfgPath *string) *cobra.Command {
	var (
		priority  int
		fromAugur bool
	)

	cmd := &cobra.Command{
		Use:   "add-repo [repo-urls...]",
		Short: "Add repos to the collection queue",
		Long: `Registers repos in the database and adds them to the scheduler queue.
The scheduler (aveloxis serve) will pick them up automatically.

Use --from-augur to import repos from an existing Augur installation's
augur_data.repo table. Each URL is verified against the forge (HTTP HEAD)
and only repos that still exist are imported.`,
		Args: func(cmd *cobra.Command, args []string) error {
			fromAugur, _ := cmd.Flags().GetBool("from-augur")
			if !fromAugur && len(args) == 0 {
				return fmt.Errorf("requires at least 1 repo URL (or --from-augur)")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromAugur {
				return runImportFromAugur(*cfgPath, priority)
			}
			return runAddRepo(*cfgPath, args, priority)
		},
	}

	cmd.Flags().IntVar(&priority, "priority", 100, "queue priority (lower = collected sooner, 0 = immediate)")
	cmd.Flags().BoolVar(&fromAugur, "from-augur", false, "import repos from augur_data.repo (verifies each URL exists)")

	return cmd
}

// isOrgURL checks if a URL points to a GitHub org or GitLab group (not a specific repo).
// Returns (isOrg, host, orgName, platform).
func isOrgURL(rawURL string) (bool, string, string, model.Platform) {
	rawURL = strings.TrimSpace(rawURL)
	rawURL = strings.TrimSuffix(rawURL, "/")
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, "", "", 0
	}
	host := strings.ToLower(u.Host)
	path := strings.Trim(u.Path, "/")
	parts := strings.Split(path, "/")

	// GitHub org: https://github.com/chaoss (exactly 1 path segment)
	if (host == "github.com") && len(parts) == 1 && parts[0] != "" {
		return true, host, parts[0], model.PlatformGitHub
	}
	// GitLab group: could be 1+ segments, but we only treat it as a group
	// if ParseRepoURL fails (meaning it can't find a project at the end).
	// For now, we try ParseRepoURL first and fall through to org expansion.
	return false, "", "", 0
}

func runAddRepo(cfgPath string, repoURLs []string, priority int) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
	// add-repo trusts that the operator has already run
	// `aveloxis migrate` once for this database.

	ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
	if err != nil {
		return fmt.Errorf("loading API keys: %w", err)
	}

	for _, repoURL := range repoURLs {
		// Check if this is an org/group URL instead of a repo URL.
		if isOrg, host, orgName, plat := isOrgURL(repoURL); isOrg {
			logger.Info("expanding organization", "org", orgName, "platform", plat)

			// Create a repo_group for this org so the refresh job can re-scan it later.
			rgType := "github_org"
			if plat == model.PlatformGitLab {
				rgType = "gitlab_group"
			}
			groupID, err := store.UpsertRepoGroup(ctx, orgName, rgType, repoURL)
			if err != nil {
				logger.Warn("failed to create repo group for org", "org", orgName, "error", err)
			}

			var repos []orgRepo
			switch plat {
			case model.PlatformGitHub:
				ghHTTP := platform.NewHTTPClient("https://api.github.com", ghKeys, logger, platform.AuthGitHub)
				repos, err = listGitHubOrgRepos(ctx, ghHTTP, orgName)
			case model.PlatformGitLab:
				glHTTP := platform.NewHTTPClient("https://"+host+"/api/v4", glKeys, logger, platform.AuthGitLab)
				repos, err = listGitLabGroupRepos(ctx, glHTTP, orgName)
			}
			if err != nil {
				logger.Error("failed to list org repos", "org", orgName, "error", err)
				continue
			}
			logger.Info("found repos in organization", "org", orgName, "count", len(repos))

			// Bridge legacy repo_groups discovery into modern
			// aveloxis_ops.user_repos so any user_group tracking this
			// org (via user_org_requests.org_url) gets every repo
			// (including forks — listGitHub/GitLab use ?type=all)
			// linked. Hoisted out of the per-repo loop so the lookup
			// runs once per scan.
			userGroupIDs, ugErr := store.GetUserGroupIDsForOrgURL(ctx, repoURL)
			if ugErr != nil {
				logger.Warn("failed to look up user_groups for org", "org_url", repoURL, "error", ugErr)
			}
			for _, r := range repos {
				addOneRepoWithGroup(ctx, store, logger, r, plat, priority, groupID)
				if len(userGroupIDs) == 0 {
					continue
				}
				repoID, ferr := store.FindRepoByURL(ctx, r.URL)
				if ferr != nil || repoID == 0 {
					continue
				}
				for _, gid := range userGroupIDs {
					if _, err := store.AddRepoToGroupByID(ctx, gid, repoID); err != nil {
						logger.Warn("failed to link repo into user_repos",
							"group_id", gid, "repo_id", repoID, "error", err)
					}
				}
			}
			continue
		}

		// Regular repo URL.
		parsed, err := platform.ParseRepoURL(repoURL)
		if err != nil {
			logger.Error("invalid URL", "url", repoURL, "error", err)
			continue
		}
		addOneRepo(ctx, store, logger, repoURL, parsed.Owner, parsed.Repo, parsed.Platform, priority)
	}
	return nil
}

func addOneRepoWithGroup(ctx context.Context, store *db.PostgresStore, logger *slog.Logger, r orgRepo, plat model.Platform, priority int, groupID int64) {
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: plat,
		GitURL:   r.URL,
		Name:     r.Name,
		Owner:    r.Owner,
		GroupID:  groupID,
		// v0.27.103: the forge numeric ID enables UpsertRepo's
		// rename-heal (untracked URL + forge-ID hit) and backfills
		// already-tracked rows via the prefer-nonempty conflict clause.
		PlatformID: r.ForgeID,
	})
	if err != nil {
		logger.Error("failed to register repo", "url", r.URL, "error", err)
		return
	}
	if err := store.EnqueueRepo(ctx, repoID, priority); err != nil {
		logger.Error("failed to enqueue repo", "url", r.URL, "error", err)
		return
	}
	logger.Info("repo added to queue", "url", r.URL, "repo_id", repoID, "priority", priority)
}

func addOneRepo(ctx context.Context, store *db.PostgresStore, logger *slog.Logger, repoURL, owner, name string, plat model.Platform, priority int) {
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: plat,
		GitURL:   repoURL,
		Name:     name,
		Owner:    owner,
	})
	if err != nil {
		logger.Error("failed to register repo", "url", repoURL, "error", err)
		return
	}
	if err := store.EnqueueRepo(ctx, repoID, priority); err != nil {
		logger.Error("failed to enqueue repo", "url", repoURL, "error", err)
		return
	}
	logger.Info("repo added to queue", "url", repoURL, "repo_id", repoID, "priority", priority)
}

type orgRepo struct {
	URL   string
	Owner string
	Name  string
	// ForgeID is the forge's numeric repository ID from the listing
	// JSON (v0.27.103 — this path was the one org-scan site the
	// v0.27.102 rename-dedup fix missed). UpsertRepo dedups on it.
	ForgeID string
}

// listGitHubOrgRepos calls GET /orgs/{org}/repos to list all public repos.
func listGitHubOrgRepos(ctx context.Context, http *platform.HTTPClient, org string) ([]orgRepo, error) {
	var repos []orgRepo
	page := 1
	for {
		path := fmt.Sprintf("/orgs/%s/repos?per_page=100&type=all&page=%d", org, page)
		resp, err := http.Get(platform.WithoutETag(ctx), path)
		if err != nil {
			return repos, err
		}
		var items []struct {
			ID       int64  `json:"id"` // v0.27.103 — rename-proof numeric identity
			FullName string `json:"full_name"`
			HTMLURL  string `json:"html_url"`
			Name     string `json:"name"`
			Owner    struct {
				Login string `json:"login"`
			} `json:"owner"`
			Archived bool `json:"archived"`
			Fork     bool `json:"fork"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			return repos, err
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			repos = append(repos, orgRepo{
				URL:     item.HTMLURL,
				Owner:   item.Owner.Login,
				Name:    item.Name,
				ForgeID: model.ForgeIDString(item.ID),
			})
		}
		page++
	}
	return repos, nil
}

// listGitLabGroupRepos calls GET /groups/{group}/projects to list all projects.
func listGitLabGroupRepos(ctx context.Context, http *platform.HTTPClient, group string) ([]orgRepo, error) {
	var repos []orgRepo
	page := 1
	encodedGroup := url.PathEscape(group)
	for {
		path := fmt.Sprintf("/groups/%s/projects?per_page=100&include_subgroups=true&page=%d", encodedGroup, page)
		resp, err := http.Get(platform.WithoutETag(ctx), path)
		if err != nil {
			return repos, err
		}
		var items []struct {
			ID                int64  `json:"id"` // v0.27.103 — rename-proof numeric identity
			PathWithNamespace string `json:"path_with_namespace"`
			WebURL            string `json:"web_url"`
			Name              string `json:"name"`
			Namespace         struct {
				FullPath string `json:"full_path"`
			} `json:"namespace"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			return repos, err
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			repos = append(repos, orgRepo{
				URL:     item.WebURL,
				Owner:   item.Namespace.FullPath,
				Name:    item.Name,
				ForgeID: model.ForgeIDString(item.ID),
			})
		}
		page++
	}
	return repos, nil
}

func runImportFromAugur(cfgPath string, priority int) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
	// import-from-augur trusts that the operator has already run
	// `aveloxis migrate` once for this database.

	// Read all repos from Augur.
	augurRepos, err := db.LoadAugurRepos(ctx, store.Pool())
	if err != nil {
		return fmt.Errorf("reading Augur repos: %w", err)
	}
	logger.Info("found repos in augur_data.repo", "count", len(augurRepos))

	httpClient := &http.Client{Timeout: 10 * time.Second}
	var imported, skipped, failed int

	for _, ar := range augurRepos {
		// Parse the URL to determine platform and owner/repo.
		parsed, err := platform.ParseRepoURL(ar.RepoGit)
		if err != nil {
			logger.Warn("skipping unparseable URL", "url", ar.RepoGit, "augur_repo_id", ar.RepoID, "error", err)
			skipped++
			continue
		}

		// Verify the repo still exists on the forge with an HTTP HEAD.
		exists, err := verifyRepoExists(ctx, httpClient, ar.RepoGit)
		if err != nil {
			logger.Warn("error verifying repo", "url", ar.RepoGit, "error", err)
			failed++
			continue
		}
		if !exists {
			logger.Warn("repo no longer exists on forge, skipping", "url", ar.RepoGit)
			skipped++
			continue
		}

		// Import into Aveloxis.
		repoID, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: parsed.Platform,
			GitURL:   ar.RepoGit,
			Name:     parsed.Repo,
			Owner:    parsed.Owner,
		})
		if err != nil {
			logger.Error("failed to register repo", "url", ar.RepoGit, "error", err)
			failed++
			continue
		}

		if err := store.EnqueueRepo(ctx, repoID, priority); err != nil {
			logger.Error("failed to enqueue repo", "url", ar.RepoGit, "error", err)
			failed++
			continue
		}

		imported++
		logger.Info("imported repo", "url", ar.RepoGit, "repo_id", repoID)
	}

	logger.Info("import complete",
		"imported", imported,
		"skipped", skipped,
		"failed", failed,
		"total_augur_repos", len(augurRepos),
	)
	return nil
}

// verifyRepoExists checks that a repo URL resolves on the forge.
// Uses HTTP HEAD to avoid downloading the full page.
func verifyRepoExists(ctx context.Context, client *http.Client, repoURL string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, repoURL, nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	// 200 = exists. 301/302 = moved (still exists). 404/410 = gone.
	return resp.StatusCode >= 200 && resp.StatusCode < 400, nil
}

// --- add-key: store API keys in the database ---

func addKeyCmd(cfgPath *string) *cobra.Command {
	var (
		plat      string
		name      string
		fromAugur bool
	)

	cmd := &cobra.Command{
		Use:   "add-key [token]",
		Short: "Store API keys in the database",
		Long: `Stores GitHub or GitLab API tokens in aveloxis_ops.worker_oauth.
Keys stored here are loaded automatically by 'aveloxis serve' and 'aveloxis collect'.

Use --from-augur to copy all keys from augur_operations.worker_oauth into
aveloxis_ops.worker_oauth in one shot. Duplicates are skipped.`,
		Args: func(cmd *cobra.Command, args []string) error {
			fromAugur, _ := cmd.Flags().GetBool("from-augur")
			if !fromAugur && len(args) != 1 {
				return fmt.Errorf("requires exactly 1 token argument (or --from-augur)")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromAugur {
				return runImportKeysFromAugur(*cfgPath)
			}
			return runAddKey(*cfgPath, args[0], plat, name)
		},
	}

	cmd.Flags().StringVar(&plat, "platform", "github", "platform for this key (github or gitlab)")
	cmd.Flags().StringVar(&name, "name", "", "optional label for this key")
	cmd.Flags().BoolVar(&fromAugur, "from-augur", false, "copy all keys from augur_operations.worker_oauth")

	return cmd
}

func runAddKey(cfgPath, token, plat, name string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	if plat != "github" && plat != "gitlab" {
		return fmt.Errorf("platform must be 'github' or 'gitlab', got %q", plat)
	}

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return err
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
	// add-key trusts that the operator has already run
	// `aveloxis migrate` once for this database.

	if err := db.SaveAPIKey(ctx, store.Pool(), name, token, plat); err != nil {
		return fmt.Errorf("saving key: %w", err)
	}

	masked := token[:4] + "..." + token[len(token)-4:]
	logger.Info("key stored", "platform", plat, "token", masked)
	return nil
}

func runImportKeysFromAugur(cfgPath string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return err
	}
	defer store.Close()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
	// import-keys-from-augur trusts that the operator has already run
	// `aveloxis migrate` once for this database.

	imported, err := db.ImportKeysFromAugur(ctx, store.Pool())
	if err != nil {
		return fmt.Errorf("importing keys from Augur: %w", err)
	}

	logger.Info("keys imported from augur_operations.worker_oauth", "count", imported)
	return nil
}

// --- prioritize: push a repo to top of queue ---

func prioritizeCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "prioritize [repo-url-or-id]",
		Short: "Push a repo to the top of the collection queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPrioritize(*cfgPath, args[0])
		},
	}
}

func runPrioritize(cfgPath, target string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return err
	}
	defer store.Close()

	// Try parsing as a repo URL first, then look up the ID.
	parsed, parseErr := platform.ParseRepoURL(target)
	if parseErr == nil {
		// Look up repo_id by URL.
		repoID, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: parsed.Platform,
			GitURL:   target,
			Name:     parsed.Repo,
			Owner:    parsed.Owner,
		})
		if err != nil {
			return err
		}
		if err := store.PrioritizeRepo(ctx, repoID); err != nil {
			return err
		}
		logger.Info("repo pushed to top of queue", "url", target, "repo_id", repoID)
		return nil
	}

	// Not a URL — error.
	return fmt.Errorf("could not parse %q as a repo URL: %w", target, parseErr)
}

// --- recollect: flag repos for full (since=zero) re-collection ---
//
// v0.18.24: two triggers set the `force_full_collect` flag on a repo's
// collection_queue row:
//
//   - Manual: `aveloxis recollect <url>...` — this command. One or more
//     URLs, each flipped to force_full_collect=TRUE. The flag is picked
//     up on the repo's next scheduled DequeueNext and determineSince
//     returns zero time for a full pass.
//   - Automatic: the scheduler flips the flag itself when a job ends
//     with a GraphQL-batch error class (see shouldForceFullRecollect in
//     internal/scheduler/scheduler.go).
//
// The flag is cleared by CompleteJob on the next successful collection.
// This command does NOT prioritize the repo — operators who want it
// sooner should also run `aveloxis prioritize <url>`.

func recollectCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "recollect [repo-urls...]",
		Short: "Flag one or more repos for a full (since=zero) re-collection on their next scheduled cycle",
		Long: `Sets the force_full_collect flag on each named repo's collection_queue row.
The flag is picked up on the next scheduler cycle — the repo is re-collected
from the beginning of time (since=zero) instead of using the incremental
window, and the flag is cleared on successful completion.

Use this after a bug fix that invalidates a repo's collected data, or after
a GraphQL PR batch error that may have left some PR child data incomplete
(the scheduler auto-flags this case too).

This command does not change queue priority. Combine with 'aveloxis
prioritize <url>' if you want the repo collected immediately.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRecollect(*cfgPath, args)
		},
	}
}

func runRecollect(cfgPath string, targets []string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return err
	}
	defer store.Close()

	var firstErr error
	for _, target := range targets {
		parsed, parseErr := platform.ParseRepoURL(target)
		if parseErr != nil {
			logger.Error("could not parse repo URL — skipping", "url", target, "error", parseErr)
			if firstErr == nil {
				firstErr = parseErr
			}
			continue
		}
		// UpsertRepo is idempotent; use it to resolve the URL to a
		// repo_id without requiring the caller to know the ID.
		repoID, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: parsed.Platform,
			GitURL:   target,
			Name:     parsed.Repo,
			Owner:    parsed.Owner,
		})
		if err != nil {
			logger.Error("failed to resolve repo_id — skipping", "url", target, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := store.SetForceFullCollect(ctx, repoID, true); err != nil {
			logger.Error("failed to set force_full_collect — skipping", "url", target, "repo_id", repoID, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		logger.Info("force_full_collect set — repo will be fully re-collected on next scheduler cycle",
			"url", target, "repo_id", repoID)
	}
	return firstErr
}

// --- migrate ---

func migrateCmd(cfgPath *string) *cobra.Command {
	var skipViews bool
	var noWait bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations",
		Long: `Runs the schema migrations and (by default) creates/refreshes
materialized views used by 8Knot and analytics.

Use --skip-views to skip the materialized view block entirely. This is
useful when you're iterating on a schema-error fix on a large database
where the matview rebuild adds significant time per attempt — run a
plain ` + "`aveloxis refresh-views`" + ` (or wait for the next scheduler
tick) once the schema errors are resolved.

Use --no-wait to fail fast if another aveloxis migration is already in
progress (rather than blocking on the advisory lock until the holder
releases). Useful in CI and ` + "`aveloxis stop all && aveloxis start all`" + `
flows where you want a clear error if a stale process is still
running migrations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-migrate"), logger)
			if err != nil {
				return err
			}
			defer store.Close()
			// The explicit migrate command always creates/refreshes views,
			// unless --skip-views is passed.
			store.SetMatviewSkip(skipViews)
			if !skipViews {
				store.SetMatviewOnStartup(true)
			}
			store.SetMigrateNoWait(noWait)
			return store.Migrate(ctx)
		},
	}
	cmd.Flags().BoolVar(&skipViews, "skip-views", false,
		"skip materialized view creation/refresh (run `aveloxis refresh-views` separately when ready)")
	cmd.Flags().BoolVar(&noWait, "no-wait", false,
		"fail fast if another aveloxis migration is in progress (don't block on the advisory lock)")
	return cmd
}

func refreshViewsCmd(cfgPath *string) *cobra.Command {
	var aggregates bool
	cmd := &cobra.Command{
		Use:   "refresh-views",
		Short: "Refresh all materialized views (for 8Knot/analytics)",
		Long: `Refreshes all 20 materialized views used by 8Knot and other analytics tools. Views are also rebuilt automatically by aveloxis serve on a weekly schedule (default Saturday; collection.matview_rebuild_day in aveloxis.json).

--aggregates additionally rebuilds the dm_repo_* / dm_repo_group_* aggregate tables after the views — the per-repo pass the weekly rebuild runs unless collection.matview_rebuild_skip_dm_aggregates is set. It is off by default because that pass runs for hours to days at fleet scale; with the skip knob on, this flag is the ONLY way the dm_ tables update (v0.28.18).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return err
			}
			defer store.Close()
			viewErr := db.RefreshMaterializedViews(ctx, store, logger)
			if !aggregates {
				return viewErr // cobra prints it once; no second copy
			}
			if viewErr != nil {
				logger.Error("materialized view refresh reported failures — continuing to the dm_ aggregate pass; both ride the exit", "error", viewErr)
			}
			// The dm_ pass reads only commits/repos, never a matview, so a
			// stale view is no reason to skip it — with the skip knob on
			// this flag is the ONLY way the dm_ tables update. Both halves'
			// failures ride the (nonzero) exit.
			logger.Info("refresh-views --aggregates: rebuilding dm_ aggregate tables (per-repo pass; hours at fleet scale)")
			aggErr := store.RefreshAllRepoAggregates(ctx, logger)
			return errors.Join(viewErr, aggErr)
		},
	}
	cmd.Flags().BoolVar(&aggregates, "aggregates", false, "also rebuild the dm_repo_* / dm_repo_group_* aggregate tables after the views (slow at fleet scale)")
	return cmd
}

func sbomCmd(cfgPath *string) *cobra.Command {
	var (
		format string
		output string
		store  bool
	)

	cmd := &cobra.Command{
		Use:   "sbom [repo-id]",
		Short: "Generate a Software Bill of Materials for a repository",
		Long: `Generates a CycloneDX or SPDX SBOM from the dependency data collected
for a repository. The repo must have been collected with dependency/libyear
analysis enabled (runs automatically during aveloxis serve).

Examples:
  aveloxis sbom 42                         # CycloneDX JSON to stdout
  aveloxis sbom 42 --format spdx           # SPDX JSON to stdout
  aveloxis sbom 42 -o sbom.json            # Write to file
  aveloxis sbom 42 --store                 # Store in repo_sbom_scans table`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)

			repoID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid repo ID: %s", args[0])
			}

			ctx := context.Background()
			dbStore, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), bootLog)
			if err != nil {
				return err
			}
			defer dbStore.Close()

			sbomFormat := collector.FormatCycloneDX
			if format == "spdx" {
				sbomFormat = collector.FormatSPDX
			}

			data, err := collector.GenerateSBOM(ctx, dbStore, repoID, sbomFormat)
			if err != nil {
				return err
			}

			if store {
				if err := collector.StoreSBOM(ctx, dbStore, repoID, data); err != nil {
					return fmt.Errorf("storing SBOM: %w", err)
				}
				fmt.Fprintf(os.Stderr, "SBOM stored in repo_sbom_scans for repo_id %d\n", repoID)
			}

			if output != "" {
				if err := os.WriteFile(output, data, 0o644); err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "SBOM written to %s\n", output)
			} else {
				fmt.Println(string(data))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&format, "format", "cyclonedx", "Output format: cyclonedx or spdx")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Write to file instead of stdout")
	cmd.Flags().BoolVar(&store, "store", false, "Also store the SBOM in repo_sbom_scans")

	return cmd
}

func installToolsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "install-tools",
		Short: "Install all optional analysis tools (scc, scorecard, scancode, etc.)",
		Long: `Installs all optional third-party tools used by Aveloxis collection phases.
Each tool is independently optional — if not installed, its phase is silently skipped.

Requires Go for scc/scorecard. Requires Python 3.10+ for scancode.

Tools installed:
  scc        — Code complexity analysis (repo_labor)
  scorecard  — OpenSSF Scorecard security checks (repo_deps_scorecard)
  scancode   — Per-file license/copyright detection (aveloxis_scan, every 30 days)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tools := collector.ExternalTools()
			installed := 0
			failed := 0

			for _, tool := range tools {
				// Check if already installed.
				if path, err := exec.LookPath(tool.CheckBinary); err == nil {
					fmt.Printf("✓ %s already installed: %s\n", tool.Name, path)
					installed++
					continue
				}

				fmt.Printf("Installing %s — %s...\n", tool.Name, tool.Description)

				if err := collector.RunToolInstall(tool); err != nil {
					fmt.Printf("✗ Failed to install %s: %v\n  Manual install: %s\n", tool.Name, err, tool.InstallCmd)
					failed++
					continue
				}

				// Verify it's on PATH.
				if path, err := exec.LookPath(tool.CheckBinary); err == nil {
					fmt.Printf("✓ %s installed: %s\n", tool.Name, path)
				} else {
					// Don't print generic PATH advice — the InstallFunc
					// already printed tool-specific guidance if needed.
					fmt.Printf("⚠ %s installed but not found on PATH.\n", tool.Name)
				}
				installed++
			}

			fmt.Printf("\n%d/%d tools installed", installed, len(tools))
			if failed > 0 {
				fmt.Printf(", %d failed", failed)
			}
			fmt.Println()
			return nil
		},
	}
}

func webCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "web",
		Short: "Run the web GUI for group management (OAuth login)",
		Long: `Starts the web GUI where users can sign in with GitHub or GitLab,
create groups, and add repos or organizations to those groups.

Requires OAuth app credentials in aveloxis.json (web.github_client_id, etc.)
Create a GitHub OAuth app at: https://github.com/settings/developers
Create a GitLab OAuth app at: https://gitlab.com/-/profile/applications`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			webPidPath := pidfile.Path("web")
			if err := pidfile.Write(webPidPath, os.Getpid()); err != nil {
				logger.Warn("failed to write PID file — 'aveloxis stop' will fall back to pgrep", "path", webPidPath, "error", err)
			}
			defer pidfile.Remove(webPidPath)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName(componentAppName("web")), logger)
			if err != nil {
				return err
			}
			defer store.Close()
			// NOTE: web does NOT run migrations. Use `aveloxis migrate` or
			// `aveloxis serve` for that. Running migrations from both serve
			// and web simultaneously causes conflicts.
			// Instead, CheckSchemaVersion warns if the DB is behind the binary.
			store.CheckSchemaVersion(ctx, logger)

			// Load GitHub keys for immediate org scanning (non-fatal for web — it
			// can still serve the GUI without keys, just can't scan orgs).
			ghKeys, _, _ := loadKeys(ctx, cfg, store, false, logger)

			warnAPIPortMismatch(cfg, logger)

			webServer := web.New(store, cfg.Web, ghKeys, logger).
				WithMailer(mailer.New(mailerConfigFrom(cfg), logger))
			srv := &http.Server{Addr: cfg.Web.Addr, Handler: webServer.Handler()}

			go func() {
				logger.Info("web GUI listening", "addr", cfg.Web.Addr)
				if err := srv.ListenAndServe(); err != http.ErrServerClosed {
					logger.Error("web server error", "error", err)
				}
			}()

			<-ctx.Done()
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelShutdown()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				logger.Warn("web server shutdown", "error", err)
			}
			return nil
		},
	}
}

func startCmd(cfgPath *string) *cobra.Command {
	var skipDeployCheck bool
	cmd := &cobra.Command{
		Use:   "start [serve|web|api|scancode-worker|all]",
		Short: "Start aveloxis components in the background",
		Long: `Launches the specified component(s) as background processes, writing
output to log files in ~/.aveloxis/:

  aveloxis start serve            → aveloxis.log          (scheduler + monitor)
  aveloxis start web              → web.log               (web GUI)
  aveloxis start api              → api.log               (REST API)
  aveloxis start scancode-worker  → scancode-worker.log   (dedicated scancode host, v0.27.6)
  aveloxis start all              → serve + web + api (never the scancode worker)

PID files are written to ~/.aveloxis/aveloxis-{serve,web,api,scancode-worker}.pid.
Use 'aveloxis stop' to shut them down gracefully.

A dedicated scancode host runs ONLY 'aveloxis start scancode-worker' —
'start serve' there is the full scheduler (it migrates and collects)
regardless of which knobs the config carries.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			components, err := resolveComponents(args[0])
			if err != nil {
				return err
			}

			// v0.29.0: gate `start serve` / `start all` on the release's
			// manual deploy/heal steps so they can't be silently skipped
			// (there is enough data-side healing this release that a
			// missed step would go unnoticed). Fresh installs and
			// already-acknowledged releases pass through silently.
			//
			// Round-8 finding 3: the already-running check comes FIRST.
			// The gate exists to stop a NEW serve from starting against
			// an un-migrated or un-acknowledged fleet; when serve is
			// already up on this host startComponent refuses the start
			// anyway, so the gate can only produce noise — and the noise
			// is misleading: its other-serve probe (a separate,
			// untagged pool) sights the operator's OWN live serve, prints
			// both readings of a "(this host)" entry, and then
			// startComponent says "serve is already running (PID N)" two
			// lines later. `start all` still starts web and api here;
			// neither was ever gated.
			if slices.Contains(components, "serve") {
				// An UNKNOWN liveness state (round-11 finding 2) skips
				// the gate too: startComponent refuses the start a few
				// lines below with the pidfile error, so prompting the
				// operator here would only precede that refusal.
				_, serveUp, livenessErr := componentAlreadyRunning("serve")
				if !serveUp && livenessErr == nil {
					proceed, err := runDeployGate(*cfgPath, skipDeployCheck)
					if err != nil {
						return fmt.Errorf("deploy-readiness check: %w", err)
					}
					if !proceed {
						return errors.New(startAbortMessage(db.ToolVersion))
					}
				}
			}

			return startComponents(components, *cfgPath, startComponent)
		},
	}
	cmd.Flags().BoolVar(&skipDeployCheck, "skip-deploy-check", false, "bypass the release deploy-steps prompt (for automation)")
	return cmd
}

// startComponents starts every component in order and returns the
// failures JOINED, so the exit status is nonzero whenever any start was
// refused while the components that CAN start still do.
//
// Round 16 (Copilot round 6, finding 1): the loop used to print
// "Failed to start %s" and return nil, so `aveloxis start serve` with
// an unreadable pidfile — the round-11 finding-2 refusal, the ONLY hard
// double-start guard — exited 0 having started nothing, and a deploy
// script read that as success. Every failure is reported (the
// v0.27.106 nonzero-exit convention); every component is still
// attempted, because `start all` starts web and api even when serve is
// already up (round-8 finding 3) and a refused serve must not take
// them down with it. A no-op start ("already running") returns nil from
// the starter and stays exit 0. The starter is injected so the loop's
// contract is driven by a test without spawning anything.
func startComponents(components []string, cfgPath string, start func(component, cfgPath string) error) error {
	var errs []error
	for _, comp := range components {
		if err := start(comp, cfgPath); err != nil {
			errs = append(errs, fmt.Errorf("failed to start %s: %w", comp, err))
		}
	}
	return errors.Join(errs...)
}

// componentAlreadyRunning is the ONE spelling (SR-17) of "this host is
// already running that component": a pidfile whose PID is live. It
// returns the live PID so a caller that reports it needs no second
// read. Four readers depend on the same answer — startComponent's
// refusal to double-start, `stop all`'s hint about the worker it left
// running, run-scorecard's refusal to compete with a live serve for the
// API budget, and (round-8 finding 3) `start serve`'s decision to skip
// the deploy gate — and a start that is a no-op must look like a no-op
// to all of them.
//
// THREE-VALUED, not two (round-11 finding 2, SR-5: a lookup ERROR is
// not "no"). Only ENOENT is a definitive "not running"; pidfile.Read
// also returns errors for EACCES, EIO and a corrupt or truncated file
// ("invalid PID in %s"), and through v0.29.4 all of those collapsed
// into not-running. A LIVE serve with an unreadable pidfile therefore
// read as stopped — and startComponent, the only HARD double-start
// guard (the other-serve probe warns, never blocks — a documented
// residual), launched a second scheduler against the same queue and API
// keys. A stale pidfile (readable, PID dead) stays a definitive
// not-running: that is what the liveness check is for.
func componentAlreadyRunning(component string) (int, bool, error) {
	_, pid, found, err := readComponentPID(component)
	if err != nil || !found {
		return 0, false, err
	}
	if !pidfile.IsRunning(pid) {
		return 0, false, nil
	}
	return pid, true, nil
}

// readComponentPID is the ONE spelling (SR-17) of the three-valued
// pidfile read every reader in this file depends on: (path, pid,
// found, err). ENOENT is the only definitive "no pidfile" —
// (path, 0, false, nil). Any other read error (EACCES, EIO, a corrupt
// or truncated file) is UNKNOWN and comes back as the error so the
// caller fails closed (SR-5). Round 17 (Copilot round 7, finding 2)
// moved it here: stopComponent had kept its own `err == nil` spelling
// after round 11 fixed componentAlreadyRunning, so a corrupt pidfile
// was silently dropped — the pgrep fallback usually still found and
// stopped the process, but the file was never reported and the next
// `start` refused on it with nothing in the operator's history to say
// why. The class fixed in round 11, one function down.
func readComponentPID(component string) (string, int, bool, error) {
	path := pidfile.Path(component)
	pid, err := pidfile.Read(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return path, 0, false, nil
		}
		return path, 0, false, fmt.Errorf("cannot determine whether %s is running on this host from %s "+
			"(an unreadable or corrupt pidfile is not evidence that it is stopped): %w", component, path, err)
	}
	return path, pid, true, nil
}

func startComponent(component, cfgPath string) error {
	// Check if already running.
	pidPath := pidfile.Path(component)
	pid, running, err := componentAlreadyRunning(component)
	if err != nil {
		// Round-11 finding 2: refuse rather than double-start. This is
		// the only HARD guard against two schedulers on one host.
		return fmt.Errorf("refusing to start %s: %w", component, err)
	}
	if running {
		fmt.Printf("%s is already running (PID %d)\n", component, pid)
		return nil
	}

	logPath := pidfile.LogPath(component)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("opening log file %s: %w", logPath, err)
	}

	// Build the command. Pass through the config path flag.
	execPath, err := os.Executable()
	if err != nil {
		logFile.Close()
		return fmt.Errorf("finding executable: %w", err)
	}

	cmdArgs := []string{component, "--config", cfgPath}
	proc := exec.Command(execPath, cmdArgs...)
	proc.Stdout = logFile
	proc.Stderr = logFile
	// Detach from the parent process group so it survives terminal close.
	proc.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := proc.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("starting %s: %w", component, err)
	}

	pid = proc.Process.Pid
	if err := pidfile.Write(pidPath, pid); err != nil {
		fmt.Printf("Warning: started %s (PID %d) but failed to write PID file: %v\n", component, pid, err)
	}

	// Release the child — we don't wait for it.
	_ = proc.Process.Release()
	logFile.Close()

	fmt.Printf("Started %s (PID %d), logging to %s\n", component, pid, logPath)
	return nil
}

// backendVerifier owns the ONE database connection `aveloxis stop` uses
// for its post-SIGTERM checks. Round-11 finding 10: the per-component
// shape opened a fresh pool (MinConns=2, MaxConns=20, plus a Ping) for
// each of serve/web/api, three times over, precisely on the path an
// operator runs when the database is already at max_connections.
//
// Opened LAZILY on the first component actually stopped, so a `stop`
// that finds nothing running never touches the database, and closed
// once. A config that will not load or a dial that fails is reported
// ONCE and then remembered (round-2 finding 9 kept: the skip is said
// out loud, it is just no longer said three times).
type backendVerifier struct {
	out     io.Writer
	cfgPath string
	cfg     *config.Config
	store   *db.PostgresStore
	opened  bool
	broken  bool
}

func (v *backendVerifier) verify(appName string) {
	if v.broken {
		return
	}
	if !v.opened {
		v.opened = true
		bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		cfg, err := config.Load(v.cfgPath)
		if err != nil {
			// No config means we can't check the DB. Operator gets the
			// SIGTERM result but no verification — said out loud
			// (round-2 finding 9: a `stop` run without -c on a
			// dedicated host used to skip the check in silence).
			fmt.Fprintf(v.out, "(config %s not loaded — backend verification skipped: %v)\n", v.cfgPath, err)
			v.broken = true
			return
		}
		// The dial gets its own bound so a slow connect can never eat
		// into the poll window (pass 41 — the pass-40 shared ctx let a
		// >5s dial reintroduce the mid-poll expiry it was fixing).
		dialCtx, dialCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dialCancel()
		store, err := db.NewPostgresStore(dialCtx, cfg.Database.ConnectionString(), bootLog)
		if err != nil {
			// Generic on purpose: the verifier is opened lazily on the
			// FIRST component stopped and the failure is reported once,
			// so naming that component would let an operator running
			// `stop all` read the other two as verified.
			fmt.Fprintf(v.out, "(database connection failed — backend verification skipped for every component of this stop: %v)\n", err)
			v.broken = true
			return
		}
		v.cfg, v.store = cfg, store
	}
	verifyBackendsDisconnected(v.out, v.store, v.cfg, appName)
}

// Close releases the shared pool. Safe on a verifier that never opened.
func (v *backendVerifier) Close() {
	if v.store != nil {
		v.store.Close()
		v.store = nil
	}
}

// verifyBackendsDisconnected polls pg_stat_activity for THIS host's
// backends with the given application_name (e.g., "aveloxis-serve")
// and waits up to the serve shutdown budget + margin for them to
// disappear. If any persist, prints the persistent PIDs paired with a
// pg_terminate_backend recipe so the operator can act in seconds
// rather than wait the full TCP keepalive timeout (tens of minutes).
// Backends carrying the same tag from OTHER hosts are reported as such
// and never offered for termination (v0.29.4 — the 2026-09-09
// incident printed 64 recipes for the primary's pool from a dedicated
// scancode host).
//
// v0.20.0 introduced this to close the gap that produced the
// 2026-05-08 26-minute orphan: SIGTERM was sent successfully, but the
// orphaned backend kept grinding a 26-minute UPDATE because the
// operator had no signal it was happening.
//
// The store is supplied by the caller (round-11 finding 10): `stop all`
// verifies three components and used to open a pool per component —
// MinConns=2, MaxConns=20 and a Ping each — on the path most often run
// when the database is at max_connections. backendVerifier opens one.
func verifyBackendsDisconnected(out io.Writer, store *db.PostgresStore, cfg *config.Config, appName string) {
	// Poll once a second for the serve's full shutdown budget plus a
	// margin (pass 39: a serve legitimately inside its bookkeeping wait
	// used to be reported as orphaned backends at 30s). The poll ctx is
	// sized FROM the budget — pass 40: a fixed 35s ctx expired mid-poll,
	// the query error's silent return skipped the WARNING below, and a
	// genuine orphan was never reported.
	budget := shutdownBudget(cfg) + 30*time.Second
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget+5*time.Second)
	defer cancel()
	// v0.29.4: the poll is scoped to THIS host's backends and lives in
	// pollBackends (its exit rule is pinned behaviorally). The
	// 2026-09-09 incident: a stop on a second host matched the tag
	// alone, waited the whole budget on the primary's 64 pool
	// backends, and printed a terminate recipe for each of them.
	last, render := pollBackends(
		func() (db.AppNameBackends, error) { return store.BackendsByAppName(ctx, appName) },
		budget,
		func() time.Duration { return time.Since(start) },
		func() { time.Sleep(1 * time.Second) },
		out, appName)
	if !render {
		return
	}
	// Persistent local backends past the FULL budget get the PIDs and
	// the actionable fix; other addresses' backends are only reported.
	printBackendVerdict(out, appName, budget, last)
}

func stopCmd(cfgPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [serve|web|api|scancode-worker|all]",
		Short: "Stop running aveloxis background processes",
		Long: `Sends SIGTERM to the specified component(s), triggering graceful shutdown.

  aveloxis stop serve            — stop the scheduler
  aveloxis stop web              — stop the web GUI
  aveloxis stop api              — stop the REST API
  aveloxis stop scancode-worker  — stop the dedicated scancode worker
  aveloxis stop all              — stop serve + web + api (never the scancode worker)
  aveloxis stop                  — (no args) same as 'all'

Active workers finish their current API call, queue locks are released,
and any unprocessed staging data is preserved for the next startup.
PID files are removed after a successful stop or when they are stale; a
file the command could not read, or whose process it could not signal,
is left in place for you to inspect. After SIGTERM, the command
watches pg_stat_activity for THIS host's backends of the component;
backends of the same component from other hosts (the primary, seen from
a dedicated scancode host) are reported and left alone.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := "all"
			if len(args) > 0 {
				target = args[0]
			}
			components, err := resolveComponents(target)
			if err != nil {
				return err
			}

			verifier := &backendVerifier{out: os.Stdout, cfgPath: *cfgPath}
			defer verifier.Close()

			stopped, stopErr := stopComponents(components, stopComponent, func(comp string) {
				// v0.20.0: poll pg_stat_activity for the matching
				// application_name and warn if backends linger past
				// the shutdown budget + margin (pass 39). Surfaces
				// orphans-after-stop without requiring the operator
				// to know about pg_locks. v0.29.4: this host's only.
				//
				// The PREFIX, not the tag: since round 12 the tag
				// carries this host's marker after '@' and every
				// host's differs, so matching on the full tag would
				// find only our own and silently drop the
				// other-hosts count this command exists to report.
				// The prefix is also what the operator-facing
				// strings should name — it is the component.
				verifier.verify(componentAppNamePrefix(comp))
			})
			if nothingRunning(stopped, stopErr) {
				fmt.Println("No running aveloxis processes found.")
			}
			if isAllTarget(target) {
				stopAllHint(os.Stdout, func(component string) bool {
					// The hint is informational; an unknown liveness
					// state (round-11 finding 2) claims nothing.
					_, running, err := componentAlreadyRunning(component)
					return err == nil && running
				})
			}
			return stopErr
		},
	}
	return cmd
}

// stopComponents stops every component in order, runs onStopped for
// each one that actually went down, and returns the count beside the
// failures JOINED. The L11 class sweep of round 16 finding 1: a
// SIGTERM that failed (EPERM on a process owned by another user is the
// ordinary shape) printed "Failed to stop" and the command still exited
// 0 — and, with nothing else stopped, printed "No running aveloxis
// processes found." over a process it had just failed to signal. A
// clean "nothing to stop" is still exit 0 (stop is idempotent); a
// signal failure is not. Every component is still attempted.
func stopComponents(components []string, stop func(component string) (bool, error), onStopped func(component string)) (int, error) {
	stopped := 0
	var errs []error
	for _, comp := range components {
		ok, err := stop(comp)
		switch {
		case err != nil && ok:
			// Stopped (via the pgrep fallback, or one of several pids)
			// but with something the operator must hear about — a
			// corrupt pidfile left in place, a second pid refused. The
			// exit is nonzero; the words must not contradict the
			// "Stopped …" line printed a moment earlier (round 17 L10).
			errs = append(errs, fmt.Errorf("%s stopped, but: %w", comp, err))
		case err != nil:
			// Nothing went down. The inner error already says what
			// happened (a refused signal, an unreadable pidfile, a
			// pgrep failure) — "failed to stop" would be the wrong verb
			// for a corrupt pidfile with nothing else found, so the
			// component name is the only prefix (round 17 L10 pass 2).
			errs = append(errs, fmt.Errorf("%s: %w", comp, err))
		}
		if ok {
			stopped++
			onStopped(comp)
		}
	}
	return stopped, errors.Join(errs...)
}

// nothingRunning is the ONE verdict behind "No running aveloxis
// processes found.": nothing went down AND nothing went wrong. A
// refused signal, an unreadable pidfile or a failed pgrep is not
// "nothing running" — printing that line over a process the command
// just failed to signal is the round-16 incident shape, and this
// predicate is the pin the round-16 fix lacked (round 17 L10 pass 3).
func nothingRunning(stopped int, err error) bool {
	return stopped == 0 && err == nil
}

// stopComponent signals the component's process(es) and reports
// (stopped, err). "Nothing running" is (false, nil) — stop is
// idempotent; a process that was FOUND but could not be signaled is an
// error (round 16, the L11 sweep of finding 1), and its pidfile is left
// in place because the process is still there.
func stopComponent(component string) (bool, error) {
	var errs []error
	// Strategy 1: PID file (preferred — reliable, written by start/serve/web/api).
	// The read is three-valued (readComponentPID): an unreadable or
	// corrupt pidfile is an ERROR that rides the return value — the
	// pgrep fallback still runs (and usually finds the process, since
	// startComponent execs `<binary> <component> --config …`), but the
	// file is reported and left in place (it is neither stale nor live)
	// so the operator knows why the next start will refuse on it.
	pidPath, pid, found, err := readComponentPID(component)
	attempted := 0 // the pidfile's pid, so the pgrep arm never signals it twice
	switch {
	case err != nil:
		errs = append(errs, fmt.Errorf("pidfile left in place — inspect and delete it by hand before the next start: %w", err))
	case !found:
		// no pidfile — fall through to pgrep
	case !pidfile.IsRunning(pid):
		fmt.Printf("%s: stale PID file (PID %d not running), cleaning up\n", component, pid)
		pidfile.Remove(pidPath)
	default:
		attempted = pid
		if serr := signalProcess(component, pid); serr != nil {
			errs = append(errs, serr)
		} else {
			pidfile.Remove(pidPath)
			return true, nil
		}
	}

	// Strategy 2: pgrep fallback — finds processes started before PID file support
	// was added, or started manually without 'aveloxis start'. pgrep's
	// exit status 1 is its documented "no processes matched" — a
	// definitive no; any other failure (2 = usage, 3 = fatal, or a
	// missing binary) is not evidence of absence (SR-5).
	out, err := exec.Command("pgrep", "-f", "aveloxis "+component).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			errs = append(errs, fmt.Errorf("pgrep for %s: %w", component, err))
		}
		return false, errors.Join(errs...)
	}

	myPID := os.Getpid()
	stopped := false
	for field := range strings.FieldsSeq(strings.TrimSpace(string(out))) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid == myPID || pid == attempted {
			continue
		}
		if serr := signalProcess(component, pid); serr != nil {
			errs = append(errs, serr)
		} else {
			stopped = true
		}
	}
	return stopped, errors.Join(errs...)
}

// sendSignal delivers one signal to one PID. It is a seam so the
// kernel's refusal (EPERM on another user's process) can be driven in
// a test without signaling a real process — the round-16 test wrote a
// pidfile naming PID 1, which in a rootless container is the
// container's own init under the same uid (round 17, Copilot round 7,
// finding 1). The production default is the real os.Process path and
// is pinned by TestSendSignalProductionDefaultDeliversRealSignals;
// nothing outside a test may reassign it.
var sendSignal = func(pid int, sig syscall.Signal) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Signal(sig)
}

// signalProcess sends SIGTERM to one process. A failed signal is
// returned, never swallowed: the process is still running and the
// caller's exit status has to say so.
func signalProcess(component string, pid int) error {
	if err := sendSignal(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signaling %s (PID %d): %w", component, pid, err)
	}
	fmt.Printf("Stopped %s (PID %d)\n", component, pid)
	return nil
}

// testMailCmd lets operators verify Gmail SMTP credentials
// without waiting for a new user to sign up. v0.20.14 — added
// after a production diagnostic where the first user signup hit
// `535 5.7.8 Username and Password not accepted` because the
// configured gmail_user wasn't an email address and the password
// wasn't an App Password. Running this once at deploy time
// surfaces both kinds of mistakes immediately.
func testMailCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "test-mail <recipient>",
		Short: "Send a test email to verify Gmail SMTP credentials",
		Long: `Reads the mail block from aveloxis.json, runs the same
ValidateConfig that aveloxis web uses at startup, and attempts a
single test send to the supplied recipient. Useful immediately
after configuring Gmail to confirm credentials work before the
first user signs up.

Common failures and what they mean:
  - "mail.gmail_user is not an email address" — gmail_user must
    be a full address (you@gmail.com or you@yourdomain.com),
    not a bare domain.
  - "Google App Passwords are exactly 16 lowercase letters" —
    you pasted a regular password. Generate an App Password at
    https://myaccount.google.com/apppasswords (2-Step
    Verification must be on first).
  - "535 5.7.8 Username and Password not accepted" from Gmail
    itself — credentials are syntactically valid but Gmail
    rejected them. Most likely an App Password from the wrong
    account, or 2-Step Verification was just disabled.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			recipient := args[0]
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			mc := mailer.Config{
				GmailUser:        cfg.Mail.GmailUser,
				GmailAppPassword: cfg.Mail.GmailAppPassword,
				FromName:         cfg.Mail.FromName,
				SiteURL:          cfg.Mail.SiteURL,
			}
			if err := mailer.ValidateConfig(mc); err != nil {
				return fmt.Errorf("mail config invalid — fix aveloxis.json and try again: %w", err)
			}
			m := mailer.New(mc, logger)
			logger.Info("sending test email", "to", recipient, "from", mc.GmailUser)
			if err := m.Send(recipient, "Aveloxis SMTP test",
				"This is a test email from `aveloxis test-mail`.\n\n"+
					"If you received this, your Gmail SMTP credentials are working.\n"+
					"— Aveloxis"); err != nil {
				return fmt.Errorf("send failed: %w", err)
			}
			logger.Info("test email sent successfully", "to", recipient)
			return nil
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run:   func(cmd *cobra.Command, args []string) { fmt.Println("aveloxis v" + Version) },
	}
}

// --- helpers ---

func loadConfig(cfgPath string, logger *slog.Logger) *config.Config {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Warn("config file not found, using defaults", "path", cfgPath, "error", err)
		cfg = config.DefaultConfig()
	}
	return cfg
}

// newLogger creates a logger from the config's log_level setting.
func newLogger(cfg *config.Config) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.SlogLevel()}))
}

// loadKeys builds key pools. Priority order:
//  1. aveloxis_ops.worker_oauth (always checked)
//  2. augur_operations.worker_oauth (if --augur-keys is set)
//  3. JSON config file (lowest priority, for standalone deployments)
func loadKeys(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys bool, logger *slog.Logger) (*platform.KeyPool, *platform.KeyPool, error) {
	ghTokens := cfg.GitHub.APIKeys
	glTokens := cfg.GitLab.APIKeys

	// Load from database (aveloxis_ops first, augur_operations as fallback).
	if dbGH, err := db.LoadAPIKeys(ctx, store.Pool(), "github", useAugurKeys); err != nil {
		logger.Error("failed to load GitHub API keys from database", "error", err)
	} else if len(dbGH) > 0 {
		logger.Info("loaded GitHub keys from database", "count", len(dbGH))
		ghTokens = append(ghTokens, dbGH...)
	}
	if dbGL, err := db.LoadAPIKeys(ctx, store.Pool(), "gitlab", useAugurKeys); err != nil {
		logger.Error("failed to load GitLab API keys from database", "error", err)
	} else if len(dbGL) > 0 {
		logger.Info("loaded GitLab keys from database", "count", len(dbGL))
		glTokens = append(glTokens, dbGL...)
	}

	if len(ghTokens) == 0 && len(glTokens) == 0 {
		return nil, nil, fmt.Errorf("no API keys configured for any platform — add keys via 'aveloxis add-key <token> --platform github' or store them in the database. Collection is impossible without API keys")
	}
	if len(ghTokens) == 0 {
		logger.Warn("no GitHub API keys configured — GitHub repos will not be collected")
	}
	if len(glTokens) == 0 {
		logger.Warn("no GitLab API keys configured — GitLab repos will not be collected")
	}

	gh := platform.NewKeyPool(ghTokens, logger)
	gl := platform.NewKeyPool(glTokens, logger)
	// 2026-09-12 admission control: the pool is the single authority for
	// every forge constraint (per-key and pool-wide in-flight ceilings,
	// the foreground budget reservation). The accessors are the one
	// default layer (SR-10); log the EFFECTIVE values at the point of use.
	// GitLab shares the same shape — its limits are lower, so the GitHub
	// ceilings are a conservative backstop there, not a tuned value.
	maxInflight := cfg.Collection.GitHubMaxInflightValue()
	maxPerKey := cfg.Collection.GitHubMaxInflightPerKeyValue()
	reservePct := cfg.Collection.GitHubBudgetForegroundReservePctValue()
	gh.SetAdmission(maxInflight, maxPerKey, reservePct)
	gl.SetAdmission(maxInflight, maxPerKey, reservePct)
	logger.Info("API key pool admission",
		"github_keys", len(ghTokens), "gitlab_keys", len(glTokens),
		"max_inflight", maxInflight, "max_inflight_per_key", maxPerKey,
		"foreground_reserve_pct", reservePct)
	return gh, gl, nil
}

// digestMailerAdapter bridges *mailer.Mailer to the scheduler's
// digestMailer role interface (v0.27.12). The mailer package stays
// free of aveloxis imports, so the db→mailer item copy happens here.
// mailerConfigFrom maps the aveloxis.json mail block onto the mailer
// package's dependency-free Config (v0.27.20: single builder so every
// process — serve digest, web, api — carries the same fields,
// including OperatorEmail for add-request notifications).
func mailerConfigFrom(cfg *config.Config) mailer.Config {
	return mailer.Config{
		GmailUser:        cfg.Mail.GmailUser,
		GmailAppPassword: cfg.Mail.GmailAppPassword,
		FromName:         cfg.Mail.FromName,
		SiteURL:          cfg.Mail.SiteURL,
		OperatorEmail:    cfg.Mail.OperatorEmail,
	}
}

type digestMailerAdapter struct{ m *mailer.Mailer }

func (a digestMailerAdapter) SendVulnerabilityDigest(to string, since time.Time, items []db.VulnDigestItem) error {
	conv := make([]mailer.VulnDigestItem, len(items))
	for i, it := range items {
		conv[i] = mailer.VulnDigestItem{
			RepoOwner:   it.RepoOwner,
			RepoName:    it.RepoName,
			VulnID:      it.VulnID,
			Severity:    it.Severity,
			PackagePurl: it.PackagePurl,
			Summary:     it.Summary,
		}
	}
	return a.m.SendVulnerabilityDigest(to, since, conv)
}
