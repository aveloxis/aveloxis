// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.27.5 — `aveloxis run-scorecard`: the bulk remote-primary OpenSSF
// Scorecard catch-up pass. Production has 86,504 repos with local-mode
// 11-check data and only 49 with the full remote 18-check set; the
// per-cycle collection phase upgrades repos as they come due, but at
// the default recollect cadence that takes weeks — this command walks
// the whole collected fleet NOW, oldest scorecard first.
//
// GitHub-only by design: remote mode (--repo, ~18 checks, ~40 API
// calls/repo measured) is what the pass exists to run, and scorecard's
// GitLab remote support is immature. GitLab/generic repos keep getting
// local-mode scans from the per-cycle phase. There is no analysis
// clone here, so no local backstop: a failed remote attempt is counted
// and skipped (the repo's next collection cycle retries).
//
// Refuses to start while `aveloxis serve` runs on this host (the pass
// would compete with the collection workers for the shared API budget)
// and while another run-scorecard holds its own pidfile (two passes
// would double-scan the same backlog).

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/pidfile"
	"github.com/spf13/cobra"
)

func runScorecardCmd(cfgPath *string) *cobra.Command {
	var (
		workers       int
		olderThanDays int
		limit         int
	)

	defaultWorkers := runtime.NumCPU() / 2
	if defaultWorkers < 1 {
		defaultWorkers = 1
	}

	cmd := &cobra.Command{
		Use:   "run-scorecard",
		Short: "Bulk remote-primary OpenSSF Scorecard pass over already-collected GitHub repos",
		Long: `Walks every non-archived, at-least-once-collected GitHub repo and runs
OpenSSF Scorecard in remote mode (--repo, the full ~18-check set),
oldest-scorecard-first (never-scanned repos first). Uses the same
invoke/persist code as the per-cycle collection phase, including the
comma-separated multi-token GITHUB_TOKEN and the per-attempt wall-clock
timeout (collection.scorecard_timeout_minutes).

Refuses to start while 'aveloxis serve' is running on this host, and
while another run-scorecard is in progress.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRunScorecard(*cfgPath, workers, olderThanDays, limit)
		},
	}

	cmd.Flags().IntVar(&workers, "workers", defaultWorkers,
		"concurrent scorecard invocations (default NumCPU/2, hard-capped at NumCPU)")
	cmd.Flags().IntVar(&olderThanDays, "older-than", 0,
		"only repos whose latest scorecard run is older than N days (0 = every repo)")
	cmd.Flags().IntVar(&limit, "limit", 0,
		"cap the number of repos this run (0 = all) — use for canary runs")
	return cmd
}

// refuseIfServeRunning errors when an `aveloxis serve` process is alive
// on this host, per its pidfile + process-liveness check.
func refuseIfServeRunning() error {
	path := pidfile.Path("serve")
	pid, err := pidfile.Read(path)
	if err != nil {
		return nil // no pidfile = no serve
	}
	if pidfile.IsRunning(pid) {
		return fmt.Errorf("refusing to start: aveloxis serve is running on this host (pid %d, pidfile %s). "+
			"run-scorecard would compete with the collection workers for the shared GitHub API budget; "+
			"stop it first: aveloxis stop serve", pid, path)
	}
	return nil
}

// acquireRunScorecardPidfile writes our own pidfile so two bulk passes
// can't overlap, returning the release func. Errors when another live
// run-scorecard already holds it.
func acquireRunScorecardPidfile() (func(), error) {
	path := pidfile.Path("run-scorecard")
	if pid, err := pidfile.Read(path); err == nil && pidfile.IsRunning(pid) {
		return nil, fmt.Errorf("refusing to start: another `aveloxis run-scorecard` is already running (pid %d, pidfile %s). "+
			"Two bulk passes would double-scan the same backlog; wait for it to finish or stop it first", pid, path)
	}
	if err := pidfile.Write(path, os.Getpid()); err != nil {
		return nil, fmt.Errorf("writing pidfile %s: %w", path, err)
	}
	return func() { pidfile.Remove(path) }, nil
}

func runRunScorecard(cfgPath string, workers, olderThanDays, limit int) error {
	if err := refuseIfServeRunning(); err != nil {
		return err
	}
	release, err := acquireRunScorecardPidfile()
	if err != nil {
		return err
	}
	defer release()

	// Hard cap at NumCPU: each invocation is one scorecard subprocess
	// (which spawns its own git clone); oversubscribing cores just
	// makes every attempt slower and closer to its wall-clock timeout.
	if workers > runtime.NumCPU() {
		workers = runtime.NumCPU()
	}
	if workers < 1 {
		workers = 1
	}

	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here —
	// schema currency is serve/migrate's job. If the scorecard_mode
	// column is missing, the resulting Postgres error is
	// self-describing; run `aveloxis migrate` first.
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-run-scorecard"), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	ghKeys, _, err := loadKeys(ctx, cfg, store, false, logger)
	if err != nil {
		return fmt.Errorf("loading API keys: %w", err)
	}
	token, instrumentToken := collector.ScorecardTokens(ghKeys, cfg.Collection.ScorecardTokenCountOrDefault())
	if token == "" {
		return fmt.Errorf("no GitHub API keys loaded — remote scorecard needs GITHUB_TOKEN; add keys via `aveloxis add-key <token> --platform github`")
	}

	backlog, err := store.ListScorecardBacklog(ctx, olderThanDays, limit)
	if err != nil {
		return fmt.Errorf("listing scorecard backlog: %w", err)
	}
	total := len(backlog)
	fmt.Printf("run-scorecard: %d repos in backlog (workers=%d, older-than=%dd, limit=%d, timeout=%s/attempt)\n",
		total, workers, olderThanDays, limit, cfg.Collection.ScorecardTimeout())
	if total == 0 {
		return nil
	}

	var (
		done     atomic.Int64
		failed   atomic.Int64
		apiCalls atomic.Int64
		start    = time.Now()
	)

	jobs := make(chan db.ScorecardBacklogRepo)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range jobs {
				repoURL := fmt.Sprintf("https://github.com/%s/%s", r.Owner, r.Name)
				// Same shared invoke/persist path as the per-cycle
				// phase. No analysis clone exists here → remote only:
				// LocalPath stays empty, so a failed remote attempt
				// surfaces as an error (recorded + skipped) instead of
				// falling back.
				res, scErr := collector.RunScorecard(ctx, store, r.RepoID, collector.ScorecardOptions{
					RepoURL:         repoURL,
					RemotePrimary:   true,
					Timeout:         cfg.Collection.ScorecardTimeout(),
					GithubToken:     token,
					InstrumentToken: instrumentToken,
				}, logger)
				if scErr != nil {
					failed.Add(1)
					logger.Warn("run-scorecard: repo failed (skipped — next collection cycle retries)",
						"repo_id", r.RepoID, "repo", r.Owner+"/"+r.Name, "error", scErr)
				} else if res != nil && res.APICalls > 0 {
					apiCalls.Add(res.APICalls)
				}

				if d := done.Add(1); d%100 == 0 {
					elapsed := time.Since(start)
					perRepo := elapsed / time.Duration(d)
					eta := time.Duration(int64(total)-d) * perRepo
					fmt.Printf("run-scorecard: %d/%d done (failed=%d), api_calls_used=%d, elapsed=%s, eta=%s\n",
						d, total, failed.Load(), apiCalls.Load(),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
			}
		}()
	}

	for _, r := range backlog {
		select {
		case jobs <- r:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	fmt.Printf("run-scorecard: complete — done=%d/%d failed=%d api_calls_used=%d elapsed=%s\n",
		done.Load(), total, failed.Load(), apiCalls.Load(), time.Since(start).Round(time.Second))
	if ctx.Err() != nil {
		return fmt.Errorf("interrupted after %d/%d repos — re-run to continue (oldest-first ordering makes the pass resumable)", done.Load(), total)
	}
	return nil
}
