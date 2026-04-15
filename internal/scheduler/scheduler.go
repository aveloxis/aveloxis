// Package scheduler runs continuous collection backed by a Postgres priority queue.
//
// Design goals (contrast with Augur's Celery-based scheduler):
//   - Deterministic ordering: repos are collected in strict priority order.
//   - Priority boost: any repo can be pushed to the top via API or CLI at any time.
//   - Transparent: queue state lives in Postgres, queryable with plain SQL.
//   - Durable: survives restarts. No Celery, no RabbitMQ, no Redis.
//   - Scalable: multiple Aveloxis instances can share the same queue via SKIP LOCKED.
//   - Stale lock recovery: crashed workers' jobs are automatically re-queued.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/collector"
	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Config configures the scheduler.
type Config struct {
	Workers          int           // concurrent collection goroutines (default 1)
	PollInterval     time.Duration // how often to check for due jobs (default 10s)
	RecollectAfter   time.Duration // how long before re-collecting a repo (default 24h)
	StaleLockTimeout time.Duration // how long before reclaiming a locked job (default 1h)
	RepoCloneDir         string        // directory for bare git clones (can be terabytes)
	OrgRefreshInterval   time.Duration // how often to re-scan orgs for new/renamed repos (default 4h)
	MatviewRebuildDay    int           // day of week for matview rebuild (0=Sun..6=Sat, -1=disabled)
	ForceFullCollection  bool          // when true, all collections use since=zero (full re-collection)
}

// Scheduler polls the Postgres-backed queue and dispatches collection workers.
type Scheduler struct {
	store    *db.PostgresStore
	ghClient platform.Client
	glClient platform.Client
	ghKeys   *platform.KeyPool
	logger   *slog.Logger
	cfg      Config
	workerID string
}

// New creates a scheduler.
func New(store *db.PostgresStore, ghClient, glClient platform.Client, logger *slog.Logger, cfg Config) *Scheduler {
	return NewWithKeys(store, ghClient, glClient, nil, logger, cfg)
}

// NewWithKeys creates a scheduler with access to the GitHub key pool for commit resolution.
func NewWithKeys(store *db.PostgresStore, ghClient, glClient platform.Client, ghKeys *platform.KeyPool, logger *slog.Logger, cfg Config) *Scheduler {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.RecollectAfter == 0 {
		cfg.RecollectAfter = 24 * time.Hour
	}
	if cfg.StaleLockTimeout == 0 {
		cfg.StaleLockTimeout = 1 * time.Hour
	}
	if cfg.OrgRefreshInterval == 0 {
		cfg.OrgRefreshInterval = 4 * time.Hour
	}

	hostname, _ := os.Hostname()
	workerID := hostname + "-" + time.Now().Format("150405")

	return &Scheduler{
		store:    store,
		ghClient: ghClient,
		glClient: glClient,
		ghKeys:   ghKeys,
		logger:   logger,
		cfg:      cfg,
		workerID: workerID,
	}
}

// Run starts the scheduler loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	s.logger.Info("scheduler started",
		"workers", s.cfg.Workers,
		"poll_interval", s.cfg.PollInterval,
		"recollect_after", s.cfg.RecollectAfter,
		"worker_id", s.workerID,
		"force_full_collection", s.cfg.ForceFullCollection,
	)
	if s.cfg.ForceFullCollection {
		s.logger.Warn("FORCE FULL COLLECTION enabled — all repos will be fully re-collected. Set collection.force_full to false in aveloxis.json after this pass completes.")
	}

	// On startup: check for tool updates (monthly), process leftover staging,
	// and release stale locks.
	collector.CheckAndUpdateTools(s.logger)
	s.processLeftoverStaging(ctx)

	// Immediately reclaim all locks held by dead worker IDs. A fresh process
	// cannot have any legitimate in-flight work, so all locks from other
	// worker IDs are definitively stale — no need to wait for the 1-hour
	// timeout. This fixes repos stuck in 'collecting' after a restart.
	if recovered, err := s.store.RecoverOtherWorkerLocks(ctx, s.workerID); err != nil {
		s.logger.Error("failed to recover other workers' locks", "error", err)
	} else if recovered > 0 {
		s.logger.Warn("recovered stale locks from previous process on startup",
			"count", recovered, "current_worker", s.workerID)
	}

	s.recoverStale(ctx)
	s.releaseOurLocks(ctx)

	sem := make(chan struct{}, s.cfg.Workers)

	pollTicker := time.NewTicker(s.cfg.PollInterval)
	defer pollTicker.Stop()

	recoveryTicker := time.NewTicker(5 * time.Minute)
	defer recoveryTicker.Stop()

	orgRefreshTicker := time.NewTicker(s.cfg.OrgRefreshInterval)
	defer orgRefreshTicker.Stop()
	// Run org refresh once on startup too.
	go s.refreshOrgs(ctx)

	// Contributor breadth: run every 6 hours to discover cross-repo activity.
	breadthTicker := time.NewTicker(6 * time.Hour)
	defer breadthTicker.Stop()

	// Materialized view rebuild: check hourly, run on Saturdays.
	// Collection is suspended during the rebuild.
	matviewCheckTicker := time.NewTicker(1 * time.Hour)
	defer matviewCheckTicker.Stop()
	var lastMatviewRebuild time.Time

	// Immediately fill worker slots on startup instead of waiting for the
	// first poll tick (default 10s). With 30 workers and 78 queued repos,
	// this avoids a visible delay before collection begins.
	s.fillWorkerSlots(ctx, sem)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopping, waiting for workers to finish")
			// Drain semaphore to wait for active workers.
			for range s.cfg.Workers {
				sem <- struct{}{}
			}
			// Release all our locks so repos go back to queued immediately
			// instead of waiting for stale lock timeout.
			s.releaseOurLocks(context.Background())
			s.logger.Info("scheduler stopped, locks released")
			return

		case <-recoveryTicker.C:
			s.recoverStale(ctx)

		case <-orgRefreshTicker.C:
			go s.refreshOrgs(ctx)
			go s.refreshUserOrgs(ctx)

		case <-breadthTicker.C:
			go s.runBreadth(ctx)

		case <-matviewCheckTicker.C:
			now := time.Now()
			rebuildDay := s.cfg.MatviewRebuildDay
			if rebuildDay >= 0 && int(now.Weekday()) == rebuildDay && now.Sub(lastMatviewRebuild) > 20*time.Hour {
				s.rebuildMatviews(ctx, sem)
				lastMatviewRebuild = now
			}

		case <-pollTicker.C:
			s.fillWorkerSlots(ctx, sem)
		}
	}
}

// fillWorkerSlots fills all available semaphore slots with jobs from the queue.
// Called on startup (immediate) and on every poll tick. Keeps claiming jobs
// until the queue is empty or all worker slots are busy.
func (s *Scheduler) fillWorkerSlots(ctx context.Context, sem chan struct{}) {
	claimed := 0
	for {
		// Check if extra parallelSlots from large-repo collection have pushed
		// us over the configured worker limit. If so, don't start new jobs
		// until the parallel goroutines finish and release their slots.
		extraSlots := int(collector.ParallelSlots.Load())
		if extraSlots > 0 && len(sem)+extraSlots >= s.cfg.Workers {
			if claimed > 0 {
				s.logger.Info("fill cycle complete (parallel slots active)", "claimed", claimed, "active", len(sem), "parallelSlots", extraSlots)
			}
			return
		}
		select {
		case sem <- struct{}{}:
			// Got a worker slot — try to claim a job.
			job, err := s.store.DequeueNext(ctx, s.workerID)
			if err != nil {
				s.logger.Error("failed to dequeue", "error", err)
				<-sem
				if claimed > 0 {
					s.logger.Info("fill cycle complete (dequeue error)", "claimed", claimed, "active", len(sem))
				}
				return
			}
			if job == nil {
				<-sem // no more work available
				if claimed > 0 {
					s.logger.Info("fill cycle complete (queue empty)", "claimed", claimed, "active", len(sem))
				}
				return
			}
			claimed++
			go func() {
				defer func() { <-sem }()
				s.runJob(ctx, job)
			}()
		default:
			// All worker slots busy.
			if claimed > 0 {
				s.logger.Info("fill cycle complete (all slots busy)", "claimed", claimed, "active", len(sem))
			}
			return
		}
	}
}

// jobOutcome accumulates results from all collection phases for a single repo.
// It is used internally by runJob to track counts across phases and determine
// the final success/failure status for CompleteJob.
type jobOutcome struct {
	issues       int
	prs          int
	messages     int
	events       int
	releases     int
	contributors int
	commits      int
	success      bool
	errMsg       string
}

func (s *Scheduler) runJob(ctx context.Context, job *db.QueueJob) {
	start := time.Now()

	// Start a heartbeat goroutine that keeps locked_at fresh every 30 seconds.
	// Without this, RecoverStaleLocks (1-hour timeout) steals active jobs from
	// workers collecting large repos (e.g., kubernetes/kubernetes takes 10+ hours).
	// The heartbeat proves the worker is alive — RecoverStaleLocks only reclaims
	// locks where locked_at is older than the timeout.
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := s.store.HeartbeatJob(heartbeatCtx, job.RepoID, s.workerID); err != nil {
					s.logger.Warn("heartbeat failed", "repo_id", job.RepoID, "error", err)
				}
			}
		}
	}()

	// Look up the repo to get URL, owner, name, platform.
	repo, err := s.store.GetRepoByID(ctx, job.RepoID)
	if err != nil {
		s.logger.Error("failed to look up repo", "repo_id", job.RepoID, "error", err)
		s.failJob(ctx, job.RepoID, err.Error())
		return
	}

	// Prelim phase: check for redirects and duplicates.
	prelim, err := collector.RunPrelim(ctx, s.store, repo, s.logger)
	if err != nil {
		s.logger.Error("prelim failed", "repo_id", job.RepoID, "error", err)
	}
	if prelim != nil && prelim.Skip {
		s.logger.Warn("skipping repo", "repo_id", job.RepoID, "reason", prelim.SkipReason)
		s.skipJob(ctx, job.RepoID, prelim.SkipReason)
		return
	}

	// Generic git repos skip API collection — they only get facade, analysis,
	// scorecard, and SBOM. Commit authors are resolved against both GitHub and
	// GitLab Search APIs to find platform identities.
	var result *collector.CollectResult
	if !repo.Platform.IsGitOnly() {
		client, clientErr := s.selectClient(repo.Platform)
		if clientErr != nil {
			s.logger.Error("unknown platform", "repo_id", job.RepoID, "platform", repo.Platform)
			s.failJob(ctx, job.RepoID, clientErr.Error())
			return
		}
		since := s.determineSince(job)
		result, err = s.collectAndProcess(ctx, job.RepoID, repo, client, since)

		// Refresh open items: re-fetch all open issues and PRs to capture
		// status changes (closed, merged), new labels, assignees, reviews, etc.
		// Runs after normal collection so we don't duplicate work for items
		// already updated via the since-based incremental fetch.
		if err == nil {
			refresher := collector.NewOpenItemRefresher(s.store, client, s.logger)
			refresher.RefreshOpenItems(ctx, job.RepoID, repo.Owner, repo.Name)
		}

		// Gap fill: after collectAndProcess, repo_info has fresh metadata counts.
		// Compare gathered vs metadata — if gap >5%, fetch the specific missing
		// items rather than waiting for the next full collection pass.
		if err == nil {
			metaIssues, metaPRs, metaErr := s.store.GetRepoMetaCounts(ctx, job.RepoID)
			if metaErr == nil && (metaIssues > 0 || metaPRs > 0) {
				gf := collector.NewGapFiller(s.store, client, s.logger)
				filled, gfErr := gf.AssessAndFillGaps(ctx, job.RepoID, repo.Owner, repo.Name, metaIssues, metaPRs)
				if gfErr != nil {
					s.logger.Warn("gap fill error", "repo_id", job.RepoID, "error", gfErr)
				} else if filled > 0 {
					s.logger.Info("gap fill completed", "repo_id", job.RepoID, "filled", filled)
				}
			}
		}
		// Enrich thin contributor profiles. Contributors from the Contributors
		// API and lazy resolution only get basic data (login, avatar). This
		// calls GET /users/{login} for company, location, email, name.
		// Runs incrementally: up to 500 per pass.
		resolver := db.NewContributorResolver(s.store)
		collector.EnrichThinContributors(ctx, s.store, resolver, client, s.logger)
	} else {
		s.logger.Info("git-only repo, skipping API collection", "repo_id", job.RepoID)
	}

	// Phase 3+4: facade then analysis (sequential — analysis needs bare clone).
	facadeResult, analysisResult := s.runFacadeAndAnalysis(ctx, job.RepoID, repo)

	// Phase 5: commit resolution.
	// For generic git repos, attempt resolution on both GitHub and GitLab
	// since we don't know where the contributor identities live.
	s.runCommitResolution(ctx, job.RepoID, repo)

	// Phase 5b: Auto-populate contributor affiliations.
	// Maps contributor email domains to organizations using company data from
	// GitHub/GitLab user profiles. Must run after enrichment + commit resolution
	// so we have the most complete email/company data.
	if affCount, err := s.store.PopulateAffiliations(ctx); err != nil {
		s.logger.Warn("affiliation population failed", "error", err)
	} else if affCount > 0 {
		s.logger.Info("auto-populated affiliations", "count", affCount)
	}

	// Phase 6: SBOM generation.
	s.generateSBOMs(ctx, job.RepoID)

	// Phase 7: Vulnerability scanning via OSV.dev.
	// Uses purls from libyear data to query for known CVEs.
	vulnResult, vulnErr := collector.ScanVulnerabilities(ctx, s.store, job.RepoID, s.logger)
	if vulnErr != nil {
		s.logger.Warn("vulnerability scan failed", "repo_id", job.RepoID, "error", vulnErr)
	} else if vulnResult != nil && vulnResult.VulnsFound > 0 {
		s.logger.Info("vulnerabilities found",
			"repo_id", job.RepoID,
			"deps_scanned", vulnResult.TotalDepsScanned,
			"vulns_found", vulnResult.VulnsFound)
	}

	// Determine outcome and complete the job.
	outcome := s.buildOutcome(result, facadeResult, analysisResult, err)
	duration := time.Since(start)

	if err := s.store.CompleteJob(ctx, job.RepoID, outcome.success, s.cfg.RecollectAfter,
		outcome.issues, outcome.prs, outcome.messages, outcome.events,
		outcome.releases, outcome.contributors, outcome.commits,
		duration.Milliseconds(), outcome.errMsg); err != nil {
		s.logger.Warn("failed to complete job", "repo_id", job.RepoID, "error", err)
	}

	s.logger.Info("job complete",
		"repo_id", job.RepoID,
		"owner", repo.Owner, "repo", repo.Name,
		"success", outcome.success,
		"duration", duration.Truncate(time.Second),
		"issues", outcome.issues, "prs", outcome.prs,
	)
}

// failJob marks a job as failed with zero counts. Used for early exits
// (repo lookup failure, unknown platform, etc.).
func (s *Scheduler) failJob(ctx context.Context, repoID int64, errMsg string) {
	if err := s.store.CompleteJob(ctx, repoID, false, s.cfg.RecollectAfter,
		0, 0, 0, 0, 0, 0, 0, 0, errMsg); err != nil {
		s.logger.Warn("failed to record job failure", "repo_id", repoID, "error", err)
	}
}

// skipJob marks a job as successfully completed with zero counts and a reason.
// Used when prelim determines the repo should be skipped (e.g., deleted, duplicate).
func (s *Scheduler) skipJob(ctx context.Context, repoID int64, reason string) {
	if err := s.store.CompleteJob(ctx, repoID, true, s.cfg.RecollectAfter,
		0, 0, 0, 0, 0, 0, 0, 0, reason); err != nil {
		s.logger.Warn("failed to record job skip", "repo_id", repoID, "error", err)
	}
}

// selectClient returns the platform client for the given platform, or an error
// if the platform is unknown.
func (s *Scheduler) selectClient(p model.Platform) (platform.Client, error) {
	switch p {
	case model.PlatformGitHub:
		return s.ghClient, nil
	case model.PlatformGitLab:
		return s.glClient, nil
	default:
		return nil, fmt.Errorf("unknown platform: %d", p)
	}
}

// determineSince returns the starting point for incremental collection.
// For repos that have never been collected, it returns zero time (full collection).
// For repos previously collected, it returns now minus the recollect window.
// When ForceFullCollection is true, always returns zero time to trigger a
// full re-collection. Use this after bug fixes to repopulate data.
func (s *Scheduler) determineSince(job *db.QueueJob) time.Time {
	if s.cfg.ForceFullCollection {
		return time.Time{} // force full re-collection
	}
	if job.LastCollected != nil {
		return time.Now().Add(-s.cfg.RecollectAfter)
	}
	return time.Time{} // zero = full collection
}

// collectAndProcess runs the two-phase staged pipeline: stage raw JSON from
// the API, then process staged data into relational tables with bulk
// contributor resolution.
func (s *Scheduler) collectAndProcess(ctx context.Context, repoID int64, repo *model.Repo, client platform.Client, since time.Time) (*collector.CollectResult, error) {
	sc := collector.NewStagedCollector(client, s.store, s.logger)
	result, err := sc.CollectRepo(ctx, repoID, repo.Owner, repo.Name, since)

	if err == nil {
		proc := collector.NewProcessor(s.store, s.logger)
		if procErr := proc.ProcessRepo(ctx, repoID, int16(repo.Platform)); procErr != nil {
			err = procErr
		}
	}
	return result, err
}

// runFacadeAndAnalysis runs facade (git clone + log) then analysis (deps, libyear,
// scc) sequentially. Analysis depends on the bare clone that facade creates, so
// they cannot run in parallel on the first collection pass for a repo.
func (s *Scheduler) runFacadeAndAnalysis(ctx context.Context, repoID int64, repo *model.Repo) (*collector.FacadeResult, *collector.AnalysisResult) {
	// Phase 3: Facade — creates/updates bare clone and parses git log.
	var facadeResult *collector.FacadeResult
	fc := collector.NewFacadeCollector(s.store, s.logger, s.cfg.RepoCloneDir)
	// Strip any existing .git suffix before appending to avoid double
	// .git.git URLs. Many JOSS/Augur-imported repos store the URL with .git.
	repoName := strings.TrimSuffix(repo.Name, ".git")
	gitURL := fmt.Sprintf("https://%s/%s/%s.git",
		platformHostForModel(repo.Platform), repo.Owner, repoName)
	result, err := fc.CollectRepo(ctx, repoID, gitURL)
	if err != nil {
		s.logger.Warn("facade collection failed", "repo_id", repoID, "error", err)
	} else if result != nil {
		s.logger.Info("facade complete",
			"repo_id", repoID,
			"commits", result.Commits,
			"commit_messages", result.CommitMessages)
	}
	facadeResult = result

	// Phase 4: Analysis — needs the bare clone from facade.
	// RetainClone keeps the temp clone alive for scorecard local execution.
	var analysisResult *collector.AnalysisResult
	ac := collector.NewAnalysisCollector(s.store, s.logger, s.cfg.RepoCloneDir)
	ac.RetainClone = true
	aResult, aErr := ac.AnalyzeRepo(ctx, repoID)
	if aErr != nil {
		s.logger.Warn("analysis failed", "repo_id", repoID, "error", aErr)
	} else if aResult != nil {
		s.logger.Info("analysis complete",
			"repo_id", repoID,
			"dependencies", aResult.Dependencies,
			"libyear_deps", aResult.LibyearDeps,
			"labor_files", aResult.LaborFiles)
	}
	analysisResult = aResult

	// Phase 4b: OpenSSF Scorecard — runs locally against the retained temp clone.
	// Local execution is much faster than remote mode: scorecard skips cloning
	// and runs many checks (Binary-Artifacts, Pinned-Dependencies, etc.) purely
	// against local files. API-dependent checks (Code-Review, Maintained, etc.)
	// still need GITHUB_TOKEN but make far fewer calls (~20-50 vs ~150-300).
	// No concurrency semaphore needed — local mode is mostly disk I/O, and the
	// small number of remaining API calls is handled by MarkDepleted so the key
	// pool rotates past used tokens.
	//
	// The temp clone is cleaned up after scorecard finishes, regardless of outcome.
	{
		repoURL := fmt.Sprintf("https://%s/%s/%s",
			platformHostForModel(repo.Platform), repo.Owner, repo.Name)

		// Determine the local clone path from analysis result.
		localPath := ""
		if analysisResult != nil && analysisResult.ClonePath != "" {
			localPath = analysisResult.ClonePath
		}

		token := ""
		var usedKey *platform.APIKey
		if s.ghKeys != nil {
			if key, err := s.ghKeys.GetKey(ctx); err == nil {
				token = key.Token
				usedKey = key
			}
		}
		_, scErr := collector.RunScorecard(ctx, s.store, repoID, repoURL, localPath, token, s.logger)
		if scErr != nil {
			s.logger.Warn("scorecard failed", "repo_id", repoID, "error", scErr)
		}

		// Mark the token as depleted. Local mode makes fewer API calls
		// (~20-50 vs ~150-300 in remote mode), so the penalty is reduced.
		if usedKey != nil && s.ghKeys != nil {
			s.ghKeys.MarkDepleted(usedKey, 100)
		}

		// Clean up the retained temp clone now that scorecard is done.
		if localPath != "" {
			os.RemoveAll(localPath)
			s.logger.Info("removed retained analysis clone after scorecard", "path", localPath)
		}
	}

	return facadeResult, analysisResult
}

// runCommitResolution resolves git commit emails to GitHub users.
// Only runs for GitHub repos when API keys are available.
func (s *Scheduler) runCommitResolution(ctx context.Context, repoID int64, repo *model.Repo) {
	if repo.Platform != model.PlatformGitHub || s.ghKeys == nil {
		return
	}

	resolver := collector.NewCommitResolver(s.store, s.ghKeys, s.logger)
	resolveResult, resolveErr := resolver.ResolveCommits(ctx, repoID, repo.Owner, repo.Name)
	if resolveErr != nil {
		s.logger.Warn("commit resolution failed", "repo_id", repoID, "error", resolveErr)
	} else if resolveResult != nil {
		s.logger.Info("commit resolution complete",
			"repo_id", repoID,
			"resolved_api", resolveResult.ResolvedAPI,
			"resolved_noreply", resolveResult.ResolvedNoreply,
			"unresolved", resolveResult.Unresolved)
	}
	// Also enrich canonical emails for contributors missing them.
	resolver.ResolveEmailsToCanonical(ctx)
}

// buildOutcome evaluates the collection and facade results to determine
// success/failure and extract counts for the job completion record.
func (s *Scheduler) buildOutcome(result *collector.CollectResult, facadeResult *collector.FacadeResult, analysisResult *collector.AnalysisResult, collectionErr error) jobOutcome {
	out := jobOutcome{success: true}

	if collectionErr != nil {
		out.success = false
		out.errMsg = collectionErr.Error()
	} else if result != nil && len(result.Errors) > 0 {
		out.success = false
		out.errMsg = result.Errors[0].Error()
	}

	if result != nil {
		out.issues = result.Issues
		out.prs = result.PullRequests
		out.messages = result.Messages
		out.events = result.Events
		out.releases = result.Releases
		out.contributors = result.Contributors
	}

	if facadeResult != nil {
		out.commits = facadeResult.Commits
	}

	// A repo with zero data across all entity types likely had an auth failure
	// or is truly empty — mark as failure so it gets retried.
	if result != nil && out.issues == 0 && out.prs == 0 && out.releases == 0 && out.contributors == 0 {
		out.success = false
		if out.errMsg == "" {
			out.errMsg = "no data collected (possible API auth failure or empty repo)"
		}
	}

	return out
}

func platformHostForModel(p model.Platform) string {
	switch p {
	case model.PlatformGitHub:
		return "github.com"
	case model.PlatformGitLab:
		return "gitlab.com"
	default:
		return "unknown"
	}
}

// generateSBOMs produces CycloneDX and SPDX SBOMs after collection completes.
// Non-fatal — if SBOM generation fails, collection still succeeds.
func (s *Scheduler) generateSBOMs(ctx context.Context, repoID int64) {
	collector.GenerateAndStoreSBOMs(ctx, s.store, repoID, s.logger)
}

// processLeftoverStaging drains any unprocessed staging rows from a previous
// interrupted run. This ensures we don't lose data that was staged but never
// processed into relational tables.
func (s *Scheduler) processLeftoverStaging(ctx context.Context) {
	// Find repos with unprocessed staging rows.
	rows, err := s.store.Pool().Query(ctx, `
		SELECT DISTINCT repo_id FROM aveloxis_ops.staging WHERE NOT processed`)
	if err != nil {
		s.logger.Warn("failed to check for leftover staging rows", "error", err)
		return
	}
	defer rows.Close()

	var repoIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			repoIDs = append(repoIDs, id)
		}
	}

	if len(repoIDs) == 0 {
		return
	}

	s.logger.Info("processing leftover staging data from previous run", "repos", len(repoIDs))
	for _, repoID := range repoIDs {
		repo, err := s.store.GetRepoByID(ctx, repoID)
		if err != nil {
			s.logger.Warn("failed to look up repo for leftover processing", "repo_id", repoID, "error", err)
			continue
		}
		proc := collector.NewProcessor(s.store, s.logger)
		if err := proc.ProcessRepo(ctx, repoID, int16(repo.Platform)); err != nil {
			s.logger.Warn("failed to process leftover staging", "repo_id", repoID, "error", err)
		} else {
			s.logger.Info("processed leftover staging data", "repo_id", repoID)
		}
	}
}

// releaseOurLocks releases all queue locks held by this worker instance,
// returning repos to 'queued' status so they can be picked up immediately
// on restart instead of waiting for stale lock timeout.
func (s *Scheduler) releaseOurLocks(ctx context.Context) {
	tag, err := s.store.Pool().Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'queued', locked_by = NULL, locked_at = NULL, due_at = NOW()
		WHERE locked_by = $1 AND status = 'collecting'`, s.workerID)
	if err != nil {
		s.logger.Warn("failed to release locks on shutdown", "error", err)
		return
	}
	if tag.RowsAffected() > 0 {
		s.logger.Info("released queue locks", "count", tag.RowsAffected(), "worker_id", s.workerID)
	}
}

// refreshOrgs re-scans all org/group-type repo groups for new repos and
// checks existing repos for renames. Runs periodically (default every 4h).
func (s *Scheduler) refreshOrgs(ctx context.Context) {
	groups, err := s.store.GetOrgRepoGroups(ctx)
	if err != nil {
		s.logger.Warn("failed to load org repo groups", "error", err)
		return
	}
	if len(groups) == 0 {
		return
	}

	s.logger.Info("refreshing org/group repo lists", "groups", len(groups))

	for _, g := range groups {
		if ctx.Err() != nil {
			return
		}

		var newRepos int
		switch g.Type {
		case "github_org":
			newRepos = s.refreshGitHubOrg(ctx, g)
		case "gitlab_group":
			newRepos = s.refreshGitLabGroup(ctx, g)
		}
		if newRepos > 0 {
			s.logger.Info("new repos discovered in org", "org", g.Name, "new", newRepos)
		}
	}

	// Check existing repos for renames via prelim.
	s.checkForRenames(ctx)
}

func (s *Scheduler) refreshGitHubOrg(ctx context.Context, g db.OrgGroup) int {
	if s.ghKeys == nil {
		return 0
	}
	http := platform.NewHTTPClient("https://api.github.com", s.ghKeys, s.logger, platform.AuthGitHub)

	newCount := 0
	page := 1
	for {
		path := fmt.Sprintf("/orgs/%s/repos?per_page=100&type=all&page=%d", g.Name, page)
		resp, err := http.Get(ctx, path)
		if err != nil {
			s.logger.Warn("org refresh API error", "org", g.Name, "error", err)
			break
		}
		var items []struct {
			HTMLURL string `json:"html_url"`
			Name    string `json:"name"`
			Owner   struct {
				Login string `json:"login"`
			} `json:"owner"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			break
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			// Check if we already have this repo.
			existing, findErr := s.store.FindRepoByURL(ctx, item.HTMLURL)
			if findErr != nil {
				s.logger.Warn("failed to check for existing repo", "url", item.HTMLURL, "error", findErr)
			}
			if existing > 0 {
				continue
			}
			// New repo — add it.
			repoID, err := s.store.UpsertRepo(ctx, &model.Repo{
				Platform: model.PlatformGitHub,
				GitURL:   item.HTMLURL,
				Name:     item.Name,
				Owner:    item.Owner.Login,
				GroupID:  g.ID,
			})
			if err != nil {
				continue
			}
			if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
				continue
			}
			s.logger.Info("new repo discovered", "org", g.Name, "repo", item.HTMLURL)
			newCount++
		}
		page++
	}
	return newCount
}

func (s *Scheduler) refreshGitLabGroup(ctx context.Context, g db.OrgGroup) int {
	// Use the gitlab client's base URL or derive from the website URL.
	glHost := "gitlab.com"
	if u, err := url.Parse(g.Website); err == nil && u.Host != "" {
		glHost = u.Host
	}
	// Need GitLab keys — check if the glClient is available.
	// We'll reuse the ghKeys pool for now; in practice GitLab keys are separate.
	// TODO: pass glKeys to the scheduler for GitLab org refresh.
	http := platform.NewHTTPClient("https://"+glHost+"/api/v4", s.ghKeys, s.logger, platform.AuthGitLab)

	newCount := 0
	page := 1
	encodedGroup := url.PathEscape(g.Name)
	for {
		path := fmt.Sprintf("/groups/%s/projects?per_page=100&include_subgroups=true&page=%d", encodedGroup, page)
		resp, err := http.Get(ctx, path)
		if err != nil {
			s.logger.Warn("group refresh API error", "group", g.Name, "error", err)
			break
		}
		var items []struct {
			WebURL    string `json:"web_url"`
			Name      string `json:"name"`
			Namespace struct {
				FullPath string `json:"full_path"`
			} `json:"namespace"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			break
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			existing, findErr := s.store.FindRepoByURL(ctx, item.WebURL)
			if findErr != nil {
				s.logger.Warn("failed to check for existing repo", "url", item.WebURL, "error", findErr)
			}
			if existing > 0 {
				continue
			}
			repoID, err := s.store.UpsertRepo(ctx, &model.Repo{
				Platform: model.PlatformGitLab,
				GitURL:   item.WebURL,
				Name:     item.Name,
				Owner:    item.Namespace.FullPath,
				GroupID:  g.ID,
			})
			if err != nil {
				continue
			}
			if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
				continue
			}
			s.logger.Info("new repo discovered", "group", g.Name, "repo", item.WebURL)
			newCount++
		}
		page++
	}
	return newCount
}

// checkForRenames runs prelim on a sample of repos to detect renames/transfers.
// Checks repos that haven't been collected recently — they're the most likely
// to have gone stale.
func (s *Scheduler) checkForRenames(ctx context.Context) {
	repos, err := s.store.GetReposForRenameCheck(ctx, 50)
	if err != nil {
		s.logger.Warn("failed to load repos for rename check", "error", err)
		return
	}
	for _, repo := range repos {
		if ctx.Err() != nil {
			return
		}
		prelim, err := collector.RunPrelim(ctx, s.store, &repo, s.logger)
		if err != nil {
			continue
		}
		if prelim != nil && (prelim.Skip || prelim.Redirected) {
			s.logger.Info("rename check result",
				"repo_id", repo.ID, "url", repo.GitURL,
				"skip", prelim.Skip, "redirected", prelim.Redirected,
				"reason", prelim.SkipReason, "new_url", prelim.NewURL)
		}
	}
}

// rebuildMatviews suspends collection, rebuilds all materialized views, then resumes.
func (s *Scheduler) rebuildMatviews(ctx context.Context, sem chan struct{}) {
	s.logger.Info("weekly matview rebuild: suspending collection workers")

	// Drain the semaphore to wait for all active workers to finish.
	for range s.cfg.Workers {
		sem <- struct{}{}
	}
	s.logger.Info("weekly matview rebuild: all workers idle, starting rebuild")

	start := time.Now()
	if err := db.RefreshMaterializedViews(ctx, s.store, s.logger); err != nil {
		s.logger.Error("weekly matview rebuild failed", "error", err)
	} else {
		s.logger.Info("weekly matview rebuild complete", "duration", time.Since(start).Truncate(time.Second))
	}

	// Refresh dm_ aggregate tables (dm_repo_annual/monthly/weekly and
	// dm_repo_group variants) while workers are still paused. These tables
	// aggregate commit data by email, affiliation, and time period.
	// Running here avoids conflicts with active collection workers.
	aggStart := time.Now()
	if err := s.store.RefreshAllRepoAggregates(ctx, s.logger); err != nil {
		s.logger.Error("dm_ aggregate refresh failed", "error", err)
	} else {
		s.logger.Info("dm_ aggregate refresh complete", "duration", time.Since(aggStart).Truncate(time.Second))
	}

	// Release all semaphore slots to resume collection.
	for range s.cfg.Workers {
		<-sem
	}
	s.logger.Info("weekly matview rebuild: collection resumed")
}

// refreshUserOrgs scans user_org_requests for new repos in tracked orgs
// and adds them to the user's group + collection queue.
func (s *Scheduler) refreshUserOrgs(ctx context.Context) {
	orgs, err := s.store.GetOrgRequests(ctx)
	if err != nil || len(orgs) == 0 {
		return
	}

	s.logger.Info("scanning user org requests", "count", len(orgs))
	for _, org := range orgs {
		groupID, err := s.store.GetGroupIDForOrgRequest(ctx, org.OrgRequestID)
		if err != nil {
			continue
		}

		var repos []struct{ URL, Owner, Name string }
		switch org.Platform {
		case "github":
			if s.ghKeys == nil {
				continue
			}
			httpC := platform.NewHTTPClient("https://api.github.com", s.ghKeys, s.logger, platform.AuthGitHub)
			// Try /orgs/ first, fall back to /users/ for personal accounts.
			basePaths := []string{
				fmt.Sprintf("/orgs/%s/repos", org.OrgName),
				fmt.Sprintf("/users/%s/repos", org.OrgName),
			}
			for _, basePath := range basePaths {
				page := 1
				found := false
				for {
					path := fmt.Sprintf("%s?per_page=100&type=all&page=%d", basePath, page)
					resp, err := httpC.Get(ctx, path)
					if err != nil {
						break
					}
					var items []struct {
						HTMLURL string `json:"html_url"`
						Name    string `json:"name"`
						Owner   struct{ Login string `json:"login"` } `json:"owner"`
					}
					if decErr := json.NewDecoder(resp.Body).Decode(&items); decErr != nil {
						s.logger.Warn("failed to decode org repos response", "path", path, "error", decErr)
					}
					resp.Body.Close()
					if len(items) == 0 {
						break
					}
					found = true
					for _, item := range items {
						repos = append(repos, struct{ URL, Owner, Name string }{item.HTMLURL, item.Owner.Login, item.Name})
					}
					page++
				}
				if found {
					break
				}
			}
		}

		newCount := 0
		for _, repo := range repos {
			// Ensure repo exists.
			repoID, findErr := s.store.FindRepoByURL(ctx, repo.URL)
			if findErr != nil {
				s.logger.Warn("failed to find repo by URL", "url", repo.URL, "error", findErr)
			}
			if repoID == 0 {
				var err error
				repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
					Platform: model.PlatformGitHub,
					GitURL:   repo.URL,
					Name:     repo.Name,
					Owner:    repo.Owner,
				})
				if err != nil {
					continue
				}
				if enqErr := s.store.EnqueueRepo(ctx, repoID, 100); enqErr != nil {
					s.logger.Warn("failed to enqueue repo", "repo_id", repoID, "error", enqErr)
				}
			}
			// Add to user group (ON CONFLICT DO NOTHING for existing).
			if err := s.store.AddRepoToGroupByID(ctx, groupID, repoID); err == nil {
				newCount++
			}
		}

		if err := s.store.MarkOrgRequestScanned(ctx, org.OrgRequestID); err != nil {
			s.logger.Warn("failed to mark org request scanned", "org_request_id", org.OrgRequestID, "error", err)
		}
		if newCount > 0 {
			s.logger.Info("user org scan found new repos",
				"org", org.OrgName, "group_id", groupID, "new_repos", newCount)
		}
	}
}

// runBreadth discovers cross-repo activity for contributors via the GitHub Events API.
func (s *Scheduler) runBreadth(ctx context.Context) {
	if s.ghKeys == nil {
		return
	}
	bw := collector.NewBreadthWorker(s.store, s.ghKeys, s.logger)
	result, err := bw.Run(ctx, 100) // process up to 100 contributors per cycle
	if err != nil {
		s.logger.Warn("breadth worker failed", "error", err)
		return
	}
	if result.ContributorsProcessed > 0 {
		s.logger.Info("breadth worker complete",
			"contributors", result.ContributorsProcessed,
			"events_inserted", result.EventsInserted)
	}
}

func (s *Scheduler) recoverStale(ctx context.Context) {
	recovered, err := s.store.RecoverStaleLocks(ctx, s.cfg.StaleLockTimeout)
	if err != nil {
		s.logger.Error("failed to recover stale locks", "error", err)
		return
	}
	if recovered > 0 {
		s.logger.Warn("recovered stale locks", "count", recovered)
	}
}
