// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — scorecard.go runs the OpenSSF Scorecard tool against
// a repository during the analysis phase.
//
// The scorecard binary (https://github.com/ossf/scorecard) must be installed
// and available on PATH. If not found, this phase is silently skipped.
//
// Execution modes (v0.27.5 — REMOTE-PRIMARY with local backstop):
//
//   - Remote mode (--repo <url>): scorecard clones the repo itself and runs
//     the FULL check set (~18 checks on a moderate repo, including
//     Code-Review, Maintained, Contributors, Branch-Protection, CI-Tests,
//     CII-Best-Practices, Signed-Releases). Measured cost: ~40 API calls
//     (32 REST + 8 GraphQL), ~25s on augurlabs/augur. GitHub repos run
//     remote FIRST.
//   - Local mode (--local <path>): scorecard runs against the retained
//     analysis clone. Measured: 0 API calls, ~7s, but only ~11 checks —
//     the API-dependent checks are skipped entirely. Used as the BACKSTOP
//     when the remote attempt errors or times out (11 checks beat none),
//     and as the ONLY mode for GitLab and generic-git repos (scorecard's
//     GitLab remote support is immature).
//
// The two modes' overall scores are NOT comparable (different check sets),
// so every persisted row — checks and the __overall__ aggregate — carries a
// scorecard_mode marker ('remote' or 'local').
//
// Multi-token: GITHUB_TOKEN may be a comma-separated token list; scorecard
// round-robins the list per request. This is what makes remote mode viable
// at fleet scale — the pre-v0.27.5 single shared token slept through
// rate-limit resets for DAYS on production.
//
// Assumptions:
//   - The `scorecard` binary is installed (e.g., via `aveloxis install-tools`)
//   - It requires GITHUB_TOKEN for API-dependent checks
//   - The temp clone's origin remote must point to the actual repo URL for
//     local mode's remaining API-dependent checks (set via `git remote set-url`)
package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// ScorecardResult holds the parsed output from the scorecard tool.
type ScorecardResult struct {
	OverallScore float64          `json:"score"`
	Checks       []ScorecardCheck `json:"checks"`

	// Mode is which execution mode produced this result: "remote" or
	// "local" (v0.27.5). Remote and local overall scores are NOT
	// comparable — different check sets.
	Mode string `json:"-"`
	// Discarded is true when the run completed but its result was NOT
	// written because a local (partial) set would have replaced a
	// stored remote (complete) one — the 2026-09-12 operator rule. The
	// attempt's cost is real, so the result still carries Mode, Checks
	// and Duration for the phase log.
	Discarded bool `json:"-"`
	// APICalls is the instrumented GitHub API spend of the run
	// (core + graphql used-delta measured via /rate_limit on the first
	// token). 0 for local mode (which makes no instrumented calls);
	// -1 when instrumentation was requested but the probe failed.
	APICalls int64 `json:"-"`
	// Duration is the wall-clock time of the whole run including any
	// fallback attempt.
	Duration time.Duration `json:"-"`
}

// ScorecardCheck is a single scorecard check result.
type ScorecardCheck struct {
	Name    string   `json:"name"`
	Score   float64  // scorecard emits numbers; fractional values must not break the parse      `json:"score"`
	Reason  string   `json:"reason"`
	Details []string `json:"details,omitempty"`
}

// scorecardStore is the narrow store surface the scorecard phase needs
// (v0.25.38 role-interface pattern) — lets behavioral tests drive the
// full remote/local fallback with a fake store + fake scorecard binary.
// *db.PostgresStore satisfies it.
type scorecardStore interface {
	// CurrentScorecardMode reports the scorecard_mode of the repo's
	// CURRENT rows (found=false when none are stored). A lookup ERROR
	// is not "none" (SR-5) — persistScorecard refuses to write on one.
	CurrentScorecardMode(ctx context.Context, repoID int64) (mode string, found bool, err error)
	RotateScorecardToHistory(ctx context.Context, repoID int64) error
	InsertScorecardResult(ctx context.Context, repoID int64, name, score string, detailsJSON []byte, mode string) error
}

// errScorecardModeProbe wraps a CurrentScorecardMode failure so the
// phase log can say the run was not written and why.
var errScorecardModeProbe = errors.New("scorecard: current-mode probe failed — result not written (a probe error is not 'no prior set')")

// ScorecardOptions bundles the inputs for RunScorecard.
type ScorecardOptions struct {
	// RepoURL is the https URL scorecard's remote mode targets
	// (e.g. https://github.com/owner/name).
	RepoURL string
	// LocalPath is the retained analysis clone; "" = no local mode
	// available (remote-only for GitHub, skip for other platforms).
	LocalPath string
	// RemotePrimary selects remote-first-with-local-backstop (GitHub).
	// False = local mode only (GitLab / generic git — scorecard's
	// GitLab remote support is immature).
	RemotePrimary bool
	// Timeout is the per-ATTEMPT wall-clock cap. <= 0 falls back to
	// 15 minutes (the config accessor default; defense in depth here
	// so a direct caller can't run unbounded).
	Timeout time.Duration
	// GithubToken is the GITHUB_TOKEN value — a comma-separated list
	// for multi-token round-robin (see ScorecardTokens).
	GithubToken string
	// InstrumentToken, when non-empty, enables the before/after
	// /rate_limit probe on remote attempts (measured API spend in the
	// completion log). Conventionally the FIRST pool token. Local mode
	// never probes — it makes no instrumented calls.
	InstrumentToken string
	// RateLimitURL overrides the probe endpoint (test seam);
	// "" = https://api.github.com/rate_limit.
	RateLimitURL string
}

// defaultScorecardTimeout is the defense-in-depth per-attempt cap
// applied when a caller passes Timeout <= 0. Must match the
// config.ScorecardTimeout accessor default.
const defaultScorecardTimeout = 15 * time.Minute

// scorecardRateLimitURL is the default endpoint for the API-spend probe.
const scorecardRateLimitURL = "https://api.github.com/rate_limit"

// ScorecardTokens builds scorecard's comma-separated GITHUB_TOKEN value
// from the key pool (v0.27.5). count 0 = all non-invalidated tokens;
// N>0 = the N least-borrowed. Returns the joined list, the first token
// (used for the /rate_limit instrumentation probe), and the release the
// caller MUST invoke once the subprocess has exited.
//
// 2026-09-12: the tokens are BORROWED through KeyPool.LendTokens rather
// than copied out with the retired AllTokens — a subprocess cannot hold
// a Go lease, so its use is accounted (which keys, how many concurrent
// borrowers) instead of invisible. Still no in-flight lease: scorecard
// paces itself across the set, and its ~40 calls over ~25 s are
// negligible per key; the point is that the pool KNOWS.
func ScorecardTokens(pool *platform.KeyPool, count int) (joined, first string, release func()) {
	if pool == nil {
		return "", "", func() {}
	}
	tokens, release := pool.LendTokens(count)
	if len(tokens) == 0 {
		release()
		return "", "", func() {}
	}
	return strings.Join(tokens, ","), tokens[0], release
}

// RunScorecard executes the OpenSSF Scorecard tool against a repo and
// stores results in repo_deps_scorecard. Requires the `scorecard` binary
// on PATH (silently skipped otherwise).
//
// Mode selection (v0.27.5):
//   - RemotePrimary (GitHub): remote attempt first; on error or
//     per-attempt timeout, fall back to local mode when a clone path
//     exists — 11 checks beat none. No clone → the remote error surfaces.
//   - Local-only (GitLab / generic git): local mode only. No clone →
//     skipped with an INFO log (scorecard's GitLab remote support is
//     immature; running --repo against non-GitHub hosts is not useful).
func RunScorecard(ctx context.Context, store scorecardStore, repoID int64, opts ScorecardOptions, logger *slog.Logger) (*ScorecardResult, error) {
	// Check if scorecard is installed.
	scorecardPath, err := exec.LookPath("scorecard")
	if err != nil {
		logger.Info("scorecard not installed, skipping OpenSSF Scorecard analysis",
			"install", "aveloxis install-tools")
		return nil, nil
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultScorecardTimeout
	}
	start := time.Now()
	// Repo URLs originate from operator/user input (add-repo, web GUI).
	// slog's TextHandler quote-escapes attribute values so log forgery
	// is not actually possible, but scrubbing CR/LF at user-influenced
	// log sites is the house defense-in-depth pattern (see
	// internal/api/logsafe.go, internal/web truncateForLog). v0.27.10.
	safeRepoURL := scrubLogValue(opts.RepoURL)

	if !opts.RemotePrimary {
		// Local-only platforms (GitLab, generic git).
		if opts.LocalPath == "" {
			logger.Info("scorecard skipped — local-only platform with no analysis clone",
				"repo_id", repoID, "url", safeRepoURL)
			return nil, nil
		}
		raw, localErr := invokeScorecard(ctx, scorecardPath, repoID, opts.RepoURL, opts.LocalPath, timeout, opts.GithubToken, logger)
		if localErr != nil {
			return nil, localErr
		}
		return finishScorecard(ctx, store, repoID, raw, "local", 0, time.Since(start), logger)
	}

	// Remote-primary (GitHub): --repo first, instrumented.
	rlURL := opts.RateLimitURL
	if rlURL == "" {
		rlURL = scorecardRateLimitURL
	}
	before := fetchRateLimitSnapshot(ctx, rlURL, opts.InstrumentToken, logger)
	raw, remoteErr := invokeScorecard(ctx, scorecardPath, repoID, opts.RepoURL, "", timeout, opts.GithubToken, logger)
	apiCalls := int64(0)
	if opts.InstrumentToken != "" {
		after := fetchRateLimitSnapshot(ctx, rlURL, opts.InstrumentToken, logger)
		apiCalls = rateLimitDelta(before, after)
	}
	if remoteErr == nil {
		return finishScorecard(ctx, store, repoID, raw, "remote", apiCalls, time.Since(start), logger)
	}

	if opts.LocalPath == "" {
		// No local backstop available (the bulk run-scorecard pass, or
		// a repo whose analysis clone was not retained).
		return nil, remoteErr
	}

	if errors.Is(remoteErr, context.Canceled) {
		return nil, remoteErr // shutdown mid-remote: no fallback on a dead ctx (pass 35)
	}
	// Local backstop: 11 checks beat none. Fresh per-attempt timeout.
	logger.Warn("scorecard remote attempt failed — falling back to local mode",
		"repo_id", repoID, "url", safeRepoURL, "error", remoteErr)
	raw, localErr := invokeScorecard(ctx, scorecardPath, repoID, opts.RepoURL, opts.LocalPath, timeout, opts.GithubToken, logger)
	if localErr != nil {
		return nil, fmt.Errorf("scorecard remote attempt failed (%v); local fallback also failed: %w", remoteErr, localErr)
	}
	return finishScorecard(ctx, store, repoID, raw, "local", apiCalls, time.Since(start), logger)
}

// finishScorecard persists a successful attempt (subject to the
// partial-never-replaces-complete gate in persistScorecard) and emits
// the completion log with the instrumented API spend. The only error it
// can return is a failed current-mode probe — the run happened, but its
// result was not written because the store's state was unknowable.
func finishScorecard(ctx context.Context, store scorecardStore, repoID int64, raw *scorecardOutput, mode string, apiCalls int64, duration time.Duration, logger *slog.Logger) (*ScorecardResult, error) {
	result, err := persistScorecard(ctx, store, repoID, raw, mode, logger)
	if err != nil {
		return nil, err
	}
	result.APICalls = apiCalls
	result.Duration = duration

	logger.Info("scorecard complete",
		"repo_id", repoID,
		"mode", mode,
		"overall_score", raw.Score,
		"checks", len(result.Checks),
		"written", !result.Discarded,
		"api_calls_used", apiCalls,
		"duration", duration)
	return result, nil
}

// invokeScorecard is the INVOKE half of the v0.27.5 split: build the
// scorecard command for one mode, run it under a per-attempt wall-clock
// timeout, and parse the JSON output. It never touches the database —
// persistScorecard is the other half, so the remote/local fallback can
// compose attempts freely.
//
// localPath selects the mode: non-empty = local (--local localPath),
// empty = remote (--repo repoURL).
func invokeScorecard(ctx context.Context, scorecardPath string, repoID int64, repoURL string, localPath string, timeout time.Duration, githubToken string, logger *slog.Logger) (out *scorecardOutput, invokeErr error) {
	// Per-attempt wall-clock cap (v0.27.5). The pre-v0.27.5 remote
	// mode could hang for DAYS: scorecard sleeps through rate-limit
	// resets when its token is drained. On expiry cmd.Cancel SIGKILLs
	// the whole process group.
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Per-ATTEMPT outcome log (2026-09-11, F2). "scorecard complete"
	// fires once, only on success, for the whole remote-then-local
	// sequence — so a repo that burned a full remote timeout before
	// succeeding locally looked identical to one that succeeded
	// immediately, and a repo that failed both attempts logged no cost
	// at all. Measured consequence: 4,608 repos hit the full 15-minute
	// cap in 5.2 days (~1,152 worker-hours, ~18% of collection
	// capacity) and the log could neither count nor attribute it.
	//
	// Named returns + one deferred site so every exit path reports,
	// including the ones that return early. timed_out separates the
	// wall-clock cohort from parse failures — different causes,
	// different fixes.
	attemptMode := "remote"
	if localPath != "" {
		attemptMode = "local"
	}
	attemptStart := time.Now()
	defer func() {
		logger.Info("scorecard attempt",
			"repo_id", repoID,
			"mode", attemptMode,
			"ok", invokeErr == nil,
			"timed_out", errors.Is(attemptCtx.Err(), context.DeadlineExceeded),
			"duration", time.Since(attemptStart),
			"timeout_cap", timeout,
			"error", invokeErr)
	}()

	var cmd *exec.Cmd
	if localPath != "" {
		// Local mode: run against existing clone. The temp clone's
		// origin points to the bare repo (a local path), not to the
		// actual GitHub/GitLab URL. Fix it so scorecard can resolve
		// the remote for any API-dependent checks that still run
		// locally. Failure is non-fatal: the pure-local checks
		// (Binary-Artifacts, Pinned-Dependencies, ...) work regardless,
		// and the mode decision belongs to the CALLER (v0.27.5) — the
		// invoke half never silently switches modes.
		if err := setRemoteOrigin(attemptCtx, localPath, repoURL); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err // shutdown, not a failure (pass 36)
			}
			logger.Warn("failed to set remote origin for local scorecard — proceeding with local checks only",
				"repo_id", repoID, "error", err)
		}
		logger.Info("running OpenSSF Scorecard (local mode)", "repo_id", repoID, "path", localPath)
		cmd = exec.CommandContext(attemptCtx, scorecardPath,
			"--local", localPath,
			"--format", "json",
		)
	} else {
		logger.Info("running OpenSSF Scorecard (remote mode)", "repo_id", repoID, "url", repoURL)
		cmd = exec.CommandContext(attemptCtx, scorecardPath,
			"--repo", repoURL,
			"--format", "json",
		)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Scorecard needs GITHUB_TOKEN for API-dependent checks. May be a
	// comma-separated multi-token list (round-robined per request).
	cmd.Env = append(cmd.Environ(), "GITHUB_TOKEN="+githubToken)

	// v0.23.3: process-group cleanup. Same shape as scancode runOne.
	// scorecard spawns its own git subprocess (in remote mode) plus
	// various check probes that survive as orphans when only the
	// immediate child is killed via ctx cancel. Setpgid puts the
	// whole subprocess tree into its own pgid; cmd.Cancel kills the
	// group on ctx cancel; WaitDelay bounds the post-cancel block.
	// Operator-reported on 2026-05-21: scorecard ghosts consume CPU
	// and memory after aveloxis stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

	// v0.23.7: split cmd.Run into Start + Wait so we have a PID for
	// the deferred straggler kill. The v0.23.3 cmd.Cancel only fires
	// on ctx-cancel while Wait is blocked; if scorecard exits
	// normally before ctx cancels, its leftover children (git
	// subprocess in remote mode, git-lfs, check-probe spawns)
	// survive. defer syscall.Kill(-pid, SIGKILL) catches them on
	// function exit. Idempotent — ESRCH on an already-dead pgid is
	// harmless.
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting scorecard: %w", err)
	}
	pid := cmd.Process.Pid
	defer syscall.Kill(-pid, syscall.SIGKILL)
	runErr := cmd.Wait()

	// Parse the JSON output regardless of exit code. Scorecard exits with
	// status 1 when individual checks fail (e.g., invalid YAML in workflow
	// files), but still produces valid JSON with scores for successful checks.
	// Only error if there's no parseable output at all.
	var raw scorecardOutput
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			// stderr often carries the reason a run went long (rate-limit
			// waits, a check retrying) right up to the SIGKILL, so it is
			// the cheapest evidence available for the 4,608-repo
			// timeout cohort.
			return nil, fmt.Errorf("scorecard timed out after %v (wall-clock cap): %w: %s",
				timeout, attemptCtx.Err(), tailForError(stderr.String()))
		}
		if runErr != nil {
			return nil, fmt.Errorf("scorecard failed: %w: %s", execErr(attemptCtx, runErr), tailForError(stderr.String()))
		}
		// Exit 0 with unparseable stdout — the 2,492-repo cohort of the
		// 2026-09-11 analysis. This arm used to drop stderr on the
		// floor while its sibling above appended it, so for those repos
		// there was no evidence at all of what scorecard said. The
		// subprocess exiting 0 is exactly what makes stderr the only
		// remaining witness. stdout_bytes disambiguates "produced
		// nothing" from "produced something malformed".
		return nil, fmt.Errorf("parsing scorecard output (stdout_bytes=%d): %w: %s",
			stdout.Len(), err, tailForError(stderr.String()))
	}
	return &raw, nil
}

// scorecardStderrTailBytes bounds how much of a failed attempt's stderr
// rides along in the returned error. Scorecard is chatty when a run goes
// wrong (one line per retried check), and these errors reach
// collection_queue.last_error and the application log — an unbounded
// tail would be a per-repo blob in both. The TAIL, not the head: the
// operative message is the last thing written before the process gave
// up or was killed.
const scorecardStderrTailBytes = 2048

// tailForError trims stderr to the last scorecardStderrTailBytes,
// marking the truncation so a reader is never misled into thinking the
// captured fragment is the whole story.
func tailForError(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= scorecardStderrTailBytes {
		return s
	}
	return "...[truncated " + strconv.Itoa(len(s)-scorecardStderrTailBytes) + " bytes]... " +
		s[len(s)-scorecardStderrTailBytes:]
}

// persistScorecard is the PERSIST half of the v0.27.5 split: rotate the
// previous snapshot to history, store the __overall__ aggregate under
// the reserved row name (db.ScorecardOverallName — never mixed into the
// checks), then store each check row. Every row carries the mode
// marker, because remote and local check sets are not comparable.
//
// 2026-09-12 — a partial run never replaces a complete set (operator
// rule). Keyed on the stored scorecard_mode, not on check counts:
//
//	new run | stored | action
//	remote  | any    | replace (the best obtainable set)
//	local   | remote | REFUSE — keep the stored set, result.Discarded
//	local   | local  | replace (no better data exists; GitLab/generic
//	        | none   | are local-only, so this is their normal path)
//
// Before this the rotation was unconditional, so a 15-minute remote
// timeout followed by a successful local fallback replaced an 18-check
// set with an 11-check one — 10,214 repos on chaoss.tv held that
// 7-check deficit. Mode is the discriminator so the rule survives
// scorecard adding or removing a check, and needs no platform case.
// A probe ERROR is not "no prior set" (SR-5): nothing is written.
func persistScorecard(ctx context.Context, store scorecardStore, repoID int64, raw *scorecardOutput, mode string, logger *slog.Logger) (*ScorecardResult, error) {
	result := &ScorecardResult{
		OverallScore: raw.Score,
		Mode:         mode,
	}

	if mode != "remote" {
		stored, found, err := store.CurrentScorecardMode(ctx, repoID)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("scorecard current-mode probe failed — result NOT written",
					"repo_id", repoID, "mode", mode, "error", err)
			}
			return nil, fmt.Errorf("%w: %w", errScorecardModeProbe, err)
		}
		if found && stored == "remote" {
			result.Discarded = true
			for _, check := range raw.Checks {
				result.Checks = append(result.Checks, ScorecardCheck{
					Name: check.Name, Score: check.Score, Reason: check.Reason, Details: check.Details,
				})
			}
			logger.Info("scorecard degraded run discarded — prior complete set retained",
				"repo_id", repoID, "run_mode", mode, "stored_mode", stored,
				"run_checks", len(raw.Checks))
			return result, nil
		}
	}

	// Rotate previous scorecard results to history before inserting new ones.
	if err := store.RotateScorecardToHistory(ctx, repoID); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			logger.Warn("failed to rotate scorecard to history", "repo_id", repoID, "error", err)
		}
	}

	// Store the aggregate ("headline") score under the reserved
	// __overall__ row name — v0.27.4; it was previously logged and
	// dropped, which the operator called out as a gap. One decimal,
	// matching scorecard's own output.
	if err := store.InsertScorecardResult(ctx, repoID, db.ScorecardOverallName,
		fmt.Sprintf("%.1f", raw.Score), nil, mode); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			logger.Warn("failed to store scorecard overall score", "repo_id", repoID, "error", err)
		}
	}

	// Store each check as a row in repo_deps_scorecard.
	for _, check := range raw.Checks {
		sc := ScorecardCheck{
			Name:    check.Name,
			Score:   check.Score,
			Reason:  check.Reason,
			Details: check.Details,
		}
		result.Checks = append(result.Checks, sc)

		// Store in database with full check details as JSONB.
		detailsJSON, _ := json.Marshal(check)
		if err := store.InsertScorecardResult(ctx, repoID, check.Name, strconv.FormatFloat(check.Score, 'f', -1, 64), detailsJSON, mode); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("failed to store scorecard check", "check", check.Name, "error", err)
			}
		}
	}

	return result, nil
}

// setRemoteOrigin sets the git remote origin URL on a local clone so scorecard
// can resolve the remote for API-dependent checks. The temp clone's origin
// initially points to the bare repo (a local path), which scorecard can't use
// to determine the GitHub/GitLab API endpoint.
func setRemoteOrigin(ctx context.Context, repoPath, remoteURL string) error {
	cmd := exec.CommandContext(ctx, "git", "remote", "set-url", "origin", remoteURL)
	cmd.Dir = repoPath
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git remote set-url failed: %w: %s", execErr(ctx, err), stderr.String())
	}
	return nil
}

// rateLimitSnapshot is one before/after observation of the first
// token's GitHub API usage.
type rateLimitSnapshot struct {
	coreUsed    int64
	graphqlUsed int64
	ok          bool
}

// fetchRateLimitSnapshot GETs the GitHub /rate_limit endpoint with the
// given token and returns the used-counters for the core + graphql
// resources. NON-FATAL by contract: any failure (network, non-200,
// parse) returns ok=false and the run proceeds uninstrumented —
// instrumentation must never sink a scorecard run. The call itself is
// free (GitHub does not count /rate_limit against the rate limit).
func fetchRateLimitSnapshot(ctx context.Context, url, token string, logger *slog.Logger) rateLimitSnapshot {
	if token == "" {
		return rateLimitSnapshot{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Debug("rate_limit probe: build request failed", "error", err)
		return rateLimitSnapshot{}
	}
	req.Header.Set("Authorization", "token "+token)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Debug("rate_limit probe failed (non-fatal)", "error", err)
		return rateLimitSnapshot{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Debug("rate_limit probe: unexpected status (non-fatal)", "status", resp.StatusCode)
		return rateLimitSnapshot{}
	}
	var body struct {
		Resources struct {
			Core struct {
				Used int64 `json:"used"`
			} `json:"core"`
			GraphQL struct {
				Used int64 `json:"used"`
			} `json:"graphql"`
		} `json:"resources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		logger.Debug("rate_limit probe: decode failed (non-fatal)", "error", err)
		return rateLimitSnapshot{}
	}
	return rateLimitSnapshot{
		coreUsed:    body.Resources.Core.Used,
		graphqlUsed: body.Resources.GraphQL.Used,
		ok:          true,
	}
}

// rateLimitDelta computes api_calls_used from two snapshots. -1 =
// unknown (either probe failed — non-fatal per the instrumentation
// contract). The delta can only observe the FIRST token; with a
// multi-token GITHUB_TOKEN scorecard spreads calls across the list, so
// this is a lower-bound sample, not an exact total.
func rateLimitDelta(before, after rateLimitSnapshot) int64 {
	if !before.ok || !after.ok {
		return -1
	}
	return (after.coreUsed - before.coreUsed) + (after.graphqlUsed - before.graphqlUsed)
}

// scorecardOutput is the JSON structure output by `scorecard --format json`.
type scorecardOutput struct {
	Score  float64 `json:"score"`
	Checks []struct {
		Name    string   `json:"name"`
		Score   float64  `json:"score"` // fractional check scores must not break the parse
		Reason  string   `json:"reason"`
		Details []string `json:"details"`
	} `json:"checks"`
}
