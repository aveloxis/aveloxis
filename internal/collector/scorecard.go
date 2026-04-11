// Package collector — scorecard.go runs the OpenSSF Scorecard tool against
// a repository during the analysis phase.
//
// The scorecard binary (https://github.com/ossf/scorecard) must be installed
// and available on PATH. If not found, this phase is silently skipped.
//
// Local execution mode (preferred): When a localPath is provided, scorecard
// runs against the existing temporary clone using --local, avoiding a redundant
// git clone. This is significantly faster — similar to how Augur ran scorecard
// locally. The temp clone's git remote is set to the actual repo URL so
// scorecard can resolve it for API-dependent checks (Code-Review, Maintained,
// Branch-Protection, etc.).
//
// Remote fallback: When localPath is empty, scorecard clones the repo itself
// via --repo (original behavior, slower).
//
// Assumptions:
//   - The `scorecard` binary is installed (e.g., via `aveloxis install-tools`)
//   - It requires a GITHUB_TOKEN env var for API-dependent checks
//   - Local mode still makes some GitHub API calls (~20-50 vs ~150-300 in remote mode)
//   - The temp clone's origin remote must point to the actual repo URL (not the bare repo)
package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"

	"github.com/augurlabs/aveloxis/internal/db"
)

// ScorecardResult holds the parsed output from the scorecard tool.
type ScorecardResult struct {
	OverallScore float64          `json:"score"`
	Checks       []ScorecardCheck `json:"checks"`
}

// ScorecardCheck is a single scorecard check result.
type ScorecardCheck struct {
	Name    string `json:"name"`
	Score   int    `json:"score"`
	Reason  string `json:"reason"`
	Details []string `json:"details,omitempty"`
}

// RunScorecard executes the OpenSSF Scorecard tool against a repo and stores
// results in repo_deps_scorecard. Requires the `scorecard` binary on PATH
// and a GitHub API token.
//
// When localPath is non-empty, scorecard runs in local mode (--local) against
// the existing clone, which is much faster because it skips cloning and runs
// many checks locally. The git remote origin is set to repoURL so scorecard
// can resolve the remote for API-dependent checks.
//
// When localPath is empty, scorecard runs in remote mode (--repo) and clones
// the repo itself (original, slower behavior).
func RunScorecard(ctx context.Context, store *db.PostgresStore, repoID int64, repoURL string, localPath string, githubToken string, logger *slog.Logger) (*ScorecardResult, error) {
	// Check if scorecard is installed.
	scorecardPath, err := exec.LookPath("scorecard")
	if err != nil {
		logger.Info("scorecard not installed, skipping OpenSSF Scorecard analysis",
			"install", "aveloxis install-tools")
		return nil, nil
	}

	// Build the scorecard command based on whether we have a local clone.
	var cmd *exec.Cmd
	if localPath != "" {
		// Local mode: run against existing clone. Much faster — no redundant
		// git clone, and many checks (Binary-Artifacts, Pinned-Dependencies,
		// Dangerous-Workflow, etc.) run purely locally.
		//
		// The temp clone's origin points to the bare repo (local path), not
		// to the actual GitHub/GitLab URL. Fix this so scorecard can resolve
		// the remote for API-dependent checks (Code-Review, Maintained, etc.).
		if err := setRemoteOrigin(ctx, localPath, repoURL); err != nil {
			logger.Warn("failed to set remote origin for local scorecard, falling back to remote mode",
				"repo_id", repoID, "error", err)
			// Fall through to remote mode.
			localPath = ""
		}
	}

	if localPath != "" {
		logger.Info("running OpenSSF Scorecard (local mode)", "repo_id", repoID, "path", localPath)
		cmd = exec.CommandContext(ctx, scorecardPath,
			"--local", localPath,
			"--format", "json",
		)
	} else {
		logger.Info("running OpenSSF Scorecard (remote mode)", "repo_id", repoID, "url", repoURL)
		cmd = exec.CommandContext(ctx, scorecardPath,
			"--repo", repoURL,
			"--format", "json",
		)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Scorecard needs GITHUB_TOKEN for API-dependent checks.
	cmd.Env = append(cmd.Environ(), "GITHUB_TOKEN="+githubToken)

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("scorecard failed: %w: %s", err, stderr.String())
	}

	// Parse the JSON output.
	var raw scorecardOutput
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("parsing scorecard output: %w", err)
	}

	result := &ScorecardResult{
		OverallScore: raw.Score,
	}

	// Rotate previous scorecard results to history before inserting new ones.
	if err := store.RotateScorecardToHistory(ctx, repoID); err != nil {
		logger.Warn("failed to rotate scorecard to history", "repo_id", repoID, "error", err)
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
		if err := store.InsertScorecardResult(ctx, repoID, check.Name, fmt.Sprintf("%d", check.Score), detailsJSON); err != nil {
			logger.Warn("failed to store scorecard check", "check", check.Name, "error", err)
		}
	}

	logger.Info("scorecard complete",
		"repo_id", repoID,
		"overall_score", raw.Score,
		"checks", len(raw.Checks),
		"mode", scorecardMode(localPath))

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
		return fmt.Errorf("git remote set-url failed: %w: %s", err, stderr.String())
	}
	return nil
}

// scorecardMode returns a human-readable mode label for logging.
func scorecardMode(localPath string) string {
	if localPath != "" {
		return "local"
	}
	return "remote"
}

// scorecardOutput is the JSON structure output by `scorecard --format json`.
type scorecardOutput struct {
	Score  float64 `json:"score"`
	Checks []struct {
		Name    string   `json:"name"`
		Score   int      `json:"score"`
		Reason  string   `json:"reason"`
		Details []string `json:"details"`
	} `json:"checks"`
}
