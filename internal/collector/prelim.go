// Package collector — prelim.go implements the preliminary phase that detects
// repo redirects (renames/transfers) before collection begins.
//
// When a GitHub/GitLab repo is renamed or transferred, the old URL returns
// a 301 redirect to the new URL. If we blindly follow it, we may end up
// collecting the same repo twice under two different URLs. This phase:
//
//  1. Sends an HTTP HEAD to the repo URL.
//  2. If the final URL differs from the stored URL (redirect followed):
//     a. Checks if we already have a repo entry for the NEW URL.
//     b. If yes: marks the OLD repo as archived/duplicate and removes it
//        from the queue. We're already collecting on the canonical URL.
//     c. If no: updates the OLD repo's URL to the new canonical URL so
//        future collection uses the correct URL.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
)

// PrelimResult describes what the prelim phase found.
type PrelimResult struct {
	// Skip is true if this repo should not be collected (dead or duplicate).
	Skip       bool
	SkipReason string
	// Redirected is true if the URL changed.
	Redirected bool
	OldURL     string
	NewURL     string
}

// RunPrelim checks whether a repo has moved, died, or is a duplicate.
func RunPrelim(ctx context.Context, store *db.PostgresStore, repo *model.Repo, logger *slog.Logger) (*PrelimResult, error) {
	result := &PrelimResult{OldURL: repo.GitURL}

	finalURL, statusCode, err := resolveRedirects(ctx, repo.GitURL)
	if err != nil {
		logger.Warn("prelim: failed to check URL", "url", repo.GitURL, "error", err)
		// Network error — don't skip, let collection try and fail naturally.
		return result, nil
	}

	// Repo is gone (404, 410). Sideline it permanently: keep all collected
	// data, but remove from the queue so we never try again.
	if statusCode == http.StatusNotFound || statusCode == http.StatusGone {
		result.Skip = true
		result.SkipReason = fmt.Sprintf("repo returned %d — sidelined permanently", statusCode)
		logger.Warn("prelim: repo no longer exists, sidelining permanently",
			"url", repo.GitURL, "status", statusCode, "repo_id", repo.ID)

		// Mark as archived so queries can filter it out.
		if err := store.ArchiveRepo(ctx, repo.ID); err != nil {
			logger.Warn("prelim: failed to archive repo", "repo_id", repo.ID, "error", err)
		}
		// Remove from queue entirely — this repo will never be collected again
		// unless manually re-added.
		if err := store.DequeueRepo(ctx, repo.ID); err != nil {
			logger.Warn("prelim: failed to dequeue dead repo", "repo_id", repo.ID, "error", err)
		}
		return result, nil
	}

	// Normalize URLs for comparison.
	oldNorm := normalizeRepoURL(repo.GitURL)
	newNorm := normalizeRepoURL(finalURL)

	if oldNorm == newNorm {
		return result, nil // no redirect, proceed normally
	}

	// URL changed — this is a redirect (repo was renamed or transferred).
	result.Redirected = true
	result.NewURL = finalURL
	logger.Info("prelim: repo redirected",
		"old", repo.GitURL, "new", finalURL, "repo_id", repo.ID)

	// Check if we already have a repo entry for the new URL.
	existingID, err := store.FindRepoByURL(ctx, finalURL)
	if err != nil {
		return result, fmt.Errorf("checking for existing repo at new URL: %w", err)
	}

	if existingID > 0 && existingID != repo.ID {
		// We already collect this repo under its new URL. The old entry is a duplicate.
		result.Skip = true
		result.SkipReason = fmt.Sprintf(
			"redirected to %s which is already collected as repo_id %d",
			finalURL, existingID)
		logger.Warn("prelim: duplicate repo detected — already collecting under new URL",
			"old_repo_id", repo.ID, "old_url", repo.GitURL,
			"new_repo_id", existingID, "new_url", finalURL)

		// Remove the old entry from the queue so we don't keep checking it.
		if err := store.DequeueRepo(ctx, repo.ID); err != nil {
			logger.Warn("prelim: failed to dequeue duplicate repo", "repo_id", repo.ID, "error", err)
		}
		return result, nil
	}

	// New URL is not yet tracked — update the old repo's URL and fix all stored
	// URLs (issue html_urls, PR urls, etc.) that contain the old org/repo path.
	if err := store.UpdateRepoURLs(ctx, repo.ID, repo.GitURL, finalURL); err != nil {
		return result, fmt.Errorf("updating repo URLs: %w", err)
	}
	logger.Info("prelim: updated repo URL to canonical",
		"repo_id", repo.ID, "old", repo.GitURL, "new", finalURL)

	// Update the repo struct so collection uses the new URL.
	repo.GitURL = finalURL
	// Re-parse owner/name from new URL if it changed.
	newOwner, newName := parseOwnerName(finalURL)
	if newOwner != "" {
		repo.Owner = newOwner
	}
	if newName != "" {
		repo.Name = newName
	}

	return result, nil
}

// resolveRedirects follows HTTP redirects and returns the final URL and status.
func resolveRedirects(ctx context.Context, repoURL string) (string, int, error) {
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil // follow redirects
		},
	}

	// Retry transient DNS/network errors with exponential backoff (1s, 3s, 9s).
	// During system crashes or network blips, DNS resolution fails briefly and
	// all prelim checks that fire during that window would permanently skip repos.
	var lastErr error
	delays := []time.Duration{0, 1 * time.Second, 3 * time.Second, 9 * time.Second}
	for attempt, delay := range delays {
		if attempt > 0 {
			time.Sleep(delay)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, repoURL, nil)
		if err != nil {
			return "", 0, err
		}
		req.Header.Set("User-Agent", "Aveloxis/1.0")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			// Only retry on DNS/network errors, not on context cancellation.
			if ctx.Err() != nil {
				return "", 0, err
			}
			if isTransientNetError(err) && attempt < len(delays)-1 {
				continue // retry
			}
			return "", 0, err
		}
		resp.Body.Close()
		return resp.Request.URL.String(), resp.StatusCode, nil
	}
	return "", 0, lastErr
}

// isTransientNetError returns true for DNS resolution failures and connection
// refused errors that are likely to resolve on retry.
func isTransientNetError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "no such host") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "network is unreachable") ||
		strings.Contains(s, "i/o timeout")
}

// normalizeRepoURL strips protocol, trailing slashes, and .git suffix for comparison.
func normalizeRepoURL(u string) string {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	return strings.ToLower(u)
}

// parseOwnerName extracts owner and repo name from a URL like https://github.com/owner/repo.
func parseOwnerName(u string) (owner, name string) {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	parts := strings.Split(u, "/")
	if len(parts) < 3 {
		return "", ""
	}
	// parts[0] = host, parts[1..n-1] = owner path, parts[n] = repo name
	name = parts[len(parts)-1]
	owner = strings.Join(parts[1:len(parts)-1], "/")
	return owner, name
}
