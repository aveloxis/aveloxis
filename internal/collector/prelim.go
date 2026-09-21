// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
//     from the queue. We're already collecting on the canonical URL.
//     c. If no: updates the OLD repo's URL to the new canonical URL so
//     future collection uses the correct URL.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
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
	if errors.Is(err, platform.ErrRedirectTargetUserinfo) {
		// The forge's redirect target carries credentials (round 6): not
		// followed, nothing written, the row keeps its (clean) URL;
		// collection proceeds against the stored URL and fails naturally if
		// the forge insists.
		logger.Error("prelim: redirect target carries credentials — not followed, repo URL unchanged",
			"repo_id", repo.ID, "url", platform.RedactURLUserinfo(repo.GitURL), "error", err)
		return result, nil
	}
	if errors.Is(err, platform.ErrURLUserinfo) {
		// The STORED URL carries them (a caller that bypassed runJob's gate).
		logger.Error("prelim: repo URL carries credentials — not probed; correct repo_git",
			"repo_id", repo.ID, "url", platform.RedactURLUserinfo(repo.GitURL), "error", err)
		return result, nil
	}
	if err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			logger.Warn("prelim: failed to check URL", "url", platform.RedactURLUserinfo(repo.GitURL), "error", err)
		}
		// Network error — don't skip, let collection try and fail naturally.
		return result, nil
	}

	// Repo is gone (404, 410). Sideline it permanently: keep all collected
	// data, but remove from the queue so we never try again.
	if statusCode == http.StatusNotFound || statusCode == http.StatusGone {
		result.Skip = true
		result.SkipReason = fmt.Sprintf("repo returned %d — sidelined permanently", statusCode)
		logger.Warn("prelim: repo no longer exists, sidelining permanently",
			"url", platform.RedactURLUserinfo(repo.GitURL), "status", statusCode, "repo_id", repo.ID)

		// Mark as archived AND gone in one statement. v0.27.39:
		// dequeuing WITHOUT the archive succeeding mints a stranded
		// row (non-archived, queue-less, invisible forever — the
		// reconcile-repos class). If marking fails, keep the queue
		// row; the next cycle retries both. v0.28.1 (A6): the gone
		// stamp is what lets the GUI say "no longer publicly
		// available on GitHub" instead of misreading the dequeued
		// state as "queued for first collection" (the
		// department-of-veterans-affairs incident: 477 privatized
		// repos, 430 with real data behind a false queued banner).
		if err := store.MarkRepoGone(ctx, repo.ID); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("prelim: failed to mark dead repo gone — keeping queue row so the next cycle retries",
					"repo_id", repo.ID, "error", err)
			}
			return result, nil
		}
		// Remove from queue entirely — this repo will never be collected again
		// unless manually re-added.
		if err := store.DequeueRepo(ctx, repo.ID); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("prelim: failed to dequeue dead repo", "repo_id", repo.ID, "error", err)
			}
		}
		return result, nil
	}

	// v0.28.1 (A6): a DEFINITIVE 2xx probe means the repo is reachable
	// — clear a stale gone stamp so resurrection is symmetric (org
	// re-publicized, repo re-added/re-enqueued). The store's
	// IS NOT NULL guard makes this a 0-row no-op for the normal
	// fleet. 2xx ONLY (SR-16, matching mark-gone-repos' probe rule):
	// indeterminate statuses — 403, 429, 5xx — prove nothing and must
	// not flip a gone-stamped repo back to the false queued banner
	// mid-outage.
	if statusCode >= 200 && statusCode < 300 {
		if err := store.ClearRepoGone(ctx, repo.ID); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("prelim: failed to clear repo_gone_at", "repo_id", repo.ID, "error", err)
			}
		}
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
		"old_url", platform.RedactURLUserinfo(repo.GitURL), "new_url", platform.RedactURLUserinfo(finalURL), "repo_id", repo.ID)

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
			"old_repo_id", repo.ID, "old_url", platform.RedactURLUserinfo(repo.GitURL),
			"new_repo_id", existingID, "new_url", platform.RedactURLUserinfo(finalURL))

		// v0.27.22 self-heal: an add-by-old-name should silently land
		// the user on the collected repo they meant. For a
		// NEVER-COLLECTED duplicate, repoint its user_repos +
		// user_repo_stars links onto the winner and delete the
		// dataless row (queue/status/staging included). healed=false =
		// the duplicate has collected data (or unexpected children) —
		// that's a deliberate-consolidation problem, so fall back to
		// the legacy skip+dequeue and leave both rows.
		healed, healErr := store.HealRenamedDuplicate(ctx, repo.ID, existingID)
		switch {
		case healErr != nil:
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(healErr, context.Canceled) {
				logger.Warn("prelim: rename-duplicate heal failed — falling back to dequeue",
					"old_repo_id", repo.ID, "new_repo_id", existingID, "error", healErr)
			}
		case healed:
			logger.Info("prelim: rename-duplicate healed — user links repointed to the collected repo",
				"old_repo_id", repo.ID, "new_repo_id", existingID, "new_url", platform.RedactURLUserinfo(finalURL))
			return result, nil // duplicate row is gone; nothing to dequeue
		default:
			logger.Warn("prelim: duplicate retained — it has collected data; consolidation is a manual decision",
				"old_repo_id", repo.ID, "new_repo_id", existingID)
		}

		// Remove the old entry from the queue so we don't keep checking it.
		if err := store.DequeueRepo(ctx, repo.ID); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("prelim: failed to dequeue duplicate repo", "repo_id", repo.ID, "error", err)
			}
		}
		return result, nil
	}

	// New URL is not yet tracked — update the old repo's URL and fix all stored
	// URLs (issue html_urls, PR urls, etc.) that contain the old org/repo path.
	if err := store.UpdateRepoURLs(ctx, repo.ID, repo.GitURL, finalURL); err != nil {
		return result, fmt.Errorf("updating repo URLs: %w", err)
	}
	logger.Info("prelim: updated repo URL to canonical",
		"repo_id", repo.ID, "old_url", platform.RedactURLUserinfo(repo.GitURL), "new_url", platform.RedactURLUserinfo(finalURL))

	// Update the repo struct so collection uses the new URL.
	repo.GitURL = finalURL
	// Re-parse owner/name from the new URL (shared parser, v0.25.32).
	// On parse failure the old owner/name are kept — same guard the
	// deleted inline parseOwnerName provided via empty returns.
	if ru, perr := platform.ParseAnyRepoURL(finalURL); perr == nil {
		if ru.Owner != "" {
			repo.Owner = ru.Owner
		}
		if ru.Repo != "" {
			repo.Name = ru.Repo
		}
	}

	return result, nil
}

// resolveRedirects follows HTTP redirects and returns the final URL and status.
// ResolveRedirectTarget resolves a repo URL's redirect chain and
// returns (finalURL, statusCode). Exported for v0.27.39's
// reconcile-repos, which classifies stranded repos by live redirect
// (the same signal prelim uses at collection time).
func ResolveRedirectTarget(ctx context.Context, repoURL string) (string, int, error) {
	return resolveRedirects(ctx, repoURL)
}

func resolveRedirects(ctx context.Context, repoURL string) (string, int, error) {
	// The HEAD below would send userinfo as basic auth. Refused HERE, in the
	// one probe every caller shares (SR-18) — the scheduler's gates sat at
	// its callers, and two CLIs (`mark-gone-repos`, `reconcile-repos`) reached
	// it ungated (v0.29.57 fix-review round 3).
	if err := platform.RefuseURLUserinfo(repoURL); err != nil {
		return "", 0, err
	}
	// The redirect chain is walked HERE, on the transport, not by
	// http.Client: a Location that carries credentials must be refused
	// before any request to it (net/http would send them as basic auth, and
	// the final URL would then be written to repo_git by the rename path),
	// and http.Client's "failed to parse Location header" error quotes the
	// raw Location — raised BEFORE CheckRedirect is consulted, so no
	// redirect policy can prevent it (fix-review round 7; the hop-limit
	// error names the LAST REQUEST, not the refused Location — round 8
	// checked by running the old code). Walking the chain here makes both
	// fixed strings. Each Location is refused, then parsed, then refused
	// again once resolved; no error returned from here names a redirect
	// target.
	const maxHops = 10
	current := repoURL
	for hop := 0; ; hop++ {
		resp, err := headWithRetry(ctx, current)
		if err != nil {
			return "", 0, err
		}
		resp.Body.Close()
		if !isRedirectStatus(resp.StatusCode) {
			return current, resp.StatusCode, nil
		}
		loc := resp.Header.Get("Location")
		if loc == "" {
			return current, resp.StatusCode, nil // a 3xx without Location: as net/http reports it
		}
		if hop >= maxHops {
			return "", 0, errors.New("too many redirects")
		}
		if platform.RefuseURLUserinfo(loc) != nil {
			return "", 0, platform.ErrRedirectTargetUserinfo
		}
		next, err := resp.Request.URL.Parse(loc)
		if err != nil {
			return "", 0, errors.New("redirect target: unparseable Location header")
		}
		if platform.RefuseURLUserinfo(next.String()) != nil {
			return "", 0, platform.ErrRedirectTargetUserinfo
		}
		current = next.String()
	}
}

// isRedirectStatus reports the statuses net/http follows for a HEAD.
func isRedirectStatus(code int) bool {
	switch code {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// probeTransport is the redirect probe's transport: one round trip per
// hop, no redirect handling (that is resolveRedirects' job). The 15 s
// budget in headWithRetry is per HOP, where the http.Client's covered the
// whole chain; ResponseHeaderTimeout covers write-to-headers only.
var probeTransport http.RoundTripper = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	ResponseHeaderTimeout: 15 * time.Second,
	IdleConnTimeout:       90 * time.Second,
}

// headWithRetry issues one HEAD to url on the transport (a redirect
// response is returned as is, never followed or parsed here), retrying
// transient DNS/network errors with exponential backoff (1s, 3s, 9s): during
// system crashes or network blips DNS resolution fails briefly, and every
// prelim check in that window would otherwise permanently skip its repo.
// url has passed RefuseURLUserinfo, so an error's quoted URL carries no
// credential.
func headWithRetry(ctx context.Context, url string) (*http.Response, error) {
	var lastErr error
	delays := []time.Duration{0, 1 * time.Second, 3 * time.Second, 9 * time.Second}
	for attempt, delay := range delays {
		if attempt > 0 {
			time.Sleep(delay)
		}
		hopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		req, err := http.NewRequestWithContext(hopCtx, http.MethodHead, url, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("User-Agent", "Aveloxis/1.0")
		resp, err := probeTransport.RoundTrip(req)
		cancel() // a HEAD's body is empty; the response is complete
		if err != nil {
			lastErr = err
			// Only retry on DNS/network errors, not on context cancellation.
			if ctx.Err() != nil {
				return nil, err
			}
			if isTransientNetError(err) && attempt < len(delays)-1 {
				continue // retry
			}
			return nil, err
		}
		return resp, nil
	}
	return nil, lastErr
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

// The inline parseOwnerName helper that used to live here was
// consolidated into platform.ParseAnyRepoURL (v0.25.32) — one shared
// owner/name parser for prelim, the web store, and URL rewrites.
