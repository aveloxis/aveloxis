// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — breadth.go implements the contributor breadth worker.
//
// This is the Go implementation of Augur's contributor_breadth_worker.
// For each contributor with a GitHub login, it calls GET /users/{login}/events
// to discover what other repos they're active in. Each event is stored in the
// contributor_repo table, mapping contributors to repos outside the tracked set.
//
// This runs as a post-collection phase, after all issues/PRs/events are collected
// and contributors are resolved.
package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.22.12 — circuit-breaker tuning constants. The values were
// confirmed by the operator on 2026-05-18 after a transient GitHub
// /events incident produced 1,429 WARN entries over a few hours.
// With these values, the same incident would have triggered the
// circuit after the first ~20 failures and paused the worker until
// GitHub recovered.
const (
	// breadthCircuitBreakerThreshold is the count of CONSECUTIVE
	// transient-5xx-class errors that trips the circuit breaker.
	// Reset to 0 on any successful contributor fetch.
	breadthCircuitBreakerThreshold = 20

	// breadthCircuitBreakerPause is how long the circuit stays
	// open. The scheduler ticker calls Run periodically; once
	// circuitOpenUntil is set, subsequent Run calls return early
	// until time.Now() catches up.
	breadthCircuitBreakerPause = 1 * time.Hour
)

// BreadthWorker discovers cross-repo activity for contributors.
//
// v0.22.12 added circuitOpenUntil — when the v0.22.12 circuit
// breaker trips (threshold consecutive 5xx-class errors), Run sets
// this to time.Now().Add(breadthCircuitBreakerPause) and returns.
// Future Run calls return early until the deadline passes.
// breadthStore is the role interface for the store methods the breadth
// worker actually uses (v0.25.38, tech-debt Action 2). Narrowing from
// the 355-method *db.PostgresStore is what makes the circuit breaker
// behaviorally testable with a fake — the pattern to follow for other
// hot-path consumers (see also mailinglist_processor.go, distribution
// worker).
type breadthStore interface {
	GetContributorsForBreadth(ctx context.Context, limit int, cooldown time.Duration) ([]db.BreadthContributor, error)
	MarkBreadthAttempted(ctx context.Context, cntrbID string) error
	RenameContributorGhLogin(ctx context.Context, cntrbID, newLogin string, ghUserID int64) error
	GetNewestContributorRepoEvent(ctx context.Context, cntrbID string) (time.Time, error)
	InsertContributorRepo(ctx context.Context, row *db.ContributorRepoRow) error
}

type BreadthWorker struct {
	store            breadthStore
	http             *platform.HTTPClient
	logger           *slog.Logger
	circuitOpenUntil time.Time
}

// NewBreadthWorker creates a breadth worker using the GitHub API.
func NewBreadthWorker(store *db.PostgresStore, keys *platform.KeyPool, logger *slog.Logger) *BreadthWorker {
	return NewBreadthWorkerWithHTTP(store,
		platform.NewHTTPClient("https://api.github.com", keys, logger, platform.AuthGitHub), logger)
}

// NewBreadthWorkerWithHTTP builds a breadth worker around an arbitrary
// HTTP client and store implementation — the injectable form behavioral
// tests use (httptest server + fake store).
func NewBreadthWorkerWithHTTP(store breadthStore, http *platform.HTTPClient, logger *slog.Logger) *BreadthWorker {
	return &BreadthWorker{
		store:  store,
		http:   http,
		logger: logger,
	}
}

// noteContributorOutcome advances or resets the consecutive-transient
// counter for one contributor's fetch outcome and trips the breaker at
// the threshold (setting circuitOpenUntil). Returns true when THIS
// outcome tripped it.
//
// Extracted as a seam for behavioral testing (v0.25.38): a transient
// 5xx exhausts the HTTP client's full retry backoff inside a single
// Get call, so driving a real 20-failure storm through httptest takes
// hours — the trip/reset/deadline logic is tested directly instead.
// Per-user 404s and other non-transient errors leave the counter alone:
// they're legitimate per-contributor problems, not an incident signal.
func (bw *BreadthWorker) noteContributorOutcome(err error, consecutive *int, login string) bool {
	if err != nil && platform.ClassifyError(err) == platform.ClassTransient {
		*consecutive++
		if *consecutive >= breadthCircuitBreakerThreshold {
			bw.circuitOpenUntil = time.Now().Add(breadthCircuitBreakerPause)
			bw.logger.Warn("GitHub events endpoint appears unhealthy — pausing breadth worker",
				"consecutive_5xx", *consecutive,
				"threshold", breadthCircuitBreakerThreshold,
				"pause", breadthCircuitBreakerPause,
				"open_until", bw.circuitOpenUntil,
				"login", login,
				"last_error", err)
			return true
		}
	} else if err == nil {
		// Reset the consecutive counter on any success.
		*consecutive = 0
	}
	return false
}

// BreadthResult tracks statistics for a breadth run.
//
// v0.22.12 added Renames (count of detected renames via /user/{id}
// fallback on 404) and CircuitBreakerTripped (true when the run
// was aborted by the 5xx circuit breaker).
type BreadthResult struct {
	ContributorsProcessed int
	EventsDiscovered      int
	EventsInserted        int
	Errors                int
	Renames               int
	CircuitBreakerTripped bool
}

// Run processes contributors that need breadth collection.
//
// v0.20.17 changes:
//   - Cooldown-driven selection. GetContributorsForBreadth now
//     filters by cntrb_last_breadth_at; contributors past the
//     cooldown window are eligible regardless of whether their
//     prior attempt yielded events.
//   - MarkBreadthAttempted is called after EVERY contributor
//     attempt, success or zero-events. Without this, contributors
//     with no public activity stayed at the head of the queue
//     forever (observed: 225/1.4M coverage on the live fleet).
//   - The 200ms inter-contributor sleep is removed. HTTPClient
//     rate limiting already paces requests via X-RateLimit-Remaining
//     and 429 backoff; the sleep capped throughput at 5/sec
//     single-threaded against a 73-key 365K/hr budget.
//
// limit and cooldown are caller-supplied; the scheduler passes
// the configured BreadthBatchSize and BreadthCooldownDuration.
func (bw *BreadthWorker) Run(ctx context.Context, limit int, cooldown time.Duration) (*BreadthResult, error) {
	result := &BreadthResult{}

	// v0.22.12 circuit breaker: skip the whole cycle if a prior
	// run tripped the threshold and the pause window hasn't elapsed.
	// The check at the top means the scheduler ticker effectively
	// short-circuits during the 1h cool-off without us needing to
	// own a goroutine that sleeps.
	if !bw.circuitOpenUntil.IsZero() && time.Now().Before(bw.circuitOpenUntil) {
		bw.logger.Info("breadth circuit open — skipping cycle",
			"open_until", bw.circuitOpenUntil)
		result.CircuitBreakerTripped = true
		return result, nil
	}

	contribs, err := bw.store.GetContributorsForBreadth(ctx, limit, cooldown)
	if err != nil {
		return result, fmt.Errorf("querying contributors for breadth: %w", err)
	}

	if len(contribs) == 0 {
		return result, nil
	}

	bw.logger.Info("contributor breadth starting", "contributors", len(contribs))

	consecutive5xxFailures := 0

	for _, c := range contribs {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		n, err := bw.processContributor(ctx, c)

		// v0.22.12: detect transient-5xx burst BEFORE stamping
		// MarkBreadthAttempted. If the circuit trips on this
		// contributor, we want them to re-enter the queue on the
		// next cycle once GitHub recovers — stamping would push
		// them back behind the cooldown window unnecessarily.
		if bw.noteContributorOutcome(err, &consecutive5xxFailures, c.Login) {
			result.CircuitBreakerTripped = true
			return result, nil
		}

		// Stamp the attempt timestamp AFTER the circuit-breaker
		// check, but regardless of fetch success. The v0.20.17
		// invariant holds: a contributor with zero public events
		// still gets stamped so they exit the unprocessed-queue.
		if markErr := bw.store.MarkBreadthAttempted(ctx, c.ID); markErr != nil {
			bw.logger.Debug("breadth: failed to mark contributor attempted",
				"login", c.Login, "error", markErr)
		}

		if err != nil {
			bw.logger.Warn("breadth: failed to process contributor",
				"login", c.Login, "error", err)
			result.Errors++
			continue
		}

		result.ContributorsProcessed++
		result.EventsInserted += n
	}

	bw.logger.Info("contributor breadth complete",
		"processed", result.ContributorsProcessed,
		"events_inserted", result.EventsInserted,
		"errors", result.Errors,
		"renames", result.Renames)

	return result, nil
}

// processContributor fetches events for a single contributor and inserts new ones.
//
// v0.22.12: on 404 from /users/{login}/events AND a stored
// gh_user_id, look up /user/{id} to recover the current login.
// If the login changed, the rename is persisted via the existing
// store.RenameContributorGhLogin (which wraps loadMergeCandidates +
// pickMergeWinner per the v0.20.2 rename-merge contract), and the
// events fetch is retried once with the new login.
func (bw *BreadthWorker) processContributor(ctx context.Context, c db.BreadthContributor) (int, error) {
	inserted, err := bw.fetchEventsForLogin(ctx, c.ID, c.Login)
	if !errors.Is(err, platform.ErrNotFound) || c.GHUserID == 0 {
		return inserted, err
	}

	// 404 + we have a stable numeric id. Try the rename-detection
	// fallback: look up the current login via /user/{id}.
	newLogin, lookupErr := bw.lookupLoginByID(ctx, c.GHUserID)
	if lookupErr != nil {
		// /user/{id} also failed (404 — user genuinely deleted, or
		// transient 5xx). Bubble the original 404 — the caller's
		// MarkBreadthAttempted will stamp this contributor and the
		// cooldown will defer the next attempt.
		return inserted, err
	}
	if newLogin == "" || newLogin == c.Login {
		// /user/{id} returned the same login (?!) or empty. Either
		// way nothing to do — bubble the original 404.
		return inserted, err
	}

	bw.logger.Info("breadth: rename detected, updating stored gh_login",
		"cntrb_id", c.ID, "old_login", c.Login, "new_login", newLogin,
		"gh_user_id", c.GHUserID)
	if renameErr := bw.store.RenameContributorGhLogin(ctx, c.ID, newLogin, c.GHUserID); renameErr != nil {
		bw.logger.Warn("breadth: failed to persist rename — bubbling original 404",
			"cntrb_id", c.ID, "new_login", newLogin, "error", renameErr)
		return inserted, err
	}

	// Retry the events fetch with the new login. If this also 404s,
	// bubble that — but don't recurse into rename detection again.
	return bw.fetchEventsForLogin(ctx, c.ID, newLogin)
}

// lookupLoginByID resolves a stored numeric GitHub user ID to the
// CURRENT login via GitHub's by-id endpoint (/user/{id}). Returns
// the empty string if the user no longer exists. Used by the
// breadth worker's 404 rename-detection fallback.
func (bw *BreadthWorker) lookupLoginByID(ctx context.Context, ghUserID int64) (string, error) {
	path := fmt.Sprintf("/user/%d", ghUserID)
	resp, err := bw.http.Get(ctx, path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var u struct {
		Login string `json:"login"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return "", err
	}
	return u.Login, nil
}

// fetchEventsForLogin is the inline events-pagination loop pulled
// out so processContributor can call it twice (once with the stored
// login, once with the post-rename login).
func (bw *BreadthWorker) fetchEventsForLogin(ctx context.Context, cntrbID, login string) (int, error) {
	// Get the newest event we already have for this contributor.
	newestEvent, err := bw.store.GetNewestContributorRepoEvent(ctx, cntrbID)
	if err != nil {
		return 0, err
	}

	inserted := 0
	page := 1

	for page <= 10 { // GitHub events API max 10 pages (300 events)
		path := fmt.Sprintf("/users/%s/events?per_page=30&page=%d", login, page)
		resp, err := bw.http.Get(ctx, path)
		if err != nil {
			return inserted, err
		}

		var events []ghUserEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			resp.Body.Close()
			return inserted, err
		}
		resp.Body.Close()

		if len(events) == 0 {
			break
		}

		for _, event := range events {
			if event.Repo.URL == "" || event.Repo.Name == "" {
				continue
			}

			eventTime, parseErr := time.Parse(time.RFC3339, event.CreatedAt)
			if parseErr != nil {
				bw.logger.Warn("failed to parse event timestamp", "created_at", event.CreatedAt, "error", parseErr)
				continue
			}

			// Stop if we've reached events we already have.
			if !newestEvent.IsZero() && eventTime.Before(newestEvent) {
				return inserted, nil
			}

			err := bw.store.InsertContributorRepo(ctx, &db.ContributorRepoRow{
				CntrbID:   cntrbID,
				RepoGit:   event.Repo.URL,
				RepoName:  event.Repo.Name,
				GHRepoID:  event.Repo.ID,
				Category:  event.Type,
				EventID:   event.ID,
				CreatedAt: eventTime,
			})
			if err != nil {
				// Duplicate events are expected (ON CONFLICT DO NOTHING).
				continue
			}
			inserted++
		}

		page++
	}

	return inserted, nil
}

// ghUserEvent is a GitHub user event from GET /users/{login}/events.
type ghUserEvent struct {
	ID   int64  `json:"id,string"`
	Type string `json:"type"` // PushEvent, PullRequestEvent, IssuesEvent, etc.
	Repo struct {
		ID   int64  `json:"id"`
		Name string `json:"name"` // "owner/repo"
		URL  string `json:"url"`  // "https://api.github.com/repos/owner/repo"
	} `json:"repo"`
	CreatedAt string `json:"created_at"` // RFC3339
}
