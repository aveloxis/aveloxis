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
//
// v0.27.8 restructure — fetch/persist separation:
//
//   - FETCH is concurrent: a pool of fetcher goroutines (config
//     collection.breadth_fetch_concurrency, default 8) consumes the
//     cycle's contributor list from a channel and runs the pure
//     HTTP+parse side (fetchContributor), returning parsed
//     contributor_repo rows instead of inserting them. Pre-v0.27.8 the
//     loop was strictly sequential — one contributor, one request in
//     flight — so a 2.3M-contributor fleet crawled at HTTP-RTT speed
//     while the 73-key / 365K-req-hr pool sat idle.
//   - PERSIST is a single coordinator (the Run goroutine itself): it
//     consumes fetch outcomes, drives the circuit breaker, inserts each
//     contributor's events via the batched InsertContributorRepoBatch,
//     and stamps cntrb_last_breadth_at via chunked
//     MarkBreadthAttemptedBatch instead of one UPDATE per contributor.
//
// ORDERING CONTRACT (v0.27.8): a contributor is queued for
// mark-attempted only AFTER their events are durably inserted. A crash
// (or insert failure) between fetch and insert leaves them UNMARKED so
// the cooldown queue re-selects them next cycle; re-inserting is safe
// because contributor_repo's INSERT is ON CONFLICT DO NOTHING. The
// coordinator enforces this by appending to the pending-marks buffer
// strictly after the InsertContributorRepoBatch call for that
// contributor succeeds, and flushing marks only from that buffer.
package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
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
	// Reset to 0 on any successful contributor fetch. Under the
	// v0.27.8 concurrent shape, "consecutive" means consecutive in
	// coordinator arrival order — the coordinator is the single
	// consumer of fetch outcomes, so the counting stays serialized.
	breadthCircuitBreakerThreshold = 20

	// breadthCircuitBreakerPause is how long the circuit stays
	// open. The scheduler ticker calls Run periodically; once
	// circuitOpenUntil is set, subsequent Run calls return early
	// until time.Now() catches up.
	breadthCircuitBreakerPause = 1 * time.Hour

	// defaultBreadthFetchConcurrency is the fetcher-pool size used
	// when the worker isn't given an explicit value (matches
	// config.BreadthFetchConcurrencyOrDefault).
	breadthDefaultFetchConcurrency = 8

	// breadthMarkFlushSize is how many pending cntrb_ids the
	// coordinator buffers before flushing a MarkBreadthAttemptedBatch
	// chunk. Matches the store-side chunk size so one flush is one
	// UPDATE statement.
	breadthMarkFlushSize = 500
)

// breadthStore is the role interface for the store methods the breadth
// worker actually uses (v0.25.38, tech-debt Action 2). Narrowing from
// the 355-method *db.PostgresStore is what makes the circuit breaker
// behaviorally testable with a fake — the pattern to follow for other
// hot-path consumers (see also mailinglist_processor.go, distribution
// worker).
//
// v0.27.8: the per-row MarkBreadthAttempted / InsertContributorRepo
// methods are replaced by their batch forms — the worker no longer
// issues single-row writes on the hot path. The single-row store
// methods remain on *db.PostgresStore for other callers.
type breadthStore interface {
	GetContributorsForBreadth(ctx context.Context, limit int, cooldown time.Duration) ([]db.BreadthContributor, error)
	MarkBreadthAttemptedBatch(ctx context.Context, cntrbIDs []string) error
	RenameContributorGhLogin(ctx context.Context, cntrbID, newLogin string, ghUserID int64) error
	GetNewestContributorRepoEvent(ctx context.Context, cntrbID string) (time.Time, error)
	InsertContributorRepoBatch(ctx context.Context, rows []*db.ContributorRepoRow) error
}

// BreadthWorker discovers cross-repo activity for contributors.
//
// v0.22.12 added circuitOpenUntil — when the v0.22.12 circuit
// breaker trips (threshold consecutive 5xx-class errors), Run sets
// this to time.Now().Add(breadthCircuitBreakerPause) and returns.
// Future Run calls return early until the deadline passes.
//
// v0.27.8: the breaker state (consecutive5xx + circuitOpenUntil) is
// mutex-guarded. Within one Run the counting is already serialized by
// the single coordinator, but the mutex makes the worker safe even if
// a scheduler tick overlaps a still-running prior cycle.
type BreadthWorker struct {
	store            breadthStore
	http             *platform.HTTPClient
	logger           *slog.Logger
	fetchConcurrency int

	mu               sync.Mutex // guards consecutive5xx + circuitOpenUntil
	consecutive5xx   int
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
		store:            store,
		http:             http,
		logger:           logger,
		fetchConcurrency: breadthDefaultFetchConcurrency,
	}
}

// WithFetchConcurrency sets the fetcher-pool size (chainable, mirrors
// the StagedCollector.WithWorkers pattern). Non-positive values fall
// back to the default.
func (bw *BreadthWorker) WithFetchConcurrency(n int) *BreadthWorker {
	if n <= 0 {
		n = breadthDefaultFetchConcurrency
	}
	bw.fetchConcurrency = n
	return bw
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
//
// v0.27.8: the counter lives on the worker (mutex-guarded) instead of
// a caller-owned local, so the state is thread-safe under the
// concurrent fetch pool and across overlapping Run invocations.
func (bw *BreadthWorker) noteContributorOutcome(err error, login string) bool {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	if err != nil && platform.ClassifyError(err) == platform.ClassTransient {
		bw.consecutive5xx++
		if bw.consecutive5xx >= breadthCircuitBreakerThreshold {
			bw.circuitOpenUntil = time.Now().Add(breadthCircuitBreakerPause)
			bw.logger.Warn("GitHub events endpoint appears unhealthy — pausing breadth worker",
				"consecutive_5xx", bw.consecutive5xx,
				"threshold", breadthCircuitBreakerThreshold,
				"pause", breadthCircuitBreakerPause,
				"open_until", bw.circuitOpenUntil,
				"login", login,
				"last_error", err)
			return true
		}
	} else if err == nil {
		// Reset the consecutive counter on any success.
		bw.consecutive5xx = 0
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

// breadthFetchOutcome is what a fetcher goroutine hands the
// coordinator for one contributor: the parsed (not yet inserted)
// contributor_repo rows plus the fetch error, if any.
type breadthFetchOutcome struct {
	contributor db.BreadthContributor
	rows        []*db.ContributorRepoRow
	renamed     bool
	err         error
}

// Run processes contributors that need breadth collection.
//
// v0.20.17 changes:
//   - Cooldown-driven selection. GetContributorsForBreadth now
//     filters by cntrb_last_breadth_at; contributors past the
//     cooldown window are eligible regardless of whether their
//     prior attempt yielded events.
//   - MarkBreadthAttempted(Batch) covers EVERY contributor attempt —
//     success, zero events, and per-user fetch errors all count as
//     attempted. Without this, contributors with no public activity
//     stayed at the head of the queue forever (observed: 225/1.4M
//     coverage on the live fleet).
//   - The 200ms inter-contributor sleep is removed. HTTPClient
//     rate limiting already paces requests via X-RateLimit-Remaining
//     and 429 backoff; the sleep capped throughput at 5/sec
//     single-threaded against a 73-key 365K/hr budget.
//
// v0.27.8 changes:
//   - fetchConcurrency fetcher goroutines fetch contributors
//     concurrently; the Run goroutine is the single persistence
//     coordinator (see the package doc's ORDERING CONTRACT).
//   - A circuit trip cancels all fetchers promptly. Contributors
//     whose outcome the coordinator had not yet persisted when the
//     trip happened stay UNMARKED, so they re-enter the queue on the
//     next cycle once GitHub recovers — same semantics as the
//     sequential v0.22.12 shape.
//   - Marks flush via MarkBreadthAttemptedBatch in chunks of
//     breadthMarkFlushSize instead of one UPDATE per contributor
//     (18,000 single-row UPDATEs per cycle at fleet batch sizes).
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
	bw.mu.Lock()
	openUntil := bw.circuitOpenUntil
	bw.mu.Unlock()
	if !openUntil.IsZero() && time.Now().Before(openUntil) {
		bw.logger.Info("breadth circuit open — skipping cycle",
			"open_until", openUntil)
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

	concurrency := bw.fetchConcurrency
	if concurrency <= 0 {
		concurrency = breadthDefaultFetchConcurrency
	}
	if concurrency > len(contribs) {
		concurrency = len(contribs)
	}

	bw.logger.Info("contributor breadth starting",
		"contributors", len(contribs),
		"fetch_concurrency", concurrency)

	start := time.Now()

	// fetchCtx lets the coordinator stop ALL fetchers promptly on a
	// circuit trip or ctx cancellation without tearing down ctx itself
	// (the mark flush below still needs a live ctx on the trip path).
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()

	jobs := make(chan db.BreadthContributor)
	outcomes := make(chan breadthFetchOutcome)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer safego.Recover(bw.logger, "breadth-fetcher")
			for c := range jobs {
				if fetchCtx.Err() != nil {
					return
				}
				rows, renamed, fetchErr := bw.fetchContributor(fetchCtx, c)
				select {
				case outcomes <- breadthFetchOutcome{contributor: c, rows: rows, renamed: renamed, err: fetchErr}:
				case <-fetchCtx.Done():
					return
				}
			}
		}()
	}

	// Feeder: streams the contributor list into the pool; bails as
	// soon as the coordinator cancels.
	go func() {
		defer safego.Recover(bw.logger, "breadth-feeder")
		defer close(jobs)
		for _, c := range contribs {
			select {
			case jobs <- c:
			case <-fetchCtx.Done():
				return
			}
		}
	}()

	// Closer: outcomes is closed exactly once, after every fetcher
	// has exited, so the coordinator's range terminates.
	go func() {
		defer safego.Recover(bw.logger, "breadth-fetchers-wait")
		wg.Wait()
		close(outcomes)
	}()

	// ---- coordinator: the single persistence writer ----
	//
	// pendingMarks holds cntrb_ids whose events ARE durably inserted
	// (or who had none / a per-user fetch error) and are therefore
	// eligible for the attempted stamp. See ORDERING CONTRACT in the
	// package doc: IDs enter this buffer only after their
	// InsertContributorRepoBatch call succeeded.
	var pendingMarks []string
	flushMarks := func() {
		if len(pendingMarks) == 0 {
			return
		}
		markErr := bw.store.MarkBreadthAttemptedBatch(ctx, pendingMarks)
		if markErr != nil && !errors.Is(markErr, context.Canceled) { // shutdown is not a failure (pass 34)
			bw.logger.Warn("breadth: failed to mark contributors attempted",
				"count", len(pendingMarks), "error", markErr)
		}
		pendingMarks = pendingMarks[:0]
	}

	aborted := false // circuit trip or ctx cancel: drain outcomes without persisting
	var abortErr error
	for oc := range outcomes {
		if aborted {
			// Post-abort outcomes are neither inserted nor marked —
			// those contributors re-enter the queue next cycle,
			// matching the sequential shape where the loop stopped
			// at the tripping contributor.
			continue
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			aborted = true
			abortErr = ctxErr
			cancelFetch()
			continue
		}

		// v0.22.12: detect transient-5xx burst BEFORE stamping the
		// attempt. If the circuit trips on this contributor, they and
		// everything not yet persisted must re-enter the queue on the
		// next cycle once GitHub recovers — stamping would push them
		// behind the cooldown window unnecessarily.
		if bw.noteContributorOutcome(oc.err, oc.contributor.Login) {
			result.CircuitBreakerTripped = true
			aborted = true
			cancelFetch()
			continue
		}

		if oc.renamed {
			result.Renames++
		}

		// ORDERING CONTRACT: insert this contributor's events FIRST;
		// only a successful insert queues the mark. An insert failure
		// leaves the contributor unmarked so the next cycle retries
		// (idempotent — ON CONFLICT DO NOTHING). Rows are persisted
		// even when the fetch ended in an error: the sequential shape
		// inserted page-by-page, so a mid-pagination failure still
		// kept the earlier pages' events.
		if len(oc.rows) > 0 {
			insErr := bw.store.InsertContributorRepoBatch(ctx, oc.rows)
			if errors.Is(insErr, context.Canceled) {
				continue // shutdown, not a failure: unmarked, retried next cycle
			}
			if insErr != nil {
				bw.logger.Warn("breadth: failed to insert contributor events — leaving unmarked for retry",
					"login", oc.contributor.Login, "events", len(oc.rows), "error", insErr)
				result.Errors++
				continue
			}
			result.EventsInserted += len(oc.rows)
		}

		if oc.err != nil {
			bw.logger.Warn("breadth: failed to process contributor",
				"login", oc.contributor.Login, "error", oc.err)
			result.Errors++
			// v0.20.17 invariant: a per-user fetch error still counts
			// as attempted (only a circuit trip, an insert failure, or
			// a shutdown mid-insert leaves a contributor unmarked). A
			// canceled fetch never reaches here: the loop-top ctx check
			// aborts the drain first.
			pendingMarks = append(pendingMarks, oc.contributor.ID)
			if len(pendingMarks) >= breadthMarkFlushSize {
				flushMarks()
			}
			continue
		}

		result.ContributorsProcessed++
		pendingMarks = append(pendingMarks, oc.contributor.ID)
		if len(pendingMarks) >= breadthMarkFlushSize {
			flushMarks()
		}
	}

	// Contributors persisted before an abort keep their stamp — their
	// events are already durable, exactly like the sequential shape
	// where each pre-trip contributor was marked as it completed.
	flushMarks()

	if abortErr != nil {
		return result, abortErr
	}
	if result.CircuitBreakerTripped {
		return result, nil
	}

	elapsed := time.Since(start)
	attempted := result.ContributorsProcessed + result.Errors
	perSec := 0.0
	if elapsed > 0 {
		perSec = float64(attempted) / elapsed.Seconds()
	}
	bw.logger.Info("contributor breadth complete",
		"processed", result.ContributorsProcessed,
		"events_inserted", result.EventsInserted,
		"errors", result.Errors,
		"renames", result.Renames,
		"duration", elapsed.Round(time.Millisecond),
		"contributors_per_sec", perSec,
		"fetch_concurrency", concurrency)

	return result, nil
}

// fetchContributor is the pure fetch side for a single contributor:
// HTTP + parse only — it returns the contributor's new events as rows
// for the coordinator to persist and NEVER writes to contributor_repo
// itself (the one store write on this path, RenameContributorGhLogin,
// touches only this contributor's own contributors row and is safe to
// issue from concurrent fetchers — distinct contributors, distinct
// rows).
//
// v0.22.12: on 404 from /users/{login}/events AND a stored
// gh_user_id, look up /user/{id} to recover the current login.
// If the login changed, the rename is persisted via the existing
// store.RenameContributorGhLogin (which wraps loadMergeCandidates +
// pickMergeWinner per the v0.20.2 rename-merge contract), and the
// events fetch is retried once with the new login.
func (bw *BreadthWorker) fetchContributor(ctx context.Context, c db.BreadthContributor) ([]*db.ContributorRepoRow, bool, error) {
	rows, err := bw.fetchEventsForLogin(ctx, c.ID, c.Login)
	if !errors.Is(err, platform.ErrNotFound) || c.GHUserID == 0 {
		return rows, false, err
	}

	// 404 + we have a stable numeric id. Try the rename-detection
	// fallback: look up the current login via /user/{id}.
	newLogin, lookupErr := bw.lookupLoginByID(ctx, c.GHUserID)
	if lookupErr != nil {
		// /user/{id} also failed (404 — user genuinely deleted, or
		// transient 5xx). Bubble the original 404 — the coordinator
		// will stamp this contributor as attempted and the cooldown
		// will defer the next attempt.
		return rows, false, err
	}
	if newLogin == "" || newLogin == c.Login {
		// /user/{id} returned the same login (?!) or empty. Either
		// way nothing to do — bubble the original 404.
		return rows, false, err
	}

	bw.logger.Info("breadth: rename detected, updating stored gh_login",
		"cntrb_id", c.ID, "old_login", c.Login, "new_login", newLogin,
		"gh_user_id", c.GHUserID)
	if renameErr := bw.store.RenameContributorGhLogin(ctx, c.ID, newLogin, c.GHUserID); renameErr != nil {
		bw.logger.Warn("breadth: failed to persist rename — bubbling original 404",
			"cntrb_id", c.ID, "new_login", newLogin, "error", renameErr)
		return rows, false, err
	}

	// Retry the events fetch with the new login. If this also 404s,
	// bubble that — but don't recurse into rename detection again.
	retryRows, retryErr := bw.fetchEventsForLogin(ctx, c.ID, newLogin)
	return retryRows, true, retryErr
}

// lookupLoginByID resolves a stored numeric GitHub user ID to the
// CURRENT login via GitHub's by-id endpoint (/user/{id}). Returns
// the empty string if the user no longer exists. Used by the
// breadth worker's 404 rename-detection fallback.
func (bw *BreadthWorker) lookupLoginByID(ctx context.Context, ghUserID int64) (string, error) {
	path := fmt.Sprintf("/user/%d", ghUserID)
	// v0.28.18: ETag-free — a single-object reader cannot use a 304, and
	// the breadth worker's client lives for the whole process (v0.27.18).
	resp, err := bw.http.Get(platform.WithoutETag(ctx), path)
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

// fetchEventsForLogin is the events-pagination loop, pulled out so
// fetchContributor can call it twice (once with the stored login, once
// with the post-rename login). v0.27.8: it ACCUMULATES parsed rows and
// returns them instead of inserting — persistence belongs to the
// coordinator. The GetNewestContributorRepoEvent read stays here (pgx
// pool reads are safe from concurrent fetchers); it bounds pagination
// to events newer than what we already have.
func (bw *BreadthWorker) fetchEventsForLogin(ctx context.Context, cntrbID, login string) ([]*db.ContributorRepoRow, error) {
	// Get the newest event we already have for this contributor.
	newestEvent, err := bw.store.GetNewestContributorRepoEvent(ctx, cntrbID)
	if err != nil {
		return nil, err
	}

	var rows []*db.ContributorRepoRow
	page := 1

	for page <= 10 { // GitHub events API max 10 pages (300 events)
		path := fmt.Sprintf("/users/%s/events?per_page=30&page=%d", login, page)
		// v0.28.18: ETag-free. This is a manual page loop, not the
		// paginator, so a 304 on a repeat read surfaced as ErrNotModified —
		// a WARN + result.Errors for a normal "nothing new" outcome. And
		// treating 304 as "no events" would be wrong here: the v0.27.8
		// ordering contract retries a contributor whose event INSERT
		// failed, and that retry must see the body again (the previous
		// fetch's rows never landed). The cooldown is the pacer, not the
		// ETag cache.
		resp, err := bw.http.Get(platform.WithoutETag(ctx), path)
		if err != nil {
			return rows, err
		}

		var events []ghUserEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			resp.Body.Close()
			return rows, err
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
				return rows, nil
			}

			rows = append(rows, &db.ContributorRepoRow{
				CntrbID:   cntrbID,
				RepoGit:   event.Repo.URL,
				RepoName:  event.Repo.Name,
				GHRepoID:  event.Repo.ID,
				Category:  event.Type,
				EventID:   event.ID,
				CreatedAt: eventTime,
			})
		}

		page++
	}

	return rows, nil
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
