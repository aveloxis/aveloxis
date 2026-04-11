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
	"fmt"
	"log/slog"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// BreadthWorker discovers cross-repo activity for contributors.
type BreadthWorker struct {
	store  *db.PostgresStore
	http   *platform.HTTPClient
	logger *slog.Logger
}

// NewBreadthWorker creates a breadth worker using the GitHub API.
func NewBreadthWorker(store *db.PostgresStore, keys *platform.KeyPool, logger *slog.Logger) *BreadthWorker {
	return &BreadthWorker{
		store:  store,
		http:   platform.NewHTTPClient("https://api.github.com", keys, logger),
		logger: logger,
	}
}

// BreadthResult tracks statistics for a breadth run.
type BreadthResult struct {
	ContributorsProcessed int
	EventsDiscovered      int
	EventsInserted        int
	Errors                int
}

// Run processes contributors that need breadth collection.
// It prioritizes contributors that have never been processed (NULL data_collection_date
// in contributor_repo), then those with the oldest collection dates.
// limit controls how many contributors to process per run (0 = all).
func (bw *BreadthWorker) Run(ctx context.Context, limit int) (*BreadthResult, error) {
	result := &BreadthResult{}

	contribs, err := bw.store.GetContributorsForBreadth(ctx, limit)
	if err != nil {
		return result, fmt.Errorf("querying contributors for breadth: %w", err)
	}

	if len(contribs) == 0 {
		return result, nil
	}

	bw.logger.Info("contributor breadth starting", "contributors", len(contribs))

	for _, c := range contribs {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		n, err := bw.processContributor(ctx, c)
		if err != nil {
			bw.logger.Warn("breadth: failed to process contributor",
				"login", c.Login, "error", err)
			result.Errors++
			continue
		}

		result.ContributorsProcessed++
		result.EventsInserted += n

		// Small delay between contributors to be respectful.
		time.Sleep(200 * time.Millisecond)
	}

	bw.logger.Info("contributor breadth complete",
		"processed", result.ContributorsProcessed,
		"events_inserted", result.EventsInserted,
		"errors", result.Errors)

	return result, nil
}

// processContributor fetches events for a single contributor and inserts new ones.
func (bw *BreadthWorker) processContributor(ctx context.Context, c db.BreadthContributor) (int, error) {
	// Get the newest event we already have for this contributor.
	newestEvent, err := bw.store.GetNewestContributorRepoEvent(ctx, c.ID)
	if err != nil {
		return 0, err
	}

	inserted := 0
	page := 1

	for page <= 10 { // GitHub events API max 10 pages (300 events)
		path := fmt.Sprintf("/users/%s/events?per_page=30&page=%d", c.Login, page)
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
				CntrbID:      c.ID,
				RepoGit:      event.Repo.URL,
				RepoName:     event.Repo.Name,
				GHRepoID:     event.Repo.ID,
				Category:     event.Type,
				EventID:      event.ID,
				CreatedAt:    eventTime,
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
	ID        int64  `json:"id,string"`
	Type      string `json:"type"` // PushEvent, PullRequestEvent, IssuesEvent, etc.
	Repo      struct {
		ID   int64  `json:"id"`
		Name string `json:"name"` // "owner/repo"
		URL  string `json:"url"`  // "https://api.github.com/repos/owner/repo"
	} `json:"repo"`
	CreatedAt string `json:"created_at"` // RFC3339
}
