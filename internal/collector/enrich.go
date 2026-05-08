// Package collector — enrich.go enriches thin contributor records with full
// profile data from the platform API (GET /users/{login}).
//
// The Contributors API (/repos/{owner}/{repo}/contributors) only returns basic
// data (login, avatar, type). Contributors discovered lazily from issue/PR/message
// UserRefs get even less. This enrichment phase calls the full user profile
// endpoint to populate company, location, email, name, and created_at.
//
// Enrichment runs incrementally after each collection pass: up to 500 thin
// contributors per run to avoid excessive API calls. Over multiple collection
// cycles, all contributors eventually get enriched.
package collector

import (
	"context"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// EnrichBatchSize limits how many contributors are enriched per collection pass.
// At 14,000 per pass, the current ~13K thin contributors will be fully enriched
// in a single pass. Each enrichment is one API call (GET /users/{login}), and
// with 73 GitHub tokens at 5,000 requests each, 14K calls is well within budget.
const EnrichBatchSize = 14000

// EnrichThinContributors finds contributors with missing profile data and
// enriches them via the platform API. Called by the scheduler's periodic
// enrichment ticker (v0.18.29 — moved off the per-job hot path).
//
// Accumulates enriched contributors into a slice and flushes via
// UpsertContributorBatch at the end. v0.18.28's per-login UpsertContributor
// loop produced ~14,000 single-row transactions per call; the batch path
// runs them under one transaction with in-memory dedup, an order-of-
// magnitude reduction in DB write traffic on a fleet-wide enrichment
// cycle.
func EnrichThinContributors(ctx context.Context, store *db.PostgresStore, resolver *db.ContributorResolver, client platform.Client, logger *slog.Logger) int {
	logins, err := resolver.GetThinContributorLogins(ctx, EnrichBatchSize)
	if err != nil {
		logger.Warn("failed to query thin contributors for enrichment", "error", err)
		return 0
	}
	if len(logins) == 0 {
		return 0
	}

	logger.Info("enriching thin contributor profiles", "count", len(logins))
	enriched := make([]model.Contributor, 0, len(logins))
	successLogins := make([]string, 0, len(logins))

	for _, login := range logins {
		contrib, err := client.EnrichContributor(ctx, login)
		if err != nil {
			// User may be deleted, suspended, or rate-limited. Still mark
			// as enriched to avoid retrying on the next pass — the user
			// likely won't become available within the cooldown window.
			if mErr := resolver.MarkContributorEnriched(ctx, login); mErr != nil {
				logger.Debug("failed to mark contributor enriched", "login", login, "error", mErr)
			}
			logger.Debug("failed to enrich contributor", "login", login, "error", err)
			continue
		}
		enriched = append(enriched, *contrib)
		successLogins = append(successLogins, login)
	}

	// Single batch flush. UpsertContributorBatch dedupes by login in
	// memory and runs one transaction.
	if len(enriched) > 0 {
		if err := store.UpsertContributorBatch(ctx, enriched); err != nil {
			logger.Warn("failed to flush enriched contributor batch",
				"count", len(enriched), "error", err)
			// Don't mark them enriched if the batch failed — they'll be
			// retried on the next periodic tick.
			return 0
		}
	}

	// Mark enrichment timestamps in a separate pass so users with genuinely
	// empty profiles (no company/location on GitHub) are not re-enriched
	// every cycle. Done after the batch flush so we only mark logins whose
	// data actually landed in the DB.
	for _, login := range successLogins {
		if mErr := resolver.MarkContributorEnriched(ctx, login); mErr != nil {
			logger.Debug("failed to mark contributor enriched", "login", login, "error", mErr)
		}
	}

	if len(enriched) > 0 {
		logger.Info("contributor enrichment complete", "enriched", len(enriched), "of", len(logins))
	}
	return len(enriched)
}
