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

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// EnrichBatchSize limits how many contributors are enriched per collection pass.
// At 14,000 per pass, the current ~13K thin contributors will be fully enriched
// in a single pass. Each enrichment is one API call (GET /users/{login}), and
// with 73 GitHub tokens at 5,000 requests each, 14K calls is well within budget.
const EnrichBatchSize = 14000

// EnrichThinContributors finds contributors with missing profile data and
// enriches them via the platform API. Called after collection and gap fill.
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
	enriched := 0

	for _, login := range logins {
		contrib, err := client.EnrichContributor(ctx, login)
		if err != nil {
			// User may be deleted, suspended, or rate-limited. Still mark
			// as enriched to avoid retrying on the next pass — the user
			// likely won't become available within the cooldown window.
			resolver.MarkContributorEnriched(ctx, login)
			logger.Debug("failed to enrich contributor", "login", login, "error", err)
			continue
		}
		if err := store.UpsertContributor(ctx, contrib); err != nil {
			logger.Debug("failed to upsert enriched contributor", "login", login, "error", err)
			continue
		}
		// Mark enrichment timestamp so users with genuinely empty profiles
		// (no company/location on GitHub) are not re-enriched every pass.
		resolver.MarkContributorEnriched(ctx, login)
		enriched++
	}

	if enriched > 0 {
		logger.Info("contributor enrichment complete", "enriched", enriched, "of", len(logins))
	}
	return enriched
}
