// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/db"
)

// RescoreStoredVulnerabilities recomputes cvss_score for every stored
// finding from its stored cvss_vector — zero OSV traffic. This is the
// in-place healer for pre-v0.27.23 rows whose scores came from the old
// six-bucket approximation. Scheduled scans heal each repo at its
// recollect cadence anyway; this closes the fleet in one pass.
//
// One UPDATE per DISTINCT vector, not per row (the keyset-window
// lesson generalized: a fleet's findings converge onto a few thousand
// vectors). Vectors that cannot be scored (CVSS v4-only, malformed)
// are set to 0 — "no score" beats a fabricated one. Idempotent: the
// store-side IS DISTINCT FROM makes re-runs no-ops.
func RescoreStoredVulnerabilities(ctx context.Context, store *db.PostgresStore, logger *slog.Logger) (rowsUpdated int64, err error) {
	vectors, err := store.ListDistinctVulnVectors(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing distinct vectors: %w", err)
	}
	var unscoreable int
	for _, vec := range vectors {
		score, ok := cvssBaseScore(vec)
		if !ok {
			unscoreable++
			score = 0 // honest: no basis for a number
		}
		n, err := store.UpdateCVSSScoreForVector(ctx, vec, score)
		if err != nil {
			return rowsUpdated, fmt.Errorf("rescoring vector %q: %w", vec, err)
		}
		rowsUpdated += n
	}
	logger.Info("CVSS rescore complete",
		"distinct_vectors", len(vectors),
		"unscoreable_vectors", unscoreable,
		"rows_updated", rowsUpdated)
	return rowsUpdated, nil
}
