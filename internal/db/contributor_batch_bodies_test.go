// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "testing"

// extractContributorBatchBodies returns the COMBINED bodies of
// UpsertContributorBatch and upsertOneContributor. v0.27.42 (summary/18
// Phase 4) extracted the per-contributor savepoint/rename/identity
// machinery into upsertOneContributor; the source-contract pins written
// against the old monolith target the pair so their contracts keep
// holding wherever the logic lives.
func extractContributorBatchBodies(t *testing.T) string {
	t.Helper()
	return extractFunctionBody(t, "postgres.go", "UpsertContributorBatch") +
		extractFunctionBody(t, "postgres.go", "upsertOneContributor")
}
