// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestUpsertContributorBatchSortsForDeterministicLockOrder pins the 2026-06-09
// deadlock fix: UpsertContributorBatch must acquire contributor locks in a
// DETERMINISTIC (sorted-by-login) order across concurrent workers. Iterating
// the dedup map directly is random per process, so two workers whose batches
// both touch popular shared contributors (regro-cf-autotick-bot, etc.) lock
// them in different orders and deadlock (40P01). A revert to `range merged`
// for the insert loop reintroduces the deadlock storm on aveloxis_large.
func TestUpsertContributorBatchSortsForDeterministicLockOrder(t *testing.T) {
	body := extractFunctionBody(t, "postgres.go", "UpsertContributorBatch")

	if !strings.Contains(body, "sort.Strings(logins)") {
		t.Error("UpsertContributorBatch must sort logins before the insert loop so concurrent workers acquire contributor locks in the same order (deadlock avoidance)")
	}
	if !strings.Contains(body, "for _, login := range logins") {
		t.Error("the contributor insert loop must iterate the SORTED logins slice")
	}
	// Negative pin: the random-order map-range insert loop must not return.
	if strings.Contains(body, "for login, contrib := range merged") {
		t.Error("UpsertContributorBatch must NOT iterate `range merged` for the insert loop — that's the random lock order that caused the 40P01 deadlock storm")
	}
}
