// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.65 — store-layer allocation backstops (CodeQL
// go/uncontrolled-allocation-size on the v0.27.61/63 PR). Every store
// method whose caller-supplied limit/pageSize feeds a `make` capacity
// (or a SQL LIMIT) must clamp its OWN upper bound: API handlers cap
// too, but the store is the shared surface and must not rely on every
// future caller remembering. The clamps use bare single-comparison
// guards (`x > 100`) — the barrier shape the scanner's upper-bound
// analysis recognizes; a compound `a || b` reassignment was flagged
// even though it bounded the value.

func TestTopContributorsClampsUpperLimitInStore(t *testing.T) {
	body := extractStoreFunc(t, "contributions.go", "func (s *PostgresStore) TopContributors(")
	if !strings.Contains(body, "limit > 100") {
		t.Error("TopContributors must clamp limit > 100 at the STORE layer — the API cap alone leaves the shared surface unbounded (CodeQL go/uncontrolled-allocation-size)")
	}
	// Round 2: the make() capacity must be a CONSTANT, never the
	// user-derived limit — the scanner doesn't credit clamp-and-
	// continue reassignments as barriers, and a constant hint costs
	// nothing (append grows past it).
	if strings.Contains(body, "make([]TopContributor, 0, limit)") {
		t.Error("TopContributors allocation capacity must be a constant, not `limit` — the taint path to make() is what CodeQL flags (2026-07-31 round 2)")
	}
}

func TestGetCollectionReposClampsPageSizeWithBareGuards(t *testing.T) {
	body := extractStoreFunc(t, "collections_store.go", "func (s *PostgresStore) GetCollectionRepos(")
	if !strings.Contains(body, "pageSize > 100") {
		t.Error("GetCollectionRepos must clamp pageSize with a bare `pageSize > 100` guard (scanner-recognizable upper bound)")
	}
	if strings.Contains(body, "pageSize < 1 || pageSize > 100") {
		t.Error("GetCollectionRepos must NOT combine the clamps into one compound condition — CodeQL's upper-bound barrier misses that shape (2026-07-31 finding)")
	}
	// Round 2: constant capacity, never the user-derived pageSize.
	if strings.Contains(body, "make([]CollectionRepo, 0, pageSize)") {
		t.Error("GetCollectionRepos allocation capacity must be a constant, not `pageSize` — the taint path to make() is what CodeQL flags (2026-07-31 round 2)")
	}
}

// extractStoreFunc returns the source of one function body: from its
// signature to the next top-level `func ` (or EOF).
func extractStoreFunc(t *testing.T, file, sig string) string {
	t.Helper()
	src := readSourceFile(t, file)
	start := strings.Index(src, sig)
	if start < 0 {
		t.Fatalf("%s: signature %q not found", file, sig)
	}
	rest := src[start+len(sig):]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		return rest
	}
	return rest[:end]
}
