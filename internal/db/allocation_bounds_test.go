// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"math"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
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
	// The clamp lives in the one normaliser both listings call (DBS-2,
	// stage 1f of summary/59); the readers must reach it.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func NormalizeListingPage("))
	if !strings.Contains(body, "pageSize > 100") {
		t.Error("NormalizeListingPage must clamp pageSize with a bare `pageSize > 100` guard (scanner-recognizable upper bound)")
	}
	if strings.Contains(body, "pageSize < 1 || pageSize > 100") {
		t.Error("NormalizeListingPage must NOT combine the clamps into one compound condition — CodeQL's upper-bound barrier misses that shape (2026-07-31 finding)")
	}
	for _, sig := range []string{"func (s *PostgresStore) GetCollectionRepos(", "func (s *PostgresStore) GetPortalGroupRepos("} {
		if !strings.Contains(srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", sig)), "NormalizeListingPage(") {
			t.Errorf("%s must take its page through NormalizeListingPage", sig)
		}
	}
	body = srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) listRepos("))
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

// ASVS 5.0 review of 0.29.125 (V1.4.2/V2.2.1, A-N2): a page number near
// MaxInt made (page−1)×page_size overflow to a negative OFFSET, which
// PostgreSQL refuses — a 500 and an ERROR log line per request for any
// signed-in caller. The one clamp (NormalizeListingPage) bounds the page so
// the offset always fits.
func TestNormalizeListingPageKeepsTheOffsetNonNegative(t *testing.T) {
	// Every legal page size (1..100), every far page: the offset fits.
	for size := 1; size <= 100; size++ {
		for _, page := range []int{math.MaxInt, math.MaxInt - 1, math.MaxInt / 2, 1 << 62} {
			p := NormalizeListingPage(page, size, "", "")
			if off := (p.Page - 1) * p.PageSize; off < 0 || p.Page < 1 {
				t.Errorf("page %d, size %d: page %d offset %d", page, size, p.Page, off)
			}
		}
	}
	// L10 round 2 of 0.29.125: the first spelling of the clamp wrapped for
	// page_size=1 (math.MaxInt/1 + 1 = MinInt) and turned EVERY page of that
	// size into offset MaxInt — an empty answer. Ordinary pages stay as asked.
	for _, tc := range []struct{ page, size, wantPage int }{{1, 1, 1}, {2, 1, 2}, {5, 1, 5}, {3, 100, 3}, {7, 50, 7}} {
		if p := NormalizeListingPage(tc.page, tc.size, "", ""); p.Page != tc.wantPage || (p.Page-1)*p.PageSize != (tc.wantPage-1)*tc.size {
			t.Errorf("page %d size %d: normalised to page %d (offset %d); want page %d", tc.page, tc.size, p.Page, (p.Page-1)*p.PageSize, tc.wantPage)
		}
	}
}
