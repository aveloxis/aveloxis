// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A4) — the completed-vuln-scan stamp. Unlike scancode and
// distribution, the vuln scanner recorded NO completion time, so the
// GUI could not distinguish "scanned, clean" from "never scanned"
// (per-finding last_seen_at cannot: a clean scan touches zero rows).
// These pins hold the stamp to its contract: exactly the three
// completed-scan exits, never the error paths.

func vulnScanSrc(t *testing.T) string {
	t.Helper()
	return srctest.Read(t, "internal/collector/vulnerability.go")
}

// The stamp fires at ALL THREE completed-scan exits: the dep-less
// early return, the all-malformed early return, and the final
// success return. Fewer sites means one completion class silently
// reads "never scanned" forever; more sites means an error path is
// claiming freshness it doesn't have.
func TestVulnScanStampsAllThreeCompletionExits(t *testing.T) {
	src := vulnScanSrc(t)
	// Count CALL sites only (exclude the definition + comments by
	// anchoring on the exact call shape).
	calls := strings.Count(src, "stampVulnScanComplete(ctx, store, repoID, logger)")
	if calls != 3 {
		t.Errorf("stampVulnScanComplete must be called at exactly the 3 completed-scan exits, found %d call sites", calls)
	}
}

// The stamp is non-fatal (a failed stamp must never fail a
// successful scan) and routes through the store's single writer.
func TestVulnScanStampIsNonFatal(t *testing.T) {
	src := vulnScanSrc(t)
	i := strings.Index(src, "func stampVulnScanComplete(")
	if i < 0 {
		t.Fatal("stampVulnScanComplete helper missing from vulnerability.go")
	}
	body := src[i:]
	if end := strings.Index(body, "\n}"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "SetVulnScanLastRun(") {
		t.Error("the stamp must route through store.SetVulnScanLastRun (the single writer)")
	}
	if !strings.Contains(body, "logger.Warn") || strings.Contains(body, "return err") {
		t.Error("the stamp must WARN on failure and never propagate — a failed stamp must not fail a successful scan")
	}
}
