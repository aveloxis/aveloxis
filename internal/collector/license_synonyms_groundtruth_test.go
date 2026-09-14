// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.31 (audit Phase 3, D5) — the license-synonym map's OUTPUTS
// validated against the embedded OFFICIAL SPDX list. The old synonym
// tests enumerated the map's own keys back at it (tautological): a
// canonical target that isn't actually an SPDX id would ship and pass
// forever. This test closes the loop with real ground truth — it
// cleared PSF-2.0 (a genuine SPDX id, the audit's specific suspicion)
// on its first run.

import (
	"os"
	"regexp"
	"testing"
)

func TestEverySynonymCanonicalIsARealSPDXID(t *testing.T) {
	src, err := os.ReadFile("../db/license_normalize.go")
	if err != nil {
		t.Fatal(err)
	}
	// The map is built via add("Canonical-ID", synonyms...) calls — the
	// first argument of each add() is the canonical the normalizer will
	// emit. Those are what must be real SPDX ids.
	entry := regexp.MustCompile(`\badd\(\s*"([^"]+)"`)
	seen := map[string]bool{}
	for _, m := range entry.FindAllStringSubmatch(string(src), -1) {
		seen[m[1]] = true
	}
	if len(seen) < 10 {
		t.Fatalf("extracted only %d canonical values — regex rot?", len(seen))
	}
	// Reviewed FAMILY-BUCKET exemptions (v0.28.6): version-unspecified
	// declarations normalize to a bare family name — the EPL/Artistic
	// precedent from the classifier — because every specific SPDX id
	// (incl. -or-later, which asserts a recipient may choose later
	// versions) would claim more than the source metadata says. Add a
	// bucket here ONLY with that rationale at the license_normalize.go
	// site; anything else is a minted invalid id.
	familyBuckets := map[string]bool{"LGPL": true}
	for id := range seen {
		if familyBuckets[id] {
			continue
		}
		if !isSPDXLicense(id) {
			t.Errorf("license_normalize.go maps synonyms to %q, which is NOT in the official SPDX list (spdx_license_ids.txt) — the normalizer is minting invalid ids", id)
		}
	}
}
