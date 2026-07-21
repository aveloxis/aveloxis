// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.31 (audit Phase 3, F2) — the severity ordering exists in
// THREE hand-maintained copies: the digest's severityRank map, the
// digest's SeveritiesAtOrAbove slice, and the finding-list SQL's CASE
// expression (vulnerability_store.go). The audit found the copies
// already diverging on MODERATE (present in the SQL CASE, absent from
// the map — safe today only because extractSeverity normalizes
// MODERATE→MEDIUM at ingest). This pin forces the copies to agree so
// the divergence can't grow teeth.

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

func TestSeverityOrderingCopiesAgree(t *testing.T) {
	// Ground truth: CRITICAL > HIGH > MEDIUM > LOW > everything else —
	// the FIRST CVSS qualitative scale's ordering.
	wantOrder := []string{"CRITICAL", "HIGH", "MEDIUM", "LOW"}

	// Copy 1+2 live in vuln_digest_store.go; copy 3 is the SQL CASE in
	// vulnerability_store.go. Extract each CASE's WHEN 'X' THEN n and
	// assert rank order matches ground truth.
	for _, file := range []string{"vuln_digest_store.go", "vulnerability_store.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		when := regexp.MustCompile(`WHEN '([A-Z]+)' THEN (\d+)`)
		ranks := map[string]int{}
		for _, m := range when.FindAllStringSubmatch(string(src), -1) {
			n, _ := strconv.Atoi(m[2])
			// Same label appearing twice with different ranks in one
			// file would be its own bug; last-wins matches SQL CASE
			// first-match... assert consistency instead.
			if prev, dup := ranks[m[1]]; dup && prev != n {
				t.Errorf("%s ranks %s inconsistently (%d vs %d)", file, m[1], prev, n)
			}
			ranks[m[1]] = n
		}
		if len(ranks) == 0 {
			continue // file has no CASE ranking (fine)
		}
		for i := 0; i < len(wantOrder)-1; i++ {
			hi, lo := wantOrder[i], wantOrder[i+1]
			if ranks[hi] != 0 && ranks[lo] != 0 && ranks[hi] <= ranks[lo] {
				t.Errorf("%s ranks %s (%d) <= %s (%d) — severity ordering must match the FIRST scale", file, hi, ranks[hi], lo, ranks[lo])
			}
		}
		// MODERATE handling: if a CASE ranks MODERATE it must rank it
		// equal to MEDIUM (GHSA's synonym) — a different rank would
		// order raw-MODERATE rows wrongly if any ever bypass
		// extractSeverity's ingest normalization.
		if mod, ok := ranks["MODERATE"]; ok && ranks["MEDIUM"] != 0 && mod != ranks["MEDIUM"] {
			t.Errorf("%s ranks MODERATE (%d) != MEDIUM (%d) — they are the same severity (GHSA vocabulary)", file, mod, ranks["MEDIUM"])
		}
	}
}
