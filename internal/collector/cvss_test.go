// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestCVSSV3BaseScoreAgainstSpecVectors pins the v3.x implementation
// against scores published by FIRST (the CVSS specification owner) and
// NVD for well-known vectors. These are the ground truth that makes
// replacing the old six-bucket approximation safe: if the arithmetic
// drifts, this table fails before merge.
func TestCVSSV3BaseScoreAgainstSpecVectors(t *testing.T) {
	cases := []struct {
		vector string
		want   float64
	}{
		// The canonical worst-case network vector (log4shell class).
		{"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H", 9.8},
		// Scope-changed variants push past 9.8.
		{"CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:C/C:H/I:H/A:H", 9.9},
		{"CVSS:3.0/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0},
		// Classic reflected XSS (CWE-79 reference vector).
		{"CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1},
		// High attack complexity knocks 9.8 down to 8.1.
		{"CVSS:3.1/AV:N/AC:H/PR:N/UI:N/S:U/C:H/I:H/A:H", 8.1},
		// Local privilege escalation reference vector.
		{"CVSS:3.1/AV:L/AC:L/PR:L/UI:N/S:U/C:H/I:H/A:H", 7.8},
		// Availability-only DoS.
		{"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5},
		// Near-floor vector: physical access, high complexity.
		{"CVSS:3.1/AV:P/AC:H/PR:H/UI:R/S:U/C:L/I:N/A:N", 1.6},
		// Zero-impact vector scores 0.0 by spec.
		{"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:N", 0.0},
	}
	for _, tc := range cases {
		got, ok := cvssBaseScore(tc.vector)
		if !ok {
			t.Errorf("%s: not parseable, want score %.1f", tc.vector, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %.1f, want %.1f", tc.vector, got, tc.want)
		}
	}
}

// TestCVSSV3FixesTheNonMonotonicBucket pins the specific defect that
// motivated the rewrite: under the old bucket table, AV:N WITHOUT AC:L
// scored 6.5 — higher than AV:N + AC:L at 5.3 — even though lower
// attack complexity is strictly worse. The real formula is monotonic.
func TestCVSSV3FixesTheNonMonotonicBucket(t *testing.T) {
	lowComplexity, ok1 := cvssBaseScore("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:L/I:N/A:N")
	highComplexity, ok2 := cvssBaseScore("CVSS:3.1/AV:N/AC:H/PR:N/UI:N/S:U/C:L/I:N/A:N")
	if !ok1 || !ok2 {
		t.Fatal("reference vectors must parse")
	}
	if highComplexity >= lowComplexity {
		t.Errorf("AC:H (%.1f) must score below AC:L (%.1f) — the old buckets inverted this", highComplexity, lowComplexity)
	}
}

// TestCVSSV2BaseScoreAgainstGuideVectors pins v2 against scores from the
// CVSS v2 guide and NVD records.
func TestCVSSV2BaseScoreAgainstGuideVectors(t *testing.T) {
	cases := []struct {
		vector string
		want   float64
	}{
		{"AV:N/AC:L/Au:N/C:P/I:P/A:P", 7.5}, // the classic pre-2016 CVE shape
		{"AV:N/AC:L/Au:N/C:C/I:C/A:C", 10.0},
		{"(AV:N/AC:M/Au:N/C:P/I:N/A:N)", 4.3}, // parenthesized form, info leak
		{"AV:L/AC:L/Au:N/C:N/I:N/A:C", 4.9},
		{"AV:N/AC:L/Au:N/C:N/I:N/A:N", 0.0}, // zero impact
	}
	for _, tc := range cases {
		got, ok := cvssBaseScore(tc.vector)
		if !ok {
			t.Errorf("%s: not parseable, want score %.1f", tc.vector, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %.1f, want %.1f", tc.vector, got, tc.want)
		}
	}
}

// TestCVSSUnparseableYieldsNoScoreNeverAGuess pins the honesty contract:
// anything we cannot actually compute returns ok=false, and the caller
// stores 0 ("no score") rather than a fabricated number.
func TestCVSSUnparseableYieldsNoScoreNeverAGuess(t *testing.T) {
	for _, vector := range []string{
		"",
		"CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N", // v4: MacroVector, not implemented
		"CVSS:9.9/AV:N",      // unknown future version
		"CVSS:3.1/AV:N/AC:L", // missing required metrics
		"CVSS:3.1/AV:X/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H", // invalid metric value
		"total garbage",
	} {
		if score, ok := cvssBaseScore(vector); ok {
			t.Errorf("%q: parsed to %.1f, want ok=false (no score, never a guess)", vector, score)
		}
	}
}

// TestOldBucketTableIsGone is the negative tripwire: the six-value
// substring lookup must not return. The distinctive shape was the pair
// of magic returns 5.3 and 6.5 adjacent to strings.Contains checks in
// a function named parseCVSSScore.
func TestOldBucketTableIsGone(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "func parseCVSSScore") {
		t.Error("parseCVSSScore is back in vulnerability.go — v0.27.23 replaced the bucket table with cvssBaseScore (cvss.go); scores must be computed, not guessed")
	}
	for _, f := range []string{"vulnerability.go", "cvss.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		// The old code's signature move: returning 5.3 straight from a
		// substring match. cvss.go legitimately never contains a bare
		// "return 5.3" / "return 6.5" — its scores come out of arithmetic.
		for _, needle := range []string{"return 5.3", "return 6.5", "return 9.8"} {
			if strings.Contains(string(src), needle) {
				t.Errorf("%s contains %q — hardcoded score buckets must not return", f, needle)
			}
		}
	}
}
