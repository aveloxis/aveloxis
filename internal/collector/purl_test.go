// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.29 — buildPurl tested against the purl SPEC's canonical
// cases (testdata/purl_spec_cases.json, derived from
// github.com/package-url/purl-spec), NOT against our own output. The
// pre-v0.27.29 tests asserted the implementation's non-canonical
// strings back at it (pkg:pypi/Flask_SQLAlchemy pinned as correct) —
// the wrong-answer-tests audit's headline instance.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBuildPurlAgainstSpecCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/purl_spec_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Version   string `json:"version"`
			Canonical string `json:"canonical"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) < 10 {
		t.Fatalf("fixture has %d cases — truncated?", len(fixture.Cases))
	}
	for _, c := range fixture.Cases {
		if got := buildPurl(c.Type, c.Name, c.Version); got != c.Canonical {
			t.Errorf("buildPurl(%q, %q, %q) = %q, want spec-canonical %q",
				c.Type, c.Name, c.Version, got, c.Canonical)
		}
	}
}

// TestSelfAdvisoryPurlIsCanonical — the versionless self purls ride
// the same builder.
func TestSelfAdvisoryPurlIsCanonical(t *testing.T) {
	if got := selfAdvisoryPurl("pypi", "NumPy"); got != "pkg:pypi/numpy" {
		t.Errorf("selfAdvisoryPurl pypi NumPy = %q, want pkg:pypi/numpy", got)
	}
	if got := selfAdvisoryPurl("conda", "whatever"); got != "" {
		t.Errorf("unmapped ecosystem must yield empty, got %q", got)
	}
}

// TestNoInlinePurlConcatenationReturns — the drift tripwire: every
// purl must flow through buildPurl. A new fmt.Sprintf("pkg:… site
// reintroduces the non-canonical class.
func TestNoInlinePurlConcatenationReturns(t *testing.T) {
	for _, f := range []string{"analysis.go", "vuln_targets.go", "sbom.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		// Strip // comments (the false-match lesson).
		var code []string
		for line := range strings.SplitSeq(string(src), "\n") {
			if i := strings.Index(line, "//"); i >= 0 {
				line = line[:i]
			}
			code = append(code, line)
		}
		s := strings.Join(code, "\n")
		if strings.Contains(s, `Sprintf("pkg:`) {
			t.Errorf("%s builds a purl by Sprintf — route it through buildPurl (spec canonicalization)", f)
		}
	}
}
