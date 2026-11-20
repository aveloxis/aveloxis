// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// The purl builder and its tests against the purl specification's cases
// live in internal/pkgid. What stays here is the tripwire over this
// package's sources.

import (
	"os"
	"strings"
	"testing"
)

// TestNoInlinePurlConcatenationReturns — the drift tripwire: every
// purl must flow through pkgid.Purl. A new fmt.Sprintf("pkg:… site
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
			t.Errorf("%s builds a purl by Sprintf — route it through pkgid.Purl (spec canonicalization)", f)
		}
	}
}
