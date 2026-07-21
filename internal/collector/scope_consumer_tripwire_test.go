// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/19 P0 tripwire (v0.27.44): CONSUMERS must never branch on a
// literal scope value — the fine vocabulary (dev/test/build/optional/
// peer) means a `== "dev"` comparison silently misses four of the five
// non-runtime scopes. Parsers ASSIGN literals (that's their job);
// consumers ask model.IsRuntimeScope.

package collector

import (
	"os"
	"strings"
	"testing"
)

func TestNoLiteralScopeBranchingInConsumers(t *testing.T) {
	// Consumer files (parser files excluded by design — they assign).
	for _, f := range []string{"sbom.go", "vulnerability.go", "vuln_targets.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if strings.Contains(line, "//") {
				line = line[:strings.Index(line, "//")]
			}
			if strings.Contains(line, `== "dev"`) || strings.Contains(line, `!= "dev"`) ||
				strings.Contains(line, `== "test"`) || strings.Contains(line, `== "build"`) {
				t.Errorf("%s:%d branches on a literal scope value — use model.IsRuntimeScope (the P0 contract)", f, i+1)
			}
		}
	}
}
