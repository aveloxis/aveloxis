// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — docs claimed "15 ecosystems" in 8 places while the code
// had 14 distinct language labels; README.md:789 stated 15 and then
// enumerated exactly 14 in the same sentence. This tripwire computes
// the count FROM the manifestFiles map and fails the build when any
// doc's "<N> ecosystems" phrase disagrees — the same pattern as
// TestDocsMatviewCountsMatchSchema / TestDocsTableCountsMatchSchema.

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

// ecosystemLabelCount parses the manifestFiles literal in analysis.go
// and returns (distinct labels, filename entries). Self-checks in the
// caller guard against regex rot silently passing.
func ecosystemLabelCount(t *testing.T) (labels int, entries int) {
	t.Helper()
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?s)manifestFiles = map\[string\]string\{.*?\n\}`).Find(src)
	if block == nil {
		t.Fatal("cannot locate the manifestFiles map literal in analysis.go")
	}
	pairs := regexp.MustCompile(`"([^"]+)":\s*"([^"]+)"`).FindAllStringSubmatch(string(block), -1)
	seen := map[string]bool{}
	for _, p := range pairs {
		seen[p[2]] = true
	}
	return len(seen), len(pairs)
}

func TestDocsEcosystemCountsMatchCode(t *testing.T) {
	labels, entries := ecosystemLabelCount(t)
	// Self-checks: the map had 28 filenames / 14 labels at v0.27.23.
	// If extraction finds drastically fewer, the regex rotted — fail
	// loudly instead of "passing" against garbage.
	if entries < 20 {
		t.Fatalf("manifestFiles extraction found only %d entries — regex rot?", entries)
	}
	if labels < 10 {
		t.Fatalf("manifestFiles extraction found only %d distinct labels — regex rot?", labels)
	}

	phrase := regexp.MustCompile(`(\d+) ecosystems`)
	for _, doc := range []string{
		"../../CLAUDE.md",
		"../../README.md",
		"../../docs/index.md",
		"../../docs/architecture/vulnerability-and-sbom.md",
	} {
		data, err := os.ReadFile(doc)
		if err != nil {
			t.Errorf("%s: %v", doc, err)
			continue
		}
		for _, m := range phrase.FindAllStringSubmatch(string(data), -1) {
			n, _ := strconv.Atoi(m[1])
			if n != labels {
				t.Errorf("%s says %q but analysis.go's manifestFiles has %d distinct language labels — update the doc (grep for '%s'; note grep -r silently skipped CLAUDE.md during the original audit, so enumerate files explicitly)", doc, m[0], labels, m[0])
			}
		}
	}
}
