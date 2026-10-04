// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every statement of the minimum Go version an operator or contributor reads
// (README, the Dockerfile, the source docs — "Go 1.NN+", "Go 1.NN or later",
// "go1.NN or later", "requires go 1.NN") names the version go.mod requires.
// Copilot review 5407390534 (PR #226): README said 1.26 while the contributor
// setup, the scancode-host guide and the Dockerfile comment still said 1.25,
// so a reader following those paths could pick a toolchain go.mod refuses.
// Feature-era notes ("Go 1.23 iterators") carry no "+"/"or later" and are
// not minimums.
func TestDocsNameTheGoMinimumFromGoMod(t *testing.T) {
	mod, err := os.ReadFile(filepath.Join(repoRootFromScripts(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^go (1\.\d+)`).FindStringSubmatch(string(mod))
	if m == nil {
		t.Fatal("go.mod has no go directive")
	}
	want := m[1]
	minimum := regexp.MustCompile(`(?i)\b(?:go ?(1\.\d+)(?:\.\d+)? ?(?:\+|or later)|requires go (1\.\d+)|golang:(1\.\d+)-)`)
	var files []string
	for _, f := range []string{"README.md", "Dockerfile"} {
		files = append(files, filepath.Join(repoRootFromScripts(t), f))
	}
	_ = filepath.WalkDir(filepath.Join(repoRootFromScripts(t), "docs"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "_build" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(p, ".md") {
			files = append(files, p)
		}
		return nil
	})
	sites := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			for _, hit := range minimum.FindAllStringSubmatch(line, -1) {
				got := hit[1] + hit[2] + hit[3] // one group matches
				sites++
				if got != want {
					rel, _ := filepath.Rel(repoRootFromScripts(t), f)
					t.Errorf("%s:%d names Go %s as the minimum; go.mod requires %s: %q", rel, i+1, got, want, strings.TrimSpace(line))
				}
			}
		}
	}
	// The denominator: README, the contributor setup, the scancode-host
	// guide, installation, deployment, the Dockerfile (comment and image
	// tag) and the CI guide's image tag each state it.
	if sites < 8 {
		t.Errorf("only %d minimum-version statements found; the pattern no longer matches the docs", sites)
	}
}
