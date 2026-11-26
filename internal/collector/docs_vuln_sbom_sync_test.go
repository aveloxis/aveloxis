// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — sync tripwires between the subsystem reference doc
// (docs/architecture/vulnerability-and-sbom.md) and the code it
// describes, following the metrics-catalog precedent: a spec bump or
// roster change that doesn't update the doc fails the build.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const vulnSBOMDocPath = "../../docs/architecture/vulnerability-and-sbom.md"

func TestVulnSBOMDocExists(t *testing.T) {
	if _, err := os.Stat(vulnSBOMDocPath); err != nil {
		t.Fatalf("%s missing — it is the operator-facing reference for this subsystem (created v0.27.22-era, wired into the RTD toctree); restore it before shipping doc-affecting changes", vulnSBOMDocPath)
	}
}

// TestVulnSBOMDocSpecVersionsMatchCode pins the doc's stated CycloneDX
// and SPDX versions to the literals sbom.go actually emits.
func TestVulnSBOMDocSpecVersionsMatchCode(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	// v0.29.67: the CycloneDX version is the cdxSpecVersion constant
	// (read directly); the SPDX version is still a literal.
	spdx := regexp.MustCompile(`SPDXVersion:\s*"SPDX-([\d.]+)"`).FindSubmatch(src)
	if spdx == nil {
		t.Fatal("cannot extract the SPDX version literal from sbom.go")
	}
	doc, err := os.ReadFile(vulnSBOMDocPath)
	if err != nil {
		t.Fatal(err)
	}
	wantCDX := fmt.Sprintf("| CycloneDX | **%s** |", cdxSpecVersion)
	wantSPDX := fmt.Sprintf("| SPDX | **%s** |", spdx[1])
	for _, want := range []string{wantCDX, wantSPDX} {
		if !strings.Contains(string(doc), want) {
			t.Errorf("doc's format table missing %q — sbom.go's spec version changed without a doc update", want)
		}
	}
}

// TestVulnSBOMDocLockfileCountMatchesRoster pins the doc's
// "Lockfile parsing — N formats" heading to len(lockfileKinds).
func TestVulnSBOMDocLockfileCountMatchesRoster(t *testing.T) {
	doc, err := os.ReadFile(vulnSBOMDocPath)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`Lockfile parsing — (\d+) formats`).FindSubmatch(doc)
	if m == nil {
		t.Fatal("doc missing the 'Lockfile parsing — N formats' heading")
	}
	want := fmt.Sprintf("Lockfile parsing — %d formats", len(lockfileKinds))
	if string(m[0]) != want {
		t.Errorf("doc says %q but lockfileKinds has %d entries — update the doc heading and its table", m[0], len(lockfileKinds))
	}
}

// The README and the docs index state the same count, of the same roster
// (operator, 2026-11-19: "The README should match the roster exactly"; they
// said 19 while the roster held 18, and nothing held them to it).
// The same for the registries libyear asks (review round 1: the heading
// said 12 and the README "all 12 registries" after CRAN made thirteen), and
// for the roster count the architecture page states in passing.
func TestEveryStatedRegistryCountIsTheShippedOnes(t *testing.T) {
	// The registries with a base URL here, and SwiftPM, which is resolved
	// through GitHub releases and has none.
	want := len(shippedRegistryBases) + 1
	found := 0
	// Every "<n> registries" in the public pages that state it, and the
	// architecture page's heading. Round 2 found five statements the first
	// form of this check did not read (it knew two phrasings).
	number := regexp.MustCompile(`(\d+) registries`)
	heading := regexp.MustCompile(`Libyear registries — (\d+)`)
	for _, doc := range []string{"../../README.md", "../../docs/index.md", "../../docs/guide/collection-pipeline.md", "../../docs/architecture/vulnerability-and-sbom.md"} {
		data, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for _, re := range []*regexp.Regexp{number, heading} {
			for _, m := range re.FindAllSubmatch(data, -1) {
				found++
				if string(m[1]) != fmt.Sprint(want) {
					t.Errorf("%s says %q; libyear asks %d registries", doc, m[0], want)
				}
			}
		}
	}
	// The denominator: the statements that exist today. Fewer means a
	// phrase was reworded and is no longer read.
	if found < 5 {
		t.Errorf("%d statements of the registry count were examined, want at least 5", found)
	}
	data, err := os.ReadFile("../../docs/architecture/vulnerability-and-sbom.md")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`Roster count = (\d+) registered filenames`).FindSubmatch(data)
	if m == nil {
		t.Fatal("the architecture page no longer states the roster count in that phrase")
	}
	if string(m[1]) != fmt.Sprint(len(lockfileKinds)) {
		t.Errorf("the architecture page says the roster has %s filenames; it has %d", m[1], len(lockfileKinds))
	}
}

func TestEveryStatedLockfileCountIsTheRosters(t *testing.T) {
	phrase := regexp.MustCompile(`(\d+) lockfile formats`)
	found := 0
	for _, doc := range []string{"../../README.md", "../../docs/index.md"} {
		data, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range phrase.FindAllSubmatch(data, -1) {
			found++
			if want := fmt.Sprint(len(lockfileKinds)); string(m[1]) != want {
				t.Errorf("%s says %q; the lockfile roster has %s kinds", doc, m[0], want)
			}
		}
	}
	// The denominator: the statements that exist today (four in the
	// README, one in the index). Fewer means the phrase was reworded and
	// this check is reading nothing.
	if found < 5 {
		t.Errorf("found %d statements of the lockfile count, want at least 5", found)
	}
}

// TestVulnSBOMDocInToctree — the doc must stay reachable from the
// Sphinx toctree; an orphaned page is not "easily findable".
func TestVulnSBOMDocInToctree(t *testing.T) {
	idx, err := os.ReadFile("../../docs/index.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(idx), "architecture/vulnerability-and-sbom") {
		t.Error("docs/index.md toctree missing architecture/vulnerability-and-sbom — the page would build orphaned")
	}
}

// TestNoStaleCycloneDXVersion — v0.29.67 review round 1: after the move to
// CycloneDX 1.7, "1.5" survived in seven places (comments, a showcase
// download label, docs). The version is cdxSpecVersion; prose that names
// another version must be history ("1.5 until v0.29.67") or date a feature
// ("CycloneDX 1.6+").
func TestNoStaleCycloneDXVersion(t *testing.T) {
	examined := 0
	// Any whitespace (a Markdown line wrap) and an optional "v" (review
	// round 2: "CycloneDX\n1.5" and "CycloneDX v1.5" escaped the first
	// pattern).
	stale := regexp.MustCompile(`CycloneDX\s+v?1\.[0-6]\b[^\n]{0,40}`)
	roots := []string{"../../internal", "../../cmd", "../../docs"}
	// Every TRACKED Markdown file at the repository root (review round 2:
	// SECURITY.md still said 1.5 and was never examined). Tracked only
	// (review round 3): private untracked notes there would turn the local
	// suite red while CI, which never sees them, stayed green.
	// Outside a git work tree (a `git archive` pristine copy, a source
	// tarball) there is no tracked-file list: skip, visibly (review round 4).
	if err := exec.Command("git", "-C", "../..", "rev-parse", "--is-inside-work-tree").Run(); err != nil {
		t.Skip("not a git work tree; the tracked root Markdown files cannot be listed")
	}
	out, err := exec.Command("git", "-C", "../..", "ls-files", "--", "*.md").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var rootMD []string
	for _, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if f != "" && !strings.Contains(f, "/") {
			rootMD = append(rootMD, filepath.Join("../..", f))
		}
	}
	if len(rootMD) < 3 {
		t.Fatalf("found %d root Markdown files; the glob is wrong", len(rootMD))
	}
	roots = append(roots, rootMD...)
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "_build" || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, "_test.go") || !(strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".md")) {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			for _, m := range stale.FindAllString(string(data), -1) {
				// "CycloneDX 1.6+" dates a feature ("since 1.6"), not the export.
				ver := regexp.MustCompile(`1\.[0-6]`).FindStringIndex(m)
				if !strings.Contains(m, "until v0.29.67") && !strings.HasPrefix(m[ver[1]:], "+") {
					t.Errorf("%s: %q names an old CycloneDX version as current (the export is %s)", path, m, cdxSpecVersion)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if examined < 100 {
		t.Fatalf("examined only %d files; the walk roots are wrong", examined)
	}
}
