// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.133 (C2) wiring pins — the behavioral halves live in
// lockfile_edges_test.go (parsers), internal/api/vuln_chains_test.go
// (the walk), and internal/db/lockfile_store_edges_test.go (the
// snapshot round-trip).
package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Edges ride the SAME knob as transitive package rows and the SAME
// snapshot transaction — knob off keeps the exact pre-C2 write set.
func TestEdgesGatedOnTransitiveKnobAndSameSnapshot(t *testing.T) {
	src, err := os.ReadFile("lockfile_scan.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "if ac.TransitiveLockfiles {\n\t\t\tedgeSeen := map[string]bool{}") {
		t.Error("edge collection must be gated on ac.TransitiveLockfiles")
	}
	if !strings.Contains(s, "ReplaceRepoLockfileSnapshot(ctx, repoID, inventory, packages, edges)") {
		t.Error("edges must ride the SAME snapshot transaction as the package rows")
	}
	if !strings.Contains(s, "ac.scanGoModGraph(ctx, workDir, declared, packages, edges)") {
		t.Error("the Go toolchain closure must feed the same snapshot (knob-gated)")
	}
}

// The go-toolchain expansion is best-effort in the scc shape: LookPath
// gate, bounded timeout, WARN-and-skip — a broken module or missing
// toolchain must never fail the analysis phase.
func TestGoModGraphIsBestEffort(t *testing.T) {
	s := srctest.Read(t, "internal/collector/gomod_graph.go")
	for _, needle := range []string{
		`exec.LookPath("go")`,
		"goModGraphTimeout",
		"context.WithTimeout(ctx, goModGraphTimeout)",
		`run("list", "-m", "all")`,
		`run("mod", "graph")`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("gomod_graph.go missing %q", needle)
		}
	}
	body := srctest.FuncBody(t, s, "func (ac *AnalysisCollector) goModGraphOne(")
	if !strings.Contains(body, "ac.logger.Warn") {
		t.Error("toolchain failures must WARN and skip, never propagate")
	}
}

// The API attributes transitive findings via the chain walk, loaded
// lazily and degrading to no attribution on lookup failure (never a
// 500 — findings without chains beat no findings).
func TestVulnAPIAttachesIntroducedBy(t *testing.T) {
	s := srctest.Read(t, "internal/api/vulnerabilities.go")
	for _, needle := range []string{
		"GetRepoLockfileEdges(",
		"GetRepoDirectPackageSets(",
		"buildChainIndex(",
		"chains.chainsFor(v.Ecosystem, v.PackageName)",
		"findings served without attribution",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("vulnerabilities handler missing %q", needle)
		}
	}
}

// The purl-alias fix: the lockfile roster's ecosystem strings that had
// NO purl mapping (their transitives were silently dropped from the
// scan — the C2 exploration find).
func TestPurlEcosystemAliasesForRosterStrings(t *testing.T) {
	src, err := os.ReadFile("vuln_targets.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{`"rubygems":`, `"packagist":`, `"swiftpm":`, `"hackage":`} {
		if !strings.Contains(s, needle) {
			t.Errorf("purlEcosystemTypes missing the roster alias %s — its transitives are silently dropped", needle)
		}
	}
	if purlForPackage("rubygems", "rails", "7.0.8") == "" {
		t.Error("a rubygems transitive must produce a purl")
	}
}
