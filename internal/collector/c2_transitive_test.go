// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.133 (C2) wiring pins — the behavioral halves live in
// lockfile_edges_test.go (parsers), internal/api/vuln_chains_test.go
// (the walk), and internal/db/lockfile_store_edges_test.go (the
// snapshot round-trip).
package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(s, "ac.scanGoModGraph(ctx, workDir, declared)") {
		t.Error("the Go toolchain closure must feed the same snapshot (knob-gated)")
	}
	// v0.27.150 (round 29): the expansion's best-effort posture must
	// not shrink the snapshot — incomplete runs preserve the PRIOR
	// Go closure, and a failed prior-rows read propagates (the
	// replace must not run on bad information).
	if !strings.Contains(s, "GetRepoGoClosureRows(ctx, repoID)") {
		t.Error("an incomplete Go expansion must carry the prior snapshot's Go closure forward (GetRepoGoClosureRows)")
	}
	if !strings.Contains(s, "preserving the prior snapshot's Go closure") {
		t.Error("the preserve branch must WARN with the discarded/preserved counts")
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
	// v0.27.148 (round 28): the timeout is REPO-WIDE — created once in
	// scanGoModGraph and shared by every module. A per-module
	// WithTimeout multiplies the budget by the go.mod count
	// (kubernetes-class monorepos carry ~30), unbounding the repo's
	// analysis phase.
	if strings.Contains(body, "context.WithTimeout") {
		t.Error("goModGraphOne must NOT create its own timeout — the budget is repo-wide (round 28)")
	}
	scan := srctest.FuncBody(t, s, "func (ac *AnalysisCollector) scanGoModGraph(")
	if !strings.Contains(scan, "context.WithTimeout(ctx, goModGraphTimeout)") {
		t.Error("scanGoModGraph must own the ONE repo-wide budget")
	}
	if !strings.Contains(scan, "modules_skipped") {
		t.Error("budget exhaustion must WARN with processed/skipped counts (no silent caps)")
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

// v0.27.148 (round 28) behavioral: an exhausted repo-wide budget stops
// the module loop BEFORE any toolchain invocation — a canceled parent
// context stands in for the expired deadline (same rctx.Err() branch).
// Pre-fix, each module got a FRESH 5-minute budget and the loop could
// hold a worker N×5min on an N-module monorepo.
func TestScanGoModGraphStopsOnExhaustedBudget(t *testing.T) {
	dir := t.TempDir()
	for _, sub := range []string{"a", "b"} {
		mod := filepath.Join(dir, sub)
		if err := os.MkdirAll(mod, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(mod, "go.mod"), []byte("module example.com/"+sub+"\n\ngo 1.22\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the repo budget derived from this is already exhausted

	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	start := time.Now()
	pkgs, edges, complete := ac.scanGoModGraph(ctx, dir, map[string]bool{})
	if pkgs != nil || edges != nil {
		t.Errorf("exhausted budget must contribute nothing, got %d pkgs / %d edges", len(pkgs), len(edges))
	}
	// v0.27.150 (round 29): incompleteness must be REPORTED — the
	// caller preserves the prior snapshot's Go closure instead of
	// snapshot-replacing with this empty result.
	if complete {
		t.Error("an exhausted budget is an INCOMPLETE expansion — reporting complete=true lets the snapshot replace wipe the prior Go closure")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the loop must break at the budget check, not grind the modules (took %v)", elapsed)
	}
}

// v0.27.153 (round 32): a discovery-walk failure must report
// INCOMPLETE — the swallow-everything walk let an unreadable subtree
// produce complete=true with empty/partial modDirs, and the caller
// then replaced the valid prior Go closure (SR-16: a walk error is
// not "zero modules").
func TestScanGoModGraphReportsWalkFailureAsIncomplete(t *testing.T) {
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	if err := os.MkdirAll(filepath.Join(locked, "mod"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "mod", "go.mod"), []byte("module example.com/hidden\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) }) // let TempDir cleanup succeed

	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pkgs, edges, complete := ac.scanGoModGraph(context.Background(), dir, map[string]bool{})
	if complete {
		t.Fatal("an unreadable subtree must report INCOMPLETE — complete=true here lets the snapshot replace wipe the prior Go closure on a transient permission/IO failure")
	}
	if pkgs != nil || edges != nil {
		t.Errorf("a failed discovery must contribute nothing (partial output is discarded by the caller anyway), got %d pkgs / %d edges", len(pkgs), len(edges))
	}
}
