// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.27.133 (C2) — the attribution walk. Enforces SR-6-adjacent
// honesty: no path to a direct root = NO chains (never a fabricated
// attribution).
func TestChainsForWalksToDirectRoots(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		{Ecosystem: "npm", ParentName: "express", ChildName: "body-parser"},
		{Ecosystem: "npm", ParentName: "body-parser", ChildName: "qs"},
		{Ecosystem: "npm", ParentName: "helmet", ChildName: "qs"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{"npm|express": true, "npm|helmet": true}}
	idx := buildChainIndex(edges, sets)

	chains, _ := idx.chainsFor("npm", "qs")
	if len(chains) != 2 {
		t.Fatalf("qs is pulled by two direct roots, got %+v", chains)
	}
	roots := map[string][]string{}
	for _, c := range chains {
		roots[c.Root] = c.Chain
	}
	// helmet → qs is the 2-element chain; express → body-parser → qs
	// the 3-element one; both root-first ending at the package.
	if len(roots["helmet"]) != 2 || roots["helmet"][0] != "helmet" || roots["helmet"][1] != "qs" {
		t.Errorf("helmet chain wrong: %v", roots["helmet"])
	}
	if len(roots["express"]) != 3 || roots["express"][1] != "body-parser" {
		t.Errorf("express chain must pass through body-parser: %v", roots["express"])
	}
}

func TestChainsForCycleSafeAndHonest(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		// a ↔ b cycle with no direct root reachable.
		{Ecosystem: "npm", ParentName: "a", ChildName: "b"},
		{Ecosystem: "npm", ParentName: "b", ChildName: "a"},
	}
	idx := buildChainIndex(edges, db.DirectPackageSets{})
	if got, _ := idx.chainsFor("npm", "a"); got != nil {
		t.Errorf("no direct root reachable — chains must be nil (honest absence), got %+v", got)
	}
}

// Round-19 (Copilot): a monorepo's lockfiles are independent resolved
// graphs. Pre-fix, one shared adjacency let apps/b's express→x edge
// chain through apps/a's x→qs edge — a path no single resolution ever
// produced. The walk must stay inside one lockfile's graph.
func TestChainsForNeverCrossLockfileGraphs(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		// apps/a: x pulls qs, but x has no parent IN THIS lockfile.
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "x", ChildName: "qs"},
		// apps/b: direct express pulls x — but b's graph has no qs.
		{Ecosystem: "npm", LockfilePath: "apps/b/package-lock.json", ParentName: "express", ChildName: "x"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{"npm|express": true}}
	idx := buildChainIndex(edges, sets)
	if got, _ := idx.chainsFor("npm", "qs"); got != nil {
		t.Errorf("cross-lockfile fabricated chain: %+v — apps/a's qs must not attribute through apps/b's express", got)
	}
}

func TestChainsForDedupsRootsAcrossLockfiles(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "express", ChildName: "qs"},
		{Ecosystem: "npm", LockfilePath: "apps/b/package-lock.json", ParentName: "express", ChildName: "mid"},
		{Ecosystem: "npm", LockfilePath: "apps/b/package-lock.json", ParentName: "mid", ChildName: "qs"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{"npm|express": true}}
	idx := buildChainIndex(edges, sets)
	chains, _ := idx.chainsFor("npm", "qs")
	if len(chains) != 1 {
		t.Fatalf("same root via two lockfiles must report once, got %+v", chains)
	}
	// Lockfile-path order: apps/a sorts first, so the 2-hop a-chain wins.
	if chains[0].Root != "express" || len(chains[0].Chain) != 2 {
		t.Errorf("expected apps/a's direct express→qs chain, got %+v", chains[0])
	}
}

func TestChainsForRootCapAndEcosystemIsolation(t *testing.T) {
	var edges []db.RepoLockfileEdge
	direct := map[string]bool{}
	for _, r := range []string{"r1", "r2", "r3", "r4", "r5"} {
		edges = append(edges, db.RepoLockfileEdge{Ecosystem: "npm", ParentName: r, ChildName: "shared"})
		direct["npm|"+r] = true
	}
	// Same package name in another ecosystem must not cross over.
	edges = append(edges, db.RepoLockfileEdge{Ecosystem: "pypi", ParentName: "flask", ChildName: "shared"})
	direct["pypi|flask"] = true
	idx := buildChainIndex(edges, db.DirectPackageSets{Declared: direct})
	chains, _ := idx.chainsFor("npm", "shared")
	if len(chains) != maxChainRoots {
		t.Errorf("root cap: want %d, got %d", maxChainRoots, len(chains))
	}
	for _, c := range chains {
		if c.Root == "flask" {
			t.Error("ecosystem isolation broken — a pypi parent attributed an npm finding")
		}
	}
}

// Round-20 (Copilot): the direct-root set must be per-lockfile too. A
// package DIRECT in apps/b but TRANSITIVE in apps/a is not an
// actionable root in apps/a's graph — the walk must pass THROUGH it to
// apps/a's own direct root instead of stopping early and naming a root
// that apps/a's manifest never declares.
func TestChainsForRootsArePerLockfile(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		// apps/a's graph: X (a's direct) -> P -> C.
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "x", ChildName: "p"},
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "p", ChildName: "c"},
	}
	sets := db.DirectPackageSets{
		ByLockfile: map[string]map[string]bool{
			"apps/a/package-lock.json": {"npm|x": true},
			// P is direct ONLY in apps/b — must not root apps/a's walk.
			"apps/b/package-lock.json": {"npm|p": true},
		},
	}
	idx := buildChainIndex(edges, sets)
	chains, _ := idx.chainsFor("npm", "c")
	if len(chains) != 1 || chains[0].Root != "x" {
		t.Fatalf("walk must continue through apps/b-only direct p to apps/a's root x, got %+v", chains)
	}
	if len(chains[0].Chain) != 3 || chains[0].Chain[1] != "p" {
		t.Errorf("chain must be x -> p -> c, got %v", chains[0].Chain)
	}
}

// The declared (manifest) set stays a repo-wide fallback by design —
// repo_deps_libyear carries no path column, so path-aware handling is
// not derivable for it. A declared dep roots any lockfile's graph.
func TestChainsForDeclaredFallbackRootsAnyGraph(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "d", ChildName: "c"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{"npm|d": true}}
	idx := buildChainIndex(edges, sets)
	chains, _ := idx.chainsFor("npm", "c")
	if len(chains) != 1 || chains[0].Root != "d" {
		t.Errorf("manifest-declared dep must root the walk as the repo-wide fallback, got %+v", chains)
	}
}

// Round-22 (Copilot): graph keys must FOLD — case, PyPI's
// underscore/dot equivalence (PEP 503), and the libyear↔lockfile
// ecosystem vocabulary split — via the ONE shared db.LockfileGraphKey.
// Raw-string keys silently dropped valid chains for exactly the
// packages whose spellings differ between subsystems.
func TestChainsForFoldsPyPINames(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		// The lockfile spells it foo-bar; the finding (from the libyear
		// row's manifest spelling) says Foo_Bar.
		{Ecosystem: "pypi", ParentName: "flask", ChildName: "foo-bar"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{db.LockfileGraphKey("pypi", "Flask"): true}}
	idx := buildChainIndex(edges, sets)
	chains, _ := idx.chainsFor("pypi", "Foo_Bar")
	if len(chains) != 1 || chains[0].Root != "flask" {
		t.Errorf("PyPI name folding failed — Foo_Bar must resolve foo-bar edges, got %+v", chains)
	}
}

func TestChainsForFoldsEcosystemAliases(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		// Gemfile.lock roster emits ecosystem "rubygems"; the declared
		// root comes from libyear's "gem" vocabulary.
		{Ecosystem: "rubygems", ParentName: "rails", ChildName: "actionpack"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{db.LockfileGraphKey("gem", "rails"): true}}
	idx := buildChainIndex(edges, sets)
	chains, _ := idx.chainsFor("rubygems", "actionpack")
	if len(chains) != 1 || chains[0].Root != "rails" {
		t.Errorf("ecosystem alias folding failed — a rubygems edge must root a gem-declared dep, got %+v", chains)
	}
}

// v0.27.148 (round 27, suppressed finding): the emitted-chain cap must
// come with the TRUE root count — a consumer cannot otherwise tell
// "exactly 3 roots" from "dozens, showing 3" and may present a
// truncated remediation set as complete.
func TestChainsForReportsTotalRootsPastTheCap(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		{Ecosystem: "npm", ParentName: "r1", ChildName: "shared-util"},
		{Ecosystem: "npm", ParentName: "r2", ChildName: "shared-util"},
		{Ecosystem: "npm", ParentName: "r3", ChildName: "shared-util"},
		{Ecosystem: "npm", ParentName: "r4", ChildName: "shared-util"},
		{Ecosystem: "npm", ParentName: "r5", ChildName: "shared-util"},
	}
	sets := db.DirectPackageSets{Declared: map[string]bool{
		"npm|r1": true, "npm|r2": true, "npm|r3": true, "npm|r4": true, "npm|r5": true,
	}}
	idx := buildChainIndex(edges, sets)

	chains, total := idx.chainsFor("npm", "shared-util")
	if len(chains) != maxChainRoots {
		t.Fatalf("emitted chains must cap at %d, got %d", maxChainRoots, len(chains))
	}
	if total != 5 {
		t.Fatalf("total roots must count PAST the cap (5 direct parents), got %d", total)
	}

	// Under the cap the two numbers agree — a consumer can rely on
	// total == len(introduced_by) meaning "complete".
	chains2, total2 := idx.chainsFor("npm", "qs")
	if len(chains2) != 0 || total2 != 0 {
		t.Fatalf("unknown package: want 0 chains / 0 total, got %d/%d", len(chains2), total2)
	}
}
