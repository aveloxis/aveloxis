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
	direct := map[string]bool{"npm|express": true, "npm|helmet": true}
	idx := buildChainIndex(edges, direct)

	chains := idx.chainsFor("npm", "qs")
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
	idx := buildChainIndex(edges, map[string]bool{})
	if got := idx.chainsFor("npm", "a"); got != nil {
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
	direct := map[string]bool{"npm|express": true}
	idx := buildChainIndex(edges, direct)
	if got := idx.chainsFor("npm", "qs"); got != nil {
		t.Errorf("cross-lockfile fabricated chain: %+v — apps/a's qs must not attribute through apps/b's express", got)
	}
}

func TestChainsForDedupsRootsAcrossLockfiles(t *testing.T) {
	edges := []db.RepoLockfileEdge{
		{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json", ParentName: "express", ChildName: "qs"},
		{Ecosystem: "npm", LockfilePath: "apps/b/package-lock.json", ParentName: "express", ChildName: "mid"},
		{Ecosystem: "npm", LockfilePath: "apps/b/package-lock.json", ParentName: "mid", ChildName: "qs"},
	}
	direct := map[string]bool{"npm|express": true}
	idx := buildChainIndex(edges, direct)
	chains := idx.chainsFor("npm", "qs")
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
	idx := buildChainIndex(edges, direct)
	chains := idx.chainsFor("npm", "shared")
	if len(chains) != maxChainRoots {
		t.Errorf("root cap: want %d, got %d", maxChainRoots, len(chains))
	}
	for _, c := range chains {
		if c.Root == "flask" {
			t.Error("ecosystem isolation broken — a pypi parent attributed an npm finding")
		}
	}
}
