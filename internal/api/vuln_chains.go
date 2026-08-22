// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// vuln_chains.go — v0.27.133 (C2): parent-chain attribution for
// transitive vulnerability findings. A transitive finding without its
// chain says "log4j-core is in your tree" but not WHAT TO BUMP; the
// chain walks the stored lockfile edges from the vulnerable package up
// to the DIRECT dependencies that pull it in.

package api

import (
	"sort"

	"github.com/aveloxis/aveloxis/internal/db"
)

// vulnChainJSON is one attribution: the direct root plus one shortest
// path root → … → vulnerable package.
type vulnChainJSON struct {
	Root  string   `json:"root"`
	Chain []string `json:"chain"`
}

// maxChainRoots bounds how many distinct direct roots are reported per
// finding (widely-shared utility packages can be pulled by dozens —
// three named roots plus the count tells the operator what to bump
// without drowning the payload).
const maxChainRoots = 3

// maxChainDepth bounds the upward walk (npm graphs can be cyclic;
// depth 20 exceeds any realistic dependency chain).
const maxChainDepth = 20

// chainIndex is the per-repo attribution state, built once per request
// when transitive findings exist.
//
// Round-19: the adjacency is PARTITIONED PER LOCKFILE. A monorepo's
// lockfiles are INDEPENDENT resolved graphs — one shared adjacency let
// a parent edge from apps/a's lockfile connect through a child name in
// apps/b's lockfile, fabricating an introduced_by path no single
// resolution ever produced. Each walk now stays inside one lockfile's
// graph; a finding still unions roots across lockfiles (a repo-level
// finding legitimately has one introduction path per lockfile that
// contains the package). Within a single lockfile, resolution stays
// NAME-level (the documented v0.27.133 edge model: lockfiles express
// parent → name@range, so same-name multi-version conflation inside
// one graph is the accepted trade-off — a full version-exact walk is
// not derivable from range constraints).
type chainIndex struct {
	graphs map[string]map[string][]string // lockfile_path -> (ecosystem|child) -> parent names
	// direct is per-lockfile (round 20): a package direct in apps/b
	// but TRANSITIVE in apps/a must not terminate apps/a's walk — it
	// is not an actionable root in that lockfile's graph. declared is
	// the repo-wide manifest fallback (repo_deps_libyear carries no
	// path column, so path-aware handling is not derivable for it —
	// an explicitly separate fallback by design).
	direct   map[string]map[string]bool // lockfile_path -> ecosystem|name
	declared map[string]bool            // repo-wide manifest-declared roots
}

func buildChainIndex(edges []db.RepoLockfileEdge, sets db.DirectPackageSets) *chainIndex {
	idx := &chainIndex{graphs: map[string]map[string][]string{}, direct: sets.ByLockfile, declared: sets.Declared}
	seen := map[string]bool{}
	for _, e := range edges {
		g := idx.graphs[e.LockfilePath]
		if g == nil {
			g = map[string][]string{}
			idx.graphs[e.LockfilePath] = g
		}
		key := db.LockfileGraphKey(e.Ecosystem, e.ChildName)
		dedup := e.LockfilePath + "\x00" + key + "<" + e.ParentName
		if seen[dedup] {
			continue
		}
		seen[dedup] = true
		g[key] = append(g[key], e.ParentName)
	}
	for _, g := range idx.graphs {
		for _, ps := range g {
			sort.Strings(ps) // deterministic chains
		}
	}
	return idx
}

// chainsFor walks child→parents from the vulnerable package to direct
// roots — one BFS per lockfile graph (shortest chains first within
// each), cycle-safe, depth-capped, roots deduped across lockfiles.
// Returns nil when no path reaches a direct root — the honest state
// for edge-less formats and orphaned packages.
func (idx *chainIndex) chainsFor(ecosystem, pkg string) []vulnChainJSON {
	paths := make([]string, 0, len(idx.graphs))
	for p := range idx.graphs {
		paths = append(paths, p)
	}
	sort.Strings(paths) // deterministic lockfile order
	var out []vulnChainJSON
	rootSeen := map[string]bool{}
	for _, p := range paths {
		if len(out) >= maxChainRoots {
			break
		}
		out = walkChainGraph(idx.graphs[p], idx.direct[p], idx.declared, ecosystem, pkg, rootSeen, out)
	}
	return out
}

// walkChainGraph is the BFS over ONE lockfile's adjacency. rootSeen is
// shared across lockfiles so the same direct root reached through two
// lockfiles is reported once (the first — lockfile-path order — wins).
func walkChainGraph(parents map[string][]string, lockfileDirect, declared map[string]bool,
	ecosystem, pkg string, rootSeen map[string]bool, out []vulnChainJSON) []vulnChainJSON {
	type node struct {
		name string
		path []string // pkg-first, reversed at emit
	}
	visited := map[string]bool{pkg: true}
	queue := []node{{name: pkg, path: []string{pkg}}}
	for len(queue) > 0 && len(out) < maxChainRoots {
		n := queue[0]
		queue = queue[1:]
		if len(n.path) > maxChainDepth {
			continue
		}
		for _, parent := range parents[db.LockfileGraphKey(ecosystem, n.name)] {
			if visited[parent] {
				continue
			}
			visited[parent] = true
			path := append(append([]string{}, n.path...), parent)
			key := db.LockfileGraphKey(ecosystem, parent)
			if lockfileDirect[key] || declared[key] {
				if rootSeen[parent] {
					continue // already attributed via another lockfile
				}
				rootSeen[parent] = true
				// Emit root-first.
				chain := make([]string, 0, len(path))
				for i := len(path) - 1; i >= 0; i-- {
					chain = append(chain, path[i])
				}
				out = append(out, vulnChainJSON{Root: parent, Chain: chain})
				if len(out) >= maxChainRoots {
					break
				}
				continue
			}
			queue = append(queue, node{name: parent, path: path})
		}
	}
	return out
}
