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
	"strings"

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
type chainIndex struct {
	parents map[string][]string // ecosystem|child -> parent names
	direct  map[string]bool     // ecosystem|name -> is a direct dependency
}

func buildChainIndex(edges []db.RepoLockfileEdge, directNames map[string]bool) *chainIndex {
	idx := &chainIndex{parents: map[string][]string{}, direct: directNames}
	seen := map[string]bool{}
	for _, e := range edges {
		key := e.Ecosystem + "|" + e.ChildName
		dedup := key + "<" + e.ParentName
		if seen[dedup] {
			continue
		}
		seen[dedup] = true
		idx.parents[key] = append(idx.parents[key], e.ParentName)
	}
	for _, ps := range idx.parents {
		sort.Strings(ps) // deterministic chains
	}
	return idx
}

// chainsFor walks child→parents from the vulnerable package to direct
// roots via BFS (shortest chains first), cycle-safe, depth-capped.
// Returns nil when no path reaches a direct root — the honest state
// for edge-less formats and orphaned packages.
func (idx *chainIndex) chainsFor(ecosystem, pkg string) []vulnChainJSON {
	type node struct {
		name string
		path []string // pkg-first, reversed at emit
	}
	var out []vulnChainJSON
	visited := map[string]bool{pkg: true}
	queue := []node{{name: pkg, path: []string{pkg}}}
	for len(queue) > 0 && len(out) < maxChainRoots {
		n := queue[0]
		queue = queue[1:]
		if len(n.path) > maxChainDepth {
			continue
		}
		for _, parent := range idx.parents[ecosystem+"|"+n.name] {
			if visited[parent] {
				continue
			}
			visited[parent] = true
			path := append(append([]string{}, n.path...), parent)
			if idx.direct[ecosystem+"|"+strings.ToLower(parent)] {
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
