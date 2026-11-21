// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// MavenManaged is what SettleMavenManaged decides about a repository's
// declared Maven dependencies.
type MavenManaged struct {
	// Kept are the dependencies to inventory: everything that is not a
	// dependencyManagement entry, and the entries of trees not read whole.
	Kept []model.LibyearDep
	// Unused are entries of trees read whole whose version was applied to no
	// dependency (unused, or used at an overriding version), and BOM
	// imports' own coordinates. Each is recorded as a note, not a row.
	Unused []model.LibyearDep
	// NotWhole names each tree that holds managed entries and was not read
	// whole, with why. Its entries are in Kept.
	NotWhole []MavenTreeGap
	// TreeHosts names each tree whose parent and BOM chains used fetched
	// poms, with the repository hosts they came from (0.29.185). A tree can
	// depend on a host with no dependency row saying so.
	TreeHosts []MavenTreeHosts
}

// MavenTreeHosts is one Maven tree and the repository hosts its chains used.
type MavenTreeHosts struct {
	Root  string   `json:"root"`
	Hosts []string `json:"hosts"`
}

// MavenTreeGap is one Maven tree that was not read whole.
type MavenTreeGap struct {
	Root   string `json:"root"`   // the tree's root-most pom, relative to the clone
	Reason string `json:"reason"` // why it is not whole
}

// SettleMavenManaged takes dependencyManagement entries out of the
// inventory (v0.29.183; summary/70, 4.5, D9). An entry is not a dependency:
// it says which version a dependency WOULD get. deps are every declared
// dependency of the repository, each with its ManifestPath relative to the
// clone.
//
// Only for a tree read whole. A tree is a pom, the poms its <modules> name,
// and every pom whose verified parent chain reaches one of them; a pom with
// neither is its own tree. It is not whole when a pom of it could not be
// read or parsed, a parent or an imported BOM could not be obtained, a BOM
// import is ambiguous, or dependencyManagement sits inside a profile: a
// module that was not seen may be the user of an entry, so that tree's
// entries stay inventoried and no note is written for it. Other trees of the
// repository are unaffected (a broken fixture is its own tree).
func (w *Walk) SettleMavenManaged(deps []model.LibyearDep) MavenManaged {
	var res MavenManaged
	// Any Maven dependency at all: a tree with no managed entry of its own
	// still has hosts to record (its parent was fetched).
	anyMaven := false
	for _, d := range deps {
		if d.Manager == "maven" {
			anyMaven = true
			break
		}
	}
	if !anyMaven {
		res.Kept = deps
		return res
	}
	t := w.pom
	t.indexTree()

	// Union-find over pom paths.
	parent := map[string]string{}
	var find func(string) string
	find = func(p string) string {
		if q, ok := parent[p]; ok && q != p {
			r := find(q)
			parent[p] = r
			return r
		}
		parent[p] = p
		return p
	}
	union := func(a, b string) { parent[find(a)] = find(b) }

	// A BOM is whole when its own parent chain was followed to its end and
	// every BOM its chain imports was obtained and is whole. Round 7: only
	// the BOM itself was looked for, so a tree was reported whole, and its
	// managed entries noted as unused, while a version of one of its
	// dependencies had been withheld because a pom BEHIND the BOM (its
	// parent, a BOM it imports) could not be obtained. The verdict agrees
	// with what the questions do (pomContext.managed): a tree that is whole
	// has had nothing withheld. Decided once per BOM for the walk.
	//
	// height is how many BOMs deep the imports below a BOM go, itself
	// counted: a question follows imports pomChainLimit deep and no deeper
	// (pomContext.lookup), so a pom whose import is higher than that has
	// imports no question reaches, and is not whole. Both facts are the
	// BOM's own, whatever depth it is met at (round 8: the verdict was
	// worked out with the depth it was first met at and remembered for every
	// other, and counted one level more than the questions do, so that at
	// exactly one depth a tree was whole whose later BOM's version had been
	// stored in place of the deeper one's).
	type bomFacts struct {
		whole  bool
		height int
		done   bool
	}
	facts := map[*pomModel]*bomFacts{}
	var bomOf func(b *pomModel) *bomFacts
	bomOf = func(b *pomModel) *bomFacts {
		if f, ok := facts[b]; ok {
			// Met again inside itself (not done): a cycle of imports, which
			// Maven refuses to build and a question does not see past
			// (pomLookup.within). Every BOM of the cycle is not whole.
			if !f.done {
				return &bomFacts{}
			}
			return f
		}
		f := &bomFacts{whole: true, height: 1}
		facts[b] = f
		if _, _, _, whole := t.chainWhole(b); !whole {
			f.whole = false
		}
		bc := t.context(b)
		for _, cm := range bc.chain {
			for _, j := range cm.index().imports {
				bg, ba, bv := importCoordinates(bc, cm.managed[j])
				next, _, _ := t.obtain(bg, ba, bv, bc.asker(cm))
				if next == nil {
					f.whole = false
					continue
				}
				below := bomOf(next)
				if !below.whole {
					f.whole = false
				}
				if below.height+1 > f.height {
					f.height = below.height + 1
				}
			}
		}
		f.done = true
		return f
	}
	bomWhole := func(b *pomModel) bool {
		f := bomOf(b)
		return f.whole && f.height <= pomChainLimit
	}

	gapOf := map[string]string{}     // a member pom -> why its tree is not whole
	hostsOf := map[string][]string{} // a member pom -> the hosts its chains used
	for _, p := range t.pomFiles {
		find(p)
		m := t.model(p)
		if m == nil {
			gapOf[p] = "a pom could not be read or parsed"
			continue
		}
		chain, _, _, whole := t.chainWhole(m)
		hostsOf[p] = chainHosts(chain)
		for _, c := range chain[1:] {
			if !strings.HasPrefix(c.path, "remote:") {
				union(p, c.path)
			}
		}
		if !whole {
			gapOf[p] = "a parent pom could not be obtained"
		}
		for _, mod := range m.modules {
			target := modulePom(p, mod)
			union(p, target)
			if t.model(target) == nil {
				gapOf[p] = "a module could not be read"
			}
		}
		if m.profileManagement {
			gapOf[p] = "dependencyManagement inside a profile is not applied"
		}
		c := t.context(m)
		for _, cm := range c.chain {
			// EVERY import the pom declares (the index's whole list, which
			// the lookups never shorten): one that leads nowhere is what
			// makes the tree not whole.
			for _, j := range cm.index().imports {
				bg, ba, bv := importCoordinates(c, cm.managed[j])
				if _, ambiguous := t.byCoordinates(bg, ba, bv, cm.path); ambiguous {
					gapOf[p] = "a BOM import matches more than one pom of the repository"
					continue
				}
				// Found through the one path (obtain): the pom of the clone
				// with the coordinates, else a repository's, asked for once
				// per walk. An import whose coordinates hold a reference is
				// another coordinate in each pom that inherits it, and is
				// asked for each (a module that sets the property otherwise
				// does import another BOM); how many coordinates an analysis
				// may ask for is the source's to bound.
				bom, _, _ := t.obtain(bg, ba, bv, c.asker(cm))
				if bom == nil {
					gapOf[p] = "an imported BOM could not be obtained"
					continue
				}
				if strings.HasPrefix(bom.path, "remote:") {
					bomChain, _, _, _ := t.chainWhole(bom)
					hostsOf[p] = mergeHosts(hostsOf[p], chainHosts(bomChain))
				}
				// Whole only if its own chain and its own imports are
				// (round 7), and its imports go no deeper than a question
				// follows them (round 8).
				if !bomWhole(bom) {
					gapOf[p] = "an imported BOM could not be read whole"
				}
			}
		}
	}
	treeGap := map[string]string{}
	members := map[string][]string{}
	for _, p := range t.pomFiles {
		root := find(p)
		members[root] = append(members[root], p)
	}
	gapped := make([]string, 0, len(gapOf))
	for p := range gapOf {
		gapped = append(gapped, p)
	}
	sort.Strings(gapped) // a tree with two gaps reports the same one every time
	for _, p := range gapped {
		if root := find(p); treeGap[root] == "" {
			treeGap[root] = gapOf[p]
		}
	}

	abs := func(d model.LibyearDep) string {
		return filepath.Clean(filepath.Join(t.root, filepath.FromSlash(d.ManifestPath)))
	}
	// What each tree's dependencies really take: name and version.
	used := map[string]map[string]bool{}
	for _, d := range deps {
		if d.Manager != "maven" || d.ManagedEntry {
			continue
		}
		root := find(abs(d))
		if used[root] == nil {
			used[root] = map[string]bool{}
		}
		used[root][d.Name+"|"+d.Version] = true
	}
	holdsManaged := map[string]bool{}
	for _, d := range deps {
		if d.Manager != "maven" || !d.ManagedEntry {
			res.Kept = append(res.Kept, d)
			continue
		}
		root := find(abs(d))
		holdsManaged[root] = true
		switch {
		case treeGap[root] != "":
			res.Kept = append(res.Kept, d)
		case d.BOMImport || !used[root][d.Name+"|"+d.Version]:
			res.Unused = append(res.Unused, d)
		}
		// Otherwise a dependency took exactly this version: that dependency
		// is its row, and the entry leaves neither a row nor a note.
	}
	for root, why := range treeGap {
		if holdsManaged[root] {
			res.NotWhole = append(res.NotWhole, MavenTreeGap{Root: t.rootMost(members[root], root), Reason: why})
		}
	}
	sort.Slice(res.NotWhole, func(i, j int) bool { return res.NotWhole[i].Root < res.NotWhole[j].Root })
	treeHosts := map[string][]string{}
	for p, hosts := range hostsOf {
		if len(hosts) > 0 {
			treeHosts[find(p)] = mergeHosts(treeHosts[find(p)], hosts)
		}
	}
	for root, hosts := range treeHosts {
		res.TreeHosts = append(res.TreeHosts, MavenTreeHosts{Root: t.rootMost(members[root], root), Hosts: hosts})
	}
	sort.Slice(res.TreeHosts, func(i, j int) bool { return res.TreeHosts[i].Root < res.TreeHosts[j].Root })
	return res
}

// importCoordinates is what the import e is in the effective model c, for
// the pass over the whole tree.
func importCoordinates(c *pomContext, e pomManaged) (bg, ba, bv string) {
	return c.interpolate(e.group), c.interpolate(e.artifact), c.interpolate(e.version)
}

// rootMost names a tree by its shallowest pom, relative to the clone.
func (t *pomTree) rootMost(members []string, fallback string) string {
	best := fallback
	if len(members) > 0 {
		best = members[0]
	}
	depth := func(p string) int { return strings.Count(filepath.ToSlash(p), "/") }
	for _, p := range members {
		if depth(p) < depth(best) || (depth(p) == depth(best) && p < best) {
			best = p
		}
	}
	if rel, err := filepath.Rel(t.root, best); err == nil {
		return filepath.ToSlash(rel)
	}
	return filepath.ToSlash(best)
}
