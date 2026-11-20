// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
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
	t.acct.enter()
	defer t.acct.leave()
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

	pass := t.walkBudget()
	gapOf := map[string]string{}     // a member pom -> why its tree is not whole
	hostsOf := map[string][]string{} // a member pom -> the hosts its chains used
	for _, p := range t.pomFiles {
		find(p)
		m := t.model(p)
		if m == nil {
			gapOf[p] = "a pom could not be read or parsed"
			continue
		}
		chain, _, whole := t.chainWhole(m)
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
			target := filepath.Join(filepath.Dir(p), filepath.FromSlash(mod))
			if info, err := os.Lstat(target); (err == nil && info.IsDir()) || !strings.HasSuffix(strings.ToLower(mod), ".xml") {
				target = filepath.Join(target, "pom.xml")
			}
			target = filepath.Clean(target)
			union(p, target)
			if t.model(target) == nil {
				gapOf[p] = "a module could not be read"
			}
		}
		if m.profileManagement {
			gapOf[p] = "dependencyManagement inside a profile is not applied"
		}
		c := t.context(m)
	imports:
		for _, cm := range c.chain {
			// EVERY import the pom declares (the index's whole list, which
			// the lookups never shorten): one that leads nowhere is what
			// makes the tree not whole.
			for _, j := range cm.index().imports {
				e := cm.managed[j]
				// Each import of each pom is a look of the walk: the poms
				// times the imports they inherit is not linear in what was
				// read. Past the walk's allowance the tree is not known to
				// be whole, and its entries stay inventoried.
				bg, ba, bv, ok := importCoordinates(pass, c, e)
				if !ok {
					gapOf[p] = mavenGapBound
					break imports
				}
				bom, ambiguous := t.byCoordinates(pass, bg, ba, bv)
				switch {
				case ambiguous:
					gapOf[p] = "a BOM import matches more than one pom of the repository"
				case bom == nil:
					// Asked through the one counted path. An import whose
					// coordinates hold a reference is another coordinate in
					// each pom that inherits it, and is asked for each (a
					// module that sets the property otherwise does import
					// another BOM) while the walk's allowance of requests
					// lasts; nothing is kept per pom and import beyond what
					// the walk's account has room for (review round 5: 199 KB
					// of poms kept 29,240 answers, 223 MB, and 194 KB asked
					// the source 199,651 times).
					notAsked := t.acct.askRefused
					remote, _ := t.remotePom(pass, bg, ba, bv, c.declaredFor)
					if remote == nil {
						gapOf[p] = "an imported BOM could not be obtained"
						if pass.tripped {
							gapOf[p] = mavenGapBound
							break imports
						}
						if t.acct.askRefused != notAsked {
							gapOf[p] = mavenGapRequests
						}
						continue
					}
					bomChain, _, _ := t.chainWhole(remote)
					hostsOf[p] = mergeHosts(hostsOf[p], chainHosts(bomChain))
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
	if t.acct.fired {
		// Nothing read after the backstop is known to be whole.
		for _, p := range t.pomFiles {
			treeGap[find(p)] = mavenGapBackstop
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
// the pass over the whole tree. ok is false when the walk's allowance did not
// cover working it out: the interpolation of any of the three, or the look.
// A coordinate that was not interpolated is not a coordinate, and is not
// looked for.
func importCoordinates(pass *pomBudget, c *pomContext, e pomManaged) (bg, ba, bv string, ok bool) {
	bg, okG := pass.interp(c, e.group)
	ba, okA := pass.interp(c, e.artifact)
	bv, okV := pass.interp(c, e.version)
	if !okG || !okA || !okV || !pass.spend(1) {
		return "", "", "", false
	}
	return bg, ba, bv, true
}

// Why a tree is not whole when it is the reader's own bounds that say so.
const (
	mavenGapBound    = "its BOM imports could not all be looked at within the reader's bound"
	mavenGapRequests = "its BOM imports could not all be asked about within the walk's allowance of requests"
	mavenGapBackstop = "the walk's wall-clock backstop ended the reading of its poms"
)

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
