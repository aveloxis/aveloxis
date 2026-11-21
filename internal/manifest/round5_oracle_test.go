// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5 (M5-5, M5-6): the oracle.
//
// Three rounds rebuilt the Maven tree's and the Gradle tree's readers for
// cost: indexes shared between effective models, entries filed by the
// literal half of their key, lists of imports compacted, scopes as chains of
// layers, readings and whole runs of applied scripts remembered and stood in
// for one another. The tests of those rounds pinned what the rebuilding was
// for (the cost), and the review found that a change to any of them that
// gave WRONG VERSIONS passed the suite.
//
// These are the readers the rebuilding replaced, written the slow way, and
// compared with the real ones over seeded random trees in several read
// orders: every value, every reason, every settle verdict. The reference
// has no index, no compaction, shares nothing and remembers nothing it works
// out (it keeps each pom as decoded, and that only): a question walks the
// chain, interpolates every managed entry in the order written, and asks
// every import in order; a Gradle scope is a map, copied for every reading.
//
// Round 6 (M6-10): the Maven reference shares NOTHING with the reader it
// checks but the text of the poms and the public types of a fetched pom: it
// decodes a pom, lists its dependencies, substitutes, ranks reasons and
// bounds a chain with code of its own, below. A mistake in the reader's
// decodePom, ParsePomXMLVersions, pomSubstitute, worseReason or modelOf is
// no longer one the reference makes too. The Gradle reference still shares
// the reading of ONE build file, of gradle.properties and of a catalogue,
// and the resolving of an apply path: it is a reference for what the tree
// does with those, not for them.

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// ---------------------------------------------------------------- Maven

// oracleSource is a source of poms held in a map, with outcomes for the
// coordinates that are not answered with a pom.
type oracleSource struct {
	poms     map[string]*RemotePom
	outcomes map[string]RemoteOutcome
}

func (s *oracleSource) RemotePom(g, a, v string, _ []string) (*RemotePom, RemoteOutcome) {
	key := g + ":" + a + ":" + v
	if o, ok := s.outcomes[key]; ok {
		return nil, o
	}
	if p, ok := s.poms[key]; ok {
		return p, RemoteFound
	}
	return nil, RemoteNotFound
}

// oraclePoms is the naive Maven tree.
type oraclePoms struct {
	root string
	src  PomSource
	// decoded holds each pom as decoded, by path: the one thing the
	// reference keeps, because decoding XML is not what is compared and is
	// nearly all of its time. Everything it works out is worked out again.
	decoded map[string]*oraclePom
	// undetermined counts the answers withheld because a pom consulted
	// before the one that manages the dependency could not be obtained.
	undetermined int
}

// model is the pom at path, decoded.
func (o *oraclePoms) model(path string) *oraclePom {
	path = filepath.Clean(path)
	if m, ok := o.decoded[path]; ok {
		return m
	}
	m := o.decode(path)
	o.decoded[path] = m
	return m
}

func (o *oraclePoms) decode(path string) *oraclePom {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	if rel, err := filepath.Rel(o.root, path); err != nil || strings.HasPrefix(rel, "..") {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return oracleDecode(string(data), path)
}

// remote is the pom a repository has under these coordinates, or why there
// is none (reason), and, when what the pom says is therefore NOT KNOWN, the
// reason a version behind it is withheld with (unknown). Round 7: only an
// answer of "not found" makes a pom known to say nothing. One that was not
// asked for (no source; a snapshot, a range, coordinates that hold a
// reference; a source that says it did not ask) is not known.
func (o *oraclePoms) remote(g, a, v string) (pom *oraclePom, reason, unknown string) {
	const notFound = "inherited_not_found"
	if o.src == nil {
		return nil, "", notFound
	}
	if g == "" || a == "" || v == "" || strings.Contains(g+a+v, "${") || strings.Contains(strings.ToUpper(v), "SNAPSHOT") || strings.ContainsAny(v, "[](),") {
		return nil, notFound, notFound
	}
	got, outcome := o.src.RemotePom(g, a, v, nil)
	switch {
	case outcome == RemoteFound && got != nil:
		return oracleRemoteModel(got, g+":"+a+":"+v), "", ""
	case outcome == RemoteFound, outcome == RemoteFailed:
		return nil, "inherited_lookup_failed", "inherited_lookup_failed"
	case outcome == RemoteRefused:
		return nil, "inherited_repository_refused", "inherited_repository_refused"
	case outcome == RemoteNotAsked:
		return nil, notFound, notFound
	}
	return nil, notFound, ""
}

// oracleCtx is one pom's effective model, built for the question.
type oracleCtx struct {
	o                        *oraclePoms
	chain                    []*oraclePom
	group, artifact, version string
	parentGroup, parentVer   string
	gap                      string
	// unknown: why what the missing end of the chain says is not known.
	unknown string
	whole   bool
}

func (o *oraclePoms) ctx(m *oraclePom) *oracleCtx {
	c := &oracleCtx{o: o, chain: []*oraclePom{m}, artifact: m.artifact, group: m.group, version: m.version}
	c.whole = true
	for cur := m; cur.parent != nil; {
		if len(c.chain) >= oracleChainLimit {
			c.whole = false
			break
		}
		var p *oraclePom
		if cur.parent.inTree {
			at := filepath.Join(filepath.Dir(cur.path), filepath.FromSlash(cur.parent.relativePath))
			if info, err := os.Lstat(at); err == nil && info.IsDir() {
				at = filepath.Join(at, "pom.xml")
			}
			at = filepath.Clean(at)
			if cand := o.model(at); cand != nil && cand.artifact == cur.parent.artifact &&
				(cand.group == "" || cand.group == cur.parent.group) &&
				(cand.version == "" || strings.Contains(cand.version, "${") || cand.version == cur.parent.version) {
				p = cand
			}
		}
		if p == nil {
			r, reason, unknown := o.remote(cur.parent.group, cur.parent.artifact, cur.parent.version)
			if r == nil {
				c.gap, c.unknown, c.whole = reason, unknown, false
				break
			}
			p = r
		}
		c.chain = append(c.chain, p)
		cur = p
	}
	if m.parent != nil {
		c.parentGroup, c.parentVer = m.parent.group, m.parent.version
		if c.group == "" {
			c.group = m.parent.group
		}
		if c.version == "" {
			c.version = m.parent.version
		}
	}
	return c
}

func (c *oracleCtx) interp(s string) string {
	return oracleSubstitute(s, func(name string) (string, bool) {
		pick := map[string]string{
			"project.version": c.version, "pom.version": c.version, "version": c.version,
			"project.groupId": c.group, "pom.groupId": c.group, "groupId": c.group,
			"project.artifactId": c.artifact, "pom.artifactId": c.artifact, "artifactId": c.artifact,
			"project.parent.version": c.parentVer, "parent.version": c.parentVer,
			"project.parent.groupId": c.parentGroup, "parent.groupId": c.parentGroup,
		}
		if v := pick[name]; v != "" {
			return v, true
		}
		for _, m := range c.chain {
			if v, ok := m.props[name]; ok {
				return v, true
			}
		}
		return "", false
	})
}

// allPoms lists every pom.xml of the clone, in the order a walk of the
// directory gives.
func (o *oraclePoms) allPoms() []string {
	var out []string
	_ = filepath.Walk(o.root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Base(path) == "pom.xml" {
			out = append(out, filepath.Clean(path))
		}
		return nil
	})
	return out
}

// byCoordinates scans every pom of the clone for the ones with these
// coordinates, as the clone alone gives them (no source asked).
func (o *oraclePoms) byCoordinates(g, a, v string) (m *oraclePom, ambiguous bool) {
	if g == "" || a == "" || v == "" || strings.Contains(g+a+v, "${") {
		return nil, false
	}
	treeOnly := &oraclePoms{root: o.root, decoded: o.decoded}
	var found []*oraclePom
	for _, p := range o.allPoms() {
		cand := treeOnly.model(p)
		if cand == nil {
			continue
		}
		c := treeOnly.ctx(cand)
		if c.interp(c.group) == g && c.interp(c.artifact) == a && c.interp(c.version) == v {
			found = append(found, cand)
		}
	}
	if len(found) != 1 {
		return nil, len(found) > 1
	}
	return found[0], false
}

func oracleHosts(chain []*oraclePom, more []string) []string {
	set := map[string]bool{}
	for _, m := range chain {
		if m.servedBy != "" {
			set[m.servedBy] = true
		}
	}
	for _, h := range more {
		if h != "" {
			set[h] = true
		}
	}
	var out []string
	for h := range set {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// oracleQuestion is one question's state: the BOMs searched and the worst
// reason met.
type oracleQuestion struct {
	searched map[string]bool
	gap      string
	// before: the worst reason among the poms consulted before where the
	// question now stands that could not be obtained (Maven's order: a
	// chain's own entries, the nearest pom first; then the imports).
	before string
}

// lookup is the question in c. stop: a pom that was read manages the
// artifact and cannot give its version; that is the answer (no version),
// and nothing after it is asked.
//
// The rule (rounds 6 and 7): a managed version is taken from a pom only if
// every pom consulted before it is KNOWN not to manage the artifact.
func (c *oracleCtx) lookup(q *oracleQuestion, group, artifact string, depth int) (version string, found bool, hosts []string, stop bool) {
	// An explicit entry anywhere in the chain: the nearest pom, and in a pom
	// the first entry written that means this coordinate here.
	for _, m := range c.chain {
		for _, e := range m.managed {
			if e.isImport {
				continue
			}
			if c.interp(e.group) == group && c.interp(e.artifact) == artifact {
				v := c.interp(e.version)
				if v != "" && !strings.Contains(v, "${") {
					return v, true, nil, false
				}
				return v, false, nil, true
			}
		}
	}
	// The chain's missing end comes before every import.
	q.before = oracleWorse(q.before, c.unknown)
	if depth >= oracleChainLimit {
		return "", false, nil, false
	}
	// Then the BOMs the chain imports, in order.
	for _, m := range c.chain {
		for _, e := range m.managed {
			if !e.isImport {
				continue
			}
			bg, ba, bv := c.interp(e.group), c.interp(e.artifact), c.interp(e.version)
			bom, ambiguous := c.o.byCoordinates(bg, ba, bv)
			reason, unknown := "", ""
			if ambiguous {
				unknown = "inherited_not_found" // two poms of the clone: which is meant is not known
			} else if bom == nil {
				bom, reason, unknown = c.o.remote(bg, ba, bv)
			}
			if bom == nil {
				q.gap = oracleWorse(q.gap, reason)
				q.before = oracleWorse(q.before, unknown)
				continue
			}
			if q.searched[bom.path] {
				continue
			}
			q.searched[bom.path] = true
			bc := c.o.ctx(bom)
			q.gap = oracleWorse(q.gap, bc.gap)
			if v, ok, via, stop := bc.lookup(q, group, artifact, depth+1); ok {
				return v, true, oracleHosts(bc.chain, via), false
			} else if stop {
				return v, false, nil, true
			}
		}
	}
	return "", false, nil, false
}

// oracleAnswer is what a reader says of one declared dependency: the
// fields the tree decides.
type oracleAnswer struct {
	Name, Version, Source, Reason string
	Hosts                         []string
}

// oracleAnswersOf are the real reader's answers: its rows that are
// dependencies (not dependencyManagement entries), in order.
func oracleAnswersOf(deps []model.LibyearDep) []oracleAnswer {
	var out []oracleAnswer
	for _, d := range deps {
		if d.ManagedEntry {
			continue
		}
		out = append(out, oracleAnswer{d.Name, oracleUnresolved(d.Version), d.VersionSource, d.VersionReason, append([]string{}, d.VersionHosts...)})
	}
	return out
}

// settle is what the pom at path gives its declared dependencies.
func (o *oraclePoms) settle(path string) []oracleAnswer {
	m := o.model(path)
	if m == nil {
		return nil
	}
	c := o.ctx(m)
	gap := c.gap
	var out []oracleAnswer
	for _, d := range m.deps {
		a := oracleAnswer{Name: d.group + ":" + d.artifact, Version: d.version, Hosts: []string{}}
		switch {
		case strings.Contains(a.Version, "${"):
			// What the pom's own text settles came through no repository;
			// what needs its parents came through the hosts that served
			// them.
			own := oracleSubstitute(a.Version, func(name string) (string, bool) {
				switch name {
				case "project.version", "pom.version", "version":
					return m.version, m.version != ""
				case "project.groupId", "pom.groupId", "groupId":
					return m.group, m.group != ""
				case "project.artifactId", "pom.artifactId", "artifactId":
					return m.artifact, m.artifact != ""
				}
				v, ok := m.props[name]
				return v, ok
			})
			if !strings.Contains(own, "${") {
				a.Version = own
			} else if v := c.interp(a.Version); !strings.Contains(v, "${") {
				a.Version, a.Hosts = v, append([]string{}, oracleHosts(c.chain, nil)...)
			}
		case a.Version == "":
			q := &oracleQuestion{searched: map[string]bool{}, gap: c.gap}
			v, found, via, _ := c.lookup(q, d.group, d.artifact, 0)
			gap = oracleWorse(gap, q.gap)
			switch {
			case found && q.before == "":
				a.Version, a.Source = v, "managed"
				a.Hosts = append([]string{}, oracleHosts(c.chain, via)...)
			case found:
				// A pom consulted before the one that manages it could not
				// be obtained: not determined.
				a.Reason = q.before
				o.undetermined++
			case gap != "":
				a.Reason = gap
			}
		}
		a.Version = oracleUnresolved(a.Version)
		out = append(out, a)
	}
	return out
}

// oracleUnresolved is a version as the comparison takes it: one that still
// holds a reference is not a version, however much of it was substituted
// (the analysis stores none for it: NormalizeParsedVersion).
func oracleUnresolved(v string) string {
	if strings.Contains(v, "${") {
		return "(unresolved)"
	}
	return v
}

// What the reference does NOT share with the readers it checks (round 6):
// its own decoding of a pom, its own model of a fetched one, its own
// substitution, its own ranking of reasons and its own bound on a chain.

// oraclePom is a pom as the reference decodes it.
type oraclePom struct {
	path                     string
	group, artifact, version string
	parent                   *oracleParent
	props                    map[string]string
	managed                  []oracleManaged
	deps                     []oracleManaged
	modules                  []string
	profileManagement        bool
	servedBy                 string
}

type oracleParent struct {
	group, artifact, version, relativePath string
	inTree                                 bool
}

type oracleManaged struct {
	group, artifact, version string
	isImport                 bool
}

// oracleChainLimit is how long a chain of parents, or of BOMs importing
// BOMs, is followed: Maven itself has no such bound, and the reader's is
// part of what it documents.
const oracleChainLimit = 32

type oracleXMLDep struct {
	G     string `xml:"groupId"`
	A     string `xml:"artifactId"`
	V     string `xml:"version"`
	Type  string `xml:"type"`
	Scope string `xml:"scope"`
}

func oracleDecode(content, path string) *oraclePom {
	var x struct {
		G      string `xml:"groupId"`
		A      string `xml:"artifactId"`
		V      string `xml:"version"`
		Parent *struct {
			G   string  `xml:"groupId"`
			A   string  `xml:"artifactId"`
			V   string  `xml:"version"`
			Rel *string `xml:"relativePath"`
		} `xml:"parent"`
		Props struct {
			All []struct {
				XMLName xml.Name
				Text    string `xml:",chardata"`
			} `xml:",any"`
		} `xml:"properties"`
		Managed  []oracleXMLDep `xml:"dependencyManagement>dependencies>dependency"`
		Deps     []oracleXMLDep `xml:"dependencies>dependency"`
		Modules  []string       `xml:"modules>module"`
		Profiles []struct {
			Managed []oracleXMLDep `xml:"dependencyManagement>dependencies>dependency"`
		} `xml:"profiles>profile"`
	}
	if err := xml.Unmarshal([]byte(content), &x); err != nil {
		return nil
	}
	t := strings.TrimSpace
	m := &oraclePom{path: path, group: t(x.G), artifact: t(x.A), version: t(x.V), props: map[string]string{}}
	if x.Parent != nil {
		m.parent = &oracleParent{group: t(x.Parent.G), artifact: t(x.Parent.A), version: t(x.Parent.V), relativePath: "../pom.xml", inTree: true}
		if x.Parent.Rel != nil {
			m.parent.relativePath = t(*x.Parent.Rel)
			m.parent.inTree = m.parent.relativePath != ""
		}
	}
	for _, p := range x.Props.All {
		m.props[p.XMLName.Local] = t(p.Text)
	}
	for _, d := range x.Managed {
		m.managed = append(m.managed, oracleManaged{t(d.G), t(d.A), t(d.V), t(d.Scope) == "import" && t(d.Type) == "pom"})
	}
	for _, d := range x.Deps {
		m.deps = append(m.deps, oracleManaged{group: t(d.G), artifact: t(d.A), version: t(d.V)})
	}
	for _, mod := range x.Modules {
		if mod = t(mod); mod != "" {
			m.modules = append(m.modules, mod)
		}
	}
	for _, pr := range x.Profiles {
		if len(pr.Managed) > 0 {
			m.profileManagement = true
		}
	}
	return m
}

func oracleRemoteModel(r *RemotePom, key string) *oraclePom {
	m := &oraclePom{path: "remote:" + key, group: r.Group, artifact: r.Artifact, version: r.Version, props: map[string]string{}, servedBy: r.ServedBy}
	for k, v := range r.Properties {
		m.props[k] = v
	}
	if r.Parent != nil {
		m.parent = &oracleParent{group: r.Parent.Group, artifact: r.Parent.Artifact, version: r.Parent.Version}
	}
	for _, e := range r.Managed {
		m.managed = append(m.managed, oracleManaged{e.Group, e.Artifact, e.Version, e.Import})
	}
	return m
}

// oracleSubstitute replaces ${name} references, pass after pass, while a
// pass changes something: ten passes at most, and nothing longer than a
// coordinate may be (then the string is left as it was written).
func oracleSubstitute(s string, resolve func(string) (string, bool)) string {
	const limit, passes = 2704, 10 // pkgid.MaxNameBytes; the depth of properties followed
	if len(s) > limit {
		return s
	}
	original := s
	for pass := 0; pass < passes && strings.Contains(s, "${"); pass++ {
		out, changed := "", false
		for rest := s; rest != ""; {
			at := strings.Index(rest, "${")
			if at < 0 {
				out += rest
				break
			}
			out += rest[:at]
			rest = rest[at:]
			end := strings.Index(rest, "}")
			if end < 0 {
				out += rest
				break
			}
			name := rest[2:end]
			if name == "" || strings.ContainsAny(name, "${") {
				out += "${"
				rest = rest[2:]
				continue
			}
			ref := rest[:end+1]
			if v, ok := resolve(name); ok && v != ref {
				out += v
				changed = true
			} else {
				out += ref
			}
			rest = rest[end+1:]
		}
		if len(out) > limit {
			return original
		}
		if !changed {
			break
		}
		s = out
	}
	return s
}

// oracleWorse is the reason to report of two: a lookup that failed may yet
// succeed and is said first, then a refusal, then "not found".
func oracleWorse(a, b string) string {
	rank := map[string]int{"inherited_lookup_failed": 3, "inherited_repository_refused": 2, "inherited_not_found": 1}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// managedVerdict is SettleMavenManaged, decided from the poms alone.
func (o *oraclePoms) managedVerdict(deps []model.LibyearDep) MavenManaged {
	var res MavenManaged
	poms := o.allPoms()
	parent := map[string]string{}
	var find func(string) string
	find = func(p string) string {
		if q, ok := parent[p]; ok && q != p {
			return find(q)
		}
		parent[p] = p
		return p
	}
	union := func(a, b string) { parent[find(a)] = find(b) }
	gapOf, hostsOf := map[string]string{}, map[string][]string{}
	// A BOM is whole when its own chain is, and every BOM its chain imports
	// was obtained and is whole (round 7): a tree that is whole has had
	// nothing withheld.
	wholeBOM := map[string]bool{}
	var bomWhole func(b *oraclePom, depth int) bool
	bomWhole = func(b *oraclePom, depth int) bool {
		if w, ok := wholeBOM[b.path]; ok {
			return w
		}
		wholeBOM[b.path] = true
		ok := depth < oracleChainLimit
		if ok {
			bc := o.ctx(b)
			ok = bc.whole
			for _, cm := range bc.chain {
				for _, e := range cm.managed {
					if !e.isImport {
						continue
					}
					bg, ba, bv := bc.interp(e.group), bc.interp(e.artifact), bc.interp(e.version)
					next, ambiguous := o.byCoordinates(bg, ba, bv)
					if next == nil && !ambiguous {
						next, _, _ = o.remote(bg, ba, bv)
					}
					if next == nil || !bomWhole(next, depth+1) {
						ok = false
					}
				}
			}
		}
		wholeBOM[b.path] = ok
		return ok
	}
	for _, p := range poms {
		find(p)
		m := o.model(p)
		if m == nil {
			gapOf[p] = "a pom could not be read or parsed"
			continue
		}
		c := o.ctx(m)
		hostsOf[p] = oracleHosts(c.chain, nil)
		for _, up := range c.chain[1:] {
			if !strings.HasPrefix(up.path, "remote:") {
				union(p, up.path)
			}
		}
		if !c.whole {
			gapOf[p] = "a parent pom could not be obtained"
		}
		for _, mod := range m.modules {
			target := filepath.Join(filepath.Dir(p), filepath.FromSlash(mod))
			if info, err := os.Lstat(target); (err == nil && info.IsDir()) || !strings.HasSuffix(strings.ToLower(mod), ".xml") {
				target = filepath.Join(target, "pom.xml")
			}
			target = filepath.Clean(target)
			union(p, target)
			if o.model(target) == nil {
				gapOf[p] = "a module could not be read"
			}
		}
		if m.profileManagement {
			gapOf[p] = "dependencyManagement inside a profile is not applied"
		}
		for _, cm := range c.chain {
			for _, e := range cm.managed {
				if !e.isImport {
					continue
				}
				bg, ba, bv := c.interp(e.group), c.interp(e.artifact), c.interp(e.version)
				bom, ambiguous := o.byCoordinates(bg, ba, bv)
				switch {
				case ambiguous:
					gapOf[p] = "a BOM import matches more than one pom of the repository"
				case bom != nil:
					if !bomWhole(bom, 0) {
						gapOf[p] = "an imported BOM could not be read whole"
					}
				case bom == nil:
					r, _, _ := o.remote(bg, ba, bv)
					if r == nil {
						gapOf[p] = "an imported BOM could not be obtained"
						continue
					}
					hostsOf[p] = oracleHosts(o.ctx(r).chain, hostsOf[p])
					if !bomWhole(r, 0) {
						gapOf[p] = "an imported BOM could not be read whole"
					}
				}
			}
		}
	}
	members := map[string][]string{}
	for _, p := range poms {
		members[find(p)] = append(members[find(p)], p)
	}
	treeGap := map[string]string{}
	for _, p := range poms { // in path order: a tree with two gaps reports the first
		if g, ok := gapOf[p]; ok && treeGap[find(p)] == "" {
			treeGap[find(p)] = g
		}
	}
	name := func(root string) string {
		best := root
		if len(members[root]) > 0 {
			best = members[root][0]
		}
		depth := func(p string) int { return strings.Count(filepath.ToSlash(p), "/") }
		for _, p := range members[root] {
			if depth(p) < depth(best) || (depth(p) == depth(best) && p < best) {
				best = p
			}
		}
		rel, _ := filepath.Rel(o.root, best)
		return filepath.ToSlash(rel)
	}
	abs := func(d model.LibyearDep) string {
		return filepath.Clean(filepath.Join(o.root, filepath.FromSlash(d.ManifestPath)))
	}
	used := map[string]bool{}
	for _, d := range deps {
		if d.Manager == "maven" && !d.ManagedEntry {
			used[find(abs(d))+"|"+d.Name+"|"+d.Version] = true
		}
	}
	holds := map[string]bool{}
	for _, d := range deps {
		if d.Manager != "maven" || !d.ManagedEntry {
			res.Kept = append(res.Kept, d)
			continue
		}
		root := find(abs(d))
		holds[root] = true
		switch {
		case treeGap[root] != "":
			res.Kept = append(res.Kept, d)
		case d.BOMImport || !used[root+"|"+d.Name+"|"+d.Version]:
			res.Unused = append(res.Unused, d)
		}
	}
	for root, why := range treeGap {
		if holds[root] {
			res.NotWhole = append(res.NotWhole, MavenTreeGap{Root: name(root), Reason: why})
		}
	}
	sort.Slice(res.NotWhole, func(i, j int) bool { return res.NotWhole[i].Root < res.NotWhole[j].Root })
	hosts := map[string][]string{}
	for p, h := range hostsOf {
		if len(h) > 0 {
			hosts[find(p)] = oracleHosts(nil, append(hosts[find(p)], h...))
		}
	}
	for root, h := range hosts {
		res.TreeHosts = append(res.TreeHosts, MavenTreeHosts{Root: name(root), Hosts: h})
	}
	sort.Slice(res.TreeHosts, func(i, j int) bool { return res.TreeHosts[i].Root < res.TreeHosts[j].Root })
	return res
}

// oraclePomTree makes one random Maven clone: a root, a middle parent,
// applications, BOMs that import each other, poms with the same
// coordinates, a fixture that may not parse; managed entries and imports
// whose coordinates are literal or hold references that the poms answer
// differently; and a source of poms with every outcome.
func oraclePomTree(rng *rand.Rand) (files map[string]string, src *oracleSource) {
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	groups := []string{"g1", "g2", "t", "${grp}", "${project.groupId}", "${nope}"}
	arts := []string{"a1", "a2", "a3", "${art}", "a${n}", "${project.artifactId}"}
	vers := []string{"1.0", "2.0", "${v1}", "${v2}", "${nope}", "${v1}-${sfx}", "${project.version}", "${project.parent.version}", "${revision}", "", "${chain}"}
	propNames := []string{"grp", "art", "n", "v1", "v2", "sfx", "chain", "revision", "bv", "bn"}
	propVals := []string{"g1", "g2", "a1", "a2", "1", "2", "3", "9.9", "${v1}", "${v2}", "${chain}", "t", "bomA", "gone1"}
	bomNames := []string{"bomA", "bomB", "bomC", "bomD", "rb1", "rb2", "gone1", "gone2", "dup", "${bn}", "snap"}
	bomVers := []string{"1", "1", "1", "${bv}", "${project.version}", "2", "1-SNAPSHOT", "[1,2)"}
	body := func(withDeps bool) (props map[string]string, managed []RemoteManaged, text string) {
		var sb strings.Builder
		props = map[string]string{}
		sb.WriteString("<properties>")
		for _, p := range propNames {
			if rng.Intn(3) == 0 {
				v := pick(propVals)
				props[p] = v
				sb.WriteString("<" + p + ">" + v + "</" + p + ">")
			}
		}
		sb.WriteString("</properties><dependencyManagement><dependencies>")
		for i, n := 0, rng.Intn(8); i < n; i++ {
			if rng.Intn(3) == 0 {
				g, a, v := pick([]string{"t", "t", "t", "${grp}"}), pick(bomNames), pick(bomVers)
				managed = append(managed, RemoteManaged{Group: g, Artifact: a, Version: v, Import: true})
				sb.WriteString(r4import(g, a, v))
			} else {
				g, a, v := pick(groups), pick(arts), pick(vers)
				managed = append(managed, RemoteManaged{Group: g, Artifact: a, Version: v})
				sb.WriteString(r4dep(g, a, v))
			}
		}
		sb.WriteString("</dependencies></dependencyManagement>")
		if withDeps {
			sb.WriteString("<dependencies>")
			for _, g := range []string{"g1", "g2", "t"} {
				for _, a := range []string{"a1", "a2", "a3"} {
					if rng.Intn(4) != 0 {
						sb.WriteString(r4dep(g, a, ""))
					}
				}
			}
			sb.WriteString(r4dep("g1", "px", "${v1}"))
			sb.WriteString("</dependencies>")
		}
		if rng.Intn(25) == 0 {
			sb.WriteString("<profiles><profile><id>p</id><dependencyManagement><dependencies>" + r4dep("g1", "a1", "5") + "</dependencies></dependencyManagement></profile></profiles>")
		}
		return props, managed, sb.String()
	}
	gen := func(artifact, parent, ver, modules string) string {
		var sb strings.Builder
		sb.WriteString("<project>")
		switch parent {
		case "":
		case "ext":
			sb.WriteString("<parent><groupId>t</groupId><artifactId>rparent</artifactId><version>1</version><relativePath/></parent>")
		case "extgone":
			sb.WriteString("<parent><groupId>t</groupId><artifactId>goneparent</artifactId><version>1</version></parent>")
		default:
			// Sometimes a version the pom at that place does not have: it is
			// then not the parent (SR-6), and the parent is asked for.
			sb.WriteString("<parent><groupId>t</groupId><artifactId>" + parent + "</artifactId><version>" + pick([]string{"1", "1", "1", "1", "8"}) + "</version><relativePath>../" + parent + "</relativePath></parent>")
		}
		if rng.Intn(4) != 0 || parent == "" {
			sb.WriteString("<groupId>t</groupId>")
		}
		sb.WriteString("<artifactId>" + artifact + "</artifactId>")
		if ver != "" {
			sb.WriteString("<version>" + ver + "</version>")
		}
		sb.WriteString(modules)
		_, _, b := body(true)
		sb.WriteString(b + "</project>")
		return sb.String()
	}
	fetched := func(name string, parent bool) *RemotePom {
		props, managed, _ := body(false)
		r := &RemotePom{Group: "t", Artifact: name, Version: "1", Properties: props, Managed: managed, ServedBy: pick([]string{"repo.example", "other.example"})}
		if parent {
			r.Parent = &RemotePomParent{Group: "t", Artifact: "rparent2", Version: "1"}
		}
		return r
	}
	mods := ""
	if rng.Intn(2) == 0 {
		mods = "<modules><module>../mid</module><module>../app2</module>" + pick([]string{"", "<module>../missing</module>", "<module>../app3</module>", "<module>../fix</module>"}) + "</modules>"
	}
	files = map[string]string{
		"root/pom.xml": gen("root", pick([]string{"", "", "ext", "extgone"}), "1", mods),
		"mid/pom.xml":  gen("mid", "root", pick([]string{"1", ""}), ""),
		"app/pom.xml":  gen("app", "mid", pick([]string{"1", "", "7"}), ""),
		"app2/pom.xml": gen("app2", "root", pick([]string{"1", "", "${revision}"}), ""),
		"app3/pom.xml": gen("app3", pick([]string{"", "ext", "extgone", "mid"}), "1", ""),
		"bomA/pom.xml": gen("bomA", "", "1", ""),
		"bomB/pom.xml": gen("bomB", pick([]string{"", "root", "bomA"}), pick([]string{"1", ""}), ""),
		"bomC/pom.xml": gen("bomC", pick([]string{"", "ext"}), "1", ""),
		"bomD/pom.xml": gen("bomD", "", pick([]string{"1", "2", "${bv}"}), ""),
	}
	if rng.Intn(3) == 0 {
		files["dup1/pom.xml"] = gen("dup", "", "1", "")
		if rng.Intn(2) == 0 {
			files["dup2/pom.xml"] = gen("dup", "", "1", "")
		}
	}
	if rng.Intn(6) == 0 {
		files["fix/pom.xml"] = pick([]string{"<project><artifactId>fix</artifactId", gen("fix", "root", "1", ""), gen("fix", "", "1", "")})
	}
	if rng.Intn(4) == 0 {
		return files, nil // no source: nothing outside the clone is asked
	}
	src = &oracleSource{poms: map[string]*RemotePom{
		"t:rb1:1": fetched("rb1", rng.Intn(2) == 0), "t:rb2:1": fetched("rb2", false),
		"t:rparent:1": fetched("rparent", rng.Intn(3) == 0), "t:rparent2:1": fetched("rparent2", false),
	}, outcomes: map[string]RemoteOutcome{}}
	for _, k := range []string{"t:gone1:1", "t:gone2:1", "t:goneparent:1", "t:rb2:1", "t:rparent2:1", "t:gone1:2", "t:rb1:2", "t:rparent:1"} {
		// A lookup that fails and a repository that is refused are what
		// withholds an answer behind them (round 6): often enough for the
		// comparison to see it.
		switch rng.Intn(8) {
		case 0, 1:
			src.outcomes[k] = RemoteFailed
		case 2, 3:
			src.outcomes[k] = RemoteRefused
		case 4:
			src.outcomes[k] = RemoteNotFound
		case 5:
			// Round 7: not asked for, which is not "not found".
			src.outcomes[k] = RemoteNotAsked
		}
	}
	return files, src
}

// How many random trees each oracle test compares, each in oracleOrders
// read orders: some fifteen thousand readings of poms and thirty thousand of
// build files. The Maven reference is the slower (every question scans the
// clone), so it has fewer. Every one of the review's mutants that gives a
// wrong answer shows within these (the round's report lists them).
const (
	oraclePomTrees    = 500
	oracleGradleTrees = 1200
	oracleOrders      = 3
)

func oracleWrite(t *testing.T, root string, files map[string]string) {
	t.Helper()
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	for rel, c := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func oracleJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func oracleDump(files map[string]string) string {
	var rels []string
	for r := range files {
		rels = append(rels, r)
	}
	sort.Strings(rels)
	var sb strings.Builder
	for _, r := range rels {
		fmt.Fprintf(&sb, "--- %s\n%s\n", r, files[r])
	}
	return sb.String()
}

// TestThePomTreeAnswersAsTheNaiveReaderDoes compares the Maven tree's
// reader with the reference: every dependency's version, source, reason and
// hosts for every pom, in several read orders, and what SettleMavenManaged
// decides from them.
func TestThePomTreeAnswersAsTheNaiveReaderDoes(t *testing.T) {
	rng := rand.New(rand.NewSource(20261120))
	root := t.TempDir()
	reads, settles, managedAnswers, reasons, refKeyed, undetermined := 0, 0, 0, 0, 0, 0
	for it := 0; it < oraclePomTrees; it++ {
		files, src := oraclePomTree(rng)
		oracleWrite(t, root, files)
		var rels []string
		for rel := range files {
			rels = append(rels, rel)
		}
		sort.Strings(rels)
		o := &oraclePoms{root: root, decoded: map[string]*oraclePom{}}
		if src != nil {
			o.src = src
		}
		want := map[string]string{}
		for _, rel := range rels {
			answers := o.settle(filepath.Join(root, rel))
			for _, a := range answers {
				if a.Source != "" {
					managedAnswers++
				}
				if a.Reason != "" {
					reasons++
				}
			}
			want[rel] = oracleJSON(answers)
		}
		undetermined += o.undetermined
		refKeyed += strings.Count(oracleDump(files), "<groupId>${")
		for order := 0; order < oracleOrders; order++ {
			seq := append([]string(nil), rels...)
			if order > 0 {
				rng.Shuffle(len(seq), func(i, j int) { seq[i], seq[j] = seq[j], seq[i] })
			}
			w := NewWalk(root, nil)
			if src != nil {
				w.SetPomSource(src)
			}
			byRel := map[string][]model.LibyearDep{}
			for _, rel := range seq {
				got := w.Read(filepath.Join(root, rel), Options{})
				reads++
				if got.Failure != nil && !strings.Contains(files[rel], "<artifactId>fix</artifactId") {
					t.Fatalf("tree %d order %v: %s was refused: %v\n%s", it, seq, rel, got.Failure, oracleDump(files))
				}
				for i := range got.Deps {
					got.Deps[i].ManifestPath = rel
				}
				byRel[rel] = got.Deps
				if g := oracleJSON(oracleAnswersOf(got.Deps)); g != want[rel] {
					t.Fatalf("tree %d, read order %v: %s\n the tree reader gives %s\n the naive reader gives %s\n%s", it, seq, rel, g, want[rel], oracleDump(files))
				}
			}
			var all []model.LibyearDep
			for _, rel := range rels {
				all = append(all, byRel[rel]...)
			}
			settles++
			if g, n := oracleJSON(w.SettleMavenManaged(all)), oracleJSON(o.managedVerdict(all)); g != n {
				t.Fatalf("tree %d, read order %v: SettleMavenManaged\n decides %s\n the naive reader decides %s\n%s", it, seq, g, n, oracleDump(files))
			}
		}
	}
	t.Logf("%d trees: %d readings and %d settle verdicts compared; the reference gave %d managed versions and %d reasons; %d reference-keyed coordinates", oraclePomTrees, reads, settles, managedAnswers, reasons, refKeyed)
	t.Logf("%d answers were withheld because a pom consulted before the one that manages the dependency could not be read", undetermined)
	// The denominator: the trees exercise what the comparison is for.
	if managedAnswers < oraclePomTrees || reasons < oraclePomTrees || refKeyed < oraclePomTrees || undetermined < oraclePomTrees/10 {
		t.Errorf("the random trees gave %d managed versions, %d reasons, %d reference-keyed coordinates and %d answers withheld over %d trees: too few to compare anything", managedAnswers, reasons, refKeyed, undetermined, oraclePomTrees)
	}
}

// ---------------------------------------------------------------- Gradle

// oracleGradle is the naive Gradle tree: a scope is one map, copied for
// every directory and every reading; a script is read again every time it
// is applied; nothing is remembered.
type oracleGradle struct {
	root string
	// paths resolves an apply path, which was not rebuilt.
	paths *gradleTree
}

type oracleScope struct {
	vars map[string]string
	libs map[string][]gradleDecl
}

func (s oracleScope) copy() oracleScope {
	out := oracleScope{vars: map[string]string{}, libs: map[string][]gradleDecl{}}
	for k, v := range s.vars {
		out.vars[k] = v
	}
	for k, v := range s.libs {
		out.libs[k] = v
	}
	return out
}

// with is vars with what one source assigned: the names it left with two
// values are not known after it, whatever was known before.
func oracleWith(vars map[string]string, own map[string]string, gone map[string]bool) map[string]string {
	out := map[string]string{}
	for k, v := range vars {
		out[k] = v
	}
	for k := range gone {
		delete(out, k)
	}
	for k, v := range own {
		out[k] = v
	}
	return out
}

func (o *oracleGradle) file(path string) (string, bool) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	data, err := os.ReadFile(path)
	return string(data), err == nil
}

func (o *oracleGradle) read(content string, vars map[string]string, libs map[string][]gradleDecl) *gradleBuild {
	var l *gradleLibs
	if len(libs) > 0 {
		l = &gradleLibs{own: libs}
	}
	return readGradleBuildWith(content, (*gradleVars)(nil).push(oracleWith(vars, nil, nil), nil), l)
}

func (o *oracleGradle) within(path string) bool {
	rel, err := filepath.Rel(o.root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// applied is vars with what the scripts named assign, each read with what
// was known before it, and the scripts it applies read first.
func (o *oracleGradle) applied(applies []string, dir string, vars map[string]string, depth int, seen map[string]bool) map[string]string {
	if depth >= gradleApplyDepth {
		return vars
	}
	for _, written := range applies {
		p := o.paths.applyPath(written, dir)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		content, ok := o.file(p)
		if !ok || !o.within(p) {
			continue
		}
		first := o.read(content, vars, nil)
		vars = o.applied(first.applies, filepath.Dir(p), vars, depth+1, seen)
		vars = oracleAfter(vars, o.read(content, vars, nil))
	}
	return vars
}

// oracleAfter is vars with what the file read as b leaves known.
func oracleAfter(vars map[string]string, b *gradleBuild) map[string]string {
	own, gone := b.result()
	return oracleWith(vars, own, gone)
}

// withFile is vars with what the build or settings file at p assigns, read
// with the scripts it applies known.
func (o *oracleGradle) withFile(p, dir string, s oracleScope, libs map[string][]gradleDecl) oracleScope {
	content, ok := o.file(p)
	if !ok {
		return s
	}
	pre := o.read(content, s.vars, nil)
	vars := o.applied(pre.applies, dir, s.vars, 0, map[string]bool{p: true})
	s.vars = oracleAfter(vars, o.read(content, vars, libs))
	return s
}

// at is what is known at dir.
func (o *oracleGradle) at(dir string) oracleScope {
	dir = filepath.Clean(dir)
	s := oracleScope{vars: map[string]string{}, libs: map[string][]gradleDecl{}}
	if !o.within(dir) {
		return s
	}
	if dir != o.root {
		up := filepath.Dir(dir)
		s = o.at(up).copy()
		aboveLibs := s.libs
		for _, name := range []string{"build.gradle", "build.gradle.kts"} {
			s = o.withFile(filepath.Join(up, name), up, s, aboveLibs)
		}
	}
	// buildSrc: its sources, twice.
	var src []string
	_ = filepath.Walk(filepath.Join(dir, "buildSrc", "src"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			switch filepath.Ext(p) {
			case ".kt", ".kts", ".groovy", ".java", ".gradle":
				src = append(src, p)
			}
		}
		return nil
	})
	sort.Strings(src)
	for round := 0; round < 2; round++ {
		for _, p := range src {
			if content, ok := o.file(p); ok {
				s.vars = oracleAfter(s.vars, o.read(content, s.vars, nil))
			}
		}
	}
	if content, ok := o.file(filepath.Join(dir, "gradle.properties")); ok {
		s.vars = oracleWith(s.vars, gradleProperties(content), nil)
	}
	for _, name := range []string{"settings.gradle", "settings.gradle.kts"} {
		s = o.withFile(filepath.Join(dir, name), dir, s, nil)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "gradle"))
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".versions.toml")
		if !ok || name == "" {
			continue
		}
		if content, ok := o.file(filepath.Join(dir, "gradle", e.Name())); ok {
			vars, libs := readGradleCatalogue(name, content)
			s.vars = oracleWith(s.vars, vars, nil)
			merged := map[string][]gradleDecl{}
			for k, v := range s.libs {
				merged[k] = v
			}
			for k, v := range libs {
				merged[k] = v
			}
			s.libs = merged
		}
	}
	return s
}

// declared is what the build file at path declares.
func (o *oracleGradle) declared(path string) []model.LibyearDep {
	content, ok := o.file(path)
	if !ok {
		return nil
	}
	dir := filepath.Dir(path)
	s := o.at(dir)
	pre := o.read(content, s.vars, nil)
	vars := o.applied(pre.applies, dir, s.vars, 0, map[string]bool{path: true})
	return o.read(content, vars, s.libs).libyearDeps()
}

// oracleGradleTree makes one random Gradle clone: properties, a settings
// file, catalogues, a buildSrc, scripts that apply one another (and
// themselves, and each other in circles), that assign, re-assign and
// conflict; and build files at several depths that apply them in every way
// an apply statement is written.
func oracleGradleTree(rng *rand.Rand) (files map[string]string, builds []string) {
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	names := []string{"base", "v", "w", "sv", "own", "k"}
	vals := []string{"1.0", "2.0", "3.1", "${base}.1", "$v", "${w}", "$sv-x", "9", "${k}"}
	scripts := []string{"common", "other", "third", "s4", "s5", "s6"}
	assign := func() string {
		n, v := pick(names), pick(vals)
		switch rng.Intn(7) {
		case 0:
			return "ext." + n + " = \"" + v + "\"\n"
		case 1:
			return "def " + n + " = \"" + v + "\"\n"
		case 2:
			return "ext {\n  " + n + " = \"" + v + "\"\n  " + pick(names) + " = '" + pick(vals[:3]) + "'\n}\n"
		case 3:
			return n + " = \"" + v + "\"\n"
		case 4:
			return "subprojects {\n  ext." + n + " = \"" + v + "\"\n}\n"
		case 5:
			return "ext." + n + " = " + pick(names) + "\n"
		default:
			return "buildscript {\n  ext." + n + " = '" + pick(vals[:3]) + "'\n}\n"
		}
	}
	applyLine := func(up string) string {
		s := pick(scripts)
		switch rng.Intn(6) {
		case 0:
			return "apply from: \"$rootDir/gradle/" + s + ".gradle\"\n"
		case 1:
			return "apply from: '" + up + "gradle/" + s + ".gradle'\n"
		case 2:
			return "apply from: rootProject.file('gradle/" + s + ".gradle')\n"
		case 3:
			return "apply from: \"${rootProject.projectDir}/gradle/" + s + ".gradle\"\n"
		case 4:
			return "apply from: '" + s + ".gradle'\n"
		default:
			return "apply(from = \"$rootDir/gradle/" + s + ".gradle\")\n"
		}
	}
	depLines := func() string {
		var sb strings.Builder
		sb.WriteString("dependencies {\n")
		for i, n := range names {
			if rng.Intn(3) != 0 {
				fmt.Fprintf(&sb, "  implementation \"g:d%d:$%s\"\n", i, n)
			}
			if rng.Intn(4) == 0 {
				fmt.Fprintf(&sb, "  api \"g:e%d:${%s}\"\n", i, n)
			}
			if rng.Intn(6) == 0 {
				fmt.Fprintf(&sb, "  implementation group: 'g', name: 'f%d', version: %s\n", i, n)
			}
		}
		if rng.Intn(2) == 0 {
			sb.WriteString("  implementation libs.x\n  implementation libs.y\n  implementation \"g:z:${libs.versions.k.get()}\"\n")
		}
		if rng.Intn(3) == 0 {
			sb.WriteString("  implementation \"g:c:${Versions.x}\"\n  implementation Versions.coord\n")
		}
		sb.WriteString("}\n")
		return sb.String()
	}
	buildFile := func(up string) string {
		var sb strings.Builder
		for i, n := 0, rng.Intn(5); i < n; i++ {
			if rng.Intn(2) == 0 {
				sb.WriteString(assign())
			} else {
				sb.WriteString(applyLine(up))
			}
		}
		sb.WriteString(depLines())
		if rng.Intn(3) == 0 {
			sb.WriteString(assign())
		}
		return sb.String()
	}
	script := func() string {
		var sb strings.Builder
		for i, n := 0, rng.Intn(6); i < n; i++ {
			switch rng.Intn(7) {
			case 0, 1:
				sb.WriteString(applyLine("../"))
			case 2:
				sb.WriteString("dependencies { implementation \"g:s:$" + pick(names) + "\" }\n")
			case 3:
				n := pick(names)
				sb.WriteString("ext." + n + " = '1'\next." + n + " = '2'\n")
			default:
				sb.WriteString(assign())
			}
		}
		return sb.String()
	}
	props := func() string {
		var sb strings.Builder
		for _, n := range names {
			if rng.Intn(3) == 0 {
				sb.WriteString(n + "=" + pick(vals[:3]) + "\n")
			}
		}
		return sb.String()
	}
	cat := func() string {
		return "[versions]\nk = \"" + pick(vals[:3]) + "\"\n[libraries]\nx = { module = \"g:x\", version.ref = \"k\" }\ny = \"g:y:" + pick(vals[:3]) + "\"\n"
	}
	files = map[string]string{}
	maybe := func(p float64, rel, c string) {
		if rng.Float64() < p {
			files[rel] = c
		}
	}
	maybe(0.7, "gradle.properties", props())
	settings := "rootProject.name='r'\n"
	if rng.Intn(3) == 0 {
		settings += assign() + applyLine("")
	}
	maybe(0.6, "settings.gradle", settings)
	maybe(0.8, "build.gradle", buildFile(""))
	maybe(0.5, "gradle/libs.versions.toml", cat())
	files["gradle/common.gradle"] = script()
	for _, s := range scripts[1:] {
		maybe(0.6, "gradle/"+s+".gradle", script())
	}
	if rng.Intn(6) == 0 {
		// A chain of scripts deeper than an apply is followed.
		for i := 0; i < gradleApplyDepth+2; i++ {
			files[fmt.Sprintf("gradle/chain%d.gradle", i)] = fmt.Sprintf("ext.%s = '%d.0'\napply from: 'chain%d.gradle'\n", pick(names), i, i+1)
		}
		files["gradle/common.gradle"] += "apply from: 'chain" + pick([]string{"0", "1", "2", "3"}) + ".gradle'\n"
	}
	maybe(0.4, "buildSrc/src/main/kotlin/Versions.kt", "object Versions {\n  const val x = \""+pick(vals[:3])+"\"\n  const val coord = \"g:bs:${x}\"\n}\n")
	for _, d := range []string{"projA", "projB", "projA/sub", "deep/x/y", "projC", "projD", "projE"} {
		up := strings.Repeat("../", strings.Count(d, "/")+1)
		maybe(0.4, d+"/gradle.properties", props())
		name := "build.gradle"
		if rng.Intn(5) == 0 {
			name = "build.gradle.kts"
		}
		files[d+"/"+name] = buildFile(up)
		builds = append(builds, d+"/"+name)
	}
	maybe(0.25, "projB/gradle/libs.versions.toml", cat())
	maybe(0.15, "projA/settings.gradle", "rootProject.name='nested'\n")
	maybe(0.2, "deep/build.gradle", buildFile("../"))
	for _, rel := range []string{"build.gradle", "deep/build.gradle"} {
		if _, ok := files[rel]; ok {
			builds = append(builds, rel)
		}
	}
	sort.Strings(builds)
	return files, builds
}

// TestTheGradleTreeAnswersAsTheNaiveReaderDoes compares the Gradle tree's
// reader with the reference: every declaration of every build file, in
// several read orders.
func TestTheGradleTreeAnswersAsTheNaiveReaderDoes(t *testing.T) {
	rng := rand.New(rand.NewSource(20261121))
	root := t.TempDir()
	reads, versions, unread, reused := 0, 0, 0, 0
	for it := 0; it < oracleGradleTrees; it++ {
		files, builds := oracleGradleTree(rng)
		oracleWrite(t, root, files)
		o := &oracleGradle{root: root, paths: newGradleTree(root, nil)}
		want := map[string]string{}
		for _, rel := range builds {
			deps := notPackages(o.declared(filepath.Join(root, filepath.FromSlash(rel))))
			for _, d := range deps {
				if d.Version != "" {
					versions++
				} else {
					unread++
				}
			}
			want[rel] = oracleJSON(deps)
		}
		for order := 0; order < oracleOrders; order++ {
			seq := append([]string(nil), builds...)
			if order > 0 {
				rng.Shuffle(len(seq), func(i, j int) { seq[i], seq[j] = seq[j], seq[i] })
			}
			w := NewWalk(root, nil)
			for _, rel := range seq {
				got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
				reads++
				if got.Failure != nil {
					t.Fatalf("tree %d order %v: %s was refused: %v\n%s", it, seq, rel, got.Failure, oracleDump(files))
				}
				if g := oracleJSON(got.Deps); g != want[rel] {
					t.Fatalf("tree %d, read order %v: %s\n the tree reader gives %s\n the naive reader gives %s\n%s", it, seq, rel, g, want[rel], oracleDump(files))
				}
			}
			for _, runs := range w.gradle.bundles {
				reused += len(runs)
			}
		}
	}
	t.Logf("%d trees: %d readings compared; the reference read %d versions and left %d unread; %d runs of applied scripts were kept for reuse", oracleGradleTrees, reads, versions, unread, reused)
	if versions < oracleGradleTrees || unread < oracleGradleTrees || reused < oracleGradleTrees {
		t.Errorf("the random trees gave %d versions, %d declarations with none and %d kept runs over %d trees: too few to compare anything", versions, unread, reused, oracleGradleTrees)
	}
}
