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

// Round 8: WHAT THE MAVEN REFERENCE BELIEVES. Through round 7 the reference
// was written from the reader, and four of round 8's findings were beliefs
// the two shared. The rules below are Maven's, written from its
// documentation (the POM reference, "Introduction to the POM", "Introduction
// to the Dependency Mechanism", the model builder's description of how an
// effective model is assembled) AS REMEMBERED: neither Maven nor Gradle can
// be run where this suite runs, and nothing here has been checked against a
// running build. Each rule is marked VERIFIED-BY-DOCS-FROM-MEMORY (the
// documentation states it, as recalled) or UNSURE (inferred, or recalled
// with doubt).
//
//	M1  VERIFIED-BY-DOCS-FROM-MEMORY. A pom's effective model is the pom over
//	    its parents: a child's property hides a parent's, and every
//	    reference is interpolated in the model of the pom being built, so a
//	    parent's managed entry means for a child what the child's properties
//	    make it mean.
//	M2  VERIFIED-BY-DOCS-FROM-MEMORY. A parent is looked for at relativePath
//	    (default ../pom.xml; a directory means the pom.xml in it) and the
//	    file there is the parent only when its coordinates are the ones
//	    asked for. An empty <relativePath/> skips that place.
//	M3  VERIFIED-BY-DOCS-FROM-MEMORY for projects of one multi-module build;
//	    UNSURE beyond it. A parent or an imported BOM that is a project of
//	    the build being run is taken from the build (the reactor), before
//	    any repository, wherever its file is. The reference takes "the
//	    build" to be every pom of the clone; Maven's is the modules reached
//	    from the pom it was started on (DISAGREEMENT D1, below).
//	M4  VERIFIED-BY-DOCS-FROM-MEMORY. Otherwise the pom is asked of the
//	    repositories the build declares, and Central. A snapshot, a range,
//	    LATEST and RELEASE do not name one pom.
//	M5  VERIFIED-BY-DOCS-FROM-MEMORY. A pom that is needed and cannot be read
//	    (not well-formed, not obtainable) fails the build: nothing behind it
//	    is known. So does a cycle of parents. Maven bounds neither the
//	    length of a chain of parents nor the depth of imports. UNSURE (from
//	    the model builder's error text as recalled, not from a manual): BOMs
//	    that import each other in a cycle fail the build too. A BOM reached
//	    by two routes is no cycle.
//	M6  VERIFIED-BY-DOCS-FROM-MEMORY. A version for a dependency declared
//	    with none is the dependencyManagement entry of the effective model:
//	    the nearest pom of the chain that declares one wins. UNSURE: that
//	    every entry written anywhere in the chain comes before every import
//	    (the model builder inherits first and imports afterwards, as
//	    recalled).
//	M7  VERIFIED-BY-DOCS-FROM-MEMORY. An import (scope import, type pom) adds
//	    the entries of the BOM's effective model that the importing model
//	    does not already have; among imports the first declared wins; a BOM's
//	    own imports are followed the same way. UNSURE: the order between the
//	    imports of a child and those of its parent (the child's first here).
//	M8  VERIFIED-BY-DOCS-FROM-MEMORY. A profile's dependencyManagement and
//	    dependencies are merged over the pom's own when the profile is
//	    active, before a child inherits. activeByDefault holds only while no
//	    other profile of the pom is activated, and the other activations
//	    (property, JDK, OS, file, the command line) are not knowable from the
//	    files. So what a profile manages, or declares with a version, is NOT
//	    KNOWN from that pom outward; an entry in a NEARER pom still wins.
//	M9  VERIFIED-BY-DOCS-FROM-MEMORY. A repository whose address is http is
//	    refused by Maven 3.8.1 and later unless mirrored; one that needs
//	    credentials, or is a file path, is not reachable from here. While
//	    the build declares one, "not found" elsewhere does not say the pom
//	    does not exist.
//	M10 VERIFIED-BY-DOCS-FROM-MEMORY. A pom is read in the encoding its XML
//	    declaration names, and XML 1.1 is read as 1.0 is.
//
// What the reference does beyond Maven is the reader's documented policy,
// and is the same rule everywhere: what is not known is not guessed. Two
// poms of the clone with one set of coordinates are not chosen between
// (SR-6); a chain or a nesting longer than 32 is not followed and is not
// known; a pom that could not be read may be any pom its legible part does
// not rule out.
//
// DISAGREEMENTS between this reference and the reader that remain: none in
// the trees generated. Between BOTH and Maven, known and not closed:
//
//	D1  (M3) a pom of the clone that is not a module of the build Maven was
//	    started on is not in Maven's reactor; both take it by coordinates.
//	    The clone does not say which pom a build starts on.
//	D2  (M8) a profile that is active changes more than managed versions
//	    (properties, modules, repositories); only what it manages or
//	    declares is treated as unknown. A profile's property that a managed
//	    version refers to is read as if the profile were inactive.
//	D3  (M8) a fetched pom carries no profiles (RemotePom has none): a
//	    profile of a parent or BOM that came from a repository is not seen.
//	D4  (M9) a mirror or a settings.xml of the machine is not known; the
//	    repositories asked are the declared https ones and Central.
//	D5  (M10) of the single-byte encodings the reference reads ISO-8859-1
//	    and US-ASCII only; the reader also reads Windows-1252, pinned by its
//	    own test (TestAPomIsReadInTheEncodingItDeclares).

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
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
	// pathOnly: a pom's own coordinates are worked out from the parents its
	// relativePath names and from nothing else (byCoordinates asks for every
	// pom's coordinates, and must not ask itself).
	pathOnly bool
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
	if err != nil || int64(len(data)) > MaxBuildFileBytes {
		return nil
	}
	return oracleDecode(string(data), path)
}

// unreadable reports a file of the clone that is there and gives no pom
// (M5).
func (o *oraclePoms) unreadable(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && o.model(path) == nil
}

// couldBe reports whether a pom of the clone that could not be read may be
// the one with these coordinates. What is legible is the text before the
// first block that holds other projects' coordinates, outside <parent>: a
// coordinate found there that is different rules the pom out, and one that
// is not found rules nothing out.
func (o *oraclePoms) couldBe(g, a, v string) bool {
	for _, p := range o.allPoms() {
		if !o.unreadable(p) {
			continue
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return true
		}
		if len(data) > 64<<10 {
			data = data[:64<<10]
		}
		text := string(data)
		for _, block := range []string{"<dependencies", "<dependencyManagement", "<build", "<profiles", "<reporting"} {
			if i := strings.Index(text, block); i >= 0 {
				text = text[:i]
			}
		}
		if i := strings.Index(text, "<parent>"); i >= 0 {
			if j := strings.Index(text[i:], "</parent>"); j >= 0 {
				text = text[:i] + text[i+j+len("</parent>"):]
			} else {
				text = text[:i]
			}
		}
		agrees := func(tag, want string) bool {
			i := strings.Index(text, "<"+tag+">")
			if i < 0 {
				return true
			}
			rest := text[i+len(tag)+2:]
			// The element's text ends where the next tag begins, whether
			// or not the file goes on to close it.
			j := strings.Index(rest, "<")
			if j < 0 {
				return true
			}
			has := strings.TrimSpace(rest[:j])
			return has == "" || strings.Contains(has, "${") || has == want
		}
		if agrees("artifactId", a) && agrees("groupId", g) && agrees("version", v) {
			return true
		}
	}
	return false
}

// obtain is the pom with these coordinates for a build whose poms read so
// far are chain: the one pom of the clone that has them (M3; two are not
// chosen between), else a repository's (M4), and what "not found" there is
// worth (M5: an unreadable pom of the clone may be it; M9: a declared
// repository was never asked).
func (o *oraclePoms) obtain(g, a, v string, chain []*oraclePom) (pom *oraclePom, reason, unknown string) {
	const notFound, failed = "inherited_not_found", "inherited_lookup_failed"
	if o.pathOnly {
		return o.remote(g, a, v) // no source: not known
	}
	in, ambiguous := o.byCoordinates(g, a, v)
	if ambiguous {
		return nil, notFound, notFound
	}
	if in != nil {
		return in, "", ""
	}
	pom, reason, unknown = o.remote(g, a, v)
	if pom != nil || unknown != "" {
		return pom, reason, unknown
	}
	if o.couldBe(g, a, v) {
		return nil, failed, failed
	}
	if o.src != nil {
		for _, m := range chain {
			if m.unasked {
				return nil, notFound, notFound
			}
		}
	}
	return nil, reason, ""
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
	if g == "" || a == "" || v == "" || strings.Contains(g+a+v, "${") || strings.Contains(strings.ToUpper(v), "SNAPSHOT") || strings.ContainsAny(v, "[](),") ||
		v == "LATEST" || v == "RELEASE" {
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
			// Longer than is followed, or a cycle (M5): not known.
			c.gap, c.unknown, c.whole = "inherited_not_found", "inherited_not_found", false
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
			} else if cand == nil && o.unreadable(at) {
				// M5: the file the child names is there and cannot be read.
				c.gap, c.unknown, c.whole = "inherited_lookup_failed", "inherited_lookup_failed", false
				break
			}
		}
		if p == nil {
			r, reason, unknown := o.obtain(cur.parent.group, cur.parent.artifact, cur.parent.version, c.chain)
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
	treeOnly := &oraclePoms{root: o.root, decoded: o.decoded, pathOnly: true}
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
	within   map[string]bool // the BOMs the question is inside of
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
	// M8: from the first pom whose profiles manage the artifact outward,
	// the answer is not known to be the build's.
	profiled := false
	doubt := func() {
		if profiled {
			q.gap = oracleWorse(q.gap, "profile_managed")
			q.before = oracleWorse(q.before, "profile_managed")
		}
	}
	for _, m := range c.chain {
		if m.profileAny || m.profileNames[group+":"+artifact] {
			profiled = true
		}
		for _, e := range m.managed {
			if e.isImport {
				continue
			}
			if c.interp(e.group) == group && c.interp(e.artifact) == artifact {
				doubt()
				v := c.interp(e.version)
				if v != "" && !strings.Contains(v, "${") {
					return v, true, nil, false
				}
				return v, false, nil, true
			}
		}
	}
	doubt()
	// The chain's missing end comes before every import.
	q.before = oracleWorse(q.before, c.unknown)
	if depth >= oracleChainLimit {
		// Nested deeper than is followed: what this BOM imports is not known.
		for _, m := range c.chain {
			for _, e := range m.managed {
				if e.isImport {
					q.gap = oracleWorse(q.gap, "inherited_not_found")
					q.before = oracleWorse(q.before, "inherited_not_found")
					return "", false, nil, false
				}
			}
		}
		return "", false, nil, false
	}
	// Then the BOMs the chain imports, in order.
	for _, m := range c.chain {
		for _, e := range m.managed {
			if !e.isImport {
				continue
			}
			bg, ba, bv := c.interp(e.group), c.interp(e.artifact), c.interp(e.version)
			bom, reason, unknown := c.o.obtain(bg, ba, bv, c.chain)
			if bom == nil {
				q.gap = oracleWorse(q.gap, reason)
				q.before = oracleWorse(q.before, unknown)
				continue
			}
			if q.within[bom.path] {
				// M5: a BOM inside itself. Maven does not build it; not known.
				q.gap = oracleWorse(q.gap, "inherited_not_found")
				q.before = oracleWorse(q.before, "inherited_not_found")
				continue
			}
			if q.searched[bom.path] {
				continue // a second route to it: it answered already
			}
			q.searched[bom.path] = true
			bc := c.o.ctx(bom)
			q.gap = oracleWorse(q.gap, bc.gap)
			if q.within == nil {
				q.within = map[string]bool{}
			}
			q.within[bom.path] = true
			v, ok, via, stop := bc.lookup(q, group, artifact, depth+1)
			delete(q.within, bom.path)
			if ok {
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
	// M8: the "group:artifact" a profile manages or declares with a version
	// (profileAny: it cannot be said which, a reference or an import).
	profileNames map[string]bool
	profileAny   bool
	// M9: it declares a repository for dependencies that is never asked.
	unasked bool
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
		Repos    []string       `xml:"repositories>repository>url"`
		Profiles []struct {
			Managed []oracleXMLDep `xml:"dependencyManagement>dependencies>dependency"`
			Deps    []oracleXMLDep `xml:"dependencies>dependency"`
		} `xml:"profiles>profile"`
	}
	// M10: XML 1.1 as 1.0, and the declared encoding (D5: of the single-byte
	// ones, ISO-8859-1 and US-ASCII).
	if end := strings.Index(content, "?>"); strings.HasPrefix(content, "<?xml") && end > 0 {
		content = strings.Replace(strings.Replace(content[:end], `version="1.1"`, `version="1.0"`, 1), `version='1.1'`, `version='1.0'`, 1) + content[end:]
	}
	dec := xml.NewDecoder(bytes.NewReader([]byte(content)))
	dec.CharsetReader = func(label string, in io.Reader) (io.Reader, error) {
		switch strings.ToLower(label) {
		case "iso-8859-1", "latin1", "us-ascii", "ascii":
			raw, err := io.ReadAll(in)
			if err != nil {
				return nil, err
			}
			runes := make([]rune, len(raw))
			for i, b := range raw {
				runes[i] = rune(b)
			}
			return strings.NewReader(string(runes)), nil
		}
		return nil, fmt.Errorf("encoding %q is not read", label)
	}
	if err := dec.Decode(&x); err != nil {
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
	for _, raw := range x.Repos {
		// M9: asked only when it is an https address with a host and no
		// credentials; anything else that is written is a repository the
		// build would ask and this reader does not.
		raw = t(raw)
		https := strings.HasPrefix(raw, "https://") && !strings.Contains(raw, "${") && !strings.Contains(strings.SplitN(raw[len("https://"):], "/", 2)[0], "@") &&
			strings.SplitN(raw[len("https://"):], "/", 2)[0] != ""
		if raw != "" && !https {
			m.unasked = true
		}
	}
	m.profileNames = map[string]bool{}
	for _, pr := range x.Profiles {
		if len(pr.Managed) > 0 {
			m.profileManagement = true
		}
		name := func(d oracleXMLDep) {
			if strings.Contains(d.G+d.A, "${") {
				m.profileAny = true
			}
			m.profileNames[t(d.G)+":"+t(d.A)] = true
		}
		for _, d := range pr.Managed {
			if t(d.Scope) == "import" && t(d.Type) == "pom" {
				m.profileAny = true
				continue
			}
			name(d)
		}
		for _, d := range pr.Deps {
			if t(d.V) != "" {
				name(d)
			}
			// What a profile declares is declared by the pom: a row of its
			// own, whether or not the profile is active (the inventory is of
			// what the files declare).
			m.deps = append(m.deps, oracleManaged{group: t(d.G), artifact: t(d.A), version: t(d.V)})
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
	rank := map[string]int{"inherited_lookup_failed": 4, "inherited_repository_refused": 3, "inherited_not_found": 2, "profile_managed": 1}
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
	// Whole, and how many BOMs deep its imports go (itself counted), are
	// the BOM's own, whatever depth it is met at; a BOM inside itself is a
	// cycle, and no BOM of a cycle is whole (M5). Nothing is remembered.
	var bomFacts func(b *oraclePom, inside map[string]bool) (whole bool, height int)
	bomFacts = func(b *oraclePom, inside map[string]bool) (bool, int) {
		if inside[b.path] {
			return false, 0
		}
		inside[b.path] = true
		defer delete(inside, b.path)
		bc := o.ctx(b)
		whole, height := bc.whole, 1
		for _, cm := range bc.chain {
			for _, e := range cm.managed {
				if !e.isImport {
					continue
				}
				next, _, _ := o.obtain(bc.interp(e.group), bc.interp(e.artifact), bc.interp(e.version), bc.chain)
				if next == nil {
					whole = false
					continue
				}
				w, h := bomFacts(next, inside)
				whole = whole && w
				if h+1 > height {
					height = h + 1
				}
			}
		}
		return whole, height
	}
	bomWhole := func(b *oraclePom, _ int) bool {
		whole, height := bomFacts(b, map[string]bool{})
		return whole && height <= oracleChainLimit
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
					r, _, _ := o.obtain(bg, ba, bv, c.chain)
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
		// Round 8 (M8): profiles that manage, declare with a version, or
		// import, active by default or not.
		if rng.Intn(8) == 0 {
			sb.WriteString("<profiles><profile><id>p</id>" + pick([]string{"", "<activation><activeByDefault>true</activeByDefault></activation>"}))
			switch rng.Intn(4) {
			case 0:
				sb.WriteString("<dependencyManagement><dependencies>" + r4dep(pick(groups), pick(arts), "5") + "</dependencies></dependencyManagement>")
			case 1:
				sb.WriteString("<dependencyManagement><dependencies>" + r4import("t", pick(bomNames), "1") + "</dependencies></dependencyManagement>")
			case 2:
				sb.WriteString("<dependencies>" + r4dep("g1", pick([]string{"a1", "a2"}), pick([]string{"5", ""})) + "</dependencies>")
			default:
				sb.WriteString("<dependencyManagement><dependencies>" + r4dep("g1", "a1", "5") + "</dependencies></dependencyManagement>")
			}
			sb.WriteString("</profile></profiles>")
		}
		return props, managed, sb.String()
	}
	gen := func(artifact, parent, ver, modules string) string {
		var sb strings.Builder
		// Round 8 (M10): a declaration of XML 1.1, or of an encoding with a
		// byte that is not UTF-8.
		accent := ""
		switch rng.Intn(20) {
		case 0:
			sb.WriteString(`<?xml version="1.1" encoding="UTF-8"?>`)
		case 1:
			sb.WriteString(`<?xml version="1.0" encoding="ISO-8859-1"?>`)
			accent = "<name>caf\xe9</name>"
		}
		sb.WriteString("<project>" + accent)
		switch parent {
		case "":
		case "reactor":
			// Round 8 (M3): a parent that is a pom of the clone and is not
			// at the place named (or none is named).
			sb.WriteString("<parent><groupId>t</groupId><artifactId>" + pick([]string{"root", "mid", "snapp", "dup", "fix"}) + "</artifactId><version>" + pick([]string{"1", "1", "1-SNAPSHOT"}) + "</version>" +
				pick([]string{"<relativePath/>", "<relativePath>../nowhere</relativePath>", ""}) + "</parent>")
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
		// Round 8 (M9): a repository the build declares, asked or not.
		if rng.Intn(10) == 0 {
			sb.WriteString("<repositories><repository><url>" + pick([]string{"http://nexus.corp.example/repo", "https://user:secret@nexus.corp.example/repo",
				"https://repo.corp.example/maven", "file:///opt/repo", "${repo.url}"}) + "</url></repository></repositories>")
		}
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
		"app3/pom.xml": gen("app3", pick([]string{"", "ext", "extgone", "mid", "reactor", "reactor", "fix"}), "1", ""),
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
		files["svc/snapp/pom.xml"] = gen("snapp", pick([]string{"", "root"}), "1-SNAPSHOT", "")
		files["svc/m/pom.xml"] = gen("m", "reactor", "1", "")
	}
	// Round 8 (M5): a chain of parents, and BOMs nested in BOMs, about as
	// long as is followed: 31 to 34, the far end managing what nothing
	// nearer does.
	if rng.Intn(25) == 0 {
		n := 31 + rng.Intn(4)
		link := func(dir string, k int, inner string) string {
			return "<project><groupId>t</groupId><artifactId>" + fmt.Sprintf("%s%d", dir, k) + "</artifactId><version>1</version>" + inner + "</project>"
		}
		far := "<dependencyManagement><dependencies>" + r4dep("g1", "far", "4.4") + "</dependencies></dependencyManagement>"
		deps := "<dependencies>" + r4dep("g1", "far", "") + r4dep("g1", "a1", "") + "</dependencies>"
		if rng.Intn(2) == 0 {
			// k0 is the application; k(n-1) the last parent.
			for k := 0; k < n; k++ {
				inner := far
				if k < n-1 {
					inner = fmt.Sprintf("<parent><groupId>t</groupId><artifactId>long%d</artifactId><version>1</version><relativePath>../%d</relativePath></parent>", k+1, k+1)
				}
				if k == 0 {
					inner += "<dependencyManagement><dependencies>" + r4import("t", "bomA", "1") + "</dependencies></dependencyManagement>" + deps
				}
				files[fmt.Sprintf("long/%d/pom.xml", k)] = link("long", k, inner)
			}
		} else {
			for k := 1; k <= n; k++ {
				inner := far
				if k < n {
					inner = "<dependencyManagement><dependencies>" + r4import("t", fmt.Sprintf("nest%d", k+1), "1") + "</dependencies></dependencyManagement>"
				}
				files[fmt.Sprintf("nest/%d/pom.xml", k)] = link("nest", k, inner)
			}
			files["nest/0/pom.xml"] = link("nest", 0, "<dependencyManagement><dependencies>"+r4import("t", "nest1", "1")+r4import("t", "bomA", "1")+"</dependencies></dependencyManagement>"+deps)
		}
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
//
// The counts are in race_off_test.go and race_on_test.go: under the race
// detector, where each tree costs several times as much, a fifth to a
// quarter of them are compared (the detector is there to find races in the
// readers, which the first hundred trees exercise as the rest do); the run
// without it compares them all.
const oracleOrders = 3

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
//
// Round 8, rule S1 (VERIFIED-BY-DOCS-FROM-MEMORY: the Gradle user manual on
// script plugins; neither tool can be run here): a build file is run from
// the top, and "apply from:" runs the named script at that point. So of a
// variable both assign, the script's value stands when the file assigned it
// BEFORE its first apply, the file's when it assigned it AFTER its last, and
// between two applies it is whichever came later, which is not worked out:
// not known unless both give one value. assigned collects what the scripts
// assign (nil: left with two values), in the order they run.
//
// S2 (the reader's limits, shared here and reported): a script does not see
// what the file that applies it assigned before the apply, and a script
// applied by a script is read before all of it, wherever it is applied.
func (o *oracleGradle) applied(applies []string, dir string, vars map[string]string, depth int, seen map[string]bool, assigned map[string]*string) map[string]string {
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
		vars = o.applied(first.applies, filepath.Dir(p), vars, depth+1, seen, assigned)
		script := o.read(content, vars, nil)
		own, gone := script.result()
		for name, v := range own {
			v := v
			assigned[name] = &v
		}
		for name := range gone {
			assigned[name] = nil
		}
		vars = oracleAfter(vars, script)
	}
	return vars
}

// readOrdered reads a file after its scripts: its assignments, rule S1 over
// them, then its declarations (the file reader's two passes, which are
// shared, with the reference's rule between them).
func (o *oracleGradle) readOrdered(content string, vars map[string]string, libs map[string][]gradleDecl, assigned map[string]*string) *gradleBuild {
	var l *gradleLibs
	if len(libs) > 0 {
		l = &gradleLibs{own: libs}
	}
	toks := gradleTokens(content)
	b := &gradleBuild{outer: (*gradleVars)(nil).push(oracleWith(vars, nil, nil), nil), own: map[string]string{}, conflicted: map[string]bool{},
		libs: l, reads: map[string]gradleRead{}, setAt: map[string]int{}}
	b.walk(toks, false)
	oracleOrder(b, assigned)
	b.walk(toks, true)
	b.springBootBOM()
	return b
}

// oracleOrder is rule S1 over the assignments of one file read as b.
func oracleOrder(b *gradleBuild, assigned map[string]*string) *gradleBuild {
	if b.firstApply == 0 {
		return b
	}
	for name, at := range b.setAt {
		script, by := assigned[name]
		switch {
		case !by, at > b.lastApply:
		case at < b.firstApply && script != nil:
			b.own[name] = *script
			delete(b.conflicted, name)
		case at < b.firstApply:
			b.conflicted[name] = true
		default:
			if own, has := b.own[name]; script == nil || !has || own != *script {
				b.conflicted[name] = true
			}
		}
	}
	return b
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
	assigned := map[string]*string{}
	vars := o.applied(pre.applies, dir, s.vars, 0, map[string]bool{p: true}, assigned)
	s.vars = oracleAfter(vars, o.readOrdered(content, vars, libs, assigned))
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
	assigned := map[string]*string{}
	vars := o.applied(pre.applies, dir, s.vars, 0, map[string]bool{path: true}, assigned)
	return o.readOrdered(content, vars, s.libs, assigned).libyearDeps()
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
