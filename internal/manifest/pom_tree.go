// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// SourceManaged is the version source of a dependency whose pom gives it no
// version and whose version comes from dependencyManagement: in the same
// pom, a parent, or an imported BOM (repo_deps_libyear.version_source).
const SourceManaged = model.VersionSourceManaged

// pomTree reads a clone's Maven poms as Maven does: as a tree (v0.29.178;
// summary/70, 4.4). A dependency with no <version> takes it from
// dependencyManagement, in its own pom, then its parent chain, then the BOMs
// those import, with properties interpolated across the chain.
//
// It follows the Cargo workspace index's rules: one index per walk; nothing
// is read above the clone's root; Lstat before every read, and a symlink is
// refused. And SR-6: a parent is taken only when the pom at its place has
// the coordinates the child names; a BOM, which has coordinates and no
// place, only when exactly one pom in the tree has them.
type pomTree struct {
	root   string
	logger *slog.Logger
	// models caches each pom read, by cleaned absolute path; a nil entry
	// is a pom that could not be read or parsed.
	models map[string]*pomModel
	// byCoord maps "group:artifact:version" to the poms in the tree with
	// those coordinates; built once, on the first BOM import.
	byCoord  map[string][]string
	pomFiles []string
	// remote answers for a parent or BOM that is not in the tree (0.29.181);
	// nil: nothing is asked, and nothing is said about what was not asked.
	remote PomSource
	// remoteAnswers caches each coordinate asked, for the walk.
	remoteAnswers map[string]remoteAnswer
	// contexts caches the effective model of each pom (context): its chain
	// and its own coordinates, nothing that grows with what the chain
	// manages.
	contexts map[*pomModel]*pomContext
	// importSteps counts every import a question passed over or looked at,
	// and asked the questions put to the source, for tests (a count where a
	// clock cannot tell: passing a thousand dead imports costs microseconds).
	importSteps int
	asked       int
	// remoteLimit is how many coordinates this walk may ask the source for
	// (SetRemotePomLimit); 0: no limit of its own. notAsked counts the
	// questions past it.
	remoteLimit int
	notAsked    int
}

// What bounds the reading of a build tree (0.29.213).
//
// Rounds 2 to 6 of the review each found a path through these readers whose
// time or memory was a product of what several files say, and each was met
// with an allowance counted in the code: units of work per byte read, bytes
// kept per byte read, requests per byte read, a wall-clock backstop. Round 6
// found five more uncounted paths, and found the allowances themselves
// refusing an honest build and giving a wrong version. They are gone. The
// bound on a walk's time and memory is the operating system's: the collector
// runs the whole manifest pass in a child process with a limit on its CPU
// time and on its heap, and a repository that passes either has its analysis
// stopped and reported by name (internal/collector, manifest_child.go).
//
// What stays here is what makes an HONEST large build cheap, as plain code,
// and what is about correctness:
//
//   - what a pom says is indexed once, on its model (pomIndex), and an
//     effective model (pomContext) is a view over its chain of models;
//   - nothing kept for the walk is keyed by a pair (a file, and something
//     another file says): what depends on the file asking is kept for that
//     file's reading only (pomReading.resolved);
//   - a file larger than a build file is, is not read (MaxBuildFileBytes); a
//     chain of parents or imports ends at pomChainLimit, which is what ends a
//     cycle; an interpolation ends at pomInterpolatedLimit.

// pomReading is the reading of ONE file by the pom tree: what its questions
// worked out that depends on the model asking.
type pomReading struct {
	// resolved are the imports whose coordinates hold a reference, as this
	// file's questions worked them out: what each means in the model that
	// asked, and whether it led nowhere. Kept for the reading of this one
	// file: the answer depends on the model asking, so across files it is a
	// product (round 4: the tree kept such a key per pom and import, 330 MB
	// for 92 KB of poms).
	resolved map[pomImportAt]*pomResolvedImport
	// refKeys are lists of reference-keyed managed entries, as this file's
	// questions worked their keys out: for one list of one pom read in one
	// model, the first entry that means each coordinate there. Kept for
	// this file's reading for the reason resolved is.
	refKeys map[pomRefListAt]map[string]int
}

// pomRefListAt names one list of a pom's reference-keyed entries (pomIndex:
// those filed under an artifact, under a group, with a reference in both
// halves, or all of them) as one effective model reads it.
type pomRefListAt struct {
	c    *pomContext
	m    *pomModel
	kind byte // 'a', 'g', 'b', '*'
	half string
}

// pomRefListSmall: a list this short is compared entry by entry; a longer
// one is worked out once for the file being read (firstRef).
const pomRefListSmall = 8

// firstRef is the first entry of list (reference-keyed entries of m, in the
// order written) whose coordinates mean key in c.
//
// A long list is worked out ONCE for the file being read: what each entry's
// key is there, and the first entry for each key. Round 7: every question
// interpolated every such entry again, so a pom's version-less dependencies
// times the entries its chain keys by reference was a product (621 KB of
// poms in 22 s).
func (c *pomContext) firstRef(rd *pomReading, m *pomModel, list []int, at pomRefListAt, key string) (int, bool) {
	means := func(j int) string {
		return c.interpolate(m.managed[j].group) + ":" + c.interpolate(m.managed[j].artifact)
	}
	if rd == nil || len(list) <= pomRefListSmall {
		for _, j := range list {
			if means(j) == key {
				return j, true
			}
		}
		return 0, false
	}
	first, ok := rd.refKeys[at]
	if !ok {
		first = make(map[string]int, len(list))
		for _, j := range list {
			if k := means(j); !mapHas(first, k) {
				first[k] = j
			}
		}
		if rd.refKeys == nil {
			rd.refKeys = map[pomRefListAt]map[string]int{}
		}
		rd.refKeys[at] = first
	}
	j, found := first[key]
	return j, found
}

func mapHas(m map[string]int, k string) bool { _, ok := m[k]; return ok }

// pomImportAt names one import of one pom as one effective model reads it.
type pomImportAt struct {
	c *pomContext
	m *pomModel
	n int
}

// pomResolvedImport is what such an import's coordinates are there, and,
// once it has been looked for and is no pom this tree can read, why.
type pomResolvedImport struct {
	group, artifact, version string
	dead                     bool
	reason, unknown          string
}

// importAt is what import n of m is in c, worked out once for this file.
func (r *pomReading) importAt(c *pomContext, m *pomModel, n int) *pomResolvedImport {
	at := pomImportAt{c, m, n}
	if got, ok := r.resolved[at]; ok {
		return got
	}
	e := m.managed[m.index().imports[n]]
	got := &pomResolvedImport{group: c.interpolate(e.group), artifact: c.interpolate(e.artifact), version: c.interpolate(e.version)}
	if r.resolved == nil {
		r.resolved = map[pomImportAt]*pomResolvedImport{}
	}
	r.resolved[at] = got
	return got
}

type remoteAnswer struct {
	model   *pomModel
	outcome RemoteOutcome
}

// PomSource obtains a pom that is not in the repository being read, by its
// coordinates, from a Maven repository. The collector supplies one; this
// package does no network I/O.
//
// declared are the repositories the requesting pom's own tree declares and
// that may be fetched from (https, no credentials, readable), the child's
// first. Whether they are asked, and after which configured repositories,
// is the source's decision.
type PomSource interface {
	RemotePom(group, artifact, version string, declared []string) (*RemotePom, RemoteOutcome)
}

// RemoteOutcome is what came of asking for one pom.
type RemoteOutcome int

const (
	// RemoteFound: the pom was obtained.
	RemoteFound RemoteOutcome = iota
	// RemoteNotFound: every repository asked answered that it has none.
	// That is an answer. (Until 0.29.214 a source also gave it for a
	// coordinate it would not ask for; that is RemoteNotAsked now.)
	RemoteNotFound
	// RemoteFailed: a repository did not answer, and nothing is stored for
	// the coordinate. Not an answer (SR-5).
	RemoteFailed
	// RemoteRefused: the only repository that could have it may not be
	// fetched from.
	RemoteRefused
	// RemoteNotAsked (0.29.214): no repository was asked, because the
	// coordinate is not one a request can be built from. What such a pom
	// says is NOT KNOWN, which "not found" (every repository answered) is
	// not: a version that a pom consulted after it would give is withheld
	// (pomContext.managed). It is reported as "not found" is
	// (inherited_not_found, whose meaning has always included "was never
	// asked for").
	RemoteNotAsked
)

// RemotePom is what a pom contributes to a child's effective model, as
// extracted and stored: coordinates, its parent, its properties and its
// dependencyManagement. A stored result is this, not the document.
type RemotePom struct {
	Group      string            `json:"group"`
	Artifact   string            `json:"artifact"`
	Version    string            `json:"version"`
	Parent     *RemotePomParent  `json:"parent,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
	Managed    []RemoteManaged   `json:"managed,omitempty"`
	// ServedBy is the host of the repository that served the pom
	// (0.29.185). Set by the source for each answer; it is not part of the
	// stored extract, which is kept per repository already.
	ServedBy string `json:"-"`
}

// RemotePomParent names a remote pom's own parent.
type RemotePomParent struct {
	Group    string `json:"group"`
	Artifact string `json:"artifact"`
	Version  string `json:"version"`
}

// RemoteManaged is one dependencyManagement entry; Import marks a BOM
// import.
type RemoteManaged struct {
	Group    string `json:"group"`
	Artifact string `json:"artifact"`
	Version  string `json:"version"`
	Import   bool   `json:"import,omitempty"`
}

// ParseRemotePom extracts a fetched pom.
func ParseRemotePom(content string) (*RemotePom, error) {
	m, err := decodePom(content, "")
	if err != nil {
		return nil, err
	}
	out := &RemotePom{Group: m.group, Artifact: m.artifact, Version: m.version, Properties: m.props}
	if m.parent != nil {
		out.Parent = &RemotePomParent{Group: m.parent.group, Artifact: m.parent.artifact, Version: m.parent.version}
	}
	for _, e := range m.managed {
		out.Managed = append(out.Managed, RemoteManaged{Group: e.group, Artifact: e.artifact, Version: e.version, Import: e.isImport})
	}
	return out, nil
}

// modelOf is a remote pom as the tree reads one. Its parent is remote too:
// a fetched pom has no place in this clone.
func (r *RemotePom) modelOf(key string) *pomModel {
	m := &pomModel{path: "remote:" + key, group: r.Group, artifact: r.Artifact, version: r.Version, props: r.Properties, servedBy: r.ServedBy}
	if m.props == nil {
		m.props = map[string]string{}
	}
	if r.Parent != nil {
		m.parent = &pomParentRef{group: r.Parent.Group, artifact: r.Parent.Artifact, version: r.Parent.Version}
	}
	for _, e := range r.Managed {
		m.managed = append(m.managed, pomManaged{group: e.Group, artifact: e.Artifact, version: e.Version, isImport: e.Import})
	}
	return m
}

// remotePom asks the source for one coordinate, once per walk. reason is
// why a pom was not obtained; "" when it was, or when nothing was asked
// because no source is installed.
//
// It is the ONE place the source is asked (TestOnlyRemotePomAsksTheSource).
// declared is called only when a request is made. How many coordinates a
// walk may ask for is the caller's to say (SetRemotePomLimit; the collector
// gives its maxRemotePomCoordinates): a request is a network round trip, not
// work of this package, and each answer is kept for the walk, so the limit is
// also what bounds how many are kept. An import whose coordinates hold a
// reference is another coordinate in each module that inherits it, and
// modules times imports of them asked 199,651 times for 194 KB of poms
// (review round 5).
//
// unknown is not "" when what the pom says is NOT KNOWN, and is then the
// reason a version that a pom consulted AFTER it would give is withheld
// with (pomContext.managed). Only an answer of "not found" from every
// repository read makes a pom known to say nothing. A pom that was not
// asked for (no source is installed; a snapshot, a range, a coordinate with
// an unresolved reference; the walk's limit) is not known (round 7: each of
// these was passed over as if it had answered, and the next BOM's version
// was stored).
func (t *pomTree) remotePom(group, artifact, version string, declared func() []string) (m *pomModel, reason, unknown string) {
	if t.remote == nil {
		return nil, "", model.VersionReasonInheritedNotFound
	}
	// A snapshot is whatever was built last, a range is not one pom, and an
	// unresolved reference is not a coordinate: none is asked for.
	if group == "" || artifact == "" || version == "" || strings.Contains(group+artifact+version, "${") ||
		strings.Contains(strings.ToUpper(version), "SNAPSHOT") || strings.ContainsAny(version, "[](),") {
		return nil, model.VersionReasonInheritedNotFound, model.VersionReasonInheritedNotFound
	}
	key := group + ":" + artifact + ":" + version
	ans, asked := t.remoteAnswers[key]
	if !asked {
		if t.remoteLimit > 0 && t.asked >= t.remoteLimit {
			// Not asked, and not remembered: the answers kept are no more
			// than the limit. Not an answer (SR-5): what depends on it is
			// not determined.
			if t.notAsked == 0 {
				t.warn("this walk has asked for as many parent and BOM poms as one analysis may; the ones still wanted are not asked for, and what depends on them is reported as not obtained", "remote:"+key, errRemotePomLimit)
			}
			t.notAsked++
			return nil, model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedLookupFailed
		}
		t.asked++
		pom, outcome := t.remote.RemotePom(group, artifact, version, declared())
		ans = remoteAnswer{outcome: outcome}
		if outcome == RemoteFound && pom != nil {
			ans.model = pom.modelOf(key)
		} else if outcome == RemoteFound {
			ans.outcome = RemoteFailed // "found" with no pom is not an answer (SR-5)
		}
		if t.remoteAnswers == nil {
			t.remoteAnswers = map[string]remoteAnswer{}
		}
		t.remoteAnswers[key] = ans
	}
	return ans.reason()
}

// errRemotePomLimit: the walk's limit on the coordinates it asks its source
// for was reached.
var errRemotePomLimit = errors.New("the limit on the poms one analysis asks repositories for was reached")

// reason is the answer as its callers take it: the pom, or why there is
// none (reason), and whether what the pom says is therefore NOT KNOWN
// (unknown: the reason a version behind it is withheld with; "" when every
// repository answered that there is no such pom, which is an answer).
func (ans remoteAnswer) reason() (m *pomModel, reason, unknown string) {
	switch ans.outcome {
	case RemoteFound:
		return ans.model, "", ""
	case RemoteFailed:
		return nil, model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedLookupFailed
	case RemoteRefused:
		return nil, model.VersionReasonInheritedRepositoryRefused, model.VersionReasonInheritedRepositoryRefused
	case RemoteNotAsked:
		return nil, model.VersionReasonInheritedNotFound, model.VersionReasonInheritedNotFound
	}
	return nil, model.VersionReasonInheritedNotFound, ""
}

// worseReason orders the reasons a chain can have: a failed lookup may yet
// succeed and outranks the rest; a refusal is the operator's and outranks
// "not found".
func worseReason(a, b string) string {
	rank := func(r string) int {
		switch r {
		case model.VersionReasonInheritedLookupFailed:
			return 3
		case model.VersionReasonInheritedRepositoryRefused:
			return 2
		case model.VersionReasonInheritedNotFound:
			return 1
		}
		return 0
	}
	if rank(b) > rank(a) {
		return b
	}
	return a
}

func newPomTree(root string, logger *slog.Logger) *pomTree {
	return &pomTree{root: filepath.Clean(root), logger: logger, models: map[string]*pomModel{}}
}

// pomModel is what one pom says, as written.
type pomModel struct {
	path                     string
	group, artifact, version string
	parent                   *pomParentRef
	props                    map[string]string
	managed                  []pomManaged
	// modules are the <modules> entries, as written; profileManagement is
	// true when a <profile> holds dependencyManagement (which this reader
	// does not apply, so a tree that has it is not read whole).
	modules           []string
	profileManagement bool
	// repositories are the <repositories> and <pluginRepositories> the pom
	// declares, each classified (0.29.184).
	repositories []DeclaredRepository
	// fetchable are the addresses of those that may be fetched from, in the
	// order written, each once: fetchableDeclared of this pom alone. Every
	// effective model whose chain has no other declaring pom holds THIS
	// slice (pomContext.declaredFor).
	fetchable []string
	// servedBy is the repository host a fetched pom came from; "" for a
	// pom of the clone.
	servedBy string
	// idx is index()'s answer.
	idx *pomIndex
}

// pomIndex is what one pom's dependencyManagement says, read once and
// shared by every effective model the pom is part of. plain maps
// "group:artifact", for the entries whose coordinates are written out, to
// the position of the first such entry; dynamic are the positions of the
// entries whose group or artifact holds a ${reference}, which mean
// different things in different effective models and are interpolated when
// asked; imports are the positions of the BOM imports, with importKeys
// their "group:artifact:version" where that is written out ("" otherwise).
type pomIndex struct {
	plain   map[string]int
	dynamic []int
	// The reference-keyed entries again, by the half of the key that IS
	// written out: an entry "${project.groupId}:module-7" can only ever be
	// the answer to a question about an artifact named module-7, so a
	// question compares the entries filed under its own artifact (or
	// group) and the ones with a reference in both halves, not all of
	// them. Each list is in the order written. (Round 4: every such entry
	// was interpolated for every question, and a reactor whose parent
	// manages its 300 modules that way lost 98 of them to the work bound.)
	dynByArtifact map[string][]int
	dynByGroup    map[string][]int
	dynBoth       []int
	// imports and importKeys are EVERY import the pom declares, in order,
	// and are never changed: SettleMavenManaged reads them to judge whether
	// the tree is whole. (Round 3 dropped the dead ones from this list
	// itself, and a tree with an unobtainable BOM read as whole.)
	imports    []int
	importKeys []string
	// dead marks, by its place in imports, each import with written-out
	// coordinates that was looked for and is no pom this tree can read,
	// with why (deadReason; "" for an ambiguous one).
	dead       []bool
	deadReason []string
	// deadUnknown is, for a dead import, why what it says is not known
	// (remotePom's unknown; for one that is ambiguous in the tree, that
	// too); "" for one that every repository answered "not found" for.
	deadUnknown []string
	// live are the places in imports a question still tries, in order: all
	// of them at first, less the dead ones once compact has run. So a pom
	// with many imports that lead nowhere costs a question nothing for them
	// after the first. gapBefore[k] is the worst reason among the dead
	// imports dropped from in front of live[k] (its last element: among
	// all that were dropped): a question takes the reasons of the dead
	// imports it PASSES, which are the ones before its answer, and not of
	// those after it (round 4: it took them all, so a version's reason
	// depended on which pom had been read before).
	live      []int
	gapBefore []string
	// unknownBefore is gapBefore for deadUnknown: the worst reason among
	// the dropped imports in front of live[k] whose content is not known.
	unknownBefore []string
	// stale: an import has died since compact last ran; inUse counts the
	// questions in the middle of the list, which compact must not move.
	stale bool
	inUse int
}

// compact drops the imports that have died from live, carrying their
// reasons into gapBefore.
func (idx *pomIndex) compact() {
	if !idx.stale || idx.inUse > 0 {
		return
	}
	live, gaps, unknowns := make([]int, 0, len(idx.live)), make([]string, 0, len(idx.live)+1), make([]string, 0, len(idx.live)+1)
	carry, carryUnknown := "", ""
	for k, n := range idx.live {
		if idx.dead[n] {
			carry = worseReason(carry, idx.deadReason[n])
			carryUnknown = worseReason(carryUnknown, idx.deadUnknown[n])
			continue
		}
		live, gaps = append(live, n), append(gaps, worseReason(idx.gapBefore[k], carry))
		unknowns = append(unknowns, worseReason(idx.unknownBefore[k], carryUnknown))
	}
	gaps = append(gaps, worseReason(idx.gapBefore[len(idx.live)], carry))
	unknowns = append(unknowns, worseReason(idx.unknownBefore[len(idx.live)], carryUnknown))
	idx.live, idx.gapBefore, idx.unknownBefore, idx.stale = live, gaps, unknowns, false
}

func (m *pomModel) index() *pomIndex {
	if m.idx != nil {
		return m.idx
	}
	idx := &pomIndex{plain: map[string]int{}}
	for i, e := range m.managed {
		refGroup, refArtifact := strings.Contains(e.group, "${"), strings.Contains(e.artifact, "${")
		switch {
		case e.isImport:
			key := ""
			if !strings.Contains(e.group+e.artifact+e.version, "${") {
				key = e.group + ":" + e.artifact + ":" + e.version
			}
			idx.live = append(idx.live, len(idx.imports))
			idx.imports = append(idx.imports, i)
			idx.importKeys = append(idx.importKeys, key)
		case refGroup || refArtifact:
			idx.dynamic = append(idx.dynamic, i)
			switch {
			case refGroup && refArtifact:
				idx.dynBoth = append(idx.dynBoth, i)
			case refGroup:
				if idx.dynByArtifact == nil {
					idx.dynByArtifact = map[string][]int{}
				}
				idx.dynByArtifact[e.artifact] = append(idx.dynByArtifact[e.artifact], i)
			default:
				if idx.dynByGroup == nil {
					idx.dynByGroup = map[string][]int{}
				}
				idx.dynByGroup[e.group] = append(idx.dynByGroup[e.group], i)
			}
		default:
			if _, earlier := idx.plain[e.group+":"+e.artifact]; !earlier {
				idx.plain[e.group+":"+e.artifact] = i
			}
		}
	}
	idx.dead, idx.deadReason, idx.deadUnknown = make([]bool, len(idx.imports)), make([]string, len(idx.imports)), make([]string, len(idx.imports))
	idx.gapBefore, idx.unknownBefore = make([]string, len(idx.imports)+1), make([]string, len(idx.imports)+1)
	m.idx = idx
	return idx
}

type pomParentRef struct {
	group, artifact, version string
	// relativePath is where the child says its parent is; "../pom.xml"
	// when the element is absent. inTree is false for an EMPTY element,
	// which is Maven's way of saying "resolve it from a repository".
	relativePath string
	inTree       bool
}

type pomManaged struct {
	group, artifact, version string
	isImport                 bool
}

type xmlPom struct {
	GroupID    string `xml:"groupId"`
	ArtifactID string `xml:"artifactId"`
	Version    string `xml:"version"`
	Parent     *struct {
		GroupID      string  `xml:"groupId"`
		ArtifactID   string  `xml:"artifactId"`
		Version      string  `xml:"version"`
		RelativePath *string `xml:"relativePath"`
	} `xml:"parent"`
	Properties struct {
		Entries []struct {
			XMLName xml.Name
			Value   string `xml:",chardata"`
		} `xml:",any"`
	} `xml:"properties"`
	Managed []struct {
		GroupID    string `xml:"groupId"`
		ArtifactID string `xml:"artifactId"`
		Version    string `xml:"version"`
		Type       string `xml:"type"`
		Scope      string `xml:"scope"`
	} `xml:"dependencyManagement>dependencies>dependency"`
	Repositories []struct {
		URL string `xml:"url"`
	} `xml:"repositories>repository"`
	PluginRepositories []struct {
		URL string `xml:"url"`
	} `xml:"pluginRepositories>pluginRepository"`
	Modules  []string `xml:"modules>module"`
	Profiles []struct {
		Managed []struct {
			ArtifactID string `xml:"artifactId"`
		} `xml:"dependencyManagement>dependencies>dependency"`
	} `xml:"profiles>profile"`
}

// model reads and parses the pom at path once. nil: it is outside the
// clone, is not a regular file (a symlink is refused), or could not be read
// or parsed.
func (t *pomTree) model(path string) *pomModel {
	path = filepath.Clean(path)
	if m, seen := t.models[path]; seen {
		return m
	}
	t.models[path] = nil
	if !regularBuildFile(t.root, path) {
		if buildFileTooLarge(path) {
			t.warn("a pom of the tree is larger than a pom is, and was not read: it answers for nothing", path, errBuildFileTooLarge)
		}
		return nil
	}
	content, err := ReadManifest(path)
	if err != nil {
		t.warn("a pom of the tree could not be read — it answers for nothing", path, err)
		return nil
	}
	m, err := decodePom(string(content), path)
	if err != nil {
		t.warn("a pom of the tree could not be parsed — it answers for nothing", path, err)
		return nil
	}
	t.models[path] = m
	return m
}

// decodePom reads one pom document into what it says, as written.
func decodePom(content, path string) (*pomModel, error) {
	var x xmlPom
	dec := xml.NewDecoder(strings.NewReader(content))
	// The bytes are already decoded; a prolog naming another encoding must
	// not make the decoder refuse the document.
	dec.CharsetReader = func(_ string, in io.Reader) (io.Reader, error) { return in, nil }
	if err := dec.Decode(&x); err != nil {
		return nil, err
	}
	m := &pomModel{path: path, group: strings.TrimSpace(x.GroupID), artifact: strings.TrimSpace(x.ArtifactID),
		version: strings.TrimSpace(x.Version), props: map[string]string{}}
	if p := x.Parent; p != nil {
		ref := &pomParentRef{group: strings.TrimSpace(p.GroupID), artifact: strings.TrimSpace(p.ArtifactID),
			version: strings.TrimSpace(p.Version), relativePath: "../pom.xml", inTree: true}
		if p.RelativePath != nil {
			ref.relativePath = strings.TrimSpace(*p.RelativePath)
			ref.inTree = ref.relativePath != ""
		}
		m.parent = ref
	}
	for _, e := range x.Properties.Entries {
		m.props[e.XMLName.Local] = strings.TrimSpace(e.Value)
	}
	for _, d := range x.Managed {
		m.managed = append(m.managed, pomManaged{group: strings.TrimSpace(d.GroupID), artifact: strings.TrimSpace(d.ArtifactID),
			version:  strings.TrimSpace(d.Version),
			isImport: strings.TrimSpace(d.Scope) == "import" && strings.TrimSpace(d.Type) == "pom"})
	}
	for _, r := range x.Repositories {
		if d, ok := classifyDeclaredRepository(r.URL, RepositoryForDependencies); ok {
			m.repositories = append(m.repositories, d)
		}
	}
	for _, r := range x.PluginRepositories {
		if d, ok := classifyDeclaredRepository(r.URL, RepositoryForPlugins); ok {
			m.repositories = append(m.repositories, d)
		}
	}
	m.fetchable = fetchableDeclared([]*pomModel{m})
	for _, mod := range x.Modules {
		if mod = strings.TrimSpace(mod); mod != "" {
			m.modules = append(m.modules, mod)
		}
	}
	for _, pr := range x.Profiles {
		if len(pr.Managed) > 0 {
			m.profileManagement = true
		}
	}
	return m, nil
}

func (t *pomTree) warn(msg, path string, err error) {
	if t.logger != nil {
		t.logger.Warn(msg, "path", path, "error", err)
	}
}

// pomChainLimit bounds a parent chain and a chain of BOM imports. Real
// chains are a handful long; the bound is what ends a cycle.
const pomChainLimit = 32

// chain returns m and its parents, nearest first, and why the chain is
// not whole when it is not ("" when it is, or when nothing could be asked).
// A parent in the tree is the pom at the child's relativePath (a directory
// means its pom.xml) whose own coordinates are the ones the child names.
// A parent that is not there (an empty relativePath, no such file, another
// project at that place) is asked of the remote source, as Maven would
// resolve it from a repository; so is every parent of a remote pom.
func (t *pomTree) chain(m *pomModel) (chain []*pomModel, gap, unknown string) {
	chain, gap, unknown, _ = t.chainWhole(m)
	return chain, gap, unknown
}

// chainWhole is chain that also says whether the chain was followed to its
// end: a pom with no parent. It is not when a parent could not be obtained
// (in the tree or from a repository), the chain loops, or it is too long.
//
// unknown is remotePom's for the parent that could not be obtained: why
// what the far end of the chain says is not known, "" when the chain is
// whole or its missing parent is known to exist nowhere.
func (t *pomTree) chainWhole(m *pomModel) (chain []*pomModel, gap, unknown string, whole bool) {
	out := []*pomModel{m}
	// A cycle of parents ends at pomChainLimit and nowhere sooner (round 7).
	// Two guards used to end one at the first pom met twice, each covered
	// by the other and by the limit, so that no test could tell either was
	// there; and the first of them stopped at a relativePath that led back
	// to a pom already in the chain BEFORE its coordinates were compared,
	// where Maven, finding the coordinates do not match, asks a repository.
	// The chain of a cycle is the cycle repeated, which answers every
	// question as its first turn does, and is never whole.
	cur := m
	for cur.parent != nil && len(out) < pomChainLimit {
		var p *pomModel
		if cur.parent.inTree {
			at := filepath.Join(filepath.Dir(cur.path), filepath.FromSlash(cur.parent.relativePath))
			if info, err := os.Lstat(at); err == nil && info.IsDir() {
				at = filepath.Join(at, "pom.xml")
			}
			at = filepath.Clean(at)
			// Verified by coordinates. The parent's own group and version
			// may be inherited from ITS parent; what it states must agree.
			if c := t.model(at); c != nil && c.artifact == cur.parent.artifact &&
				(c.group == "" || c.group == cur.parent.group) &&
				(c.version == "" || strings.Contains(c.version, "${") || c.version == cur.parent.version) {
				p = c
			}
		}
		if p == nil {
			soFar := out
			remote, reason, unknown := t.remotePom(cur.parent.group, cur.parent.artifact, cur.parent.version, func() []string {
				return fetchableDeclared(soFar)
			})
			if remote == nil {
				return out, reason, unknown, false
			}
			p = remote
		}
		out = append(out, p)
		cur = p
	}
	// The loop ended at a pom with no parent (whole), or at the bound with a
	// parent still to follow. (Until 0.29.214 a chain of exactly
	// pomChainLimit poms that ended was called not whole: the count was
	// compared and not the end.)
	return out, "", "", cur.parent == nil
}

// pomContext is the effective model of one pom: what Maven builds from it
// and its parents. Interpolation happens here, in the context of the pom
// being built, as in Maven: a property the child overrides changes what a
// parent's entry means for that child.
//
// It is a VIEW over its chain (0.29.210): it holds the chain and the pom's
// own coordinates, and nothing that grows with what the chain says. A
// property is looked up along the chain, the nearest pom first; a managed
// entry in each pom's own index. It holds no state from a question: why a
// question could not be answered (a BOM not obtained) is the question's
// (pomLookup), so a pom's reasons never depend on what was read before it.
type pomContext struct {
	tree                     *pomTree
	chain                    []*pomModel
	group, artifact, version string
	parentGroup, parentVer   string
	// chainGap is why this model is not whole in its own chain: a parent
	// could not be obtained. "" when the chain is whole.
	chainGap string
	// chainUnknown is why what the missing end of the chain says is NOT
	// KNOWN (remotePom's unknown); "" when the chain is whole or its
	// missing parent is on no repository.
	chainUnknown string
	// declaredList is declaredFor's answer: the one declaring pom's own
	// list, by reference, or a merged one.
	declaredList []string
	declaredDone bool
}

// declaredFor is the repositories this model's chain declares and that may
// be fetched from.
//
// The list belongs to the pom that DECLARES it (pomModel.fetchable) and is
// shared by reference: a chain in which one pom declares repositories, which
// is every module of a reactor whose root declares them, holds that pom's
// list and nothing of its own. Round 5: each effective model kept its own
// copy, the modules times the repositories (83 MB for 574 KB of poms). A
// chain in which two or more poms declare has a list of its own, merged, and
// it is written out only when a request is made.
func (c *pomContext) declaredFor() []string {
	if c.declaredDone {
		return c.declaredList
	}
	var only *pomModel
	declaring := 0
	for _, m := range c.chain {
		if len(m.fetchable) > 0 && !strings.HasPrefix(m.path, "remote:") {
			if declaring++; only == nil {
				only = m
			}
		}
	}
	switch declaring {
	case 0:
	case 1:
		c.declaredList = only.fetchable
	default:
		c.declaredList = fetchableDeclared(c.chain)
	}
	c.declaredDone = true
	return c.declaredList
}

// context is the effective model of m, built once per tree: a BOM's used to
// be built again for every dependency looked up in it.
func (t *pomTree) context(m *pomModel) *pomContext {
	if c, ok := t.contexts[m]; ok {
		return c
	}
	c := t.newContext(m)
	if t.contexts == nil {
		t.contexts = map[*pomModel]*pomContext{}
	}
	t.contexts[m] = c
	return c
}

func (t *pomTree) newContext(m *pomModel) *pomContext {
	chain, gap, unknown := t.chain(m)
	c := &pomContext{tree: t, chain: chain, chainGap: gap, chainUnknown: unknown, artifact: m.artifact}
	// Group and version are the pom's own, else the parent's as the pom
	// NAMES it (which holds even when the parent is not in the tree).
	c.group, c.version = m.group, m.version
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

// pomInterpolatedLimit bounds what an interpolation may produce. Every
// value interpolated here is a coordinate, a version or a repository
// address, and none longer than pkgid.MaxNameBytes can be stored or keyed
// (the figure is the database's own index-entry bound). The passes are
// bounded too, but passes do not bound size: a property that is another
// one twenty times over, five levels deep, is 3.2 MB after five passes (a
// 1,290-byte pom with forty repetitions allocated 3.7 GB; review round 1).
const pomInterpolatedLimit = pkgid.MaxNameBytes

// pomSubstitutePasses bounds how deep a chain of properties is followed:
// a property whose value names another, ten times over.
const pomSubstitutePasses = 10

// pomSubstitute is THE substitution of ${name} references in a pom, for
// the file's own reader (resolvePomProperties) and the tree's
// (pomContext.interpolate). resolve answers for a name; a reference it does
// not know is left as written.
//
// What it builds is bounded WHILE it is built (0.29.209): the moment the
// output would pass pomInterpolatedLimit, s is returned as it was given,
// still holding its references, which every caller reads as unresolved.
// Before, the tree's reader looked at the bound after each whole pass (one
// pass over 4,000 references to a 50 KB property is 200 MB), and the file's
// reader had none (a 78 KB pom built an 80 MB version). A string already
// longer than the bound is not a coordinate or a version and is returned
// untouched. The cost of a call is at most the passes times the bound.
func pomSubstitute(s string, resolve func(name string) (string, bool)) string {
	if len(s) > pomInterpolatedLimit || !strings.Contains(s, "${") {
		return s
	}
	// Each name is resolved once for this call: resolving walks a chain of
	// up to pomChainLimit poms, and a value that names one property five
	// hundred times, through ten passes, walked it for each (review round
	// 6: 9 microseconds for each byte of pom read). The first answer is
	// held in place and a map is made only for a second name, so the usual
	// string, with one reference, allocates nothing for it. Nothing is kept
	// beyond the call: an effective model holds its chain, never a copy of
	// what the chain says.
	var (
		firstName, firstValue string
		firstOK, haveFirst    bool
		others                map[string]pomSubstituted
	)
	inner := resolve
	resolve = func(name string) (string, bool) {
		if haveFirst && name == firstName {
			return firstValue, firstOK
		}
		if a, ok := others[name]; ok {
			return a.value, a.ok
		}
		v, ok := inner(name)
		if !haveFirst {
			firstName, firstValue, firstOK, haveFirst = name, v, ok, true
			return v, ok
		}
		if others == nil {
			others = map[string]pomSubstituted{}
		}
		others[name] = pomSubstituted{v, ok}
		return v, ok
	}
	original := s
	for pass := 0; pass < pomSubstitutePasses && strings.Contains(s, "${"); pass++ {
		var sb strings.Builder
		changed, over := false, false
		write := func(piece string) {
			if sb.Len()+len(piece) > pomInterpolatedLimit {
				over = true
				return
			}
			sb.WriteString(piece)
		}
		for i := 0; i < len(s) && !over; {
			at := strings.Index(s[i:], "${")
			if at < 0 {
				write(s[i:])
				break
			}
			write(s[i : i+at])
			start := i + at
			end := strings.IndexByte(s[start:], '}')
			if end < 0 {
				write(s[start:])
				break
			}
			name := s[start+2 : start+end]
			if name == "" || strings.ContainsAny(name, "${") {
				// Not a reference ("${a${b}}": the inner one is, and is
				// found next).
				write("${")
				i = start + 2
				continue
			}
			ref := s[start : start+end+1]
			if v, ok := resolve(name); ok && v != ref {
				write(v)
				changed = true
			} else {
				write(ref)
			}
			i = start + end + 1
		}
		if over {
			return original
		}
		if !changed {
			break
		}
		s = sb.String()
	}
	return s
}

// pomSubstituted is one name's answer within one substitution.
type pomSubstituted struct {
	value string
	ok    bool
}

// interpolate resolves ${...} references. What it cannot resolve is left as
// written, and so is a string whose expansion passes pomInterpolatedLimit:
// it still holds its reference, which every caller reads as unresolved.
func (c *pomContext) interpolate(s string) string {
	return pomSubstitute(s, c.resolve)
}

// resolve is the value of one reference in this context.
func (c *pomContext) resolve(name string) (string, bool) {
	switch name {
	case "project.version", "pom.version", "version":
		if c.version != "" {
			return c.version, true
		}
	case "project.groupId", "pom.groupId", "groupId":
		if c.group != "" {
			return c.group, true
		}
	case "project.artifactId", "pom.artifactId", "artifactId":
		if c.artifact != "" {
			return c.artifact, true
		}
	case "project.parent.version", "parent.version":
		if c.parentVer != "" {
			return c.parentVer, true
		}
	case "project.parent.groupId", "parent.groupId":
		if c.parentGroup != "" {
			return c.parentGroup, true
		}
	}
	// The nearest pom that sets it: a child overrides its parents.
	for _, m := range c.chain {
		if v, ok := m.props[name]; ok {
			return v, true
		}
	}
	return "", false
}

// pomLookup is one question put to an effective model: which version
// dependencyManagement gives a coordinate. It carries the reading it is part
// of, the BOMs it has already searched, and what it could not consult.
type pomLookup struct {
	rd *pomReading
	// searched: a BOM is searched once per question. Searched again it
	// answers the same, so a second route to it (a diamond, or a cycle of
	// poms that import each other) is skipped. Without this the depth limit
	// was the only bound, and each level multiplied by the number of
	// imports: two poms importing each other twice were 2^32 calls (review
	// round 1).
	searched map[*pomModel]bool
	// gap is the worst reason a pom the question MET could not be obtained:
	// the missing end of a chain it looked into, a BOM import that led
	// nowhere. It is what a dependency NOTHING manages is reported with.
	gap string
	// before is the worst reason among the poms the question has come PAST
	// that could not be obtained: the missing end of a parent chain whose
	// own entries did not answer, and each BOM import that led nowhere.
	// Maven consults them in that order, so when an answer is found, before
	// is why it may not be the answer: a pom that comes first could not be
	// read. (gap is not that: it also holds the missing end of the very
	// chain whose nearer pom answered, which the nearer pom outranks.)
	before string
}

// managed is the version dependencyManagement gives group:artifact in this
// pom's effective model: an explicit entry anywhere in the chain (the
// nearest pom wins, and in a pom the first entry), then the BOMs the chain
// imports, in order. found is false when nothing in the tree manages it, or
// the version it is given cannot be resolved.
//
// hosts are the repository hosts the answer came through beyond this
// model's own chain: those of the BOM that supplied it and of that BOM's
// chain.
//
// gap is why the answer may not be the whole truth: a pom of this chain, or
// a BOM this question looked for, could not be obtained ("" when all were).
//
// THE RULE (rounds 6 and 7; SR-5): a managed version is taken from a pom
// only if every pom Maven consults BEFORE it for the artifact is KNOWN not
// to manage it. Known needs the pom's content:
//
//   - before is why a FOUND answer is not known to be the answer: what a
//     pom consulted before the one that answered says is not known. Its
//     lookup failed, its repository may not be fetched from, or it was
//     never asked for (a snapshot, a range, a coordinate with an unresolved
//     reference or that no request can be built from, no source installed,
//     the walk's limit on requests), or the tree holds two poms under its
//     coordinates. "" when there is none such. An answer with a before is
//     NOT the version: it is withheld, and before is the reason stored. A
//     pom that every repository read answered "not found" for is known to
//     say nothing, and the search goes on past it as Maven's build would
//     not: that is an answer.
//   - a pom that DOES manage the artifact, at a version it cannot give (a
//     reference to a property nothing it could read sets), has answered:
//     the search stops there with no version, and a later BOM's is not
//     used. gap then says what of that pom's chain could not be obtained.
func (c *pomContext) managed(rd *pomReading, group, artifact string) (version string, found bool, hosts []string, gap, before string) {
	version, found, _, hosts, gap, before = c.ask(rd, group, artifact)
	return version, found, hosts, gap, before
}

// ask is managed that also says whether the question was ANSWERED with no
// version (stop: a pom that was read manages the artifact and cannot give
// its version). A caller that consults several poms in turn, as the Gradle
// tree does its platforms, must not go on to the next one then.
func (c *pomContext) ask(rd *pomReading, group, artifact string) (version string, found, stop bool, hosts []string, gap, before string) {
	lk := &pomLookup{rd: rd, searched: map[*pomModel]bool{}, gap: c.chainGap}
	version, found, hosts, stop = c.lookup(lk, group, artifact, 0)
	return version, found, stop, hosts, lk.gap, lk.before
}

// lookup answers the question in c. stop is true when a pom that was read
// manages the artifact at a version that cannot be resolved: the question
// is answered (with no version), and no pom after it is asked.
func (c *pomContext) lookup(lk *pomLookup, group, artifact string, depth int) (version string, found bool, bomHosts []string, stop bool) {
	key := group + ":" + artifact
	for _, m := range c.chain {
		idx := m.index()
		best, has := idx.plain[key]
		// An entry whose coordinates hold a reference comes first if it is
		// written first and means this coordinate here. Such an entry can
		// only answer a question about the half of its key that IS written
		// out, so the lists asked are the ones filed under this artifact
		// and this group and the one with a reference in both halves. A
		// coordinate with a colon in it is not told apart from the colon
		// between the halves, so a question that has one asks them all.
		if len(idx.dynamic) > 0 {
			earlier := func(j int, ok bool) {
				if ok && (!has || j < best) {
					best, has = j, true
				}
			}
			if strings.Contains(group, ":") || strings.Contains(artifact, ":") {
				earlier(c.firstRef(lk.rd, m, idx.dynamic, pomRefListAt{c, m, '*', ""}, key))
			} else {
				earlier(c.firstRef(lk.rd, m, idx.dynByArtifact[artifact], pomRefListAt{c, m, 'a', artifact}, key))
				earlier(c.firstRef(lk.rd, m, idx.dynByGroup[group], pomRefListAt{c, m, 'g', group}, key))
				earlier(c.firstRef(lk.rd, m, idx.dynBoth, pomRefListAt{c, m, 'b', ""}, key))
			}
		}
		if has {
			v := c.interpolate(m.managed[best].version)
			if v != "" && !strings.Contains(v, "${") {
				// The chain's own entry: nothing that could not be obtained
				// comes before it (a chain is missing its far end, and the
				// nearer pom wins).
				return v, true, nil, false
			}
			// It manages the artifact and cannot say at what version.
			return v, false, nil, true
		}
	}
	// No pom of the chain that was read manages it. One that could not be
	// obtained may, and it comes before every import.
	lk.before = worseReason(lk.before, c.chainUnknown)
	if depth >= pomChainLimit {
		return "", false, nil, false
	}
	for _, m := range c.chain {
		if v, ok, via, stop := c.lookupImports(lk, m, group, artifact, depth); ok || stop {
			return v, ok, via, stop
		}
	}
	return "", false, nil, false
}

// lookupImports asks the BOMs that m imports, in order.
//
// The question takes the reason of every import it passes that leads
// nowhere, and of none it does not reach: the ones in front of its answer
// (idx.gapBefore, and what it finds dead itself), or all of them when no
// import answers.
func (c *pomContext) lookupImports(lk *pomLookup, m *pomModel, group, artifact string, depth int) (version string, found bool, bomHosts []string, stop bool) {
	idx := m.index()
	idx.compact()
	idx.inUse++
	defer func() { idx.inUse-- }()
	for k := 0; k < len(idx.live); k++ {
		n := idx.live[k]
		c.tree.importSteps++
		if idx.dead[n] {
			// Found so by a question inside this one, and passed.
			lk.pass(idx.deadReason[n], idx.deadUnknown[n])
			continue
		}
		e := m.managed[idx.imports[n]]
		plain := idx.importKeys[n] != ""
		bg, ba, bv := e.group, e.artifact, e.version
		var at *pomResolvedImport
		if !plain {
			// Its coordinates hold a reference: what they are depends on
			// the model being built. Worked out once for the file being
			// read.
			at = lk.rd.importAt(c, m, n)
			if at.dead {
				lk.pass(at.reason, at.unknown)
				continue
			}
			bg, ba, bv = at.group, at.artifact, at.version
		}
		bom, ambiguous := c.tree.byCoordinates(bg, ba, bv)
		reason, unknown := "", ""
		switch {
		case ambiguous:
			// The tree holds two poms under that name, and a repository's
			// would be a third candidate, not the answer: it is not asked,
			// and what the import says is not known.
			unknown = model.VersionReasonInheritedNotFound
		case bom == nil:
			// Not in the tree: asked of a repository.
			bom, reason, unknown = c.tree.remotePom(bg, ba, bv, c.declaredFor)
		}
		if bom == nil {
			// It leads nowhere: for every question of this walk when its
			// coordinates are written out, for this file's when they depend
			// on the model. Not looked for again.
			lk.pass(reason, unknown)
			if plain {
				idx.dead[n], idx.deadReason[n], idx.deadUnknown[n], idx.stale = true, reason, unknown, true
			} else {
				at.dead, at.reason, at.unknown = true, reason, unknown
			}
			continue
		}
		if lk.searched[bom] {
			continue
		}
		lk.searched[bom] = true
		// A BOM is built in its OWN context.
		bc := c.tree.context(bom)
		lk.gap = worseReason(lk.gap, bc.chainGap)
		v, ok, via, stop := bc.lookup(lk, group, artifact, depth+1)
		if ok || stop {
			lk.pass(idx.gapBefore[k], idx.unknownBefore[k])
			if !ok {
				return v, false, nil, true
			}
			return v, true, mergeHosts(chainHosts(bc.chain), via), false
		}
	}
	lk.pass(idx.gapBefore[len(idx.live)], idx.unknownBefore[len(idx.live)])
	return "", false, nil, false
}

// pass records an import the question came past that could not be obtained:
// why (reason), and why what it says is not known (unknown; "" when every
// repository read answered that there is no such pom, which is an answer:
// SR-5).
func (lk *pomLookup) pass(reason, unknown string) {
	lk.gap = worseReason(lk.gap, reason)
	lk.before = worseReason(lk.before, unknown)
}

// byCoordinates returns the one pom in the tree with these coordinates, or
// nil when there is none or more than one (SR-6: fixture and template poms
// share coordinates with real ones; neither is chosen). ambiguous says
// which: more than one.
func (t *pomTree) byCoordinates(group, artifact, version string) (m *pomModel, ambiguous bool) {
	if group == "" || artifact == "" || version == "" || strings.Contains(group+artifact+version, "${") {
		return nil, false
	}
	t.indexTree()
	paths := t.byCoord[group+":"+artifact+":"+version]
	if len(paths) != 1 {
		return nil, len(paths) > 1
	}
	return t.model(paths[0]), false
}

// indexTree reads every pom.xml of the clone once: pomFiles lists them (the
// ones that do not parse too) and byCoord maps coordinates to those that do.
func (t *pomTree) indexTree() {
	if t.byCoord == nil {
		t.byCoord = map[string][]string{}
		_ = filepath.Walk(t.root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.Mode()&os.ModeSymlink != 0 {
				return nil
			}
			if info.IsDir() {
				switch filepath.Base(path) {
				case "vendor", "node_modules", ".git":
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Base(path) != "pom.xml" {
				return nil
			}
			t.pomFiles = append(t.pomFiles, filepath.Clean(path))
			m := t.model(path)
			if m == nil {
				return nil
			}
			// Its own coordinates, from the tree alone: indexing must not
			// send a request per pom of the tree.
			c := t.treeOnly().context(m)
			key := c.interpolate(c.group) + ":" + c.interpolate(c.artifact) + ":" + c.interpolate(c.version)
			t.byCoord[key] = append(t.byCoord[key], m.path)
			return nil
		})
	}
}

// treeOnly is the tree with no remote source: the same poms, and nothing
// asked.
func (t *pomTree) treeOnly() *pomTree {
	if t.remote == nil {
		return t
	}
	return &pomTree{root: t.root, logger: t.logger, models: t.models, byCoord: t.byCoord, pomFiles: t.pomFiles}
}

// settle gives the dependencies read from the pom at path what the tree
// knows: a version written as a property the pom inherits, and, for a
// dependency with no version at all, the version dependencyManagement gives
// it. The second is marked SourceManaged. What the tree cannot answer is
// left as it was, for the remote resolver.
func (t *pomTree) settle(path string, deps []model.LibyearDep) []model.LibyearDep {
	m := t.model(path)
	if m == nil {
		return deps
	}
	return t.settleDeps(m, deps, &pomReading{})
}

func (t *pomTree) settleDeps(m *pomModel, deps []model.LibyearDep, rd *pomReading) []model.LibyearDep {
	c := t.context(m)
	// Why a version-less dependency may have a version this reader could
	// not see: a pom of the chain, or a BOM that a question of THIS pom
	// looked for, could not be obtained. It grows with the pom's own
	// questions, in order, and with nothing else.
	gap := c.chainGap
	for i := range deps {
		group, artifact, ok := strings.Cut(deps[i].Name, ":")
		if !ok {
			continue
		}
		switch {
		case strings.Contains(deps[i].Version, "${"):
			if v := c.interpolate(deps[i].Version); !strings.Contains(v, "${") {
				deps[i].Version = v
				deps[i].Requirement = deps[i].Name + ":" + v
				// The property may have come from a fetched parent.
				deps[i].VersionHosts = chainHosts(c.chain)
			}
		case deps[i].Version == "":
			v, found, via, g, before := c.managed(rd, group, artifact)
			gap = worseReason(gap, g)
			switch {
			case found && before == "":
				deps[i].Version = v
				deps[i].VersionSource = SourceManaged
				// The hosts this version came through (0.29.185): the
				// fetched poms of this pom's own chain, and of the BOM
				// that supplied it.
				deps[i].VersionHosts = mergeHosts(chainHosts(c.chain), via)
			case found:
				// A BOM manages it, and a pom consulted before that BOM
				// could not be obtained: the version is not determined, and
				// that is said (SR-5; see managed).
				deps[i].VersionReason = before
			case gap != "":
				// It has no version, and a pom that might have given it
				// one could not be obtained: that is said, and is not
				// the same as "declares no version".
				deps[i].VersionReason = gap
			}
		}
	}
	return deps
}

// Which of a build's uses a declared repository serves.
const (
	RepositoryForDependencies = "dependencies"
	RepositoryForPlugins      = "plugins"
)

// Why a declared repository is recorded and never fetched from.
const (
	// NotFetchedNotHTTPS: the address is not an https URL (http, file, a
	// local path).
	NotFetchedNotHTTPS = "not_https"
	// NotFetchedCredentials: the address carries a user name or password.
	// They are not stored.
	NotFetchedCredentials = "credentials_in_url"
	// NotFetchedUnreadable: the address is built from a variable that could
	// not be read, or is not a URL.
	NotFetchedUnreadable = "unreadable"
	// NotFetchedPluginsOnly: a plugin repository; recorded, and not used
	// for dependencies.
	NotFetchedPluginsOnly = "plugins_only"
	// The Gradle kinds that name no repository to fetch from (0.29.195):
	// the machine's own Maven cache, a directory of files, an Ivy
	// repository (another layout), and JCenter, which was shut down.
	NotFetchedLocal    = "local"
	NotFetchedFlatDir  = "flat_dir"
	NotFetchedIvy      = "ivy"
	NotFetchedShutDown = "shut_down"
)

// DeclaredRepository is one repository a build file names (v0.29.184;
// repo_declared_repositories).
type DeclaredRepository struct {
	ManifestPath string // relative to the clone; set when listed
	// URL is the address with no trailing slash and NO credentials; for an
	// address that could not be read, the text as written.
	URL  string
	Kind string // RepositoryForDependencies or RepositoryForPlugins
	// NotFetched is why it is never fetched from; "" for one that may be.
	NotFetched string
}

// classifyDeclaredRepository reads one declared address. ok is false for an
// empty one.
func classifyDeclaredRepository(raw, kind string) (DeclaredRepository, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DeclaredRepository{}, false
	}
	d := DeclaredRepository{URL: raw, Kind: kind}
	if strings.Contains(raw, "${") {
		d.NotFetched = NotFetchedUnreadable
		return d, true
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		d.NotFetched = NotFetchedUnreadable
		return d, true
	}
	hadCredentials := u.User != nil
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	d.URL = strings.TrimRight(u.String(), "/")
	switch {
	case hadCredentials:
		d.NotFetched = NotFetchedCredentials
	case u.Scheme != "https" || u.Hostname() == "":
		d.NotFetched = NotFetchedNotHTTPS
	case kind == RepositoryForPlugins:
		d.NotFetched = NotFetchedPluginsOnly
	}
	return d, true
}

// fetchableDeclared are the repositories the tree poms of a chain declare
// and that may be fetched from, the nearest pom's first, each once. A
// fetched pom's own declarations are not followed.
func fetchableDeclared(chain []*pomModel) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range chain {
		if strings.HasPrefix(m.path, "remote:") {
			continue
		}
		for _, d := range m.repositories {
			if d.NotFetched == "" && !seen[d.URL] {
				seen[d.URL] = true
				out = append(out, d.URL)
			}
		}
	}
	return out
}

// declaredRepositories lists what every pom of the clone that was read
// declares, once per manifest and address.
func (t *pomTree) declaredRepositories() []DeclaredRepository {
	var out []DeclaredRepository
	seen := map[string]bool{}
	for path, m := range t.models {
		if m == nil || strings.HasPrefix(path, "remote:") {
			continue
		}
		rel, err := filepath.Rel(t.root, path)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(rel)
		for _, d := range m.repositories {
			key := rel + "\x00" + d.Kind + "\x00" + d.URL
			if seen[key] {
				continue
			}
			seen[key] = true
			d.ManifestPath = rel
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ManifestPath != out[j].ManifestPath {
			return out[i].ManifestPath < out[j].ManifestPath
		}
		return out[i].URL < out[j].URL
	})
	return out
}

// chainHosts are the repository hosts the fetched poms of a chain came from,
// sorted, each once. A chain of the clone's own poms has none.
func chainHosts(chain []*pomModel) []string {
	return mergeHosts(nil, func() []string {
		var out []string
		for _, m := range chain {
			if m.servedBy != "" {
				out = append(out, m.servedBy)
			}
		}
		return out
	}())
}

// mergeHosts is the sorted union of two host lists; nil when it is empty.
func mergeHosts(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, h := range append(append([]string(nil), a...), b...) {
		if h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	sort.Strings(out)
	return out
}
