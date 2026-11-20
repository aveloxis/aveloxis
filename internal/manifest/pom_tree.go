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
	// work is the walk's counter of looks against what all the poms and
	// Gradle files read allow (pomWorkFactor); shared with treeOnly.
	work *pomWork
	// noRoom are the poms that were read and not kept: the walk's account
	// had no room for them (model).
	noRoom map[string]bool
	// acct is the walk's account (walkAccount): what is kept, asked and
	// taken. The Gradle tree of the same walk holds the same one.
	acct *walkAccount
	// looks counts every look made, and importSteps every import a
	// question passed over or looked at, for tests (a count where a clock
	// cannot tell: passing a thousand dead imports costs microseconds).
	looks       int
	importSteps int
}

// The rule (0.29.210, after three review rounds each found another product):
// a walk's total work and what it retains are linear in the bytes it reads.
// A bound on each FILE does not give that. Two thousand modules under one
// parent with two thousand managed entries are 763 KB of poms, and a copy of
// the parent's entries per module was 498 MB; the same holds for every
// structure that is built per file from what the file shares with others.
// So:
//
//   - what a pom says is indexed ONCE, on its model (pomIndex), and an
//     effective model (pomContext) is a view over its chain of models: a
//     question walks the chain, at most pomChainLimit long;
//   - work that is not constant per question is counted in units (a
//     "look": one BOM import tried, or one managed entry whose coordinates
//     hold a reference and must be interpolated to be compared; and
//     pomLookBytes bytes of interpolation), twice: against the FILE being
//     read (pomBudget), so that one pathological pom costs itself and not
//     its neighbours, and against the walk (pomWork), so that the sum stays
//     linear whatever the files share;
//   - NOTHING kept for the walk is keyed by a pair (a file, and something
//     another file says). What a question works out that depends on the
//     file asking (an import whose coordinates hold a reference means a
//     different BOM in each pom that inherits it) is kept for that file's
//     reading only (pomBudget.resolved) and dropped with it. Round 4: the
//     tree kept such a key per (pom, import), 330 MB for 92 KB of poms.

// pomWork is the walk's counter: every unit, against pomWorkFactor units
// per byte of pom or Gradle build file read by the walk (each file once).
type pomWork struct {
	used, limit int
	// carry is the bytes touched by work that belongs to no one file (a
	// pass over the tree, the building of a chain) and not yet a whole unit.
	carry int
}

// pomLookBytes is how many bytes of interpolation cost what one look
// costs: counting the bytes is what makes the unit a unit of TIME. Round 4
// measured a look at 0.5 us when it interpolated nothing long and at 9.5 us
// through long properties, and a bound in looks alone was a bound on
// neither.
//
// PROVISIONAL (summary/71, section 8). Its derivation is a measurement:
// BenchmarkPomLook reads a pom whose looks pass BOM imports with literal
// coordinates (120,000 units) and one whose looks interpolate a 2,600-byte
// property through a chain into every managed entry they compare (53,923
// units), and reports the time of each per unit, parsing included: 281 and
// 277 ns (0.29.211, an Apple M-series core). With a kilobyte to the look
// the cheapest and the dearest look cost the same per unit, which is what
// the constant is for; it is a power of two, and was not tuned further.
//
// Round 5: the bytes are counted wherever a string is touched (pomBudget.touch:
// an interpolation, a key joined and hashed, coordinates compared), not for
// interpolation alone, and none is free. TestTheUnitsOfBothWalksAreUnitsOfTime
// measures a look past short coordinates at about 40 ns a unit, one past a
// 200 KB coordinate at about 240 and one that interpolates a 2,600-byte
// property at about 170, parsing included: the unit is between 40 and 300 ns
// whatever the poms hold, and a walk's poms cost at most that for every byte
// read.
const pomLookBytes = 1024

// pomWorkFactor is the allowance: one unit per byte, for a file and for the
// walk. PROVISIONAL (summary/71, section 8); its derivation:
//
// As a bound on a worker's time. A unit is about 0.3 microseconds
// (BenchmarkPomLook), so a file of MaxBuildFileBytes (8 MiB) can cost its
// reader about two and a half seconds, and a walk as much for every 8 MiB
// it reads, whatever the files share (the hostile shapes of
// TestBuildFileReadersScaleLinearly stop there).
//
// As room for honest builds. What costs a real build units is small beside
// its text: a version-less dependency is some eighty bytes and asks one
// question; the question costs a unit for each live BOM import it passes
// and for each managed entry it must interpolate to compare, and those are
// the entries whose artifact (or group) is the one asked for
// (pomIndex.dynByArtifact), not all of them. Measured:
// TestAnHonestReactorIsFarInsideTheWorkBound (600 modules of 30
// version-less dependencies under a parent with 300 entries keyed by
// ${project.groupId} and 2,200 plain ones) and
// TestARealShapedMavenBuildIsFarFromTheWorkBound (300 dependencies no BOM
// manages, forty BOMs) each ask for a tenfold margin and have far more. A
// file that passes the bound is reported as a manifest that could not be
// read.
const pomWorkFactor = 1

// pomBudget is what reading ONE file may cost the pom tree.
type pomBudget struct {
	tree        *pomTree
	used, limit int
	// counted are the poms whose bytes are already in limit.
	counted map[*pomModel]bool
	// tripped: a look was refused. What the file's reader worked out is
	// then a part, and it takes nothing.
	tripped bool
	// walkOnly: no allowance of its own; the walk's is the only bound (a
	// pass over the whole tree, SettleMavenManaged).
	walkOnly bool
	// carry is the bytes touched (touch) that are not yet a whole unit.
	carry int
	// resolved are the imports whose coordinates hold a reference, as this
	// file's questions worked them out: what each means in the model that
	// asked, and whether it led nowhere. Kept for the reading of this ONE
	// file: the answer depends on the model asking, so across files it is
	// a product (see the rule above).
	resolved map[pomImportAt]*pomResolvedImport
}

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
	reason                   string
}

// interp is c.interpolate(s), with what it cost charged: pomLookBytes bytes
// of it are a unit. ok is false when the allowance is used up.
func (b *pomBudget) interp(c *pomContext, s string) (out string, ok bool) {
	out, work := pomSubstituteWork(s, c.resolve)
	if !b.touch(work) {
		return s, false
	}
	return out, true
}

// touch charges n bytes read, written, compared or hashed: pomLookBytes of
// them are a unit, and none is free (what is left of a unit is carried to
// the next call). Everything whose cost grows with the length of a string is
// charged here, where the string is touched: an interpolation, a key built
// or hashed, a coordinate compared (review round 5: a look was one unit
// whatever the length of the coordinates it concatenated and hashed, and an
// import with a 6 MiB groupId cost 570,975 ns a unit). false: the allowance
// is used up.
func (b *pomBudget) touch(n int) bool {
	carry := &b.carry
	if b.walkOnly {
		carry = &b.tree.walkWork().carry
	}
	*carry += n
	if units := *carry / pomLookBytes; units > 0 {
		*carry -= units * pomLookBytes
		ok := b.spend(units)
		if ok {
			b.tree.looks -= units // bytes, not looks: the tests' count of looks is of looks
		}
		return ok
	}
	return !b.tripped || b.walkOnly
}

// importAt is what import n of m is in c, worked out once for this file. A
// first working-out costs its interpolation, and a unit for each byte of
// the coordinates it gives: they may become a key that is kept (an answer
// from a repository is kept by coordinate, for the walk), and what is kept
// must not outgrow what was read. nil: the allowance is used up.
func (b *pomBudget) importAt(c *pomContext, m *pomModel, n int) *pomResolvedImport {
	at := pomImportAt{c, m, n}
	if r, ok := b.resolved[at]; ok {
		return r
	}
	e := m.managed[m.index().imports[n]]
	r := &pomResolvedImport{}
	var ok1, ok2, ok3 bool
	r.group, ok1 = b.interp(c, e.group)
	r.artifact, ok2 = b.interp(c, e.artifact)
	r.version, ok3 = b.interp(c, e.version)
	if !ok1 || !ok2 || !ok3 || !b.spend(len(r.group)+len(r.artifact)+len(r.version)) {
		return nil
	}
	if b.resolved == nil {
		b.resolved = map[pomImportAt]*pomResolvedImport{}
	}
	b.resolved[at] = r
	return r
}

// budget is the allowance for one file of ownBytes bytes.
func (t *pomTree) budget(ownBytes int) *pomBudget {
	return &pomBudget{tree: t, limit: pomWorkFactor * (ownBytes + 1), counted: map[*pomModel]bool{}}
}

// walkBudget is a budget bounded by the walk's allowance alone.
func (t *pomTree) walkBudget() *pomBudget {
	return &pomBudget{tree: t, walkOnly: true}
}

// reach adds the bytes of the poms a question reaches, each once.
func (b *pomBudget) reach(models ...*pomModel) {
	if b.walkOnly {
		return
	}
	for _, m := range models {
		if !b.counted[m] {
			b.counted[m] = true
			b.limit += pomWorkFactor * (m.size + 1)
		}
	}
}

// spend records n looks; false when the file's allowance or the walk's is
// used up.
func (b *pomBudget) spend(n int) bool {
	w := b.tree.walkWork()
	if (!b.walkOnly && b.used+n > b.limit) || w.used+n > w.limit || b.tree.acct.tick(n) {
		b.tripped = true
		return false
	}
	b.used += n
	w.used += n
	b.tree.looks += n
	return true
}

// pomRemoteEntryBytes stands for the size of one entry of a fetched pom,
// whose text the tree never sees: about what one managed entry or property
// takes in a pom.
const pomRemoteEntryBytes = 100

// errPomWorkBound: resolving the pom's managed versions passed pomWorkFactor.
var errPomWorkBound = errors.New("the pom's managed versions could not be resolved within the reader's bound (pomWorkFactor); nothing was taken from it")

func (t *pomTree) walkWork() *pomWork {
	if t.work == nil {
		t.work = &pomWork{}
	}
	return t.work
}

// allow grants the walk the work that reading n bytes allows.
func (t *pomTree) allow(n int) {
	t.walkWork().limit += pomWorkFactor * (n + 1)
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
	// RemoteNotFound: every repository asked answered that it has none; or
	// none was asked, because the coordinate is not one pom that can be
	// asked for (a source refuses a coordinate a request cannot be built
	// from, as this reader does not ask for a range or a snapshot).
	RemoteNotFound
	// RemoteFailed: a repository did not answer, and nothing is stored for
	// the coordinate. Not an answer (SR-5).
	RemoteFailed
	// RemoteRefused: the only repository that could have it may not be
	// fetched from.
	RemoteRefused
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
// It is the ONE place the source is asked (TestOnlyRemotePomAsksTheSource),
// and each request is counted against the walk's allowance of them
// (walkAccount.mayAsk). bud is charged the bytes of the coordinates (they
// are joined into a key and hashed). declared is asked for only when a
// request is made; cost is what writing its list out took, in entries, and
// is charged too. An answer is kept for the walk while its account has room
// (keep); one that is not kept is asked for again, as another request. A
// request that is not made, and a pom obtained that there is no room to
// keep, are a lookup that failed: not an answer (SR-5).
func (t *pomTree) remotePom(bud *pomBudget, group, artifact, version string, declared func() (list []string, cost int)) (m *pomModel, reason string) {
	if t.remote == nil {
		return nil, ""
	}
	if !bud.touch(len(group) + len(artifact) + len(version)) {
		return nil, model.VersionReasonInheritedLookupFailed
	}
	// A snapshot is whatever was built last, a range is not one pom, and an
	// unresolved reference is not a coordinate: none is asked for.
	if group == "" || artifact == "" || version == "" || strings.Contains(group+artifact+version, "${") ||
		strings.Contains(strings.ToUpper(version), "SNAPSHOT") || strings.ContainsAny(version, "[](),") {
		return nil, model.VersionReasonInheritedNotFound
	}
	key := group + ":" + artifact + ":" + version
	ans, asked := t.remoteAnswers[key]
	if !asked {
		list, cost := declared()
		if !bud.spend(cost/pomCompactPerUnit) || !t.acct.mayAsk() {
			return nil, model.VersionReasonInheritedLookupFailed
		}
		t.acct.pause()
		pom, outcome := t.remote.RemotePom(group, artifact, version, list)
		t.acct.resume()
		ans = remoteAnswer{outcome: outcome}
		kept := memString(key) + walkMapSlot + 2*walkWord
		if outcome == RemoteFound && pom != nil {
			ans.model = pom.modelOf(key)
			// A fetched pom's text is never seen: its entries stand for
			// its size, in the walk's allowance and in the allowance of
			// each file whose question reaches it.
			ans.model.size = pomRemoteEntryBytes * (len(pom.Managed) + len(pom.Properties) + 1)
			t.allow(ans.model.size)
			t.acct.read += ans.model.size
			kept += ans.model.bytes()
			if !t.acct.keep(kept) {
				// It cannot be used without being kept (the models that
				// inherit from it hold it).
				t.warn("a fetched pom was not kept: the walk's memory allowance (walkMemFactor) has no room for it, and what depends on it is reported as not obtained", "remote:"+key, errWalkMemory)
				return nil, model.VersionReasonInheritedLookupFailed
			}
		} else {
			if outcome == RemoteFound {
				ans.outcome = RemoteFailed
			}
			if !t.acct.keep(kept) {
				return ans.reason()
			}
		}
		if t.remoteAnswers == nil {
			t.remoteAnswers = map[string]remoteAnswer{}
		}
		t.remoteAnswers[key] = ans
	}
	return ans.reason()
}

// errWalkMemory: the walk's memory allowance has no room for a structure a
// file cannot be read without.
var errWalkMemory = errors.New("the walk's memory allowance (walkMemFactor times what was read) has no room for it")

// reason is the answer as its callers take it.
func (ans remoteAnswer) reason() (m *pomModel, reason string) {
	switch ans.outcome {
	case RemoteFound:
		return ans.model, ""
	case RemoteFailed:
		return nil, model.VersionReasonInheritedLookupFailed
	case RemoteRefused:
		return nil, model.VersionReasonInheritedRepositoryRefused
	}
	return nil, model.VersionReasonInheritedNotFound
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
	return &pomTree{root: filepath.Clean(root), logger: logger, models: map[string]*pomModel{}, acct: newWalkAccount(logger)}
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
	// size is the pom's size in bytes (for a fetched one, what its entries
	// stand for): what reading it allows.
	size int
	// idx is index()'s answer.
	idx *pomIndex
}

// pomModelBytes is the fixed part of what a pom's model and its index keep:
// the two structs (the headers of their strings, slices and maps) and the
// parent reference, 520 bytes, to the next multiple of 128 (the allocator's
// class). pomEntryBytes is the fixed part of one managed entry:
// its three string headers and flag in the model, and in the index its
// place in one of the lists or maps, with a key of its own.
const (
	pomModelBytes = 640
	pomEntryBytes = 3*walkStringHeader + walkWord + 2*walkStringHeader + 3*walkWord + walkMapSlot
)

// bytes is what the model keeps for the walk, its index included (the index
// is built later and its size is known now: an entry's place in it and a
// key made of the entry's own coordinates).
func (m *pomModel) bytes() int {
	n := pomModelBytes + len(m.path) + len(m.group) + len(m.artifact) + len(m.version) + len(m.servedBy)
	if m.parent != nil {
		n += len(m.parent.group) + len(m.parent.artifact) + len(m.parent.version) + len(m.parent.relativePath)
	}
	n += memStringMap(m.props)
	for _, e := range m.managed {
		n += pomEntryBytes + 2*(len(e.group)+len(e.artifact)+len(e.version))
	}
	n += memStrings(m.modules)
	for _, r := range m.repositories {
		n += 5*walkStringHeader + len(r.URL) + len(r.ManifestPath) // the entry, and its place in fetchable
	}
	return n
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
	live, gaps := make([]int, 0, len(idx.live)), make([]string, 0, len(idx.live)+1)
	carry := ""
	for k, n := range idx.live {
		if idx.dead[n] {
			carry = worseReason(carry, idx.deadReason[n])
			continue
		}
		live, gaps = append(live, n), append(gaps, worseReason(idx.gapBefore[k], carry))
	}
	gaps = append(gaps, worseReason(idx.gapBefore[len(idx.live)], carry))
	idx.live, idx.gapBefore, idx.stale = live, gaps, false
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
	idx.dead, idx.deadReason = make([]bool, len(idx.imports)), make([]string, len(idx.imports))
	idx.gapBefore = make([]string, len(idx.imports)+1)
	m.idx = idx
	return idx
}

// candidates are the reference-keyed entries that can be the answer to a
// question about group:artifact, in the order written: the ones filed under
// that artifact or that group, and the ones with a reference in both
// halves. A coordinate with a colon in it is not told apart from the colon
// between the halves, so a question that has one compares them all, as the
// comparison of whole keys always did.
func (idx *pomIndex) candidates(group, artifact string) []int {
	if len(idx.dynamic) == 0 {
		return nil
	}
	if strings.Contains(group, ":") || strings.Contains(artifact, ":") {
		return idx.dynamic
	}
	a, g, both := idx.dynByArtifact[artifact], idx.dynByGroup[group], idx.dynBoth
	switch {
	case len(g) == 0 && len(both) == 0:
		return a
	case len(a) == 0 && len(both) == 0:
		return g
	case len(a) == 0 && len(g) == 0:
		return both
	}
	out := make([]int, 0, len(a)+len(g)+len(both))
	for len(a)+len(g)+len(both) > 0 {
		next := &a
		for _, l := range []*[]int{&g, &both} {
			if len(*l) > 0 && (len(*next) == 0 || (*l)[0] < (*next)[0]) {
				next = l
			}
		}
		out = append(out, (*next)[0])
		*next = (*next)[1:]
	}
	return out
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
	m.size = len(content)
	t.acct.readFile(path, len(content))
	// What the pom says is kept for the walk, and is registered first. A pom
	// there is no room for is not kept: it answers for nothing, as one that
	// could not be read does, and is named.
	if !t.acct.keep(m.bytes() + 2*memString(path) + 2*walkMapSlot) {
		t.warn("a pom of the tree was not kept: the walk's memory allowance (walkMemFactor) has no room for what it says; it answers for nothing", path, errWalkMemory)
		if t.noRoom == nil {
			t.noRoom = map[string]bool{}
		}
		t.noRoom[path] = true
		return nil
	}
	t.models[path] = m
	t.allow(len(content))
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
func (t *pomTree) chain(m *pomModel) ([]*pomModel, string) {
	out, gap, _ := t.chainWhole(m)
	return out, gap
}

// chainWhole is chain that also says whether the chain was followed to its
// end: a pom with no parent. It is not when a parent could not be obtained
// (in the tree or from a repository), the chain loops, or it is too long.
func (t *pomTree) chainWhole(m *pomModel) (chain []*pomModel, gap string, whole bool) {
	// Building a chain belongs to no one file: its work (the coordinates of
	// each parent asked for) is the walk's.
	bud := t.walkBudget()
	out := []*pomModel{m}
	seen := map[string]bool{m.path: true}
	for cur := m; cur.parent != nil && len(out) < pomChainLimit; {
		var p *pomModel
		if cur.parent.inTree {
			at := filepath.Join(filepath.Dir(cur.path), filepath.FromSlash(cur.parent.relativePath))
			if info, err := os.Lstat(at); err == nil && info.IsDir() {
				at = filepath.Join(at, "pom.xml")
			}
			at = filepath.Clean(at)
			if seen[at] {
				return out, "", false
			}
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
			remote, reason := t.remotePom(bud, cur.parent.group, cur.parent.artifact, cur.parent.version, func() ([]string, int) {
				list := fetchableDeclared(soFar)
				return list, declaredCost(soFar)
			})
			if remote == nil {
				return out, reason, false
			}
			p = remote
		}
		if seen[p.path] {
			return out, "", false
		}
		seen[p.path] = true
		out = append(out, p)
		cur = p
	}
	// The loop ended at a pom with no parent (whole), or at the bound.
	return out, "", len(out) < pomChainLimit
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
	// declaredList is declaredFor's answer, when it is held: the one
	// declaring pom's own list, by reference, or a merged one the walk's
	// account had room for.
	declaredList []string
	declaredDone bool
}

// declaredFor is the repositories this model's chain declares and that may
// be fetched from, and what writing the list out cost, in entries (0 when it
// was held already).
//
// The list belongs to the pom that DECLARES it (pomModel.fetchable) and is
// shared by reference: a chain in which one pom declares repositories, which
// is every module of a reactor whose root declares them, holds that pom's
// list and nothing of its own. Round 5: each effective model kept its own
// copy, the modules times the repositories (83 MB for 574 KB of poms). A
// chain in which two or more poms declare has a list of its own, merged; it
// is kept while the walk's account has room and written out again for each
// request when it has not, which is charged.
func (c *pomContext) declaredFor() (list []string, cost int) {
	if c.declaredDone {
		return c.declaredList, 0
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
		c.declaredDone = true
		return nil, 0
	case 1:
		c.declaredList, c.declaredDone = only.fetchable, true
		return c.declaredList, 0
	}
	list, cost = fetchableDeclared(c.chain), declaredCost(c.chain)
	if c.tree.acct.keep(walkSliceHeader + walkStringHeader*len(list)) {
		c.declaredList, c.declaredDone = list, true
	}
	return list, cost
}

// declaredCost is how many entries fetchableDeclared(chain) passes.
func declaredCost(chain []*pomModel) int {
	n := 0
	for _, m := range chain {
		n += len(m.fetchable)
	}
	return n
}

// context is the effective model of m, built once per tree: a BOM's used to
// be built again for every dependency looked up in it.
func (t *pomTree) context(m *pomModel) *pomContext {
	if c, ok := t.contexts[m]; ok {
		return c
	}
	c := t.newContext(m)
	// Kept while the walk's account has room; built again for each question
	// when it has not (the chain's poms are kept, so that is a walk along
	// them).
	if t.acct.keep(pomContextBytes + walkWord*len(c.chain)) {
		if t.contexts == nil {
			t.contexts = map[*pomModel]*pomContext{}
		}
		t.contexts[m] = c
	}
	return c
}

// pomContextBytes is what an effective model keeps beside its chain: the
// struct (a pointer, three slice or string headers of the chain and list,
// seven strings' headers whose bytes are the models') and its place in the
// tree's map.
const pomContextBytes = 3*walkSliceHeader + 8*walkStringHeader + 4*walkWord + walkMapSlot

func (t *pomTree) newContext(m *pomModel) *pomContext {
	chain, gap := t.chain(m)
	c := &pomContext{tree: t, chain: chain, chainGap: gap, artifact: m.artifact}
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
	out, _ := pomSubstituteWork(s, resolve)
	return out
}

// pomSubstituteWork is pomSubstitute, and how many bytes it read and wrote
// doing it (pomLookBytes of them are a unit of the work bound).
func pomSubstituteWork(s string, resolve func(name string) (string, bool)) (out string, work int) {
	if len(s) > pomInterpolatedLimit || !strings.Contains(s, "${") {
		return s, 0
	}
	original := s
	for pass := 0; pass < pomSubstitutePasses && strings.Contains(s, "${"); pass++ {
		work += len(s)
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
		work += sb.Len()
		if over {
			return original, work
		}
		if !changed {
			break
		}
		s = sb.String()
	}
	return s, work
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
// dependencyManagement gives a coordinate. It carries what the question
// costs (bud), the BOMs it has already searched, and why it could not be
// answered in full.
type pomLookup struct {
	bud *pomBudget
	// searched: a BOM is searched once per question. Searched again it
	// answers the same, so a second route to it (a diamond, or a cycle of
	// poms that import each other) is skipped. Without this the depth limit
	// was the only bound, and each level multiplied by the number of
	// imports: two poms importing each other twice were 2^32 calls (review
	// round 1).
	searched map[*pomModel]bool
	// gap is the worst reason a pom that might have answered could not be
	// obtained: a parent of a chain, or a BOM.
	gap string
}

// managed is the version dependencyManagement gives group:artifact in this
// pom's effective model: an explicit entry anywhere in the chain (the
// nearest pom wins, and in a pom the first entry), then the BOMs the chain
// imports, in order. found is false when nothing in the tree manages it, or
// the version it is given cannot be resolved.
//
// hosts are the repository hosts the answer came through beyond this
// model's own chain: those of the BOM that supplied it and of that BOM's
// chain. gap is why the answer may not be the whole truth: a pom of this
// chain, or a BOM this question looked for, could not be obtained ("" when
// all were).
func (c *pomContext) managed(bud *pomBudget, group, artifact string) (version string, found bool, hosts []string, gap string) {
	lk := &pomLookup{bud: bud, searched: map[*pomModel]bool{}, gap: c.chainGap}
	version, found, hosts = c.lookup(lk, group, artifact, 0)
	return version, found, hosts, lk.gap
}

func (c *pomContext) lookup(lk *pomLookup, group, artifact string, depth int) (version string, found bool, bomHosts []string) {
	lk.bud.reach(c.chain...)
	key := group + ":" + artifact
	// The key is built here and hashed in every pom of the chain.
	if !lk.bud.touch(len(key) * (1 + len(c.chain))) {
		return "", false, nil
	}
	for _, m := range c.chain {
		idx := m.index()
		best, has := idx.plain[key]
		// An entry whose coordinates hold a reference comes first if it is
		// written first and means this coordinate here. Each one compared
		// is a look, and its interpolation is charged by the byte.
		for _, j := range idx.candidates(group, artifact) {
			if has && j > best {
				break
			}
			if !lk.bud.spend(1) {
				return "", false, nil
			}
			g, okG := lk.bud.interp(c, m.managed[j].group)
			a, okA := lk.bud.interp(c, m.managed[j].artifact)
			if !okG || !okA {
				return "", false, nil
			}
			// Joined and compared: the candidate's own bytes.
			if !lk.bud.touch(len(g) + len(a)) {
				return "", false, nil
			}
			if g+":"+a == key {
				best, has = j, true
				break
			}
		}
		if has {
			v, ok := lk.bud.interp(c, m.managed[best].version)
			if !ok {
				return "", false, nil
			}
			return v, v != "" && !strings.Contains(v, "${"), nil
		}
	}
	if depth >= pomChainLimit {
		return "", false, nil
	}
	for _, m := range c.chain {
		if v, ok, via, done := c.lookupImports(lk, m, group, artifact, depth); done {
			return v, ok, via
		}
	}
	return "", false, nil
}

// lookupImports asks the BOMs that m imports, in order. done: the question
// is settled (answered, or cut short by the bound).
//
// The question takes the reason of every import it passes that leads
// nowhere, and of none it does not reach: the ones in front of its answer
// (idx.gapBefore, and what it finds dead itself), or all of them when no
// import answers.
func (c *pomContext) lookupImports(lk *pomLookup, m *pomModel, group, artifact string, depth int) (version string, found bool, bomHosts []string, done bool) {
	idx := m.index()
	if idx.stale && idx.inUse == 0 {
		// Dropping the dead ones is a pass over the list: charged, so that
		// a pom whose imports die one question at a time does not make
		// each question cost the whole list for nothing.
		if !lk.bud.spend(len(idx.live) / pomCompactPerUnit) {
			return "", false, nil, true
		}
		idx.compact()
	}
	idx.inUse++
	defer func() { idx.inUse-- }()
	for k := 0; k < len(idx.live); k++ {
		n := idx.live[k]
		c.tree.importSteps++
		if idx.dead[n] {
			// Found so by a question inside this one, and passed.
			lk.gap = worseReason(lk.gap, idx.deadReason[n])
			continue
		}
		e := m.managed[idx.imports[n]]
		plain := idx.importKeys[n] != ""
		bg, ba, bv := e.group, e.artifact, e.version
		var at *pomResolvedImport
		if !plain {
			// Its coordinates hold a reference: what they are depends on
			// the model being built. Worked out once for the file being
			// read (charged there), and each passing of it is a look.
			if at = lk.bud.importAt(c, m, n); at == nil {
				return "", false, nil, true
			}
			if !lk.bud.spend(1) {
				return "", false, nil, true
			}
			if at.dead {
				lk.gap = worseReason(lk.gap, at.reason)
				continue
			}
			bg, ba, bv = at.group, at.artifact, at.version
		} else if !lk.bud.spend(1) {
			// One import looked at is one look: a pom with n version-less
			// dependencies and n imports looks at n squared of them, and
			// ends at the bound.
			return "", false, nil, true
		}
		bom, ambiguous := c.tree.byCoordinates(lk.bud, bg, ba, bv)
		reason := ""
		if bom == nil && !ambiguous {
			// Not in the tree: asked of a repository. An AMBIGUOUS one is
			// not: the tree holds two poms under that name, and a
			// repository's would be a third candidate, not the answer.
			bom, reason = c.tree.remotePom(lk.bud, bg, ba, bv, c.declaredFor)
		}
		if lk.bud.tripped {
			return "", false, nil, true
		}
		if bom == nil {
			// It leads nowhere: for every question of this walk when its
			// coordinates are written out, for this file's when they depend
			// on the model. Not looked for again.
			lk.gap = worseReason(lk.gap, reason)
			if plain {
				idx.dead[n], idx.deadReason[n], idx.stale = true, reason, true
			} else {
				at.dead, at.reason = true, reason
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
		if v, ok, via := bc.lookup(lk, group, artifact, depth+1); ok {
			lk.gap = worseReason(lk.gap, idx.gapBefore[k])
			return v, true, mergeHosts(chainHosts(bc.chain), via), true
		}
		if lk.bud.tripped {
			return "", false, nil, true
		}
	}
	lk.gap = worseReason(lk.gap, idx.gapBefore[len(idx.live)])
	return "", false, nil, false
}

// pomCompactPerUnit is how many entries of an import list compact passes
// over for one unit: a step of that pass is a comparison and a copy, a few
// nanoseconds against a unit's half microsecond.
const pomCompactPerUnit = 64

// byCoordinates returns the one pom in the tree with these coordinates, or
// nil when there is none or more than one (SR-6: fixture and template poms
// share coordinates with real ones; neither is chosen). ambiguous says
// which: more than one.
//
// bud is charged the bytes of the coordinates: they are scanned for a
// reference, joined into a key and hashed, each time.
func (t *pomTree) byCoordinates(bud *pomBudget, group, artifact, version string) (m *pomModel, ambiguous bool) {
	if !bud.touch(len(group)+len(artifact)+len(version)) ||
		group == "" || artifact == "" || version == "" || strings.Contains(group+artifact+version, "${") {
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
			// A key is the pom's coordinates as interpolated, which can be
			// far longer than the pom (a property of a parent, three times
			// over). One there is no room for is not kept: the pom is then
			// not found by its coordinates, and that is said.
			if !t.acct.keep(memString(key) + walkMapSlot + walkSliceHeader + walkStringHeader) {
				t.warn("a pom of the tree is not indexed by its coordinates: the walk's memory allowance (walkMemFactor) has no room for them; an import or parent that names it is looked for in a repository", path, errWalkMemory)
				return nil
			}
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
	return &pomTree{root: t.root, logger: t.logger, models: t.models, byCoord: t.byCoord, pomFiles: t.pomFiles, work: t.walkWork(), acct: t.acct, noRoom: t.noRoom}
}

// settleBounded gives the dependencies read from the pom at path what the tree
// knows: a version written as a property the pom inherits, and, for a
// dependency with no version at all, the version dependencyManagement gives
// it. The second is marked SourceManaged. What the tree cannot answer is
// left as it was, for the remote resolver.
//
// An error (errPomWorkBound): the work bound cut it short. What was settled
// is then a part, and the caller takes nothing from the pom.
func (t *pomTree) settleBounded(path string, deps []model.LibyearDep) ([]model.LibyearDep, error) {
	t.acct.enter()
	defer t.acct.leave()
	if t.acct.fired {
		return nil, errWalkBackstop
	}
	m := t.model(path)
	if m == nil {
		if t.noRoom[filepath.Clean(path)] {
			// Not a pom that says nothing: one that was not kept. Its
			// dependencies as written are not what it declares.
			return nil, errWalkMemory
		}
		return deps, nil
	}
	// The pom's own allowance: its bytes, and those of every pom its
	// questions reach (pomBudget.reach).
	bud := t.budget(0)
	out := t.settleDeps(m, deps, bud)
	if t.acct.fired {
		return nil, errWalkBackstop
	}
	if bud.tripped {
		t.warn("a pom's managed versions could not be resolved within the reader's bound; nothing was taken from it", path, errPomWorkBound)
		return nil, errPomWorkBound
	}
	return out, nil
}

func (t *pomTree) settleDeps(m *pomModel, deps []model.LibyearDep, bud *pomBudget) []model.LibyearDep {
	c := t.context(m)
	// What the file may cost includes the poms of its own chain, from the
	// start: a version written as a property is interpolated before any
	// question is asked.
	bud.reach(c.chain...)
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
			if v, _ := bud.interp(c, deps[i].Version); !strings.Contains(v, "${") {
				deps[i].Version = v
				deps[i].Requirement = deps[i].Name + ":" + v
				// The property may have come from a fetched parent.
				deps[i].VersionHosts = chainHosts(c.chain)
			}
		case deps[i].Version == "":
			v, found, via, g := c.managed(bud, group, artifact)
			gap = worseReason(gap, g)
			if found {
				deps[i].Version = v
				deps[i].VersionSource = SourceManaged
				// The hosts this version came through (0.29.185): the
				// fetched poms of this pom's own chain, and of the BOM
				// that supplied it.
				deps[i].VersionHosts = mergeHosts(chainHosts(c.chain), via)
			} else if gap != "" {
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
