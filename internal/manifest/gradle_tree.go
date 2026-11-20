// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The Gradle tree reader (v0.29.193; summary/70, 4.6). A build file's
// versions, and parts of its names, are often held elsewhere in the build:
// gradle.properties, settings.gradle, the build files above it, a file it
// applies, a version catalogue, constants in buildSrc. This reads those, in
// the clone only, and hands a build file's reader what they say. Nothing is
// run and nothing outside the clone is opened.

// Bounds on what one clone's tree may cost. A build has a handful of these
// files; a clone of fixtures can have thousands, and the reader is not owed
// to them.
const (
	// gradleApplyDepth bounds "apply from" inside an applied file.
	gradleApplyDepth = 8
	// gradleBuildSrcFiles bounds the source files read under one buildSrc.
	gradleBuildSrcFiles = 200
)

type gradleTree struct {
	root   string
	logger *slog.Logger
	// pom is the Maven tree of the same walk: a platform is a BOM, read and
	// obtained exactly as a pom's import is.
	pom *pomTree
	// repos are the repositories each Gradle file read declares, by its
	// path relative to the clone.
	repos map[string][]gradleRepo
	// dirs is what is known at each directory; belows what a directory
	// directly below each directory starts from (below). Each holds
	// pointers into shared chains, never a copy of what is above it.
	dirs   map[string]*gradleKnown
	belows map[string]*gradleKnown
	// unreadParts are the files that could not be read within the reader's
	// bounds, in the order they were met (noteUnread); unreadWarned the
	// ones already said in the log.
	unreadParts  []string
	unreadWarned map[string]bool
	// work is the walk's work counter (gradleWalkWork); counted the files
	// whose bytes are in its allowance.
	work    *gradleWalkWork
	counted map[string]bool
	// memo holds, for each applied script, what reading it gave, with what
	// it took from outside (gradleMemo). memoBytes is what the memo holds
	// and memoLimit what it may: the bytes of Gradle files read.
	memo                 map[string][]*gradleMemo
	memoBytes, memoLimit int
	// readCost is what the last whole reading of each applied script cost.
	readCost map[string]int
	// bundles holds, for each applied script, what applying it left behind
	// (gradleBundle): the script's own layer and those of every script it
	// applies, as one shared run.
	bundles map[string][]*gradleBundle
	// pastBound are the files found past their own work bound. One is read
	// once: reached again (as the build file above another directory, after
	// being read as a build file of its own) it is not read, and costs
	// nothing.
	pastBound map[string]bool
	// walkFactor is the walk's allowance in units per byte of Gradle file
	// read: gradleWalkFactor, set otherwise by tests.
	walkFactor int
	// roots caches rootOf.
	roots map[string]string
	// fullReads counts the files read in full (tokenised and walked) and
	// opens the files opened, for tests: a count where a clock cannot tell.
	fullReads int
	opens     int
	// scanned counts the bytes tokenised, for tests.
	scanned int
	// toks are the tokens of the files read, kept while the walk's account
	// has room (scan).
	toks map[string]gradleKeptTokens
	// applying is the applied script being read (readApplied), "" otherwise.
	applying string
	// acct is the walk's account (walkAccount), the Maven tree's too.
	acct *walkAccount
}

// gradleKnown is what is known at one directory of the tree. Every field is
// a pointer into a chain shared with the directories above: a directory
// that adds nothing holds what its parent holds, and one that adds holds
// one more link.
type gradleKnown struct {
	vars *gradleVars
	libs *gradleLibs
	// platforms are the BOMs the build files above apply to the projects
	// below them (allprojects, subprojects).
	platforms *gradleList[gradleDecl]
	// shared are the repositories that serve this directory's dependencies
	// and may be fetched from, declared by the build files above (for all
	// their projects) and the settings file, the nearest first.
	shared *gradleList[string]
	// unread names a file this directory's build depends on (a settings
	// file, a build file above, an applied script, a buildSrc source, a
	// catalogue) that could not be read within the reader's bounds; "" when
	// all were. A build file under it is then not read: see declared.
	unread string
}

// gradleList is a list built by adding in front of, or after, the list of
// the directory above, without copying that list: the directories times
// the entries they inherit would not be linear in what was read.
type gradleList[T any] struct {
	before, after []T
	parent        *gradleList[T]
	n             int
}

func (l *gradleList[T]) len() int {
	if l == nil {
		return 0
	}
	return l.n
}

// with is l with items in front of it and after it.
func (l *gradleList[T]) with(before, after []T) *gradleList[T] {
	if len(before) == 0 && len(after) == 0 {
		return l
	}
	return &gradleList[T]{before: before, after: after, parent: l, n: l.len() + len(before) + len(after)}
}

// flat is the list written out: for each link, what it put in front, the
// list above, what it put after.
func (l *gradleList[T]) flat() []T {
	if l == nil {
		return nil
	}
	out := make([]T, 0, l.n)
	var afters [][]T
	for ; l != nil; l = l.parent {
		out = append(out, l.before...)
		afters = append(afters, l.after)
	}
	for i := len(afters) - 1; i >= 0; i-- {
		out = append(out, afters[i]...)
	}
	return out
}

// gradleMemo is one reading of an applied script: what it took from outside
// (reads), and what it gave. A script applied by many projects is read once
// for all whose scope answers the same for the names it looked up.
type gradleMemo struct {
	reads     map[string]gradleRead
	own       map[string]string
	gone      map[string]bool
	applies   []string
	overBound bool
}

// errGradleBuildPartUnread: a file the build file depends on is too large
// or passed the work bound.
var errGradleBuildPartUnread = errors.New("a file this build file depends on could not be read within the reader's bounds (too large, or past gradleWorkFactor); nothing was taken from the build file")

// noteUnread records that a file of the build could not be read within the
// reader's bounds, and says so in the log by name, once per file and walk.
func (t *gradleTree) noteUnread(path, why string) {
	t.unreadParts = append(t.unreadParts, path)
	if t.unreadWarned[path] {
		return
	}
	if t.unreadWarned == nil {
		t.unreadWarned = map[string]bool{}
	}
	t.unreadWarned[path] = true
	if t.logger != nil {
		t.logger.Warn("a file of the Gradle build could not be read within the reader's bounds; the build files that depend on it are not read",
			"path", path, "why", why)
	}
}

// whyUnread says which bound ended a read.
func whyUnread(b *gradleBuild) string {
	if b.walkBound && b.shared != nil && b.shared.acct != nil && b.shared.acct.fired {
		return "the walk's wall-clock backstop ended the build-tree readers (a path of theirs was not accounted for)"
	}
	if b.walkBound {
		return "past the walk's work allowance (gradleWorkFactor times the Gradle files read)"
	}
	return "past the work bound (gradleWorkFactor)"
}

// scan reads content with the walk's counter, noting nothing: for a read
// whose only use is the list of files the content applies.
//
// A file's tokens are made once for the walk and kept, while the walk's
// account has room for them (a token is gradleTokBytes, and a file of line
// ends is one for every byte: such a file is tokenised again for each
// reading, and each is charged its bytes).
func (t *gradleTree) scan(path, content string, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	t.fullReads++
	kt, kept := t.toks[path]
	if !kept {
		t.scanned += len(content)
		kt = gradleKeptTokens{toks: gradleTokens(content), size: len(content)}
		// Kept for a script that is applied, which is the file read again
		// and again (a build file is read for itself, and once more for the
		// directories below it). The tokens hold the file's text (their
		// strings are parts of it).
		if path == t.applying && t.acct.keep(walkSliceHeader+walkWord+gradleTokBytes*cap(kt.toks)+memString(content)+memString(path)+walkMapSlot) {
			if t.toks == nil {
				t.toks = map[string]gradleKeptTokens{}
			}
			t.toks[path] = kt
		}
	}
	return readGradleTokens(kt.toks, kt.size, !kept, outer, libs, t.work)
}

// gradleKeptTokens are a file's tokens and its size in bytes.
type gradleKeptTokens struct {
	toks []gradleTok
	size int
}

// gradleTokBytes is what one token keeps: its kind and flag (a word, with
// padding) and its text's header.
const gradleTokBytes = 2*walkWord + walkStringHeader

// scanFile is scan for the file at path, which is read once if it is past
// its own bound: found so, it is not read again on this walk (the work
// bound is a fact about the file's text). The walk is charged a file's
// bound for one reading of it and is allowed it once; read again for each
// directory below it, a file past its bound took what it cost out of the
// allowance of files that had nothing to do with it (round 4).
func (t *gradleTree) scanFile(path, content string, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	if t.pastBound[path] {
		return &gradleBuild{overBound: true}
	}
	b := t.scan(path, content, outer, libs)
	if b.overBound && !b.walkBound {
		t.pastBound[path] = true
	}
	return b
}

// read is scanFile for a file the tree reads on a build file's behalf: one
// that passes a bound is noted.
func (t *gradleTree) read(path, content string, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	b := t.scanFile(path, content, outer, libs)
	if b.overBound {
		t.noteUnread(path, whyUnread(b))
	}
	return b
}

// readApplied is what reading the applied script at path gives with outer
// known. A reading whose every lookup outside the script is answered by
// outer as it was before is not made again: the result is the same. ok is
// false when the file cannot be read at all.
func (t *gradleTree) readApplied(path string, outer *gradleVars) (*gradleMemo, bool) {
next:
	for _, e := range t.memo[path] {
		for name, r := range e.reads {
			v, ok, layers := outer.get(name)
			if !t.work.spend(layers) {
				break next // the walk's allowance is used up: read, and be told so
			}
			if ok != r.ok || v != r.value {
				continue next
			}
		}
		if e.overBound {
			t.noteUnread(path, "past the work bound (gradleWorkFactor)")
		}
		return e, true
	}
	// A script that has been read before is not opened again when what is
	// left of the walk's allowance is less than reading it cost: it is not
	// read, and that is noted. Each project brings a little allowance of
	// its own, never enough for a large script; opened and split into
	// tokens for each all the same, the script cost its size per project.
	if cost, read := t.readCost[path]; read && t.work.limit-t.work.used < cost {
		t.noteUnread(path, "past the walk's work allowance (gradleWorkFactor times the Gradle files read)")
		return &gradleMemo{overBound: true}, true
	}
	// A script whose tokens are kept is not opened again: it is read from
	// them.
	content := ""
	if _, kept := t.toks[path]; !kept {
		var ok bool
		if content, ok = t.content(path); !ok {
			return nil, false
		}
	}
	t.applying = path
	b := t.read(path, content, outer, nil)
	t.applying = ""
	if !b.walkBound {
		if t.readCost == nil {
			t.readCost = map[string]int{}
		}
		t.readCost[path] = min(b.work, b.workLimit)
	}
	e := &gradleMemo{reads: b.reads, applies: b.applies, overBound: b.overBound}
	e.own, e.gone = b.result()
	if b.walkBound {
		return e, true // the walk's allowance ran out: not a fact about the script, not kept
	}
	// What the entry holds, as text: the names and values. An entry's text
	// is no more than its script's, so the first reading of a script is
	// always kept, and the map overhead on top is a constant factor.
	size := 0
	for k, r := range e.reads {
		size += len(k) + len(r.value)
	}
	for k, v := range e.own {
		size += len(k) + len(v)
	}
	for k := range e.gone {
		size += len(k)
	}
	for _, a := range e.applies {
		size += len(a)
	}
	// Kept while the memo is no larger than the files read: many scripts
	// each read with many different scopes are read again, not remembered.
	// (What the walk's account registers is the entry as it is held: its
	// text and its maps' tables.)
	held := gradleMemoEntryBytes + memMap(len(e.reads), 2*walkStringHeader+walkWord) + memMap(len(e.own), 2*walkStringHeader) +
		memMap(len(e.gone), walkStringHeader+walkWord) + walkStringHeader*len(e.applies)
	if t.memoBytes+size <= t.memoLimit && t.acct.keep(size+held) {
		if t.memo == nil {
			t.memo = map[string][]*gradleMemo{}
		}
		t.memo[path] = append(t.memo[path], e)
		t.memoBytes += size
	}
	return e, true
}

// firstUnread is the first file noted since mark, or earlier when there is
// none.
func (t *gradleTree) firstUnread(mark int, earlier string) string {
	if earlier == "" && len(t.unreadParts) > mark {
		return t.unreadParts[mark]
	}
	return earlier
}

func newGradleTree(root string, logger *slog.Logger) *gradleTree {
	return (&gradleTree{root: filepath.Clean(root), logger: logger, dirs: map[string]*gradleKnown{}, belows: map[string]*gradleKnown{},
		work: &gradleWalkWork{}, counted: map[string]bool{}, pastBound: map[string]bool{}, walkFactor: gradleWalkFactor, acct: newWalkAccount(logger)}).withAccount()
}

// withAccount gives the tree's work counter the tree's account.
func (t *gradleTree) withAccount() *gradleTree {
	t.work.acct = t.acct
	return t
}

// gradleDeclared is the roster's reader for build.gradle and
// build.gradle.kts.
func gradleDeclared(w *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
	return w.gradle.declared(path)
}

func (t *gradleTree) within(path string) bool {
	rel, err := filepath.Rel(t.root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// content reads a file of the tree: inside the clone and a regular file (a
// symbolic link is refused, as for a pom).
func (t *gradleTree) content(path string) (string, bool) {
	path = filepath.Clean(path)
	if !regularBuildFile(t.root, path) {
		if buildFileTooLarge(path) {
			t.noteUnread(path, "larger than a build file is (MaxBuildFileBytes)")
		}
		return "", false
	}
	data, err := ReadManifest(path)
	if err != nil {
		if t.logger != nil {
			t.logger.Warn("a file of the Gradle build could not be read: what it assigns is not known", "path", path, "error", err)
		}
		return "", false
	}
	t.opens++
	// What the walk may spend, and what its memo may hold, grow with each
	// file read, once.
	if !t.counted[path] {
		t.counted[path] = true
		t.acct.readFile(path, len(data))
		t.work.limit += t.walkFactor * (len(data) + 1)
		t.memoLimit += len(data) + gradleMemoPerFile
	}
	return string(data), true
}

// declared reads the build file at path with what its tree knows. An
// error: the file is larger than a build file is, or reading it passed the
// reader's work bound; nothing is taken from such a file.
func (t *gradleTree) declared(path string) ([]model.LibyearDep, error) {
	t.acct.enter()
	defer t.acct.leave()
	if t.acct.fired {
		return nil, errWalkBackstop
	}
	deps, err := t.declaredIn(path)
	if t.acct.fired {
		return nil, errWalkBackstop
	}
	return deps, err
}

func (t *gradleTree) declaredIn(path string) ([]model.LibyearDep, error) {
	path = filepath.Clean(path)
	content, ok := t.content(path)
	if !ok {
		if buildFileTooLarge(path) {
			return nil, errBuildFileTooLarge
		}
		return nil, nil
	}
	dir := filepath.Dir(path)
	known := t.at(dir)
	mark := len(t.unreadParts)
	// Read once for the files it applies. A file that passes its bound here
	// would pass it again below, and is not read twice.
	pre := t.scanFile(path, content, known.vars, nil)
	vars := t.applied(pre.applies, dir, known.vars, 0, newGradleSeen(path), nil)
	// A file this one depends on could not be read within the bounds: a
	// settings file, a build file above, a script it applies, a buildSrc
	// source, a catalogue. What it would have assigned (a version, a
	// platform, a repository) is not known, and a build read without it
	// reads as a whole one with other versions. So nothing is taken from
	// this file, and it is reported as a manifest that could not be read,
	// naming the file (0.29.209; such a file was dropped without a word).
	if part := t.firstUnread(mark, known.unread); part != "" {
		rel, err := filepath.Rel(t.root, part)
		if err != nil {
			rel = filepath.Base(part)
		}
		return nil, fmt.Errorf("%w: %s", errGradleBuildPartUnread, filepath.ToSlash(rel))
	}
	b := pre
	if !pre.overBound {
		b = t.scanFile(path, content, vars, known.libs)
	}
	if b.overBound {
		if t.logger != nil {
			t.logger.Warn("a Gradle build file could not be read within the reader's bound; nothing was taken from it",
				"path", path, "bytes", len(content), "work_factor", gradleWorkFactor, "why", whyUnread(b))
		}
		return nil, errGradleWorkBound
	}
	t.noteRepositories(path, b)
	if t.pom == nil {
		return b.libyearDeps(), nil
	}
	// The platforms are poms: looking through them is the pom tree's work,
	// which this file's size allows as a pom's does, for the walk and for
	// the file (its own bytes, and those of each BOM it reaches).
	t.pom.allow(len(content))
	bud := t.pom.budget(len(content))
	own := b.platformDecls()
	if len(own)+known.platforms.len() > 0 {
		// Writing out the lists the directories above share is work in
		// proportion to their length, for every file that has a platform.
		if bud.spend(known.platforms.len() + known.shared.len()) {
			// The repositories a BOM of this file may be asked of: the
			// file's own, then the ones shared from above.
			var repos []string
			for _, r := range b.repos {
				repos = append(repos, r.fetchable()...)
			}
			t.applyPlatforms(b, append(append([]gradleDecl{}, own...), known.platforms.flat()...),
				uniqueStrings(append(repos, known.shared.flat()...)), bud)
		}
	}
	if bud.tripped {
		if t.logger != nil {
			t.logger.Warn("a Gradle build file's platforms could not be looked through within the reader's bound; nothing was taken from it",
				"path", path, "work_factor", pomWorkFactor)
		}
		return nil, errPomWorkBound
	}
	return b.libyearDeps(), nil
}

// at is what is known at dir: what is known at the directory above, then
// this directory's gradle.properties, its settings file, its catalogues and
// its buildSrc. The build file of a directory above is known to the ones
// below; a directory's own build file is read by declared.
func (t *gradleTree) at(dir string) *gradleKnown {
	dir = filepath.Clean(dir)
	if k, ok := t.dirs[dir]; ok {
		return k
	}
	k := &gradleKnown{}
	t.dirs[dir] = k // set first: a cycle of directories is impossible, a re-entry is harmless
	if dir != t.root && t.within(dir) {
		// What every directory directly below `up` starts from is the
		// same, so it is worked out once per `up`, and SHARED: this
		// directory holds its parent's chains, and adds links for what it
		// has of its own (0.29.210; it used to copy every variable and
		// accessor of the directories above).
		*k = *t.below(filepath.Dir(dir))
	}
	if !t.within(dir) {
		return k
	}
	mark := len(t.unreadParts)
	base := *k
	defer func() {
		t.keepKnown(dir, k, &base)
		k.unread = t.firstUnread(mark, k.unread)
	}()
	t.buildSrc(k, filepath.Join(dir, "buildSrc"))
	if content, ok := t.content(filepath.Join(dir, "gradle.properties")); ok {
		k.vars = k.vars.push(gradleProperties(content), nil)
	}
	for _, name := range []string{"settings.gradle", "settings.gradle.kts"} {
		p := filepath.Join(dir, name)
		if content, ok := t.content(p); ok {
			pre := t.scanFile(p, content, k.vars, nil)
			if pre.overBound {
				t.noteUnread(p, whyUnread(pre)) // and not read a second time
				continue
			}
			vars := t.applied(pre.applies, dir, k.vars, 0, newGradleSeen(p), nil)
			b := t.read(p, content, vars, nil)
			k.vars = vars.push(b.result())
			t.noteRepositories(p, b)
			// A settings file's repositories are the whole build's.
			var settings []string
			for _, r := range b.repos {
				settings = append(settings, r.fetchable()...)
			}
			k.shared = k.shared.with(nil, settings)
		}
	}
	t.catalogues(k, filepath.Join(dir, "gradle"))
	return k
}

// below is what a directory directly below up starts from: what is known
// at up, then up's own build file, whose ext is the rootProject's (or a
// parent project's) and is visible below. Read once per up; the result is
// shared by every directory below.
func (t *gradleTree) below(up string) *gradleKnown {
	if k, ok := t.belows[up]; ok {
		return k
	}
	above := t.at(up)
	k := &gradleKnown{}
	*k = *above
	mark := len(t.unreadParts)
	for _, name := range []string{"build.gradle", "build.gradle.kts"} {
		p := filepath.Join(up, name)
		if content, ok := t.content(p); ok {
			pre := t.scanFile(p, content, k.vars, nil)
			if pre.overBound {
				t.noteUnread(p, whyUnread(pre)) // and not read a second time
				continue
			}
			vars := t.applied(pre.applies, up, k.vars, 0, newGradleSeen(p), nil)
			b := t.read(p, content, vars, above.libs)
			k.vars = vars.push(b.result())
			t.noteRepositories(p, b)
			var nearer []string
			for _, r := range b.repos {
				if r.shared {
					nearer = append(nearer, r.fetchable()...)
				}
			}
			var platforms []gradleDecl
			for _, d := range b.platformDecls() {
				if d.forSubprojects {
					platforms = append(platforms, d)
				}
			}
			k.shared = k.shared.with(nearer, nil)
			k.platforms = k.platforms.with(nil, platforms)
		}
	}
	t.keepKnown(up, k, above)
	k.unread = t.firstUnread(mark, k.unread)
	t.belows[up] = k
	return k
}

// gradleKnownBytes is the fixed part of what is kept for a directory: the
// struct and its place in a map of the tree, with the directory's path.
const gradleKnownBytes = 5*walkWord + walkStringHeader + walkMapSlot

// keepKnown registers what k, known at or below dir, holds beyond base (what
// was known before this directory's own files were read): the layers,
// catalogue accessors and list links those files added. Everything else in
// k is shared with base, by reference.
//
// When the walk's account has no room, what the files added is NOT kept: k
// is left with what base knows, and the directory is noted as one that
// could not be read within the bounds, so every build file at or below it
// is refused by name (declared), exactly as when one of the directory's
// files is past its work bound. What stays is the struct itself, a
// tombstone: one for a directory on the path of a file that was read.
func (t *gradleTree) keepKnown(dir string, k, base *gradleKnown) {
	if k.vars == base.vars && k.libs == base.libs && k.platforms == base.platforms && k.shared == base.shared {
		return // nothing of its own: the struct alone, as a tombstone is
	}
	n := gradleKnownBytes + len(dir)
	for v := k.vars; v != nil && v != base.vars; v = v.parent {
		n += 4*walkWord + memStringMap(v.own) + memStringSet(v.gone)
	}
	for l := k.libs; l != nil && l != base.libs; l = l.parent {
		n += 2 * walkWord
		for name, decls := range l.own {
			n += memString(name) + walkSliceHeader + walkMapSlot
			for _, d := range decls {
				n += gradleDeclBytes + len(d.group) + len(d.artifact) + len(d.version) + len(d.written)
			}
		}
	}
	for l := k.platforms; l != nil && l != base.platforms; l = l.parent {
		n += 2*walkSliceHeader + 2*walkWord
		for _, d := range append(append([]gradleDecl(nil), l.before...), l.after...) {
			n += gradleDeclBytes + len(d.group) + len(d.artifact) + len(d.version) + len(d.written)
		}
	}
	for l := k.shared; l != nil && l != base.shared; l = l.parent {
		n += 2*walkSliceHeader + 2*walkWord + memStrings(l.before) + memStrings(l.after)
	}
	if t.acct.keep(n) {
		return
	}
	k.vars, k.libs, k.platforms, k.shared = base.vars, base.libs, base.platforms, base.shared
	t.noteUnread(dir, "what its build files assign is past the walk's memory allowance (walkMemFactor)")
}

// gradleDeclBytes and gradleRepoBytes are the fixed parts of one declaration
// and one repository entry kept: their structs (seven string headers, a slice
// header and four flags; four string headers and a flag).
// TestTheAccountsSizesAreTheStructs holds them to the structs.
const (
	gradleDeclBytes = 7*walkStringHeader + walkSliceHeader + 2*walkWord
	gradleRepoBytes = 4*walkStringHeader + walkWord
)

// gradleBundle is what applying one script left behind, under one outer
// scope: the layers of every script applied inside it and its own, as ONE
// run (seg) that every scope it applies in holds by reference.
//
// Round 4: a script applied by k directories that itself applies k small
// scripts gave every directory a layer, and a check, for each small script:
// 99 MB kept for 246 KB of files, and time to match. A run is built once,
// for each outer scope that answers differently, and a directory takes one
// link for it.
//
// It stands for another scope exactly when that scope answers as the one
// it was built under did for every name a reading in it looked for outside
// its own text (reads). That is more than is needed (a name an earlier
// script of the run assigns is never asked of the scope), and never less:
// with those answers equal, every reading in the run is given what it was
// given before, in the same order.
type gradleBundle struct {
	reads map[string]gradleRead
	seg   *gradleVars
	// scripts are the files applied in it, the script itself first: none
	// may have been applied already by the file being read. nest is how
	// many levels of "apply from" it holds below itself.
	scripts []string
	has     map[string]bool
	nest    int
}

// gradleSeen is what the file being read has applied so far: scripts one
// by one, and whole runs by reference.
type gradleSeen struct {
	paths map[string]bool
	runs  []*gradleBundle
}

func newGradleSeen(self string) *gradleSeen {
	return &gradleSeen{paths: map[string]bool{self: true}}
}

// has reports whether p was applied already. Each run looked into is a
// unit of work, charged when work is given.
func (s *gradleSeen) has(p string, work *gradleWalkWork) bool {
	if s.paths[p] {
		return true
	}
	if work != nil {
		work.spend(len(s.runs))
	}
	for _, r := range s.runs {
		if r.has[p] {
			return true
		}
	}
	return false
}

// gradleBundleBuild is a bundle being put together: what the readings in
// it looked for, the scripts applied, and whether it can be kept at all.
type gradleBundleBuild struct {
	reads   map[string]bool
	scripts []string
	nest    int
	// partial: something in it depended on more than the outer scope (a
	// script already applied by the file being read was passed over, the
	// depth bound cut it short, or a file could not be read): it is what
	// THIS file gets, and is not kept for another.
	partial bool
}

func (bb *gradleBundleBuild) note(reads map[string]gradleRead) {
	if bb == nil {
		return
	}
	for name := range reads {
		if bb.reads == nil {
			bb.reads = map[string]bool{}
		}
		bb.reads[name] = true
	}
}

// applied returns outer with what the files named in applies assign, each
// read with what was known before it. Nothing is copied: each script's
// assignments are one more layer, and a script applied before under a
// scope that answered the same is one link to the run it left (applyOne).
// into, when not nil, is the bundle these applications are part of.
func (t *gradleTree) applied(applies []string, dir string, outer *gradleVars, depth int, seen *gradleSeen, into *gradleBundleBuild) *gradleVars {
	vars := outer
	if depth >= gradleApplyDepth {
		if into != nil && len(applies) > 0 {
			into.partial = true
		}
		return vars
	}
	for _, written := range applies {
		p := t.applyPath(written, dir)
		if p == "" {
			continue
		}
		if seen.has(p, t.work) {
			// Applied already by the file being read. Inside the bundle
			// being built that is its own doing, and the same every time;
			// before it, it is this file's, and the bundle is not kept.
			if into != nil && !slices.Contains(into.scripts, p) {
				into.partial = true
			}
			continue
		}
		vars = t.applyOne(p, vars, depth, seen, into)
	}
	return vars
}

// applyOne is vars with what applying the script at p leaves behind.
func (t *gradleTree) applyOne(p string, vars *gradleVars, depth int, seen *gradleSeen, into *gradleBundleBuild) *gradleVars {
next:
	for _, bd := range t.bundles[p] {
		if depth+bd.nest >= gradleApplyDepth {
			continue
		}
		// The check is work: a unit for each script of the run (none may
		// have been applied already by this file), and the layers each
		// name is looked for through.
		if !t.work.spend(len(bd.scripts) * (1 + len(seen.runs))) {
			break
		}
		for _, s := range bd.scripts[1:] {
			if seen.has(s, nil) {
				continue next
			}
		}
		for name, r := range bd.reads {
			v, ok, layers := vars.get(name)
			if !t.work.spend(layers) {
				break next // the walk's allowance is used up: read, and be told so
			}
			if ok != r.ok || v != r.value {
				continue next
			}
		}
		// The run's scripts are now applied by this file: by reference to
		// the run, not one by one (k files that each marked the k scripts
		// of a shared run allocated the pairs).
		seen.runs = append(seen.runs, bd)
		if into != nil {
			into.scripts = append(into.scripts, bd.scripts...)
			into.nest = max(into.nest, 1+bd.nest)
			for name := range bd.reads {
				into.note(map[string]gradleRead{name: {}})
			}
		}
		return vars.pushSeg(bd.seg)
	}
	seen.paths[p] = true
	bb := &gradleBundleBuild{scripts: []string{p}}
	mark, start := len(t.unreadParts), vars
	// Read for the files it applies itself, then, with those known, for
	// what it assigns. With none of its own the second reading is the
	// first.
	first, ok := t.readApplied(p, vars)
	if !ok || first.overBound {
		// Not read (and noted): a second reading would cost its bound again.
		t.bundled(into, bb, true)
		return vars
	}
	bb.note(first.reads)
	before := vars
	vars = t.applied(first.applies, filepath.Dir(p), vars, depth+1, seen, bb)
	e := first
	if vars != before {
		if e, ok = t.readApplied(p, vars); !ok {
			t.bundled(into, bb, true)
			return vars
		}
		bb.note(e.reads)
	}
	vars = vars.push(e.own, e.gone)
	partial := bb.partial || len(t.unreadParts) != mark || e.overBound
	t.bundled(into, bb, partial)
	if !partial {
		t.keepBundle(p, bb, start, vars)
	}
	return vars
}

// bundled folds a bundle that was built (or given up) into the one it is
// part of.
func (t *gradleTree) bundled(into, bb *gradleBundleBuild, partial bool) {
	if into == nil {
		return
	}
	into.scripts = append(into.scripts, bb.scripts...)
	into.nest = max(into.nest, 1+bb.nest)
	into.partial = into.partial || partial
	for name := range bb.reads {
		if into.reads == nil {
			into.reads = map[string]bool{}
		}
		into.reads[name] = true
	}
}

// keepBundle keeps what applying p under start left behind (the layers
// between start and vars), while the memo has room for it.
func (t *gradleTree) keepBundle(p string, bb *gradleBundleBuild, start, vars *gradleVars) {
	bd := &gradleBundle{reads: make(map[string]gradleRead, len(bb.reads)), scripts: bb.scripts, nest: bb.nest, has: make(map[string]bool, len(bb.scripts))}
	for _, s := range bb.scripts {
		bd.has[s] = true
	}
	size := 0
	for name := range bb.reads {
		v, ok, layers := start.get(name)
		if !t.work.spend(layers) {
			return
		}
		bd.reads[name] = gradleRead{value: v, ok: ok}
		size += len(name) + len(v)
	}
	// The scripts' paths are the tree's own strings, held once already:
	// here they are a slice of references.
	size += 2 * gradleMemoRefBytes * len(bb.scripts) // the list and the set
	// The run: the links between vars and start, in their order, on a
	// chain of its own. The maps (and the runs of the scripts inside) are
	// shared, not copied.
	var links []*gradleVars
	held := 0
	for n := vars; n != start && n != nil; n = n.parent {
		links = append(links, n)
	}
	for i := len(links) - 1; i >= 0; i-- {
		bd.seg = &gradleVars{own: links[i].own, gone: links[i].gone, seg: links[i].seg, parent: bd.seg}
		// What a link holds is held by the run: its assignments are
		// counted with it (a run inside it is counted where it was kept).
		// Without this a run kept the assignments of a reading the memo
		// itself had no room for.
		for k, v := range links[i].own {
			size += len(k) + len(v)
		}
		for k := range links[i].gone {
			size += len(k)
		}
		held += memMap(len(links[i].own), 2*walkStringHeader) + memMap(len(links[i].gone), walkStringHeader+walkWord)
	}
	size += gradleBundleLinkBytes * len(links)
	// (What the walk's account registers is the run as it is held: its
	// text, and the tables of its maps and of its layers' maps.)
	held += gradleMemoEntryBytes + memMap(len(bd.reads), 2*walkStringHeader+walkWord) + memMap(len(bd.has), walkStringHeader+walkWord)
	if t.memoBytes+size > t.memoLimit || !t.acct.keep(size+held) {
		return
	}
	if t.bundles == nil {
		t.bundles = map[string][]*gradleBundle{}
	}
	t.bundles[p] = append(t.bundles[p], bd)
	t.memoBytes += size
}

// What the memo counts for the parts of a kept run that are not text: a
// link of the run (a gradleVars and its place), a reference to a script's
// path. And what each file read adds to the memo's allowance beyond its
// bytes: room for a link and a reference at every level of "apply from" a
// script can be nested at (it is part of its own run, of the run of the
// script that applies it, and so on up), so that a run of many small
// scripts, whose text is less than the links that stand for them, is kept.
// The memo is still bounded by what was read: by its bytes and the number
// of its files.
const (
	// gradleMemoEntryBytes is the fixed part of one kept reading or run:
	// its struct (three map headers, a slice or two) and its place in the
	// list of its script.
	gradleMemoEntryBytes  = 128
	gradleBundleLinkBytes = 64
	gradleMemoRefBytes    = 16
	gradleMemoPerFile     = gradleApplyDepth * (gradleBundleLinkBytes + 2*gradleMemoRefBytes)
)

// applyPath resolves the path an "apply from" names: relative to the file
// that applies it, or to the root when it is written from the root
// ($rootDir, ${rootProject.projectDir}). A URL, or a path with another
// variable, is not opened.
func (t *gradleTree) applyPath(written, dir string) string {
	for _, rootVar := range []string{"${rootDir}", "$rootDir", "${rootProject.projectDir}", "$rootProject.projectDir", "${rootProject.rootDir}", "${project.rootDir}"} {
		if rest, ok := strings.CutPrefix(written, rootVar); ok {
			return filepath.Clean(filepath.Join(t.rootOf(dir), filepath.FromSlash(rest)))
		}
	}
	for _, here := range []string{"${projectDir}", "$projectDir", "${project.projectDir}"} {
		if rest, ok := strings.CutPrefix(written, here); ok {
			written = strings.TrimPrefix(rest, "/")
		}
	}
	if written == "" || strings.Contains(written, "$") || strings.Contains(written, "://") {
		return ""
	}
	if filepath.IsAbs(written) {
		return ""
	}
	return filepath.Clean(filepath.Join(dir, filepath.FromSlash(written)))
}

// rootOf is the root of the build dir belongs to: the nearest directory at
// or above it that holds a settings file, or the clone's root.
func (t *gradleTree) rootOf(dir string) string {
	dir = filepath.Clean(dir)
	if r, ok := t.roots[dir]; ok {
		return r
	}
	r := t.rootOfDir(dir)
	if t.roots == nil {
		t.roots = map[string]string{}
	}
	t.roots[dir] = r
	return r
}

func (t *gradleTree) rootOfDir(dir string) string {
	for d := filepath.Clean(dir); t.within(d); d = filepath.Dir(d) {
		for _, name := range []string{"settings.gradle", "settings.gradle.kts"} {
			if info, err := os.Lstat(filepath.Join(d, name)); err == nil && info.Mode().IsRegular() {
				return d
			}
		}
		if d == t.root {
			break
		}
	}
	return t.root
}

// gradleProperties reads a gradle.properties file: name=value or
// name: value, # and ! comments.
func gradleProperties(content string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}
		i := strings.IndexAny(line, "=:")
		if i <= 0 {
			continue
		}
		name, value := strings.TrimSpace(line[:i]), strings.TrimSpace(line[i+1:])
		if name != "" && value != "" {
			out[name] = value
		}
	}
	return out
}

// buildSrc adds the constants declared in a buildSrc directory's sources
// (object Versions { const val kotlin = "1.9" } is Versions.kotlin). The
// files are read twice, so a constant built from one declared in a later
// file is settled.
func (t *gradleTree) buildSrc(k *gradleKnown, dir string) {
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		return
	}
	var files []string
	_ = filepath.WalkDir(filepath.Join(dir, "src"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		switch filepath.Ext(p) {
		case ".kt", ".kts", ".groovy", ".java", ".gradle":
			if len(files) < gradleBuildSrcFiles {
				files = append(files, p)
			}
		}
		return nil
	})
	sort.Strings(files)
	over := map[string]bool{}
	for round := 0; round < 2; round++ {
		for _, p := range files {
			if over[p] {
				continue // past its bound in the first round: not read again
			}
			if content, ok := t.content(p); ok {
				if round == 0 {
					b := t.read(p, content, k.vars, nil)
					over[p] = b.overBound
					k.vars = k.vars.push(b.result())
				} else {
					k.vars = k.vars.push(t.scan(p, content, k.vars, nil).result()) // noted in the first round
				}
			}
		}
	}
}

// catalogues adds the version catalogues of a gradle directory: every
// *.versions.toml, under the name before the suffix (libs.versions.toml is
// "libs").
func (t *gradleTree) catalogues(k *gradleKnown, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".versions.toml")
		if !ok || name == "" {
			continue
		}
		content, ok := t.content(filepath.Join(dir, e.Name()))
		if !ok {
			continue
		}
		vars, libs := readGradleCatalogue(name, content)
		k.vars = k.vars.push(vars, nil)
		if len(libs) > 0 {
			k.libs = &gradleLibs{own: libs, parent: k.libs}
		}
	}
}

// gradleAccessor is the accessor of a catalogue alias: '-', '_' and '.'
// all separate its parts (androidx-core_ktx is androidx.core.ktx).
func gradleAccessor(alias string) string { return gradleAccessorDots.Replace(alias) }

var gradleAccessorDots = strings.NewReplacer("-", ".", "_", ".")

// readGradleCatalogue reads one version catalogue: its versions as
// variables named <name>.versions.<alias>, its libraries and bundles as
// accessors.
func readGradleCatalogue(name, content string) (vars map[string]string, libs map[string][]gradleDecl) {
	vars, libs = map[string]string{}, map[string][]gradleDecl{}
	sections := gradleTOMLSections(content)
	versions := map[string]string{} // by alias as written
	for alias, raw := range sections["versions"] {
		v := gradleCatalogueVersion(gradleTOMLValue(raw))
		versions[alias] = v
		if v != "" {
			vars[name+".versions."+gradleAccessor(alias)] = v
		}
	}
	byAlias := map[string]gradleDecl{}
	for alias, raw := range sections["libraries"] {
		f := gradleTOMLValue(raw)
		var d gradleDecl
		if coord, plain := f[""]; plain { // alias = "g:a:v"
			parts := strings.Split(coord, ":")
			if len(parts) < 2 {
				continue
			}
			d.group, d.artifact = parts[0], parts[1]
			if len(parts) >= 3 {
				d.version = parts[2]
			}
			d.written = coord
		} else {
			d.group, d.artifact = f["group"], f["name"]
			if m := f["module"]; m != "" {
				d.group, d.artifact, _ = strings.Cut(m, ":")
			}
			d.written = d.group + ":" + d.artifact
			switch {
			case f["version.ref"] != "":
				d.version = versions[f["version.ref"]]
				d.written += ":${" + name + ".versions." + f["version.ref"] + "}"
			default:
				sub := map[string]string{}
				for key, v := range f {
					if key == "version" {
						sub[""] = v
					} else if rest, ok := strings.CutPrefix(key, "version."); ok {
						sub[rest] = v
					}
				}
				d.version = gradleCatalogueVersion(sub)
				if d.version != "" {
					d.written += ":" + d.version
				}
			}
		}
		if d.group == "" || d.artifact == "" {
			continue
		}
		if !gradleIsVersion(d.version) {
			d.version = ""
		}
		byAlias[alias] = d
		libs[name+"."+gradleAccessor(alias)] = []gradleDecl{d}
	}
	for alias, raw := range sections["bundles"] {
		var decls []gradleDecl
		for _, member := range gradleTOMLArray(raw) {
			if d, ok := byAlias[member]; ok {
				decls = append(decls, d)
			}
		}
		libs[name+".bundles."+gradleAccessor(alias)] = decls
	}
	return vars, libs
}

// gradleCatalogueVersion is the one version a catalogue's version value
// stands for: the string; or of a rich version the strict or required one,
// or the preferred one when it is alone. A range is not one version.
func gradleCatalogueVersion(f map[string]string) string {
	v := f[""]
	switch {
	case v != "":
	case f["strictly"] != "":
		v = f["strictly"]
	case f["require"] != "":
		v = f["require"]
	default:
		v = f["prefer"]
	}
	if !gradleIsVersion(v) {
		return ""
	}
	return v
}

// gradleTOMLSections reads a catalogue's tables: section -> key -> the raw
// text of its value (a value that opens a table or an array runs to the
// line that closes it). It reads the catalogue's grammar, which is a small
// part of TOML: bare or quoted keys, strings, inline tables, arrays.
func gradleTOMLSections(content string) map[string]map[string]string {
	out := map[string]map[string]string{}
	section := ""
	lines := strings.Split(content, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(gradleTOMLStripComment(lines[i]))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") && !strings.Contains(line, "=") {
			section = strings.TrimSpace(strings.Trim(line, "[]"))
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(line[:eq]), `"'`)
		value := strings.TrimSpace(line[eq+1:])
		// A value that leaves a table or an array open runs on. Whether it
		// is still open is carried from line to line and each line is read
		// once (0.29.209): the whole value so far used to be scanned again,
		// and copied again, at every line, which for an array that never
		// closes is the square of the file.
		if depth, quote := gradleTOMLScan(value, 0, 0); depth > 0 {
			var sb strings.Builder
			sb.WriteString(value)
			for depth > 0 && i+1 < len(lines) {
				i++
				next := strings.TrimSpace(gradleTOMLStripComment(lines[i]))
				sb.WriteByte(' ')
				sb.WriteString(next)
				depth, quote = gradleTOMLScan(" "+next, depth, quote)
			}
			value = sb.String()
		}
		if out[section] == nil {
			out[section] = map[string]string{}
		}
		out[section][key] = value
	}
	return out
}

// gradleTOMLStripComment removes a # comment that is outside a string.
func gradleTOMLStripComment(line string) string {
	var quote byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '#':
			return line[:i]
		}
	}
	return line
}

// gradleTOMLScan reads more of a value: the depth of tables and arrays
// open after it and the quote it ends inside (0 for none), given the two
// before it.
func gradleTOMLScan(value string, depth int, quote byte) (int, byte) {
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			depth--
		}
	}
	return depth, quote
}

// gradleTOMLSplit splits s at each comma outside strings and brackets.
func gradleTOMLSplit(s string) []string {
	var out []string
	depth, from := 0, 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == '\\' && quote == '"' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			depth--
		case c == ',' && depth == 0:
			out = append(out, s[from:i])
			from = i + 1
		}
	}
	return append(out, s[from:])
}

func gradleTOMLString(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1], true
	}
	return "", false
}

// gradleTOMLValue reads a value into its fields: a string is the field "";
// an inline table's keys are fields, a nested table's keys joined with a
// dot (version = { ref = "x" } and version.ref = "x" are both
// "version.ref").
func gradleTOMLValue(raw string) map[string]string {
	out := map[string]string{}
	raw = strings.TrimSpace(raw)
	if s, ok := gradleTOMLString(raw); ok {
		out[""] = s
		return out
	}
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return out
	}
	for _, field := range gradleTOMLSplit(raw[1 : len(raw)-1]) {
		eq := strings.IndexByte(field, '=')
		if eq <= 0 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(field[:eq]), `"'`)
		for sub, v := range gradleTOMLValue(field[eq+1:]) {
			if sub == "" {
				out[key] = v
			} else {
				out[key+"."+sub] = v
			}
		}
	}
	return out
}

// gradleTOMLArray reads an array of strings.
func gradleTOMLArray(raw string) []string {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil
	}
	var out []string
	for _, item := range gradleTOMLSplit(raw[1 : len(raw)-1]) {
		if s, ok := gradleTOMLString(item); ok {
			out = append(out, s)
		}
	}
	return out
}

// applyPlatforms gives each version-less dependency of b the version its
// platforms manage (summary/70, 4.6). A platform is a BOM: one of the
// clone's poms, or one obtained through the walk's PomSource, read in its
// own context as a pom's import is.
//
// A dependency the build gave a version to is not answered for, read or
// not. A dependency no platform manages has no version; when a platform
// could not be obtained it says why (the worst of the reasons), since that
// platform might have managed it. A platform that was obtained whole is
// marked a managed entry: its versions are applied, and it leaves the
// inventory with a note, as a pom's BOM import does. One that was not
// stays a row.
func (t *gradleTree) applyPlatforms(b *gradleBuild, platforms []gradleDecl, declared []string, bud *pomBudget) {
	if t.pom == nil || len(platforms) == 0 {
		return
	}
	type applied struct {
		ctx  *pomContext
		decl gradleDecl
	}
	var boms []applied
	gap := ""
	whole := map[string]bool{} // "g:a:v" of each platform obtained whole
	for _, p := range platforms {
		if p.version == "" {
			continue
		}
		// Finding one platform is a look.
		if !bud.spend(1) {
			return
		}
		bom, ambiguous := t.pom.byCoordinates(bud, p.group, p.artifact, p.version)
		if bom == nil && !ambiguous {
			var reason string
			bom, reason = t.pom.remotePom(bud, p.group, p.artifact, p.version, func() ([]string, int) { return declared, 0 })
			gap = worseReason(gap, reason)
		}
		if bom == nil {
			continue
		}
		c := t.pom.context(bom)
		// Walk the BOM's own imports now, so that whether it was read whole
		// is known before anything is decided from it (a lookup of a
		// coordinate nothing manages visits every import).
		_, _, _, bomGap := c.managed(bud, "\x00", "\x00")
		boms = append(boms, applied{ctx: c, decl: p})
		if bomGap == "" {
			whole[p.group+":"+p.artifact+":"+p.version] = true
		} else {
			gap = worseReason(gap, bomGap)
		}
	}
	for i := range b.decls {
		d := &b.decls[i]
		if d.platform {
			d.managed = whole[d.group+":"+d.artifact+":"+d.version]
			continue
		}
		if d.version != "" || d.versionWritten {
			continue
		}
		for _, bom := range boms {
			// One platform asked about one dependency is one look of the
			// pom tree's work: n dependencies against n platforms end at
			// its bound (declared reports it).
			if !bud.spend(1) {
				return
			}
			v, found, via, g := bom.ctx.managed(bud, d.group, d.artifact)
			gap = worseReason(gap, g)
			if found && gradleIsVersion(v) {
				d.version, d.source = v, SourceManaged
				d.hosts = mergeHosts(chainHosts(bom.ctx.chain), via)
				break
			}
		}
		if d.version == "" {
			d.reason = gap
		}
	}
}

// fetchable is the entry's address when it serves dependencies and may be
// fetched from.
func (r gradleRepo) fetchable() []string {
	if r.NotFetched != "" || r.Kind != RepositoryForDependencies {
		return nil
	}
	return []string{r.URL}
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// noteRepositories records what the Gradle file at path declares.
func (t *gradleTree) noteRepositories(path string, b *gradleBuild) {
	rel, err := filepath.Rel(t.root, path)
	if err != nil || len(b.repos) == 0 {
		return
	}
	n := memString(rel) + walkSliceHeader + walkMapSlot
	for _, r := range b.repos {
		n += gradleRepoBytes + len(r.URL)
	}
	if !t.acct.keep(n) {
		// Not kept, and said: the file's dependencies are read all the
		// same, and its repositories are missing from the record of them.
		if t.logger != nil {
			t.logger.Warn("the repositories a Gradle file declares were not recorded: the walk's memory allowance (walkMemFactor) has no room for them", "path", path, "repositories", len(b.repos))
		}
		return
	}
	if t.repos == nil {
		t.repos = map[string][]gradleRepo{}
	}
	t.repos[filepath.ToSlash(rel)] = b.repos
}

// declaredRepositories lists what every Gradle file read so far declares,
// once per file, use and address.
func (t *gradleTree) declaredRepositories() []DeclaredRepository {
	var out []DeclaredRepository
	for rel, repos := range t.repos {
		for _, r := range repos {
			d := r.DeclaredRepository
			d.ManifestPath = rel
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ManifestPath != b.ManifestPath {
			return a.ManifestPath < b.ManifestPath
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.URL < b.URL
	})
	return out
}
