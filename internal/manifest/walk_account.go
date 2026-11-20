// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"log/slog"
	"time"
)

// walkAccount is the ONE place a walk's build-tree readers (the Maven tree
// and the Gradle tree) account for what is not a single file's reading
// (review round 5, decided as a class after five rounds each found another
// product):
//
//   - MEMORY. Every structure that outlives the reading of one file is
//     registered here before it is kept (keep), in bytes, against an
//     allowance proportional to what the walk has read. Past the allowance
//     a cache is not kept and its answer is worked out again per question
//     (and that work is counted); a structure a file cannot be read without
//     is not kept either, and the file is refused by name. Which of the two
//     each structure does is the table of TestEveryWalkLifetimeFieldIsAccounted,
//     which fails for a field it does not list.
//   - REQUESTS. Every question put to the PomSource goes through
//     pomTree.remotePom, which counts it here against an allowance
//     proportional to what was read.
//   - TIME, as a backstop. The work bounds (pomWork, gradleWalkWork) are the
//     bound on time; they hold for the paths that are counted. The clock here
//     is for a path that is not: see walkBackstopFloor.
type walkAccount struct {
	logger *slog.Logger

	// read is the bytes the walk has read, each file once: the contents of
	// the poms and Gradle files, the bytes of their paths (a path is read
	// from a directory, and is kept), and what a fetched pom's entries stand
	// for. files is how many.
	read, files int
	// kept is the bytes registered; notKept how many structures were
	// refused room.
	kept, notKept int
	// asked is the requests made of the PomSource; askRefused how many were
	// not made because the allowance was used up.
	asked, askRefused int

	// The backstop's clock. now is time.Now, set otherwise by tests. depth
	// counts the readers entered, since is when the outermost was, and spent
	// what earlier entries took. Time inside the PomSource is not the
	// readers' and is not counted (pause, resume).
	now        func() time.Time
	depth      int
	since      time.Time
	spent      time.Duration
	sinceCheck int
	fired      bool
}

func newWalkAccount(logger *slog.Logger) *walkAccount {
	return &walkAccount{logger: logger, now: time.Now}
}

// walkMemFactor is the memory allowance: this many bytes kept for each byte
// read. PROVISIONAL (summary/71, section 8). Its derivation: what MUST be
// kept of a file is what it says, decoded, and the densest thing a build
// file can say is an empty property element of a pom ("<a/>", four bytes),
// which decoded is an entry of a map of strings: two string headers (32
// bytes), the name, and the slot's share of the table (walkMapSlot), about
// 49 bytes: twelve times its text. An assignment of gradle.properties
// ("a=b" and a line end) is the same entry for four bytes. Sixteen is the
// next power of two: every file's own content fits whatever it holds, with a
// quarter to spare, and the caches (effective models, answers of the
// source, the Gradle memo) take what honest files leave, which is most of
// it: TestHonestBuildsAreFarInsideTheMemoryAllowance measures a reactor of
// 600 modules and a publishing build of 1,000 projects, and holds both to a
// quarter of the allowance (they use some 4% and 13%).
const walkMemFactor = 16

// walkMemPerFile is what each file read adds to the memory allowance beyond
// its bytes: the fixed part of what is kept for a file whatever it says (a
// decoded model and its index, an effective model with a chain of up to
// pomChainLimit pointers, a place in each of the maps keyed by path or
// directory, or the Gradle tree's entries for the file and its directory).
// Those are structs of some hundred bytes each and a chain of 256; the
// figure is the next power of two above their sum. PROVISIONAL.
const walkMemPerFile = 1024

// Sizes the estimates are made of (64-bit): a string or interface header, a
// slice header, a pointer or int, and a map slot's share of its table beyond
// the key and value themselves (control bytes, load factor, growth).
const (
	walkStringHeader = 16
	walkSliceHeader  = 24
	walkWord         = 8
	walkMapSlot      = 16
	// walkMapHeader is a map's own header.
	walkMapHeader = 48
)

// memString is what a string field keeps: its header and its bytes. The
// bytes are counted even where two fields share them (substrings of one
// file's text): the estimate errs upward.
func memString(s string) int { return walkStringHeader + len(s) }

// memStrings is what a slice of strings keeps.
func memStrings(l []string) int {
	n := walkSliceHeader
	for _, s := range l {
		n += memString(s)
	}
	return n
}

// memMap is what a map of n entries keeps beside what its keys and values
// point to: its header, and its table, which has room for eight entries
// before it has one and doubles when it is seven eighths full. slot is the
// size of a key and a value in the table; each slot has a control byte too.
// (A map of one short assignment is some 370 bytes: the round's first
// estimate counted the entry alone, and a thousand small maps were kept at
// four times what was registered.)
func memMap(n, slot int) int {
	if n == 0 {
		return 0
	}
	slots := 8
	for slots*7/8 < n {
		slots *= 2
	}
	return walkMapHeader + slots*(slot+1)
}

// memStringMap is what a map from strings to strings keeps.
func memStringMap(m map[string]string) int {
	n := memMap(len(m), 2*walkStringHeader)
	for k, v := range m {
		n += len(k) + len(v)
	}
	return n
}

// memStringSet is what a set of strings keeps.
func memStringSet(m map[string]bool) int {
	n := memMap(len(m), walkStringHeader+walkWord)
	for k := range m {
		n += len(k)
	}
	return n
}

// readFile counts a file the walk read, once: its bytes and its path's.
//
// What is kept for a file whatever it says, in the trees' maps keyed by its
// path (that it was counted, whether it is past its bound, what reading it
// cost, the root of its build), is registered here, with the file: it is
// inside what the file itself allows (walkMemPerFile), so it is never
// refused.
func (a *walkAccount) readFile(path string, bytes int) {
	a.read += bytes + len(path)
	a.files++
	a.kept += walkFileEntries + 2*len(path)
}

// walkFileEntries is the fixed part of those entries: three map slots of a
// string header and a word each, with their share of the tables.
const walkFileEntries = 3 * (walkStringHeader + walkWord + walkMapSlot)

// limit is the memory allowance now.
func (a *walkAccount) limit() int {
	return walkMemFactor*a.read + walkMemPerFile*a.files
}

// keep registers n bytes the walk is about to keep. false: there is no
// room, and the caller does not keep them.
func (a *walkAccount) keep(n int) bool {
	if a.kept+n > a.limit() {
		a.notKept++
		return false
	}
	a.kept += n
	return true
}

// pomRequestBytes is the request allowance: one request of the PomSource for
// this many bytes read. PROVISIONAL (summary/71, section 8). Its
// derivation: only a parent reference and a BOM import ask, and neither can
// be written in fewer bytes than this and still name a pom that can be asked
// for (a group, an artifact and a version): the shortest parent element is
// 87 bytes, the shortest import 131, and a Gradle platform with its
// configuration and the path of its file is no shorter. 64 is the power of
// two below the shortest. So everything a walk's files WRITE can always be
// asked about, and a fetched pom's own parent and imports with it (its
// entries stand for pomRemoteEntryBytes each, more than this). What the
// allowance ends is the product: an import whose coordinates hold a
// reference is another coordinate in each module that inherits it, and is
// asked for each of them, as Maven would ask (a module that sets the
// property differently does import another BOM), while the modules' own
// bytes allow it. 480 modules under 480 such imports are 194 KB and were
// 199,651 requests; they are now about 4,000 (the paths of the 481 files are
// read too).
const pomRequestBytes = 64

// mayAsk counts one request of the PomSource. false: the allowance is used
// up and the request is not made, which is said in the log once per walk.
func (a *walkAccount) mayAsk() bool {
	if a.asked >= a.read/pomRequestBytes {
		if a.askRefused == 0 && a.logger != nil {
			a.logger.Warn("the walk's allowance of requests for poms is used up (one for every pomRequestBytes read); the poms still asked for are not looked up, and what depends on them is reported as not obtained",
				"asked", a.asked, "bytes_read", a.read)
		}
		a.askRefused++
		return false
	}
	a.asked++
	return true
}

// The wall-clock backstop (review round 5). It should never fire: the work
// bounds are the bound on time, and they are measured (walkUnitNanos). It is
// here because five review rounds each found a path whose work was not
// counted, and an uncounted path has no bound at all: a collection worker
// was held for as long as the text asked. When it fires, the files still to
// be read by the build-tree readers are refused by name, with a reason that
// says it was the backstop, and the log says at Warn that a path was not
// accounted for: that is a defect of this package to be reported, not a
// property of the repository.
//
// PROVISIONAL (summary/71, section 8). The allowance is walkBackstopFloor
// plus walkBackstopMargin times the stated bound for the bytes read
// (walkUnitNanos nanoseconds for each of the gradleWalkFactor units a byte
// allows: the Gradle walk's bound, the dearer of the two per byte). The
// margin of ten is for a machine ten times slower than the one the unit was
// measured on, or as busy; the floor is for a walk too small for a
// proportion to mean anything (a cold disk answers an Lstat in
// milliseconds, and a small tree makes hundreds).
const (
	walkBackstopFloor  = time.Minute
	walkBackstopMargin = 10
	// walkUnitNanos is the dearest measured cost of one unit of the walks'
	// work, rounded up: see gradleWalkFactor (about 60 ns a unit) and
	// pomWorkFactor (about 300 ns a unit, of which a byte allows one).
	walkUnitNanos = 60
	// walkClockEvery is how many units pass between two looks at the clock:
	// about four milliseconds of work, where a look costs some fifty
	// nanoseconds.
	walkClockEvery = 1 << 16
)

// errWalkBackstop: the wall-clock backstop ended the walk's build-tree
// resolution.
var errWalkBackstop = errors.New("the walk's wall-clock backstop ended the build-tree readers (walkBackstopFloor): a path of theirs was not accounted for by the work bounds; nothing was taken from this file")

// backstop is how long the readers may have taken for what was read so far.
func (a *walkAccount) backstop() time.Duration {
	return walkBackstopFloor + time.Duration(walkBackstopMargin*walkUnitNanos*gradleWalkFactor)*time.Duration(a.read)
}

// enter and leave bracket a reader's entry point; only time between them is
// the readers'.
func (a *walkAccount) enter() {
	if a.depth == 0 {
		a.since = a.now()
	}
	a.depth++
}

func (a *walkAccount) leave() {
	if a.depth--; a.depth == 0 {
		a.spent += a.now().Sub(a.since)
	}
}

// pause and resume bracket a call that is not the readers' work (the
// PomSource: a request over a network).
func (a *walkAccount) pause() {
	if a.depth > 0 {
		a.spent += a.now().Sub(a.since)
	}
}

func (a *walkAccount) resume() {
	if a.depth > 0 {
		a.since = a.now()
	}
}

// tick is called where work is charged, with the units charged. true: the
// backstop has fired, now or before, and the caller stops.
func (a *walkAccount) tick(units int) bool {
	if a.fired {
		return true
	}
	if a.sinceCheck += units; a.sinceCheck < walkClockEvery || a.depth == 0 {
		return false
	}
	a.sinceCheck = 0
	took := a.spent + a.now().Sub(a.since)
	if took <= a.backstop() {
		return false
	}
	a.fired = true
	if a.logger != nil {
		a.logger.Warn("the build-tree readers passed their wall-clock backstop: a path through them is NOT ACCOUNTED FOR by the work bounds (a defect of the manifest readers; please report the repository). The build files still to be read on this walk are not read, and are reported so",
			"took", took.String(), "backstop", a.backstop().String(), "bytes_read", a.read)
	}
	return true
}
