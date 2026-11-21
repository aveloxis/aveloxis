// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The Gradle build-file reader (v0.29.192; summary/70, 4.6). It replaces a
// line matcher that saw one notation ('group:artifact:version' in quotes) in
// seven configurations. A build file is a program, and this does not run it:
// it reads the file as tokens, finds the dependencies blocks, and reads each
// declaration in the notations Gradle documents (the string, the map,
// Kotlin's named arguments, platform(...)), in every configuration. A
// version written as a variable is looked up among the assignments it can
// see; one it cannot read leaves the version unknown and what was written
// kept. It never puts an expression where a version goes.

// gradleTok is one token: an identifier (dotted paths are one token), a
// string, a punctuation character, or the end of a statement.
type gradleTok struct {
	kind   byte // 'i' identifier, 's' string, 'p' punctuation, 'n' end of statement
	text   string
	interp bool // a double-quoted string: $name and ${expr} are interpolated
}

func gradleIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func gradleIdentPart(c byte) bool { return gradleIdentStart(c) || (c >= '0' && c <= '9') }

// gradleTokens reads a build file's text. It cannot fail: text that is cut
// off ends the token it is in.
func gradleTokens(src string) []gradleTok {
	var out []gradleTok
	n := len(src)
	for i := 0; i < n; {
		c := src[i]
		switch {
		case c == '\n' || c == ';':
			out = append(out, gradleTok{kind: 'n'})
			i++
		case c == ' ' || c == '\t' || c == '\r':
			i++
		case c == '/' && i+1 < n && src[i+1] == '/':
			for i < n && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				i = n
			} else {
				i += end + 4
			}
		case c == '\'' || c == '"':
			text, next := gradleString(src, i)
			out = append(out, gradleTok{kind: 's', text: text, interp: c == '"'})
			i = next
		case gradleIdentStart(c):
			j := i + 1
			for j < n && (gradleIdentPart(src[j]) || (src[j] == '.' && j+1 < n && gradleIdentStart(src[j+1]))) {
				j++
			}
			out = append(out, gradleTok{kind: 'i', text: src[i:j]})
			i = j
		case c >= '0' && c <= '9':
			j := i + 1
			for j < n && (gradleIdentPart(src[j]) || src[j] == '.') {
				j++
			}
			out = append(out, gradleTok{kind: 'i', text: src[i:j]})
			i = j
		default:
			out = append(out, gradleTok{kind: 'p', text: src[i : i+1]})
			i++
		}
	}
	return out
}

// gradleString reads the string that opens at src[i] and returns its
// content and the index after it. Inside a double-quoted string a ${...}
// expression is read to its closing brace, so a quote inside it (as in
// ${extra["name"]}) does not end the string.
func gradleString(src string, i int) (string, int) {
	q := src[i]
	n := len(src)
	triple := i+2 < n && src[i+1] == q && src[i+2] == q
	start := i + 1
	if triple {
		start = i + 3
	}
	for j := start; j < n; j++ {
		switch {
		case src[j] == '\\':
			j++
		case q == '"' && src[j] == '$' && j+1 < n && src[j+1] == '{':
			depth := 0
			for ; j < n; j++ {
				if src[j] == '{' {
					depth++
				} else if src[j] == '}' {
					depth--
					if depth == 0 {
						break
					}
				}
			}
		case triple && src[j] == q && j+2 < n && src[j+1] == q && src[j+2] == q:
			return src[start:j], j + 3
		case !triple && src[j] == q:
			return src[start:j], j + 1
		case !triple && src[j] == '\n':
			return src[start:j], j // an unclosed string ends with its line
		}
	}
	if start > n {
		start = n
	}
	return src[start:], n
}

// gradleDecl is one declaration of a dependencies block.
type gradleDecl struct {
	config   string
	group    string
	artifact string
	version  string // "" when none is written or it could not be read
	written  string // the coordinate as written
	platform bool
	// enforced: the platform was written enforcedPlatform(...), whose
	// versions override every other (gradleTree.applyPlatforms).
	enforced bool
	// versionWritten: a version was written, read or not. A platform does
	// not answer for a dependency the build gave a version to.
	versionWritten bool
	// forSubprojects: declared in an allprojects or subprojects block, so
	// for the projects below this build file too.
	forSubprojects bool
	// Set when a platform supplied the version (gradle_tree.go).
	source  string
	hosts   []string
	reason  string
	managed bool // a platform whose versions were applied: no longer a row
}

// gradleBuild is what one build file says.
type gradleBuild struct {
	decls []gradleDecl
	// enforcing: the arguments being read are those of enforcedPlatform(...).
	enforcing bool
	// outer are the variables known from elsewhere when the file is read
	// (gradleVars: shared, never copied). A name is looked up in own
	// first, then there.
	outer *gradleVars
	// own are the variables this file assigns, to tell a second, different
	// assignment (one per branch of a condition) from the file overriding a
	// value known from elsewhere. conflicted are the names with two values:
	// they are not known, here or from outer.
	own        map[string]string
	conflicted map[string]bool
	// reads are the names this file looked up in outer, each with what it
	// was given. Everything the file's reading took from outside is here,
	// so a second reading with an outer that answers the same for these
	// names gives the same result (gradleTree's memo of applied scripts).
	reads map[string]gradleRead
	// applies are the files this one applies ("apply from: ..."), as
	// written.
	applies []string
	// libs are the version catalogues' library accessors
	// ("libs.androidx.core" -> what it stands for; a bundle stands for
	// several).
	libs *gradleLibs
	// boms are the BOMs the build names that are not declarations of a
	// dependencies block: Spring's dependencyManagement { imports {
	// mavenBom "g:a:v" } }, and the Spring Boot plugin's own.
	boms []gradleDecl
	// managed are the versions Spring's dependency-management plugin is
	// given outright: dependencyManagement { dependencies { dependency
	// 'g:a:v' } }, and dependencySet's entries. "group:artifact" -> version;
	// "" for an entry whose version could not be read. They override every
	// imported BOM (gradleTree.applyPlatforms).
	managed map[string]string
	// set is the dependencySet whose entries are being read.
	setGroup, setVersion string
	// unread are the variables this file assigns a value too long to be
	// read (gradleValueTokens, gradleMapTokens): a version written with one
	// is not read, and says so (model.VersionReasonValueNotRead).
	unread map[string]bool
	// seq counts the statements the assignments pass has looked at; setAt is
	// where each variable was last assigned, and firstApply and lastApply
	// where the file's first and last "apply from:" stand (0: none). What an
	// applied script assigns comes after what the file assigned before
	// applying it (applyOrder).
	seq                   int
	setAt                 map[string]int
	firstApply, lastApply int
	// plugins are the plugins block's ids and versions ("" when none is
	// written).
	plugins map[string]string
	// inShared is set while the walk is inside an allprojects or
	// subprojects block.
	inShared bool
	// repos are the entries of the file's repositories blocks; repoSeen
	// the kind and address of each, so an entry is looked for in one step.
	repos    []gradleRepo
	repoSeen map[string]bool
	// idx is the index of the file's tokens (statement); fromsRead what
	// applyFrom has read through it (fromParts).
	idx       *gradleIndex
	fromsRead gradleFromsRead
}

// gradleIndex is what is worked out ONCE about a file's tokens so that
// finding a statement costs nothing that grows with the statement (review
// round 6). The reader restarts at every line and every brace, and a
// statement runs to its end however far that is: an unclosed call after
// every brace, or a comma at the end of every line, made every restart read
// to the end of the file and copy what it read, the square of the file (49
// KB took more than eight seconds). Until 0.29.212 a count of work ended
// such a file; there is none now, and the reading is linear instead:
//
//   - end[i] is where the statement that begins at toks[i] ends, for every
//     i, from one pass (newGradleIndex);
//   - flat is the tokens without their line ends, and pos[i] how many of
//     toks[:i] are in it: a statement's tokens, which are its span without
//     the line ends inside it, are flat[pos[i]:pos[end]], with no copy.
//
// gradleStatement is the same reading done the slow way, token by token. It
// is kept for a slice of tokens that is not a whole file (a repository's own
// block) and as what TestTheStatementIndexIsTheStatementReader compares the
// index with.
type gradleIndex struct {
	toks []gradleTok
	flat []gradleTok
	pos  []int32
	end  []int32
	// What the readers of a statement ask of its tokens, by their place in
	// flat, so that no reader goes through a statement to its end to learn
	// one thing about it (a statement can be most of the file, and begin
	// again at every brace inside it):
	//
	// partner[k] is the place of the parenthesis or square bracket that
	// closes the one at k (-1: none, or not an opening one): unparen, and
	// whether a value is one map (encloses).
	// depth[k] is the brackets open before flat[k], from the start of the
	// file; commas[d] are the places of the commas at depth d and froms[d]
	// those of them that the word `from` follows: a statement's parts are
	// cut at the commas at its own depth (parts, fromParts).
	// nextString[k] is the first string that is not empty at or after k, nextApply[k] the
	// first word `apply`, and lastVersion[k] the last string before k that
	// follows the word `version` (len(flat) and -1 for none): plugin.
	partner     []int32
	depth       []int32
	commas      map[int32][]int32
	froms       map[int32][]int32
	nextString  []int32
	nextApply   []int32
	lastVersion []int32
}

// place is where toks begins in flat, when toks is a part of it: a
// statement the index handed out, or a slice of one. (flat is handed out
// with its capacity, so a slice's capacity says where it begins.)
func (x *gradleIndex) place(toks []gradleTok) (int, bool) {
	if x == nil || len(toks) == 0 {
		return 0, false
	}
	at := cap(x.flat) - cap(toks)
	if at < 0 || at+len(toks) > len(x.flat) || &x.flat[at] != &toks[0] {
		return 0, false
	}
	return at, true
}

// finish fills what is asked of flat.
func (x *gradleIndex) finish() {
	m := len(x.flat)
	x.partner, x.depth = make([]int32, m), make([]int32, m+1)
	x.nextString, x.nextApply, x.lastVersion = make([]int32, m+1), make([]int32, m+1), make([]int32, m+1)
	x.commas, x.froms = map[int32][]int32{}, map[int32][]int32{}
	var open, openSquare []int32
	last := int32(-1)
	for k, t := range x.flat {
		x.partner[k] = -1
		x.depth[k+1] = x.depth[k]
		x.lastVersion[k] = last
		if t.kind == 's' && k > 0 && x.flat[k-1].kind == 'i' && x.flat[k-1].text == "version" {
			last = int32(k)
		}
		if t.kind != 'p' {
			continue
		}
		switch t.text {
		case "(":
			open = append(open, int32(k))
			x.depth[k+1]++
		case "[":
			openSquare = append(openSquare, int32(k))
			x.depth[k+1]++
		case ")":
			if len(open) > 0 {
				x.partner[open[len(open)-1]] = int32(k)
				open = open[:len(open)-1]
			}
			x.depth[k+1]--
		case "]":
			if len(openSquare) > 0 {
				x.partner[openSquare[len(openSquare)-1]] = int32(k)
				openSquare = openSquare[:len(openSquare)-1]
			}
			x.depth[k+1]--
		case ",":
			d := x.depth[k]
			x.commas[d] = append(x.commas[d], int32(k))
			if k+1 < m && x.flat[k+1].kind == 'i' && x.flat[k+1].text == "from" {
				x.froms[d] = append(x.froms[d], int32(k))
			}
		}
	}
	x.lastVersion[m] = last
	x.nextString[m], x.nextApply[m] = int32(m), int32(m)
	for k := m - 1; k >= 0; k-- {
		x.nextString[k], x.nextApply[k] = x.nextString[k+1], x.nextApply[k+1]
		switch t := x.flat[k]; {
		case t.kind == 's' && t.text != "":
			x.nextString[k] = int32(k)
		case t.kind == 'i' && t.text == "apply":
			x.nextApply[k] = int32(k)
		}
	}
}

// firstAtOrAfter is the first of list (ascending) that is at least at; its
// index in list.
func firstAtOrAfter(list []int32, at int) int {
	lo, hi := 0, len(list)
	for lo < hi {
		if mid := (lo + hi) / 2; int(list[mid]) < at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// unparen is gradleUnparen: for tokens of the file, from the index.
func (b *gradleBuild) unparen(toks []gradleTok) []gradleTok {
	at, ok := b.idx.place(toks)
	if !ok {
		return gradleUnparen(toks)
	}
	if len(toks) < 2 {
		return toks
	}
	strip := 0
	for strip < len(toks) && toks[strip].kind == 'p' && toks[strip].text == "(" && int(b.idx.partner[at+strip]) == at+len(toks)-1-strip {
		strip++
	}
	return toks[strip : len(toks)-strip]
}

// parts is gradleSplit, at most max parts of it (0: all): for tokens of the
// file, from the index. more: there are parts beyond the ones returned.
func (b *gradleBuild) parts(toks []gradleTok, max int) (out [][]gradleTok, more bool) {
	at, ok := b.idx.place(toks)
	if !ok {
		return gradleSplit(toks), false
	}
	commas := b.idx.commas[b.idx.depth[at]]
	from := at
	for k := firstAtOrAfter(commas, at); k < len(commas) && int(commas[k]) < at+len(toks); k++ {
		if max > 0 && len(out) == max {
			return out, true
		}
		c := int(commas[k])
		out = append(out, toks[from-at:c-at])
		from = c + 1
	}
	if max > 0 && len(out) == max {
		return out, true
	}
	return append(out, toks[from-at:]), false
}

// fromParts are the parts of toks (gradleSplit) that begin with the word
// `from`, in order: all that applyFrom reads.
//
// seen, when not nil, is what earlier statements of this reading have
// already had read: a statement that begins again inside an earlier one
// (the reader restarts at every line, and a line that ends in a comma
// continues) has that one's parts again, all but its own first. Read again
// they name the same files again, which applying skips; ten thousand such
// lines read each other's parts fifty million times (review round 6). The
// parts already read for a statement that ends where this one does are not
// read again.
func (b *gradleBuild) fromParts(toks []gradleTok, seen *gradleFromsRead) [][]gradleTok {
	at, ok := b.idx.place(toks)
	if !ok {
		var out [][]gradleTok
		for _, part := range gradleSplit(toks) {
			if len(part) > 0 && part[0].kind == 'i' && part[0].text == "from" {
				out = append(out, part)
			}
		}
		return out
	}
	d := b.idx.depth[at]
	commas, froms := b.idx.commas[d], b.idx.froms[d]
	end := at + len(toks)
	// partFrom is the part that begins at flat[p]: to the next comma at
	// this depth, or the end.
	partFrom := func(p int) []gradleTok {
		stop := end
		if k := firstAtOrAfter(commas, p); k < len(commas) && int(commas[k]) < end {
			stop = int(commas[k])
		}
		return toks[p-at : stop-at]
	}
	var out [][]gradleTok
	if toks[0].kind == 'i' && toks[0].text == "from" {
		out = append(out, partFrom(at))
	}
	// The commas of this depth in the statement that `from` follows: places
	// lo to hi of the list (a comma at end-1 has nothing after it here).
	lo, hi := firstAtOrAfter(froms, at), firstAtOrAfter(froms, end-1)
	if seen != nil {
		// r.end == end: the parts were read for a statement that ended
		// where this one does, so each part is the same tokens. Round 7's
		// review dropped the condition and no test failed, and no input was
		// found for which it decides: it is kept, because the only thing it
		// can do is make this skip LESS often (a part read again is applied
		// once: gradleTree.applied), and a part read for a statement with
		// another end may be cut somewhere else.
		if r, ok := seen.at[d]; ok && r.end == end && r.lo <= lo && hi <= r.hi {
			return out
		}
		if seen.at == nil {
			seen.at = map[int32]gradleFromRange{}
		}
		seen.at[d] = gradleFromRange{end: end, lo: lo, hi: hi}
	}
	for k := lo; k < hi; k++ {
		out = append(out, partFrom(int(froms[k])+1))
	}
	return out
}

// gradleFromsRead is, for each depth of brackets, the `from` parts last
// read: those between places lo and hi of the index's list for that depth,
// for a statement that ended at end.
type gradleFromsRead struct{ at map[int32]gradleFromRange }

type gradleFromRange struct{ end, lo, hi int }

func newGradleIndex(toks []gradleTok) *gradleIndex {
	n := len(toks)
	x := &gradleIndex{toks: toks, flat: make([]gradleTok, 0, n), pos: make([]int32, n+1), end: make([]int32, n)}
	// depth[j]: the brackets open before toks[j], counted from the start of
	// the file (it goes below zero where a bracket is closed that was not
	// opened). A statement that begins at i has depth[j]-depth[i] open at j.
	depth := make([]int32, n+1)
	for j, t := range toks {
		depth[j+1] = depth[j]
		x.pos[j+1] = x.pos[j]
		if t.kind == 'p' {
			switch t.text {
			case "(", "[":
				depth[j+1]++
			case ")", "]":
				depth[j+1]--
			}
		}
		if t.kind != 'n' {
			x.flat = append(x.flat, t)
			x.pos[j+1]++
		}
	}
	// A statement that begins at i ends at the first token j at or after i
	// that CAN end one (a brace; a line end that does not follow a comma, a
	// colon, an operator or an equals sign: gradleStatementSpan) with no
	// bracket of the statement's own open there: depth[j] <= depth[i].
	//
	// From the right, the tokens that can end a statement are kept on a
	// stack, without those that a nearer one hides (a nearer one with no
	// greater depth is met first by every statement that could end at the
	// farther): what is left has its depths in order, and the first with
	// depth at most depth[i] is found by halving.
	var stops []int32
	for i := n - 1; i >= 0; i-- {
		t := toks[i]
		canEnd := false
		switch {
		case t.kind == 'p' && (t.text == "{" || t.text == "}"):
			canEnd = true
		case t.kind == 'n':
			canEnd = !(i > 0 && toks[i-1].kind == 'p' && strings.Contains(",:+=", toks[i-1].text))
		}
		if canEnd {
			for len(stops) > 0 && depth[stops[len(stops)-1]] >= depth[i] {
				stops = stops[:len(stops)-1]
			}
			stops = append(stops, int32(i))
		}
		if t.kind == 'n' {
			x.end[i] = int32(i) // a line end is a statement of nothing, whatever precedes it
			continue
		}
		// stops nearer the top are nearer i and deeper: the nearest whose
		// depth is at most depth[i] is the greatest k with that depth.
		lo, hi := 0, len(stops)
		for lo < hi {
			mid := (lo + hi) / 2
			if depth[stops[mid]] <= depth[i] {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo == 0 {
			x.end[i] = int32(n)
		} else {
			x.end[i] = stops[lo-1]
		}
	}
	x.finish()
	return x
}

// statement is gradleStatement for the file's own tokens, from the index:
// the statement that begins at toks[i], without its line ends, and the
// index after it.
func (b *gradleBuild) statement(toks []gradleTok, i int) (stmt []gradleTok, next int) {
	x := b.idx
	if x == nil || i >= len(toks) || len(toks) != len(x.toks) || &toks[0] != &x.toks[0] {
		return gradleStatement(toks, i)
	}
	end := int(x.end[i])
	return x.flat[x.pos[i]:x.pos[end]], end
}

// gradleLevel is what the walk knows of one open block, worked out when the
// block is entered: whether statements in it are declarations of a
// dependencies block or entries of a repositories block (the innermost
// block that is not control flow decides), and what the blocks around it
// make of a repository declared there. It was asked of the whole stack at
// every statement, which for blocks nested n deep is n times n.
type gradleLevel struct {
	inDeps, inRepos        bool
	forPlugins, repoShared bool
	// inManagement: inside Spring's dependencyManagement { } block, where a
	// dependencies block holds managed versions and not dependencies.
	inManagement bool
}

func (above gradleLevel) enter(name string) gradleLevel {
	lv := gradleLevel{forPlugins: above.forPlugins, repoShared: above.repoShared, inManagement: above.inManagement || name == "dependencyManagement"}
	switch name {
	case "pluginManagement", "buildscript":
		lv.forPlugins = true
	case "allprojects", "subprojects", "dependencyResolutionManagement":
		lv.repoShared = true
	}
	if gradleControlFlow[name] {
		lv.inDeps, lv.inRepos = above.inDeps, above.inRepos
		return lv
	}
	lv.inRepos = name == "repositories" || name == "forRepository"
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	lv.inDeps = name == "dependencies"
	return lv
}

// What bounds the reading of a Gradle build (0.29.213): see pom_tree.go.
// The reader is not a parser of the language: it restarts at every line and
// every brace, and text can be written that makes each restart read far.
// Each such shape found was made linear. For the ones not found there was a
// count of work in this file (gradleWorkFactor, since 0.29.195) and, later,
// one for the walk; both are gone. The bound on a walk's time is the limit
// on the CPU time of the process the collector reads manifests in.

// gradleRead is what one name looked up outside the file was given.
type gradleRead struct {
	value string
	ok    bool
}

// gradleVars is what is known from outside a file when it is read: the
// variables assigned by gradle.properties, the settings file, the build
// files above, the scripts applied before it, buildSrc and the catalogues.
//
// It is a chain of layers, each the assignments of ONE source, the nearest
// first, and a layer is never copied (0.29.210). It was one map per
// directory, copied from the directory above: two thousand project
// directories under a gradle.properties of two thousand lines, 105 KB in
// all, kept 313 MB. A lookup walks the chain; each layer visited is a unit
// of the reading file's work.
type gradleVars struct {
	// own are the layer's assignments; gone the names its source left with
	// two different values, which are not known below it, whatever a layer
	// above says. Both are shared with whoever built them and never
	// written after.
	own    map[string]string
	gone   map[string]bool
	parent *gradleVars
	// seg, when set, stands for a run of layers in this place: a chain of
	// its own, ending in nil, built once and shared by every scope it is
	// part of (what one applied script and the scripts it applies leave
	// behind: gradleBundle). The link holds no assignments itself.
	seg *gradleVars
}

// push is s with one more layer in front. Empty maps add nothing.
func (s *gradleVars) push(own map[string]string, gone map[string]bool) *gradleVars {
	if len(own) == 0 && len(gone) == 0 {
		return s
	}
	return &gradleVars{own: own, gone: gone, parent: s}
}

// pushSeg is s with a shared run of layers in front: one link, however
// many layers the run has.
func (s *gradleVars) pushSeg(seg *gradleVars) *gradleVars {
	if seg == nil {
		return s
	}
	return &gradleVars{seg: seg, parent: s}
}

// get is the value of name, and how many layers were looked at.
func (s *gradleVars) get(name string) (value string, ok bool, layers int) {
	value, ok, _, layers = s.find(name)
	return value, ok, layers
}

// find is get, and whether a layer settled the name (has it, or lost it):
// a shared run that does not is passed through to what is behind it.
func (s *gradleVars) find(name string) (value string, ok, settled bool, layers int) {
	for ; s != nil; s = s.parent {
		if s.seg != nil {
			v, has, done, n := s.seg.find(name)
			layers += n
			if done {
				return v, has, true, layers
			}
			continue
		}
		layers++
		if s.gone[name] {
			return "", false, true, layers
		}
		if v, has := s.own[name]; has {
			return v, true, true, layers
		}
	}
	return "", false, false, layers
}

// result is what the file leaves known below it, as a layer's two maps:
// what it assigns, less the names it assigned twice differently, and those
// names.
func (b *gradleBuild) result() (own map[string]string, gone map[string]bool) {
	if len(b.own) == 0 && len(b.conflicted) == 0 {
		return nil, nil
	}
	if len(b.conflicted) == 0 {
		return b.own, nil
	}
	own = make(map[string]string, len(b.own))
	for k, v := range b.own {
		if !b.conflicted[k] {
			own[k] = v
		}
	}
	return own, b.conflicted
}

// gradleLibs are the catalogues' library accessors known at a directory: a
// chain of layers, one per catalogue, as gradleVars is for variables.
type gradleLibs struct {
	own    map[string][]gradleDecl
	parent *gradleLibs
}

func (l *gradleLibs) get(name string) ([]gradleDecl, bool) {
	for ; l != nil; l = l.parent {
		if d, ok := l.own[name]; ok {
			return d, true
		}
	}
	return nil, false
}

// gradleInterpolatedLimit bounds a string that is interpolated and what the
// interpolation produces: pomInterpolatedLimit, for the same reason (a
// variable that is another one twice over doubles at every line).
const gradleInterpolatedLimit = pomInterpolatedLimit

// gradleValueTokens and gradleMapTokens bound how many tokens are read as
// ONE value (a string, or strings and variables joined by +), and as one
// map of values. A value of gradleInterpolatedLimit bytes has at most that
// many parts and as many + between them; a map of four thousand entries is
// larger than any a build declares its versions in. Longer is not read.
//
// Why a count and not only the bytes built (round 7): a statement that
// does not end (a line that ends in +, an unclosed bracket) is begun again
// at every line of it, each time to the end of the file. Reading each such
// statement whole took time in the square of the file's size (`val x by
// extra a +`, line after line: 573 KB in 269 s). With these bounds each is
// read in constant time once it is longer than a value can be.
//
// What each is for (round 8). gradleValueTokens is what ONE value can be,
// twice the bytes one may have; a variable given a longer one is not read,
// and a dependency whose version is written with it says so
// (model.VersionReasonValueNotRead; the log names the file). A MAP of
// values has no such bound when it is the file's own tokens: it is a map
// only when the bracket that opens it is closed by its last token
// (encloses), which the index answers in one step, so a map is read once,
// whatever its length, and a statement that merely begins like one and runs
// on to the end of the file is not read as one. Until 0.29.215 a map was
// bounded at 1<<14 tokens, and an honest ext map of 4,096 versions, which
// every earlier release read, was silently not read. gradleMapTokens is
// what is left of that bound: tokens that are not a file's (a slice read
// the slow way) are scanned for the closing bracket no further than it.
const (
	gradleValueTokens = 2 * gradleInterpolatedLimit
	gradleMapTokens   = 1 << 14
)

// gradleRepo is one entry of a repositories block. shared: it serves the
// projects below this file too (allprojects, subprojects, or a settings
// file's dependencyResolutionManagement).
type gradleRepo struct {
	DeclaredRepository
	shared bool
}

// gradleNotConfigurations are the words that open a statement in a
// dependencies block and are not configurations.
var gradleNotConfigurations = map[string]bool{
	"println": true, "print": true, "apply": true, "because": true, "exclude": true, "def": true, "val": true, "var": true,
	"return": true, "assert": true,
}

// readGradleBuild reads a build file. outer are variables known from
// elsewhere (the next release's tree reader supplies them); the file's own
// assignments are added to a copy.
func readGradleBuild(content string, outer *gradleVars) *gradleBuild {
	return readGradleBuildWith(content, outer, nil)
}

// readGradleBuildWith is readGradleBuild with the catalogues' accessors.
func readGradleBuildWith(content string, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	return readGradleTokens(gradleTokens(content), outer, libs)
}

// readGradleTokens reads a build file from its tokens.
func readGradleTokens(toks []gradleTok, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	return readGradleIndexed(newGradleIndex(toks), outer, libs)
}

// readGradleIndexed reads a build file from its tokens and their index. A
// nil index's fields are never read: every reader of a statement then reads
// it token by token, as all did before the index
// (TestAGradleFileIsReadTheSameWithAndWithoutItsIndex reads files both ways).
func readGradleIndexed(idx *gradleIndex, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	return readGradleWith(idx.toks, idx, outer, libs)
}

func readGradleWith(toks []gradleTok, idx *gradleIndex, outer *gradleVars, libs *gradleLibs) *gradleBuild {
	return readGradleAfter(toks, idx, outer, nil, false, libs)
}

// readGradleAfter reads a file whose applied scripts have been read: outer
// holds what they assign in front of applied (the scope the file started
// from). ordered is false for a reading that does not know which of outer's
// layers are the applied scripts' (applied is then not looked at).
func readGradleAfter(toks []gradleTok, idx *gradleIndex, outer, applied *gradleVars, ordered bool, libs *gradleLibs) *gradleBuild {
	b := &gradleBuild{outer: outer, own: map[string]string{}, conflicted: map[string]bool{}, libs: libs,
		reads: map[string]gradleRead{}, idx: idx, setAt: map[string]int{}}
	// Two passes: the assignments first, so a variable assigned below its
	// use (or in a later ext block) is known when the declarations are read.
	b.walk(toks, false)
	if ordered {
		b.applyOrder(applied)
	}
	b.walk(toks, true)
	b.springBootBOM()
	return b
}

// applyOrder puts what the applied scripts assign in its place among the
// file's own assignments. Gradle runs a build file from the top: a script
// applied AFTER the file assigned a variable assigns over it, and one
// applied before is assigned over. The reader keeps the file's own
// assignments in front of everything the scripts assign, which is right only
// for the second (round 8: `ext.v = '1.0.0'` and then `apply from:` a script
// that sets it to 2.0.0 read as 1.0.0).
//
// For each variable both assign: assigned by the file before its first
// "apply from:", the scripts' value stands; after its last, the file's; in
// between, it is not known which script it is (the scripts' assignments are
// one scope here), and the variable is not read.
func (b *gradleBuild) applyOrder(stop *gradleVars) {
	if b.firstApply == 0 {
		return
	}
	for name, at := range b.setAt {
		v, ok, settled := b.outer.findUntil(stop, name)
		if !settled || at > b.lastApply {
			continue
		}
		own, has := b.own[name]
		switch {
		case at < b.firstApply && ok:
			b.own[name] = v
			delete(b.conflicted, name)
		case at < b.firstApply:
			b.conflicted[name] = true // the scripts left it with two values
		case !ok || !has || own != v:
			b.conflicted[name] = true
		}
	}
}

// findUntil is find over the layers in front of stop: what was pushed onto
// stop to make s.
func (s *gradleVars) findUntil(stop *gradleVars, name string) (value string, ok, settled bool) {
	for ; s != nil && s != stop; s = s.parent {
		if s.seg != nil {
			if v, has, done, _ := s.seg.find(name); done {
				return v, has, true
			}
			continue
		}
		if s.gone[name] {
			return "", false, true
		}
		if v, has := s.own[name]; has {
			return v, true, true
		}
	}
	return "", false, false
}

// managedEntry reads one statement of a dependencyManagement block's
// dependencies: dependency 'g:a:v' (or its map form), dependencySet(group:
// 'g', version: 'v') or dependencySet('g:v'), and entry 'a' inside one.
func (b *gradleBuild) managedEntry(toks []gradleTok, i int, head, block string) {
	stmt, _ := b.statement(toks, i)
	if len(stmt) < 2 {
		return
	}
	args := b.unparen(stmt[1:])
	note := func(group, artifact, version string) {
		group, artifact = strings.TrimSpace(group), strings.TrimSpace(artifact)
		if group == "" || artifact == "" || strings.ContainsAny(group+artifact, "\x00${}()\"' \t") {
			return
		}
		if !gradleIsVersion(version) {
			version = ""
		}
		if b.managed == nil {
			b.managed = map[string]string{}
		}
		// The plugin takes the last one given; so does this.
		b.managed[group+":"+artifact] = version
	}
	switch {
	case head == "dependency" && block != "dependencySet":
		before := len(b.decls)
		b.arguments("dependency", args, false)
		for _, d := range b.decls[before:] {
			note(d.group, d.artifact, d.version)
		}
		b.decls = b.decls[:before]
	case head == "dependencySet":
		b.setGroup, b.setVersion = "", ""
		if len(args) > 0 && args[0].kind == 's' && len(args) == 1 {
			// 'group:version'
			text := args[0].text
			if args[0].interp {
				text, _ = b.interpolate(text)
			}
			b.setGroup, b.setVersion, _ = strings.Cut(text, ":")
			return
		}
		parts, _ := b.parts(args, 0)
		for _, part := range parts {
			if len(part) < 3 || part[0].kind != 'i' || part[1].kind != 'p' || (part[1].text != ":" && part[1].text != "=") {
				continue
			}
			v, _, ok := b.value(part[2:])
			if !ok {
				continue
			}
			switch part[0].text {
			case "group":
				b.setGroup = v
			case "version":
				b.setVersion = v
			}
		}
	case head == "entry" && block == "dependencySet":
		if args[0].kind == 's' && b.setGroup != "" {
			name := args[0].text
			if args[0].interp {
				name, _ = b.interpolate(name)
			}
			note(b.setGroup, name, b.setVersion)
		}
	}
}

// walk goes over the statements of the file, keeping the stack of named
// blocks it is inside. With decls false it collects assignments; with true,
// the declarations of each dependencies block.
func (b *gradleBuild) walk(toks []gradleTok, decls bool) {
	var stack []string
	// quals runs beside stack: the name of an object or class block
	// ("object Versions {"), "" for any other. A constant declared inside is
	// Versions.name.
	//
	// The qualifier of a statement is kept as it is built, one prefix per
	// level (0.29.209): it was joined again from every level at every
	// statement, the depth times the names' length each time.
	var quals []string
	// levels runs beside stack too (gradleLevel).
	var levels []gradleLevel
	level := func() gradleLevel {
		if len(levels) == 0 {
			return gradleLevel{}
		}
		return levels[len(levels)-1]
	}
	second := "" // the word after head
	head := ""   // the word that opened the current statement
	atStart := true
	top := func() string {
		if len(stack) == 0 {
			return ""
		}
		return stack[len(stack)-1]
	}
	parens := 0 // a line end inside parentheses does not end the statement
	// shared counts the allprojects and subprojects blocks on the stack.
	shared := 0
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		switch {
		case t.kind == 'p' && (t.text == "(" || t.text == "["):
			parens++
		case t.kind == 'p' && (t.text == ")" || t.text == "]"):
			if parens > 0 {
				parens--
			}
		case t.kind == 'n' && parens > 0:
			continue
		case t.kind == 'n':
			atStart, head = true, ""
			continue
		case t.kind == 'p' && t.text == "{":
			levels = append(levels, level().enter(head))
			stack = append(stack, head)
			if head == "allprojects" || head == "subprojects" {
				shared++
			}
			outer := ""
			if len(quals) > 0 {
				outer = quals[len(quals)-1]
			}
			switch {
			case outer == gradleNoQualifier, gradleTypeWords[head] && len(outer)+len(second)+1 > gradleInterpolatedLimit:
				// A qualified name longer than a name can be names no
				// variable; neither does anything declared below it. (Kept
				// short: the prefix would otherwise be copied for every
				// statement inside, the depth times the names' length.)
				quals = append(quals, gradleNoQualifier)
			case gradleTypeWords[head] && second != "":
				quals = append(quals, outer+second+".")
			default:
				quals = append(quals, outer)
			}
			atStart, head = true, ""
			continue
		case t.kind == 'p' && t.text == "}":
			if len(stack) > 0 {
				if name := stack[len(stack)-1]; name == "allprojects" || name == "subprojects" {
					shared--
				}
				stack, quals, levels = stack[:len(stack)-1], quals[:len(quals)-1], levels[:len(levels)-1]
			}
			atStart, head = true, ""
			continue
		}
		if !atStart {
			continue
		}
		atStart = false
		if t.kind == 'i' {
			head, second = t.text, ""
			// "object X", "class X", past any modifiers ("data class X",
			// "public final class X").
			for j := i; j < len(toks) && toks[j].kind == 'i'; j++ {
				if gradleTypeWords[toks[j].text] && j+1 < len(toks) && toks[j+1].kind == 'i' {
					head, second = toks[j].text, toks[j+1].text
					break
				}
			}
		}
		at := level()
		inDeps := at.inDeps
		b.inShared = shared > 0
		switch {
		case decls && at.inManagement && (inDeps || top() == "dependencySet"):
			// dependencyManagement { dependencies { dependency 'g:a:v' } }: a
			// managed version, not a dependency (round 8: it was read as one,
			// a row for a package the build may not use at all).
			b.managedEntry(toks, i, head, top())
		case decls && inDeps:
			if next, ok := b.declaration(toks, i); ok {
				i = next - 1
				atStart, head = true, ""
			}
		case decls && at.inRepos:
			if next, ok := b.repository(toks, i, at); ok {
				i = next - 1
				atStart, head = true, ""
			}
		case decls && head == "mavenBom" && top() == "imports":
			b.mavenBom(toks, i)
		case decls && head == "id" && top() == "plugins":
			b.plugin(toks, i)
		case !decls && !inDeps && t.kind == 'i':
			qual := ""
			if len(quals) > 0 {
				qual = quals[len(quals)-1]
			}
			if qual != gradleNoQualifier {
				b.assignment(toks, i, top(), qual)
			}
		}
	}
}

// gradleNoQualifier stands for the qualifier of a block nested in types
// whose names together are longer than a name can be.
const gradleNoQualifier = "\x00"

// gradleControlFlow are the blocks that do not change what their statements
// are: a declaration under "if" in a dependencies block is a declaration.
var gradleControlFlow = map[string]bool{
	"if": true, "else": true, "for": true, "while": true, "do": true, "try": true, "catch": true, "finally": true, "when": true,
}

// gradleStatement returns the tokens of the statement that begins at
// toks[i], up to its end, and the index after it. A statement ends at an
// end-of-statement token outside brackets that does not follow a comma, a
// colon, an operator or an opening bracket; and before a closure's brace or
// the brace that closes the enclosing block.
func gradleStatement(toks []gradleTok, i int) (stmt []gradleTok, next int) {
	stmt, next = gradleStatementSpan(toks, i)
	// Line ends inside the statement (within brackets, after a comma) are
	// layout, not tokens of it.
	out := stmt[:0:0]
	for _, t := range stmt {
		if t.kind != 'n' {
			out = append(out, t)
		}
	}
	return out, next
}

func gradleStatementSpan(toks []gradleTok, i int) (stmt []gradleTok, next int) {
	depth := 0
	j := i
	for ; j < len(toks); j++ {
		t := toks[j]
		if t.kind == 'p' {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
			case "{", "}":
				if depth <= 0 {
					return toks[i:j], j
				}
			}
			continue
		}
		if t.kind != 'n' {
			continue
		}
		if depth > 0 {
			continue
		}
		if j > i {
			if p := toks[j-1]; p.kind == 'p' && strings.Contains(",:+=", p.text) {
				continue
			}
		}
		return toks[i:j], j
	}
	return toks[i:j], j
}

// gradleSkipBlock returns the index after the block whose opening brace is
// toks[i].
func gradleSkipBlock(toks []gradleTok, i int) int {
	depth := 0
	for j := i; j < len(toks); j++ {
		if toks[j].kind != 'p' {
			continue
		}
		switch toks[j].text {
		case "{":
			depth++
		case "}":
			depth--
			if depth == 0 {
				return j + 1
			}
		}
	}
	return len(toks)
}

// gradleUnparen removes one pair of parentheses that encloses all of toks.
//
// One pass (0.29.209): the leading parentheses are removed for as long as
// each one's partner is the token that mirrors it at the end. It used to
// search for the partner again after every pair removed, which for a
// coordinate inside n pairs read the tokens n times.
func gradleUnparen(toks []gradleTok) []gradleTok {
	lead := 0
	for lead < len(toks) && toks[lead].kind == 'p' && toks[lead].text == "(" {
		lead++
	}
	if lead == 0 || len(toks) < 2 {
		return toks
	}
	// partner[k] is the index of the token that closes the k-th leading
	// parenthesis; found in one pass with a stack of the open ones.
	partner := make([]int, lead)
	for k := range partner {
		partner[k] = -1
	}
	var open []int // indexes of the "(" still open; -1 for one that is not a leading one
	for j, t := range toks {
		if t.kind != 'p' {
			continue
		}
		switch t.text {
		case "(":
			if j < lead {
				open = append(open, j)
			} else {
				open = append(open, -1)
			}
		case ")":
			if len(open) == 0 {
				continue
			}
			if k := open[len(open)-1]; k >= 0 {
				partner[k] = j
			}
			open = open[:len(open)-1]
		}
	}
	strip := 0
	for strip < lead && partner[strip] == len(toks)-1-strip {
		strip++
	}
	return toks[strip : len(toks)-strip]
}

// gradleSplit splits toks at each comma outside brackets.
func gradleSplit(toks []gradleTok) [][]gradleTok {
	var out [][]gradleTok
	depth, from := 0, 0
	for j, t := range toks {
		if t.kind != 'p' {
			continue
		}
		switch t.text {
		case "(", "[":
			depth++
		case ")", "]":
			depth--
		case ",":
			if depth == 0 {
				out = append(out, toks[from:j])
				from = j + 1
			}
		}
	}
	return append(out, toks[from:])
}

// declaration reads the statement at toks[i] of a dependencies block. ok is
// false when the statement opens a nested block (constraints, components),
// which the walk then enters by itself; otherwise next is the index after
// the statement and its closure.
func (b *gradleBuild) declaration(toks []gradleTok, i int) (next int, ok bool) {
	stmt, end := b.statement(toks, i)
	if len(stmt) == 0 {
		return i, false
	}
	config, args := "", stmt[1:]
	switch head := stmt[0]; {
	case head.kind == 's': // Kotlin: "implementation"("g:a:v")
		config = head.text
	case head.kind == 'i' && gradleControlFlow[head.text]:
		return i, false // the walk enters the block
	case head.kind != 'i' || gradleNotConfigurations[head.text]:
		return gradleEndOf(toks, end), true
	case head.text == "add" || strings.HasSuffix(head.text, ".add"): // add("implementation", "g:a:v")
		parts := gradleSplit(gradleUnparen(args))
		if len(parts) < 2 || len(parts[0]) != 1 || parts[0][0].kind != 's' {
			return gradleEndOf(toks, end), true
		}
		config = parts[0][0].text
		args = nil
		for k, p := range parts[1:] {
			if k > 0 {
				args = append(args, gradleTok{kind: 'p', text: ","})
			}
			args = append(args, p...)
		}
	default:
		config = head.text
	}
	if len(args) == 0 {
		if end < len(toks) && toks[end].kind == 'p' && toks[end].text == "{" {
			return i, false // a nested block, not a declaration
		}
		return gradleEndOf(toks, end), true
	}
	b.arguments(config, args, false)
	return gradleEndOf(toks, end), true
}

// gradleEndOf skips the closure that follows a statement, if one does.
func gradleEndOf(toks []gradleTok, end int) int {
	if end < len(toks) && toks[end].kind == 'p' && toks[end].text == "{" {
		return gradleSkipBlock(toks, end)
	}
	return end
}

// arguments reads what a configuration is given.
func (b *gradleBuild) arguments(config string, args []gradleTok, platform bool) {
	args = b.unparen(args)
	if len(args) == 0 {
		return
	}
	// platform(...), enforcedPlatform(...), testFixtures(...): what is inside.
	if args[0].kind == 'i' && len(args) >= 3 && args[1].kind == 'p' && args[1].text == "(" {
		fn := args[0].text
		if i := strings.LastIndexByte(fn, '.'); i >= 0 {
			fn = fn[i+1:]
		}
		switch {
		case fn == "platform" || fn == "enforcedPlatform":
			was := b.enforcing
			b.enforcing = was || fn == "enforcedPlatform"
			b.arguments(config, args[1:], true)
			b.enforcing = was
			return
		case fn == "testFixtures":
			b.arguments(config, args[1:], platform)
			return
		default:
			// project(...), files(...), fileTree(...), gradleApi(), kotlin("x"),
			// any other call: the repository's own projects, files on disk,
			// Gradle's own API. No coordinate.
			return
		}
	}
	// The map: group: 'g', name: 'a', version: 'v' (Kotlin: group = "g", ...).
	if args[0].kind == 'i' && len(args) >= 2 && args[1].kind == 'p' && (args[1].text == ":" || args[1].text == "=") {
		fields := map[string]string{}
		written := map[string]string{}
		for _, part := range gradleSplit(args) {
			if len(part) < 3 || part[0].kind != 'i' {
				continue
			}
			v, w, _ := b.value(part[2:])
			fields[part[0].text], written[part[0].text] = v, w
		}
		if written["name"] == "" {
			return
		}
		w := written["group"] + ":" + written["name"]
		if written["version"] != "" {
			w += ":" + written["version"]
		}
		b.add(config, fields["group"], fields["name"], fields["version"], w, platform)
		return
	}
	// Coordinates, one or several.
	for _, part := range gradleSplit(args) {
		switch {
		case len(part) == 0:
		case part[0].kind == 's':
			b.coordinate(config, part, platform)
		case part[0].kind == 'i' && len(part) == 1:
			b.accessor(config, part[0].text, platform)
		}
	}
}

// accessor reads a name that stands where a coordinate would: a version
// catalogue's accessor (libs.guava, libs.bundles.testing) or a constant that
// holds a coordinate (Deps.guava). One that is neither declares nothing.
func (b *gradleBuild) accessor(config, name string, platform bool) {
	if decls, ok := b.libs.get(strings.TrimSuffix(name, ".get")); ok {
		for _, d := range decls {
			d.config, d.platform, d.forSubprojects = config, platform, b.inShared
			d.versionWritten = strings.Count(d.written, ":") >= 2
			b.decls = append(b.decls, d)
		}
		return
	}
	if v, ok := b.lookup(name); ok && strings.Count(v, ":") >= 1 {
		b.coordinate(config, []gradleTok{{kind: 's', text: v}}, platform)
	}
}

// value reads an expression that stands for one string: a string, a
// variable, or strings and variables joined by +. known is false when a
// part could not be read; written is the expression as it was written.
//
// A value is a version, a coordinate or an address: one longer than
// gradleInterpolatedLimit is none of them and is not known (0.29.209). The
// bound is on what is BUILT, piece by piece: `def x1 = x0 + x0`, line
// after line, doubles a value at every line, and a concatenation of n
// strings built with += copies what it has n times.
func (b *gradleBuild) value(toks []gradleTok) (v, written string, known bool) {
	if len(toks) > gradleValueTokens {
		return "", "", false
	}
	known = true
	var vb, wb strings.Builder
	add := func(sb *strings.Builder, s string) {
		if sb.Len()+len(s) > gradleInterpolatedLimit {
			known = false
			return
		}
		sb.WriteString(s)
	}
	for _, t := range toks {
		switch t.kind {
		case 's':
			s, ok := t.text, true
			if t.interp {
				s, ok = b.interpolate(t.text)
			}
			add(&vb, s)
			add(&wb, t.text)
			known = known && ok
		case 'i':
			s, ok := b.lookup(t.text)
			add(&vb, s)
			add(&wb, "$"+t.text)
			known = known && ok
		case 'p':
			if t.text != "+" {
				return "", wb.String(), false
			}
		}
	}
	if !known {
		return "", wb.String(), false
	}
	return vb.String(), wb.String(), true
}

// coordinate reads group:artifact[:version[:classifier]][@extension], given
// as a string or as strings and variables joined by +.
func (b *gradleBuild) coordinate(config string, toks []gradleTok, platform bool) {
	// Each part of the written coordinate is interpolated on its own, so an
	// unread version does not cost the name.
	// Built in one buffer (a string added to at every token copied what
	// was built so far each time).
	if len(toks) > gradleValueTokens {
		return // longer than a coordinate can be (gradleValueTokens)
	}
	var wb strings.Builder
	for _, t := range toks {
		switch {
		case t.kind == 's' && t.interp:
			wb.WriteString(t.text)
		case t.kind == 's':
			wb.WriteString(strings.ReplaceAll(t.text, "$", "\x00")) // a single-quoted $ is a literal
		case t.kind == 'i':
			wb.WriteString("${" + t.text + "}")
		case t.kind == 'p' && t.text == "+":
		default:
			return
		}
	}
	written := wb.String()
	parts := strings.Split(written, ":")
	if len(parts) < 2 {
		return
	}
	group, okG := b.interpolate(parts[0])
	artifact, okA := b.interpolate(parts[1])
	if !okG || !okA {
		return // a name that cannot be read names nothing
	}
	version := ""
	if len(parts) >= 3 {
		raw := parts[2]
		if at := strings.LastIndexByte(parts[len(parts)-1], '@'); at >= 0 && len(parts) == 3 {
			raw = raw[:strings.LastIndexByte(raw, '@')]
		}
		if v, ok := b.interpolate(raw); ok {
			version = v
		} else if b.writtenWithUnread(raw) {
			before := len(b.decls)
			b.add(config, group, artifact, "", strings.ReplaceAll(written, "\x00", "$"), platform)
			if len(b.decls) > before {
				b.decls[len(b.decls)-1].reason = model.VersionReasonValueNotRead
			}
			return
		}
	} else if at := strings.IndexByte(artifact, '@'); at >= 0 {
		artifact = artifact[:at]
	}
	b.add(config, group, artifact, version, strings.ReplaceAll(written, "\x00", "$"), platform)
}

// add records a declaration, if it names a package.
func (b *gradleBuild) add(config, group, artifact, version, written string, platform bool) {
	group, artifact, version = strings.TrimSpace(group), strings.TrimSpace(artifact), strings.TrimSpace(version)
	// A coordinate with no group (':tests:1.0', or name: 'tests') is a
	// declaration: a file from a flatDir, which no registry holds. It is
	// kept under the name ":tests", and the inventory records why it has no
	// row (no_namespace). project(':core') is a call and never reaches here.
	if artifact == "" || strings.ContainsAny(group+artifact, "\x00${}()\"' \t") {
		return
	}
	versionWritten := strings.Count(written, ":") >= 2
	if !gradleIsVersion(version) {
		version = ""
	}
	b.decls = append(b.decls, gradleDecl{config: config, group: group, artifact: artifact, version: version, written: written,
		platform: platform, enforced: platform && b.enforcing, versionWritten: versionWritten, forSubprojects: b.inShared})
}

// gradleIsVersion reports whether s is one version: not empty, and not a
// range, a dynamic version ("1.+", "latest.release") or an expression.
func gradleIsVersion(s string) bool {
	if s == "" || strings.HasPrefix(s, "latest.") {
		return false
	}
	return !strings.ContainsAny(s, "\x00$+[](){},\"' \t")
}

// interpolate replaces $name, $dotted.name and ${expression} in s. ok is
// false when one of them could not be read.
func (b *gradleBuild) interpolate(s string) (out string, ok bool) {
	if len(s) > gradleInterpolatedLimit {
		return s, false // not a coordinate or a version, whatever it holds
	}
	ok = true
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if sb.Len() > gradleInterpolatedLimit {
			return s, false
		}
		if s[i] != '$' || i+1 >= len(s) {
			sb.WriteByte(s[i])
			continue
		}
		if s[i+1] == '{' {
			end := strings.IndexByte(s[i:], '}')
			if end < 0 {
				return s, false
			}
			v, found := b.lookup(s[i+2 : i+end])
			ok = ok && found
			sb.WriteString(v)
			i += end
			continue
		}
		if !gradleIdentStart(s[i+1]) {
			sb.WriteByte(s[i])
			continue
		}
		j := i + 1
		for j < len(s) && (gradleIdentPart(s[j]) || (s[j] == '.' && j+1 < len(s) && gradleIdentStart(s[j+1]))) {
			j++
		}
		// The longest dotted path that names a variable; what follows it is text.
		path := s[i+1 : j]
		found := false
		for {
			if v, has := b.lookup(path); has {
				sb.WriteString(v)
				sb.WriteString(s[i+1+len(path) : j])
				found = true
				break
			}
			cut := strings.LastIndexByte(path, '.')
			if cut < 0 {
				break
			}
			path = path[:cut]
		}
		ok = ok && found
		i = j - 1
	}
	if sb.Len() > gradleInterpolatedLimit {
		return s, false
	}
	return sb.String(), ok
}

// gradleVarNameBrackets rewrites the bracket and call forms of a variable's
// name to the dotted one. Built once: it was built at every call.
var gradleVarNameBrackets = strings.NewReplacer(`["`, ".", `"]`, "", `['`, ".", `']`, "", `("`, ".", `")`, "", `('`, ".", `')`, "")

// gradleVarName reduces the ways a build file names a variable to one:
// rootProject.ext.x, project.x, ext.x, extra["x"], property("x"),
// versions["x"] are x, x, x, x, x and versions.x.
func gradleVarName(expr string) string {
	e := strings.TrimSpace(expr)
	for _, suffix := range []string{".toString()", ".get()", " as String"} {
		e = strings.TrimSuffix(e, suffix)
	}
	e = gradleVarNameBrackets.Replace(e)
	for changed := true; changed; {
		changed = false
		for _, prefix := range []string{"rootProject.", "project.", "gradle.", "ext.", "extra.", "property.", "findProperty.", "properties."} {
			if strings.HasPrefix(e, prefix) {
				e, changed = e[len(prefix):], true
			}
		}
	}
	return e
}

func (b *gradleBuild) lookup(expr string) (string, bool) {
	name := gradleVarName(expr)
	if b.conflicted[name] {
		return "", false
	}
	if v, ok := b.own[name]; ok {
		return v, true
	}
	v, ok, _ := b.outer.get(name)
	if _, seen := b.reads[name]; !seen {
		b.reads[name] = gradleRead{value: v, ok: ok}
	}
	return v, ok
}

// set gives a variable its value. A second, different value in the same
// file makes it unknown: the two are usually the branches of a condition,
// and choosing one would be a guess.
func (b *gradleBuild) set(name, value string) {
	if b.conflicted[name] {
		return
	}
	if b.setAt != nil {
		b.setAt[name] = b.seq
	}
	if earlier, assigned := b.own[name]; assigned && earlier != value {
		b.conflicted[name] = true
		return
	}
	b.own[name] = value
}

// gradleTypeWords open a block that names a type.
var gradleTypeWords = map[string]bool{"object": true, "class": true, "interface": true, "enum": true}

// gradlePropertyBlocks are the blocks (and "", the top of the file) in which
// a plain assignment sets a property that a version expression can name.
var gradlePropertyBlocks = map[string]bool{"": true, "ext": true, "extra": true, "allprojects": true, "subprojects": true}

// gradleModifiers are the words that may come before a variable's name.
var gradleModifiers = map[string]bool{
	"def": true, "val": true, "var": true, "const": true, "final": true, "private": true, "public": true, "internal": true,
	"static": true, "String": true, "lateinit": true,
}

// assignment reads the statement at toks[i] when it gives a variable a
// string, or a map of strings. block is the named block it is in.
func (b *gradleBuild) assignment(toks []gradleTok, i int, block, qual string) {
	stmt, _ := b.statement(toks, i)
	if len(stmt) < 3 {
		return
	}
	head := stmt[0].text
	b.seq++
	if head == "apply" {
		before := len(b.applies)
		b.applyFrom(b.unparen(stmt[1:]))
		if len(b.applies) > before {
			if b.firstApply == 0 {
				b.firstApply = b.seq
			}
			b.lastApply = b.seq
		}
		return
	}
	// set("name", "value") on ext or extra, or inside their block.
	if base, isSet := strings.CutSuffix(head, ".set"); head == "set" || isSet {
		if head == "set" || gradleVarName(base+".") == "" {
			// Two parts and no more: a third is looked for, and no further.
			parts, more := b.parts(b.unparen(stmt[1:]), 2)
			if !more && len(parts) == 2 && len(parts[0]) == 1 && parts[0][0].kind == 's' {
				b.assign(parts[0][0].text, parts[1])
			}
		}
		return
	}
	// ext["name"] = "value"; extra["name"] = "value".
	if len(stmt) >= 6 && stmt[1].kind == 'p' && stmt[1].text == "[" && stmt[2].kind == 's' && stmt[3].text == "]" && stmt[4].text == "=" && gradleVarName(head+".") == "" {
		b.assign(stmt[2].text, stmt[5:])
		return
	}
	// [modifiers] name [: Type] = value   |   val name by extra("value")
	j := 0
	declared := false
	for j < len(stmt) && stmt[j].kind == 'i' && gradleModifiers[stmt[j].text] {
		j++
		declared = true
	}
	if j >= len(stmt) || stmt[j].kind != 'i' {
		return
	}
	name := stmt[j].text
	j++
	if j+1 < len(stmt) && stmt[j].kind == 'p' && stmt[j].text == ":" && stmt[j+1].kind == 'i' {
		j += 2 // a Kotlin type
	}
	if j >= len(stmt) {
		return
	}
	switch {
	case stmt[j].kind == 'i' && stmt[j].text == "by" && declared:
		rest := stmt[j+1:]
		if len(rest) >= 2 && rest[0].kind == 'i' && (rest[0].text == "extra" || strings.HasSuffix(rest[0].text, ".extra")) {
			b.assign(name, b.unparen(rest[1:]))
		}
	case stmt[j].kind == 'p' && stmt[j].text == "=":
		// A plain "name = value" sets a property of the project where it is
		// written at the top of the file or for all projects ("version =
		// '1.0'", read back as ${project.version}), and a variable inside an
		// ext block. Inside any other block it configures that block
		// (android { ndkVersion = "25" }) and is no variable.
		normal := gradleVarName(name)
		if !declared && normal == name && !gradlePropertyBlocks[block] {
			return
		}
		if declared {
			normal = qual + normal // a constant of an object: Versions.kotlin
		}
		b.assign(normal, stmt[j+1:])
	}
}

// applyFrom records the file an "apply from: ..." statement names: a
// string, or file('x') / rootProject.file('x'). The path is kept as written;
// the tree reader resolves it.
func (b *gradleBuild) applyFrom(args []gradleTok) {
	for _, part := range b.fromParts(args, &b.fromsRead) {
		if len(part) < 3 {
			continue
		}
		v := part[2:]
		switch {
		case v[0].kind == 's':
			b.applies = append(b.applies, v[0].text)
		case v[0].kind == 'i' && strings.HasSuffix(v[0].text, "file") && len(v) >= 4 && v[2].kind == 's':
			path := v[2].text
			if strings.HasPrefix(v[0].text, "rootProject.") {
				path = "$rootDir/" + path
			}
			b.applies = append(b.applies, path)
		}
	}
}

// assign gives name the value toks stand for: a string, or a map of strings
// ([a: '1', b: '2'] or mapOf("a" to "1")), whose entries become name.a.
func (b *gradleBuild) assign(name string, toks []gradleTok) {
	name = gradleVarName(name)
	if name == "" || len(toks) == 0 {
		return
	}
	inner := toks
	isMap := false
	switch {
	case toks[0].kind == 'p' && toks[0].text == "[":
		if b.encloses(toks, 0) {
			inner, isMap = toks[1:len(toks)-1], true
		}
	case toks[0].kind == 'i' && (toks[0].text == "mapOf" || toks[0].text == "mutableMapOf") && len(toks) >= 3:
		if toks[1].kind == 'p' && toks[1].text == "(" && b.encloses(toks, 1) {
			inner, isMap = toks[2:len(toks)-1], true
		}
	}
	if !isMap {
		if v, _, ok := b.value(toks); ok {
			b.set(name, v)
		} else if len(toks) > gradleValueTokens {
			b.notRead(name)
		}
		return
	}
	for _, entry := range gradleSplit(inner) {
		if len(entry) < 3 || (entry[0].kind != 'i' && entry[0].kind != 's') {
			continue
		}
		sep := entry[1]
		if !(sep.kind == 'p' && sep.text == ":") && !(sep.kind == 'i' && sep.text == "to") {
			continue
		}
		if v, _, ok := b.value(entry[2:]); ok {
			b.set(name+"."+entry[0].text, v)
		}
	}
}

// encloses reports whether the bracket at toks[open] is closed by the last
// token of toks: what follows it is one bracketed list, and nothing after.
func (b *gradleBuild) encloses(toks []gradleTok, open int) bool {
	if at, ok := b.idx.place(toks); ok {
		return int(b.idx.partner[at+open]) == at+len(toks)-1
	}
	if len(toks) > gradleMapTokens {
		return false
	}
	// As the index pairs them (gradleIndex.finish): a parenthesis with the
	// last one left open, a square bracket likewise, each kind on its own.
	var round, square []int
	for j := open; j < len(toks); j++ {
		if toks[j].kind != 'p' {
			continue
		}
		switch toks[j].text {
		case "(":
			round = append(round, j)
		case "[":
			square = append(square, j)
		case ")":
			if len(round) > 0 {
				if round[len(round)-1] == open {
					return j == len(toks)-1
				}
				round = round[:len(round)-1]
			}
		case "]":
			if len(square) > 0 {
				if square[len(square)-1] == open {
					return j == len(toks)-1
				}
				square = square[:len(square)-1]
			}
		}
	}
	return false
}

// notRead records a variable whose value is longer than is read.
func (b *gradleBuild) notRead(name string) {
	if b.unread == nil {
		b.unread = map[string]bool{}
	}
	b.unread[name] = true
}

// writtenWithUnread reports a version written with a variable whose value
// was not read (notRead): the variable itself, or an entry of it when it is
// a map.
func (b *gradleBuild) writtenWithUnread(raw string) bool {
	for name := range b.unread {
		for _, lead := range []string{"$" + name, "${" + name} {
			for at := strings.Index(raw, lead); at >= 0; {
				rest := raw[at+len(lead):]
				// The name whole: not the start of a longer one.
				if rest == "" || !(rest[0] == '_' || rest[0] >= '0' && rest[0] <= '9' || rest[0] >= 'a' && rest[0] <= 'z' || rest[0] >= 'A' && rest[0] <= 'Z') {
					return true
				}
				next := strings.Index(rest, lead)
				if next < 0 {
					break
				}
				at += len(lead) + next
			}
		}
	}
	return false
}

// gradleScope is the scope a configuration's dependencies have, from its
// name: a test configuration's are development dependencies (as before this
// reader), an annotation processor's or the build script's own are build
// dependencies, and every other configuration's are runtime.
func gradleScope(config string) string {
	c := strings.ToLower(config)
	switch {
	case strings.Contains(c, "test"):
		return model.ScopeDev
	case c == "classpath" || strings.HasSuffix(c, "annotationprocessor") || strings.HasPrefix(c, "kapt") || strings.HasPrefix(c, "ksp"):
		return model.ScopeBuild
	}
	return model.ScopeRuntime
}

func (b *gradleBuild) libyearDeps() []model.LibyearDep {
	var deps []model.LibyearDep
	for _, d := range b.decls {
		deps = append(deps, model.LibyearDep{
			Name: d.group + ":" + d.artifact, Version: d.version, Requirement: d.written,
			Type: gradleScope(d.config), Manager: "maven", BOMImport: d.platform,
			ManagedEntry: d.managed, VersionSource: d.source, VersionHosts: d.hosts, VersionReason: d.reason,
		})
	}
	return deps
}

// mavenBom reads "mavenBom 'g:a:v'" of Spring's dependency-management
// plugin: a BOM applied to the project, with no row of its own.
func (b *gradleBuild) mavenBom(toks []gradleTok, i int) {
	stmt, _ := b.statement(toks, i)
	args := b.unparen(stmt[1:])
	if len(args) == 0 || args[0].kind != 's' {
		return
	}
	before := len(b.decls)
	b.coordinate("mavenBom", args, true)
	if len(b.decls) > before {
		b.boms = append(b.boms, b.decls[before:]...)
		b.decls = b.decls[:before]
	}
}

// plugin reads one line of a plugins block: id 'x' [version 'y'].
func (b *gradleBuild) plugin(toks []gradleTok, i int) {
	stmt, _ := b.statement(toks, i)
	id, version := "", ""
	if at, ok := b.idx.place(stmt); ok {
		// From the index: the first string that is not empty is the id; the
		// LAST string after it that follows the word `version` is the
		// version; the word
		// `apply` anywhere says the plugin is not applied here.
		x, end := b.idx, at+len(stmt)
		if int(x.nextApply[at]) < end {
			return // "apply false": declared for others, not applied here
		}
		first := int(x.nextString[at])
		if first >= end {
			return
		}
		id = x.flat[first].text
		if v := int(x.lastVersion[end]); v > first {
			t := x.flat[v]
			version = t.text
			if t.interp {
				version, _ = b.interpolate(t.text)
			}
		}
	} else {
		for j, t := range stmt {
			if t.kind != 's' {
				continue
			}
			if id == "" {
				id = t.text
			} else if j > 0 && stmt[j-1].kind == 'i' && stmt[j-1].text == "version" {
				version = t.text
				if t.interp {
					version, _ = b.interpolate(t.text)
				}
			}
		}
		for _, t := range stmt {
			if t.kind == 'i' && t.text == "apply" {
				return // "apply false": declared for others, not applied here
			}
		}
	}
	if id != "" {
		if b.plugins == nil {
			b.plugins = map[string]string{}
		}
		b.plugins[id] = version
	}
}

// springBootBOM adds the BOM the Spring Boot plugin brings: when the
// dependency-management plugin is applied beside it, the build's
// dependencies are managed by spring-boot-dependencies at the plugin's own
// version (Spring Boot's Gradle plugin reference, "Managing dependencies").
// The Boot plugin alone manages nothing.
func (b *gradleBuild) springBootBOM() {
	version := b.plugins["org.springframework.boot"]
	if _, managed := b.plugins["io.spring.dependency-management"]; !managed || !gradleIsVersion(version) {
		return
	}
	b.boms = append(b.boms, gradleDecl{config: "plugin", group: "org.springframework.boot", artifact: "spring-boot-dependencies",
		version: version, written: "org.springframework.boot:spring-boot-dependencies:" + version, platform: true, versionWritten: true})
}

// platformDecls are the BOMs that answer for this file's version-less
// dependencies, in order: the platforms it declares, then the BOMs it names
// otherwise.
func (b *gradleBuild) platformDecls() []gradleDecl {
	var out []gradleDecl
	for _, d := range b.decls {
		if d.platform {
			out = append(out, d)
		}
	}
	return append(out, b.boms...)
}

// parseBuildGradle extracts dependency names from build.gradle /
// build.gradle.kts.
func parseBuildGradle(content string) []string {
	var names []string
	for _, d := range readGradleBuild(content, nil).decls {
		names = append(names, d.group+":"+d.artifact)
	}
	return names
}

// parseBuildGradleVersions extracts the declarations of a build.gradle or
// build.gradle.kts, with the versions the file itself settles.
func parseBuildGradleVersions(content string) []model.LibyearDep {
	return readGradleBuild(content, nil).libyearDeps()
}

// The addresses Gradle's repository shorthands stand for. mavenCentral() is
// given as the address Aveloxis's own default configuration uses for Maven
// Central, so the shorthand and the configured repository are one host.
const (
	gradleMavenCentralURL = "https://repo1.maven.org/maven2"
	gradleGoogleURL       = "https://dl.google.com/dl/android/maven2"
	gradlePluginPortalURL = "https://plugins.gradle.org/m2"
	gradleJCenterURL      = "https://jcenter.bintray.com"
)

// repository reads the entry at toks[i] of a repositories block. ok is
// false for a statement that opens a block the walk should enter
// (exclusiveContent, a condition).
func (b *gradleBuild) repository(toks []gradleTok, i int, at gradleLevel) (next int, ok bool) {
	stmt, end := b.statement(toks, i)
	if len(stmt) == 0 || stmt[0].kind != 'i' {
		return gradleEndOf(toks, end), true
	}
	head := stmt[0].text
	if gradleControlFlow[head] || head == "exclusiveContent" || head == "forRepository" {
		return i, false
	}
	kind, shared := RepositoryForDependencies, at.repoShared
	if at.forPlugins {
		kind = RepositoryForPlugins
	}
	// The entry's own block, when it has one: maven { url ... }.
	var body []gradleTok
	after := end
	if end < len(toks) && toks[end].kind == 'p' && toks[end].text == "{" {
		after = gradleSkipBlock(toks, end)
		body = toks[end+1 : after]
	}
	add := func(d DeclaredRepository) {
		key := d.Kind + "\x00" + d.URL
		if b.repoSeen[key] {
			return
		}
		if b.repoSeen == nil {
			b.repoSeen = map[string]bool{}
		}
		b.repoSeen[key] = true
		b.repos = append(b.repos, gradleRepo{DeclaredRepository: d, shared: shared})
	}
	known := func(address string) {
		if d, ok := classifyDeclaredRepository(address, kind); ok {
			add(d)
		}
	}
	never := func(what, reason string) { add(DeclaredRepository{URL: what, Kind: kind, NotFetched: reason}) }
	switch head {
	case "mavenCentral":
		known(gradleMavenCentralURL)
	case "google":
		known(gradleGoogleURL)
	case "gradlePluginPortal":
		known(gradlePluginPortalURL)
	case "jcenter":
		never(gradleJCenterURL, NotFetchedShutDown)
	case "mavenLocal":
		never("mavenLocal", NotFetchedLocal)
	case "flatDir":
		never("flatDir", NotFetchedFlatDir)
	case "maven", "ivy":
		address, read := b.repositoryURL(gradleUnparen(stmt[1:]), body)
		switch {
		case head == "ivy":
			if !read || address == "" {
				address = "ivy"
			} else if d, ok := classifyDeclaredRepository(address, kind); ok {
				address = d.URL // no credentials are kept
			}
			never(address, NotFetchedIvy)
		case address == "" || (!read && !strings.Contains(address, "://")):
			// No address, or one that is a call or a bare variable: there
			// is nothing of it worth showing.
			never("maven", NotFetchedUnreadable)
		case !read:
			never(address, NotFetchedUnreadable)
		default:
			known(address)
		}
	default:
		// Not a kind this reader knows: recorded, and never fetched.
		never(head, NotFetchedUnreadable)
	}
	return after, true
}

// repositoryURL finds the address of a maven or ivy entry: maven("x"),
// maven(url = "x"), or in its block url "x" / url = uri("x") / setUrl("x").
// read is false when the address is written and cannot be read (a variable
// that is not known, a call); address is then what was written.
func (b *gradleBuild) repositoryURL(args, body []gradleTok) (address string, read bool) {
	expr := func(toks []gradleTok) (string, bool) {
		for len(toks) > 0 && toks[0].kind == 'p' && (toks[0].text == "=" || toks[0].text == ":") {
			toks = toks[1:]
		}
		toks = gradleUnparen(toks)
		// uri("x"), URI.create("x"): what is inside.
		if len(toks) >= 3 && toks[0].kind == 'i' && toks[1].kind == 'p' && toks[1].text == "(" {
			if name := toks[0].text; name == "uri" || strings.HasSuffix(name, ".uri") || strings.HasSuffix(name, "URI.create") || name == "URI" {
				toks = gradleUnparen(toks[1:])
			}
		}
		if len(toks) == 0 {
			return "", false
		}
		v, written, known := b.value(toks)
		if !known {
			return written, false
		}
		return v, true
	}
	if len(args) > 0 {
		if args[0].kind == 'i' && args[0].text == "url" {
			return expr(args[1:])
		}
		return expr(args)
	}
	// The block's url statement, at the block's own level.
	depth, start := 0, true
	for j := 0; j < len(body); j++ {
		t := body[j]
		switch {
		case t.kind == 'n':
			start = true
			continue
		case t.kind == 'p' && t.text == "{":
			depth++
		case t.kind == 'p' && t.text == "}":
			depth--
			start = true
			continue
		case start && depth == 0 && t.kind == 'i' && (t.text == "url" || t.text == "setUrl"):
			stmt, _ := gradleStatement(body, j)
			return expr(stmt[1:])
		}
		start = false
	}
	return "", false
}
