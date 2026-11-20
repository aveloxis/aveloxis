// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
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
	// shared is the walk's work counter (gradleWalkWork); nil outside a
	// tree. walkBound is set when it, not the file's own bound, ended the
	// read.
	shared    *gradleWalkWork
	walkBound bool
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
	// work is what reading this file has cost so far and workLimit what it
	// may cost (gradleWorkFactor); overBound is set when it is passed, and
	// the walk stops.
	work, workLimit int
	overBound       bool
}

// gradleWorkFactor bounds the reading of one build file: its work may be
// this many times the file's size in bytes, in all. The unit is a token of
// a statement handed to a reader, a level of the block stack looked at for
// a statement, or a byte of a variable reference looked up; each is at most
// a byte of the file read once.
//
// Why a bound at all: the reader is not a parser of the language. It
// restarts at every line and every brace, and a statement runs to its end
// however far that is, so text can be written (an unclosed call after every
// brace, a comma at the end of every line, calls nested in closing braces)
// that makes each restart read to the end of the file: the square of its
// size, 25 minutes for 600 KB on a collection worker. Each shape found was
// closed; the bound is for the ones not found.
//
// Why this value: a build read in the ordinary way costs about its size
// once (TestRealGradleBuildsAreFarFromTheWorkBound measures the corpus's
// builds and a large generated one, and asks for a fourfold margin). The
// factor is the depth figure this package already uses for a chain of poms
// (pomChainLimit, 32), read here as: no token of a real build is part of
// more than 32 statements. PROVISIONAL (summary/71, section 8): a real
// build that passes it is reported as a manifest that could not be parsed,
// by name, in the log and in coverage. So is every build file that depends
// on a file that passed it (a settings file, a build file above, an
// applied script, a buildSrc source): gradleTree.noteUnread names the file
// in the log, and declared fails what depends on it, naming it (0.29.209).
//
// The rule the bound belongs to (0.29.209, after two review rounds each
// found more shapes): every loop over a build file is linear in its size
// or is counted here. TestBuildFileReadersScaleLinearly is its pin: a new
// hostile shape is one line there.
const gradleWorkFactor = pomChainLimit

// gradleWalkFactor is the WALK's allowance: this many units for each byte
// of Gradle file the walk reads (each file once). It is not the file's
// factor. A file's bound says no one reading of it may cost more than
// gradleWorkFactor times its size; the walk's says what all the readings
// together may cost, and an honest build makes readings the file's bound
// knows nothing of: a publishing script that reads a property every module
// sets (its artifact's name) is read again for each module, because each
// gives it another answer.
//
// PROVISIONAL (summary/71, section 8). Its derivation:
//
// Room for honest builds. TestAnHonestPublishingBuildIsFarInsideTheWalksAllowance
// is 1,000 modules of about a kilobyte each reading such a property through
// a 60 KB script: 27 million units for 1.1 MB of files, about 25 units a
// byte read (a reading costs half a unit a byte of the script, and the
// script is sixty times a module). With the file's factor as the walk's,
// that build lost 138 modules (round 4). Ten times the file's factor is a
// tenfold margin over it, with a little to spare.
//
// A bound on a worker's time, and the price of that margin. Where the
// allowance is what ends a walk (projects that each read one script their
// own way), a unit measured about 60 ns (0.29.211, an Apple M-series
// core: 139 million units in 8.8 s), most of it the allocation of each
// reading's assignments. 320 units a byte is then about 20 microseconds a
// byte: some three minutes for every 8 MiB of Gradle files a walk reads,
// whatever the files share, where the file's factor as the walk's gave
// seventeen seconds. One number does not give both a tenfold margin for
// that honest build and a bound of seconds; the margin was chosen.
//
// Round 5 made that a bound in fact: every unit is charged where bytes are
// touched (the bytes tokenised, gradleScanBytes; the tokens passed,
// gradleTokensPerUnit; the tokens of each statement; the layers of the
// scope a lookup passes), so no reading costs time and no units, and a
// script that is applied is tokenised once for the walk and read again from
// its tokens (gradleTree.scan). TestTheUnitsOfBothWalksAreUnitsOfTime
// measures each of those at its dearest: 15 to 41 ns a unit, so the three
// minutes above is the bound with room, and walkBackstopFloor's clock is
// set at ten times it.
const gradleWalkFactor = 10 * gradleWorkFactor

// gradleScanBytes is how many bytes of a file are tokenised for one unit,
// and gradleTokensPerUnit how many of its tokens a pass over them steps
// through for one: what makes a reading's units a measure of its TIME
// whatever the file holds (review round 5: units were counted for the
// tokens of statements, so blank lines and comments cost time and nothing;
// 600 projects applying an 8 MiB script of them took 44 s for 0.02% of the
// walk's allowance).
//
// PROVISIONAL (summary/71, section 8). Their derivation is a measurement
// (TestEveryByteTokenisedIsCharged logs it): tokenising is dearest for a
// file of line ends, a token each, at about 14 ns a byte, and cheapest for
// comments, under 1 ns; a unit of the reader's other work is about 60 ns
// (gradleWalkFactor). Four bytes to the unit is the power of two at which
// the dearest tokenising costs no more per unit than that. A pass over
// tokens that are not part of a statement (line ends) is a comparison each,
// a few nanoseconds: sixteen to the unit.
const (
	gradleScanBytes     = 4
	gradleTokensPerUnit = 16
)

// errGradleWorkBound: reading the file passed gradleWorkFactor.
var errGradleWorkBound = errors.New("the build file is not one this reader can read within its bound (gradleWorkFactor); nothing was taken from it")

// spend records n units of work, against the file's bound and the walk's.
//
// The walk is charged no more than the file's own bound for one reading of
// it (the step that passes the bound may be as long as the file): what a
// file adds to the walk's allowance is exactly what one reading of it can
// cost, so a file that uses its whole bound takes nothing from the files
// read after it.
func (b *gradleBuild) spend(n int) {
	charge := n
	if room := b.workLimit - b.work; charge > room {
		charge = max(room, 0)
	}
	b.work += n
	if b.work > b.workLimit {
		b.overBound = true
	}
	if b.shared != nil && charge > 0 && !b.shared.spend(charge) {
		b.overBound, b.walkBound = true, true
	}
}

// spendWalk records n units that are the walk's and not the file's: work
// whose amount depends on where the file is read, not on its text (the
// layers of the scope a lookup passes). A file's own bound is a fact about
// the file (review round 5: charged to the file, the layers put a script
// inside its bound under a shallow scope and past it under a deep one, and
// which projects were refused depended on the order they were read in).
func (b *gradleBuild) spendWalk(n int) {
	if b.shared != nil && n > 0 && !b.shared.spend(n) {
		b.overBound, b.walkBound = true, true
	}
}

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
// names. A file that was not read whole leaves nothing.
func (b *gradleBuild) result() (own map[string]string, gone map[string]bool) {
	if b.overBound || (len(b.own) == 0 && len(b.conflicted) == 0) {
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

// gradleWalkWork is the walk's counter of the Gradle readers' work: every
// unit a file's reading spends, against gradleWorkFactor units per byte of
// Gradle file the walk has read (each file once). A file's own bound does
// not bound the walk: a script applied by every project is read for each
// of them, projects times its size. A reading that takes nothing new from
// outside is not repeated (the memo), so an ordinary build spends its files
// about once; past the allowance the files still to be read are not read,
// and are reported so, by name.
type gradleWalkWork struct {
	used, limit int
	// acct is the walk's account: its clock is looked at here, where the
	// work is charged (the backstop).
	acct *walkAccount
}

func (w *gradleWalkWork) spend(n int) bool {
	w.used += n
	if w.acct != nil && w.acct.tick(n) {
		return false
	}
	return w.used <= w.limit
}

// statement is gradleStatement, with its cost recorded.
func (b *gradleBuild) statement(toks []gradleTok, i int) (stmt []gradleTok, next int) {
	stmt, next = gradleStatement(toks, i)
	b.spend(next - i + 1)
	return stmt, next
}

// gradleInterpolatedLimit bounds a string that is interpolated and what the
// interpolation produces: pomInterpolatedLimit, for the same reason (a
// variable that is another one twice over doubles at every line).
const gradleInterpolatedLimit = pomInterpolatedLimit

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
	return readGradleBuildWith(content, outer, nil, nil)
}

// readGradleBuildWith is readGradleBuild with the catalogues' accessors and
// the walk's work counter (nil outside a tree).
func readGradleBuildWith(content string, outer *gradleVars, libs *gradleLibs, shared *gradleWalkWork) *gradleBuild {
	return readGradleTokens(gradleTokens(content), len(content), true, outer, libs, shared)
}

// readGradleTokens reads a build file of size bytes from its tokens.
// tokenised says they were made for this reading (and not kept from an
// earlier one of the same walk): the bytes are then charged.
func readGradleTokens(toks []gradleTok, size int, tokenised bool, outer *gradleVars, libs *gradleLibs, shared *gradleWalkWork) *gradleBuild {
	b := &gradleBuild{outer: outer, own: map[string]string{}, conflicted: map[string]bool{}, libs: libs,
		reads: map[string]gradleRead{}, shared: shared, workLimit: gradleWorkFactor * (size + 1)}
	// The bytes tokenised, and the two passes over the tokens: charged
	// before either is made, whatever the tokens are.
	units := 2 * (len(toks) / gradleTokensPerUnit)
	if tokenised {
		units += size / gradleScanBytes
	}
	b.spend(units)
	// Two passes: the assignments first, so a variable assigned below its
	// use (or in a later ext block) is known when the declarations are read.
	b.walk(toks, false)
	b.walk(toks, true)
	if b.overBound {
		// Not read whole: what was gathered is not the build, and a part
		// of one reads as all of it. The variables known from outside stand.
		b.decls, b.boms, b.repos, b.repoSeen, b.applies, b.plugins = nil, nil, nil, nil, nil, nil
		return b
	}
	b.springBootBOM()
	return b
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
		if b.overBound {
			return
		}
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
				stack, quals = stack[:len(stack)-1], quals[:len(quals)-1]
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
		// What looking at the stack for this statement costs: the blocks
		// that are control flow are passed over one by one, and the
		// qualifier is built from every level.
		b.spend(len(stack) + 1)
		inDeps := gradleInDependencies(stack)
		b.inShared = shared > 0
		switch {
		case decls && inDeps:
			if next, ok := b.declaration(toks, i); ok {
				i = next - 1
				atStart, head = true, ""
			}
		case decls && gradleInRepositories(stack):
			if next, ok := b.repository(toks, i, stack); ok {
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

// gradleInDependencies reports whether the innermost block that is not
// control flow is a dependencies block (project.dependencies and
// buildscript's count; constraints and components inside one do not).
func gradleInDependencies(stack []string) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		name := stack[i]
		if gradleControlFlow[name] {
			continue
		}
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		return name == "dependencies"
	}
	return false
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
	// Each call reads what it is given once, and a call nested in a call
	// (platform(platform(...))) reads the inner part again: counted, so a
	// coordinate inside n calls ends at the work bound.
	b.spend(len(args))
	if b.overBound {
		return
	}
	args = gradleUnparen(args)
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
			b.arguments(config, args[1:], true)
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
		platform: platform, versionWritten: versionWritten, forSubprojects: b.inShared})
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
			b.spend(len(path)) // a lookup reads the path it is given
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
	v, ok, layers := b.outer.get(name)
	b.spendWalk(layers)
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
	if head == "apply" {
		b.applyFrom(gradleUnparen(stmt[1:]))
		return
	}
	// set("name", "value") on ext or extra, or inside their block.
	if base, isSet := strings.CutSuffix(head, ".set"); head == "set" || isSet {
		if head == "set" || gradleVarName(base+".") == "" {
			parts := gradleSplit(gradleUnparen(stmt[1:]))
			if len(parts) == 2 && len(parts[0]) == 1 && parts[0][0].kind == 's' {
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
			b.assign(name, gradleUnparen(rest[1:]))
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
	for _, part := range gradleSplit(args) {
		if len(part) < 3 || part[0].kind != 'i' || part[0].text != "from" {
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
		if last := toks[len(toks)-1]; last.kind == 'p' && last.text == "]" {
			inner, isMap = toks[1:len(toks)-1], true
		}
	case toks[0].kind == 'i' && (toks[0].text == "mapOf" || toks[0].text == "mutableMapOf") && len(toks) >= 3:
		inner, isMap = gradleUnparen(toks[1:]), true
	}
	if !isMap {
		if v, _, ok := b.value(toks); ok {
			b.set(name, v)
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
	args := gradleUnparen(stmt[1:])
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

// gradleInRepositories reports whether the innermost block that is not
// control flow is a repositories block, or the forRepository block of an
// exclusiveContent inside one: the places a repository is declared.
func gradleInRepositories(stack []string) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		name := stack[i]
		if gradleControlFlow[name] {
			continue
		}
		return name == "repositories" || name == "forRepository"
	}
	return false
}

// repository reads the entry at toks[i] of a repositories block. ok is
// false for a statement that opens a block the walk should enter
// (exclusiveContent, a condition).
func (b *gradleBuild) repository(toks []gradleTok, i int, stack []string) (next int, ok bool) {
	stmt, end := b.statement(toks, i)
	if len(stmt) == 0 || stmt[0].kind != 'i' {
		return gradleEndOf(toks, end), true
	}
	head := stmt[0].text
	if gradleControlFlow[head] || head == "exclusiveContent" || head == "forRepository" {
		return i, false
	}
	kind, shared := RepositoryForDependencies, false
	for _, name := range stack {
		switch name {
		case "pluginManagement", "buildscript":
			kind = RepositoryForPlugins
		case "allprojects", "subprojects", "dependencyResolutionManagement":
			shared = true
		}
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
