// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/model"
)

// canonicalManifestName is the ONE spelling the three manifest dispatches
// (the inventory lookup, ParseDependencyFile's switch, the libyear walk's
// switch) compare: the requirements family is matched case-insensitively
// (`Requirements.txt`, `REQUIREMENTS.TXT` were collected by nothing —
// worklist §4, v0.29.68) and every other name byte-exact, because the other
// mixed-case keys (Gemfile, Cargo.toml, Package.swift, Pipfile, …) are the
// real names and lowering them would drop those inventories. Decided as the
// class: the requirements family only.
func canonicalManifestName(base string) string {
	if strings.EqualFold(base, "requirements.txt") {
		return "requirements.txt"
	}
	return base
}

func parsePackageJSON(data []byte) ([]string, error) {
	var pkg struct {
		Dependencies    map[string]interface{} `json:"dependencies"`
		DevDependencies map[string]interface{} `json:"devDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []string
	for name := range pkg.Dependencies {
		deps = append(deps, name)
	}
	for name := range pkg.DevDependencies {
		deps = append(deps, name)
	}
	return deps, nil
}

// cutPyInlineComment returns s up to an inline comment: a '#' that opens
// the text or follows whitespace (space or tab), pip's own rule. It is the
// ONE comment cut for every Python requirement reader, inventory and libyear
// alike (SR-17; review round 4 on v0.29.66: the readers cut " #" in three
// places, one of them missing, and none cut a tab). A '#' inside a URL
// (a "#sha256=" fragment) has no whitespace before it and is kept.
func cutPyInlineComment(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t') {
			return strings.TrimSpace(s[:i])
		}
	}
	return strings.TrimSpace(s)
}

// cleanPyRequirementLine reduces one requirements-file line to the
// requirement itself, or "" when the line holds none: blank, comment and
// option lines ("-r", "-e", "--hash=…") are skipped; a trailing "\"
// continuation, an inline comment, a PEP 508 environment marker and
// same-line pip options are stripped. ONE cleanup for both requirements
// readers (SR-17; v0.29.62 review round 1: only the libyear reader
// stripped " --" options, so "name --global-option=x" was a dependency
// there and nothing on the name path).
func cleanPyRequirementLine(raw string) string {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "-") {
		return ""
	}
	// v0.27.71 (the zephyr false-CRITICAL incident): pip's hash-pinned
	// format ends the pin line with a backslash continuation
	// ("pyyaml==6.0.3 \"); it survived into the version and defeated
	// OSV's version matching.
	line = strings.TrimSpace(strings.TrimSuffix(line, "\\"))
	// Inline comments, PEP 508 environment markers, and same-line pip
	// args are not version content ("attrs==3.1.0  # Apache-2.0",
	// "croniter==0.4.6 ; sys_platform == 'win32'").
	line = cutPyInlineComment(line)
	if idx := strings.Index(line, ";"); idx > 0 {
		line = strings.TrimSpace(line[:idx])
	}
	if idx := strings.Index(line, " --"); idx > 0 {
		line = strings.TrimSpace(line[:idx])
	}
	return line
}

func ParseRequirementsTxt(content string) []string {
	var deps []string
	for _, raw := range strings.Split(content, "\n") {
		line := cleanPyRequirementLine(raw)
		if line == "" {
			continue
		}
		if name := pyInventoryName(line); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

func parseGoMod(content string) []string {
	var deps []string
	inRequire := false
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "require (" {
			inRequire = true
			continue
		}
		if line == ")" {
			inRequire = false
			continue
		}
		if inRequire && line != "" && !strings.HasPrefix(line, "//") {
			parts := strings.Fields(line)
			if len(parts) >= 1 {
				deps = append(deps, parts[0])
			}
		}
	}
	return deps
}

// parseTOMLDeps lists the dependency names declared in one TOML section
// (Cargo [dependencies], poetry's [tool.poetry.dependencies]) through the
// shared table scanner (manifest_toml.go), whatever their source.
func parseTOMLDeps(content, section string) []string {
	var deps []string
	for _, e := range scanTOMLDepTables(content, map[string]bool{section: true}) {
		if e.Name != "python" {
			deps = append(deps, e.Name)
		}
	}
	return deps
}

// gemDeclaredName is the gem a `gem` line declares, or "" if the line
// declares none. ONE spelling for both readers of a Gemfile (SR-17,
// v0.29.57): the inventory used to split the raw line on `\'` and fall back
// to `"` only when the line held no apostrophe at all, which meant
//
//   - an apostrophe in a trailing comment became the delimiter, so
//     `gem "rails" # don't upgrade` was inventoried as a gem named
//     `t upgrade` and rails was lost; and
//   - a double-quoted name with a single-quoted CONSTRAINT
//     (`gem "rails", '~> 7.0'`) inventoried the constraint — 387 rows of
//     `aveloxis_large` are a Ruby "package" called `~> 7.0` or similar.
//
// Item 71 (v0.29.69): "the first comma-separated argument" was not the name
// either. A trailing modifier or an interpolated name became a fabricated gem
// (`json" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9`,
// `github-pages" if ENV["GH_PAGES"]`, `beaker-#{ENV['BEAKER_HYPERVISOR']}`),
// stored in the inventory and minted into purls. The name is now read the way
// Ruby reads it: parseGemDeclaration.
func gemDeclaredName(line string) string {
	d, _ := parseGemDeclaration(line)
	return d.Name
}

// gemDeclaration is one `gem` call of a Gemfile: the static name, the
// version requirements given as string literals right after it, and Text,
// the call's source without its comment or a trailing `if`/`unless`
// modifier — what a reader stores as the declared requirement, so the
// modifier's `< '1.9'` or `=~` never reaches classifyRequirement. Keyword
// options stay in Text, as they always have in the stored requirement.
type gemDeclaration struct {
	Name         string
	Requirements []string
	// Declared is the call through its last version literal — what the
	// requirement classifier reads (ClassificationText). Keyword options are
	// not version material (`:require => false` read as a `>` lower bound;
	// `install_if: -> { RUBY_VERSION < "3.0" }` as an upper one), so they
	// stop here (item 71 review round 2).
	Declared string
	// Text is the call without its comment or a trailing modifier, keyword
	// options included — the stored requirement (the raw manifest truth the
	// display shows, `path: "../shared"` included) and where the inline
	// group scope is read.
	Text string
}

// parseGemDeclaration reads a `gem` call the way Ruby does, for every reader
// of a Gemfile (SR-17). The first argument must be a string literal — "x",
// 'x', %q(x), %Q(x) or %(x) — and the name is exactly its contents. The
// requirements are the string literals that follow, each after a comma;
// anything else (keyword options, a splat, a computed value, a trailing `if`
// or `unless` modifier, a comment) ends the list. A name built by
// interpolation (`"beaker-#{hv}"`) is not a static name and cannot be
// resolved, so the line declares nothing — the same as any other line the
// readers cannot read (no log: a manifest reader reports what it found). An
// interpolated requirement is dropped with the ones after it.
func parseGemDeclaration(line string) (gemDeclaration, bool) {
	rest := strings.TrimLeft(line, " \t")
	start := rest
	if !strings.HasPrefix(rest, "gem") {
		return gemDeclaration{}, false
	}
	// `gem "x"`, `gem("x")` and `gem"x"` are all the call; `gemspec`,
	// `gems "x"` and `gem_name "x"` are not, and fail below because a
	// string literal cannot start with an identifier character.
	rest = strings.TrimLeft(rest[3:], " \t(")
	name, interpolated, rest, ok := rubyStringLiteral(rest)
	if !ok || interpolated || name == "" {
		return gemDeclaration{}, false
	}
	d := gemDeclaration{Name: name}
	for {
		rest = strings.TrimLeft(rest, " \t")
		if !strings.HasPrefix(rest, ",") {
			break
		}
		req, interp, after, ok := rubyStringLiteral(strings.TrimLeft(rest[1:], " \t"))
		if !ok || interp {
			break
		}
		d.Requirements = append(d.Requirements, req)
		rest = after
	}
	d.Declared = strings.TrimRight(start[:len(start)-len(rest)], " \t")
	tail := stripHashComment(rest)
	if cut := rubyModifierIndex(tail); cut >= 0 {
		tail = tail[:cut]
	}
	d.Text = strings.TrimRight(start[:len(start)-len(rest)]+tail, " \t")
	return d, true
}

// rubyModifierIndex is the offset in s of a statement modifier keyword
// (`if`, `unless`) outside any string, or -1. A keyword must stand as a word
// (at the start or after a space, followed by a space, `(` or the end): `if:`
// (a hash key) and `diff` are not modifiers. Declined edges (item 71 review): `if!x` / `)if` with no
// space are left uncut (legal Ruby, not seen in Gemfiles), and an `unless`
// inside an `install_if: -> { ... }` block cuts inside the option (the
// declared literals are already read; only the stored text is shorter).
func rubyModifierIndex(s string) int {
	at := -1
	scanOutsideStrings(s, func(i int, c byte) bool {
		if i > 0 && s[i-1] != ' ' && s[i-1] != '\t' {
			return true
		}
		for _, kw := range []string{"if", "unless"} {
			if !strings.HasPrefix(s[i:], kw) {
				continue
			}
			if j := i + len(kw); j == len(s) || s[j] == ' ' || s[j] == '\t' || s[j] == '(' {
				at = i
				return false
			}
		}
		return true
	})
	return at
}

// rubyStringLiteral reads the Ruby string literal at the start of s: its
// contents, whether it interpolates (`#{` in a double-quoted or %Q/% form),
// and the text after it. ok is false when s does not start with a literal or
// the literal is unterminated. Delimiters of the %-forms do not nest (no gem
// name contains one).
func rubyStringLiteral(s string) (contents string, interpolated bool, rest string, ok bool) {
	if s == "" {
		return "", false, s, false
	}
	var closer byte
	interpolating := false
	body := 0
	switch {
	case s[0] == '"':
		closer, interpolating, body = '"', true, 1
	case s[0] == '\'':
		closer, body = '\'', 1
	case strings.HasPrefix(s, "%q") && len(s) > 2:
		closer, body = rubyPercentCloser(s[2]), 3
	case strings.HasPrefix(s, "%Q") && len(s) > 2:
		closer, interpolating, body = rubyPercentCloser(s[2]), true, 3
	case s[0] == '%' && len(s) > 1 && strings.IndexByte("([{<", s[1]) >= 0:
		closer, interpolating, body = rubyPercentCloser(s[1]), true, 2
	default:
		return "", false, s, false
	}
	if closer == 0 {
		return "", false, s, false
	}
	var b strings.Builder
	for i := body; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			i++
			b.WriteByte(s[i])
		case c == closer:
			return b.String(), interpolated, s[i+1:], true
		default:
			if interpolating && c == '#' && i+1 < len(s) && s[i+1] == '{' {
				interpolated = true
			}
			b.WriteByte(c)
		}
	}
	return "", false, s, false
}

// rubyPercentCloser is the closing delimiter of a %-literal opened by open,
// or 0 when open cannot open one (alphanumerics and space).
func rubyPercentCloser(open byte) byte {
	switch open {
	case '(':
		return ')'
	case '[':
		return ']'
	case '{':
		return '}'
	case '<':
		return '>'
	}
	if open == ' ' || open == '\t' || ('a' <= open && open <= 'z') || ('A' <= open && open <= 'Z') || ('0' <= open && open <= '9') {
		return 0
	}
	return open
}

func parseGemfile(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		if name := gemDeclaredName(line); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

func parsePomXML(content string) []string {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []string
	// Simple extraction of <artifactId> within <dependency> blocks.
	inDep := false
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "<dependency>") {
			inDep = true
		}
		if strings.Contains(line, "</dependency>") {
			inDep = false
		}
		if inDep && strings.Contains(line, "<artifactId>") {
			name := strings.TrimPrefix(line, "<artifactId>")
			name = strings.TrimSuffix(name, "</artifactId>")
			name = strings.TrimSpace(name)
			if name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// parsePyprojectDeps extracts dependency names from pyproject.toml.
// Handles both PEP 621 ([project] dependencies = [...]) and Poetry
// ([tool.poetry.dependencies]) formats.
func parsePyprojectDeps(content string) ([]string, error) {
	var deps []string

	// Try PEP 621 format: [project] section with dependencies = ["pkg>=1.0", ...]
	deps = append(deps, parsePEP621Deps(content)...)

	// Try Poetry format: [tool.poetry.dependencies] with key = value pairs
	poetryDeps := parseTOMLDeps(content, "[tool.poetry.dependencies]")
	deps = append(deps, poetryDeps...)

	return deps, nil
}

// parsePEP621Deps extracts dependency names from PEP 621 format pyproject.toml.
// Parses the dependencies = [...] array under [project].
func parsePEP621Deps(content string) []string {
	var deps []string
	lines := strings.Split(content, "\n")
	inProject := false
	inDepsArray := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Track [project] section.
		if tomlHeaderIs(trimmed, "project") {
			inProject = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") && !tomlHeaderIs(trimmed, "project") {
			inProject = false
			inDepsArray = false
			continue
		}

		if !inProject {
			continue
		}

		// Detect dependencies = [ start. Whether a bracket opens or closes
		// the array is decided OUTSIDE strings and comments (v0.29.57): the
		// `]` of an extra — `dependencies = ["celery[redis]>=5.0",` — used
		// to end the array on its opening line, dropping every dependency
		// after it from the inventory, from libyear and from the OSV scan.
		if strings.HasPrefix(trimmed, "dependencies") && strings.Contains(trimmed, "=") {
			code := stripHashComment(trimmed)
			opens, closes := listBrackets(code)
			if opens {
				// Items may share the opening line whether or not the array
				// closes on it.
				deps = append(deps, extractPEP621DepsFromLine(code)...)
				inDepsArray = !closes
			}
			continue
		}

		if inDepsArray {
			// The closer may carry the last item (`"celery>=5.0"]`), so the
			// line is read before the array ends; a line that is only a
			// closer yields nothing.
			code := stripHashComment(trimmed)
			_, closes := listBrackets(code)
			deps = append(deps, extractPEP621DepsFromLine(code)...)
			if closes {
				inDepsArray = false
			}
		}
	}
	return deps
}

// extractPEP621DepName extracts a package name from a PEP 621 dependency string.
// Input: `"flask>=2.0",` or `"tomli>=2.2.1 ; python_version < '3.11'",`
// Output: `flask` or `tomli`
func extractPEP621DepName(line string) string {
	line = strings.TrimSpace(line)
	// TOML literal strings are legal in pyproject.toml, so an item may be
	// single-quoted. Trimming only '"' kept the apostrophe in the name
	// ('tomli) while the libyear path stripped it — the two disagreed about
	// the same file (v0.29.56).
	line = strings.Trim(line, "\"',")
	line = strings.TrimSpace(line)
	// The inline comment goes before the marker strip, because a comment
	// can contain a ';' (review round 4 on v0.29.66: PEP 621 kept
	// "requests # HTTP" whole, so it failed the name gate while the
	// libyear side cut the comment and looked requests up).
	line = cutPyInlineComment(line)
	if line == "" {
		return ""
	}
	// Strip environment markers: everything after ';'
	if idx := strings.Index(line, ";"); idx > 0 {
		line = strings.TrimSpace(line[:idx])
	}
	return pyInventoryName(line)
}

// extractPEP621DepsFromLine extracts dep names from an inline deps array.
func extractPEP621DepsFromLine(line string) []string {
	var deps []string
	// Bounds come from the shared scanner: a bracket inside a quoted item
	// is part of the value, not the array (v0.29.57).
	inner := listInner(line)

	// Only a comma BETWEEN items separates them (v0.29.56). A comma inside
	// the quoted item is data — a bounded range ("requests>=2.31.0,<3.0.0")
	// or an extras list ("celery[redis,auth]>=5.3") — and splitting on it
	// cuts one dependency into fragments, so the real package is never
	// resolved and a fragment is inventoried in its place. (Which fragments
	// depends on how each path strips extras, so this comment does not name
	// them; TestInlineDeclarationsSplitOnStructuralCommasOnly asserts the
	// CORRECT output for both paths.)
	for _, item := range splitTOMLTopLevel(inner) {
		if name := extractPEP621DepName(item); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// parseSetupPyDeps extracts dependency names from setup.py install_requires.
func parseSetupPyDeps(content string) ([]string, error) {
	return extractSetupPyInstallRequires(content), nil
}

// setupKeywordValue returns, line by line, the text of the VALUE of one
// setup() keyword argument (a list `[...]` or a dict `{...}`): from its
// opening bracket through the bracket that closes it, and NOTHING after.
// Lists joined with `+` (`['a'] + ['b']`) are all part of the value.
//
// It is the one reader of "where does this keyword's value end" for every
// setup.py reader (0.29.163). Before, each took everything from the opening
// bracket to the end of the line, so a setup() call written on one line gave
// the next keyword's strings to this one: `install_requires=['click'],
// extras_require={'dev': ['black']}` stored `black` and the extra's NAME,
// `dev`, as runtime requirements, and `dev` is a different package on PyPI.
//
// A requirement someone commented out is not one, and brackets inside a
// quoted requirement (`"celery[redis]>=5.0"`) are not nesting: both rules are
// the shared helpers' (stripHashComment, scanOutsideStrings).
func setupKeywordValue(content, keyword string) []string {
	var chunks []string
	depth := 0
	// take consumes s, which is inside the value at the given depth (or, at
	// depth 0, starts at the value's opening bracket), up to the bracket
	// that closes the value. It returns the part taken and what follows it.
	take := func(s string) (taken, rest string) {
		end := -1
		scanOutsideStrings(s, func(i int, c byte) bool {
			switch c {
			case '[', '{':
				depth++
			case ']', '}':
				depth--
				if depth <= 0 {
					depth = 0
					end = i
					return false
				}
			}
			return true
		})
		if end < 0 {
			return s, ""
		}
		return s[:end+1], s[end+1:]
	}
	for _, line := range strings.Split(content, "\n") {
		code := stripHashComment(strings.TrimSpace(line))
		if depth == 0 {
			at := strings.Index(code, keyword)
			if at < 0 {
				continue
			}
			open := strings.IndexAny(code[at:], "[{")
			if open < 0 {
				continue
			}
			code = code[at+open:]
		}
		for code != "" {
			taken, rest := take(code)
			chunks = append(chunks, taken)
			if depth > 0 {
				break // the value continues on the next line
			}
			// The value closed. Only a `+` joining another list continues it.
			rest = strings.TrimSpace(rest)
			if !strings.HasPrefix(rest, "+") {
				break
			}
			rest = strings.TrimSpace(rest[1:])
			if !strings.HasPrefix(rest, "[") {
				break
			}
			code = rest
		}
	}
	return chunks
}

// extractSetupPyInstallRequires parses the install_requires=[...] list from setup.py.
func extractSetupPyInstallRequires(content string) []string {
	var deps []string
	for _, chunk := range setupKeywordValue(content, "install_requires") {
		deps = append(deps, extractQuotedPyDeps(chunk)...)
	}
	return deps
}

// extractQuotedPyDeps extracts package names from Python quoted dependency strings.
// Input: `'flask>=2.0', "requests==2.28.0",`
// Output: ["flask", "requests"]
func extractQuotedPyDeps(line string) []string {
	var deps []string
	// Extract all quoted strings.
	for _, quote := range []byte{'\'', '"'} {
		rest := line
		for {
			start := strings.IndexByte(rest, quote)
			if start < 0 {
				break
			}
			end := strings.IndexByte(rest[start+1:], quote)
			if end < 0 {
				break
			}
			depStr := rest[start+1 : start+1+end]
			rest = rest[start+1+end+1:]
			if name := extractPyDepName(depStr); name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// extractPyDepName extracts the package name from a Python requirement string.
// "flask>=2.0" -> "flask", "numpy" -> "numpy"
func extractPyDepName(req string) string {
	req = strings.TrimSpace(req)
	if req == "" {
		return ""
	}
	// The inline comment goes before the marker strip, because a comment
	// can contain a ';' (review rounds 3-4 on v0.29.66).
	req = cutPyInlineComment(req)
	// Strip environment markers.
	if idx := strings.Index(req, ";"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	return pyInventoryName(req)
}

// pyInventoryName is the ONE name rule for the Python dependency inventory
// (requirements.txt, PEP 621 dependencies, setup.py/setup.cfg
// install_requires; SR-17, PR #212 review): given a requirement with its
// comment and environment marker already stripped, it returns the package
// name, or "" when the requirement names none.
//   - A PEP 508 direct reference names its package before the '@'
//     ("requests @ git+https://…"); an '@' after a "://" belongs to a bare
//     URL, which names no package.
//   - Extras and the version specifier (operators or the parenthesized
//     form) are not part of the name.
//   - The result must be a PEP 508 name: a URL, path or prose line is not.
func pyInventoryName(req string) string {
	if at := strings.IndexByte(req, '@'); at > 0 && !strings.Contains(req[:at], "://") && isPyDirectReferenceURI(req[at+1:]) {
		req = req[:at]
	}
	name, _ := splitPyNameSpec(stripPyExtras(req))
	if !isPyRequirementName(name) {
		return ""
	}
	return name
}

// isPyDirectReferenceURI reports whether the text after a requirement's
// '@' is PEP 508's url_req URI: a scheme URL (git+https://, file:///) or a
// file: reference. Anything else is not a direct reference — scp-style
// "git@github.com:o/r.git" names no package, and cutting at its '@' stored
// "git" (review round 1 on v0.29.66).
func isPyDirectReferenceURI(rest string) bool {
	rest = strings.ToLower(strings.TrimSpace(rest))
	return strings.Contains(rest, "://") || strings.HasPrefix(rest, "file:")
}

// parseSetupPyVersions extracts deps with versions from setup.py install_requires.
func parseSetupPyVersions(content string) []model.LibyearDep {
	// The same value as extractSetupPyInstallRequires reads: the two
	// readers of this list agree because they share setupKeywordValue.
	var deps []model.LibyearDep
	for _, chunk := range setupKeywordValue(content, "install_requires") {
		deps = append(deps, extractQuotedPyVersionDeps(chunk)...)
	}
	return deps
}

// extractQuotedPyVersionDeps extracts deps with versions from the quoted
// Python strings of a requirement list. A string that is a function-call
// argument or a keyword value is not a list element: v0.29.56 found
// "read_without_comments('dev-requirements')" in pixie-io/cpplint's
// extras_require sent to PyPI as a package, so a string whose preceding
// token is '(' , '=' or an identifier is skipped. Strings are scanned in
// order, so a quote of one kind inside the other kind is not a delimiter.
func extractQuotedPyVersionDeps(line string) []model.LibyearDep {
	var deps []model.LibyearDep
	for i := 0; i < len(line); i++ {
		quote := line[i]
		if quote != '\'' && quote != '"' {
			continue
		}
		end := strings.IndexByte(line[i+1:], quote)
		if end < 0 {
			break
		}
		depStr := line[i+1 : i+1+end]
		prev := strings.TrimRight(line[:i], " \t")
		i += end + 1
		if prev != "" {
			switch c := prev[len(prev)-1]; {
			case c == '(' || c == '=' || c == '_' || c == '.' ||
				('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9'):
				continue
			}
		}
		if d := ParsePyRequirement(depStr); d != nil {
			deps = append(deps, *d)
		}
	}
	return deps
}

// pyVersionOperators are PEP 440's comparison operators, longest first so a
// scan at one position matches "===" before "==" and ">=" before ">".
var pyVersionOperators = []string{"===", "==", "~=", "!=", ">=", "<=", ">", "<"}

// stripPyExtras removes a PEP 508 extras group ("celery[redis,auth]>=5.3"
// -> "celery>=5.3"). An UNCLOSED bracket keeps only the prefix.
//
// One spelling, because there were five (v0.29.56) and they disagreed about
// the unclosed case in three different ways: "celery[redis" answered
// "celery" from pyproject.toml and setup.py but "celery[redis" from
// requirements.txt — and pkgid.Purl turns that into "pkg:pypi/celery[redis",
// which wireValidPurl ACCEPTS, so it reaches OSV as a package that cannot
// exist. A name can never legitimately contain '[': PEP 508 names are
// [A-Za-z0-9._-] (SR-17).
func stripPyExtras(req string) string {
	idx := strings.Index(req, "[")
	if idx <= 0 {
		return req
	}
	if end := strings.Index(req, "]"); end > idx {
		return req[:idx] + req[end+1:]
	}
	return req[:idx]
}

// pep508NameRe is PEP 508's project-name grammar: letters and digits at
// both ends, ".", "_" and "-" allowed inside.
var pep508NameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`)

// isPyRequirementName reports whether a requirements-file line split to a
// real package name. splitPyNameSpec splits only at a version operator, so
// a line that is not a requirement becomes one long "name" — conda exports
// ("ujson=5.9.0", "pytorch::cpuonly"), RST and prose, templates, and
// space-separated lists all fail this check and are skipped by BOTH
// requirements readers (2026-09-23 log review; SR-17: one rule).
func isPyRequirementName(name string) bool {
	return pep508NameRe.MatchString(name)
}

// splitPyNameSpec splits a PEP 508 requirement at its LEFTMOST version
// operator: name is the text before it, spec the whole specifier set from
// that operator on ("" when the requirement carries no operator, and name ""
// when it opens with one and so names nothing).
//
// Position, not preference (v0.29.56). A specifier set is legal in either
// order, and five parsers each scanned their own preference list — in four
// different orders, one of them missing five operators — so the first
// operator in the LIST rather than in the STRING ended the name.
// "numpy<2,>=1.22", which is what apache/airflow ships, matched ">=" first
// and yielded the package name "numpy<2,": no registry can answer for it, so
// the dependency got no libyear row, no purl and no OSV coverage. The
// preference lists were reaching for the VERSION, which is a separate
// question — see pyFloorVersion (SR-17).
//
// A '(' ends the name too (PR #212 review): "requests (>=2.0)" is PEP 508's
// parenthesized form ('(' version_many ')', which Poetry 2.x writes); the
// spec is the text inside the parentheses.
func splitPyNameSpec(req string) (name, spec string) {
	for i := 0; i < len(req); i++ {
		// PEP 508's parenthesized form, "name (>=1.0)": the '(' ends the
		// name only when a version operator follows it — "Note (optional)"
		// is prose, not a requirement (review round 1 on v0.29.66).
		if req[i] == '(' {
			inner := strings.TrimSpace(req[i+1:])
			if hasPyVersionOperatorPrefix(inner) {
				return strings.TrimSpace(req[:i]), strings.TrimSpace(strings.TrimSuffix(inner, ")"))
			}
		}
		for _, op := range pyVersionOperators {
			if strings.HasPrefix(req[i:], op) {
				return strings.TrimSpace(req[:i]), strings.TrimSpace(req[i:])
			}
		}
	}
	return strings.TrimSpace(req), ""
}

// hasPyVersionOperatorPrefix reports whether s opens with a PEP 440
// comparison operator.
func hasPyVersionOperatorPrefix(s string) bool {
	for _, op := range pyVersionOperators {
		if strings.HasPrefix(s, op) {
			return true
		}
	}
	return false
}

// pyPermittedVersionRank ranks the PEP 440 operators that may supply the
// version to record, best first: an exact pin, then a compatible release,
// then an inclusive floor, then an inclusive ceiling.
//
// Membership is the rule, not the ranking (v0.29.56): an operator appears
// here only if its OWN clause permits the version it names. "!=", "<" and
// ">" are all EXCLUSIVE — each rules its version out — so none of them can
// answer, and their absence from this map is what enforces that.
//
// The check is per CLAUSE, not per set. A redundant set such as "<3,<=4"
// still yields 4, because "<=4" permits 4 on its own even though "<3" rules
// it out. (The set itself is satisfiable — 2.0 is in it — so the recorded
// value is wrong, not the requirement.) Catching that needs whole-set
// version comparison, and two independent sweeps of real manifests —
// checking every recorded version against its own specifier set — found NOT
// ONE that its set rejects. So this is left undone deliberately rather than
// overlooked. (No tally, on purpose: the corpora were fetched ad hoc and are
// not in the repo, so any figure here would be one a later reader cannot
// re-derive. This release shipped four such counts, the last of them inside
// the sentence that replaced the third.)
//
// Recording an excluded version writes a release the manifest contradicts
// into repo_dependencies and into the purl, and OSV then answers about a
// version nobody installed (SR-6). For "!=" that invents findings; for "<"
// it HIDES them, which is the worse direction. Measured against OSV on
// 2026-09-17: pkg:pypi/pyyaml@6.0 reports 0 vulnerabilities, while permitted
// releases below it carry up to 6 (5.3.1 → 2, 5.3 → 4, 5.1 → 6; 5.4 and
// 5.4.1 are also permitted and carry 0, so the bound is not uniformly
// safe-looking — it is just not the version in use). An upper bound is
// often pinned because that release is the one that changed something, so
// the excluded value tends to sit outside the vulnerable range.
//
// An empty version yields a versionless purl, which OSV answers with the
// package's whole advisory history — less precise, but not a claim the
// manifest contradicts.
//
// 0.29.209 (operator, 2026-11-19: "Treat all three as version not
// determined"): "<=" is no longer a version either. The bound IS permitted,
// which is why it ranked last here until then, but an upper bound says
// nothing about which release is installed: `tensorflow <=1.14` was
// recorded as using 1.14.
var pyPermittedVersionRank = map[string]int{"===": 4, "==": 4, "~=": 3, ">=": 2}

// pyFloorVersion picks the version to record for a PEP 508 specifier set.
func pyFloorVersion(spec string) string {
	best, bestRank := "", -1
	for _, clause := range strings.Split(spec, ",") {
		clause = strings.TrimSpace(clause)
		for _, op := range pyVersionOperators {
			if !strings.HasPrefix(clause, op) {
				continue
			}
			if r, ok := pyPermittedVersionRank[op]; ok && r > bestRank {
				best, bestRank = strings.TrimSpace(clause[len(op):]), r
			}
			break
		}
	}
	return best
}

// ParsePyRequirement parses a single Python requirement string into a model.LibyearDep.
func ParsePyRequirement(req string) *model.LibyearDep {
	req = strings.TrimSpace(req)
	if req == "" {
		return nil
	}
	// Strip an inline comment FIRST, and the array punctuation a list item
	// carries. The non-registry predicate below matches " @ ", URLs and
	// paths, all of which appear in comments: leaving one attached deletes a
	// legitimate pinned dependency, which then has no libyear row and drops
	// out of the OSV scan entirely. requirements*.txt learned this in
	// v0.29.56; these entry points inherited the predicate afterwards and
	// must strip the same way. (The trailing `",` also left versions like
	// `2.31.0"` behind on the PEP 621 path.)
	req = strings.Trim(strings.TrimSpace(req), "\"',")
	req = strings.Trim(cutPyInlineComment(req), "\"',")
	// Strip environment markers.
	if idx := strings.Index(req, ";"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	// A VCS or URL reference, a local path, or a PEP 508 direct reference
	// names no PyPI package (v0.29.56). requirements*.txt learned this from
	// the eclipse-velocitas shape; every other Python entry point —
	// pyproject.toml, setup.py, setup.cfg, and the dev/build variants —
	// builds its deps HERE, so the check belongs here too, or the same
	// "velocitas-sdk @ git+https://…" goes to PyPI as a package name from
	// one file format away.
	if IsNonRegistryPyRequirement(req) {
		return nil
	}
	cleanReq := stripPyExtras(req)

	name, spec := splitPyNameSpec(cleanReq)
	version := pyFloorVersion(spec)
	// The same PEP 508 name gate as the inventory (pyInventoryName) and
	// requirements.txt's libyear reader: prose such as "Note (optional)"
	// names no package, and without the gate it reached PyPI and OSV from
	// setup.py, setup.cfg, PEP 621 and the dev/build variants (review
	// round 2 on v0.29.66).
	if !isPyRequirementName(name) {
		return nil
	}
	return &model.LibyearDep{Name: name, Version: version, Requirement: req, Type: "runtime", Manager: "pypi"}
}

// parsePipfileDeps extracts dependency names from Pipfile [packages] section.
func parsePipfileDeps(content string) ([]string, error) {
	// The shared table reader (worklist items 42 and 43): every declared
	// name, whatever its source — the inventory lists git and path
	// dependencies by name as npm's and Cargo's do.
	var deps []string
	for _, e := range scanTOMLDepTables(content, map[string]bool{"[packages]": true}) {
		deps = append(deps, e.Name)
	}
	return deps, nil
}

// parsePipfileVersions extracts deps with versions from Pipfile [packages]
// through the shared table reader (pythonTOMLDeps; worklist items 42 and 43).
func parsePipfileVersions(content string) []model.LibyearDep {
	return pythonTOMLDeps(content, map[string]string{"[packages]": "runtime"})
}

// ParsePyprojectVersionsFromContent extracts deps with versions from pyproject.toml content.
// Handles both PEP 621 and Poetry formats.
func ParsePyprojectVersionsFromContent(content string) []model.LibyearDep {
	var deps []model.LibyearDep

	// PEP 621: [project] dependencies = ["flask==2.0.2", ...]
	deps = append(deps, parsePEP621Versions(content)...)

	// Poetry: [tool.poetry.dependencies] key = "^version"
	deps = append(deps, parsePoetryVersions(content)...)

	return deps
}

// parsePEP621Versions extracts deps with versions from PEP 621 format.
func parsePEP621Versions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	lines := strings.Split(content, "\n")
	inProject := false
	inDepsArray := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if tomlHeaderIs(trimmed, "project") {
			inProject = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") && !tomlHeaderIs(trimmed, "project") {
			inProject = false
			inDepsArray = false
			continue
		}
		if !inProject {
			continue
		}

		// The array's bounds are read outside strings and comments — see
		// parsePEP621Deps, which reads the same array for the name
		// inventory and must agree with this reader line for line.
		if strings.HasPrefix(trimmed, "dependencies") && strings.Contains(trimmed, "=") {
			code := stripHashComment(trimmed)
			opens, closes := listBrackets(code)
			if opens {
				deps = append(deps, extractPEP621VersionDeps(code)...)
				inDepsArray = !closes
			}
			continue
		}

		if inDepsArray {
			code := stripHashComment(trimmed)
			_, closes := listBrackets(code)
			deps = append(deps, extractPEP621VersionDeps(code)...)
			if closes {
				inDepsArray = false
			}
		}
	}
	return deps
}

// extractPEP621VersionDeps extracts deps with versions from inline PEP 621 arrays.
func extractPEP621VersionDeps(line string) []model.LibyearDep {
	var deps []model.LibyearDep
	// Bounds come from the shared scanner: a bracket inside a quoted item
	// is part of the value, not the array (v0.29.57).
	inner := listInner(line)

	// Structural commas only — see extractPEP621DepsFromLine (v0.29.56).
	for _, item := range splitTOMLTopLevel(inner) {
		item = strings.Trim(strings.TrimSpace(item), "\"'")
		if item != "" {
			if d := ParsePyRequirement(item); d != nil {
				deps = append(deps, *d)
			}
		}
	}
	return deps
}

// parsePoetryVersions extracts deps with versions from Poetry's
// [tool.poetry.dependencies] through the shared table reader
// (pythonTOMLDeps; worklist items 42 and 43).
func parsePoetryVersions(content string) []model.LibyearDep {
	return pythonTOMLDeps(content, map[string]string{"[tool.poetry.dependencies]": "runtime"})
}

// parseDirectoryPackagesProps extracts dependency names from .NET Directory.Packages.props.
// Format: <PackageVersion Include="Name" Version="1.0.0" />
func parseDirectoryPackagesProps(content string) ([]string, error) {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageVersion") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps, nil
}

// parseDirectoryPackagesPropsVersions extracts deps with versions from Directory.Packages.props.
func parseDirectoryPackagesPropsVersions(content string) []model.LibyearDep {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []model.LibyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageVersion") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		version := extractXMLAttr(line, "Version")
		// v0.27.44 (summary/19 P1): PrivateAssets="all" marks a
		// build/analyzer-only package that never flows to consumers
		// or the runtime output → build. Only the attribute form is
		// visible to this line-based parser; the child-element form
		// (<PrivateAssets>all</PrivateAssets>) spans lines and is a
		// documented limitation.
		scope := "runtime"
		if strings.EqualFold(extractXMLAttr(line, "PrivateAssets"), "all") {
			scope = model.ScopeBuild
		}
		if name != "" {
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "nuget"})
		}
	}
	return deps
}

// extractXMLAttr extracts the value of an XML attribute from a line.
// extractXMLAttr(`<PackageVersion Include="Foo" />`, "Include") returns "Foo".
func extractXMLAttr(line, attr string) string {
	key := attr + "=\""
	idx := strings.Index(line, key)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(key):]
	end := strings.Index(rest, "\"")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func parsePackageJSONVersions(path string) ([]model.LibyearDep, error) {
	data, err := ReadManifest(path)
	if err != nil {
		return nil, err
	}
	var pkg struct {
		Dependencies         map[string]string `json:"dependencies"`
		DevDependencies      map[string]string `json:"devDependencies"`
		PeerDependencies     map[string]string `json:"peerDependencies"`
		OptionalDependencies map[string]string `json:"optionalDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []model.LibyearDep
	add := func(m map[string]string, scope string) {
		for name, requirement := range m {
			if d, ok := npmManifestDep(name, requirement, scope); ok {
				deps = append(deps, d)
			}
		}
	}
	add(pkg.Dependencies, "runtime")
	add(pkg.DevDependencies, "dev")
	// v0.27.44 (summary/19 P1): peer/optional carry their OWN scopes —
	// they were conflated with runtime. Peer deps are expectations the
	// CONSUMER satisfies (not vendored by this package); optional deps
	// may be absent at runtime.
	add(pkg.PeerDependencies, model.ScopePeer)
	add(pkg.OptionalDependencies, model.ScopeOptional)
	return deps, nil
}

// npmManifestDep turns one package.json dependency into a registry lookup.
// v0.29.56: an alias ("typescript-3.5": "npm:typescript@~3.5.3") resolves
// the package it names, and a dependency that is not on the registry at
// all — a local package (file:, link:, portal:, workspace:) or a git, URL
// or GitHub-shorthand reference — is skipped: its key was sent to the npm
// registry as a name ("@ZettaScaleLabs/zenoh-ts": "file:…" in
// eclipse-zenoh/zenoh-demos → 404). catalog: stays: it names a registry
// version pinned elsewhere (the unpinned pathway).
func npmManifestDep(name, requirement, scope string) (model.LibyearDep, bool) {
	req := strings.TrimSpace(requirement)
	lower := strings.ToLower(req)
	resolvedName, version := name, req
	if strings.HasPrefix(lower, "npm:") {
		alias := req[len("npm:"):]
		if n, v, ok := SplitNPMSpec(alias); ok {
			resolvedName, version = n, v
		} else {
			resolvedName, version = alias, ""
		}
	}
	nonRegistry := false
	for _, prefix := range []string{"file:", "link:", "portal:", "workspace:", "git+", "git:", "git://", "github:", "gitlab:", "bitbucket:", "http://", "https://"} {
		if strings.HasPrefix(lower, prefix) {
			nonRegistry = true
		}
	}
	// GitHub shorthand "owner/repo" or "owner/repo#ref": a '/' outside a
	// version range, with no leading scope '@'.
	if strings.Contains(req, "/") && !strings.HasPrefix(lower, "npm:") && !strings.HasPrefix(req, "@") {
		nonRegistry = true
	}
	if resolvedName == "" {
		return model.LibyearDep{}, false
	}
	return model.LibyearDep{Name: resolvedName, Version: CleanVersion(version), Requirement: requirement,
		Type: scope, Manager: "npm", NonRegistry: nonRegistry}, true
}

func ParseRequirementsTxtVersions(path string) []model.LibyearDep {
	data, err := ReadManifest(path)
	if err != nil {
		return nil
	}
	var deps []model.LibyearDep
	for _, raw := range strings.Split(string(data), "\n") {
		line := cleanPyRequirementLine(raw)
		if line == "" {
			continue
		}
		// AFTER the comment/marker strip: a comment can contain " @ " (the
		// direct-reference marker) — "numpy==1.26.0  # updated @ 2026-01-01"
		// was dropped whole, taking numpy out of libyear AND out of the OSV
		// scan.
		if IsNonRegistryPyRequirement(line) {
			continue
		}
		name, spec := splitPyNameSpec(line)
		version := CleanVersion(pyFloorVersion(spec))
		// Strip PEP 508 extras from the name (anyio[trio] → anyio) —
		// the local-canary catch, 2026-07-21: extras-suffixed names
		// produced 404ing registry URLs and unmatchable purls, so
		// those deps silently got no libyear and no OSV coverage.
		name = strings.TrimSpace(stripPyExtras(name))
		if isPyRequirementName(name) {
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "pypi"})
		}
	}
	return deps
}

// IsNonRegistryPyRequirement reports a requirement line that does not name
// a PyPI package: a VCS or URL reference (git+https://…, https://…/x.tar.gz,
// file:…), a local path (./pkg, ../pkg, /opt/pkg) or a PEP 508 direct
// reference ("name @ url"). v0.29.56: these were sent to PyPI as names
// ("pypi/git+https://github.com/eclipse-velocitas/vehicle-model-python.git@v0.3.0/json").
func IsNonRegistryPyRequirement(line string) bool {
	lower := strings.ToLower(line)
	for _, prefix := range []string{"git+", "hg+", "svn+", "bzr+", "http://", "https://", "file:", "./", "../", "/", "~/"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	// PEP 508's direct-reference marker takes OPTIONAL whitespace
	// ("urlspec = '@' URI_reference", with wsp* around it), so
	// "mypkg@https://host/x.whl" is as legal as the spaced form and matching
	// only " @ " sent it to PyPI as a package name (v0.29.56). A bare '@' is
	// safe to key on: neither a PEP 508 name ([A-Za-z0-9._-]) nor a version
	// specifier can contain one, and this runs on the line AFTER its comment
	// and environment marker are stripped.
	return strings.Contains(line, "@")
}

// CleanVersion strips version specifier prefixes (^, ~, >=, ==, etc.)
// and extracts the first version from compound ranges (>=1.0,<2.0).
//
// An upper bound or an exclusion (`<`, `<=`, `!=`) is not a version in use
// (0.29.209; operator, 2026-11-19: "Treat all three as version not
// determined"). A range that leads with one is searched for a floor or a
// pin, which is then the version; with none there is no version. It used
// to strip the operator and return the bound: `<13.5.0` was "13.5.0", the
// version the requirement excludes.
func CleanVersion(v string) string {
	v = strings.TrimSpace(v)
	if upperOrExclusion(v) {
		v = firstFloorOrPin(v)
		if v == "" {
			return ""
		}
	}

	// For compound ranges (>=1.0,<2.0 or ^1 || ^2), take only the first segment.
	if idx := strings.IndexAny(v, ",|"); idx > 0 {
		v = v[:idx]
		v = strings.TrimSpace(v)
	}

	// Strip operator prefixes. Order matters: longest first.
	for _, prefix := range []string{"^", "~=", "~", ">=", "<=", "==", "!=", ">", "<", "="} {
		v = strings.TrimPrefix(v, prefix)
	}

	// Trim any whitespace left after operator removal (e.g., "~> 1.2" -> " 1.2").
	v = strings.TrimSpace(v)

	// v0.27.71: space-separated compound ranges keep the floor. pub
	// ("'>=0.11.1 <0.12.0'"), npm hyphen ranges ("1.2.3 - 2.3.4"),
	// and hex ("~> 3.0.0 and < 5.0.0") all separate bounds with
	// spaces, which the comma/pipe split above never saw — the whole
	// range leaked into versions and purls. No real version contains
	// a space, so cutting at the first one is universally safe.
	if idx := strings.IndexByte(v, ' '); idx > 0 {
		v = strings.TrimSpace(v[:idx])
	}
	return v
}

// upperOrExclusion reports whether a requirement begins with an upper bound
// or an exclusion.
func upperOrExclusion(v string) bool {
	return strings.HasPrefix(v, "<") || strings.HasPrefix(v, "!=")
}

// boundOperatorSpace is an operator and the space some ecosystems write
// after it ("< 2.0", "~> 1.2"), so that a range can be cut into its bounds
// at the spaces and commas that separate them.
var boundOperatorSpace = regexp.MustCompile(`(<=|>=|!=|==|~=|~>|[<>=^~])\s+`)

// versionBound is one bound of a range: an operator, or none, and a
// version. Nothing else is one: not a word of a comment after the range,
// and not a clause of an environment marker.
//
// A version may lead with a `v` in either case, and may end in a Composer
// stability flag (`>=1.0@dev`): round 4, each ended the search for a floor.
var versionBound = regexp.MustCompile(`^(<=|>=|!=|===|==|~=|~>|<|>|=|~|\^)?[vV]?[0-9][0-9A-Za-z.*+_-]*(@[A-Za-z]+)?$`)

// firstFloorOrPin is the first bound of a range's FIRST alternative that
// is a floor or a pin, as written (operator kept); "" when every bound of
// that alternative is an upper bound or an exclusion.
//
// Only a bound can be one (0.29.210). The range ends at the first thing
// that is neither a bound nor a word that joins two (`and`, `&&`, the `-`
// of a hyphen range): a comment (`'<2.0.0' # pinned until 3` gave "3"), an
// environment marker (`<1.0 ; python_version >= "3.8"` gave `"3.8"`), a
// closing quote. And it ends at an alternative, `||` or hex's `or`: a floor
// there is the floor of another range (`< 2.0.0 or > 3.0.0` gave "3.0.0"),
// and which alternative is in use is not known.
func firstFloorOrPin(v string) string {
	v = boundOperatorSpace.ReplaceAllString(v, "$1")
	for _, bound := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		// The words that join bounds, in either case (`AND`, round 4).
		switch word := strings.ToLower(bound); {
		case word == "and" || bound == "&&" || bound == "-":
			continue
		case word == "or" || strings.HasPrefix(bound, "|"):
			return ""
		}
		// A quote that closes the value ends it; one around a bound is
		// not part of it.
		bare := strings.Trim(bound, `"'`)
		cut := strings.Index(bare, "|")
		if cut >= 0 {
			bare = bare[:cut] // `<2||>=3`: the alternative begins inside the token
		}
		if !versionBound.MatchString(bare) {
			return ""
		}
		if !upperOrExclusion(bare) {
			return bare
		}
		if cut >= 0 || bare != bound && strings.HasSuffix(bound, `'`) || bare != bound && strings.HasSuffix(bound, `"`) {
			return "" // the alternative, or the value, ended with this bound
		}
	}
	return ""
}

// yamlScalar is a YAML value as written, without its quotes or a comment
// after it. For a value in quotes it is what the quotes hold; otherwise it
// ends where a comment begins.
func yamlScalar(v string) string {
	v = strings.TrimSpace(v)
	if len(v) > 0 && (v[0] == '\'' || v[0] == '"') {
		if end := strings.IndexByte(v[1:], v[0]); end >= 0 {
			return v[1 : 1+end]
		}
		return v[1:]
	}
	if i := strings.Index(v, " #"); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

// ReadManifest reads a dependency manifest and normalizes its encoding:
// UTF-16 (LE or BE, by its byte-order mark) becomes UTF-8, and a UTF-8
// byte-order mark is dropped. It is the ONE raw manifest read in this file
// (SR-17): the libyear walk used a bare os.ReadFile while the
// dependency-name path decoded UTF-16, so the same file produced clean
// names on one path and NUL-interleaved ones on the other, and neither
// stripped a UTF-8 BOM (2026-09-23 log review: 8 UTF-16LE requirements
// files, a BOM on "astroid" and on a root package.json).
// TestAnalysisReadsManifestsThroughOneReader pins it.
func ReadManifest(path string) ([]byte, error) {
	if bound := readBound.Load(); bound != nil {
		// The bound is told the file's length (-1 when it has none to
		// tell: not a regular file, or it cannot be looked at), so that it
		// can make room for a file it would otherwise refuse. A process
		// with a bound never reads a file whole: when no room is left the
		// bound is one byte, which only an empty file fits.
		size := int64(-1)
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			size = info.Size()
		}
		max := (*bound)(size)
		if max < 1 {
			max = 1
		}
		return ReadManifestWithin(path, max)
	}
	return ReadManifestWithin(path, 0)
}

// readBound is the process's bound on one read of a manifest, when it has
// one; nil in every process that does not install it.
var readBound atomic.Pointer[func(size int64) int64]

// SetReadBound installs, for the whole process, how many bytes one read of a
// manifest or lockfile may take (0.29.214), and returns what removes it
// again. It is for the process that exists to read one clone under a memory
// limit (the collector's manifest reader): bound is asked before each read
// for the room left now, with the length of the file about to be read (-1
// when that is not known), and every reader in this package (they all read
// through ReadManifest) then refuses a file longer than that from its
// length, and stops reading one that grows at it (ReadManifestWithin). The
// refusal is a *ManifestTooLargeError, which the roster reports as a
// manifest that could not be read.
//
// It is process-wide, and not a field of Walk, because most readers here are
// functions of a path that no walk reaches; a process serving other work
// must not install it. With none installed a read is whole, as it always
// was.
//
// The bound should answer the same for the same file a moment later (the
// roster reads a file once to decide it is readable and its reader reads it
// again, and several readers treat a file they cannot read as declaring
// nothing): a bound made of the room a heap limit leaves frees what it can
// before it refuses.
func SetReadBound(bound func(size int64) int64) (restore func()) {
	prev := readBound.Swap(&bound)
	return func() { readBound.Store(prev) }
}

// ManifestTooLargeError is ReadManifestWithin's refusal of a file longer
// than its caller allows. Size is the file's length when it was known
// before reading, else -1 (it was read up to the limit and went on).
type ManifestTooLargeError struct {
	Path string
	Size int64
	Max  int64
}

func (e *ManifestTooLargeError) Error() string {
	if e.Size < 0 {
		return fmt.Sprintf("%s is longer than the %d bytes that may be read of it; it was not read", e.Path, e.Max)
	}
	return fmt.Sprintf("%s is %d bytes, more than the %d that may be read of it; it was not read", e.Path, e.Size, e.Max)
}

// ReadManifestWithin is ReadManifest for a caller that has a bound on what
// it may hold (round 7: a file was read whole whatever its size, and a
// reader with a memory limit was two gibibytes past it before anything
// looked). max <= 0 is no bound, which is ReadManifest.
//
// A regular file longer than max is refused BEFORE any of it is read, from
// its length (*ManifestTooLargeError). Anything else is read in pieces and
// never more than max+1 bytes of it, so a file that grows while it is read,
// or one whose length is not known (a pipe), is refused at the limit and not
// held. The bound is on the bytes of the file: a UTF-16 file's text is
// about half of them.
func ReadManifestWithin(path string, max int64) ([]byte, error) {
	var data []byte
	if max <= 0 {
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return nil, err
		}
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if info, err := f.Stat(); err != nil {
			return nil, err
		} else if info.Mode().IsRegular() && info.Size() > max {
			return nil, &ManifestTooLargeError{Path: path, Size: info.Size(), Max: max}
		}
		if data, err = io.ReadAll(io.LimitReader(f, max+1)); err != nil {
			return nil, err
		}
		if int64(len(data)) > max {
			return nil, &ManifestTooLargeError{Path: path, Size: -1, Max: max}
		}
	}
	data = decodeIfUTF16(data)
	return bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf}), nil
}

// decodeIfUTF16 detects UTF-16 BOM and converts to UTF-8. Some Windows-created
// manifest files (especially requirements.txt) are saved as UTF-16LE, producing
// null-interleaved ASCII that fails PostgreSQL's UTF-8 validation.
func decodeIfUTF16(data []byte) []byte {
	if len(data) < 2 {
		return data
	}
	// UTF-16LE BOM: 0xff 0xfe
	if data[0] == 0xff && data[1] == 0xfe {
		return utf16LEToUTF8(data[2:]) // skip BOM
	}
	// UTF-16BE BOM: 0xfe 0xff
	if data[0] == 0xfe && data[1] == 0xff {
		return utf16BEToUTF8(data[2:]) // skip BOM
	}
	return data
}

func utf16LEToUTF8(data []byte) []byte {
	if len(data)%2 != 0 {
		data = data[:len(data)-1] // drop trailing byte
	}
	var result []byte
	for i := 0; i+1 < len(data); i += 2 {
		ch := rune(data[i]) | rune(data[i+1])<<8
		if ch < 0x80 {
			result = append(result, byte(ch))
		} else if ch < 0x800 {
			result = append(result, byte(0xC0|(ch>>6)), byte(0x80|(ch&0x3F)))
		} else {
			result = append(result, byte(0xE0|(ch>>12)), byte(0x80|((ch>>6)&0x3F)), byte(0x80|(ch&0x3F)))
		}
	}
	return result
}

func utf16BEToUTF8(data []byte) []byte {
	if len(data)%2 != 0 {
		data = data[:len(data)-1]
	}
	var result []byte
	for i := 0; i+1 < len(data); i += 2 {
		ch := rune(data[i])<<8 | rune(data[i+1])
		if ch < 0x80 {
			result = append(result, byte(ch))
		} else if ch < 0x800 {
			result = append(result, byte(0xC0|(ch>>6)), byte(0x80|(ch&0x3F)))
		} else {
			result = append(result, byte(0xE0|(ch>>12)), byte(0x80|((ch>>6)&0x3F)), byte(0x80|(ch&0x3F)))
		}
	}
	return result
}

// ParseGoModVersions extracts deps with versions from go.mod.
// Handles both block form "require (" and single-line "require module version".
func ParseGoModVersions(path string) []model.LibyearDep {
	data, err := ReadManifest(path)
	if err != nil {
		return nil
	}
	var deps []model.LibyearDep
	inRequire := false
	// A module replaced by a directory is that directory's code, not the
	// proxy's module of the name (0.29.208).
	local := goModLocalReplacements(string(data))
	defer func() {
		for i := range deps {
			if local[deps[i].Name] {
				deps[i].NonRegistry = true
			}
		}
	}()
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		// Handle single-line require: "require github.com/foo/bar v1.2.3"
		if strings.HasPrefix(line, "require ") && !strings.Contains(line, "(") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				// Keep the v prefix — purl spec for golang keeps it, and
				// OSV.dev matches on versions with v. Stripping it broke
				// all Go vulnerability scanning.
				deps = append(deps, model.LibyearDep{Name: parts[1], Version: parts[2], Requirement: line, Type: "runtime", Manager: "go"})
			}
			continue
		}

		if line == "require (" {
			inRequire = true
			continue
		}
		if line == ")" {
			inRequire = false
			continue
		}
		if inRequire && line != "" && !strings.HasPrefix(line, "//") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				// Keep the v prefix for purl and OSV compatibility.
				version := parts[1]
				deps = append(deps, model.LibyearDep{Name: parts[0], Version: version, Requirement: line, Type: "runtime", Manager: "go"})
			}
		}
	}
	return deps
}

// goModLocalReplacements are the modules a go.mod's replace directives
// point at a directory: `replace old [version] => ./dir`, on one line or in
// a block. The go command's own rule for "a directory": the target begins
// with ./ or ../ or is an absolute path, and has no version. A replacement
// by another MODULE (`=> example.com/fork v1.0.1`) is not marked: the
// requirement is still a module the proxy has, and which module answers for
// it is a question this reader does not settle.
func goModLocalReplacements(content string) map[string]bool {
	out := map[string]bool{}
	inBlock := false
	for _, line := range strings.Split(content, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "replace (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "replace ") && !inBlock:
			line = strings.TrimSpace(strings.TrimPrefix(line, "replace "))
		case !inBlock:
			continue
		}
		left, right, ok := strings.Cut(line, "=>")
		if !ok {
			continue
		}
		old, target := strings.Fields(left), strings.Fields(right)
		if len(old) == 0 || len(target) == 0 {
			continue
		}
		if t := target[0]; strings.HasPrefix(t, "./") || strings.HasPrefix(t, "../") || t == "." || t == ".." || filepath.IsAbs(t) || strings.HasPrefix(t, "/") {
			out[old[0]] = true
		}
	}
	return out
}

// gemBlock is one open block of a Gemfile.
type gemBlock struct {
	scope       string // the group scope in force inside it; "" for none
	nonRegistry bool   // inside a path, git, github or source block
}

// gemBlockOpens reports whether a Gemfile line opens a block that an `end`
// closes: a `do` block (group, path, git, source, platforms, install_if...)
// or a Ruby conditional at the start of a line. Every one must be counted,
// or its `end` closes the group around it.
var gemBlockOpens = regexp.MustCompile(`(\sdo(\s*\|[^|]*\|)?$)|^(if|unless|case|while|until|begin|def)\b`)

// gemClosedOnItsLine: a conditional that ends where it began (`if X then
// gem 'a' end`). It opens no block: counted as one, its missing `end` kept
// the group or the path block around it open to the end of the file
// (review round 2).
var gemClosedOnItsLine = regexp.MustCompile(`^(if|unless|case|while|until|begin|def)\b.*[\s;]end$`)

// gemOneLine: a line (its string literals masked, its comment cut) that
// opens a block with `then` or `do` and closes it with `end`, gem
// declarations between; a statement modifier may follow the `end`. What it
// captures is the block's body, which gemOneLineSplit cuts into statements:
// at a `;`, and at the `else` or `elsif ... then` of a conditional (both
// arms are declared: which is taken is not known to a reader).
//
// Round 4: one pattern read one gem between the keywords and the end of the
// line, so a second gem was dropped, `else` hid the other arm, `gem(` was
// not seen, and a modifier after `end` left the block open to the end of
// the file.
//
// Round 5: a `case` whose arms are on the line (`case X when 'a' then gem
// 'x' when 'b' then gem 'y' end`) is such a block, and its `when ... then`
// separates statements as `elsif ... then` does; a `;` may follow the `do`
// (`group :x do; gem 'a'; end`); and blocks nested on one line close with
// one `end` each, none of which is part of the last declaration (its
// requirement was stored as `gem 'n', '1.0' end`).
//
// gemOneLineBraces is the same block written with braces (`group(:test) {
// gem 'a'; gem 'b' }`). Only a block keyword opens one: a brace after `gem`
// is a hash of its options.
var (
	gemOneLine       = regexp.MustCompile(`^(?:if|unless|case|group|platforms?|path|git|github|gitlab|bitbucket|source|install_if|env)\b.*?\b(?:then|do)[\s;]+(?:\|[^|]*\|\s*)?(gem\b.*?)(?:[\s;]+end)+(?:\s+(?:if|unless|while|until)\b.*)?$`)
	gemOneLineBraces = regexp.MustCompile(`^(?:group|platforms?|path|git|github|gitlab|bitbucket|source|install_if|env)\b[^{]*\{\s*(?:\|[^|]*\|\s*)?(gem\b.*?)[\s;]*\}$`)
	// gemOnOpeningLine: gems after the `do` or `then` of a block that stays
	// open (`group :test do gem 'a'; gem 'b'`, closed lines later).
	gemOnOpeningLine = regexp.MustCompile(`^(?:if|unless|group|platforms?|path|git|github|gitlab|bitbucket|source|install_if|env)\b.*?\b(?:then|do)[\s;]+(?:\|[^|]*\|\s*)?(gem\b.*)$`)
	gemOneLineSplit  = regexp.MustCompile(`\s*;\s*|\s+(?:else|elsif\b.*?\bthen|when\b.*?\bthen)\s+`)
	// gemBlockEnd: the line that closes a block, with or without a
	// statement modifier (`end if ENV['CI']`: round 5, such an `end` closed
	// nothing and the group ran to the end of the file).
	gemBlockEnd   = regexp.MustCompile(`^end(?:\s+(?:if|unless|while|until)\b.*)?$`)
	maskedLiteral = regexp.MustCompile(`"` + quotedMark + `([0-9]+)"`)
)

// gemLogicalLines splits a line at each `;` that ends a whole statement: a
// block that opens and closes on the line (`group :test do gem 'a' end; gem
// 'b'`), or a gem declaration (`gem 'a'; gem 'b'` is two declarations, as it
// is in Ruby, and gives two rows). A `;` inside a block that is still open
// (`group :x do; gem 'a'; end`) is not such a place, and the line is left
// whole for the one-line block's own reading.
//
// Round 6: the one-line patterns end at the end of the line, so a block
// followed by `; ...` was not seen as closed: its gem's requirement held the
// `end`, and every gem after it took the block's group.
//
// Round 7: that fix asked three patterns about the whole head at every `;`
// and read the rest again from its start after every split, so a line of
// many `;` took time in the square of its length (100 KB: over a minute).
// The line is now masked once, the places the patterns look for are found
// once, and each `;` is decided from them: the same answers
// (TestGemLogicalLinesAnswersAsThePatternsDo), in one pass.
func gemLogicalLines(line string) []string {
	literals := quotedLiteral.FindAllString(line, -1)
	n := -1
	masked := quotedLiteral.ReplaceAllStringFunc(line, func(string) string { n++; return `"` + quotedMark + strconv.Itoa(n) + `"` })
	if i := strings.Index(masked, "#"); i >= 0 {
		masked = masked[:i]
	}
	if !strings.Contains(masked, ";") {
		return []string{line}
	}
	restore := func(piece string) string {
		return maskedLiteral.ReplaceAllStringFunc(piece, func(mark string) string {
			k, err := strconv.Atoi(mark[1+len(quotedMark) : len(mark)-1])
			if err != nil || k >= len(literals) {
				return mark
			}
			return literals[k]
		})
	}
	idx := newGemLineIndex(masked)
	var out []string
	start := 0
	st := idx.statementAt(start)
	lastWord := -1 // the end of the last non-space rune since start
	for i := 0; i < len(masked); {
		r, size := utf8.DecodeRuneInString(masked[i:])
		if r != ';' {
			if !unicode.IsSpace(r) {
				lastWord = i + size
			}
			i += size
			continue
		}
		if i+1 >= idx.lastWord {
			break // nothing but space follows: no rest to split off
		}
		if st.kind != gemStatementOther && lastWord > st.from && idx.whole(st, lastWord) {
			out = append(out, restore(masked[st.from:lastWord]))
			start = i + 1
			st = idx.statementAt(start)
			lastWord = -1
		} else {
			lastWord = i + 1 // a `;` that splits nothing is part of its statement
		}
		i++
	}
	if start == 0 {
		return []string{line}
	}
	return append(out, restore(strings.TrimSpace(masked[start:])))
}

// What a statement of a Gemfile line opens with decides which of the three
// patterns can ever hold of it.
const (
	gemStatementOther = iota // none can: the rest of the line is one piece
	gemStatementGem          // gemPlainDeclaration
	gemStatementBlock        // gemOneLine or gemOneLineBraces
)

type gemStatement struct {
	kind   int
	from   int  // the statement's first non-space byte
	body   int  // the first byte after its opening keyword
	braces bool // its keyword may open a brace block (not if, unless, case)
}

// gemLineIndex holds the places in one masked line that the one-line
// patterns look for, found once.
type gemLineIndex struct {
	masked   string
	lastWord int      // the end of the line's last non-space rune
	openers  [][2]int // each `then|do` followed by a gem: where the word starts, where its gem ends
	closers  [][2]int // each `end` followed by a statement modifier: its leading separator, its end
	braces   []int    // each `{`
	braceGem map[int]int
}

var (
	gemStatementKeyword = regexp.MustCompile(`^(?:(if|unless|case)|group|platforms?|path|git|github|gitlab|bitbucket|source|install_if|env)\b`)
	gemStatementIsGem   = regexp.MustCompile(`^gem\b`)
	gemOpenerWord       = regexp.MustCompile(`\b(?:then|do)[\s;]+`)
	gemEndModifier      = regexp.MustCompile(`[\s;]end\s+(?:if|unless|while|until)\b`)
	gemBraceBody        = regexp.MustCompile(`^\{\s*(?:\|[^|]*\|\s*)?gem\b`)
)

func newGemLineIndex(masked string) *gemLineIndex {
	x := &gemLineIndex{masked: masked, lastWord: len(strings.TrimRightFunc(masked, unicode.IsSpace)), braceGem: map[int]int{}}
	// `\b(?:then|do)[\s;]+(?:\|[^|]*\|\s*)?gem\b`, asked once from each
	// opener word. A `|...|` region is crossed by the one opener in front
	// of it.
	for _, m := range gemOpenerWord.FindAllStringIndex(masked, -1) {
		at := m[1]
		if at < len(masked) && masked[at] == '|' {
			close := strings.IndexByte(masked[at+1:], '|')
			if close < 0 {
				continue
			}
			at += close + 2
			for at < len(masked) && isRegexpSpace(masked[at]) {
				at++
			}
		}
		if strings.HasPrefix(masked[at:], "gem") && (at+3 == len(masked) || !isRegexpWord(masked[at+3])) {
			x.openers = append(x.openers, [2]int{m[0], at + 3})
		}
	}
	// The earliest gem of the openers from each one on: an opener inside
	// another's `|...|` has its gem first.
	for k := len(x.openers) - 2; k >= 0; k-- {
		if x.openers[k+1][1] < x.openers[k][1] {
			x.openers[k][1] = x.openers[k+1][1]
		}
	}
	for _, m := range gemEndModifier.FindAllStringIndex(masked, -1) {
		x.closers = append(x.closers, [2]int{m[0], m[1]})
	}
	for i := 0; i < len(masked); i++ {
		if masked[i] == '{' {
			x.braces = append(x.braces, i)
		}
	}
	return x
}

// statementAt classifies the statement that begins at or after from.
func (x *gemLineIndex) statementAt(from int) gemStatement {
	rest := strings.TrimLeftFunc(x.masked[from:], unicode.IsSpace)
	at := len(x.masked) - len(rest)
	head := rest
	if len(head) > 16 {
		head = head[:16] // longer than any keyword and the byte after it
	}
	if gemStatementIsGem.MatchString(head) {
		return gemStatement{kind: gemStatementGem, from: at, body: at + 3}
	}
	if m := gemStatementKeyword.FindStringSubmatchIndex(head); m != nil {
		return gemStatement{kind: gemStatementBlock, from: at, body: at + m[1], braces: m[2] < 0}
	}
	return gemStatement{kind: gemStatementOther, from: at}
}

// whole reports whether the statement, ending at end (the end of its last
// non-space rune), is one the three patterns accept: what
// gemOneLine.MatchString || gemOneLineBraces.MatchString ||
// gemPlainDeclaration.MatchString say of masked[st.from:end].
func (x *gemLineIndex) whole(st gemStatement, end int) bool {
	brace := -1
	if k := sort.SearchInts(x.braces, st.from); k < len(x.braces) && x.braces[k] < end {
		brace = x.braces[k]
	}
	if st.kind == gemStatementGem {
		return brace < 0
	}
	// gemOneLine: the first opener after the keyword whose gem is inside,
	// then an `end` that closes the statement: at its very end, or with a
	// modifier after it.
	k := sort.Search(len(x.openers), func(i int) bool { return x.openers[i][0] > st.body })
	if k < len(x.openers) && x.openers[k][1] <= end {
		gem := x.openers[k][1]
		if end-4 >= gem && x.masked[end-3:end] == "end" && (x.masked[end-4] == ';' || isRegexpSpace(x.masked[end-4])) {
			return true
		}
		c := sort.Search(len(x.closers), func(i int) bool { return x.closers[i][0] >= gem })
		if c < len(x.closers) && x.closers[c][1] <= end {
			return true
		}
	}
	// gemOneLineBraces: the statement's first `{`, a gem first inside it,
	// and `}` at the very end.
	if st.braces && brace >= 0 && x.masked[end-1] == '}' {
		gem, known := x.braceGem[brace]
		if !known {
			gem = -1
			if m := gemBraceBody.FindStringIndex(x.masked[brace:]); m != nil {
				gem = brace + m[1]
			}
			x.braceGem[brace] = gem
		}
		return gem >= 0 && gem <= end-1
	}
	return false
}

// isRegexpWord: what `\w` matches.
func isRegexpWord(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// isRegexpSpace: what `\s` matches.
func isRegexpSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\f' || b == '\r'
}

// gemPlainDeclaration: a gem declaration that opens no block.
var gemPlainDeclaration = regexp.MustCompile(`^gem\b[^{]*$`)

// gemSourceValue reads the address a `source` block or option names.
var gemSourceValue = regexp.MustCompile(`(?:^|[\s,(])(?::source\s*=>|source:?)\s*\(?\s*(?:["']([^"']*)["']|:(\w+))`)

// gemSourceIsRubygems reports whether a line's `source` names rubygems.org
// itself: the registry, written out. Bundler's documentation recommends
// exactly that beside a private source (`source "https://rubygems.org" do`),
// and such a gem is the registry's (review round 2: it was marked as not
// from it, and so never checked). An address that cannot be read is not
// taken for the registry.
//
// The address is read from the `source` that is CODE: the line's string
// literals are set aside first, so a string that holds the word (a note, a
// path) cannot answer for the option after it (review round 3).
func gemSourceIsRubygems(line string) bool {
	masked, literals := maskQuoted(line)
	m := gemSourceValue.FindStringSubmatch(masked)
	if m == nil {
		return false
	}
	if m[2] != "" {
		return m[2] == "rubygems" || m[2] == "gemcutter" // Bundler's old symbols for it
	}
	n, err := strconv.Atoi(strings.TrimPrefix(m[1], quotedMark))
	if err != nil || n < 0 || n >= len(literals) {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(literals[n]))
	for _, scheme := range []string{"https://", "http://"} {
		host = strings.TrimPrefix(host, scheme)
	}
	host = strings.TrimSuffix(host, "/")
	return host == "rubygems.org" || host == "www.rubygems.org"
}

// gemSourceBlock: a block whose gems come from a directory, a git
// repository or another gem server.
var gemSourceBlock = regexp.MustCompile(`^(path|git|github|gitlab|bitbucket|source)[\s(]`)

// gemSourceOption: the same, as a keyword option of one gem (either hash
// spelling). Read from the declaration with its string literals removed,
// so a value that contains `path:` is not an option.
var gemSourceOption = regexp.MustCompile(`(^|[\s,(])(:(path|git|github|gitlab|bitbucket|gist|source)\s*=>|(path|git|github|gitlab|bitbucket|gist|source):(\s|$))`)

// gemNonRegistryOption reports whether a gem's own options take it from
// somewhere that is not rubygems.org: any of the source options, except a
// `source:` that names rubygems.org.
func gemNonRegistryOption(text string) bool {
	for _, m := range gemSourceOption.FindAllStringSubmatch(withoutQuoted(text), -1) {
		kind := m[3] + m[4]
		if kind != "source" || !gemSourceIsRubygems(text) {
			return true
		}
	}
	return false
}

var quotedLiteral = regexp.MustCompile("\"[^\"]*\"|'[^']*'")

// quotedMark begins what maskQuoted puts in place of a string literal.
const quotedMark = "\x00"

// maskQuoted replaces each string literal of a line with "<mark>N" in
// quotes, N its position in literals (the text between its quotes).
func maskQuoted(s string) (masked string, literals []string) {
	masked = quotedLiteral.ReplaceAllStringFunc(s, func(lit string) string {
		literals = append(literals, lit[1:len(lit)-1])
		return `"` + quotedMark + strconv.Itoa(len(literals)-1) + `"`
	})
	return masked, literals
}

// withoutQuoted empties a line's string literals.
func withoutQuoted(s string) string { return quotedLiteral.ReplaceAllString(s, `""`) }

// ParseGemfileVersions extracts deps with versions from Gemfile.
func ParseGemfileVersions(path string) []model.LibyearDep {
	data, err := ReadManifest(path)
	if err != nil {
		return nil
	}
	var deps []model.LibyearDep
	// v0.27.44 (summary/19 P1): Bundler group blocks + inline group:
	// options were conflated as runtime. :test → test; :development
	// (and any other non-default group) → dev. Nested groups keep the
	// innermost classification; "end" pops the stack.
	//
	// 0.29.208: every block is on the stack, not only a group. A
	// `platforms :ruby do ... end` inside a group used to close the group at
	// its own `end`; and a gem inside a `path`, `git` or `source` block
	// comes from there, not from rubygems.org.
	var groupStack []gemBlock
	var lines []string
	for _, physical := range strings.Split(string(data), "\n") {
		lines = append(lines, gemLogicalLines(strings.TrimSpace(physical))...)
	}
	for _, line := range lines {
		// A block line, read without its comment or its strings.
		opener := withoutQuoted(line)
		if i := strings.Index(opener, "#"); i >= 0 {
			opener = strings.TrimSpace(opener[:i])
		}
		// What a block that opens on this line puts in force inside it.
		block := func() gemBlock {
			b := gemBlock{}
			if len(groupStack) > 0 {
				b = groupStack[len(groupStack)-1]
			}
			if strings.HasPrefix(line, "group ") || strings.HasPrefix(line, "group(") {
				b.scope = gemGroupScope(line)
			}
			if gemSourceBlock.MatchString(opener) && !(strings.HasPrefix(opener, "source") && gemSourceIsRubygems(line)) {
				b.nonRegistry = true
			}
			return b
		}
		// add reads one gem declaration, in the block it stands in.
		add := func(text string, in *gemBlock) {
			// gem 'name', '~> 1.0' — read by the one shared declaration
			// parser (item 71): only a string-literal argument after the
			// name is a version requirement, so keyword options (v0.27.71:
			// "require: false" was the #1 rubygems garbage version in
			// production) and a trailing `if`/`unless` modifier (`">= 1.5"
			// if RUBY_VERSION < '1.9'` was stored as `1.5"`) never are.
			decl, ok := parseGemDeclaration(text)
			if !ok {
				return
			}
			// v0.29.56: a trailing comment was read into the name or
			// version ("gem 'logger'   # stdlib in Ruby <= 3.x" → name
			// "logger'   # stdlib in Ruby <= 3.x", meshery/meshery.io). The
			// parser stops before it and before a modifier, so decl.Text
			// is the stored requirement (keyword options kept for display;
			// the classifier reads decl.Declared through
			// ClassificationText).
			version := ""
			if len(decl.Requirements) > 0 {
				// Every requirement of the declaration, as one range: the
				// floor may be the second (`'< 5.0', '>= 4.2'`).
				version = CleanVersion(strings.Join(decl.Requirements, ","))
			}
			scope := "runtime"
			nonRegistry := gemNonRegistryOption(decl.Text)
			if in != nil {
				if in.scope != "" {
					scope = in.scope
				}
				nonRegistry = nonRegistry || in.nonRegistry
			}
			if inlineGroup := gemInlineGroupScope(decl.Text); inlineGroup != "" {
				scope = inlineGroup
			}
			deps = append(deps, model.LibyearDep{Name: decl.Name, Version: version, Requirement: decl.Text, Type: scope, Manager: "rubygems", NonRegistry: nonRegistry})
		}
		// A block that opens and closes on its line (`if X then gem 'a'
		// end`, `group :test do gem 'x'; gem 'y' end if Z`): its gems are
		// read, in the block's scope, and nothing stays open, whether or
		// not any of them could be read (0.29.210, 0.29.211).
		masked := quotedLiteral.ReplaceAllStringFunc(line, func() func(string) string {
			n := -1
			return func(string) string { n++; return `"` + quotedMark + strconv.Itoa(n) + `"` }
		}())
		if i := strings.Index(masked, "#"); i >= 0 {
			masked = strings.TrimSpace(masked[:i])
		}
		m := gemOneLine.FindStringSubmatch(masked)
		if m == nil {
			m = gemOneLineBraces.FindStringSubmatch(masked)
		}
		// addEach reads the gems of a masked run of statements, each as
		// written: every literal back in its place.
		addEach := func(statements string, in *gemBlock) {
			literals := quotedLiteral.FindAllString(line, -1)
			for _, statement := range gemOneLineSplit.Split(statements, -1) {
				add(maskedLiteral.ReplaceAllStringFunc(statement, func(mark string) string {
					n, err := strconv.Atoi(mark[1+len(quotedMark) : len(mark)-1])
					if err != nil || n >= len(literals) {
						return mark
					}
					return literals[n]
				}), in)
			}
		}
		if m != nil {
			in := block()
			addEach(m[1], &in)
			continue
		}
		// A line that opens a block and goes on with a gem (`path 'x' do
		// gem 'a'`) opens it as one that ends with `do` does: unseen, its
		// `end` closed the block around it.
		onOpening := gemOnOpeningLine.FindStringSubmatch(masked)
		if (strings.HasPrefix(line, "group ") || gemBlockOpens.MatchString(opener) || onOpening != nil) && !gemClosedOnItsLine.MatchString(opener) {
			groupStack = append(groupStack, block())
			// Gems on the opening line itself are in the block it opens.
			if onOpening != nil {
				addEach(onOpening[1], &groupStack[len(groupStack)-1])
			}
			continue
		}
		if gemBlockEnd.MatchString(opener) && len(groupStack) > 0 {
			groupStack = groupStack[:len(groupStack)-1]
			continue
		}
		var in *gemBlock
		if len(groupStack) > 0 {
			in = &groupStack[len(groupStack)-1]
		}
		add(line, in)
	}
	return deps
}

// gemGroupScope classifies a Bundler `group :x[, :y] do` line: any
// mention of :test → test; any other named group (development, ci,
// lint, ...) → dev. A group line is by definition non-default.
func gemGroupScope(line string) string {
	if strings.Contains(line, ":test") {
		return model.ScopeTest
	}
	return model.ScopeDev
}

// gemInlineGroupScope classifies `gem 'x', group: :test` /
// `groups: [:development, :test]` inline options; "" = no group option.
func gemInlineGroupScope(line string) string {
	if !strings.Contains(line, "group") {
		return ""
	}
	idx := strings.Index(line, "group")
	tail := line[idx:]
	if !strings.Contains(tail, ":") {
		return ""
	}
	if strings.Contains(tail, ":test") {
		return model.ScopeTest
	}
	if strings.Contains(tail, ":development") || strings.Contains(tail, ":dev") {
		return model.ScopeDev
	}
	return ""
}

// parseSetupCfgDeps extracts dependency names from setup.cfg [options] install_requires.
func parseSetupCfgDeps(content string) ([]string, error) {
	var deps []string
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Detect install_requires = (with possible inline deps)
		if strings.HasPrefix(trimmed, "install_requires") && strings.Contains(trimmed, "=") {
			inRequires = true
			// Check for inline deps after =
			afterEq := strings.SplitN(trimmed, "=", 2)
			if len(afterEq) == 2 {
				inline := strings.TrimSpace(afterEq[1])
				if inline != "" {
					if name := extractPyDepName(inline); name != "" {
						deps = append(deps, name)
					}
				}
			}
			continue
		}

		// In setup.cfg, continuation lines are indented.
		if inRequires {
			if trimmed == "" || (!strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t")) {
				// End of install_requires section (non-indented non-empty line).
				if trimmed != "" {
					inRequires = false
				}
				continue
			}
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if name := extractPyDepName(trimmed); name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps, nil
}

// parseSetupCfgVersions extracts deps with versions from setup.cfg.
func parseSetupCfgVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "install_requires") && strings.Contains(trimmed, "=") {
			inRequires = true
			afterEq := strings.SplitN(trimmed, "=", 2)
			if len(afterEq) == 2 {
				inline := strings.TrimSpace(afterEq[1])
				if inline != "" {
					if d := ParsePyRequirement(inline); d != nil {
						deps = append(deps, *d)
					}
				}
			}
			continue
		}
		if inRequires {
			if trimmed == "" || (!strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t")) {
				if trimmed != "" {
					inRequires = false
				}
				continue
			}
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if d := ParsePyRequirement(trimmed); d != nil {
				deps = append(deps, *d)
			}
		}
	}
	return deps
}

// parseCsprojDeps extracts dependency names from .csproj PackageReference elements.
// Format: <PackageReference Include="Name" Version="1.0.0" />
func parseCsprojDeps(content string) ([]string, error) {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageReference") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps, nil
}

// parseCsprojVersions extracts deps with versions from .csproj files.
func parseCsprojVersions(content string) []model.LibyearDep {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []model.LibyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageReference") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		version := extractXMLAttr(line, "Version")
		// v0.27.44 (summary/19 P1): PrivateAssets="all" marks a
		// build/analyzer-only package that never flows to consumers
		// or the runtime output → build. Only the attribute form is
		// visible to this line-based parser; the child-element form
		// (<PrivateAssets>all</PrivateAssets>) spans lines and is a
		// documented limitation.
		scope := "runtime"
		if strings.EqualFold(extractXMLAttr(line, "PrivateAssets"), "all") {
			scope = model.ScopeBuild
		}
		if name != "" {
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "nuget"})
		}
	}
	return deps
}

// parseComposerJSON extracts dependency names from composer.json (PHP).
func parseComposerJSON(data []byte) ([]string, error) {
	var pkg struct {
		Require    map[string]string `json:"require"`
		RequireDev map[string]string `json:"require-dev"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []string
	for name := range pkg.Require {
		if name != "php" && !strings.HasPrefix(name, "ext-") {
			deps = append(deps, name)
		}
	}
	for name := range pkg.RequireDev {
		deps = append(deps, name)
	}
	return deps, nil
}

// parseBuildSbt extracts dependency names from Scala build.sbt.
func parseBuildSbt(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		// A declaration someone commented out is not one. Scala comments
		// with `//`, and the version it carries looks valid, so such a row
		// survived normalisation and reached the purl, the registry lookup
		// and the OSV scan (v0.29.57).
		line = strings.TrimSpace(stripSlashComment(line))
		if !strings.Contains(line, "libraryDependencies") || !strings.Contains(line, "%") {
			continue
		}
		// libraryDependencies += "org" %% "name" % "version"
		parts := strings.Split(line, "\"")
		if len(parts) >= 4 {
			org := parts[1]
			name := parts[3]
			deps = append(deps, org+":"+name)
		}
	}
	return deps
}

// parseNuGetPackagesConfig extracts package names from NuGet packages.config (XML).
func parseNuGetPackagesConfig(content string) []string {
	var deps []string
	for _, el := range packagesConfigElements(content) {
		if name := xmlAttrFold(el, "id"); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// packagesConfigElements returns each `<package ...>` element of a
// packages.config document, as written, wherever it sits: at the start of a
// line, after other markup on the line, or spread over several lines. It is
// the one reader of "where is a package element" for both packages.config
// readers (0.29.163); before, an element had to start its line, so a file
// written on one line declared nothing.
//
// A declaration someone commented out is not one, and an XML comment spans
// lines, so the whole document is stripped of comments first (v0.29.57: the
// version inside such a block is valid-looking, so it survived normalisation
// and reached the purl and the OSV scan). The tag is matched without regard
// to case, and must be followed by white space, so `<packages>` is not one.
// A `>` inside a quoted attribute value does not end the element.
func packagesConfigElements(content string) []string {
	content = stripXMLComments(content)
	lower := asciiLower(content)
	const tag = "<package"
	var out []string
	for from := 0; ; {
		at := strings.Index(lower[from:], tag)
		if at < 0 {
			return out
		}
		start := from + at
		after := start + len(tag)
		from = after
		if after >= len(content) || !isXMLSpace(content[after]) {
			continue // <packages>, <packageSources>, or the end of the text
		}
		end := -1
		scanOutsideStrings(content[start:], func(i int, c byte) bool {
			if c == '>' {
				end = start + i
				return false
			}
			return true
		})
		if end < 0 {
			return out // an element that never closes declares nothing
		}
		out = append(out, content[start:end+1])
		from = end + 1
	}
}

func isXMLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// xmlAttrFold returns the value of an attribute of one element, matching the
// attribute's name without regard to case (older .NET projects write
// Id="..." and Version="..."). The name must begin the attribute: `id` does
// not match inside `packageid`. asciiLower, not strings.ToLower: the index
// slices the ORIGINAL text, and only a length-preserving fold keeps the two
// in step.
func xmlAttrFold(element, attr string) string {
	lower := asciiLower(element)
	key := attr + `="`
	for from := 0; ; {
		at := strings.Index(lower[from:], key)
		if at < 0 {
			return ""
		}
		at += from
		from = at + len(key)
		if at > 0 && !isXMLSpace(element[at-1]) {
			continue
		}
		rest := element[at+len(key):]
		if end := strings.IndexByte(rest, '"'); end >= 0 {
			return rest[:end]
		}
		return ""
	}
}

// parsePackageYaml extracts dependency names from Haskell package.yaml
// (hpack): the names the versioned reader finds, so that the two cannot
// disagree about where a dependencies list ends (0.29.208; this one used to
// end a list only at an unindented line, and stored the entries of the
// lists after it, `LambdaCase`, `-Wall`, a module name, as dependencies).
func parsePackageYaml(content string) []string {
	var deps []string
	for _, d := range parseHaskellPackageYamlVersions(content) {
		deps = append(deps, d.Name)
	}
	return deps
}

// parseMixExsDeps extracts dependency names from Elixir mix.exs.
func parseMixExsDeps(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{:") {
			continue
		}
		// {:phoenix, "~> 1.7.0"}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) < 1 {
			continue
		}
		name := strings.TrimPrefix(parts[0], "{:")
		name = strings.TrimSpace(name)
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// ParsePomXMLVersions extracts groupId:artifactId with version from pom.xml.
// Handles both multi-line and single-line XML formats.
func ParsePomXMLVersions(content string) []model.LibyearDep {
	// A declaration someone commented out is not one, and an XML comment
	// spans lines, so the whole document is stripped before it is walked
	// (v0.29.57 — the version inside such a block is valid-looking, so it
	// survived normalisation and reached the purl and the OSV scan).
	content = stripXMLComments(content)
	var deps []model.LibyearDep
	props := pomProperties(content)
	// Process dependency blocks — works even when everything is on one line
	// by scanning for <dependency>...</dependency> substrings.
	rest := content
	// v0.29.178: a dependency declared inside a <plugin> is that plugin's
	// own, build tooling. Its place in the document says so.
	plugins := pomPluginSpans(content)
	// v0.29.183: an entry of dependencyManagement is marked as one; what is
	// done with it is the tree's decision (SettleMavenManaged).
	managed := pomElementSpans(content, "dependencyManagement")
	consumed := 0
	for {
		start := strings.Index(rest, "<dependency>")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], "</dependency>")
		if end < 0 {
			break
		}
		block := rest[start : start+end+len("</dependency>")]
		blockAt := consumed + start
		consumed += start + end + len("</dependency>")
		rest = rest[start+end+len("</dependency>"):]

		// v0.29.56: ${…} references resolve from the pom's <properties> and
		// project coordinates (knative/func "${quarkus.platform.group-id}",
		// dogtagpki/pki "${project.groupId}"). A coordinate that stays
		// unresolved is not sent to Maven Central; an unresolved version
		// is left for NormalizeParsedVersion's unpinned pathway.
		groupID := resolvePomProperties(extractXMLValue(block, "groupId"), props)
		artifactID := resolvePomProperties(extractXMLValue(block, "artifactId"), props)
		version := resolvePomProperties(extractXMLValue(block, "version"), props)
		if strings.Contains(groupID, "${") || strings.Contains(artifactID, "${") {
			continue
		}
		// v0.27.44 (summary/19 P1): Maven's own scope element.
		// test → test; provided = compile-time-only, supplied by the
		// runtime container → build. compile/runtime/absent → runtime.
		depScope := "runtime"
		switch extractXMLValue(block, "scope") {
		case "test":
			depScope = model.ScopeTest
		case "provided":
			depScope = model.ScopeBuild
		}
		if plugins.contains(blockAt) {
			depScope = model.ScopeBuild
		}
		if groupID != "" && artifactID != "" {
			name := groupID + ":" + artifactID
			isManaged := managed.contains(blockAt)
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: name + ":" + version, Type: depScope, Manager: "maven",
				ManagedEntry: isManaged,
				BOMImport:    isManaged && extractXMLValue(block, "scope") == "import" && extractXMLValue(block, "type") == "pom"})
		}
	}
	return deps
}

// pomPropertyRe matches one <name>value</name> element; the closing name
// is compared in code (RE2 has no backreferences).
var pomPropertyRe = regexp.MustCompile(`<([A-Za-z0-9_.\-]+)>([^<]*)</([A-Za-z0-9_.\-]+)>`)

// pomProperties collects a pom's own property values: every
// <properties> element, plus project.groupId/artifactId/version (the
// project's own coordinates, falling back to <parent>'s groupId and
// version, as Maven inherits them) and their parent.* forms.
func pomProperties(content string) map[string]string {
	props := map[string]string{}
	rest := content
	for {
		start := strings.Index(rest, "<properties>")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], "</properties>")
		if end < 0 {
			break
		}
		for _, m := range pomPropertyRe.FindAllStringSubmatch(rest[start:start+end], -1) {
			if m[1] == m[3] {
				if _, seen := props[m[1]]; !seen {
					props[m[1]] = strings.TrimSpace(m[2])
				}
			}
		}
		rest = rest[start+end:]
	}
	parent := ""
	top := content
	if ps := strings.Index(content, "<parent>"); ps >= 0 {
		if pe := strings.Index(content[ps:], "</parent>"); pe >= 0 {
			parent = content[ps : ps+pe]
			top = content[:ps] + content[ps+pe:]
		}
	}
	// The project's own coordinates are the ones outside every nested block.
	// One pass per kind of block (0.29.208): cutting each block out of the
	// string in turn copied the rest of the document every time, the square
	// of its size for a pom of many small blocks.
	for _, block := range []string{"dependencyManagement", "dependencies", "build", "profiles", "reporting", "properties", "modules"} {
		opening, closing := "<"+block+">", "</"+block+">"
		if !strings.Contains(top, opening) {
			continue
		}
		var kept strings.Builder
		rest := top
		for {
			bs := strings.Index(rest, opening)
			if bs < 0 {
				break
			}
			be := strings.Index(rest[bs:], closing)
			if be < 0 {
				break
			}
			kept.WriteString(rest[:bs])
			rest = rest[bs+be+len(closing):]
		}
		kept.WriteString(rest)
		top = kept.String()
	}
	for _, key := range []string{"groupId", "artifactId", "version"} {
		if v := extractXMLValue(parent, key); v != "" {
			props["parent."+key] = v
			props["project.parent."+key] = v
		}
		v := extractXMLValue(top, key)
		if v == "" && key != "artifactId" {
			v = extractXMLValue(parent, key)
		}
		if v != "" {
			props["project."+key] = v
			props["pom."+key] = v
		}
	}
	return props
}

// resolvePomProperties substitutes ${name} references from props, by the
// one bounded substitution a pom has (pomSubstitute, 0.29.209). It had its
// own loop, one reference per turn with nothing bounding what it built: a
// version that is one 64-byte property 20,000 times over was built whole,
// copied 20,000 times on the way.
func resolvePomProperties(v string, props map[string]string) string {
	return pomSubstitute(v, func(name string) (string, bool) {
		val, ok := props[name]
		return val, ok
	})
}

func extractXMLValue(line, tag string) string {
	start := strings.Index(line, "<"+tag+">")
	end := strings.Index(line, "</"+tag+">")
	// The tags may appear in the WRONG ORDER — `</groupId><groupId>` is a
	// document a repository can carry — and slicing between them without
	// checking would panic, killing the analysis phase for that whole
	// repository. Same class as the v0.27.99 `]` before `[` fuzz find; this
	// one surfaced the moment ParsePomXMLVersions joined the fuzz target
	// (v0.29.57).
	if start < 0 || end < start+len(tag)+2 {
		return ""
	}
	return strings.TrimSpace(line[start+len(tag)+2 : end])
}

// parseComposerJSONVersions is parseComposerJSONVersionsIn for a manifest
// read on its own: its directory is the tree it may look into.
func parseComposerJSONVersions(path string) ([]model.LibyearDep, error) {
	return parseComposerJSONVersionsIn(filepath.Dir(path), path)
}

// parseComposerJSONVersionsIn extracts deps with versions from
// composer.json. root is the clone's root: a package the manifest's own
// `repositories` supply from a directory of the clone, or declare inline,
// is not Packagist's package of that name (0.29.208).
func parseComposerJSONVersionsIn(root, path string) ([]model.LibyearDep, error) {
	data, err := ReadManifest(path)
	if err != nil {
		return nil, err
	}
	var pkg struct {
		Require      map[string]string `json:"require"`
		RequireDev   map[string]string `json:"require-dev"`
		Repositories json.RawMessage   `json:"repositories"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	own := composerOwnRepositoryPackages(root, filepath.Dir(path), pkg.Repositories)
	var deps []model.LibyearDep
	for name, version := range pkg.Require {
		if isComposerPlatformName(name) {
			continue
		}
		deps = append(deps, model.LibyearDep{Name: name, Version: CleanVersion(version), Requirement: version, Type: "runtime", Manager: "packagist", NonRegistry: own[strings.ToLower(name)]})
	}
	for name, version := range pkg.RequireDev {
		if isComposerPlatformName(name) {
			continue
		}
		deps = append(deps, model.LibyearDep{Name: name, Version: CleanVersion(version), Requirement: version, Type: "dev", Manager: "packagist", NonRegistry: own[strings.ToLower(name)]})
	}
	return deps, nil
}

// composerOwnRepositoryPackages are the package names (lower case; Composer
// compares names without case) that a composer.json's `repositories` supply
// from somewhere this reader can see: a `package` repository, which states
// its package inline, and a `path` repository whose directory is in the
// clone, where each composer.json names one. The key is a list or an
// object; an entry that is not a repository (`{"packagist.org": false}`)
// has no type and is passed over.
//
// A `vcs` or `git` repository's package is named in that repository, which
// is not read: its requirement stays a Packagist one (declined in review
// round 1 with this reason: knowing the name would take a fetch).
func composerOwnRepositoryPackages(root, dir string, raw json.RawMessage) map[string]bool {
	out := map[string]bool{}
	if len(raw) == 0 {
		return out
	}
	type repository struct {
		Type    string          `json:"type"`
		URL     string          `json:"url"`
		Package json.RawMessage `json:"package"`
	}
	var repos []repository
	var list []json.RawMessage
	if json.Unmarshal(raw, &list) != nil {
		var keyed map[string]json.RawMessage
		if json.Unmarshal(raw, &keyed) != nil {
			return out
		}
		for _, v := range keyed {
			list = append(list, v)
		}
	}
	for _, v := range list {
		var r repository
		if json.Unmarshal(v, &r) == nil {
			repos = append(repos, r)
		}
	}
	var named struct {
		Name string `json:"name"`
	}
	for _, r := range repos {
		switch r.Type {
		case "package":
			// One package, or a list of them.
			var many []json.RawMessage
			if json.Unmarshal(r.Package, &many) != nil {
				many = []json.RawMessage{r.Package}
			}
			for _, one := range many {
				named.Name = ""
				if json.Unmarshal(one, &named) == nil && named.Name != "" {
					out[strings.ToLower(named.Name)] = true
				}
			}
		case "path":
			if r.URL == "" || filepath.IsAbs(r.URL) || strings.HasPrefix(r.URL, "~") {
				continue
			}
			// The URL may be a glob (`packages/*`). It is judged BEFORE it
			// is expanded (0.29.209): a pattern that leaves the clone
			// (`../../../*/*/*`) used to be expanded first, which lists
			// directories of the host, and each match refused after.
			pattern := filepath.Join(dir, filepath.FromSlash(r.URL))
			if !composerPatternInClone(root, pattern) {
				continue
			}
			matches := composerExpand(root, pattern)
			for _, m := range matches {
				file := filepath.Join(m, "composer.json")
				if !regularBuildFile(root, file) {
					continue
				}
				body, err := ReadManifest(file)
				named.Name = ""
				if err == nil && json.Unmarshal(body, &named) == nil && named.Name != "" {
					out[strings.ToLower(named.Name)] = true
				}
			}
		}
	}
	return out
}

// composerEntriesLooked is how many directory entries the expansions have
// looked at, for the test of the bound. Atomic: manifests are read on
// several workers at once, and nothing a parse writes may be a plain
// package variable (round 4: this one and pomSpanSteps were, a data race).
var composerEntriesLooked atomic.Int64

// composerExpand is what a path repository's pattern matches: directories
// of the clone. A seam: the test records what is expanded.
var composerExpand = composerExpandInClone

// composerExpandInClone expands the pattern one path element at a time,
// and never lists or enters a directory that is not really in the clone
// (0.29.210). filepath.Glob was given the whole pattern: containment was
// checked for its fixed leading directories, and a symbolic link in the
// clone that a wildcard matched was expanded through, out of the clone,
// before any match was refused.
//
// The work is bounded as well as the result, and the result does not
// depend on the order a directory lists its entries in (0.29.211): for each
// wildcard element at most composerPathMatchLimit directory entries are
// looked at in all. A pattern that would need more is NOT expanded: none of
// its packages is known to be the manifest's own, and all stay Packagist
// requirements, which is what every such package was before 0.29.208.
// (Round 4: the first thousand matches in directory order were kept, a
// different thousand on another file system.) Within the bound every match
// is found, and they are returned sorted.
//
// A link is resolved only where there is one: an entry that is a plain
// directory of a directory really in the clone is really in the clone.
func composerExpandInClone(root, pattern string) []string {
	root = filepath.Clean(root)
	rel, err := filepath.Rel(root, filepath.Clean(pattern))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil
	}
	// viaLink: the directory a link leads to is really inside the clone.
	viaLink := func(p string) bool {
		real, err := filepath.EvalSymlinks(p)
		if err != nil || !pathWithin(realRoot, real) {
			return false
		}
		info, err := os.Stat(real)
		return err == nil && info.IsDir()
	}
	// inClone: p, a child of a directory really in the clone, is a
	// directory really in the clone. mode is its own (not followed).
	inClone := func(p string, mode fs.FileMode) bool {
		switch {
		case mode.IsDir():
			return true
		case mode&fs.ModeSymlink != 0:
			return viaLink(p)
		}
		return false
	}
	elements := strings.Split(rel, string(filepath.Separator))
	wildcards := 0
	for _, el := range elements {
		if strings.ContainsAny(el, `*?[\`) {
			wildcards++
		}
	}
	looked, mayLook := 0, composerPathMatchLimit*wildcards
	defer func() { composerEntriesLooked.Add(int64(looked)) }()
	dirs := []string{root}
	for _, el := range elements {
		var next []string
		for _, d := range dirs {
			if !strings.ContainsAny(el, `*?[\`) {
				p := filepath.Join(d, el)
				if info, err := os.Lstat(p); err == nil && inClone(p, info.Mode()) {
					next = append(next, p)
				}
				continue
			}
			f, err := os.Open(d)
			if err != nil {
				continue
			}
			for {
				entries, err := f.ReadDir(composerReadDirBatch)
				if looked+len(entries) > mayLook {
					// More entries than this pattern may look at: it is
					// not expanded at all.
					looked += len(entries)
					_ = f.Close()
					return nil
				}
				for _, e := range entries {
					looked++
					if ok, _ := filepath.Match(el, e.Name()); ok {
						if p := filepath.Join(d, e.Name()); inClone(p, e.Type()) {
							next = append(next, p)
						}
					}
				}
				if err != nil || len(entries) == 0 {
					break
				}
			}
			_ = f.Close()
		}
		sort.Strings(next)
		if dirs = next; len(dirs) == 0 {
			return nil
		}
	}
	return dirs
}

// composerReadDirBatch is how many entries of a directory are read at a
// time, so that a directory far larger than the bound is not read whole to
// find that out.
const composerReadDirBatch = 64

// composerPathMatchLimit bounds the directory entries one wildcard of a
// `path` repository pattern may look at (and so the directories whose
// composer.json is read). A pattern past it is not expanded.
// PROVISIONAL (summary/71, section 8): the figure is not measured; it is
// several times the largest monorepo of path packages I know of (a few
// hundred components).
const composerPathMatchLimit = 1000

// composerPatternInClone reports whether a path repository's pattern stays
// in the clone: as written (the pattern cleaned, so a `..` after a wildcard
// is counted), and where its fixed leading directories really are once
// symbolic links are followed.
func composerPatternInClone(root, pattern string) bool {
	root = filepath.Clean(root)
	if !pathWithin(root, pattern) {
		return false
	}
	fixed := pattern
	for strings.ContainsAny(fixed, `*?[\`) {
		fixed = filepath.Dir(fixed)
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	realFixed, err := filepath.EvalSymlinks(fixed)
	if err != nil {
		return false // it is not there: nothing would match
	}
	return pathWithin(realRoot, realFixed)
}

// mixWithoutComment cuts a line's comment. It is given the line with its
// strings emptied, so a `#` inside one is not a comment's start; without
// this a remark about where a dependency used to come from (`# was git:`)
// marked a Hex dependency as not from Hex (review round 2).
func mixWithoutComment(line string) string {
	if i := strings.IndexByte(line, '#'); i >= 0 {
		return line[:i]
	}
	return line
}

// isComposerPlatformName reports whether a composer.json requirement names
// the platform and not a package: PHP itself, a PHP extension (ext-*), a
// system library (lib-*), or one of Composer's own API constraints. None is
// on Packagist and none has a vendor, so none has a purl. The versioned
// reader leaves them out of require and require-dev alike (0.29.164: it left
// out only php and ext-*, and only in require). The dependency-name reader
// (parseComposerJSON) keeps its own narrower filter on purpose: the name
// inventory's counts do not change in this program.
func isComposerPlatformName(name string) bool {
	return name == "php" || name == "composer-runtime-api" || name == "composer-plugin-api" ||
		strings.HasPrefix(name, "ext-") || strings.HasPrefix(name, "lib-")
}

// mixSourceOption: a Mix dependency taken from a directory, a git
// repository or the umbrella it lives in, and so not Hex's package of the
// name. Read from the line with its string literals removed. A private Hex
// organization (`organization:`) is still Hex.
var mixSourceOption = regexp.MustCompile(`[\s,](path|git|github|in_umbrella):`)

// parseMixExsVersions extracts deps with versions from Elixir mix.exs.
func parseMixExsVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	for _, line := range strings.Split(content, "\n") {
		// A list written with the comma in front (`, {:b, "~> 2.0"}`) holds
		// the same tuples (round 5: every one but the first was unread).
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), ","))
		if !strings.HasPrefix(line, "{:") {
			continue
		}
		// {:phoenix, "~> 1.7.0"}
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimPrefix(strings.TrimSpace(parts[0]), "{:")
		// v0.27.71: only a QUOTED second element is a version
		// requirement ("~> 1.7.0"). Keyword options (git:, github:,
		// path:) mark source deps with no registry version — the
		// pre-fix code captured the option text as the version.
		version := ""
		if p1 := strings.TrimSpace(parts[1]); strings.HasPrefix(p1, `"`) {
			if end := strings.IndexByte(p1[1:], '"'); end >= 0 {
				// What the quotes hold, and nothing after them: a comment
				// after the tuple was read into the version (`1.0"}`,
				// round 4).
				version = CleanVersion(p1[1 : 1+end])
			} else {
				// (A requirement with a comma in it: its first bound.)
				version = CleanVersion(strings.Trim(p1, "\"'}] "))
			}
			// A requirement built by interpolation (`"~> #{@version}"`) is
			// worked out when the build runs: what is written is no version
			// (round 5: `#{@version}` was stored as one).
			if strings.Contains(p1, "#{") {
				version = ""
			}
		}
		// v0.27.44 (summary/19 P1): Mix's only: option restricts a dep
		// to specific envs. only: :test → test; anything mentioning
		// :dev → dev (only: [:dev, :test] is a dev-tooling dep that
		// also runs in test). No only: → runtime.
		scope := "runtime"
		if onlyIdx := strings.Index(line, "only:"); onlyIdx >= 0 {
			onlyPart := line[onlyIdx:]
			switch {
			case strings.Contains(onlyPart, ":dev"):
				scope = model.ScopeDev
			case strings.Contains(onlyPart, ":test"):
				scope = model.ScopeTest
			}
		}
		if name != "" {
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "hex",
				NonRegistry: mixSourceOption.MatchString(mixWithoutComment(withoutQuoted(line)))})
		}
	}
	return deps
}

// parseNuGetPackagesConfigVersions extracts packages with versions from packages.config.
func parseNuGetPackagesConfigVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	for _, el := range packagesConfigElements(content) {
		name := xmlAttrFold(el, "id")
		if name == "" {
			continue
		}
		// v0.27.44 (summary/19 P1): developmentDependency="true" is the
		// legacy packages.config marker for build-only packages.
		scope := "runtime"
		if strings.EqualFold(xmlAttrFold(el, "developmentdependency"), "true") {
			scope = model.ScopeBuild
		}
		// The requirement is the element as written: for a file with one
		// element per line that is the line it always was.
		deps = append(deps, model.LibyearDep{Name: name, Version: xmlAttrFold(el, "version"), Requirement: el, Type: scope, Manager: "nuget"})
	}
	return deps
}

// parseBuildSbtVersions extracts deps with versions from build.sbt.
func parseBuildSbtVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	for _, line := range strings.Split(content, "\n") {
		// A declaration someone commented out is not one. Scala comments
		// with `//`, and the version it carries looks valid, so such a row
		// survived normalisation and reached the purl, the registry lookup
		// and the OSV scan (v0.29.57).
		line = strings.TrimSpace(stripSlashComment(line))
		if !strings.Contains(line, "libraryDependencies") || !strings.Contains(line, "%") {
			continue
		}
		parts := strings.Split(line, "\"")
		if len(parts) >= 6 {
			org := parts[1]
			name := parts[3]
			version := parts[5]
			fullName := org + ":" + name
			// v0.27.44 (summary/19 P1): sbt configuration after the
			// version — `% Test` (or the quoted "test" form) → test;
			// `% Provided` mirrors Maven provided → build;
			// `% Optional` → optional. No configuration → runtime.
			scope := "runtime"
			switch {
			case strings.Contains(line, "% Test") || strings.Contains(line, `% "test"`):
				scope = model.ScopeTest
			case strings.Contains(line, "% Provided") || strings.Contains(line, `% "provided"`):
				scope = model.ScopeBuild
			case strings.Contains(line, "% Optional") || strings.Contains(line, `% "optional"`):
				scope = model.ScopeOptional
			}
			deps = append(deps, model.LibyearDep{Name: fullName, Version: version, Requirement: line, Type: scope, Manager: "maven"})
		}
	}
	return deps
}

// parsePubspecDeps extracts dependency names from Dart pubspec.yaml.
func parsePubspecDeps(content string) []string {
	var deps []string
	inDeps := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" || trimmed == "dev_dependencies:" {
			inDeps = true
			continue
		}
		// Section ends at a top-level key (no leading whitespace).
		if inDeps && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inDeps = false
		}
		if !inDeps {
			continue
		}
		// Skip sdk dependencies like "flutter: sdk: flutter".
		if strings.Contains(trimmed, "sdk:") {
			continue
		}
		// Line like "  http: ^0.13.6" — name is the key before the colon.
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "#") {
			name := strings.TrimSpace(strings.SplitN(trimmed, ":", 2)[0])
			if name != "" && name != "flutter" && name != "flutter_test" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// pubSourceKeys are the keys under a pubspec dependency that say where it
// comes from when that is not pub.dev: a directory, a git repository, an
// SDK, or another package server.
var pubSourceKeys = map[string]bool{"path": true, "git": true, "sdk": true, "hosted": true}

// pubHostedIsPubDev reports whether a `hosted` value names pub.dev itself
// (or its old name), written out: the registry, and no other server.
func pubHostedIsPubDev(value string) bool {
	host := strings.ToLower(strings.Trim(strings.TrimSpace(value), `"'`))
	for _, scheme := range []string{"https://", "http://"} {
		host = strings.TrimPrefix(host, scheme)
	}
	host = strings.TrimSuffix(host, "/")
	return host == "pub.dev" || host == "pub.dartlang.org"
}

// pubSourceKey reports whether one `key: value` under a dependency says it
// comes from somewhere that is not pub.dev. `hosted` does only when it
// names another server: `hosted: https://pub.dev` is the registry (review
// round 2: it was marked as not from it). The older form, `hosted:` with
// `name:` and `url:` below it, is decided by its `url:` line: undecided
// reports that, and the caller waits for the address.
func pubSourceKey(pair string) (source, undecided bool) {
	key, value, ok := strings.Cut(pair, ":")
	key = strings.TrimSpace(key)
	if !ok || !pubSourceKeys[key] {
		return false, false
	}
	if key != "hosted" {
		return true, false
	}
	if strings.TrimSpace(value) == "" {
		return false, true
	}
	return !pubHostedIsPubDev(value), false
}

// parsePubspecVersions extracts deps with versions from Dart pubspec.yaml.
func parsePubspecVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	inDeps := false
	depType := "runtime"
	// v0.27.71: block-style deps nest their source under the name:
	//
	//	my_lib:
	//	  path: ../
	//
	// The pre-fix parser emitted the SUB-KEYS as deps — a package
	// named "path" at version "../" (90 production rows), fake "git"/
	// "url"/"ref" packages. Track the indent of the first dep line in
	// each section; anything deeper is a sub-key, not a dep. (Name
	// filtering would be wrong: "path" is a real pub.dev package that
	// legitimately appears at dep level.)
	depIndent := -1
	// last is the dependency whose sub-keys the next deeper lines are; -1
	// for none. A sub-key that names a SOURCE (0.29.208) makes it a
	// dependency pub.dev does not supply.
	last := -1
	// hostedAt is the indentation of an open `hosted:` block of the last
	// dependency (-1 for none): its `url:` says which server.
	hostedAt := -1
	// versionAt is the indentation of a `version:` key of the last
	// dependency that had no value on its line (-1 for none): the value is
	// the next line, written deeper (round 5).
	versionAt := -1
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" {
			inDeps = true
			depType = "runtime"
			depIndent = -1
			last = -1
			continue
		}
		if trimmed == "dev_dependencies:" {
			inDeps = true
			depType = "dev"
			depIndent = -1
			last = -1
			continue
		}
		if inDeps && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inDeps = false
		}
		if !inDeps {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if versionAt >= 0 && trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			at := versionAt
			versionAt = -1
			// The value, when the line is one: a key of its own (`path:
			// ../x`, `ref: v1`) or a list item is not a version, and is read
			// below as what it is (round 6: it was taken for the version,
			// and a source named there was never seen).
			if last >= 0 && indent > at && pubPlainScalar(trimmed) {
				if deps[last].Version == "" {
					deps[last].Version = CleanVersion(yamlScalar(trimmed))
				}
				continue
			}
		}
		if last >= 0 && indent > depIndent && !strings.HasPrefix(trimmed, "#") {
			switch {
			case hostedAt >= 0 && indent > hostedAt:
				// Inside `hosted:`: the address decides.
				if key, value, ok := strings.Cut(trimmed, ":"); ok && strings.TrimSpace(key) == "url" && !pubHostedIsPubDev(value) {
					deps[last].NonRegistry = true
				}
			default:
				hostedAt = -1
				source, undecided := pubSourceKey(trimmed)
				if source {
					deps[last].NonRegistry = true
				}
				if undecided {
					hostedAt = indent
				}
				// The block form has its version on a line of its own
				// (`hosted:` and `version:` under the name): round 4, it
				// had none.
				if key, value, ok := strings.Cut(trimmed, ":"); ok && strings.TrimSpace(key) == "version" && deps[last].Version == "" {
					deps[last].Version = CleanVersion(yamlScalar(value))
					if strings.TrimSpace(yamlScalar(value)) == "" {
						versionAt = indent
					}
				}
			}
		} else if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			hostedAt = -1
		}
		if strings.Contains(trimmed, "sdk:") {
			continue
		}
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "#") {
			if depIndent == -1 {
				depIndent = indent
			}
			if indent > depIndent {
				continue // sub-key of a block-style dep
			}
			last = -1
			parts := strings.SplitN(trimmed, ":", 2)
			name := strings.TrimSpace(parts[0])
			if name == "" || name == "flutter" || name == "flutter_test" {
				continue
			}
			versionStr := strings.TrimSpace(parts[1])
			// The value, not the rest of the line (0.29.210): a comment
			// after it was read as part of the range.
			version := CleanVersion(yamlScalar(versionStr))
			// The same sources written inline: `name: {path: ../name}`.
			inline := false
			if strings.HasPrefix(versionStr, "{") {
				for _, part := range strings.Split(strings.Trim(versionStr, "{} "), ",") {
					// An inline `hosted:` with no address of its own
					// (a nested table) is not read here and is taken
					// for another server.
					if source, undecided := pubSourceKey(part); source || undecided {
						inline = true
					}
				}
			}
			deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: trimmed, Type: depType, Manager: "pub", NonRegistry: inline})
			last = len(deps) - 1
		}
	}
	return deps
}

// pubPlainScalar reports whether a line of a pubspec is a plain value: not
// a mapping key and not a list item. A quoted line is a value whatever it
// holds.
func pubPlainScalar(trimmed string) bool {
	if trimmed == "" {
		return false
	}
	if trimmed[0] == '"' || trimmed[0] == '\'' {
		return true
	}
	if trimmed == "-" || strings.HasPrefix(trimmed, "- ") {
		return false
	}
	// Without its comment.
	if i := strings.Index(trimmed, " #"); i >= 0 {
		trimmed = strings.TrimSpace(trimmed[:i])
	}
	return !strings.HasSuffix(trimmed, ":") && !strings.Contains(trimmed, ": ")
}

// parsePackageSwiftDeps extracts dependency names from Package.swift.
func parsePackageSwiftDeps(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		// See parseBuildSbt: a commented-out `.package(url:)` is not a
		// dependency. The stripper is quote-aware because every package URL
		// contains `//` inside its quotes.
		line = strings.TrimSpace(stripSlashComment(line))
		if !strings.Contains(line, ".package(url:") {
			continue
		}
		// Extract repo name from URL: "https://github.com/Alamofire/Alamofire.git"
		if name := ExtractSwiftPackageName(line); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// parsePackageSwiftVersions extracts deps with versions from Package.swift.
func parsePackageSwiftVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	for _, line := range strings.Split(content, "\n") {
		// See parseBuildSbt: a commented-out `.package(url:)` is not a
		// dependency. The stripper is quote-aware because every package URL
		// contains `//` inside its quotes.
		line = strings.TrimSpace(stripSlashComment(line))
		if !strings.Contains(line, ".package(url:") {
			continue
		}
		name := ExtractSwiftPackageName(line)
		if name == "" {
			continue
		}
		// Extract version: from: "5.6.0", exact: "5.0.1", .upToNextMajor(from: "10.40.0")
		version := extractSwiftVersion(line)
		repoURL := extractSwiftRepoURL(line)
		deps = append(deps, model.LibyearDep{
			Name:        name,
			Version:     version,
			Requirement: repoURL,
			Type:        "runtime",
			Manager:     "swiftpm",
		})
	}
	return deps
}

// ExtractSwiftPackageName pulls the repo name from a .package(url:...) line.
func ExtractSwiftPackageName(line string) string {
	// Find URL between quotes after "url:"
	urlStart := strings.Index(line, `url:`)
	if urlStart < 0 {
		return ""
	}
	rest := line[urlStart+4:]
	// Find the quoted URL.
	q1 := strings.IndexByte(rest, '"')
	if q1 < 0 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '"')
	if q2 < 0 {
		return ""
	}
	url := rest[q1+1 : q1+1+q2]
	// v0.29.59: the shared Swift URL parse (ParseSwiftPackageURL), so the
	// manifest's name is the SAME repository name the Package.resolved
	// reader stores; a non-URL value keeps the last-segment fallback.
	if ref, ok := ParseSwiftPackageURL(url); ok {
		return ref.Repo
	}
	// Extract repo name: last path component, without the URL's ".git" or
	// trailing "/" (the shared suffix rule, follow-up 8).
	parts := strings.Split(model.NormalizeRepoGitURL(url), "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// extractSwiftRepoURL pulls the git URL from a .package(url:...) line.
func extractSwiftRepoURL(line string) string {
	urlStart := strings.Index(line, `url:`)
	if urlStart < 0 {
		return ""
	}
	rest := line[urlStart+4:]
	q1 := strings.IndexByte(rest, '"')
	if q1 < 0 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '"')
	if q2 < 0 {
		return ""
	}
	return rest[q1+1 : q1+1+q2]
}

// extractSwiftVersion pulls the version from patterns like:
//
//	from: "5.6.0", exact: "5.0.1", .upToNextMajor(from: "10.40.0")
func extractSwiftVersion(line string) string {
	// Try patterns in order: from: "x", exact: "x", .upToNextMajor(from: "x")
	for _, prefix := range []string{`from: "`, `exact: "`, `.upToNextMajor(from: "`, `.upToNextMinor(from: "`} {
		idx := strings.Index(line, prefix)
		if idx < 0 {
			continue
		}
		rest := line[idx+len(prefix):]
		end := strings.IndexByte(rest, '"')
		if end > 0 {
			return rest[:end]
		}
	}
	return ""
}

// parseHaskellPackageYamlVersions extracts deps with versions from package.yaml.
// Version constraints like ">= 2.0" are parsed to extract the lower bound.
func parseHaskellPackageYamlVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	inDeps := false
	// v0.27.44 (summary/19 P1): a dependencies: block nested under a
	// top-level tests: or benchmarks: section is test-only. The
	// section tracker watches unindented `key:` lines; library:,
	// executables:, and the top level stay runtime.
	sectionScope := "runtime"
	depsIndent, itemIndent := 0, -1
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && strings.HasSuffix(trimmed, ":") {
			switch strings.TrimSuffix(trimmed, ":") {
			case "tests", "benchmarks":
				sectionScope = model.ScopeTest
			default:
				sectionScope = "runtime"
			}
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if trimmed == "dependencies:" {
			inDeps = true
			depsIndent, itemIndent = indent, -1
			continue
		}
		// 0.29.208: the list ends at the next line that is not one of its
		// items: a key at the list's own indentation or above it. It used
		// to end only at an unindented line, so the lists after it in a
		// section (default-extensions, exposed-modules, ghc-options) were
		// read as Hackage packages. A deeper line belongs to an item.
		if inDeps && trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			isItem := strings.HasPrefix(trimmed, "- ")
			switch {
			case isItem && itemIndent == -1 && indent >= depsIndent:
				itemIndent = indent
			case isItem && indent == itemIndent:
			case itemIndent != -1 && indent > itemIndent:
				continue // part of an item
			default:
				inDeps = false
			}
		}
		if inDeps && strings.HasPrefix(trimmed, "- ") {
			dep := strings.TrimPrefix(trimmed, "- ")
			// The object form: `- name: aeson`, its other keys below it.
			if rest, ok := strings.CutPrefix(dep, "name:"); ok {
				dep = strings.Trim(strings.TrimSpace(rest), `"'`)
			}
			name := dep
			version := ""
			// Parse "base >= 4.7 && < 5" -> name="base", version="4.7"
			if idx := strings.IndexAny(dep, " ><=!"); idx > 0 {
				name = dep[:idx]
				// Extract the first version number from the constraint.
				rest := dep[idx:]
				version = extractFirstVersion(rest)
			}
			if name != "" {
				deps = append(deps, model.LibyearDep{Name: name, Version: version, Requirement: dep, Type: sectionScope, Manager: "hackage"})
			}
		} else if inDeps && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && trimmed != "" {
			inDeps = false
		}
	}
	return deps
}

// extractFirstVersion pulls the first semver-like number from a constraint string.
// e.g., ">= 2.0 && < 3" -> "2.0", " ^4.7" -> "4.7"
func extractFirstVersion(s string) string {
	var start, end int
	inVersion := false
	for i, c := range s {
		if (c >= '0' && c <= '9') || c == '.' {
			if !inVersion {
				start = i
				inVersion = true
			}
			end = i + 1
		} else if inVersion {
			break
		}
	}
	if inVersion {
		return strings.TrimRight(s[start:end], ".")
	}
	return ""
}

// pomSpans are the [start, end) offsets of every <plugin>...</plugin> in a
// pom.
type pomSpans [][2]int

func (s pomSpans) contains(offset int) bool {
	// The spans are in document order and do not overlap (pomElementSpans
	// moves past each one), so the one that could hold offset is found by
	// halving (0.29.209). Every dependency block used to be compared with
	// every span: a pom of n plugins and n dependencies, n squared.
	i := sort.Search(len(s), func(k int) bool { pomSpanSteps.Add(1); return s[k][1] > offset })
	return i < len(s) && offset >= s[i][0]
}

// pomSpanSteps counts the spans contains has looked at, for the test that
// pins the halving: on this path a scan of every span costs milliseconds
// and allocates nothing, so neither a clock nor the allocator shows it.
// Atomic: poms are read on several workers at once (see
// composerEntriesLooked).
var pomSpanSteps atomic.Int64

func pomPluginSpans(content string) pomSpans { return pomElementSpans(content, "plugin") }

// pomElementSpans are the spans of every <name>...</name> in a pom.
func pomElementSpans(content, name string) pomSpans {
	var out pomSpans
	open, shut := "<"+name+">", "</"+name+">"
	at := 0
	for {
		start := strings.Index(content[at:], open)
		if start < 0 {
			return out
		}
		start += at
		end := strings.Index(content[start:], shut)
		if end < 0 {
			return out
		}
		out = append(out, [2]int{start, start + end})
		at = start + end + len(shut)
	}
}
