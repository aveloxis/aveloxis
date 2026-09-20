// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// manifest_toml.go — v0.29.56. One scanner for TOML dependency tables
// (Cargo.toml [dependencies]/[dev-dependencies]/[build-dependencies],
// poetry's [tool.poetry.dependencies]), shared by the libyear parser
// (parseCargoVersions) and the name inventory (parseTOMLDeps) so the two
// cannot read the same file differently (SR-17).
//
// Both line parsers took any line with '=' as "name = value": a comment
// ("# zenoh-flat's `unstable` feature (= `zenoh/unstable`)"), the closing
// line of a multi-line inline table ("], default-features = false }") and
// the dotted-key form ("oci-spec.workspace = true") all became package
// names, which crates.io answered 404 (10 lines, 1,702 dependencies in two
// hours of the 2026-09-17 chaoss.tv log for the dotted form alone).
//
// This is not a TOML parser: it reads the key = value lines of the named
// sections, which is all a dependency table holds.

import (
	"strings"
)

// maxTOMLDeclLen caps the stored declaration text. Real dependency
// declarations are one short line; anything longer is a malformed file
// whose value never closed.
const maxTOMLDeclLen = 512

// tomlDepEntry is one dependency declared in a TOML dependency table.
type tomlDepEntry struct {
	Section  string // the section header, e.g. "[dev-dependencies]"
	Name     string
	Version  string // the version string or table's version key; "" when absent
	Git      bool   // sourced from git
	Path     bool   // sourced from a local path
	Registry bool   // sourced from a registry other than the default
	Raw      string // the declaration as written, comments removed; dotted keys joined with "; "
}

// scanTOMLDepTables returns the entries declared in the given sections, in
// first-appearance order. Dotted keys of one dependency (serde.version,
// serde.features) merge into a single entry.
func scanTOMLDepTables(content string, sections map[string]bool) []tomlDepEntry {
	var out []tomlDepEntry
	index := map[string]int{} // section + name → out index
	section := ""
	var pendingKey, pendingValue string
	depth := 0

	add := func(key, value string) {
		if !sections[section] {
			return
		}
		name, sub := splitTOMLDottedKey(key)
		if name == "" {
			return
		}
		k := section + "\x00" + name
		i, ok := index[k]
		if !ok {
			out = append(out, tomlDepEntry{Section: section, Name: name})
			i = len(out) - 1
			index[k] = i
		}
		e := &out[i]
		value = strings.TrimSpace(value)
		// The declaration is stored as the row's `requirement` (the raw
		// manifest truth the GUI renders). A multi-line value is joined, and
		// a manifest with an unclosed bracket would otherwise join the rest
		// of the file into one string, so cap it.
		decl := key + " = " + value
		if len(decl) > maxTOMLDeclLen {
			decl = decl[:maxTOMLDeclLen] + "…"
		}
		if e.Raw == "" {
			e.Raw = decl
		} else {
			e.Raw += "; " + decl
		}
		switch sub {
		case "":
			if strings.HasPrefix(value, "{") {
				applyTOMLDepTable(e, value)
			} else {
				e.Version = tomlUnquote(value)
			}
		case "version":
			e.Version = tomlUnquote(value)
		case "git":
			e.Git = true
		case "path":
			e.Path = true
		case "registry", "registry-index":
			e.Registry = true
		}
		// A workspace-inherited dep ("name.workspace = true") needs no case:
		// it declares no version here, so it takes the unpinned pathway and
		// is looked up by name, exactly as the inline form
		// ({ workspace = true }) has always been (the manifest corpus pins
		// that). ASSUMPTION, not a verified fact: that the workspace root
		// declares it from crates.io. If the root declares it as a path or
		// git dependency, this looks up whatever crates.io package shares
		// the name — the SR-6 class. Closing it means reading the root's
		// [workspace.dependencies], which this per-file scanner does not
		// see; it is on the worklist rather than guessed at here.
	}

	for _, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(stripHashComment(raw))
		if depth > 0 {
			// A value that never closes must not swallow the rest of the
			// file: a section header ends it, since no inline table can span
			// one. This scanner is the FIFTH reader that tracks this depth
			// (the four line readers in analysis.go and analysis_devbuild.go
			// are the others) and the last to get the escape — without it a
			// Cargo.toml with one unclosed table lost every dependency after
			// it from the inventory, from libyear and from the OSV scan,
			// and pendingValue grew to hold the rest of the file (v0.29.57).
			if tomlSectionHeader(line) {
				add(pendingKey, pendingValue)
				depth = 0
				section = line
				continue
			}
			pendingValue += " " + line
			depth += bracketDelta(line)
			if depth <= 0 {
				add(pendingKey, pendingValue)
				depth = 0
			}
			continue
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			section = line
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if d := bracketDelta(value); d > 0 {
			pendingKey, pendingValue, depth = key, value, d
			continue
		}
		add(key, value)
	}
	return out
}

// applyTOMLDepTable reads the keys of an inline dependency table.
func applyTOMLDepTable(e *tomlDepEntry, table string) {
	body := strings.TrimSpace(table)
	body = strings.TrimSuffix(strings.TrimPrefix(body, "{"), "}")
	for _, kv := range splitTOMLTopLevel(body) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "version":
			e.Version = tomlUnquote(v)
		case "git":
			e.Git = true
		case "path":
			e.Path = true
		case "registry", "registry-index":
			e.Registry = true
		}
	}
}

// splitTOMLDottedKey splits "serde.version" into ("serde", "version"). A
// quoted key is ONE name however many dots it holds — but it can still carry
// a dotted subkey after the closing quote.
//
// The closing quote is found first (v0.29.57). Treating the whole quoted key
// as the name meant `"zope.interface".version = "6"` became a dependency
// literally named `zope.interface".version`, and the real package's version
// was never applied. Quoted names with dots are ordinary — zope.interface and
// ruamel.yaml on PyPI — so this is not an exotic shape.
func splitTOMLDottedKey(key string) (name, sub string) {
	key = strings.TrimSpace(key)
	if q := key[:min(1, len(key))]; q == `"` || q == "'" {
		end := strings.Index(key[1:], q)
		if end < 0 {
			// Unterminated: the remainder is the best name available, and
			// it must not keep the quote character.
			return strings.TrimSpace(key[1:]), ""
		}
		name = key[1 : 1+end]
		rest := strings.TrimSpace(key[1+end+1:])
		return name, strings.TrimSpace(strings.TrimPrefix(rest, "."))
	}
	name, sub, _ = strings.Cut(key, ".")
	return strings.TrimSpace(name), strings.TrimSpace(sub)
}

// scanOutsideStrings walks line and calls fn for every byte that is NOT
// inside a quoted string, plus the opening quote of each string (so a caller
// can note where a quoted run began). fn returns false to stop the walk.
//
// It is the one place the LINE scanners here decide what "inside a string"
// means (SR-17). They used to decide it separately and one of them was right:
// `stripRubyComment` honoured a backslash escape, every other copy —
// `stripTOMLComment`, `tomlBracketDelta`, `splitTOMLTopLevel` — was
// escape-blind, so a balanced line like `note = "a \" { b"` counted an
// unclosed brace and the depth tracking then swallowed the rest of the file
// (v0.29.57). Unifying them adopted the Ruby scanner's escape rule, narrowed
// to double-quoted strings — so the Gemfile readers lost escape handling
// inside single quotes, which is the dominant Gemfile style. That narrowing
// is deliberate (a TOML literal string has no escapes) and its cost is
// stated at stripHashComment.
//
// `stripJSONC` (lockfile_parser.go) is NOT built on this and is not meant to
// be: it walks a whole FILE rather than a line, and JSONC's block comments
// carry state across lines that a line-at-a-time walker has no way to hold.
//
// A backslash escapes the next byte inside a DOUBLE-quoted string, which is
// true of TOML basic strings, Python and Ruby; inside a single-quoted string
// it does not, which is true of TOML literal strings ('C:\dir\' is that
// text) and is the rarer error in the other two.
func scanOutsideStrings(line string, fn func(i int, c byte) bool) {
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
			if !fn(i, c) {
				return
			}
		default:
			if !fn(i, c) {
				return
			}
		}
	}
}

// splitTOMLTopLevel splits on commas outside strings and brackets.
func splitTOMLTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	scanOutsideStrings(s, func(i int, c byte) bool {
		switch c {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
		return true
	})
	return append(parts, s[start:])
}

// bracketDelta counts opening minus closing brackets outside strings.
func bracketDelta(s string) int {
	delta := 0
	scanOutsideStrings(s, func(_ int, c byte) bool {
		switch c {
		case '[', '{':
			delta++
		case ']', '}':
			delta--
		}
		return true
	})
	return delta
}

// stripHashComment removes a '#' comment that is outside a string. It serves
// every manifest grammar that comments with '#': TOML, Python and Ruby (the
// Gemfile readers called their own copy of this until v0.29.57 — SR-17).
//
// A backslash escapes the next byte inside a DOUBLE-quoted string, which is
// true in all three; inside a single-quoted string it does not, which is true
// of TOML literal strings ('C:\dir\' is that text) and is the rarer error in
// Python and Ruby, where \' is legal but a manifest carrying one is not a
// shape that has ever been seen here.
func stripHashComment(line string) string { return stripLineComment(line, '#', 0) }

// stripSlashComment removes a `//` comment that is outside a string, for the
// grammars that comment that way (build.sbt, Package.swift). Every Swift
// package URL contains `//` inside its quotes, so the quote rule is not
// optional here.
func stripSlashComment(line string) string { return stripLineComment(line, '/', '/') }

// stripLineComment returns the line up to a comment marker found outside any
// string: one byte (`#`) when second is 0, two (`//`) otherwise.
func stripLineComment(line string, first, second byte) string {
	cut := -1
	scanOutsideStrings(line, func(i int, c byte) bool {
		if c != first {
			return true
		}
		if second != 0 && (i+1 >= len(line) || line[i+1] != second) {
			return true
		}
		cut = i
		return false
	})
	if cut < 0 {
		return line
	}
	return line[:cut]
}

func tomlUnquote(v string) string {
	return strings.Trim(strings.TrimSpace(v), `"'`)
}

// tomlDepKeyName is the package a dependency key names, and the attribute of
// it the key sets. A QUOTED key is the name without its quotes, and its
// dotted suffix is the attribute: `"zope.interface"` is (zope.interface, "")
// and `"zope.interface".version` is (zope.interface, "version"). An unquoted
// key stays WHOLE with no sub-key on purpose — `ruamel.yaml = "^0.18"` is how
// these packages are written in the wild, and splitting it there would drop
// the dependency.
//
// Only `version` holds a version; a caller that stores the value of any other
// sub-key as one invents a release for a real package (`optional = true`
// became zope.interface@true, SR-6). This is the rule scanTOMLDepTables
// applies through its own `sub` switch.
//
// It is the one spelling of the KEY rule for the readers of the
// `name = constraint` grammar (v0.29.57, Copilot on PR #210; SR-17):
// parsePoetryVersions, parsePipfileVersions, parsePipfileDeps and the
// dev/build reader — grep for the callers rather than trusting this list. The
// dependency-NAME inventory reads the same sections through
// scanTOMLDepTables, which splits an unquoted dotted key (Cargo's
// `serde.version`) and so disagrees with this rule on `ruamel.yaml` — that
// divergence predates this helper and is a worklist item, because settling it
// changes stored dependency names.
func tomlDepKeyName(key string) (name, sub string) {
	key = strings.TrimSpace(key)
	if strings.HasPrefix(key, `"`) || strings.HasPrefix(key, "'") {
		return splitTOMLDottedKey(key)
	}
	return key, ""
}

// tomlDepKeyVersionable reports whether a key's value is the dependency's
// version: a bare name, or its `version` sub-key. Every other sub-key
// (`optional`, `markers`, `extras`, `source`) sets something else.
func tomlDepKeyVersionable(sub string) bool { return sub == "" || sub == "version" }

// stripHashComment, stripSlashComment, listBrackets, listInner and
// bracketDelta — some above this block, some below — are
// LINE-level scanners rather than TOML ones: "a `#` outside a string starts a
// comment" and "a bracket inside a string is data" hold identically for a
// Python requirement list, so setup.py's readers share these rather than
// spelling the rule a second time (SR-17). Everything else in this file is
// TOML grammar and keeps its name.
//
// listInner returns a line's array contents: everything after the first
// opening bracket and before the last closing one, counting only brackets
// OUTSIDE strings. `dependencies = ["celery[redis]>=5.0",` yields
// `"celery[redis]>=5.0",` — the extra's brackets belong to the value, and
// taking them as the array's bounds cut the item in half.
//
// A line with no bracket is all contents (an item line inside a multiline
// array); a closing bracket that precedes the opening one yields nothing,
// the malformed shape FuzzManifestParsers found in v0.27.99.
func listInner(line string) string {
	open, close := -1, -1
	scanOutsideStrings(line, func(i int, c byte) bool {
		switch c {
		case '[':
			if open < 0 {
				open = i
			}
		case ']':
			close = i
		}
		return true
	})
	if open < 0 {
		open = 0
	} else {
		open++
	}
	if close < 0 {
		close = len(line)
	}
	if open > close {
		return ""
	}
	return line[open:close]
}

// asciiLower lowercases A–Z and nothing else, so the result has the SAME
// BYTES AS THE INPUT at every position and an index found in it is an index
// into the original. strings.ToLower does not promise that: an invalid UTF-8
// byte becomes a 3-byte replacement rune and the Kelvin sign becomes one
// byte, so `line[strings.Index(strings.ToLower(line), …)]` can slice out of
// range and panic — which it did, killing the analysis phase of any
// repository carrying such a file (v0.29.57).
//
// Every grammar that needs this is matching an ASCII keyword or attribute
// name, so folding only A–Z loses nothing.
func asciiLower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// stripXMLComments removes every `<!-- … -->` span from an XML document.
// XML is the one manifest grammar here whose comment is a BLOCK: the markers
// need not share a line, so this runs over the whole document before any
// reader splits it — a line scanner cannot see that it is inside one.
//
// An unterminated `<!--` takes the rest of the document with it, which is
// what a parser would do with the malformed file anyway. XML comments cannot
// nest and cannot contain `--`, so there is no quoting rule to honour: this
// is the one grammar where scanOutsideStrings does not apply.
func stripXMLComments(content string) string {
	var b strings.Builder
	for {
		i := strings.Index(content, "<!--")
		if i < 0 {
			b.WriteString(content)
			return b.String()
		}
		b.WriteString(content[:i])
		rest := content[i+4:]
		j := strings.Index(rest, "-->")
		if j < 0 {
			return b.String()
		}
		content = rest[j+3:]
	}
}

// tomlHeaderIs reports whether a line is the table header for name. TOML
// allows a comment AFTER a table header (`[packages]  # runtime`), and every
// reader used to compare the raw line, so such a header matched nothing and
// the whole section was skipped — the name inventory and the libyear reader
// then disagreed about one file, because the inventory's scanner already
// stripped the comment (v0.29.57).
func tomlHeaderIs(line, name string) bool {
	return strings.TrimSpace(stripHashComment(line)) == "["+name+"]"
}

// tomlSectionHeader reports whether a line is a table header — `[packages]`,
// `[tool.poetry.group.dev.dependencies]`. The test is deliberately tight,
// because it is used to decide that an inline table which never closed has
// ended: the inner text must look like a table name and nothing else, so a
// wrapped ARRAY VALUE that merely starts and ends with a bracket
// (`["d"]`, `[1, 2]`) is not one.
func tomlSectionHeader(line string) bool {
	line = strings.TrimSpace(stripHashComment(line))
	if len(line) < 3 || line[0] != '[' || line[len(line)-1] != ']' {
		return false
	}
	inner := line[1 : len(line)-1]
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-':
		default:
			return false
		}
	}
	return len(inner) > 0
}

// listBrackets reports whether the line carries an opening and/or a
// closing bracket OUTSIDE any string. `"pytest[all]>=7.0"]` CLOSES an array
// and does not open one: the brackets around an extra belong to the value.
// Reading them as array syntax leaves the walk inside an array that ended,
// and every later group inherits that array's scope (v0.29.57).
func listBrackets(line string) (opens, closes bool) {
	scanOutsideStrings(line, func(_ int, c byte) bool {
		switch c {
		case '[':
			opens = true
		case ']':
			closes = true
		}
		return true
	})
	return opens, closes
}
