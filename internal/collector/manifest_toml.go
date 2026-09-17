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
		line := strings.TrimSpace(stripTOMLComment(raw))
		if depth > 0 {
			pendingValue += " " + line
			depth += tomlBracketDelta(line)
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
		if d := tomlBracketDelta(value); d > 0 {
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
// quoted key ("a.b" = …) is one name.
func splitTOMLDottedKey(key string) (name, sub string) {
	key = strings.TrimSpace(key)
	if strings.HasPrefix(key, `"`) || strings.HasPrefix(key, "'") {
		return tomlUnquote(key), ""
	}
	name, sub, _ = strings.Cut(key, ".")
	return strings.TrimSpace(name), strings.TrimSpace(sub)
}

// splitTOMLTopLevel splits on commas outside strings and brackets.
func splitTOMLTopLevel(s string) []string {
	var parts []string
	depth := 0
	var quote byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// tomlBracketDelta counts opening minus closing brackets outside strings.
func tomlBracketDelta(s string) int {
	delta := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '[' || c == '{':
			delta++
		case c == ']' || c == '}':
			delta--
		}
	}
	return delta
}

// stripTOMLComment removes a '#' comment that is outside a string.
func stripTOMLComment(line string) string {
	var quote byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == quote {
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

func tomlUnquote(v string) string {
	return strings.Trim(strings.TrimSpace(v), `"'`)
}
