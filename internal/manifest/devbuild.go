// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// parseRequirementsTxtVersionsScoped parses a requirements-variant
// file and stamps every dep with the filename-derived scope.
func parseRequirementsTxtVersionsScoped(path, scope string) []model.LibyearDep {
	deps := ParseRequirementsTxtVersions(path)
	for i := range deps {
		deps[i].Type = scope
	}
	return deps
}

// parsePyprojectDevBuildVersions extracts the NON-runtime pyproject
// sections the base parser deliberately skips:
//
//   - [build-system] requires = [...]            → build
//   - [project.optional-dependencies] k = [...]  → optional
//   - [dependency-groups] k = [...] (PEP 735)    → test when the
//     group name mentions test, else dev
//   - [tool.poetry.group.<g>.dependencies]       → test when g
//     mentions test, else dev
//   - [tool.poetry.dev-dependencies] (legacy)    → dev
//
// Returned deps are ADDITIVE to ParsePyprojectVersionsFromContent's
// runtime set; the caller appends both.
func parsePyprojectDevBuildVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	// Poetry's group tables (`[tool.poetry.group.<g>.dependencies]`,
	// `[tool.poetry.dev-dependencies]`) go through the shared table reader
	// (pythonTOMLDeps; worklist items 42 and 43 — the line reader below
	// lost a multi-line table's version); the PEP 508 arrays stay with the
	// line reader, since an array is not a table.
	deps = append(deps, pythonTOMLDeps(content, poetryGroupSections(content))...)
	// arrayScope: non-empty while inside a section whose values are
	// PEP 508 requirement arrays. kvScope: non-empty while inside a
	// poetry-style key = "version" section — read above, skipped here.
	arrayScope := ""
	kvScope := ""
	inArray := false
	// arrayItemScope: the scope the open array's key chose, kept for the
	// item lines inside it (they carry no key of their own).
	arrayItemScope := ""
	// tableDepth: >0 while a multi-line inline table is open. Its
	// continuation lines are the TABLE's keys, read by pythonTOMLDeps over
	// scanTOMLDepTables; this branch only skips them.
	tableDepth := 0
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if tableDepth > 0 {
			// A table that never closed must not swallow the rest of the
			// file: a section header ends it, since no inline table can
			// span one (v0.29.57 — before the depth tracking, such a file
			// recovered here, and it still does).
			if tomlSectionHeader(trimmed) {
				tableDepth = 0
			} else {
				tableDepth += bracketDelta(stripHashComment(trimmed))
				if tableDepth < 0 {
					tableDepth = 0
				}
				continue
			}
		}
		// A table header may carry a trailing comment (v0.29.57).
		if header, ok := pyprojectHeader(trimmed); ok {
			section := strings.Trim(header, "[]")
			arrayScope, kvScope, inArray, arrayItemScope = "", "", false, ""
			switch {
			case section == "build-system":
				arrayScope = model.ScopeBuild
			case section == "project.optional-dependencies":
				arrayScope = model.ScopeOptional
			case section == "dependency-groups":
				arrayScope = model.ScopeDev
			case strings.HasPrefix(section, "tool.poetry.group.") && strings.HasSuffix(section, ".dependencies"):
				group := strings.TrimSuffix(strings.TrimPrefix(section, "tool.poetry.group."), ".dependencies")
				if strings.Contains(group, "test") {
					kvScope = model.ScopeTest
				} else {
					kvScope = model.ScopeDev
				}
			case section == "tool.poetry.dev-dependencies":
				kvScope = model.ScopeDev
			}
			continue
		}
		switch {
		case arrayScope != "":
			// Inside a multiline array the item lines carry no key, so the
			// scope chosen when the array opened is the one they keep
			// (Copilot on PR #210: a multiline PEP 735 `test` group was
			// emitting dev).
			//
			// DECLINED, with the reason here rather than a fix: on MALFORMED
			// input — an array that never closes — this now stamps later
			// groups with the unclosed group's scope, where the older code
			// used the section default. Both are wrong for a file no TOML
			// parser accepts, and the shape that is worth getting right is
			// the well-formed one. A section header does not rescue it the
			// way it rescues an inline table, because an array item is a
			// bare string and `[x]` inside one is indistinguishable from a
			// header without the charset rule tomlSectionHeader applies.
			scope := arrayScope
			if inArray && arrayItemScope != "" {
				scope = arrayItemScope
			}
			// [build-system] has non-array keys too (build-backend);
			// only the requires key opens an array there.
			if arrayScope == model.ScopeBuild && !inArray && !strings.HasPrefix(trimmed, "requires") {
				continue
			}
			// PEP 735 test-named groups are test deps.
			if arrayScope == model.ScopeDev && !inArray {
				if key, _, found := strings.Cut(trimmed, "="); found && strings.Contains(strings.ToLower(key), "test") {
					scope = model.ScopeTest
				}
			}
			// Brackets inside a string or a comment are not array syntax:
			// `"pytest[all]>=7.0"]` closes the array without opening one.
			// The ITEMS come off the same stripped line, or a commented-out
			// requirement inside the array is collected as a dependency.
			code := stripHashComment(trimmed)
			opensArray, closesArray := listBrackets(code)
			if !inArray {
				arrayItemScope = scope
			}
			if inArray || opensArray {
				for _, d := range extractQuotedPyVersionDeps(code) {
					d.Type = scope
					deps = append(deps, d)
				}
			}
			if opensArray && !closesArray {
				inArray = true
			}
			if inArray && closesArray && !opensArray {
				inArray = false
			}
		case kvScope != "" && strings.Contains(trimmed, "=") && !strings.HasPrefix(trimmed, "#"):
			// Read by pythonTOMLDeps above; only the inline-table depth is
			// tracked here so a table's continuation lines are not mistaken
			// for array items or headers.
			if d := bracketDelta(strings.TrimSpace(stripHashComment(trimmed))); d > 0 {
				tableDepth = d
			}
		}
	}
	return deps
}

// poetryGroupSections maps every Poetry group table header in content to
// the scope its dependencies take: a group named like a test group is test,
// every other group and the legacy dev-dependencies table is dev.
//
// The headers are found with the line reader's own test (pyprojectHeader),
// not the bare-key tomlSectionHeader: a group whose name needs quoting
// (`[tool.poetry.group."docs-x".dependencies]`) failed the bare-key test
// and its dependencies were dropped (PR #218 review A1).
func poetryGroupSections(content string) map[string]string {
	out := map[string]string{}
	for _, raw := range strings.Split(content, "\n") {
		header, ok := pyprojectHeader(raw)
		if !ok {
			continue
		}
		section := strings.Trim(header, "[]")
		switch {
		case strings.HasPrefix(section, "tool.poetry.group.") && strings.HasSuffix(section, ".dependencies"):
			group := strings.TrimSuffix(strings.TrimPrefix(section, "tool.poetry.group."), ".dependencies")
			if strings.Contains(group, "test") {
				out[header] = model.ScopeTest
			} else {
				out[header] = model.ScopeDev
			}
		case section == "tool.poetry.dev-dependencies":
			out[header] = model.ScopeDev
		}
	}
	return out
}

// pyprojectHeader reports whether a pyproject.toml line is a table header —
// a `[`...`]` line, trailing comment removed, carrying no `=` — and returns
// the header as scanTOMLDepTables keys its sections. The one header test for
// both readers of this file (the section list above and the line reader in
// parsePyprojectDevBuildVersions), so they cannot disagree about a quoted
// group name (PR #218 review A1).
func pyprojectHeader(line string) (string, bool) {
	header := strings.TrimSpace(stripHashComment(line))
	if strings.HasPrefix(header, "[") && strings.HasSuffix(header, "]") && !strings.Contains(header, "=") {
		return header, true
	}
	return "", false
}

// parsePipfileDevPackages extracts the [dev-packages] section the
// base Pipfile parser deliberately skips. Same value grammar as
// [packages]; every dep is dev-scoped.
func parsePipfileDevPackages(content string) []model.LibyearDep {
	// Reuse the [packages] parser by SWAPPING the two headers and handing
	// it the whole file. Isolating the section by hand meant a second
	// section splitter, and it had the ordering this release fixed
	// everywhere else — a continuation line beginning with `[` ended the
	// section, dropping the rest of [dev-packages] before the delegate's
	// own table tracking could see it (v0.29.57). Delegating the whole file
	// leaves exactly one reader of this grammar.
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		switch {
		case tomlHeaderIs(line, "packages"):
			// Any header the delegate does not read. It must not be a
			// plausible dev section either.
			lines[i] = "[packages-runtime-not-read-here]"
		case tomlHeaderIs(line, "dev-packages"):
			lines[i] = "[packages]"
		}
	}
	deps := parsePipfileVersions(strings.Join(lines, "\n"))
	for i := range deps {
		deps[i].Type = model.ScopeDev
	}
	return deps
}

// parseSetupPyDevBuildVersions extracts setup.py's tests_require
// (→ test) and extras_require (→ optional) argument lists, which the
// base parser (install_requires only) skips. extras_require is a
// dict — the quoted KEYS must not be mistaken for dep names, so
// lines containing a ':' have everything through the key stripped
// before quoted-string extraction.
func parseSetupPyDevBuildVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	collect := func(keyword, scope string) {
		// The keyword's own value and nothing after it (setupKeywordValue):
		// on a one-line setup() call tests_require used to take
		// extras_require's strings as well.
		for _, chunk := range setupKeywordValue(content, keyword) {
			// Dict entries: drop the quoted KEYS so an extra's name does not
			// parse as a dependency (dropDictKeys handles a dict on one
			// line and one key per line alike, v0.29.57).
			if scope == model.ScopeOptional {
				chunk = dropDictKeys(chunk)
			}
			for _, d := range extractQuotedPyVersionDeps(chunk) {
				d.Type = scope
				deps = append(deps, d)
			}
		}
	}
	collect("tests_require", model.ScopeTest)
	collect("extras_require", model.ScopeOptional)
	return deps
}

// dropDictKeys blanks the quoted KEYS of a Python dict literal — the runs
// that sit between a `{` or `,` and the `:` that follows — leaving the values
// for the requirement extractor. A `:` inside a quoted requirement (a URL, a
// PEP 508 marker) is not a key separator, so the scan is quote-aware.
func dropDictKeys(line string) string {
	out := []byte(line)
	keyStart := -1 // where the current candidate key began, -1 when none
	scanOutsideStrings(line, func(i int, c byte) bool {
		switch c {
		case '"', '\'':
			if keyStart < 0 {
				keyStart = i
			}
		case '{', ',':
			keyStart = -1
		case ':':
			if keyStart >= 0 {
				for j := keyStart; j < i; j++ {
					out[j] = ' '
				}
			}
			keyStart = -1
		case '[':
			// Inside a value; anything quoted from here is a requirement.
			keyStart = -1
		}
		return true
	})
	return string(out)
}

// parseSetupCfgExtrasVersions extracts the [options.extras_require]
// ini section (→ optional): each key is an extra name whose value is
// an inline requirement or an indented multi-line list.
func parseSetupCfgExtrasVersions(content string) []model.LibyearDep {
	var deps []model.LibyearDep
	in := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			in = trimmed == "[options.extras_require]"
			continue
		}
		if !in || trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		payload := trimmed
		// "test =" key lines: inline value after '=', or nothing
		// (deps follow indented). Indented continuation lines are the
		// requirement themselves.
		if strings.Contains(trimmed, "=") && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			parts := strings.SplitN(trimmed, "=", 2)
			payload = strings.TrimSpace(parts[1])
			if payload == "" {
				continue
			}
		}
		if d := ParsePyRequirement(payload); d != nil {
			d.Type = model.ScopeOptional
			deps = append(deps, *d)
		}
	}
	return deps
}
