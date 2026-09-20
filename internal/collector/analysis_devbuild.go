// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Dev/build dependency expansion (v0.27.45, summary/19 P2 — the
// numpy fix). Python's dev/test/build tooling lives in manifest
// families the walk never visited: requirements-variant files
// (requirements-dev.txt, test_requirements.txt, or another .txt directly
// inside a requirements/ directory — never requirements.txt itself),
// pyproject [build-system].requires / [project.optional-dependencies]
// / PEP 735 [dependency-groups] / poetry groups, Pipfile
// [dev-packages], setup.py tests_require/extras_require, setup.cfg
// [options.extras_require]. Everything here is gated on
// collection.dev_build_deps (default FALSE — the findings-volume
// driver; canary on the small chaoss.tv DB before any default flip,
// per the v0.27.19 first-wave lesson). Knob off = the walk's
// pre-v0.27.45 row set, byte-identical.
//
// The Go C1 classifier (classifyGoModTestOnlyDeps) is NOT gated: it
// only relabels already-collected go.mod deps (test-only modules →
// test scope), adding no rows and no findings volume — the same
// posture as the P1 relabels.

package collector

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// requirementsFileScope classifies a requirements-variant filename
// into a dependency scope. ok=false means the file is not a
// requirements variant (or is the exact "requirements.txt", which the
// walk's dedicated case already handles as runtime). Classification
// is by the ecosystem's own filename convention (open decision #6,
// operator-accepted): test → test; dev/lint/doc → dev;
// build/ci/release → build; bare/prod/base → runtime.
func requirementsFileScope(base, path string) (string, bool) {
	lower := strings.ToLower(base)
	if !strings.HasSuffix(lower, ".txt") {
		return "", false
	}
	if lower == "requirements.txt" {
		return "", false // the walk's exact-name case owns this
	}
	stem := strings.TrimSuffix(lower, ".txt")
	inRequirementsDir := filepath.Base(filepath.Dir(path)) == "requirements"
	isVariant := strings.HasPrefix(stem, "requirements") ||
		strings.HasSuffix(stem, "-requirements") ||
		strings.HasSuffix(stem, "_requirements")
	if !isVariant && !inRequirementsDir {
		return "", false
	}
	// Token classification on the stem with the requirements part
	// stripped: "requirements-dev" → "dev", "test_requirements" →
	// "test", requirements/ci.txt → "ci".
	token := strings.Trim(strings.ReplaceAll(strings.ReplaceAll(stem, "requirements", ""), "_", "-"), "-")
	switch {
	case strings.Contains(token, "test"):
		return model.ScopeTest, true
	case strings.Contains(token, "dev"), strings.Contains(token, "lint"), strings.Contains(token, "doc"):
		return model.ScopeDev, true
	case strings.Contains(token, "build"), strings.Contains(token, "ci"), strings.Contains(token, "release"):
		return model.ScopeBuild, true
	default:
		// bare requirements/foo.txt, requirements-prod.txt, base…
		return "runtime", true
	}
}

// parseRequirementsTxtVersionsScoped parses a requirements-variant
// file and stamps every dep with the filename-derived scope.
func parseRequirementsTxtVersionsScoped(path, scope string) []libyearDep {
	deps := parseRequirementsTxtVersions(path)
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
// Returned deps are ADDITIVE to parsePyprojectVersionsFromContent's
// runtime set; the caller appends both.
func parsePyprojectDevBuildVersions(content string) []libyearDep {
	var deps []libyearDep
	// arrayScope: non-empty while inside a section whose values are
	// PEP 508 requirement arrays. kvScope: non-empty while inside a
	// poetry-style key = "version" section.
	arrayScope := ""
	kvScope := ""
	inArray := false
	// arrayItemScope: the scope the open array's key chose, kept for the
	// item lines inside it (they carry no key of their own).
	arrayItemScope := ""
	// tableDepth: >0 while a multi-line inline table is open. Its
	// continuation lines are the TABLE's keys — see parsePoetryVersions.
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
		if header := strings.TrimSpace(stripHashComment(trimmed)); strings.HasPrefix(header, "[") && strings.HasSuffix(header, "]") && !strings.Contains(header, "=") {
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
			// A trailing comment is not part of the value: without this the
			// version kept the closing quote (`25.0"`) and reached the purl
			// (Copilot round on PR #210). Same stripper as the TOML reader.
			code := strings.TrimSpace(stripHashComment(trimmed))
			// An unclosed inline table opens here, before any of the
			// continues below: a declaration this reader skips still has to
			// close, or the rest of the section is read as its keys.
			if d := bracketDelta(code); d > 0 {
				tableDepth = d
			}
			parts := strings.SplitN(code, "=", 2)
			if len(parts) != 2 {
				continue
			}
			name, sub := tomlDepKeyName(parts[0])
			raw := strings.TrimSpace(parts[1])
			if name == "" || name == "python" || !tomlDepKeyVersionable(sub) {
				continue
			}
			version := ""
			if strings.HasPrefix(raw, "{") {
				// Poetry's group tables build deps the same way its runtime
				// table does, so they take the same rule through the same
				// helpers (SR-17): a path/git/url source is not the PyPI
				// package of that name, and the version lives under the
				// version key, not in the table body.
				if pythonTableIsNonRegistry(raw) {
					continue
				}
				version = cleanVersion(pythonTableVersion(raw))
			} else {
				version = cleanVersion(strings.Trim(raw, "\"'^~>="))
			}
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: kvScope, Manager: "pypi"})
		}
	}
	return deps
}

// parsePipfileDevPackages extracts the [dev-packages] section the
// base Pipfile parser deliberately skips. Same value grammar as
// [packages]; every dep is dev-scoped.
func parsePipfileDevPackages(content string) []libyearDep {
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
func parseSetupPyDevBuildVersions(content string) []libyearDep {
	var deps []libyearDep
	collect := func(marker, scope string) {
		in := false
		depth := 0
		for _, line := range strings.Split(content, "\n") {
			// A commented-out requirement is not one, and brackets inside a
			// quoted requirement are not nesting (v0.29.57 — the same two
			// rules the install_requires readers apply).
			trimmed := stripHashComment(strings.TrimSpace(line))
			if !in && strings.Contains(trimmed, marker) && strings.ContainsAny(trimmed, "[{") {
				in = true
				if idx := strings.Index(trimmed, marker); idx >= 0 {
					trimmed = trimmed[idx+len(marker):]
				}
			}
			if !in {
				continue
			}
			depth += bracketDelta(trimmed)
			// Dict entries: drop the quoted KEYS so extras names don't parse
			// as deps. Cutting at the first ':' only handled the multi-line
			// form, one key per line; on a single-line dict
			// (`{'test': [...], 'dev': [...]}`) every later key was left in
			// and became a package of its own (v0.29.57).
			payload := trimmed
			if scope == model.ScopeOptional {
				payload = dropDictKeys(payload)
			}
			for _, d := range extractQuotedPyVersionDeps(payload) {
				d.Type = scope
				deps = append(deps, d)
			}
			if depth <= 0 {
				in = false
				depth = 0
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
func parseSetupCfgExtrasVersions(content string) []libyearDep {
	var deps []libyearDep
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
		if d := parsePyRequirement(payload); d != nil {
			d.Type = model.ScopeOptional
			deps = append(deps, *d)
		}
	}
	return deps
}

// classifyGoModTestOnlyDeps is the Go C1 classifier (operator
// decision #7: "since we are Go", the go.mod honesty gap gets a real
// fix, not a doc note). go.mod cannot distinguish test-only deps
// structurally, but the repo's own source can: a module whose
// packages are imported ONLY from _test.go files is test-scope.
// Static import scan via go/parser ImportsOnly — no toolchain, no
// subprocess, no module downloads. Modules never imported at all
// (indirect deps, tool deps) honestly stay runtime: absence of
// evidence is not test evidence, and unknown scopes present as
// runtime everywhere (IsRuntimeScope).
//
// NOT gated on dev_build_deps: this relabels existing rows without
// adding any (the P1 posture).
func classifyGoModTestOnlyDeps(workDir string, deps []libyearDep) []libyearDep {
	hasGoDeps := false
	for _, d := range deps {
		if d.Manager == "go" {
			hasGoDeps = true
			break
		}
	}
	if !hasGoDeps {
		return deps
	}

	type usage struct{ nonTest, test bool }
	imports := map[string]*usage{}
	fset := token.NewFileSet()
	const maxGoFiles = 50000 // pathology bound; beyond it, skip silently (deps keep runtime)
	seen := 0
	_ = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == "node_modules" || base == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		seen++
		if seen > maxGoFiles {
			return filepath.SkipAll
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil || f == nil {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			u := imports[p]
			if u == nil {
				u = &usage{}
				imports[p] = u
			}
			if isTest {
				u.test = true
			} else {
				u.nonTest = true
			}
		}
		return nil
	})

	for i := range deps {
		if deps[i].Manager != "go" || !model.IsRuntimeScope(deps[i].Type) {
			continue
		}
		mod := deps[i].Name
		anyTest, anyNonTest := false, false
		for p, u := range imports {
			if p == mod || strings.HasPrefix(p, mod+"/") {
				anyTest = anyTest || u.test
				anyNonTest = anyNonTest || u.nonTest
			}
		}
		if anyTest && !anyNonTest {
			deps[i].Type = model.ScopeTest
		}
	}
	return deps
}
