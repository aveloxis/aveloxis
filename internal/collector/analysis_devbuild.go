// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Dev/build dependency expansion (v0.27.45, summary/19 P2 — the
// numpy fix). Python's dev/test/build tooling lives in manifest
// families the walk never visited: requirements-variant files
// (requirements-dev.txt, test_requirements.txt, requirements/*.txt),
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
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") && !strings.Contains(trimmed, "=") {
			section := strings.Trim(trimmed, "[]")
			arrayScope, kvScope, inArray = "", "", false
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
			scope := arrayScope
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
			opensArray := strings.Contains(trimmed, "[")
			closesArray := strings.Contains(trimmed, "]")
			if inArray || opensArray {
				for _, d := range extractQuotedPyVersionDeps(trimmed) {
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
			parts := strings.SplitN(trimmed, "=", 2)
			name := strings.TrimSpace(parts[0])
			version := cleanVersion(strings.Trim(strings.TrimSpace(parts[1]), "\"'^~>= {}"))
			// Poetry table values ({version = "...", optional = true})
			// carry the version inside; fall back to the raw trim.
			if strings.Contains(parts[1], "version") {
				for _, kv := range strings.Split(strings.Trim(strings.TrimSpace(parts[1]), "{}"), ",") {
					kv = strings.TrimSpace(kv)
					if strings.HasPrefix(kv, "version") {
						if _, v, found := strings.Cut(kv, "="); found {
							version = cleanVersion(strings.Trim(strings.TrimSpace(v), "\"'^~>="))
						}
						break
					}
				}
			}
			if name != "" && name != "python" {
				deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: kvScope, Manager: "pypi"})
			}
		}
	}
	return deps
}

// parsePipfileDevPackages extracts the [dev-packages] section the
// base Pipfile parser deliberately skips. Same value grammar as
// [packages]; every dep is dev-scoped.
func parsePipfileDevPackages(content string) []libyearDep {
	// Reuse the [packages] parser by isolating the [dev-packages]
	// section body and renaming its header — the value grammar is
	// identical and this keeps ONE copy of the hairy table-value
	// parsing.
	var section []string
	in := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[dev-packages]" {
			in = true
			section = append(section, "[packages]")
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			in = false
			continue
		}
		if in {
			section = append(section, line)
		}
	}
	if len(section) == 0 {
		return nil
	}
	deps := parsePipfileVersions(strings.Join(section, "\n"))
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
			trimmed := strings.TrimSpace(line)
			if !in && strings.Contains(trimmed, marker) && strings.ContainsAny(trimmed, "[{") {
				in = true
				if idx := strings.Index(trimmed, marker); idx >= 0 {
					trimmed = trimmed[idx+len(marker):]
				}
			}
			if !in {
				continue
			}
			depth += strings.Count(trimmed, "[") + strings.Count(trimmed, "{")
			depth -= strings.Count(trimmed, "]") + strings.Count(trimmed, "}")
			// Dict entries: strip the quoted key (everything through
			// the first ':') so extras keys don't parse as deps.
			payload := trimmed
			if colon := strings.Index(payload, ":"); colon >= 0 && scope == model.ScopeOptional {
				payload = payload[colon+1:]
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
