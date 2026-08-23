// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — lockfile_parser_formats.go: the non-JavaScript
// lockfile parsers behind the v0.27.11 roster in lockfile_parser.go.
// All pure (bytes in, entries out), all best-effort.
package collector

import (
	"encoding/json"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// ============================================================
// Python + Rust — [[package]] TOML blocks
// ============================================================

// parsePoetryStyleTOML handles the machine-generated TOML lockfiles
// that share the [[package]] block shape: poetry.lock, uv.lock,
// pdm.lock, and Cargo.lock. Only `name = "..."` and `version = "..."`
// keys at block top level matter; direct deps are not distinguished
// by any of these formats.
//
// A hand-rolled line scanner (house style — the manifest parsers in
// analysis.go do the same) is reliable here because lockfiles are
// machine-generated with completely regular layout.
func parsePoetryStyleTOML(data []byte) (parsedLockfileData, error) {
	var out parsedLockfileData
	inPackage := false
	inDepsTable := false // poetry: [package.dependencies] sub-table
	inDepsArray := false // Cargo: dependencies = [ "name", ... ]
	name, version, category := "", "", ""
	flush := func() {
		if name != "" && version != "" {
			scope := ""
			if category == "dev" {
				scope = "dev"
			}
			out.Entries = append(out.Entries, LockfileEntry{Name: name, Version: version, Scope: scope})
		}
		name, version, category = "", "", ""
	}
	addEdge := func(child, constraint string) {
		if name != "" && child != "" {
			out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: version, ChildName: child, Constraint: constraint})
		}
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "[[package]]":
			flush()
			inPackage = true
			inDepsTable, inDepsArray = false, false
			continue
		case trimmed == "[package.dependencies]":
			// v0.27.133 C2: poetry's per-package dependency sub-table —
			// previously this branch fell into the generic "[" case,
			// terminating the block and discarding the edges. The parent
			// name/version are already parsed (they precede sub-tables
			// in poetry's output ordering).
			inDepsTable = true
			continue
		case strings.HasPrefix(trimmed, "["):
			// Any other section ([metadata], [package.metadata], …) ends
			// the scan for this package.
			flush()
			inPackage = false
			inDepsTable, inDepsArray = false, false
			continue
		}
		if !inPackage {
			continue
		}
		if inDepsTable {
			// `child = "constraint"` or `child = {version = "...", ...}`.
			if k, v, ok := strings.Cut(trimmed, "="); ok {
				child := strings.Trim(strings.TrimSpace(k), `"`)
				constraint := strings.Trim(strings.TrimSpace(v), `" `)
				if strings.HasPrefix(constraint, "{") {
					if inner, iok := tomlStringValue(strings.Trim(constraint, "{}"), "version"); iok {
						constraint = inner
					} else {
						constraint = ""
					}
				}
				addEdge(child, constraint)
			}
			continue
		}
		if inDepsArray {
			// Cargo: quoted array elements "name" or "name version".
			if trimmed == "]" {
				inDepsArray = false
				continue
			}
			el := strings.Trim(strings.TrimSuffix(trimmed, ","), `" `)
			if el != "" {
				child, constraint, _ := strings.Cut(el, " ")
				addEdge(child, constraint)
			}
			continue
		}
		if trimmed == "dependencies = [" {
			// v0.27.133 C2: Cargo.lock's per-package dependency array —
			// previously ignored (array elements fell through
			// tomlStringValue).
			inDepsArray = true
			continue
		}
		if v, ok := tomlStringValue(trimmed, "name"); ok {
			name = v
		}
		if v, ok := tomlStringValue(trimmed, "version"); ok {
			version = v
		}
		if v, ok := tomlStringValue(trimmed, "category"); ok {
			category = v
		}
	}
	flush()
	return out, nil
}

// tomlStringValue extracts `key = "value"` from a single TOML line.
func tomlStringValue(line, key string) (string, bool) {
	if !strings.HasPrefix(line, key) {
		return "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, key))
	if !strings.HasPrefix(rest, "=") {
		return "", false
	}
	rest = strings.TrimSpace(strings.TrimPrefix(rest, "="))
	if len(rest) < 2 || rest[0] != '"' {
		return "", false
	}
	if end := strings.IndexByte(rest[1:], '"'); end >= 0 {
		return rest[1 : 1+end], true
	}
	return "", false
}

// parsePipfileLock handles Pipfile.lock: JSON with "default" and
// "develop" sections mapping package → {"version": "==1.2.3"}. The
// sections hold the FULL resolved closure, so direct deps are not
// distinguished.
func parsePipfileLock(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Default map[string]struct {
			Version string `json:"version"`
		} `json:"default"`
		Develop map[string]struct {
			Version string `json:"version"`
		} `json:"develop"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var entries []LockfileEntry
	add := func(section map[string]struct {
		Version string `json:"version"`
	}) {
		for name, pkg := range section {
			version := strings.TrimPrefix(strings.TrimSpace(pkg.Version), "==")
			if version != "" {
				entries = append(entries, LockfileEntry{Name: name, Version: version})
			}
		}
	}
	add(doc.Default)
	add(doc.Develop)
	return parsedLockfileData{Entries: entries}, nil
}

// ============================================================
// Ruby
// ============================================================

// gemfileLockSpecRe matches a resolved gem line in the GEM/specs
// section: exactly 4-space indentation, "name (version)". 6-space
// lines are the gem's own constraint list, not resolutions.
var gemfileLockSpecRe = regexp.MustCompile(`^    ([A-Za-z0-9_.-]+) \(([^)]+)\)\s*$`)

// gemfileLockDepRe matches a DEPENDENCIES entry: 2-space indentation,
// name with optional "!" pin marker and optional constraint parens.
var gemfileLockDepRe = regexp.MustCompile(`^  ([A-Za-z0-9_.-]+)!?( \(.*\))?\s*$`)

// gemfileLockChildRe matches a gem's own constraint line under a spec:
// exactly 6-space indentation, "name (constraint)" or bare "name"
// (v0.27.133 C2 — these lines are the specs section's edges).
var gemfileLockChildRe = regexp.MustCompile(`^      ([A-Za-z0-9_.-]+)( \([^)]*\))?\s*$`)

// parseGemfileLock handles Bundler's Gemfile.lock. Resolved packages
// come from the GEM/specs section (4-space indent); the DEPENDENCIES
// section lists the direct gems, so direct IS distinguished.
func parseGemfileLock(data []byte) (parsedLockfileData, error) {
	var out parsedLockfileData
	direct := map[string]bool{}
	section := ""
	parentName, parentVersion := "", ""
	for _, raw := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" && !strings.HasPrefix(raw, " ") {
			section = trimmed // GEM / GIT / PATH / PLATFORMS / DEPENDENCIES / BUNDLED WITH
			parentName, parentVersion = "", ""
			continue
		}
		switch section {
		case "GEM", "GIT", "PATH":
			if m := gemfileLockSpecRe.FindStringSubmatch(raw); m != nil {
				out.Entries = append(out.Entries, LockfileEntry{Name: m[1], Version: m[2]})
				parentName, parentVersion = m[1], m[2]
				continue
			}
			// v0.27.133 C2: 6-space lines under a 4-space spec are the
			// gem's OWN dependency constraints — edges, previously
			// deliberately unmatched.
			if parentName != "" {
				if m := gemfileLockChildRe.FindStringSubmatch(raw); m != nil {
					out.Edges = append(out.Edges, LockfileEdge{ParentName: parentName, ParentVersion: parentVersion, ChildName: m[1], Constraint: strings.Trim(m[2], "() ")})
				}
			}
		case "DEPENDENCIES":
			if m := gemfileLockDepRe.FindStringSubmatch(raw); m != nil {
				direct[m[1]] = true
			}
		}
	}
	for i := range out.Entries {
		out.Entries[i].Direct = direct[out.Entries[i].Name]
	}
	out.DirectKnown = true
	return out, nil
}

// ============================================================
// PHP
// ============================================================

// parseComposerLock handles composer.lock: JSON arrays "packages" and
// "packages-dev" of {name, version}. The arrays hold the full resolved
// closure — direct deps are not distinguished. Leading "v" on versions
// (v7.8.1) is stripped to match manifest/purl conventions.
func parseComposerLock(data []byte) (parsedLockfileData, error) {
	type composerPkg struct {
		Name       string            `json:"name"`
		Version    string            `json:"version"`
		Require    map[string]string `json:"require"`
		RequireDev map[string]string `json:"require-dev"`
	}
	var doc struct {
		Packages    []composerPkg `json:"packages"`
		PackagesDev []composerPkg `json:"packages-dev"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var out parsedLockfileData
	for _, list := range [][]composerPkg{doc.Packages, doc.PackagesDev} {
		for _, p := range list {
			version := p.Version
			if len(version) > 1 && version[0] == 'v' && version[1] >= '0' && version[1] <= '9' {
				version = version[1:]
			}
			if p.Name == "" || version == "" {
				continue
			}
			out.Entries = append(out.Entries, LockfileEntry{Name: p.Name, Version: version})
			// v0.27.133 C2: each package's require map — platform
			// pseudo-packages (php, ext-*, lib-*) carry no "/" and are
			// not composer packages; skip them.
			for _, req := range []map[string]string{p.Require, p.RequireDev} {
				for child, constraint := range req {
					if !strings.Contains(child, "/") {
						continue
					}
					out.Edges = append(out.Edges, LockfileEdge{ParentName: p.Name, ParentVersion: version, ChildName: child, Constraint: constraint})
				}
			}
		}
	}
	return out, nil
}

// ============================================================
// Elixir
// ============================================================

// mixLockHexRe matches a :hex entry:
//
//	"phoenix": {:hex, :phoenix, "1.7.10", ...
//
// :git and :path entries carry no registry version and are skipped.
var mixLockHexRe = regexp.MustCompile(`"([^"]+)":\s*\{:hex,\s*:[A-Za-z0-9_]+,\s*"([^"]+)"`)

// mixLockDepTupleRe matches a dependency tuple INSIDE an entry's deps
// list: `{:castore, "~> 1.0", [...]}`. The entry's own `{:hex, :name`
// head cannot match — after `{:hex,` comes an atom, not a quoted
// requirement (v0.27.133 C2).
var mixLockDepTupleRe = regexp.MustCompile(`\{:([a-z0-9_]+),\s*"([^"]*)"`)

func parseMixLock(data []byte) (parsedLockfileData, error) {
	var out parsedLockfileData
	for _, line := range strings.Split(string(data), "\n") {
		m := mixLockHexRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		out.Entries = append(out.Entries, LockfileEntry{Name: m[1], Version: m[2]})
		for _, dm := range mixLockDepTupleRe.FindAllStringSubmatch(line, -1) {
			out.Edges = append(out.Edges, LockfileEdge{ParentName: m[1], ParentVersion: m[2], ChildName: dm[1], Constraint: dm[2]})
		}
	}
	return out, nil
}

// ============================================================
// Dart
// ============================================================

// parsePubspecLock handles pubspec.lock: YAML "packages" map whose
// entries carry dependency: "direct main" / "direct dev" /
// "direct overridden" / "transitive" — direct IS distinguished.
func parsePubspecLock(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Packages map[string]struct {
			Dependency string `yaml:"dependency"`
			Version    string `yaml:"version"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var entries []LockfileEntry
	for name, pkg := range doc.Packages {
		if pkg.Version == "" {
			continue
		}
		entries = append(entries, LockfileEntry{
			Name:    name,
			Version: pkg.Version,
			Direct:  strings.HasPrefix(pkg.Dependency, "direct"),
		})
	}
	return parsedLockfileData{Entries: entries, DirectKnown: true}, nil
}

// ============================================================
// Swift
// ============================================================

// parsePackageResolved handles SwiftPM's Package.resolved: v1 nests
// pins under "object" and names them "package"; v2/v3 have top-level
// "pins" named by "identity" (the lowercased repo name — matching the
// Package.swift manifest parser's repo-name extraction after the
// case-insensitive lockfileMatchKey). Direct deps are not
// distinguished.
func parsePackageResolved(data []byte) (parsedLockfileData, error) {
	type pin struct {
		Identity string `json:"identity"` // v2/v3
		Package  string `json:"package"`  // v1
		State    struct {
			Version string `json:"version"`
		} `json:"state"`
	}
	var doc struct {
		Pins   []pin `json:"pins"` // v2/v3
		Object struct {
			Pins []pin `json:"pins"` // v1
		} `json:"object"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	pins := doc.Pins
	if len(pins) == 0 {
		pins = doc.Object.Pins
	}
	var entries []LockfileEntry
	for _, p := range pins {
		name := p.Identity
		if name == "" {
			name = p.Package
		}
		if name != "" && p.State.Version != "" {
			entries = append(entries, LockfileEntry{Name: name, Version: p.State.Version})
		}
	}
	return parsedLockfileData{Entries: entries}, nil
}

// ============================================================
// .NET
// ============================================================

// parseDotnetPackagesLock handles NuGet's packages.lock.json:
// "dependencies" → target framework → package → {type, resolved}.
// type "Direct" vs "Transitive" distinguishes direct deps; entries are
// deduped across target frameworks (Direct anywhere wins).
func parseDotnetPackagesLock(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Dependencies map[string]map[string]struct {
			Type         string            `json:"type"`
			Resolved     string            `json:"resolved"`
			Dependencies map[string]string `json:"dependencies"`
		} `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	type key struct{ name, version string }
	seen := map[key]bool{} // key → direct
	edgeSeen := map[string]bool{}
	var out parsedLockfileData
	for _, framework := range doc.Dependencies {
		for name, pkg := range framework {
			if pkg.Resolved == "" {
				continue
			}
			k := key{name, pkg.Resolved}
			seen[k] = seen[k] || strings.EqualFold(pkg.Type, "Direct")
			// v0.27.133 C2: per-package dependency maps, deduped across
			// target frameworks.
			for child, constraint := range pkg.Dependencies {
				ek := name + "@" + pkg.Resolved + ">" + child
				if edgeSeen[ek] {
					continue
				}
				edgeSeen[ek] = true
				out.Edges = append(out.Edges, LockfileEdge{ParentName: name, ParentVersion: pkg.Resolved, ChildName: child, Constraint: constraint})
			}
		}
	}
	for k, direct := range seen {
		out.Entries = append(out.Entries, LockfileEntry{Name: k.name, Version: k.version, Direct: direct})
	}
	out.DirectKnown = true
	return out, nil
}

// ============================================================
// Gradle
// ============================================================

// parseGradleLockfile handles gradle.lockfile: one
// "group:artifact:version=configurations" line per resolved module.
// Names are "group:artifact" — the same shape the pom.xml and
// build.gradle manifest parsers produce, so matching lines up. The
// "empty=..." bookkeeping line and # comments are skipped. Direct deps
// are not distinguished.
func parseGradleLockfile(data []byte) (parsedLockfileData, error) {
	var entries []LockfileEntry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		coord, _, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		parts := strings.Split(coord, ":")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			continue // "empty=..." and malformed lines
		}
		entries = append(entries, LockfileEntry{Name: parts[0] + ":" + parts[1], Version: parts[2]})
	}
	return parsedLockfileData{Entries: entries}, nil
}

// ============================================================
// Haskell
// ============================================================

// stackHackageNameVersionRe splits "aeson-2.1.2.1" at the last dash
// before an all-numeric version tail.
var stackHackageNameVersionRe = regexp.MustCompile(`^(.+)-([0-9][0-9.]*)$`)

// parseStackYamlLock handles stack.yaml.lock: YAML "packages" list
// whose completed.hackage values are "name-version@sha256:...". Direct
// deps are not distinguished (the lock covers extra-deps, all of which
// are declared, but the snapshot resolution model doesn't map onto a
// direct/transitive split).
func parseStackYamlLock(data []byte) (parsedLockfileData, error) {
	var doc struct {
		Packages []struct {
			Completed struct {
				Hackage string `yaml:"hackage"`
			} `yaml:"completed"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return parsedLockfileData{}, err
	}
	var entries []LockfileEntry
	for _, p := range doc.Packages {
		spec := p.Completed.Hackage
		if at := strings.IndexByte(spec, '@'); at >= 0 {
			spec = spec[:at]
		}
		if m := stackHackageNameVersionRe.FindStringSubmatch(spec); m != nil {
			entries = append(entries, LockfileEntry{Name: m[1], Version: m[2]})
		}
	}
	return parsedLockfileData{Entries: entries}, nil
}
