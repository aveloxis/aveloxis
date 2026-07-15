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
func parsePoetryStyleTOML(data []byte) ([]LockfileEntry, bool, error) {
	var entries []LockfileEntry
	inPackage := false
	name, version := "", ""
	flush := func() {
		if name != "" && version != "" {
			entries = append(entries, LockfileEntry{Name: name, Version: version})
		}
		name, version = "", ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "[[package]]":
			flush()
			inPackage = true
			continue
		case strings.HasPrefix(trimmed, "["):
			// Any other section ([metadata], [package.dependencies],
			// [package.metadata], …) ends the top-level key scan.
			flush()
			inPackage = false
			continue
		}
		if !inPackage {
			continue
		}
		if v, ok := tomlStringValue(trimmed, "name"); ok {
			name = v
		}
		if v, ok := tomlStringValue(trimmed, "version"); ok {
			version = v
		}
	}
	flush()
	return entries, false, nil
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
func parsePipfileLock(data []byte) ([]LockfileEntry, bool, error) {
	var doc struct {
		Default map[string]struct {
			Version string `json:"version"`
		} `json:"default"`
		Develop map[string]struct {
			Version string `json:"version"`
		} `json:"develop"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, err
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
	return entries, false, nil
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

// parseGemfileLock handles Bundler's Gemfile.lock. Resolved packages
// come from the GEM/specs section (4-space indent); the DEPENDENCIES
// section lists the direct gems, so direct IS distinguished.
func parseGemfileLock(data []byte) ([]LockfileEntry, bool, error) {
	var entries []LockfileEntry
	direct := map[string]bool{}
	section := ""
	for _, raw := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" && !strings.HasPrefix(raw, " ") {
			section = trimmed // GEM / GIT / PATH / PLATFORMS / DEPENDENCIES / BUNDLED WITH
			continue
		}
		switch section {
		case "GEM", "GIT", "PATH":
			if m := gemfileLockSpecRe.FindStringSubmatch(raw); m != nil {
				entries = append(entries, LockfileEntry{Name: m[1], Version: m[2]})
			}
		case "DEPENDENCIES":
			if m := gemfileLockDepRe.FindStringSubmatch(raw); m != nil {
				direct[m[1]] = true
			}
		}
	}
	for i := range entries {
		entries[i].Direct = direct[entries[i].Name]
	}
	return entries, true, nil
}

// ============================================================
// PHP
// ============================================================

// parseComposerLock handles composer.lock: JSON arrays "packages" and
// "packages-dev" of {name, version}. The arrays hold the full resolved
// closure — direct deps are not distinguished. Leading "v" on versions
// (v7.8.1) is stripped to match manifest/purl conventions.
func parseComposerLock(data []byte) ([]LockfileEntry, bool, error) {
	var doc struct {
		Packages []struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"packages"`
		PackagesDev []struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"packages-dev"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, err
	}
	var entries []LockfileEntry
	for _, list := range [][]struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}{doc.Packages, doc.PackagesDev} {
		for _, p := range list {
			version := p.Version
			if len(version) > 1 && version[0] == 'v' && version[1] >= '0' && version[1] <= '9' {
				version = version[1:]
			}
			if p.Name != "" && version != "" {
				entries = append(entries, LockfileEntry{Name: p.Name, Version: version})
			}
		}
	}
	return entries, false, nil
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

func parseMixLock(data []byte) ([]LockfileEntry, bool, error) {
	var entries []LockfileEntry
	for _, m := range mixLockHexRe.FindAllStringSubmatch(string(data), -1) {
		entries = append(entries, LockfileEntry{Name: m[1], Version: m[2]})
	}
	return entries, false, nil
}

// ============================================================
// Dart
// ============================================================

// parsePubspecLock handles pubspec.lock: YAML "packages" map whose
// entries carry dependency: "direct main" / "direct dev" /
// "direct overridden" / "transitive" — direct IS distinguished.
func parsePubspecLock(data []byte) ([]LockfileEntry, bool, error) {
	var doc struct {
		Packages map[string]struct {
			Dependency string `yaml:"dependency"`
			Version    string `yaml:"version"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, false, err
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
	return entries, true, nil
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
func parsePackageResolved(data []byte) ([]LockfileEntry, bool, error) {
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
		return nil, false, err
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
	return entries, false, nil
}

// ============================================================
// .NET
// ============================================================

// parseDotnetPackagesLock handles NuGet's packages.lock.json:
// "dependencies" → target framework → package → {type, resolved}.
// type "Direct" vs "Transitive" distinguishes direct deps; entries are
// deduped across target frameworks (Direct anywhere wins).
func parseDotnetPackagesLock(data []byte) ([]LockfileEntry, bool, error) {
	var doc struct {
		Dependencies map[string]map[string]struct {
			Type     string `json:"type"`
			Resolved string `json:"resolved"`
		} `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, err
	}
	type key struct{ name, version string }
	seen := map[key]bool{} // key → direct
	for _, framework := range doc.Dependencies {
		for name, pkg := range framework {
			if pkg.Resolved == "" {
				continue
			}
			k := key{name, pkg.Resolved}
			seen[k] = seen[k] || strings.EqualFold(pkg.Type, "Direct")
		}
	}
	var entries []LockfileEntry
	for k, direct := range seen {
		entries = append(entries, LockfileEntry{Name: k.name, Version: k.version, Direct: direct})
	}
	return entries, true, nil
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
func parseGradleLockfile(data []byte) ([]LockfileEntry, bool, error) {
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
	return entries, false, nil
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
func parseStackYamlLock(data []byte) ([]LockfileEntry, bool, error) {
	var doc struct {
		Packages []struct {
			Completed struct {
				Hackage string `yaml:"hackage"`
			} `yaml:"completed"`
		} `yaml:"packages"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, false, err
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
	return entries, false, nil
}
