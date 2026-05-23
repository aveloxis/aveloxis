// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package distribution houses the v0.24.0 DistributionWorker
// supporting code. Manifest-name parsers live here; the worker
// itself is in internal/collector/distribution_worker.go.
package distribution

import (
	"encoding/json"
	"regexp"
	"strings"
)

// ParseManifestName returns the declared package name from the given
// manifest content, or "" if the parser cannot reliably extract one.
//
// Conservative contract: parsers prefer false negatives (return "")
// over false positives (return wrong name). The manifest_type column
// already records the *intent* via filename; the parsed name is a
// best-effort enrichment for the headline analysis query.
//
// filename is matched case-insensitively against well-known manifest
// basenames AND a small set of suffix patterns (*.gemspec, etc.).
func ParseManifestName(filename, content string) string {
	if content == "" {
		return ""
	}
	lower := strings.ToLower(filename)
	// Strip directory prefix if a full path was passed.
	if idx := strings.LastIndex(lower, "/"); idx >= 0 {
		lower = lower[idx+1:]
	}

	switch lower {
	case "package.json":
		return parseJSONName(content)
	case "composer.json":
		return parseJSONName(content)
	case "cargo.toml":
		return parseTOMLName(content, "package")
	case "pyproject.toml":
		// PEP 621 [project] takes precedence; Poetry uses [tool.poetry].
		if name := parseTOMLName(content, "project"); name != "" {
			return name
		}
		return parseTOMLNamePath(content, []string{"tool", "poetry"})
	case "setup.py":
		return parseSetupPyName(content)
	case "setup.cfg":
		return parseSetupCfgName(content)
	case "pom.xml":
		return parsePomXMLName(content)
	case "go.mod":
		return parseGoModName(content)
	case "project.toml", "juliaproject.toml":
		// Julia: top-level `name = "..."` outside any [section].
		return parseJuliaProjectName(content)
	case "description":
		// R/CRAN: `Package: <name>` as the first or near-first
		// header in a Debian-control-style file.
		return parseRDescriptionName(content)
	case "meta.yaml", "recipe.yaml":
		// conda: `package:\n  name: <value>` near the top of the
		// recipe. YAML proper, but we hand-roll a minimal scanner
		// to avoid pulling a YAML dependency.
		return parseCondaRecipeName(content)
	}

	// Suffix-based matching for *.gemspec / *.csproj / *.podspec etc.
	switch {
	case strings.HasSuffix(lower, ".gemspec"):
		return parseGemspecName(content, lower)
	}
	return ""
}

// ---- JSON-based parsers (package.json, composer.json) -----------------

// parseJSONName decodes content as JSON and returns the top-level
// "name" string field. Returns "" on parse failure or missing field.
func parseJSONName(content string) string {
	var obj struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(content), &obj); err != nil {
		return ""
	}
	return obj.Name
}

// ---- TOML-based parsers (Cargo.toml, pyproject.toml) ------------------

// parseTOMLName extracts `name = "..."` from the FIRST occurrence of
// `[<section>]` in a TOML document. Hand-rolled because we don't want
// to pull a TOML dependency just for this — manifests are simple
// enough that a line scan suffices.
func parseTOMLName(content, section string) string {
	return parseTOMLNamePath(content, []string{section})
}

// parseTOMLNamePath supports dotted-section addressing like
// [tool.poetry]. Returns the first `name = "..."` line found inside
// the matching section, or "" if the section isn't present.
func parseTOMLNamePath(content string, path []string) string {
	target := "[" + strings.Join(path, ".") + "]"
	inSection := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			inSection = (trimmed == target)
			continue
		}
		if !inSection {
			continue
		}
		// Match: name = "value" or name = 'value'
		if name := extractKVString(trimmed, "name"); name != "" {
			return name
		}
	}
	return ""
}

// extractKVString matches `<key> = "<value>"` or `<key> = '<value>'`
// (with arbitrary whitespace). Returns "" on no match.
var kvStringPattern = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_-]*)\s*=\s*["']([^"']+)["']`)

func extractKVString(line, key string) string {
	m := kvStringPattern.FindStringSubmatch(line)
	if len(m) != 3 {
		return ""
	}
	if m[1] != key {
		return ""
	}
	return m[2]
}

// ---- setup.py (Python) -----------------------------------------------

// setupPyNamePattern matches setup(name="...") or setup(name='...')
// with any amount of whitespace and newlines in between. Best-effort —
// setup.py is arbitrary Python and a real parse would require
// executing it. We're OK with this only catching the common idiom.
var setupPyNamePattern = regexp.MustCompile(`(?s)setup\s*\([^)]*?\bname\s*=\s*["']([^"']+)["']`)

func parseSetupPyName(content string) string {
	m := setupPyNamePattern.FindStringSubmatch(content)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// ---- setup.cfg (INI-ish) ---------------------------------------------

func parseSetupCfgName(content string) string {
	inMetadata := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			inMetadata = (trimmed == "[metadata]")
			continue
		}
		if !inMetadata {
			continue
		}
		if strings.HasPrefix(trimmed, "name") {
			// name = foo OR name=foo
			parts := strings.SplitN(trimmed, "=", 2)
			if len(parts) == 2 && strings.TrimSpace(parts[0]) == "name" {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

// ---- pom.xml (Maven) -------------------------------------------------

// pomGroupIDPattern + pomArtifactIDPattern. We want the project's own
// groupId/artifactId, NOT the parent's. Maven POMs put project ID
// elements as direct children of <project>; parent ID elements live
// inside <parent>. Conservative regex: capture the FIRST groupId/
// artifactId that is not inside a <parent> ... </parent> block.
var (
	pomGroupIDPattern    = regexp.MustCompile(`(?s)<groupId>\s*([^<\s]+)\s*</groupId>`)
	pomArtifactIDPattern = regexp.MustCompile(`(?s)<artifactId>\s*([^<\s]+)\s*</artifactId>`)
	pomParentBlock       = regexp.MustCompile(`(?s)<parent>.*?</parent>`)
)

func parsePomXMLName(content string) string {
	// Strip <parent> blocks so the FindStringSubmatch picks up the
	// project's own ID elements.
	stripped := pomParentBlock.ReplaceAllString(content, "")
	g := pomGroupIDPattern.FindStringSubmatch(stripped)
	a := pomArtifactIDPattern.FindStringSubmatch(stripped)
	if len(g) != 2 || len(a) != 2 {
		// Some POMs omit groupId and inherit from <parent>. Without
		// the groupId, returning just the artifactId would be
		// ambiguous; better to bail.
		return ""
	}
	return g[1] + ":" + a[1]
}

// ---- go.mod (Go modules) ---------------------------------------------

func parseGoModName(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "module ") {
			rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "module"))
			// Strip trailing comments
			if idx := strings.Index(rest, "//"); idx >= 0 {
				rest = strings.TrimSpace(rest[:idx])
			}
			// Strip optional quotes
			rest = strings.Trim(rest, `"`)
			return rest
		}
	}
	return ""
}

// ---- Project.toml (Julia) --------------------------------------------

// parseJuliaProjectName extracts the top-level `name = "..."` line
// from a Julia Project.toml. Julia's Pkg.jl writes `name` and `uuid`
// at the FILE TOP outside any [section] header (unlike Cargo or
// pyproject which scope name inside [package] / [project]). Our
// TOML scanner needs a "top-level" mode where inSection==true
// initially and flips to false when the first [...] header is seen.
//
// Example shape:
//
//	name = "Drifters"
//	uuid = "..."
//	authors = ["..."]
//	version = "0.5.0"
//	[deps]
//	...
func parseJuliaProjectName(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		// Stop at the first section header; `name` past that is
		// scoped to a different table (e.g. [deps].name would be a
		// dependency name, not the package name).
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			return ""
		}
		if name := extractKVString(trimmed, "name"); name != "" {
			return name
		}
	}
	return ""
}

// ---- DESCRIPTION (R / CRAN / Bioconductor) ---------------------------

// parseRDescriptionName extracts the `Package: <name>` header from
// an R DESCRIPTION file. DESCRIPTION uses RFC 822 / Debian-control
// style: case-sensitive header keys, colon-separated, optional
// folded continuations on indented lines. We only need `Package:`
// which by convention appears as the first field.
//
// Conservative scan: the header is matched at line start (after
// trimming leading whitespace) with the exact key `Package:` (R
// is case-sensitive on this field). Values are trimmed; continuation
// lines (rare for the Package field) are not followed.
func parseRDescriptionName(content string) string {
	for _, line := range strings.Split(content, "\n") {
		// DESCRIPTION lines are NOT trimmed of leading whitespace
		// for header matching — folded continuations are indented,
		// new headers are not.
		if strings.HasPrefix(line, "Package:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "Package:"))
			// Strip any trailing comment (R DESCRIPTION doesn't
			// formally allow comments but some files include #-prefixed
			// notes; be defensive).
			if idx := strings.Index(val, "#"); idx >= 0 {
				val = strings.TrimSpace(val[:idx])
			}
			return val
		}
	}
	return ""
}

// ---- meta.yaml / recipe.yaml (conda) ---------------------------------

// parseCondaRecipeName extracts `package:\n  name: <value>` from a
// conda-build meta.yaml or rattler-build recipe.yaml. We avoid
// pulling a YAML dependency for this single field — the structure
// is shallow and the key positions are predictable.
//
// Example meta.yaml shape:
//
//	package:
//	  name: numpy
//	  version: 1.26.0
//	source:
//	  url: ...
//
// Templating ({{ name|lower }}) is left as-is — we return the raw
// string. Operator workflow that wants resolved names should
// snapshot the rendered recipe instead.
func parseCondaRecipeName(content string) string {
	inPackage := false
	for _, line := range strings.Split(content, "\n") {
		// A top-level key (no leading indent) ends the package: block.
		if len(line) > 0 && (line[0] != ' ' && line[0] != '\t') {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "package:") {
				inPackage = true
				continue
			}
			// Any other top-level key exits the package: block.
			inPackage = false
			continue
		}
		if !inPackage {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "name:") {
			val := strings.TrimSpace(strings.TrimPrefix(trimmed, "name:"))
			// Strip surrounding quotes if present.
			val = strings.Trim(val, `"'`)
			// Strip trailing YAML comment.
			if idx := strings.Index(val, "#"); idx >= 0 {
				val = strings.TrimSpace(val[:idx])
			}
			return val
		}
	}
	return ""
}

// ---- *.gemspec (RubyGems) --------------------------------------------

// gemspecNamePattern matches `s.name = "..."` or `spec.name = '...'`
// inside a Gem::Specification.new block. Best-effort.
var gemspecNamePattern = regexp.MustCompile(`\b(?:s|spec|gem)\.name\s*=\s*["']([^"']+)["']`)

func parseGemspecName(content, filename string) string {
	m := gemspecNamePattern.FindStringSubmatch(content)
	if len(m) == 2 {
		return m[1]
	}
	// Fallback: derive name from filename. `my-gem.gemspec` → `my-gem`.
	// Only used when the content doesn't carry a clear name= line; the
	// gemspec basename convention is reliable in practice.
	if strings.HasSuffix(filename, ".gemspec") {
		return strings.TrimSuffix(filename, ".gemspec")
	}
	return ""
}
