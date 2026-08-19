// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Fuzz targets for the collector's trust-boundary parsers. These are the
// exact code paths that have produced production incident classes:
// garbage manifest versions defeating OSV matching (v0.27.71), raw purls
// 400-ing whole scan batches (v0.27.73), lockfile formats arriving
// malformed/truncated (v0.27.11). All targets run under ClusterFuzzLite
// (see .clusterfuzzlite/build.sh) and, seeds-only, in ordinary `go test`.

// FuzzParseLockfile drives all 18 lockfile formats with hostile bytes.
// Seeds come from the committed v0.27.11 fixture corpus so coverage
// starts warm. Contract: never panic; parse errors are the designed
// degrade path (WARN + skip in scanLockfiles).
func FuzzParseLockfile(f *testing.F) {
	kinds := []string{
		"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "bun.lock",
		"bun.lockb", "poetry.lock", "Pipfile.lock", "uv.lock", "pdm.lock",
		"Cargo.lock", "Gemfile.lock", "composer.lock", "mix.lock",
		"pubspec.lock", "Package.resolved", "packages.lock.json",
		"gradle.lockfile", "stack.yaml.lock",
	}
	// Seed every fixture under its kind (fixture filenames are the kind
	// name, optionally with a variant suffix like Package.resolved.v2).
	fixtures, _ := os.ReadDir(filepath.Join("testdata", "lockfiles"))
	for _, fx := range fixtures {
		data, err := os.ReadFile(filepath.Join("testdata", "lockfiles", fx.Name()))
		if err != nil {
			continue
		}
		for _, kind := range kinds {
			if strings.HasPrefix(fx.Name(), kind) {
				f.Add(kind, data)
				break
			}
		}
	}
	for _, kind := range kinds {
		f.Add(kind, []byte("{"))
		f.Add(kind, []byte("\x00\xff garbage: [unterminated"))
	}

	f.Fuzz(func(t *testing.T, kind string, data []byte) {
		res, err := ParseLockfile(kind, data)
		if err != nil {
			return
		}
		if res == nil {
			t.Fatal("ParseLockfile returned nil result with nil error")
		}
	})
}

// FuzzNormalizeParsedVersion asserts the v0.27.72 version-hygiene
// invariant on ARBITRARY input, not just the corpus: every emitted
// version is "" or passes the final gate — except the githubactions
// manager, whose refs (release tags, SHAs) pass through verbatim by
// design (v0.27.47 client-side evaluation). An unparseable version that
// escapes this gate silently matches EVERY OSV advisory for the package
// (the zephyr false-CRITICAL class).
func FuzzNormalizeParsedVersion(f *testing.F) {
	f.Add("pypi", "6.0.3 \\")
	f.Add("cargo", "workspace = true")
	f.Add("npm", ">=0.11.1 <0.12.0")
	f.Add("maven", "${project.version}")
	f.Add("githubactions", "releases/v1")
	f.Add("hex", "~> 1.2")
	f.Add("npm", "workspace:*")
	f.Fuzz(func(t *testing.T, manager, version string) {
		got := normalizeParsedVersion(manager, version)
		if manager == "githubactions" {
			return // verbatim passthrough by design
		}
		if got != "" && !versionGateOK(got) {
			t.Fatalf("normalizeParsedVersion(%q, %q) = %q — emitted a version "+
				"that fails versionGateOK; the v0.27.72 hygiene invariant "+
				"(every emitted version is \"\" or gate-valid) is broken", manager, version, got)
		}
	})
}

// FuzzPurlHelpers drives the purl construction/rewrite/validation trio
// on arbitrary input. Contract: never panic. NOTE: wire-validity of
// buildPurl output is deliberately NOT asserted — purlEscapeSegment
// handles the URL-reserved set, and wireValidPurl is the designed
// last-line gate for anything else (control bytes in garbage names);
// asserting the stronger property would flag designed behavior.
func FuzzPurlHelpers(f *testing.F) {
	f.Add("pypi", "flask", "2.5.0")
	f.Add("npm", "@babel/core", "7.0.0")
	f.Add("maven", "org.apache:commons", "1.0")
	f.Add("gem", "tzinfo-data", "platforms: %i[mingw]")
	f.Fuzz(func(t *testing.T, ecosystem, name, version string) {
		p := purlForPackage(ecosystem, name, version)
		_ = wireValidPurl(p)
		_ = purlWithVersion(p, version)
		_ = purlReplaceVersion(p, version)
		_ = purlReplaceVersion(name, version) // arbitrary non-purl input
		_ = selfAdvisoryPurl(ecosystem, name)
	})
}

// FuzzManifestParsers feeds one hostile document to every pure
// content-string manifest parser the analysis walk dispatches. These
// consume files exactly as they appear in cloned repositories —
// attacker-authored by definition. Contract: never panic.
func FuzzManifestParsers(f *testing.F) {
	// Seed with the committed manifest corpus (the v0.27.72 golden net) —
	// every fixture carries both legit declarations and production
	// garbage corners.
	fixtures, _ := os.ReadDir(filepath.Join("testdata", "manifest_corpus"))
	for _, fx := range fixtures {
		if strings.HasSuffix(fx.Name(), ".json") && fx.Name() == "golden.json" {
			continue
		}
		if data, err := os.ReadFile(filepath.Join("testdata", "manifest_corpus", fx.Name())); err == nil {
			f.Add(string(data))
		}
	}
	f.Add("pyyaml==6.0.3 \\\n  --hash=sha256:deadbeef\n")
	f.Add("[dependencies]\nserde = { workspace = true }\n")
	f.Add("gem 'rails', require: false\n")

	f.Fuzz(func(t *testing.T, content string) {
		_ = parseRequirementsTxt(content)
		_ = parseGoMod(content)
		_ = parseTOMLDeps(content, "dependencies")
		_ = parseGemfile(content)
		_ = parsePomXML(content)
		_ = parsePEP621Deps(content)
		_, _ = parseSetupPyDeps(content)
		_, _ = parsePipfileDeps(content)
		_ = parsePyRequirement(content)
		_, _ = parsePackageJSON([]byte(content))
		// Version-extracting variants — the second
		// extractPEP621-style slice site lives on this path (the twin
		// of the first fuzz find).
		_ = parsePEP621Versions(content)
		_ = parsePoetryVersions(content)
		_ = parsePipfileVersions(content)
		_ = parseSetupPyVersions(content)
	})
}
