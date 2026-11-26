// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		got := NormalizeParsedVersion(manager, version)
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

// FuzzManifestParsers feeds one hostile document to the pure content-string
// manifest parsers listed below. These consume files exactly as they appear
// in cloned repositories — attacker-authored by definition. Contract: never
// panic.
//
// The list is NOT every parser the analysis walk dispatches, and saying so
// was an over-claim (v0.29.57). Absent — each stem checked against the
// target's body by TestFuzzManifestParsersNoteIsCurrent, since the first
// version of this note went stale within the release: the BuildGradle,
// MixExs, Pubspec, PackageYaml, ComposerJSON, SetupCfgDeps and
// SetupCfgVersions readers (the SetupCfgExtras reader IS here), and the
// lockfile readers (every *Lock* parser and PackageResolved). A parser this
// release touches is added here, which is the rule that keeps the gap from
// growing.
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
		_ = ParseRequirementsTxt(content)
		_ = parseGoMod(content)
		_ = parseTOMLDeps(content, "dependencies")
		_ = parseGemfile(content)
		_ = parsePomXML(content)
		_ = parsePEP621Deps(content)
		_, _ = parseSetupPyDeps(content)
		_, _ = parsePipfileDeps(content)
		_ = ParsePyRequirement(content)
		_, _ = parsePackageJSON([]byte(content))
		// Version-extracting variants — the second
		// extractPEP621-style slice site lives on this path (the twin
		// of the first fuzz find).
		_ = parsePEP621Versions(content)
		_ = parsePoetryVersions(content)
		_ = parsePipfileVersions(content)
		_ = parseSetupPyVersions(content)
		// The dev/build readers walk the same files with their own
		// line-splitting, and one of them met this contract the hard way:
		// its case arm is chosen on an `=` in the RAW line while the value
		// is split from the comment-stripped one, so `black  # = "^24"`
		// reached the split with nothing to split (v0.29.57).
		_ = parsePyprojectDevBuildVersions(content)
		_ = parsePipfileDevPackages(content)
		_ = parseSetupPyDevBuildVersions(content)
		_ = parseSetupCfgExtrasVersions(content)
		// Rewritten in v0.29.57 to strip `//` comments.
		_ = parseBuildSbt(content)
		_ = parseBuildSbtVersions(content)
		_ = parsePackageSwiftDeps(content)
		_ = parsePackageSwiftVersions(content)
		// Rewritten in v0.29.57 to strip `<!-- … -->` blocks.
		_ = parsePomXML(content)
		_ = ParsePomXMLVersions(content)
		_, _ = parseCsprojDeps(content)
		_ = parseCsprojVersions(content)
		_ = parseNuGetPackagesConfig(content)
		_ = parseNuGetPackagesConfigVersions(content)
		_, _ = parseDirectoryPackagesProps(content)
		_ = parseDirectoryPackagesPropsVersions(content)
	})
}
