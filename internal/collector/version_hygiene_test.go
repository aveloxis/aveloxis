// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.27.71 — version hygiene. The 2026-08-01 zephyr incident: pip's
// hash-pinned requirements format ends every pin line with a backslash
// continuation ("pyyaml==6.0.3 \"), the parser kept the backslash in
// the version, and the malformed purl (pkg:pypi/pyyaml@6.0.3 \)
// defeated OSV's version matching — OSV degraded to package-level
// matching and returned the package's ENTIRE advisory history, so
// zephyr showed "6.0.3, fixed in 5.4" CRITICALs. The fleet survey
// found the same class in five more shapes (production counts):
//
//	backslash continuation   pypi     21K dep rows → 33K open FP findings
//	property interpolation   maven/nuget ${..} $(..)  → 7.4K open
//	inline-table capture     cargo "workspace = true" (13.5K rows), git/path
//	option capture           rubygems "require: false", hex "git: .."
//	block sub-key fake deps  pub "path: ../" rows named "path"
//	wildcard / protocols     "*", workspace:^, catalog:, file:..
//
// Two defense layers, both pinned here:
//  1. Parser fixes at the source (requirements.txt, Cargo.toml,
//     Gemfile, mix.exs, pubspec.yaml, manifest.CleanVersion space ranges).
//  2. manifest.NormalizeParsedVersion — the central choke point applied to
//     EVERY parser's output in the analysis walk, so future parser
//     slop can never reach purls/registry lookups again.

// ─── Layer 2: the central normalizer ────────────────────────────

// ─── Layer 1: manifest.CleanVersion space-separated ranges ───────────────

// ─── Layer 1: requirements.txt (the zephyr shape) ───────────────

// ─── Layer 1: Cargo.toml inline tables ──────────────────────────

func TestCargoInlineTableDeps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Cargo.toml")
	content := `[dependencies]
serde = { workspace = true }
tokio = { version = "1.38", features = ["full"] }
local = { path = "../.." }
fork = { git = "https://github.com/x/y" }
anyhow = "1.0.86"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseCargoVersions(path)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	if got["serde"] != "" {
		t.Errorf("workspace-inherited dep captured version %q, want \"\" — 'workspace = true' is not a version (13,471 production rows)", got["serde"])
	}
	if got["local"] != "" {
		t.Errorf("path dep captured version %q, want \"\"", got["local"])
	}
	if got["fork"] != "" {
		t.Errorf("git dep captured version %q, want \"\"", got["fork"])
	}
	if got["tokio"] != "1.38" {
		t.Errorf("inline-table version key = %q, want \"1.38\"", got["tokio"])
	}
	if got["anyhow"] != "1.0.86" {
		t.Errorf("scalar version = %q, want \"1.0.86\"", got["anyhow"])
	}
}

// ─── Layer 1: Gemfile option arguments ──────────────────────────

// ─── Layer 1: mix.exs non-version options ───────────────────────

// ─── Layer 1: pubspec.yaml block-style sub-keys ─────────────────

// ─── Layer 1: gradle project() references ───────────────────────

// ─── Scan-side purl rebuild (heal-vulnerabilities path) ─────────

// ─── v0.27.73: the OSV wire gate ────────────────────────────────

// The gate must be WIRED: ScanVulnerabilities filters targets through
// it before the OSV batch, logging what it drops (the "index N"
// anonymity of OSV's error is what made the 2026-08-01 investigation
// take hours — the log must NAME the dropped purl).
func TestScanVulnerabilitiesGatesWirePurls(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The gate itself is scantarget.Build's (TestBuildWireGate there); the
	// scan's part is to take its targets from Build and name what was dropped.
	if !strings.Contains(code, "scantarget.Build(") || !strings.Contains(code, "range set.Dropped") {
		t.Error("ScanVulnerabilities must take its targets from scantarget.Build and log each dropped one — " +
			"one malformed purl 400s the ENTIRE repo batch at OSV (81 repos on the 2026-08-01 heal run)")
	}
	if !strings.Contains(code, "dropping malformed scan target") {
		t.Error("dropped targets must be logged with the offending purl NAMED " +
			"(OSV's 'error in query at index N' identifies nothing)")
	}
}

// The choke point must actually be wired into the analysis walk —
// a normalizer nobody calls protects nothing.
func TestAnalysisWalkAppliesNormalizer(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "manifest.NormalizeParsedVersion(allDeps[i].Manager, allDeps[i].Version)") {
		t.Error("the analysis manifest walk must pass every parsed dep through " +
			"normalizeParsedVersion before resolution/storage — the central " +
			"hygiene layer of the v0.27.71 malformed-version fix")
	}
}

// The scan must normalize stored versions at read time — stored dep
// rows keep garbage until each repo's ANALYSIS re-runs, and without
// this layer heal-vulnerabilities would re-send the malformed purls.
func TestScanVulnerabilitiesNormalizesStoredVersions(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Normalising at read time is scantarget.Build's
	// (TestBuildNormalisesStoredVersions there drives it by behaviour).
	if !strings.Contains(code, "scantarget.Build(") {
		t.Error("ScanVulnerabilities must normalize dep versions read from the store " +
			"(v0.27.72) — heal-vulnerabilities re-scans from the deps table, which " +
			"carries pre-fix garbage until analysis re-runs")
	}
}
