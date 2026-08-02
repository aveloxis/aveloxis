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
//     Gemfile, mix.exs, pubspec.yaml, cleanVersion space ranges).
//  2. normalizeParsedVersion — the central choke point applied to
//     EVERY parser's output in the analysis walk, so future parser
//     slop can never reach purls/registry lookups again.

// ─── Layer 2: the central normalizer ────────────────────────────

func TestNormalizeParsedVersionTable(t *testing.T) {
	cases := []struct {
		manager, in, want string
	}{
		// The zephyr shape: trailing continuation backslash.
		{"pypi", `6.0.3 \`, "6.0.3"},
		{"pypi", `2.7.0 \`, "2.7.0"},
		// Legit versions pass through untouched.
		{"pypi", "6.0.3", "6.0.3"},
		{"pypi", "1!2.0", "1!2.0"}, // PEP 440 epoch
		{"go", "v0.0.0-20260101000000-abcdef123456", "v0.0.0-20260101000000-abcdef123456"},
		{"maven", "1.0.0.Final", "1.0.0.Final"},
		{"maven", "2.0-SNAPSHOT", "2.0-SNAPSHOT"},
		{"npm", "2.0.0-rc.1+build.5", "2.0.0-rc.1+build.5"},
		{"nuget", "1.0.0-preview.7", "1.0.0-preview.7"},
		// Property interpolation (maven ${..}, msbuild $(..)) — the
		// manifest literally can't tell us the version.
		{"maven", "${project.version}", ""},
		{"maven", "${spring.version}", ""},
		{"nuget", "$(MauiVersion)", ""},
		// Template/placeholder garbage.
		{"pypi", "{{latest-pypi-version}}", ""},
		{"pypi", "%s", ""},
		{"pypi", `{version = "*"`, ""},
		// Cargo inline-table fragments (belt — the parser fix is the
		// suspenders).
		{"cargo", "workspace = true", ""},
		{"cargo", `git = "https://github.com/p-hoffmann/trexsql-rs"`, ""},
		{"cargo", `path = "../.."`, ""},
		// npm monorepo protocols.
		{"npm", "workspace:^", ""},
		{"npm", "workspace:.", ""},
		{"npm", "workspace:*", ""},
		{"npm", "catalog:", ""},
		{"npm", "catalog:testing", ""},
		{"npm", "file:../../../tools/eslint", ""},
		{"npm", "/tmp/prisma-0.0.0.tgz", ""},
		// Wildcards mean "any version" — unpinned, not a version.
		{"npm", "*", ""},
		{"pypi", "*", ""},
		{"packagist", "1.0.*@dev", ""},
		// Packagist stability flags.
		{"packagist", "@dev", ""},
		{"packagist", "@stable", ""},
		{"packagist", "2.1.0@beta", "2.1.0"},
		// Space-separated compound ranges keep the floor.
		{"pub", "0.11.1 <0.12.0", "0.11.1"},
		{"npm", "16.14.0 <20.0.0", "16.14.0"},
		{"hex", "3.0.0 and < 5.0.0", "3.0.0"},
		// Comma ranges keep the floor (requirements.txt "10,<11").
		{"pypi", "10,<11", "10"},
		// Option/keyword capture (belt for the Gemfile/mix fixes).
		{"rubygems", "require: false", ""},
		{"rubygems", `path: "../`, ""},
		{"rubygems", ":require => false", ""},
		{"hex", `git: "https://github.com/plausible/clickhouse_ecto.git`, ""},
		{"hex", `github: "burrito-elixir/burrito`, ""},
		{"hex", `path: path("ibrowse")`, ""},
		{"pub", "../", ""},
		{"pub", "../../../lib/dart", ""},
		// Env markers / comments that leak past a parser.
		{"pypi", `0.4.6 ; sys_platform == 'win32'`, "0.4.6"},
		{"pypi", "3.1.0 # Apache-2.0", "3.1.0"},
		// Digit-less tokens are never usable versions.
		{"maven", "RELEASE", ""},
		{"npm", "latest", ""},
		// Empty stays empty.
		{"pypi", "", ""},
	}
	for _, c := range cases {
		if got := normalizeParsedVersion(c.manager, c.in); got != c.want {
			t.Errorf("normalizeParsedVersion(%q, %q) = %q, want %q", c.manager, c.in, got, c.want)
		}
	}
}

// GitHub Actions refs are refs, not registry versions — "releases/v1"
// is a legitimate pin evaluated client-side (actionRefAffected,
// v0.27.47). The normalizer must never touch them.
func TestNormalizeParsedVersionExemptsGitHubActions(t *testing.T) {
	for _, ref := range []string{"releases/v1", "release/v1", "gha/v0", "v4", "5c049362c2b3f4b8dd1ee0f6d0b7f2e9a0e8f7d6"} {
		if got := normalizeParsedVersion("githubactions", ref); got != ref {
			t.Errorf("normalizeParsedVersion(githubactions, %q) = %q — Actions refs must pass through verbatim", ref, got)
		}
	}
}

// The choke point must actually be wired into the analysis walk —
// a normalizer nobody calls protects nothing.
func TestAnalysisWalkAppliesNormalizer(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "normalizeParsedVersion(allDeps[i].Manager, allDeps[i].Version)") {
		t.Error("the analysis manifest walk must pass every parsed dep through " +
			"normalizeParsedVersion before resolution/storage — the central " +
			"hygiene layer of the v0.27.71 malformed-version fix")
	}
}

// ─── Layer 1: cleanVersion space-separated ranges ───────────────

func TestCleanVersionSpaceSeparatedRanges(t *testing.T) {
	cases := map[string]string{
		">=0.11.1 <0.12.0":  "0.11.1",
		"0.11.1 <0.12.0":    "0.11.1",
		"1.2.3 - 2.3.4":     "1.2.3", // npm hyphen range
		"~> 1.2":            "1.2",   // must NOT regress the operator-then-space shape
		"3.0.0 and < 5.0.0": "3.0.0",
	}
	for in, want := range cases {
		if got := cleanVersion(in); got != want {
			t.Errorf("cleanVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

// ─── Layer 1: requirements.txt (the zephyr shape) ───────────────

func TestRequirementsTxtHashPinnedFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "requirements.txt")
	content := "pyyaml==6.0.3 \\\n" +
		"    --hash=sha256:aaaa \\\n" +
		"    --hash=sha256:bbbb\n" +
		"urllib3==2.7.0 \\\n" +
		"    --hash=sha256:cccc\n" +
		"croniter==0.4.6 ; sys_platform == 'win32'\n" +
		"attrs==3.1.0  # Apache-2.0\n" +
		"django>=10,<11\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseRequirementsTxtVersions(path)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	want := map[string]string{
		"pyyaml":   "6.0.3",
		"urllib3":  "2.7.0",
		"croniter": "0.4.6",
		"attrs":    "3.1.0",
		"django":   "10",
	}
	for name, v := range want {
		if got[name] != v {
			t.Errorf("requirements.txt %s: version = %q, want %q (full parse: %v)", name, got[name], v, got)
		}
	}
}

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

func TestGemfileOptionArgsAreNotVersions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Gemfile")
	content := `source "https://rubygems.org"
gem "rails", "~> 7.1"
gem "bootsnap", require: false
gem "local_thing", path: "../shared"
gem "jekyll-feed", group: :jekyll_plugins
gem "pg", ">= 1.1", require: false
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseGemfileVersions(path)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	if got["rails"] != "7.1" {
		t.Errorf("rails version = %q, want \"7.1\"", got["rails"])
	}
	if got["pg"] != "1.1" {
		t.Errorf("pg version = %q, want \"1.1\" (quoted requirement before options)", got["pg"])
	}
	for _, name := range []string{"bootsnap", "local_thing", "jekyll-feed"} {
		if got[name] != "" {
			t.Errorf("%s: keyword option captured as version %q, want \"\" — "+
				"'require: false' was the #1 rubygems garbage version in production (1,212 rows)", name, got[name])
		}
	}
}

// ─── Layer 1: mix.exs non-version options ───────────────────────

func TestMixExsNonVersionOptsAreNotVersions(t *testing.T) {
	content := `defp deps do
    [
      {:phoenix, "~> 1.7.0"},
      {:bamboo, git: "https://github.com/pablo-co/bamboo_postmark.git", tag: "v1.0"},
      {:local_dep, path: "../shared"},
      {:burrito, github: "burrito-elixir/burrito"},
      {:ranged, ">= 3.0.0 and < 5.0.0"}
    ]
  end
`
	deps := parseMixExsVersions(content)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	if got["phoenix"] != "1.7.0" {
		t.Errorf("phoenix version = %q, want \"1.7.0\"", got["phoenix"])
	}
	if got["ranged"] != "3.0.0" {
		t.Errorf("ranged version = %q, want \"3.0.0\" (space-range floor)", got["ranged"])
	}
	for _, name := range []string{"bamboo", "local_dep", "burrito"} {
		if got[name] != "" {
			t.Errorf("%s: option captured as version %q, want \"\"", name, got[name])
		}
	}
}

// ─── Layer 1: pubspec.yaml block-style sub-keys ─────────────────

// Block-style deps nest their source under the dep name:
//
//	my_lib:
//	  path: ../
//
// The pre-v0.27.71 parser emitted the SUB-KEYS as deps: a package
// named "path" at version "../" (90 production rows). The fix tracks
// the dep indent level — deeper lines are sub-keys, not deps. The
// real pub.dev package NAMED "path" ("path: ^1.8.0" at dep level)
// must keep working.
func TestPubspecBlockSubKeysAreNotDeps(t *testing.T) {
	content := `name: my_app
dependencies:
  path: ^1.8.0
  my_lib:
    path: ../
  from_git:
    git:
      url: https://github.com/x/y.git
      ref: main
  http: '>=0.11.1 <0.12.0'
dev_dependencies:
  test: ^1.24.0
`
	deps := parsePubspecVersions(content)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	if got["path"] != "1.8.0" {
		t.Errorf("the real 'path' package at dep level = %q, want \"1.8.0\"", got["path"])
	}
	if got["http"] != "0.11.1" {
		t.Errorf("http range floor = %q, want \"0.11.1\"", got["http"])
	}
	if got["test"] != "1.24.0" {
		t.Errorf("dev dep test = %q, want \"1.24.0\"", got["test"])
	}
	if _, exists := got["url"]; exists {
		t.Error("block sub-key 'url' emitted as a dep — sub-keys of block-style deps are not packages")
	}
	if _, exists := got["ref"]; exists {
		t.Error("block sub-key 'ref' emitted as a dep")
	}
	for _, d := range deps {
		if d.Name == "my_lib" && d.Version != "" {
			t.Errorf("block-style path dep my_lib version = %q, want \"\"", d.Version)
		}
		if d.Name == "git" || (d.Name == "path" && d.Version == "../") {
			t.Errorf("sub-key emitted as dep: %s@%s", d.Name, d.Version)
		}
	}
}

// ─── Layer 1: gradle project() references ───────────────────────

// `implementation project(":core:cas-server-core-util-api")` is a
// reference to the repo's OWN module, not an external dependency.
// The pre-v0.27.71 parser split the quoted path on ":" and emitted a
// fake maven dep named ":core" with the MODULE NAME as its version
// (583+ production rows for apereo/cas alone).
func TestGradleProjectRefsAreNotDeps(t *testing.T) {
	content := `dependencies {
    implementation project(":core:cas-server-core-util-api")
    implementation(project(":compiler:fir:tree"))
    testImplementation project(':solr:core')
    implementation "org.springframework:spring-core:6.1.0"
    api 'com.google.guava:guava:33.0.0-jre'
}
`
	deps := parseBuildGradleVersions(content)
	for _, d := range deps {
		if strings.HasPrefix(d.Name, ":") {
			t.Errorf("project() reference emitted as dep: %s@%s — intra-repo modules are not external deps", d.Name, d.Version)
		}
	}
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	if got["org.springframework:spring-core"] != "6.1.0" {
		t.Errorf("external coord = %q, want \"6.1.0\"", got["org.springframework:spring-core"])
	}
	if got["com.google.guava:guava"] != "33.0.0-jre" {
		t.Errorf("external coord = %q, want \"33.0.0-jre\"", got["com.google.guava:guava"])
	}
}

// ─── Scan-side purl rebuild (heal-vulnerabilities path) ─────────

// purlReplaceVersion swaps or strips a purl's version segment so the
// vuln scan can rebuild purls from normalized versions at READ time —
// what lets `aveloxis heal-vulnerabilities` clean up the malformed-
// purl false positives IMMEDIATELY, without waiting for each repo's
// analysis phase to re-run.
func TestPurlReplaceVersion(t *testing.T) {
	cases := []struct{ purl, version, want string }{
		{`pkg:pypi/pyyaml@6.0.3 \`, "6.0.3", "pkg:pypi/pyyaml@6.0.3"},
		{"pkg:cargo/serde@workspace = true", "", "pkg:cargo/serde"},
		{"pkg:npm/express@4.18.0", "4.19.2", "pkg:npm/express@4.19.2"},
		// npm scope marker '@' must not be mistaken for the version
		// separator.
		{"pkg:npm/%40scope/name@1.0", "2.0", "pkg:npm/%40scope/name@2.0"},
		{"pkg:npm/%40scope/name", "1.0", "pkg:npm/%40scope/name@1.0"},
		{"pkg:pypi/flask", "", "pkg:pypi/flask"},
		{"", "1.0", ""},
	}
	for _, c := range cases {
		if got := purlReplaceVersion(c.purl, c.version); got != c.want {
			t.Errorf("purlReplaceVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
		}
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
	if !strings.Contains(code, "normalizeParsedVersion(") {
		t.Error("ScanVulnerabilities must normalize dep versions read from the store " +
			"(v0.27.72) — heal-vulnerabilities re-scans from the deps table, which " +
			"carries pre-fix garbage until analysis re-runs")
	}
	if !strings.Contains(code, "purlReplaceVersion(") {
		t.Error("ScanVulnerabilities must rebuild purls from the normalized version " +
			"— normalizing the version without the purl leaves the malformed purl in the query")
	}
}

// ─── v0.27.73: the OSV wire gate ────────────────────────────────

// The 2026-08-01 heal run: 81 repos failed with OSV 400s like
// `invalid URL escape "%i["` — pre-v0.27.29 RAW purls (built by
// concatenation before the canonical escaper) that survived in
// legacy-era dep rows. One malformed query 400s the ENTIRE repo
// batch, and OSV's "error in query at index N" names no purl, so
// diagnosis requires production forensics. wireValidPurl is the
// last-line syntactic gate: anything that would 400 at OSV is
// dropped (and NAMED in the log) instead of sinking the repo.
func TestWireValidPurl(t *testing.T) {
	valid := []string{
		"pkg:pypi/pyyaml@6.0.3",
		"pkg:pypi/numpy@%25s", // escape-valid (decodes to the garbage %s, but syntactically sendable)
		"pkg:gem/tzinfo-data@platforms:%20%25i[mingw%20mswin%20x64_mingw%20jruby]",
		"pkg:npm/%40scope/name@1.0.0",
		"pkg:pypi/flask", // versionless (unpinned / self-advisory)
		"pkg:golang/golang.org/x/text@v0.16.0",
	}
	for _, p := range valid {
		if !wireValidPurl(p) {
			t.Errorf("wireValidPurl(%q) = false, want true", p)
		}
	}
	invalid := []string{
		// The exact production 400 shapes (raw pre-escaper purls).
		`pkg:gem/tzinfo-data@platforms: %i[mingw mswin x64_mingw jruby]`,
		"pkg:pypi/numpy@%s",
		"pkg:gem/foo@1.0 %= bar",
		"pkg:gem/foo@100%",
		// Missing / blank name regions ("purl is missing name").
		"pkg:pypi/",
		"pkg:pypi/%20",
		"pkg:pypi/%20@1.0",
		// Whitespace anywhere is never wire-safe.
		"pkg:gem/foo bar@1.0",
		// Not a purl at all.
		"",
		"nonsense",
	}
	for _, p := range invalid {
		if wireValidPurl(p) {
			t.Errorf("wireValidPurl(%q) = true, want false", p)
		}
	}
}

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
	if !strings.Contains(code, "wireValidPurl(") {
		t.Error("ScanVulnerabilities must filter targets through wireValidPurl — " +
			"one malformed purl 400s the ENTIRE repo batch at OSV (81 repos on the 2026-08-01 heal run)")
	}
	if !strings.Contains(code, "dropping malformed scan target") {
		t.Error("dropped targets must be logged with the offending purl NAMED " +
			"(OSV's 'error in query at index N' identifies nothing)")
	}
}
