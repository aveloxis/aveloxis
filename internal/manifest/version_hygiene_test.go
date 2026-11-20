// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		if got := NormalizeParsedVersion(c.manager, c.in); got != c.want {
			t.Errorf("normalizeParsedVersion(%q, %q) = %q, want %q", c.manager, c.in, got, c.want)
		}
	}
}

// GitHub Actions refs are refs, not registry versions — "releases/v1"
// is a legitimate pin evaluated client-side (actionRefAffected,
// v0.27.47). The normalizer must never touch them.
func TestNormalizeParsedVersionExemptsGitHubActions(t *testing.T) {
	for _, ref := range []string{"releases/v1", "release/v1", "gha/v0", "v4", "5c049362c2b3f4b8dd1ee0f6d0b7f2e9a0e8f7d6"} {
		if got := NormalizeParsedVersion("githubactions", ref); got != ref {
			t.Errorf("normalizeParsedVersion(githubactions, %q) = %q — Actions refs must pass through verbatim", ref, got)
		}
	}
}

func TestCleanVersionSpaceSeparatedRanges(t *testing.T) {
	cases := map[string]string{
		">=0.11.1 <0.12.0":  "0.11.1",
		"0.11.1 <0.12.0":    "0.11.1",
		"1.2.3 - 2.3.4":     "1.2.3", // npm hyphen range
		"~> 1.2":            "1.2",   // must NOT regress the operator-then-space shape
		"3.0.0 and < 5.0.0": "3.0.0",
	}
	for in, want := range cases {
		if got := CleanVersion(in); got != want {
			t.Errorf("cleanVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

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
	deps := ParseRequirementsTxtVersions(path)
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
	deps := ParseGemfileVersions(path)
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
