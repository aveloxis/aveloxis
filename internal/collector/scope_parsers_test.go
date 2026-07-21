// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.44 (summary/19 P1) — behavioral tests for the dependency-scope
// relabeling. Eight ecosystems used to conflate dev/test/build/peer/
// optional dependencies as "runtime"; each fixture below is shaped
// like a real manifest (ground-truth rule: expected values derive
// from each package manager's own documentation, not from what our
// parser happens to emit).

package collector

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// scopeOf finds a dep by name and returns its Type; fails the test if
// the dep is absent (a scope test that silently skips a missing dep
// would pass against a parser that dropped the line entirely).
func scopeOf(t *testing.T, deps []libyearDep, name string) string {
	t.Helper()
	for _, d := range deps {
		if d.Name == name {
			return d.Type
		}
	}
	t.Fatalf("dep %q not found in parse result (%d deps)", name, len(deps))
	return ""
}

func TestNpmScopePeerAndOptional(t *testing.T) {
	dir := t.TempDir()
	pkg := `{
  "dependencies": {"react-router": "^6.0.0"},
  "devDependencies": {"jest": "^29.0.0"},
  "peerDependencies": {"react": ">=18"},
  "optionalDependencies": {"fsevents": "^2.3.2"}
}`
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(pkg), 0o644); err != nil {
		t.Fatal(err)
	}
	deps, err := parsePackageJSONVersions(filepath.Join(dir, "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := scopeOf(t, deps, "react-router"); got != "runtime" {
		t.Errorf("dependencies → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "jest"); got != model.ScopeDev {
		t.Errorf("devDependencies → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "react"); got != model.ScopePeer {
		t.Errorf("peerDependencies → %q, want peer (was conflated as runtime pre-v0.27.44)", got)
	}
	if got := scopeOf(t, deps, "fsevents"); got != model.ScopeOptional {
		t.Errorf("optionalDependencies → %q, want optional", got)
	}
}

func TestCargoScopeSections(t *testing.T) {
	dir := t.TempDir()
	toml := `[package]
name = "demo"

[dependencies]
serde = "1.0"

[dev-dependencies]
criterion = "0.5"

[build-dependencies]
cc = "1.0"
`
	if err := os.WriteFile(filepath.Join(dir, "Cargo.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseCargoVersions(filepath.Join(dir, "Cargo.toml"))
	if got := scopeOf(t, deps, "serde"); got != "runtime" {
		t.Errorf("[dependencies] → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "criterion"); got != model.ScopeDev {
		t.Errorf("[dev-dependencies] → %q, want dev (Cargo's own section semantics)", got)
	}
	if got := scopeOf(t, deps, "cc"); got != model.ScopeBuild {
		t.Errorf("[build-dependencies] → %q, want build", got)
	}
	// NOTE: the per-dep table form ([dependencies.tokio] with a
	// version = "..." line) exits the section tracker and is not
	// parsed — a pre-existing parser limitation, unchanged by the
	// v0.27.44 scope work.
}

func TestGemfileScopeGroups(t *testing.T) {
	dir := t.TempDir()
	gemfile := `source 'https://rubygems.org'

gem 'rails', '~> 7.1'
gem 'debug', group: :development

group :test do
  gem 'rspec-rails', '~> 6.0'
end

group :development, :test do
  gem 'pry'
end

group :development do
  gem 'rubocop'
end
`
	path := filepath.Join(dir, "Gemfile")
	if err := os.WriteFile(path, []byte(gemfile), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseGemfileVersions(path)
	if got := scopeOf(t, deps, "rails"); got != "runtime" {
		t.Errorf("ungrouped gem → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "debug"); got != model.ScopeDev {
		t.Errorf("inline group: :development → %q, want dev", got)
	}
	if got := scopeOf(t, deps, "rspec-rails"); got != model.ScopeTest {
		t.Errorf("group :test block → %q, want test", got)
	}
	// Mixed :development, :test group: gemGroupScope gives :test
	// precedence, so the whole group classifies as test.
	if got := scopeOf(t, deps, "pry"); got != model.ScopeTest {
		t.Errorf("group :development, :test → %q, want test (:test wins per gemGroupScope)", got)
	}
	if got := scopeOf(t, deps, "rubocop"); got != model.ScopeDev {
		t.Errorf("group :development → %q, want dev", got)
	}
}

func TestGemfileGroupEndPopsScope(t *testing.T) {
	dir := t.TempDir()
	gemfile := `group :test do
  gem 'rspec'
end
gem 'puma', '~> 6.0'
`
	path := filepath.Join(dir, "Gemfile")
	if err := os.WriteFile(path, []byte(gemfile), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseGemfileVersions(path)
	if got := scopeOf(t, deps, "puma"); got != "runtime" {
		t.Errorf("gem after group end → %q, want runtime — the end pop is broken", got)
	}
}

func TestMavenScopeElement(t *testing.T) {
	pom := `<project>
  <dependencies>
    <dependency>
      <groupId>com.google.guava</groupId>
      <artifactId>guava</artifactId>
      <version>33.0.0-jre</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.0</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>jakarta.servlet</groupId>
      <artifactId>jakarta.servlet-api</artifactId>
      <version>6.0.0</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>`
	deps := parsePomXMLVersions(pom)
	if got := scopeOf(t, deps, "com.google.guava:guava"); got != "runtime" {
		t.Errorf("no scope element → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "org.junit.jupiter:junit-jupiter"); got != model.ScopeTest {
		t.Errorf("<scope>test</scope> → %q, want test (Maven's own scope)", got)
	}
	if got := scopeOf(t, deps, "jakarta.servlet:jakarta.servlet-api"); got != model.ScopeBuild {
		t.Errorf("<scope>provided</scope> → %q, want build (compile-time only, container supplies at runtime)", got)
	}
}

func TestMixScopeOnlyOption(t *testing.T) {
	mix := `defmodule Demo.MixProject do
  defp deps do
    [
      {:phoenix, "~> 1.7.0"},
      {:ex_machina, "~> 2.7", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev}
    ]
  end
end`
	deps := parseMixExsVersions(mix)
	if got := scopeOf(t, deps, "phoenix"); got != "runtime" {
		t.Errorf("no only: → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "ex_machina"); got != model.ScopeTest {
		t.Errorf("only: :test → %q, want test", got)
	}
	if got := scopeOf(t, deps, "credo"); got != model.ScopeDev {
		t.Errorf("only: [:dev, :test] → %q, want dev (:dev precedence)", got)
	}
	if got := scopeOf(t, deps, "dialyxir"); got != model.ScopeDev {
		t.Errorf("only: :dev → %q, want dev", got)
	}
}

func TestSbtScopeConfigurations(t *testing.T) {
	sbt := `libraryDependencies += "org.typelevel" %% "cats-core" % "2.10.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.17.0" % "test"
libraryDependencies += "javax.servlet" % "javax.servlet-api" % "4.0.1" % Provided
`
	deps := parseBuildSbtVersions(sbt)
	if got := scopeOf(t, deps, "org.typelevel:cats-core"); got != "runtime" {
		t.Errorf("no configuration → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "org.scalatest:scalatest"); got != model.ScopeTest {
		t.Errorf("%% Test → %q, want test", got)
	}
	if got := scopeOf(t, deps, "org.scalacheck:scalacheck"); got != model.ScopeTest {
		t.Errorf("%% \"test\" quoted form → %q, want test", got)
	}
	if got := scopeOf(t, deps, "javax.servlet:javax.servlet-api"); got != model.ScopeBuild {
		t.Errorf("%% Provided → %q, want build", got)
	}
}

func TestCsprojScopePrivateAssets(t *testing.T) {
	csproj := `<Project Sdk="Microsoft.NET.Sdk">
  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
    <PackageReference Include="StyleCop.Analyzers" Version="1.1.118" PrivateAssets="all" />
    <PackageReference Include="Nerdbank.GitVersioning" Version="3.6.133" PrivateAssets="All" />
  </ItemGroup>
</Project>`
	deps := parseCsprojVersions(csproj)
	if got := scopeOf(t, deps, "Newtonsoft.Json"); got != "runtime" {
		t.Errorf("plain PackageReference → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "StyleCop.Analyzers"); got != model.ScopeBuild {
		t.Errorf("PrivateAssets=\"all\" → %q, want build (analyzer never flows to output)", got)
	}
	if got := scopeOf(t, deps, "Nerdbank.GitVersioning"); got != model.ScopeBuild {
		t.Errorf("PrivateAssets=\"All\" case-variant → %q, want build (EqualFold match)", got)
	}
}

func TestHaskellScopeTestSections(t *testing.T) {
	yaml := `name: demo

dependencies:
- base >= 4.7 && < 5
- text

library:
  source-dirs: src

tests:
  demo-test:
    main: Spec.hs
    dependencies:
    - hspec
    - QuickCheck

benchmarks:
  demo-bench:
    dependencies:
    - criterion
`
	deps := parseHaskellPackageYamlVersions(yaml)
	if got := scopeOf(t, deps, "base"); got != "runtime" {
		t.Errorf("top-level dependencies → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "text"); got != "runtime" {
		t.Errorf("top-level dependencies → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "hspec"); got != model.ScopeTest {
		t.Errorf("tests: section dependencies → %q, want test", got)
	}
	if got := scopeOf(t, deps, "criterion"); got != model.ScopeTest {
		t.Errorf("benchmarks: section dependencies → %q, want test", got)
	}
}
