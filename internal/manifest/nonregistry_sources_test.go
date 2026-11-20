// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A dependency a manifest takes from a path, a git repository or another
// server is not the registry's package of that name (SR-6), in every
// ecosystem (review round 1 of the vulnerability program's first part).
// npm, Cargo, Python, NuGet and R said so; these five readers did not, and
// since 0.29.162 a dependency whose lookup fails keeps its row, so each
// such entry had a registry identity (`pkg:hex/local_dep`), was sent to
// OSV under it, and was listed in the SBOM as that registry package.

func nonRegistryOf(deps []model.LibyearDep) map[string]bool {
	out := map[string]bool{}
	for _, d := range deps {
		out[d.Name] = d.NonRegistry
	}
	return out
}

func checkNonRegistry(t *testing.T, what string, deps []model.LibyearDep, want map[string]bool) {
	t.Helper()
	got := nonRegistryOf(deps)
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("%s: %s is not among the dependencies (%v)", what, name, got)
			continue
		}
		if g != w {
			t.Errorf("%s: %s NonRegistry = %v, want %v", what, name, g, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%s: %d dependencies, want %d: %v", what, len(got), len(want), got)
	}
}

func TestPubspecSourcesOtherThanPubDev(t *testing.T) {
	deps := parsePubspecVersions(`name: app
dependencies:
  http: ^1.0.0
  path: ^1.8.0
  local_lib:
    path: ../local_lib
  from_git:
    git:
      url: https://github.com/x/from_git.git
      ref: main
  short_git:
    git: https://github.com/x/short_git.git
  mirrored:
    hosted: https://pub.corp.example
    version: ^2.0.0
  flutter_localizations:
    sdk: flutter
  inline_path: {path: ../inline_path}
  inline_git: { git: "https://github.com/x/inline_git.git" }
  block_version:
    version: ^3.1.0
  after: ^4.0.0
  flutter:
    sdk: flutter
dev_dependencies:
  test: ^1.24.0
  dev_local:
    path: tool/dev_local
`)
	checkNonRegistry(t, "pubspec.yaml", deps, map[string]bool{
		"http": false, "path": false, "local_lib": true, "from_git": true, "short_git": true, "mirrored": true,
		"flutter_localizations": true, "inline_path": true, "inline_git": true, "block_version": false, "after": false,
		"test": false, "dev_local": true,
	})
	// `flutter` itself is left out of the inventory, and its `sdk:` line
	// says nothing about the dependency written before it ("after").
}

func TestMixSourcesOtherThanHex(t *testing.T) {
	deps := parseMixExsVersions(`defp deps do
  [
    {:phoenix, "~> 1.7.0"},
    {:local_dep, path: "../local_dep"},
    {:from_git, git: "https://github.com/x/from_git.git", tag: "v1"},
    {:from_github, github: "x/from_github"},
    {:sibling, in_umbrella: true},
    {:pinned_fork, "~> 2.0", github: "me/pinned_fork"},
    {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
    {:path_like_name, "~> 1.0"},
    {:private_pkg, "~> 1.0", organization: "acme"}
  ]
end`)
	checkNonRegistry(t, "mix.exs", deps, map[string]bool{
		"phoenix": false, "local_dep": true, "from_git": true, "from_github": true, "sibling": true, "pinned_fork": true,
		"credo": false, "path_like_name": false, "private_pkg": false,
	})
}

func TestGemfileSourcesOtherThanRubygems(t *testing.T) {
	root := writeTree(t, map[string]string{"Gemfile": `source "https://rubygems.org"
gem "rails", "~> 7.0"
gem "local_gem", path: "../local_gem"
gem "old_style", :path => "vendor/old_style"
gem "from_git", git: "https://github.com/x/from_git.git", branch: "main"
gem "from_github", "~> 1.0", github: "x/from_github"
gem "other_server", "1.2", source: "https://gems.corp.example"
gem "pathname"
gem "requires", require: "git"
path "components" do
  gem "component_a"
  gem "component_b", "1.0"
end
git "https://github.com/x/mono.git" do
  gem "mono_one"
end
source "https://gems.corp.example" do
  gem "corp_gem"
end
group :development do
  platforms :ruby do
    gem "byebug"
  end
  gem "still_dev"
  gem "dev_local", path: "dev"
end
gem "after_all"
`})
	deps := ParseGemfileVersions(filepath.Join(root, "Gemfile"))
	checkNonRegistry(t, "Gemfile", deps, map[string]bool{
		"rails": false, "local_gem": true, "old_style": true, "from_git": true, "from_github": true, "other_server": true,
		"pathname": false, "requires": false, "component_a": true, "component_b": true, "mono_one": true, "corp_gem": true,
		"byebug": false, "still_dev": false, "dev_local": true, "after_all": false,
	})
	scope := map[string]string{}
	for _, d := range deps {
		scope[d.Name] = d.Type
	}
	// A block inside a group ends with its own `end`, and the group goes on.
	if scope["byebug"] != model.ScopeDev || scope["still_dev"] != model.ScopeDev || scope["after_all"] != "runtime" || scope["component_a"] != "runtime" {
		t.Errorf("scopes: %v", scope)
	}
}

func TestGoModReplacedByALocalPath(t *testing.T) {
	root := writeTree(t, map[string]string{"go.mod": `module example.com/app

go 1.22

require (
	example.com/local v0.0.0
	example.com/kept v1.2.3
	example.com/forked v1.0.0
	example.com/blocklocal v0.1.0
	example.com/versioned v1.5.0
)

require example.com/single v0.0.0-00010101000000-000000000000

replace example.com/local => ../local

replace example.com/single => ./third_party/single

replace example.com/forked => example.com/fork/forked v1.0.1

replace (
	example.com/blocklocal => /abs/blocklocal
	example.com/versioned v1.5.0 => ./patched // a comment
	example.com/notrequired => ../x
)
`})
	deps := ParseGoModVersions(filepath.Join(root, "go.mod"))
	checkNonRegistry(t, "go.mod", deps, map[string]bool{
		"example.com/local": true, "example.com/kept": false, "example.com/forked": false, "example.com/blocklocal": true,
		"example.com/versioned": true, "example.com/single": true,
	})
}

func TestComposerPackagesFromTheManifestsOwnRepositories(t *testing.T) {
	root := writeTree(t, map[string]string{
		"composer.json": `{
  "repositories": [
    {"type": "path", "url": "packages/*"},
    {"type": "path", "url": "../outside"},
    {"type": "package", "package": {"name": "acme/inline", "version": "1.0.0", "dist": {"url": "https://x.example/a.zip", "type": "zip"}}},
    {"type": "vcs", "url": "https://github.com/acme/fork"},
    {"packagist.org": false}
  ],
  "require": {"php": ">=8.1", "acme/local-one": "*", "acme/inline": "1.0.0", "monolog/monolog": "^3.0", "acme/fork": "dev-main"},
  "require-dev": {"acme/local-two": "@dev", "phpunit/phpunit": "^10"}
}`,
		"packages/one/composer.json": `{"name": "acme/local-one"}`,
		"packages/two/composer.json": `{"name": "Acme/Local-Two"}`,
	})
	deps := NewWalk(root, nil).Read(filepath.Join(root, "composer.json"), Options{}).Deps
	checkNonRegistry(t, "composer.json", deps, map[string]bool{
		"acme/local-one": true, "acme/inline": true, "monolog/monolog": false, "acme/local-two": true, "phpunit/phpunit": false,
		// A vcs repository's package name is in that repository, which is
		// not read: the requirement stays a Packagist one.
		"acme/fork": false,
	})
	// The repositories key may also be an object keyed by name.
	root2 := writeTree(t, map[string]string{
		"composer.json":     `{"repositories": {"local": {"type": "path", "url": "./lib"}}, "require": {"acme/lib": "*", "other/pkg": "^1"}}`,
		"lib/composer.json": `{"name": "acme/lib"}`,
	})
	checkNonRegistry(t, "composer.json (object form)", NewWalk(root2, nil).Read(filepath.Join(root2, "composer.json"), Options{}).Deps,
		map[string]bool{"acme/lib": true, "other/pkg": false})
}

// hpack's package.yaml: a `dependencies:` list ends where the next key at
// its own indentation, or above it, begins. It ended only at an unindented
// line, so the lists that follow it in a section (default-extensions,
// exposed-modules, ghc-options) were read as Hackage packages.
func TestHpackSiblingListsAreNotDependencies(t *testing.T) {
	deps := parseHaskellPackageYamlVersions(`name: thing
dependencies:
- base >= 4.7 && < 5
- text

library:
  source-dirs: src
  dependencies:
    - aeson >= 2.0
    - containers
  default-extensions:
    - LambdaCase
    - OverloadedStrings
  exposed-modules:
    - Lib
  ghc-options:
    - -Wall

executables:
  thing-exe:
    main: Main.hs
    dependencies:
      - thing
      - optparse-applicative
    ghc-options:
      - -threaded

tests:
  thing-test:
    dependencies:
      - hspec
      # a comment inside the list
      - QuickCheck
    default-extensions:
      - TypeApplications
`)
	want := map[string]string{"base": "runtime", "text": "runtime", "aeson": "runtime", "containers": "runtime", "thing": "runtime",
		"optparse-applicative": "runtime", "hspec": model.ScopeTest, "QuickCheck": model.ScopeTest}
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Type
	}
	for name, scope := range want {
		if got[name] != scope {
			t.Errorf("%s: scope %q, want %q", name, got[name], scope)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%q was read as a Hackage dependency; it is not in a dependencies list", name)
		}
	}
}

// An item written as an object (hpack allows `- name: x` with keys below
// it) is one item: its deeper lines do not end the list, and it is read by
// its name.
func TestHpackObjectItemsDoNotEndTheList(t *testing.T) {
	deps := parseHaskellPackageYamlVersions(`dependencies:
  - name: aeson
    version: ">= 2.0"
    mixin:
      - hiding (Prelude)
  - text
other-key:
  - NotADependency
`)
	got := map[string]bool{}
	for _, d := range deps {
		got[d.Name] = true
	}
	if !got["aeson"] || !got["text"] || len(got) != 2 {
		t.Errorf("dependencies = %v, want aeson and text", got)
	}
}

// The Haskell NAME reader (the dependency-name inventory) follows the same
// list rule as the versioned reader: it is the same reader. It used to end
// a dependencies list only at an unindented line, so the lists after it in
// a section (default-extensions, exposed-modules, ghc-options) were stored
// as dependency names.
func TestHpackNamesAreTheVersionedReadersNames(t *testing.T) {
	content := `name: thing
dependencies:
  - base >= 4.7 && < 5
  - name: aeson
    version: ">= 2.0"
library:
  source-dirs: src
  dependencies:
    - text
  default-extensions:
    - LambdaCase
    - OverloadedStrings
  exposed-modules:
    - Lib
  ghc-options:
    - -Wall
tests:
  spec:
    dependencies:
      - hspec
    other-modules:
      - Paths_thing
`
	got := strings.Join(parsePackageYaml(content), " ")
	if want := "base aeson text hspec"; got != want {
		t.Errorf("names = %q, want %q", got, want)
	}
	var versioned []string
	for _, d := range parseHaskellPackageYamlVersions(content) {
		versioned = append(versioned, d.Name)
	}
	if strings.Join(versioned, " ") != got {
		t.Errorf("the two readers disagree: names %q, versioned %q", got, strings.Join(versioned, " "))
	}
}
