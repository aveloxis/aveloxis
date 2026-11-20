// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The dangerous direction of the non-registry mark (review round 2 of the
// vulnerability program's first part). A dependency marked as not from the
// registry has no row and is never asked of OSV, so marking one that IS the
// registry's package takes a real dependency out of the checks. For each
// of the five readers that learned the mark in 0.29.208, and R's: the
// registry-form entries that must NOT be marked, beside one that must, so
// the table cannot pass by marking nothing.

func registryForms(t *testing.T, what string, deps []model.LibyearDep, registry []string, other []string) {
	t.Helper()
	got := nonRegistryOf(deps)
	for _, name := range registry {
		marked, ok := got[name]
		if !ok {
			t.Errorf("%s: %s is not among the dependencies (%v)", what, name, got)
		} else if marked {
			t.Errorf("%s: %s is the registry's package and is marked as not from it: it would never be checked", what, name)
		}
	}
	for _, name := range other {
		if marked, ok := got[name]; !ok || !marked {
			t.Errorf("%s: %s is not from the registry and is not marked (present %v)", what, name, ok)
		}
	}
}

func TestGemfileRegistryFormsAreNotMarked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Gemfile")
	gemfile := `source 'https://rubygems.org'
gem 'top', '1.0'
source 'https://rubygems.org' do
  gem 'in_block', '1.0'
end
source "https://rubygems.org/" do
  gem 'in_block_slash', '1.0'
end
source 'https://gems.corp.example' do
  gem 'private_block', '1.0'
end
gem 'opt_new', '1.0', source: 'https://rubygems.org'
gem 'opt_rocket', '1.0', :source => "https://rubygems.org/"
gem 'opt_http', '1.0', source: "http://rubygems.org"
gem 'opt_private', '1.0', source: 'https://gems.corp.example'
gem 'opt_git', '1.0', git: 'https://github.com/x/y'
gem 'plain_require', '1.0', require: false
gem 'plain_platforms', '1.0', platforms: [:ruby]
gem 'mentions', '1.0', require: 'source/path'
group :test do
  if ENV['A'] then gem 'one_line' end
  gem 'in_test', '1.0'
end
gem 'after_group', '1.0'
path 'vendor' do
  unless ENV['A'] then gem 'skipped' end
  gem 'vendored', '1.0'
end
gem 'after_path', '1.0'
case RUBY_ENGINE when 'jruby' then gem 'j' end
gem 'after_case', '1.0'
`
	if err := os.WriteFile(path, []byte(gemfile), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := ParseGemfileVersions(path)
	registryForms(t, "Gemfile", deps,
		[]string{"top", "in_block", "in_block_slash", "opt_new", "opt_rocket", "opt_http", "plain_require", "plain_platforms", "mentions",
			"in_test", "after_group", "after_path", "after_case"},
		[]string{"private_block", "opt_private", "opt_git", "vendored"})
	// A conditional that opens and closes on one line is not a block left
	// open: the group and the path block above each end at their own `end`.
	scope := map[string]string{}
	for _, d := range deps {
		scope[d.Name] = d.Type
	}
	if scope["in_test"] != model.ScopeTest || scope["after_group"] != "runtime" || scope["after_path"] != "runtime" || scope["after_case"] != "runtime" {
		t.Errorf("scopes: %v; want in_test test and the three after a block runtime", scope)
	}
}

func TestPubspecRegistryFormsAreNotMarked(t *testing.T) {
	deps := parsePubspecVersions(`name: app
dependencies:
  plain: ^1.0.0
  hosted_url:
    hosted: https://pub.dev
    version: ^1.0.0
  hosted_url_slash:
    hosted: "https://pub.dev/"
    version: ^1.0.0
  hosted_old:
    hosted:
      name: hosted_old
      url: https://pub.dartlang.org
    version: ^1.0.0
  hosted_inline: {hosted: "https://pub.dev", version: ^1.0.0}
  hosted_private:
    hosted: https://dart.corp.example
    version: ^1.0.0
  hosted_private_old:
    hosted:
      name: hosted_private_old
      url: https://dart.corp.example
    version: ^1.0.0
  hosted_private_inline: {hosted: "https://dart.corp.example", version: ^1.0.0}
  from_path:
    path: ../from_path
`)
	registryForms(t, "pubspec.yaml", deps,
		[]string{"plain", "hosted_url", "hosted_url_slash", "hosted_old", "hosted_inline"},
		[]string{"hosted_private", "hosted_private_old", "hosted_private_inline", "from_path"})
}

func TestMixRegistryFormsAreNotMarked(t *testing.T) {
	deps := parseMixExsVersions(`defp deps do
  [
    {:plain, "~> 1.0"},
    {:commented, "~> 1.0", override: true}, # was git: "https://x"
    {:todo, "~> 1.0"}, # TODO path: dep later
    {:with_only, "~> 1.0", only: :test, runtime: false},
    {:with_org, "~> 1.0", organization: "acme"},
    {:mentions, "~> 1.0", app: false}, # github: nope
    {:local, path: "../local"},
    {:forged, github: "x/forged"}
  ]
end
`)
	registryForms(t, "mix.exs", deps, []string{"plain", "commented", "todo", "with_only", "with_org", "mentions"}, []string{"local", "forged"})
}

func TestRRemotesRegistryFormsAreNotMarked(t *testing.T) {
	names := rRemoteNames("b2=cran::b2, cran::b3, standard::b4, named=standard::other, owner/forged, local=local::./pkg, g1=github::o/r")
	for _, registry := range []string{"b2", "b3", "b4", "named", "other"} {
		if names[registry] {
			t.Errorf("%s comes from the registry (pak's cran:: or standard::) and is marked as not from it", registry)
		}
	}
	for _, other := range []string{"forged", "local", "g1"} {
		if !names[other] {
			t.Errorf("%s is not from the registry and is not marked: %v", other, names)
		}
	}
}

// A Composer `path` repository is a place in the clone. Its pattern is
// judged before it is expanded: one that leaves the clone is not expanded
// at all (it listed directories of the host), and the directories a
// pattern matches are bounded.
func TestComposerPathRepositoriesStayInTheClone(t *testing.T) {
	outside := t.TempDir()
	for _, d := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(outside, "pkgs", d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(outside, "pkgs", d, "composer.json"), []byte(`{"name":"out/`+d+`"}`), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root := writeTree(t, map[string]string{
		"app/composer.json":          `{}`,
		"packages/one/composer.json": `{"name":"Acme/One"}`,
		"packages/two/composer.json": `{"name":"acme/two"}`,
	})
	rel, err := filepath.Rel(filepath.Join(root, "app"), filepath.Join(outside, "pkgs"))
	if err != nil {
		t.Fatal(err)
	}
	var globbed []string
	old := composerExpand
	composerExpand = func(root, pattern string) []string {
		globbed = append(globbed, pattern)
		return old(root, pattern)
	}
	t.Cleanup(func() { composerExpand = old })
	repos := `[{"type":"path","url":"../packages/*"},{"type":"path","url":"` + filepath.ToSlash(rel) + `/*"},{"type":"path","url":"../../../../../../../../*/*/*"},{"type":"path","url":"../packages/../../*"}]`
	got := composerOwnRepositoryPackages(root, filepath.Join(root, "app"), []byte(repos))
	if !got["acme/one"] || !got["acme/two"] || len(got) != 2 {
		t.Errorf("packages = %v; want the clone's two", got)
	}
	for _, p := range globbed {
		if !strings.HasPrefix(p, root+string(filepath.Separator)) {
			t.Errorf("a pattern outside the clone was expanded: %s", p)
		}
	}
	if len(globbed) != 1 {
		t.Errorf("%d patterns expanded, want the one inside the clone: %v", len(globbed), globbed)
	}
	// The matches of one pattern are bounded.
	many := map[string]string{"app/composer.json": `{}`}
	for i := 0; i < composerPathMatchLimit+50; i++ {
		many[filepath.Join("pk", "p"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+strings.Repeat("y", i/26), "composer.json")] = `{"name":"m/p` + strings.Repeat("z", i) + `"}`
	}
	root2 := writeTree(t, many)
	if got := composerOwnRepositoryPackages(root2, filepath.Join(root2, "app"), []byte(`[{"type":"path","url":"../pk/*"}]`)); len(got) > composerPathMatchLimit {
		t.Errorf("%d packages read from one pattern; the bound is %d", len(got), composerPathMatchLimit)
	}
}
