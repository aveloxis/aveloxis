// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"
)

// Item 71: the Gemfile reader took the text up to the first comma as the gem
// name, so a trailing `if`/`unless` modifier or an interpolated name became a
// fabricated gem — production libyear lines named
// `json" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9`,
// `github-pages" if ENV["GH_PAGES"]` and `beaker-#{ENV['BEAKER_HYPERVISOR']}`.
// The name is the first argument's string literal, exactly as Ruby reads it.
func TestGemDeclaredNameReadsTheStringLiteral(t *testing.T) {
	for _, tc := range []struct{ line, want string }{
		// Production shapes.
		{`gem "json" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9'`, "json"},
		{`gem "json", ">= 1.5" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9'`, "json"},
		{`gem "github-pages" if ENV["GH_PAGES"]`, "github-pages"},
		{`gem 'rake' unless ENV['CI']`, "rake"},
		// Interpolated: not a static name, so no gem at all.
		{`gem "beaker-#{ENV['BEAKER_HYPERVISOR'] || 'docker'}"`, ""},
		{`gem "beaker-#{ENV["BEAKER_HYPERVISOR"]}", *location_for(ENV['BEAKER_VERSION'])`, ""},
		{`gem %Q(beaker-#{hv})`, ""},
		// A single-quoted `#{` is literal text in Ruby, not interpolation.
		{`gem 'odd#{x}'`, "odd#{x}"},
		// Ordinary shapes.
		{`gem 'rails', '~> 7.0'`, "rails"},
		{`gem "pg", ">= 0.18", "< 2.0"`, "pg"},
		{`gem 'debug', platforms: %i[mri windows]`, "debug"},
		{`gem "nokogiri" # comment`, "nokogiri"},
		{`  gem "rspec"`, "rspec"},
		{"gem\t'tabbed'", "tabbed"},
		{`gem"nospace"`, "nospace"},
		{`gem("paren", "1.0")`, "paren"},
		{`gem %q(percent-q)`, "percent-q"},
		{`gem %q{brace}, '1.0'`, "brace"},
		{`gem "hash#tag", "1.0"`, "hash#tag"},
		// Not a declaration.
		{`gemspec`, ""},
		{`gems "x"`, ""},
		{`gem_name "x"`, ""},
		{`gem`, ""},
		{`# gem "old"`, ""},
		{`gem name_var`, ""},
		{`gem ENV['GEM']`, ""},
		{`gem ""`, ""},
		{`gem "unterminated`, ""},
		{`gem %q(unterminated`, ""},
	} {
		if got := gemDeclaredName(tc.line); got != tc.want {
			t.Errorf("gemDeclaredName(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

// An empty literal declares no gem, for either reader.
func TestGemDeclarationEmptyNameDeclaresNothing(t *testing.T) {
	if d, ok := parseGemDeclaration(`gem "", "1.0"`); ok {
		t.Errorf("parseGemDeclaration(empty name) = %+v, true; want false", d)
	}
	path := writeManifest(t, "Gemfile", "gem \"\", \"1.0\"\ngem 'rails'\n")
	assertDeps(t, "parseGemfileVersions", parseGemfileVersions(path), []string{"rails@"})
}

// The version requirements are the string literals that follow the name; a
// modifier, keyword options or a computed argument end them.
func TestParseGemDeclarationRequirements(t *testing.T) {
	for _, tc := range []struct {
		line string
		want []string
	}{
		{`gem "json", ">= 1.5" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9'`, []string{">= 1.5"}},
		{`gem "pg", ">= 0.18", "< 2.0"`, []string{">= 0.18", "< 2.0"}},
		{`gem 'rails', '~> 7.0', require: false`, []string{"~> 7.0"}},
		{`gem 'debug', platforms: %i[mri windows]`, nil},
		{`gem 'x', ENV['X_VERSION'] || '1.0'`, nil},
		{`gem 'x', "~> #{ver}"`, nil},
		{`gem "nokogiri" # comment, "1.0"`, nil},
	} {
		d, ok := parseGemDeclaration(tc.line)
		if !ok {
			t.Errorf("parseGemDeclaration(%q) declared nothing", tc.line)
			continue
		}
		if strings.Join(d.Requirements, "|") != strings.Join(tc.want, "|") {
			t.Errorf("parseGemDeclaration(%q).Requirements = %q, want %q", tc.line, d.Requirements, tc.want)
		}
	}
}

const gemfileItem71 = `source "https://rubygems.org"
gem 'rails', '~> 7.0'
gem "pg", ">= 0.18", "< 2.0"
gem 'debug', platforms: %i[mri windows]
gem "nokogiri" # comment
gem "json", ">= 1.5" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9'
gem "github-pages" if ENV["GH_PAGES"]
gem "beaker-#{ENV['BEAKER_HYPERVISOR'] || 'docker'}"

group :test do
  gem "rspec", "~> 3.12"
end
`

func TestGemfileBothReadersUseTheLiteralName(t *testing.T) {
	assertNames(t, "parseGemfile", parseGemfile(gemfileItem71),
		[]string{"rails", "pg", "debug", "nokogiri", "json", "github-pages", "rspec"})

	path := writeManifest(t, "Gemfile", gemfileItem71)
	deps := parseGemfileVersions(path)
	assertDeps(t, "parseGemfileVersions", deps,
		[]string{"rails@7.0", "pg@0.18", "debug@", "nokogiri@", "json@1.5", "github-pages@", "rspec@3.12"})
	for _, d := range deps {
		if d.Name == "rspec" && d.Type != "test" {
			t.Errorf("rspec inside `group :test do` has scope %q, want test", d.Type)
		}
		if d.Name != "rspec" && d.Type != "runtime" {
			t.Errorf("%s outside any group has scope %q, want runtime", d.Name, d.Type)
		}
	}
}

// The stored requirement is the declaration without its comment or a
// trailing modifier; keyword options stay (the raw manifest truth the
// display shows, v0.27.11). The CLASSIFIER reads the call through its last
// version literal (classificationText; item 71 review round 2 — taken by
// the parent): `:require => false` read as a `>` lower bound, and option
// bodies (`install_if: -> { RUBY_VERSION < "3.0" }`, `platforms: :mri_19`)
// carried version-like text. The scope the options carry is read from the
// full call.
func TestGemfileRequirementStopsAtTheDeclaration(t *testing.T) {
	path := writeManifest(t, "Gemfile", `gem "json", ">= 1.5" if defined?(RUBY_VERSION) && RUBY_VERSION < '1.9'
gem 'z', '>= 2.0' unless RUBY_VERSION =~ /^1/
gem "github-pages" if ENV["GH_PAGES"]
gem 'rails', '~> 7.0' # pinned
gem "pg", ">= 1.1", require: false if RUBY_ENGINE == "ruby"
gem "diffy", ">= 3.0", if: :x
gem "motif", ">= 1.0", platforms: :motif
gem 'rocket', '1.2.3', :require => false
gem 'lambda', '2.0.0', install_if: -> { RUBY_VERSION < "3.0" }
gem 'mri', '3.1.0', platforms: :mri_19
gem 'testonly', '1.0.0', group: :test
`)
	want := map[string]struct{ req, class, scope string }{
		"json":         {`gem "json", ">= 1.5"`, resolutionRangeFloor, "runtime"},
		"z":            {`gem 'z', '>= 2.0'`, resolutionRangeFloor, "runtime"},
		"github-pages": {`gem "github-pages"`, resolutionUnpinned, "runtime"},
		"rails":        {`gem 'rails', '~> 7.0'`, resolutionBoundedRange, "runtime"},
		"pg":           {`gem "pg", ">= 1.1", require: false`, resolutionRangeFloor, "runtime"},
		"diffy":        {`gem "diffy", ">= 3.0", if: :x`, resolutionRangeFloor, "runtime"},
		"motif":        {`gem "motif", ">= 1.0", platforms: :motif`, resolutionRangeFloor, "runtime"},
		"rocket":       {`gem 'rocket', '1.2.3', :require => false`, resolutionExact, "runtime"},
		"lambda":       {`gem 'lambda', '2.0.0', install_if: -> { RUBY_VERSION < "3.0" }`, resolutionExact, "runtime"},
		"mri":          {`gem 'mri', '3.1.0', platforms: :mri_19`, resolutionExact, "runtime"},
		"testonly":     {`gem 'testonly', '1.0.0', group: :test`, resolutionExact, "test"},
	}
	deps := parseGemfileVersions(path)
	if len(deps) != len(want) {
		t.Fatalf("got %d deps, want %d: %+v", len(deps), len(want), deps)
	}
	for _, d := range deps {
		w, ok := want[d.Name]
		if !ok {
			t.Errorf("unexpected dep %q", d.Name)
			continue
		}
		if d.Requirement != w.req {
			t.Errorf("%s: Requirement = %q, want %q", d.Name, d.Requirement, w.req)
		}
		if got := classifyRequirement(classificationText("rubygems", d.Requirement), d.Version); got != w.class {
			t.Errorf("%s: classifyRequirement = %q, want %q", d.Name, got, w.class)
		}
		if d.Type != w.scope {
			t.Errorf("%s: scope %q, want %q (the inline group is read from the full call)", d.Name, d.Type, w.scope)
		}
	}
}

// TestClassificationTextLeavesOtherManagersAlone: only a rubygems
// requirement is re-read; every other manager's stored text is classified
// as stored, and a rubygems text the parser cannot read falls back to it.
func TestClassificationTextLeavesOtherManagersAlone(t *testing.T) {
	for _, tc := range []struct{ manager, req, want string }{
		{"npm", "^4.18.0", "^4.18.0"},
		{"pypi", `numpy = {version = "*", extras = ["dev"]}`, `numpy = {version = "*", extras = ["dev"]}`},
		{"rubygems", `gem 'rocket', '1.2.3', :require => false`, `gem 'rocket', '1.2.3'`},
		{"rubygems", "not a gem call", "not a gem call"},
	} {
		if got := classificationText(tc.manager, tc.req); got != tc.want {
			t.Errorf("classificationText(%q, %q) = %q; want %q", tc.manager, tc.req, got, tc.want)
		}
	}
}
