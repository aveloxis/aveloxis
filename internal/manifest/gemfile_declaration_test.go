// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

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
	assertDeps(t, "parseGemfileVersions", ParseGemfileVersions(path), []string{"rails@"})
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
	deps := ParseGemfileVersions(path)
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

// TestClassificationTextLeavesOtherManagersAlone: a rubygems requirement
// and a pypi TOML table declaration are re-read (PR #218 review A8 for the
// latter); every other manager's stored text is classified as stored, and a
// text the re-reader cannot read falls back to it.
func TestClassificationTextLeavesOtherManagersAlone(t *testing.T) {
	for _, tc := range []struct{ manager, req, want string }{
		{"npm", "^4.18.0", "^4.18.0"},
		{"pypi", `numpy = {version = "*", extras = ["dev"]}`, "*"},
		{"pypi", "requests>=2.31; python_version < '3.8'", "requests>=2.31; python_version < '3.8'"},
		{"pypi", "requests == 2.31", "requests == 2.31"},
		{"cargo", `serde = { version = "1.0", features = ["derive"] }`, `serde = { version = "1.0", features = ["derive"] }`},
		{"rubygems", `gem 'rocket', '1.2.3', :require => false`, `gem 'rocket', '1.2.3'`},
		{"rubygems", "not a gem call", "not a gem call"},
	} {
		if got := ClassificationText(tc.manager, tc.req); got != tc.want {
			t.Errorf("classificationText(%q, %q) = %q; want %q", tc.manager, tc.req, got, tc.want)
		}
	}
}
