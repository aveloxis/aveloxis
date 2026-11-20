// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// The stored requirement is the declaration without its comment or a
// trailing modifier; keyword options stay (the raw manifest truth the
// display shows, v0.27.11). The CLASSIFIER reads the call through its last
// version literal (manifest.ClassificationText; item 71 review round 2 — taken by
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
		"json":         {`gem "json", ">= 1.5"`, versionuse.RangeFloor, "runtime"},
		"z":            {`gem 'z', '>= 2.0'`, versionuse.RangeFloor, "runtime"},
		"github-pages": {`gem "github-pages"`, versionuse.Unpinned, "runtime"},
		"rails":        {`gem 'rails', '~> 7.0'`, versionuse.BoundedRange, "runtime"},
		"pg":           {`gem "pg", ">= 1.1", require: false`, versionuse.RangeFloor, "runtime"},
		"diffy":        {`gem "diffy", ">= 3.0", if: :x`, versionuse.RangeFloor, "runtime"},
		"motif":        {`gem "motif", ">= 1.0", platforms: :motif`, versionuse.RangeFloor, "runtime"},
		"rocket":       {`gem 'rocket', '1.2.3', :require => false`, versionuse.Exact, "runtime"},
		"lambda":       {`gem 'lambda', '2.0.0', install_if: -> { RUBY_VERSION < "3.0" }`, versionuse.Exact, "runtime"},
		"mri":          {`gem 'mri', '3.1.0', platforms: :mri_19`, versionuse.Exact, "runtime"},
		"testonly":     {`gem 'testonly', '1.0.0', group: :test`, versionuse.Exact, "test"},
	}
	deps := manifest.ParseGemfileVersions(path)
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
		if got := versionuse.ClassifyRequirement(manifest.ClassificationText("rubygems", d.Requirement), d.Version); got != w.class {
			t.Errorf("%s: classifyRequirement = %q, want %q", d.Name, got, w.class)
		}
		if d.Type != w.scope {
			t.Errorf("%s: scope %q, want %q (the inline group is read from the full call)", d.Name, d.Type, w.scope)
		}
	}
}
