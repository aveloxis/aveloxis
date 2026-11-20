// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5: the readers of one file (M5-9, M5-10, and the shapes the
// review listed as still unread).

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

type r5Gem struct {
	version, scope string
	nonRegistry    bool
}

func r5Gemfile(t *testing.T, gemfile string) (map[string]r5Gem, map[string]string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "Gemfile")
	if err := os.WriteFile(path, []byte(gemfile), 0o644); err != nil {
		t.Fatal(err)
	}
	got, requirement := map[string]r5Gem{}, map[string]string{}
	for _, d := range ParseGemfileVersions(path) {
		got[d.Name] = r5Gem{d.Version, d.Type, d.NonRegistry}
		requirement[d.Name] = d.Requirement
	}
	return got, requirement
}

func r5SameGems(t *testing.T, what string, got, want map[string]r5Gem) {
	t.Helper()
	for name, w := range want {
		if g, ok := got[name]; !ok || g != w {
			t.Errorf("%s: %s is %+v (read: %v), want %+v", what, name, g, ok, w)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s: %s was read and is not a gem of the file", what, name)
		}
	}
}

// The one-line and block shapes the review found unread: a `case` with its
// arms on one line; a `do` followed by a semicolon; a multi-line block
// closed by an `end` with a modifier; a block in braces.
func TestGemfileBlockShapesOfRound5(t *testing.T) {
	got, requirement := r5Gemfile(t, `source 'https://rubygems.org'
case RUBY_ENGINE when 'ruby' then gem 'c1', '1.0' when 'jruby' then gem 'c2' else gem 'c3', '3.0' end
gem 'after0'
group :test do; gem 'semi1'; gem 'semi2', '2.0'; end
gem 'after1'
group :test do
  gem 'multi'
end if ENV['CI']
gem 'after2'
group(:development) { gem 'brace1', '1.5'; gem 'brace2' }
gem 'after3'
path('vendor/x') { gem 'brace3' }
gem 'after4'
`)
	r5SameGems(t, "round 5 shapes", got, map[string]r5Gem{
		"c1": {"1.0", "runtime", false}, "c2": {"", "runtime", false}, "c3": {"3.0", "runtime", false},
		"after0": {"", "runtime", false},
		"semi1":  {"", model.ScopeTest, false}, "semi2": {"2.0", model.ScopeTest, false},
		"after1": {"", "runtime", false},
		"multi":  {"", model.ScopeTest, false},
		"after2": {"", "runtime", false}, // the `end if` closed the group
		"brace1": {"1.5", model.ScopeDev, false}, "brace2": {"", model.ScopeDev, false},
		"after3": {"", "runtime", false},
		"brace3": {"", "runtime", true},
		"after4": {"", "runtime", false},
	})
	for name, r := range requirement {
		if strings.Contains(r, " end") || strings.Contains(r, "}") || strings.Contains(r, " when ") {
			t.Errorf("%s: the requirement %q holds more than the declaration", name, r)
		}
	}
}

// M5-10. Blocks nested on one line: the requirement is the declaration, and
// the `end` of the outer block is not part of it. M5-9 (R6b): each arm of a
// one-line conditional is read, the `elsif` arms too.
func TestGemfileNestedOneLineBlocksAndElsifArms(t *testing.T) {
	got, requirement := r5Gemfile(t, `group :development do group :test do gem 'n', '1.0' end end
gem 'after1'
if a then gem 'x' elsif b then gem 'y', '2.0' elsif c then gem 'w' else gem 'z', '4.0' end
gem 'after2'
`)
	r5SameGems(t, "nested and elsif", got, map[string]r5Gem{
		"n":      {"1.0", model.ScopeTest, false},
		"after1": {"", "runtime", false},
		"x":      {"", "runtime", false}, "y": {"2.0", "runtime", false}, "w": {"", "runtime", false}, "z": {"4.0", "runtime", false},
		"after2": {"", "runtime", false},
	})
	if r := requirement["n"]; r != "gem 'n', '1.0'" {
		t.Errorf("the nested gem's requirement is %q, want the declaration alone", r)
	}
}

// M5-10. A mix.exs requirement that is an interpolation is no version. And
// the review's unread shape: a tuple on a line that begins with the comma.
func TestMixExsInterpolatedVersionsAndLeadingCommas(t *testing.T) {
	deps := parseMixExsVersions(`defp deps do
  [
    {:plain, "~> 1.7.0"}
    , {:lead, "~> 2.0"}
    , {:lead_git, git: "https://example.org/x.git"}
    ,{:tight, "3.1.0", only: :test}
    {:interp, "~> #{@version}"},
    {:interp2, "#{@version}"},
    {:hash_in_comment, "~> 4.0"}, # see #{issue}
  ]
end
`)
	type row struct {
		version, scope string
		nonRegistry    bool
	}
	got := map[string]row{}
	for _, d := range deps {
		got[d.Name] = row{d.Version, d.Type, d.NonRegistry}
	}
	want := map[string]row{
		"plain": {"1.7.0", "runtime", false}, "lead": {"2.0", "runtime", false}, "lead_git": {"", "runtime", true},
		"tight": {"3.1.0", model.ScopeTest, false}, "interp": {"", "runtime", false}, "interp2": {"", "runtime", false},
		"hash_in_comment": {"4.0", "runtime", false},
	}
	for name, w := range want {
		if g, ok := got[name]; !ok || g != w {
			t.Errorf("%s is %+v (read: %v), want %+v", name, g, ok, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("read %d dependencies, want %d: %v", len(got), len(want), got)
	}
}

// The review's unread shape: a pubspec `version:` whose value is on the
// line after it. And M5-9 (R11b): a dependency's version, once read, is not
// replaced by a later key of the same name deeper in its block.
func TestPubspecBlockVersions(t *testing.T) {
	deps := parsePubspecVersions(`name: app
dependencies:
  next_line:
    hosted: https://pub.dev
    version:
      ^1.2.3
  quoted_next_line:
    version:
      ">=2.0.0 <3.0.0"
  own_then_nested:
    version: ^1.2.0
    git:
      url: https://example.org/x.git
      version: 9.9.9
  empty_then_dep:
    version:
  plain: ^4.0.0
`)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	want := map[string]string{"next_line": "1.2.3", "quoted_next_line": "2.0.0", "own_then_nested": "1.2.0", "empty_then_dep": "", "plain": "4.0.0"}
	for name, w := range want {
		if g, ok := got[name]; !ok || g != w {
			t.Errorf("%s has version %q (read: %v), want %q", name, g, ok, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("read %v, want %d dependencies", got, len(want))
	}
}

// M5-9 (R4), shown equivalent. `or` in either case ends a range: so does any
// word that is not a bound, which is why reading only the lower-case
// spelling gives the same answers. These are the answers.
func TestAnAlternativeEndsARangeInEitherCase(t *testing.T) {
	for _, v := range []string{"< 2.0.0 or > 3.0.0", "< 2.0.0 OR > 3.0.0", "< 2.0.0 Or > 3.0.0", "< 2.0.0 || > 3.0.0", "<2||>=3"} {
		if got := firstFloorOrPin(v); got != "" {
			t.Errorf("firstFloorOrPin(%q) = %q; the floor after the alternative is another range's", v, got)
		}
	}
	for v, want := range map[string]string{">= 1.0 AND < 2.0": ">=1.0", ">= 1.0 and < 2.0": ">=1.0", ">= 1.2 OR >= 3.0": ">=1.2"} {
		if got := firstFloorOrPin(v); got != want {
			t.Errorf("firstFloorOrPin(%q) = %q, want %q", v, got, want)
		}
	}
}

// M5-9 (R14, R9). A Composer path pattern that leaves the clone is not
// expanded, by the expansion itself (its callers check too); and what it
// returns is in order, whatever order the directory was listed in.
func TestComposerExpansionStaysInTheCloneAndIsOrdered(t *testing.T) {
	top := t.TempDir()
	root := filepath.Join(top, "clone")
	for _, d := range []string{"clone/packages", "outside/a", "outside/b"} {
		if err := os.MkdirAll(filepath.Join(top, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, pattern := range []string{filepath.Join(root, "..", "outside", "*"), filepath.Join(root, "..", "outside", "a"), filepath.Join(root, ".."), filepath.Join(top, "outside", "*")} {
		if got := composerExpandInClone(root, pattern); got != nil {
			t.Errorf("composerExpandInClone(%q) = %v; the pattern leaves the clone and must not be expanded", pattern, got)
		}
	}
	// Names whose order in a directory listing is not their sorted order on
	// the filesystems this runs on (a hash of the name, or creation order).
	const n = 300
	for i := 0; i < n; i++ {
		if err := os.Mkdir(filepath.Join(root, "packages", fmt.Sprintf("p%04d", (i*7919)%n)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got := composerExpandInClone(root, filepath.Join(root, "packages", "*"))
	if len(got) != n {
		t.Fatalf("%d directories matched, want %d", len(got), n)
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("the expansion is not in order: %v ...", got[:5])
	}
}
