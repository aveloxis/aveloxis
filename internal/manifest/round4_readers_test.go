// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 4 of the vulnerability program's first part: the readers of
// one file.

// R4-M11. A Gemfile block that opens and closes on its line is read whatever
// stands between: several gems, the two arms of a conditional, a call with
// parentheses, a modifier after the `end`. And it leaves nothing open,
// whether or not a gem in it could be read (R4-M6, M15b).
func TestOneLineGemfileBlocks(t *testing.T) {
	gemfile := `source 'https://rubygems.org'
group :test do gem 'x' end if ENV['T']
gem 'after1'
path 'vendor' do gem 'a'; gem 'b', '2.0' end
if ENV['Z'] then gem 'c' else gem 'd', '1.1' end
group :development do gem('p','1.0') end
group :test do gem name_of_it end
gem 'after2'
group :test do gem "q's", "~> 3.1" end # a note; with a semicolon
gem 'after3'
`
	path := filepath.Join(t.TempDir(), "Gemfile")
	if err := os.WriteFile(path, []byte(gemfile), 0o644); err != nil {
		t.Fatal(err)
	}
	type row struct {
		version, scope string
		nonRegistry    bool
	}
	got := map[string]row{}
	var order []string
	for _, d := range ParseGemfileVersions(path) {
		got[d.Name] = row{d.Version, d.Type, d.NonRegistry}
		order = append(order, d.Name)
	}
	want := map[string]row{
		"x":      {"", model.ScopeTest, false},
		"after1": {"", "runtime", false}, // the group closed on its line
		"a":      {"", "runtime", true},
		"b":      {"2.0", "runtime", true},
		"c":      {"", "runtime", false},
		"d":      {"1.1", "runtime", false},
		"p":      {"1.0", model.ScopeDev, false},
		"after2": {"", "runtime", false}, // a block with no readable gem closed too
		"q's":    {"3.1", model.ScopeTest, false},
		"after3": {"", "runtime", false},
	}
	if len(got) != len(want) {
		t.Errorf("read %v; want %d gems", order, len(want))
	}
	for name, w := range want {
		if g, ok := got[name]; !ok || g != w {
			t.Errorf("gem %q: %+v (read %v); want %+v", name, g, ok, w)
		}
	}
}

// R4-M12. The floor of a range that leads with an upper bound is found when
// it carries a Composer stability flag, a `v` in either case, or follows an
// AND in capitals.
func TestAFloorAfterAnUpperBoundIsFoundInItsOtherSpellings(t *testing.T) {
	for requirement, want := range map[string]string{
		"<2.0 >=1.0@dev":   "1.0@dev",
		"<2.0,>=1.0@beta":  "1.0@beta",
		"<2 V1.0":          "V1.0",
		"<2 v1.0":          "v1.0",
		"<2.0 AND >=1.0":   "1.0",
		"<2.0 and >=1.0":   "1.0",
		"<2.0 OR >=3.0":    "", // another alternative: not this range's floor
		"<2.0 >=1.0@":      "", // not a flag
		"<2.0 >=1.0@dev@x": "",
		"<2.0 # was 1.0":   "",
		"<2.0":             "",
	} {
		if got := CleanVersion(requirement); got != want {
			t.Errorf("CleanVersion(%q) = %q, want %q", requirement, got, want)
		}
	}
}

// Aside of round 4. A mix.exs requirement is what its quotes hold: a comment
// after the tuple was read into it (`1.0"}`).
func TestAMixRequirementEndsAtItsQuote(t *testing.T) {
	deps := parseMixExsVersions(`  defp deps do
    [
      {:c, "~> 1.0"} # comment
      {:d, "~> 2.1"}, # the HTTP client, see "notes"
      {:e, "~> 3.0", only: :test},
      {:f, github: "o/f"}
    ]
  end
`)
	want := map[string]string{"c": "1.0", "d": "2.1", "e": "3.0", "f": ""}
	if len(deps) != len(want) {
		t.Fatalf("%d dependencies, want %d: %+v", len(deps), len(want), deps)
	}
	for _, d := range deps {
		if d.Version != want[d.Name] {
			t.Errorf("%s: version %q, want %q", d.Name, d.Version, want[d.Name])
		}
	}
}

// Aside of round 4. A pubspec dependency written as a block has its version
// on a line of its own; it had none. Which server it comes from is decided
// as before, by `hosted:`.
func TestAPubspecBlockDependencyHasItsVersion(t *testing.T) {
	deps := parsePubspecVersions(`name: app
dependencies:
  from_pub:
    hosted: https://pub.dev
    version: ^1.2.0
  from_elsewhere:
    hosted:
      name: from_elsewhere
      url: https://packages.example.com
    version: ">=2.0.0 <3.0.0" # a note
  plain: ^4.0.0
  local:
    path: ../local
dev_dependencies:
  version: ^9.9.9
`)
	type row struct {
		version     string
		nonRegistry bool
	}
	want := map[string]row{
		"from_pub":       {"1.2.0", false},
		"from_elsewhere": {"2.0.0", true},
		"plain":          {"4.0.0", false},
		"local":          {"", true},
		"version":        {"9.9.9", false}, // a package of that name, at dependency level
	}
	if len(deps) != len(want) {
		t.Fatalf("%d dependencies, want %d: %+v", len(deps), len(want), deps)
	}
	for _, d := range deps {
		if w := want[d.Name]; d.Version != w.version || d.NonRegistry != w.nonRegistry {
			t.Errorf("%s: version %q, not from the registry %v; want %q, %v", d.Name, d.Version, d.NonRegistry, w.version, w.nonRegistry)
		}
	}
}

// R4-M6 (M11b). A FIXED element of a Composer path pattern that is a link
// out of the clone is not followed, with or without a wildcard after it;
// one that stays in the clone is.
func TestAFixedElementOfAComposerPatternStaysInTheClone(t *testing.T) {
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outside, "secret"), 0o755); err != nil {
		t.Fatal(err)
	}
	root := writeTree(t, map[string]string{"pk/real/a/composer.json": `{"name":"in/a"}`})
	if err := os.Symlink(outside, filepath.Join(root, "pk", "out")); err != nil {
		t.Fatalf("creating a symbolic link: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "pk", "real"), filepath.Join(root, "pk", "alias")); err != nil {
		t.Fatalf("creating a symbolic link: %v", err)
	}
	for _, pattern := range []string{filepath.Join("pk", "out", "*"), filepath.Join("pk", "out"), filepath.Join("pk", "*", "..", "out", "*")} {
		if got := composerExpandInClone(root, filepath.Join(root, pattern)); len(got) != 0 {
			t.Errorf("%s: %v; a link out of the clone was followed", pattern, got)
		}
	}
	if got := composerExpandInClone(root, filepath.Join(root, "pk", "alias", "*")); len(got) != 1 {
		t.Errorf("pk/alias/*: %v; want the one package through the link inside the clone", got)
	}
	if got := composerExpandInClone(root, filepath.Join(root, "pk", "real", "a", "composer.json")); len(got) != 0 {
		t.Errorf("a file is not a directory of packages: %v", got)
	}
}
