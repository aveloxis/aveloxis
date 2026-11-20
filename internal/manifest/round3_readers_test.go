// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Review round 3, the readers.

// After an upper bound only a BOUND may supply the version: not a word of a
// comment, not a clause of an environment marker; and not a floor of
// another alternative (`||`, hex's `or`), since which alternative is in use
// is not known.
func TestOnlyABoundOfTheFirstAlternativeSuppliesAVersion(t *testing.T) {
	for in, want := range map[string]string{
		`<2.0.0' # pinned until 3`:       "",
		`<2.0.0 # was 1.0`:               "",
		`<1.0 ; python_version >= "3.8"`: "",
		`<1.0; python_version >= "3.8"`:  "",
		`< 2.0.0 or > 3.0.0`:             "",
		`< 2.0.0 or >= 3.0.0`:            "",
		`<2.0.0 || >=3.0.0`:              "",
		`<2.0.0||>=3.0.0`:                "",
		`!= 1.5 || 1.4`:                  "",
		`<2 see notes 1.9`:               "",
		// A floor or a pin of the SAME alternative still is one.
		`< 5.0.0 and ~> 3.0.0`:      "3.0.0",
		`<2.0.0 >=1.0.0 || >=3.0.0`: "1.0.0",
		`<=1.5.0 >=1.0.0`:           "1.0.0",
		`<3, v2.1`:                  "v2.1",
		`!=1.5,==1.4`:               "1.4",
		// And a range that does not lead with an upper bound is untouched.
		`~> 1.0 or ~> 2.0`: "1.0",
		`>=1.0 # at least`: "1.0",
	} {
		if got := CleanVersion(in); got != want {
			t.Errorf("CleanVersion(%q) = %q, want %q", in, got, want)
		}
	}
	// Through the readers, as the files are written.
	root := writeTree(t, map[string]string{
		"pubspec.yaml": "name: x\ndependencies:\n  a: '<2.0.0' # pinned until 3\n  b: <2.0.0 # was 1.0\n  c: \">=1.2.0 <2.0.0\" # fine\n  d: ^3.1.0 # caret\n  e: any\n  f: '>=1.0.0' # see 2.0\n  g: \"1.4.2\" # exact\n",
		"mix.exs":      "defmodule X.MixProject do\n  defp deps do\n    [\n      {:b, \"< 2.0.0 or > 3.0.0\"},\n      {:c, \"~> 1.0 and < 1.5\"},\n      {:d, \"< 1.5 and ~> 1.0\"}\n    ]\n  end\nend\n",
	})
	for file, want := range map[string]map[string]string{
		// f and g: a quoted value with a comment after it. The value is
		// what the quotes hold; the rest of the line used to be handed
		// over with it, and the closing quote stayed on the version.
		"pubspec.yaml": {"a": "", "b": "", "c": "1.2.0", "d": "3.1.0", "f": "1.0.0", "g": "1.4.2"},
		"mix.exs":      {"b": "", "c": "1.0", "d": "1.0"},
	} {
		got := map[string]string{}
		for _, d := range NewWalk(root, nil).Read(filepath.Join(root, file), Options{}).Deps {
			got[d.Name] = d.Version
		}
		for name, v := range want {
			if g, ok := got[name]; !ok || g != v {
				t.Errorf("%s: %s has version %q (present %v), want %q", file, name, g, ok, v)
			}
		}
	}
}

// A block that opens and closes on one line holds a gem like any other:
// it is read, in the block's scope, and nothing stays open after it.
func TestAOneLineGemfileBlockIsRead(t *testing.T) {
	root := writeTree(t, map[string]string{"Gemfile": `source 'https://rubygems.org'
if RUBY_VERSION >= '3.0' then gem 'oneline', '1.0' end
group :test do gem 'intest', '2.0' end
gem 'after', '3.0'
group :development do
  unless ENV['CI'] then gem 'indev', '4.0' end
  path 'vendor/gems' do gem 'local', '5.0' end
  gem 'devtoo', '6.0'
end
platforms :ruby do gem 'plat', '7.0' end # a comment
gem 'last', '8.0'
if ENV['X'] then puts 'no gem here' end
gem 'verylast', '9.0'
`})
	type row struct {
		version, scope string
		nonRegistry    bool
	}
	got := map[string]row{}
	for _, d := range NewWalk(root, nil).Read(filepath.Join(root, "Gemfile"), Options{}).Deps {
		got[d.Name] = row{d.Version, d.Type, d.NonRegistry}
	}
	for name, want := range map[string]row{
		"oneline":  {"1.0", "runtime", false},
		"intest":   {"2.0", "test", false},
		"after":    {"3.0", "runtime", false}, // the one-line group did not stay open
		"indev":    {"4.0", "dev", false},
		"local":    {"5.0", "dev", true},
		"devtoo":   {"6.0", "dev", false}, // the one-line path block did not stay open
		"plat":     {"7.0", "runtime", false},
		"last":     {"8.0", "runtime", false},
		"verylast": {"9.0", "runtime", false},
	} {
		if g, ok := got[name]; !ok || g != want {
			t.Errorf("%s = %+v (present %v), want %+v", name, g, ok, want)
		}
	}
}

// Where a gem comes from is read from its `source` OPTION: a string on the
// line that holds the word cannot answer for it.
func TestAGemsSourceIsReadFromItsOption(t *testing.T) {
	root := writeTree(t, map[string]string{"Gemfile": `gem 'x', '1.0', note: " source 'https://rubygems.org'", source: 'https://gems.internal.example'
gem 'y', '1.0', note: "source: 'https://gems.internal.example'", source: 'https://rubygems.org'
gem 'z', '1.0', :source => "https://rubygems.org/"
gem 'w', '1.0', source: "https://gems.internal.example"
`})
	got := map[string]bool{}
	for _, d := range NewWalk(root, nil).Read(filepath.Join(root, "Gemfile"), Options{}).Deps {
		got[d.Name] = d.NonRegistry
	}
	for name, want := range map[string]bool{"x": true, "y": false, "z": false, "w": true} {
		if g, ok := got[name]; !ok || g != want {
			t.Errorf("%s: not from the registry = %v (present %v), want %v", name, g, ok, want)
		}
	}
}

// A Composer path repository's pattern lists nothing outside the clone,
// even through a symbolic link that a wildcard matches, and looks at a
// bounded number of directory entries.
func TestAComposerPathPatternStaysInTheCloneThroughLinks(t *testing.T) {
	outside := t.TempDir()
	for _, name := range []string{"secret-one", "secret-two"} {
		if err := os.MkdirAll(filepath.Join(outside, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(outside, name, "composer.json"), []byte(`{"name":"out/`+name+`"}`), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root := writeTree(t, map[string]string{
		"app/composer.json":       `{}`,
		"pk/real/a/composer.json": `{"name":"in/a"}`,
		"pk/real/b/composer.json": `{"name":"in/b"}`,
	})
	if err := os.Symlink(outside, filepath.Join(root, "pk", "link")); err != nil {
		t.Fatalf("creating a symbolic link: %v", err)
	}
	// A link that stays in the clone is the repository's own layout.
	if err := os.Symlink(filepath.Join(root, "pk", "real"), filepath.Join(root, "pk", "alias")); err != nil {
		t.Fatalf("creating a symbolic link: %v", err)
	}
	got := composerExpandInClone(root, filepath.Join(root, "pk", "*", "*"))
	for _, m := range got {
		real, err := filepath.EvalSymlinks(m)
		if err != nil || strings.HasPrefix(real, outside) || strings.Contains(m, "secret") {
			t.Errorf("the expansion left the clone: %s", m)
		}
	}
	if len(got) != 4 { // real/a, real/b, alias/a, alias/b
		t.Errorf("matches = %v; want the two packages, each by its real directory and by the link inside the clone", got)
	}
	pkgs := composerOwnRepositoryPackages(root, filepath.Join(root, "app"), []byte(`[{"type":"path","url":"../pk/*/*"}]`))
	if !pkgs["in/a"] || !pkgs["in/b"] || len(pkgs) != 2 {
		t.Errorf("packages = %v; want the clone's two and none from outside", pkgs)
	}

	// The work is bounded: a directory of many entries is not listed past
	// what one wildcard may look at.
	big := t.TempDir()
	for i := 0; i < composerPathMatchLimit+500; i++ {
		if err := os.Mkdir(filepath.Join(big, "d"+strings.Repeat("0", 4-len(itoa(i)))+itoa(i)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Past the bound the pattern is not expanded at all (0.29.211): which
	// of its matches were met first is the file system's order, and a
	// part of them chosen that way is a different part elsewhere.
	composerEntriesLooked.Store(0)
	if n := len(composerExpandInClone(big, filepath.Join(big, "*"))); n != 0 {
		t.Errorf("%d matches of %d directories; a pattern past what one wildcard may look at (%d) is not expanded", n, composerPathMatchLimit+500, composerPathMatchLimit)
	}
	// Entries are read in batches, so the count may pass the bound by less
	// than one batch.
	if looked := int(composerEntriesLooked.Load()); looked < composerPathMatchLimit || looked >= composerPathMatchLimit+composerReadDirBatch {
		t.Errorf("%d directory entries looked at; one wildcard may look at %d", looked, composerPathMatchLimit)
	}
	// A pattern whose every wildcard matches nothing looks at nothing more.
	deep := writeTree(t, map[string]string{"a/b/c/composer.json": `{"name":"x/y"}`})
	composerEntriesLooked.Store(0)
	if got := composerExpandInClone(deep, filepath.Join(deep, "*", "*", "*")); len(got) != 1 || composerEntriesLooked.Load() != 3 {
		t.Errorf("a/b/c through three wildcards: %v, %d entries looked at; want the one directory and three entries", got, composerEntriesLooked.Load())
	}
	if n := len(composerExpandInClone(big, filepath.Join(big, "*", "*", "*"))); n != 0 {
		t.Errorf("%d matches three levels down a tree with one", n)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for ; i > 0; i /= 10 {
		s = string(rune('0'+i%10)) + s
	}
	return s
}
