// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

// Review round 6 (M6-11). A pubspec dependency's `version:` with nothing
// after it has its value on the next, deeper line (round 5). That line is
// the value only when it is a plain scalar: the next line of a block that
// is a KEY of its own, or a list item, is not a version. 0.29.212 took
// whatever stood there ("ref:", "hosted:", "-") and, having taken it, never
// read the line as the source it names.
func TestAPubspecVersionOnTheNextLineIsAPlainScalarOnly(t *testing.T) {
	content := "dependencies:\n" +
		"  plain:\n    version:\n      ^1.2.3\n" +
		"  quoted:\n    version:\n      \"2.0.0\"\n" +
		"  keyed:\n    version:\n      ref: v1\n" +
		"  sourced:\n    version:\n    path: ../sourced\n" +
		"  deepsource:\n    version:\n      path: ../deep\n" +
		"  hostedthen:\n    version:\n      hosted: https://my.example\n" +
		"  listed:\n    version:\n      - 1.0.0\n" +
		"  dash:\n    version:\n      -\n" +
		"  empty:\n    version:\n" +
		"  after: 9.9.9\n"
	got := map[string]struct {
		version     string
		nonRegistry bool
	}{}
	for _, d := range parsePubspecVersions(content) {
		got[d.Name] = struct {
			version     string
			nonRegistry bool
		}{d.Version, d.NonRegistry}
	}
	for name, want := range map[string]struct {
		version     string
		nonRegistry bool
	}{
		"plain":      {"1.2.3", false},
		"quoted":     {"2.0.0", false},
		"keyed":      {"", false},
		"sourced":    {"", true},
		"deepsource": {"", true},
		"hostedthen": {"", true},
		"listed":     {"", false},
		"dash":       {"", false},
		"empty":      {"", false},
		"after":      {"9.9.9", false},
	} {
		if g, ok := got[name]; !ok || g != want {
			t.Errorf("%s: version %q, non-registry %v (read: %v); want %q, %v", name, g.version, g.nonRegistry, ok, want.version, want.nonRegistry)
		}
	}
}

// Review round 6 (M6-13). A block that opens and closes on its line may be
// followed by more on the same line (`group :test do gem 'a' end; gem 'b'`):
// the block is closed, its gem's requirement does not hold the `end`, and
// what follows the `;` is a declaration outside it.
func TestAOneLineGemBlockFollowedByMoreOnItsLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Gemfile")
	content := "group :test do gem 'ee', '1.0' end; gem 'ff', '2.0'\n" +
		"gem 'gg', '3.0'\n" +
		"if ENV['X'] then gem 'hh' end; gem 'ii', '4.0'; gem 'jj'\n" +
		"group(:development) { gem 'kk', '5.0' }; gem 'll', '6.0'\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	type row struct{ version, scope, requirement string }
	got := map[string]row{}
	for _, d := range ParseGemfileVersions(path) {
		got[d.Name] = row{d.Version, d.Type, d.Requirement}
	}
	for name, want := range map[string]row{
		"ee": {"1.0", "test", "gem 'ee', '1.0'"},
		"ff": {"2.0", "runtime", "gem 'ff', '2.0'"},
		"gg": {"3.0", "runtime", "gem 'gg', '3.0'"},
		"hh": {"", "runtime", "gem 'hh'"},
		"ii": {"4.0", "runtime", "gem 'ii', '4.0'"},
		"jj": {"", "runtime", "gem 'jj'"},
		"kk": {"5.0", "dev", "gem 'kk', '5.0'"},
		"ll": {"6.0", "runtime", "gem 'll', '6.0'"},
	} {
		if got[name] != want {
			t.Errorf("%s: %+v, want %+v", name, got[name], want)
		}
	}
	if len(got) != 8 {
		t.Errorf("%d gems read: %v", len(got), got)
	}
}
