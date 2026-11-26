// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

// A gem written on the line that OPENS a block which stays open (`group
// :test do gem 'x'`, more lines, `end`) is a declaration in that block
// (round 5: it was not read at all). The block still closes at its `end`.
func TestGemOnTheLineThatOpensABlockIsReadInThatBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Gemfile")
	content := "group :test do gem 'first', '1.0'; gem 'second'\n" +
		"  gem 'third', '3.0'\n" +
		"end\n" +
		"if ENV['X'] then gem 'cond', '2.0'\n" +
		"  gem 'cond2'\n" +
		"end\n" +
		"group :development do\n" +
		"  path 'vendor' do |p| gem 'local'\n" +
		"  end\n" +
		"  gem 'stilldev'\n" +
		"end\n" +
		"gem 'after', '9.0'\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	type row struct {
		version, scope string
		nonRegistry    bool
	}
	got := map[string]row{}
	for _, d := range ParseGemfileVersions(path) {
		got[d.Name] = row{d.Version, d.Type, d.NonRegistry}
	}
	want := map[string]row{
		"first":  {"1.0", "test", false},
		"second": {"", "test", false},
		"third":  {"3.0", "test", false},
		"cond":   {"2.0", "runtime", false},
		"cond2":  {"", "runtime", false},
		"local":  {"", "dev", true},
		// The inner block's `end` closed the inner block, not the group.
		"stilldev": {"", "dev", false},
		"after":    {"9.0", "runtime", false},
	}
	if len(got) != len(want) {
		t.Errorf("read %d gems, want %d: %v", len(got), len(want), got)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: %+v, want %+v", name, got[name], w)
		}
	}
}
