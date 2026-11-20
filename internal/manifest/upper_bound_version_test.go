// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"testing"
)

// An upper bound or an exclusion alone says nothing about the version in
// use (operator, 2026-11-19: "Treat all three as version not determined":
// `<`, `<=` and `!=`). The readers stripped the operator and stored the
// bound as the version: `rich = "<13.5.0"` was recorded as using 13.5.0,
// which it excludes, and `tokenizers = "!=0.12.0"` as using the one version
// it forbids. One production database held about 1,580 such rows with
// about 6,550 open findings reported at those versions (review round 2).
func TestCleanVersionTakesNoVersionFromAnUpperBoundOrExclusion(t *testing.T) {
	for in, want := range map[string]string{
		// Alone: no version.
		"<2.0":       "",
		"< 2.0":      "",
		"<=1.8.0":    "",
		"<= 1.8.0":   "",
		"!=0.12.0":   "",
		"!= 0.12.0":  "",
		"<2.0,!=1.5": "",
		"<2.0 !=1.5": "",
		// With a floor or a pin anywhere in the range: that one.
		"<2.0,>=1.0":           "1.0",
		"< 2.0, >= 1.0":        "1.0",
		"!=1.5,>=1.0,<2.0":     "1.0",
		"<0.12.0 >=0.11.1":     "0.11.1",
		"< 5.0.0 and ~> 3.0.0": "3.0.0",
		"!=1.5,==1.4":          "1.4",
		// Unchanged: everything that was right.
		">=1.0,<2.0":           "1.0",
		">=0.11.1 <0.12.0":     "0.11.1",
		"^1.2.3":               "1.2.3",
		"~> 1.2":               "1.2",
		"~=3.4":                "3.4",
		"==1.0":                "1.0",
		"1.2.3":                "1.2.3",
		"1.2.3 - 2.3.4":        "1.2.3",
		"^1 || ^2":             "1",
		"~> 3.0.0 and < 5.0.0": "3.0.0",
		">1.0":                 "1.0",
		"=1.0":                 "1.0",
		"*":                    "*",
		"":                     "",
	} {
		if got := CleanVersion(in); got != want {
			t.Errorf("CleanVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

// The same through each reader that had it, as the files are written.
func TestReadersTakeNoVersionFromAnUpperBoundOrExclusion(t *testing.T) {
	root := writeTree(t, map[string]string{
		"requirements.txt": "torch<=1.8.0\ncogapp < 3.6.0\ntokenizers!=0.12.0\nnumpy>=1.20,<2\npandas<3,>=1.5\nflask==2.0\n",
		"pyproject.toml":   "[tool.poetry.dependencies]\nrich = \"<13.5.0\"\ntokenizers = \"!=0.12.0\"\nclick = \"^8.0\"\nboth = \"<3,>=2.1\"\n",
		"Gemfile":          "source 'https://rubygems.org'\ngem 'xcodeproj', '< 1.26.0'\ngem 'rails', '< 5.0', '>= 4.2'\ngem 'rake', '~> 13.0'\ngem 'bad', '!= 1.2'\n",
		"pubspec.yaml":     "name: x\ndependencies:\n  foo: '<2.0.0'\n  bar: '>=1.0.0 <2.0.0'\n  baz: ^3.1.0\n",
		"composer.json":    `{"require": {"vendor/upper": "<2.0", "vendor/both": "<3.0,>=2.1", "vendor/caret": "^1.4"}}`,
		"Cargo.toml":       "[package]\nname = \"x\"\nversion = \"0.1.0\"\n[dependencies]\nupper = \"<2.0\"\ncaret = \"1.4\"\nboth = \">=1.2, <2\"\n",
		"package.json":     `{"dependencies": {"upper": "<2.0.0", "upto": "<=1.5.0", "caret": "^4.17.1", "both": ">=1.2.0 <2.0.0"}}`,
	})
	want := map[string]map[string]string{
		"requirements.txt": {"torch": "", "cogapp": "", "tokenizers": "", "numpy": "1.20", "pandas": "1.5", "flask": "2.0"},
		"pyproject.toml":   {"rich": "", "tokenizers": "", "click": "8.0", "both": "2.1"},
		"Gemfile":          {"xcodeproj": "", "rails": "4.2", "rake": "13.0", "bad": ""},
		"pubspec.yaml":     {"foo": "", "bar": "1.0.0", "baz": "3.1.0"},
		"composer.json":    {"vendor/upper": "", "vendor/both": "2.1", "vendor/caret": "1.4"},
		"Cargo.toml":       {"upper": "", "caret": "1.4", "both": "1.2"},
		"package.json":     {"upper": "", "upto": "", "caret": "4.17.1", "both": "1.2.0"},
	}
	for file, deps := range want {
		got := map[string]string{}
		for _, d := range NewWalk(root, nil).Read(filepath.Join(root, file), Options{}).Deps {
			got[d.Name] = d.Version
		}
		for name, version := range deps {
			if v, ok := got[name]; !ok || v != version {
				t.Errorf("%s: %s has version %q (present %v), want %q", file, name, v, ok, version)
			}
		}
	}
}
