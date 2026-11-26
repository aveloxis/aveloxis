// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A setup.py reader takes install_requires' OWN list and nothing after it
// (found 0.29.153; summary/70, top of 4.2). Before 0.29.163 the readers took
// everything from the list's opening bracket to the end of the line, so a
// setup() call written on one line stored its extras, and the extra's NAME,
// as runtime requirements: `extras_require={'dev': ['black']}` made "dev" a
// dependency, and "dev" is a different package on PyPI (SR-6).

func scoped(deps []model.LibyearDep) string {
	var out []string
	for _, d := range deps {
		out = append(out, d.Name+":"+d.Type)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func TestSetupPyReadersStopAtTheEndOfInstallRequires(t *testing.T) {
	cases := []struct {
		name, content string
		runtime       string // names the base readers must return, sorted
		devBuild      string // name:scope the dev/build reader must return
	}{
		{"both keywords on one line",
			"setup(install_requires=['click>=8'], extras_require={'dev': ['black']})\n",
			"click", "black:optional"},
		{"three keywords on one line",
			"setup(name='x', install_requires=['click>=8', 'requests'], tests_require=['pytest'], extras_require={'docs': ['sphinx']})\n",
			"click requests", "pytest:test sphinx:optional"},
		{"the closing line carries the next keyword",
			"setup(\n    install_requires=[\n        'click>=8',\n        'requests'], extras_require={'dev': ['black']},\n)\n",
			"click requests", "black:optional"},
		{"an extra's brackets inside a requirement",
			"setup(install_requires=['celery[redis]>=5.0', 'flask'], extras_require={'dev': ['black']})\n",
			"celery flask", "black:optional"},
		{"two lists joined with +",
			"setup(install_requires=['click'] + ['requests'], extras_require={'dev': ['black']})\n",
			"click requests", "black:optional"},
		{"one keyword per line, as before",
			"setup(\n    install_requires=['click>=8'],\n    extras_require={'dev': ['black']},\n)\n",
			"click", "black:optional"},
		{"a multi-line list, as before",
			"setup(\n    install_requires=[\n        'click>=8',\n        # 'commented-out',\n        'requests',\n    ],\n)\n",
			"click requests", ""},
		{"no install_requires at all",
			"setup(name='x', extras_require={'dev': ['black']})\n",
			"", "black:optional"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			names, err := parseSetupPyDeps(c.content)
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(names)
			if got := strings.Join(names, " "); got != c.runtime {
				t.Errorf("names = %q, want %q", got, c.runtime)
			}
			var versioned []string
			for _, d := range parseSetupPyVersions(c.content) {
				versioned = append(versioned, d.Name)
				if d.Type != "runtime" && d.Type != "" {
					t.Errorf("%s read from install_requires with scope %q", d.Name, d.Type)
				}
			}
			sort.Strings(versioned)
			if got := strings.Join(versioned, " "); got != c.runtime {
				t.Errorf("versioned = %q, want %q", got, c.runtime)
			}
			if got := scoped(parseSetupPyDevBuildVersions(c.content)); got != c.devBuild {
				t.Errorf("dev/build = %q, want %q", got, c.devBuild)
			}
		})
	}
}

// Through the roster, as the walk reads it: with the dev/build knob on, the
// one-line setup() yields each dependency once, with its own scope.
func TestOneLineSetupPyThroughTheRoster(t *testing.T) {
	path := writeFixture(t, "setup.py", "setup(install_requires=['click>=8'], extras_require={'dev': ['black']})\n")
	w := NewWalk(pathDir(path), nil)
	off, err := w.Declared(path, Options{})
	if err != nil || scoped(off) != "click:runtime" {
		t.Errorf("knob off: %q (%v), want click:runtime", scoped(off), err)
	}
	on, err := w.Declared(path, Options{DevBuildDeps: true})
	if err != nil || scoped(on) != "black:optional click:runtime" {
		t.Errorf("knob on: %q (%v), want black:optional click:runtime", scoped(on), err)
	}
}
