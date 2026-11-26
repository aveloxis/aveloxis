// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"sort"
	"strings"
	"testing"
)

// The packages.config readers find `<package ...>` elements wherever they
// are (found 0.29.153; summary/70, top of 4.2). Before 0.29.163 an element had
// to start its line, so a file written on one line, or minified, declared
// nothing; and the two readers disagreed about capitals (the name reader
// missed `Id="..."`, which the versioned reader took).

func TestPackagesConfigElementsAreFoundWhereverTheyAre(t *testing.T) {
	const perLine = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<packages>\n  <package id=\"Newtonsoft.Json\" version=\"13.0.1\" targetFramework=\"net48\" />\n  <package id=\"NUnit\" version=\"3.13.3\" developmentDependency=\"true\" />\n</packages>\n"
	cases := []struct {
		name, content string
		want          string // name@version:scope, sorted
	}{
		{"one element per line, as before", perLine,
			"NUnit@3.13.3:build Newtonsoft.Json@13.0.1:runtime"},
		{"the whole file on one line",
			`<packages><package id="Newtonsoft.Json" version="13.0.1" /><package id="NUnit" version="3.13.3" developmentDependency="true" /></packages>`,
			"NUnit@3.13.3:build Newtonsoft.Json@13.0.1:runtime"},
		{"an element after other text on its line",
			"<packages> <package id=\"Serilog\" version=\"3.1.1\" />\n</packages>\n",
			"Serilog@3.1.1:runtime"},
		{"capitalised attributes and tag",
			"<packages>\n  <Package Id=\"Dapper\" Version=\"2.1.28\" />\n</packages>\n",
			"Dapper@2.1.28:runtime"},
		{"attributes on separate lines",
			"<packages>\n  <package\n      id=\"Polly\"\n      version=\"8.2.0\" />\n</packages>\n",
			"Polly@8.2.0:runtime"},
		{"a commented-out element is not a declaration",
			"<packages>\n  <!-- <package id=\"Old\" version=\"1.0.0\" /> -->\n  <package id=\"New\" version=\"2.0.0\" />\n</packages>\n",
			"New@2.0.0:runtime"},
		{"an element with no id declares nothing",
			`<packages><package version="1.0.0" /></packages>`, ""},
		{"<packages> is not <package>",
			`<packages id="not-a-package" version="9"></packages>`, ""},
		{"a '>' inside an attribute value does not end the element",
			`<packages><package id="A" allowedVersions="[1,2)" note="a>b" version="1.5.0" /></packages>`,
			"A@1.5.0:runtime"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got, names []string
			for _, d := range parseNuGetPackagesConfigVersions(c.content) {
				got = append(got, d.Name+"@"+d.Version+":"+d.Type)
				names = append(names, d.Name)
				if d.Manager != "nuget" {
					t.Errorf("%s: manager %q", d.Name, d.Manager)
				}
			}
			sort.Strings(got)
			if strings.Join(got, " ") != c.want {
				t.Errorf("versioned = %q, want %q", strings.Join(got, " "), c.want)
			}
			// The two readers agree on which packages the file declares.
			nameOnly := parseNuGetPackagesConfig(c.content)
			sort.Strings(nameOnly)
			sort.Strings(names)
			if strings.Join(nameOnly, " ") != strings.Join(names, " ") {
				t.Errorf("the name reader found %v, the versioned reader %v", nameOnly, names)
			}
		})
	}
}

// For a file with one element per line the stored requirement text is what it
// was: the element as written. It is stored and classified, so it must not
// move for the files that already read correctly.
func TestPackagesConfigRequirementIsTheElementAsWritten(t *testing.T) {
	deps := parseNuGetPackagesConfigVersions("<packages>\n  <package id=\"A\" version=\"1.0.0\" targetFramework=\"net48\" />\n</packages>\n")
	if len(deps) != 1 || deps[0].Requirement != `<package id="A" version="1.0.0" targetFramework="net48" />` {
		t.Errorf("requirement = %+v", deps)
	}
}
