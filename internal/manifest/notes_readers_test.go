// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"sort"
	"strings"
	"testing"
)

// A YAML anchor is not part of an action's name. `uses: &checkout
// actions/checkout@v4` names actions/checkout; until 0.29.164 the reader
// stored "&checkout actions/checkout" (eleven such names had inventory rows
// on production on 2026-11-19, and none could ever match an advisory).
func TestWorkflowUsesStripsAYAMLAnchor(t *testing.T) {
	content := strings.Join([]string{
		"jobs:",
		"  build:",
		"    steps:",
		"      - uses: &checkout actions/checkout@v4",
		"      - uses: &setup_uv astral-sh/setup-uv@v3 # pinned",
		"      - uses: *checkout",                    // an alias: the anchor above already recorded it
		"      - uses: \"&quoted actions/cache@v4\"", // the anchor inside quotes
		"      - uses: actions/setup-go@v5",
		"      - uses: & actions/upload-artifact@v4", // an anchor with no name is not one
	}, "\n")
	var got []string
	for _, d := range parseWorkflowUses(content) {
		got = append(got, d.Name+"@"+d.Version)
		if strings.ContainsAny(d.Name, "& ") {
			t.Errorf("action name %q still carries an anchor", d.Name)
		}
	}
	sort.Strings(got)
	want := "actions/cache@v4 actions/checkout@v4 actions/setup-go@v5 astral-sh/setup-uv@v3"
	if strings.Join(got, " ") != want {
		t.Errorf("actions = %q, want %q", strings.Join(got, " "), want)
	}
}

// Composer's platform names are not packages on Packagist: php, the ext-*
// and lib-* families, and the two composer-*-api names. The versioned reader
// leaves them out of BOTH require and require-dev (it left out only php and
// ext-* and only in require, so `"ext-xdebug"` in require-dev was looked up
// as a package and, since 0.29.162, would keep a row). The dependency-name
// reader keeps its own filter: the name inventory's counts do not change.
func TestComposerPlatformNamesAreNotDependencies(t *testing.T) {
	body := `{"require":{"php":">=8.1","ext-json":"*","lib-curl":"*","composer-runtime-api":"^2","monolog/monolog":"^3.0"},
		"require-dev":{"php":">=8.2","ext-xdebug":"*","lib-openssl":"*","composer-plugin-api":"^2","phpunit/phpunit":"^10"}}`
	path := writeFixture(t, "composer.json", body)
	deps, err := parseComposerJSONVersions(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := scoped(deps); got != "monolog/monolog:runtime phpunit/phpunit:dev" {
		t.Errorf("versioned = %q, want only the two packages", got)
	}
	names, err := parseComposerJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	// Unchanged: this reader filters php and ext-* in require only.
	if got := strings.Join(names, " "); got != "composer-plugin-api composer-runtime-api ext-xdebug lib-curl lib-openssl monolog/monolog php phpunit/phpunit" {
		t.Errorf("the dependency-name reader changed: %q", got)
	}
}
