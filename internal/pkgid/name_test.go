// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import (
	"sort"
	"strings"
	"testing"
)

// productionNamesRefused are the seventeen (manager, name) pairs with an
// inventory row on production on 2026-11-19 that the grammar refuses: the
// names it drops. None is a package name.
//
// Eleven are a YAML anchor read as part of an action's name
// (`uses: &checkout actions/checkout@v4`); the workflow reader strips the
// anchor since 0.29.164, so those workflows now yield `actions/checkout`,
// which the grammar accepts. Five are placeholders or fragments. One is a
// Maven coordinate with no group, which has no purl either.
var productionNamesRefused = map[string]bool{
	"githubactions\t$/actions": true,
	"githubactions\t${{ matrix.template-repo.repo }}/${{ matrix.template-repo.path }}": true,
	"githubactions\t&actions-upload-artifact actions/upload-artifact":                  true,
	"githubactions\t&actions_cache actions/cache":                                      true,
	"githubactions\t&actions_checkout actions/checkout":                                true,
	"githubactions\t&actions_setup_go actions/setup-go":                                true,
	"githubactions\t&actions_setup_node actions/setup-node":                            true,
	"githubactions\t&actions_setup_python actions/setup-python":                        true,
	"githubactions\t&checkout actions/checkout":                                        true,
	"githubactions\t&setup_proxy_cache nv-gha-runners/setup-proxy-cache":               true,
	"githubactions\t&setup_uv astral-sh/setup-uv":                                      true,
	"githubactions\t&upload_artifact actions/upload-artifact":                          true,
	"githubactions\t&uv-step astral-sh/setup-uv":                                       true,
	"githubactions\tTDesignOteam/":                                                     true,
	"githubactions\t[REPOSITORY_NAME]\"/vdc":                                           true,
	"githubactions\tactions/":                                                          true,
	"maven\t:tests":                                                                    true,
}

// TestGrammarAcceptsTheProductionSample is the third golden (summary/70,
// 4.2): over the committed sample of production names (every name with an
// unusual character is in it), the grammar refuses exactly the listed ones.
// The same check over ALL 156,490 names is in name_production_test.go, which
// needs the operator's export and is built only with the productionnames tag.
func TestGrammarAcceptsTheProductionSample(t *testing.T) {
	rows := goldenRows(t, "N", 8)
	if len(rows) != goldenNameRows {
		t.Fatalf("golden has %d name rows, want %d", len(rows), goldenNameRows)
	}
	var refused []string
	listedInSample := 0
	for _, r := range rows {
		pair := r[1] + "\t" + r[2]
		if productionNamesRefused[pair] {
			listedInSample++
		}
		if !ValidName(r[1], r[2]) {
			refused = append(refused, pair)
		}
	}
	sort.Strings(refused)
	for _, pair := range refused {
		if !productionNamesRefused[pair] {
			t.Errorf("the grammar refuses %q, which has an inventory row on production and is not on the list of names it drops", pair)
		}
	}
	// And no listed name in the sample is accepted: the list cannot hold a
	// name the grammar takes. (The sample holds 15 of the 17; the two it
	// lacks end in a slash, a shape the sample has from other names. The
	// full export, under the productionnames tag, has all 17.)
	if len(refused) != listedInSample || listedInSample != 15 {
		t.Errorf("the grammar refused %d sampled names; %d listed names are in the sample (want 15):\n%s", len(refused), listedInSample, strings.Join(refused, "\n"))
	}
}

func TestValidName(t *testing.T) {
	valid := map[string][]string{
		"npm":           {"left-pad", "@babel/core", "@Scope/Name_1.x", "~tilde", "lodash.merge"},
		"pypi":          {"flask", "Flask_SQLAlchemy", "ruamel.yaml", "a", "zope.interface"},
		"cargo":         {"serde", "serde_json", "tokio-util"},
		"go":            {"github.com/pkg/errors", "golang.org/x/text", "gopkg.in/yaml.v3", "example.com/~user/mod"},
		"maven":         {"org.apache.commons:commons-lang3", "junit:junit", "_x:_y"},
		"nuget":         {"Newtonsoft.Json", "Microsoft.Extensions.Logging", "_Private"},
		"rubygems":      {"rails", "aws-sdk-s3", "net-http_persistent"},
		"packagist":     {"monolog/monolog", "Symfony/Console", "a/b"},
		"pub":           {"http", "flutter_test", "_private"},
		"hex":           {"phoenix", "ecto_sql"},
		"hackage":       {"aeson", "unordered-containers", "base64"},
		"cran":          {"Rcpp", "data.table", "R6", "ggplot2", "ab"},
		"swiftpm":       {"Alamofire", "swift-nio", "SnapKit.swift"},
		"githubactions": {"actions/checkout", "tj-actions/changed-files", "owner/repo/sub/dir"},
	}
	invalid := map[string][]string{
		"npm":       {"", "@scope", "@scope/", "has space", "a/b/c", "name@1.0.0", "é"},
		"pypi":      {"", "-flask", "flask-", "flask sqlalchemy", "pkg>=1.0", ".. _prerequisites:", "{{ value }}", "Copyright 2017 Google Inc."},
		"cargo":     {"", "has.dot", "a b"},
		"go":        {"", "/leading", "trailing/", "a//b", "has space/x", "x!y"},
		"maven":     {"artifact-only", ":tests", "group:", "g:a:b", "${project.groupId}:core"},
		"nuget":     {"", "has space", "a/b"},
		"rubygems":  {"", "gem 'rails'", "a b"},
		"packagist": {"php", "vendor/", "/name", "a/b/c"},
		"pub":       {"", "1leading", "has-dash"},
		"hex":       {"", "Phoenix", "has-dash", "1x"},
		"hackage":   {"", "trailing-", "-leading", "a--b", "has_underscore"},
		// "Writing R Extensions": letters, digits and dots, at least two
		// characters, starting with a letter and not ending in a dot.
		"cran":          {"", "a", "1pkg", ".hidden", "trailing.", "has_underscore", "has-dash", "has space", "pkg (>= 1.0)", "user/repo"},
		"swiftpm":       {"", "has space", "a/b"},
		"githubactions": {"actions", "actions/", "/checkout", "&anchor actions/checkout", "${{ x }}/y", "docker://alpine"},
	}
	for manager, names := range valid {
		for _, n := range names {
			if !ValidName(manager, n) {
				t.Errorf("ValidName(%q, %q) = false, want true", manager, n)
			}
		}
	}
	for manager, names := range invalid {
		for _, n := range names {
			if ValidName(manager, n) {
				t.Errorf("ValidName(%q, %q) = true, want false", manager, n)
			}
		}
	}
	if len(valid) != len(nameGrammars) || len(invalid) != len(nameGrammars) {
		t.Errorf("the table covers %d and %d managers; the grammar has %d", len(valid), len(invalid), len(nameGrammars))
	}
}

// A manager with no grammar has no valid names, and a name past the bound is
// refused whatever it is made of.
func TestValidNameBounds(t *testing.T) {
	if ValidName("conda", "numpy") || ValidName("", "x") {
		t.Error("a package manager with no grammar must have no valid names")
	}
	atBound := strings.Repeat("a", MaxNameBytes)
	if !ValidName("cargo", atBound) {
		t.Errorf("a %d-byte name is at the bound and must be accepted", MaxNameBytes)
	}
	if ValidName("cargo", atBound+"a") {
		t.Errorf("a %d-byte name is past the bound and must be refused", MaxNameBytes+1)
	}
}

// Every package manager that has a purl type, and GitHub Actions, has a
// grammar: a manager the inventory can write a row for and the grammar
// cannot judge would lose every row.
func TestEveryInventoryManagerHasAGrammar(t *testing.T) {
	// The managers the libyear loop dispatches on (collector's resolver
	// switch), as the manifest readers spell them.
	for _, m := range []string{"npm", "pypi", "go", "cargo", "rubygems", "maven", "packagist", "hex", "nuget", "pub", "hackage", "swiftpm", "githubactions", "cran"} {
		if nameGrammars[m] == nil {
			t.Errorf("no name grammar for package manager %q", m)
		}
	}
	if len(nameGrammars) != 14 {
		t.Errorf("the grammar covers %d managers, want the 14 the inventory writes", len(nameGrammars))
	}
}
