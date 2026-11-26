// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A dependency with no version is "version not determined", whatever its
// requirement's text looks like (review round 1 of the vulnerability
// program's first part, High; the behaviour predates the program).
//
// With no version its purl has none, so OSV is asked about the package and
// answers with every advisory it has ever had. The class used to be read
// from the requirement's TEXT, by looking for a digit and an operator: and
// the text is often the whole manifest line, package name included
// (`boto3`), or holds only an upper bound (`jinja2<3.1`). Such a dependency
// was classed exact or bounded, and its whole advisory history was counted
// as exposure at the version in use. On one production database that was
// 85,806 open findings in 3,649 repositories.
func TestADependencyWithNoVersionIsNotDetermined(t *testing.T) {
	for _, c := range []struct{ manager, name, requirement string }{
		// A digit in the NAME.
		{"pypi", "boto3", "boto3"},
		{"pypi", "oauth2client", "oauth2client"},
		{"pypi", "jinja2", "jinja2"},
		{"rubygems", "oauth2", "gem 'oauth2'"},
		{"hackage", "base64-bytestring", "base64-bytestring"},
		{"pub", "http2", "http2: any"},
		{"maven", "org.apache.logging.log4j:log4j-core", "org.apache.logging.log4j:log4j-core:"},
		{"nuget", "Log4Net", `<PackageReference Include="Log4Net" />`},
		// An upper bound or an exclusion alone: no floor, no version.
		{"pypi", "tensorflow", "tensorflow<2.0"},
		{"pypi", "urllib3", "urllib3<2"},
		{"pypi", "Django", "Django!=3.0"},
		{"packagist", "vendor/pkg", "<2.0"},
		// Go, whose go.mod is exact by construction ONLY when it gives a
		// version: a line whose version is not one (a templated go.mod's
		// `{{.V}}`, `latest`) is stored with none (round 2: the Go rule
		// ran before this one and called it locked).
		{"go", "example.com/a", "example.com/a {{.V}}"},
		{"go", "example.com/c", ""},
		// Nothing at all.
		{"npm", "left-pad", ""},
		{"cran", "data.table", ""},
	} {
		uses := Of(model.VulnScanDep{PackageManager: c.manager, Name: c.name, Requirement: c.requirement}, nil)
		if len(uses) != 1 || uses[0].Resolution != Unpinned || uses[0].Version != "" || uses[0].FromLockfile {
			t.Errorf("%s %s (%q) with no version: %+v, want one use, unpinned", c.manager, c.name, c.requirement, uses)
		}
	}
}

// What has a version, or is known another way, is classed as before.
func TestADependencyWithAVersionIsClassedAsBefore(t *testing.T) {
	for _, c := range []struct {
		dep  model.VulnScanDep
		want string
	}{
		{model.VulnScanDep{PackageManager: "pypi", Name: "boto3", CurrentVersion: "1.26.0", Requirement: "boto3==1.26.0"}, Exact},
		{model.VulnScanDep{PackageManager: "pypi", Name: "boto3", CurrentVersion: "1.26.0", Requirement: "boto3>=1.26.0"}, RangeFloor},
		{model.VulnScanDep{PackageManager: "pypi", Name: "boto3", CurrentVersion: "1.26.0", Requirement: "boto3>=1.26.0,<2"}, BoundedRange},
		{model.VulnScanDep{PackageManager: "npm", Name: "x", CurrentVersion: "4.0.0", Requirement: "^4.0.0"}, BoundedRange},
		// A Maven version from dependencyManagement.
		{model.VulnScanDep{PackageManager: "maven", Name: "g:a", CurrentVersion: "2.0", Requirement: "g:a:", VersionSource: model.VersionSourceManaged}, Managed},
		// Go's go.mod is exact by construction.
		{model.VulnScanDep{PackageManager: "go", Name: "github.com/x/y", CurrentVersion: "v1.2.3"}, Locked},
		// A GitHub Action is classed by its ref, which is not a version.
		{model.VulnScanDep{PackageManager: "githubactions", Name: "actions/checkout", CurrentVersion: "main"}, ClassifyActionRef("main")},
		{model.VulnScanDep{PackageManager: "githubactions", Name: "actions/checkout", CurrentVersion: ""}, ClassifyActionRef("")},
	} {
		uses := Of(c.dep, nil)
		if len(uses) != 1 || uses[0].Resolution != c.want {
			t.Errorf("%s %s@%q (%q): %+v, want %s", c.dep.PackageManager, c.dep.Name, c.dep.CurrentVersion, c.dep.Requirement, uses, c.want)
		}
	}
	// A lockfile's version is the version, whatever the declaration lacks.
	locked := map[string][]string{"pypi|boto3": {"1.34.0"}}
	uses := Of(model.VulnScanDep{PackageManager: "pypi", Name: "boto3", Requirement: "boto3"}, locked)
	if len(uses) != 1 || uses[0].Resolution != Locked || uses[0].Version != "1.34.0" || !uses[0].FromLockfile {
		t.Errorf("a versionless declaration that a lockfile resolves: %+v, want locked at 1.34.0", uses)
	}
}
