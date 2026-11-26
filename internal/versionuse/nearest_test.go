// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func lock(path, eco string, versions map[string][]string) Lockfile {
	return Lockfile{Path: path, Ecosystem: eco, Versions: versions}
}

func TestNearest(t *testing.T) {
	root := lock("package-lock.json", "npm", map[string][]string{"npm|express": {"4.18.2"}, "npm|lodash": {"4.17.21"}})
	appA := lock("apps/a/package-lock.json", "npm", map[string][]string{"npm|express": {"4.17.0"}})
	appB := lock("apps/b/yarn.lock", "npm", map[string][]string{"npm|left-pad": {"1.3.0"}})
	two := lock("apps/c/package-lock.json", "npm", map[string][]string{"npm|dup": {"1.0.0", "2.0.0"}})
	yarnBeside := lock("apps/d/yarn.lock", "npm", map[string][]string{"npm|both": {"1.0.0"}})
	npmBeside := lock("apps/d/package-lock.json", "npm", map[string][]string{"npm|both": {"1.0.0"}, "npm|one": {"3.0.0"}})
	poetry := lock("apps/a/poetry.lock", "pypi", map[string][]string{"pypi|flask-sqlalchemy": {"3.0.0"}})
	all := []Lockfile{root, appA, appB, two, yarnBeside, npmBeside, poetry}

	for _, c := range []struct {
		name, manifest, manager, dep string
		version, source              string
	}{
		{"the lockfile beside the manifest", "apps/a/package.json", "npm", "express", "4.17.0", SourceLocked},
		{"the root's manifest reads the root's lockfile", "package.json", "npm", "express", "4.18.2", SourceLocked},
		{"a deeper manifest reads the closest lockfile above it", "apps/a/src/tool/package.json", "npm", "express", "4.17.0", SourceLocked},
		{"no lockfile beside it: the workspace root's", "apps/z/package.json", "npm", "lodash", "4.17.21", SourceLocked},
		// The nearest lockfile exists and does not hold the name. Nothing is
		// claimed; the search does NOT go on to the root's lockfile, which
		// locks a different application.
		{"the nearest lockfile does not hold the name", "apps/b/package.json", "npm", "express", "", ""},
		{"several versions in the nearest lockfile", "apps/c/package.json", "npm", "dup", "", SourceLockedSeveral},
		{"two lockfiles equally near, both hold it", "apps/d/package.json", "npm", "both", "", SourceLockedSeveral},
		{"two lockfiles equally near, one holds it", "apps/d/package.json", "npm", "one", "3.0.0", SourceLocked},
		{"another ecosystem's lockfile is not this one's", "apps/b/requirements.txt", "pypi", "flask", "", ""},
		{"the name is matched as the scan matches it", "apps/a/requirements.txt", "pypi", "Flask_SQLAlchemy", "3.0.0", SourceLocked},
		{"no lockfile of the ecosystem anywhere above", "svc/Cargo.toml", "cargo", "serde", "", ""},
		{"a Go module is known by go.mod", "go.mod", "go", "golang.org/x/net", "", ""},
		{"an action is known by its ref", ".github/workflows/ci.yml", "githubactions", "actions/checkout", "", ""},
		{"a Windows-written path", `apps\a\package.json`, "npm", "express", "4.17.0", SourceLocked},
	} {
		version, source := Nearest(c.manifest, c.manager, c.dep, all)
		if version != c.version || source != c.source {
			t.Errorf("%s: (%q, %q), want (%q, %q)", c.name, version, source, c.version, c.source)
		}
	}
	if v, s := Nearest("package.json", "npm", "express", nil); v != "" || s != "" {
		t.Errorf("no lockfiles: (%q, %q)", v, s)
	}
}

// A Maven dependency whose version comes from dependencyManagement is
// "managed": its version is known, so it is not unpinned, whatever its
// requirement text (which has no version) would say.
func TestAManagedVersionIsNotUnpinned(t *testing.T) {
	dep := model.VulnScanDep{Name: "io.netty:netty-all", PackageManager: "maven", CurrentVersion: "4.1.100.Final",
		Requirement: "io.netty:netty-all:", Purl: "pkg:maven/io.netty/netty-all@4.1.100.Final", VersionSource: model.VersionSourceManaged}
	uses := Of(dep, nil)
	if len(uses) != 1 || uses[0].Resolution != Managed || uses[0].Version != "4.1.100.Final" {
		t.Errorf("uses = %+v; want one, managed, at the managed version", uses)
	}
	// The same row with no source is classified from its text, as before.
	dep.VersionSource = ""
	if got := Of(dep, nil)[0].Resolution; got == Managed {
		t.Errorf("a row with no source was labelled managed")
	}
	// A lockfile still wins.
	dep.VersionSource = model.VersionSourceManaged
	if got := Of(dep, map[string][]string{"maven|io.netty:netty-all": {"4.1.101.Final"}}); len(got) != 1 || got[0].Resolution != Locked {
		t.Errorf("with a lockfile: %+v; want locked", got)
	}
	// A managed source with no version (the tree could not resolve it) is
	// not "managed": there is no version to scan at.
	dep.CurrentVersion = ""
	if got := Of(dep, nil)[0].Resolution; got != Unpinned {
		t.Errorf("a managed row with no version: %q, want unpinned", got)
	}
}
