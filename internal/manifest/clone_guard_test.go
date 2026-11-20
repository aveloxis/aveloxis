// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A file the Maven and Gradle readers open on a build's say-so must be
// INSIDE the clone, by where it really is. The guard compared the path as
// written and looked at the last element only, so a committed symbolic link
// to a DIRECTORY led out: `apply from: 'sym/x.gradle'` read whatever `sym`
// pointed at, and a value from it was stored as a dependency's name (review
// round 1 of the vulnerability program's first part).

// outsideTree makes a directory beside the clone, with the files given.
func outsideTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, body := range files {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// mustSymlink makes a symbolic link. It fails the test when it cannot:
// every platform these tests run on can, and a guard against links that is
// silently not tested is the worse outcome.
func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("creating a symbolic link: %v", err)
	}
}

func TestGradleDoesNotReadThroughADirectoryLinkOutOfTheClone(t *testing.T) {
	outside := outsideTree(t, map[string]string{
		"secrets.gradle":      "ext.leak = 'SECRET-TOKEN-123'\n",
		"libs.versions.toml":  "[libraries]\ncat = { module = \"out.side:catalogue\", version = \"9.9.9-outside\" }\n",
		"gradle/a.properties": "x=1\n",
	})
	root := writeTree(t, map[string]string{
		"build.gradle": "apply from: 'sym/secrets.gradle'\ndependencies {\n  implementation \"com.example:${leak}:1.0\"\n  implementation libs.cat\n  implementation 'in.side:fine:2.0'\n}\n",
	})
	mustSymlink(t, outside, filepath.Join(root, "sym"))
	mustSymlink(t, outside, filepath.Join(root, "gradle")) // where version catalogues are looked for
	deps := NewWalk(root, nil).Read(filepath.Join(root, "build.gradle"), Options{}).Deps
	seen := map[string]string{}
	for _, d := range deps {
		seen[d.Name] = d.Version
		if strings.Contains(d.Name+d.Version+d.Requirement, "SECRET") || strings.Contains(d.Name+d.Version, "outside") || strings.HasPrefix(d.Name, "out.side") {
			t.Errorf("a value from outside the clone reached a dependency: %+v", d)
		}
	}
	if seen["in.side:fine"] != "2.0" {
		t.Errorf("the build's own dependency: %v", seen)
	}
}

func TestMavenDoesNotReadThroughADirectoryLinkOutOfTheClone(t *testing.T) {
	outside := outsideTree(t, map[string]string{
		"pom.xml": `<project><groupId>p</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>
<properties><v>7.7.7-outside</v></properties></project>`,
	})
	root := writeTree(t, map[string]string{
		"pom.xml": `<project><parent><groupId>p</groupId><artifactId>parent</artifactId><version>1</version><relativePath>sym/pom.xml</relativePath></parent>
<artifactId>child</artifactId>
<dependencies><dependency><groupId>a</groupId><artifactId>b</artifactId><version>${v}</version></dependency></dependencies></project>`,
	})
	mustSymlink(t, outside, filepath.Join(root, "sym"))
	for _, d := range NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{}).Deps {
		if strings.Contains(d.Version, "outside") {
			t.Errorf("a value from a pom outside the clone reached a dependency: %+v", d)
		}
	}
}

// A link that stays inside the clone is the repository's own layout, and
// is read. A link as the file itself is refused, as it always was.
func TestALinkInsideTheCloneIsRead(t *testing.T) {
	root := writeTree(t, map[string]string{
		"build.gradle":         "apply from: 'shared/versions.gradle'\ndependencies {\n  implementation \"com.example:lib:${v}\"\n}\n",
		"real/versions.gradle": "ext.v = '3.1'\n",
		"real/other.gradle":    "ext.w = '4.4'\n",
		// Its own variable: `v` reaches app/ from the root build above it.
		"app/build.gradle": "apply from: 'linked.gradle'\ndependencies {\n  implementation \"com.example:lib:${w}\"\n}\n",
	})
	mustSymlink(t, filepath.Join(root, "real"), filepath.Join(root, "shared"))
	mustSymlink(t, filepath.Join(root, "real", "other.gradle"), filepath.Join(root, "app", "linked.gradle"))
	got := func(rel string) string {
		for _, d := range NewWalk(root, nil).Read(filepath.Join(root, rel), Options{}).Deps {
			if d.Name == "com.example:lib" {
				return d.Version
			}
		}
		return "(absent)"
	}
	if v := got("build.gradle"); v != "3.1" {
		t.Errorf("through a directory link that stays inside the clone: version %q, want 3.1", v)
	}
	if v := got("app/build.gradle"); v == "4.4" {
		t.Errorf("a file that is itself a link was read: version %q", v)
	}
}

func TestRegularFileInClone(t *testing.T) {
	outside := outsideTree(t, map[string]string{"f": "x"})
	root := writeTree(t, map[string]string{"in/f": "x", "dir/keep": ""})
	mustSymlink(t, outside, filepath.Join(root, "out"))
	mustSymlink(t, filepath.Join(root, "in"), filepath.Join(root, "alias"))
	mustSymlink(t, filepath.Join(root, "in", "f"), filepath.Join(root, "linkfile"))
	for rel, want := range map[string]bool{
		"in/f":         true,
		"alias/f":      true,  // a directory link that stays inside
		"out/f":        false, // a directory link that leads out
		"linkfile":     false, // the file itself is a link
		"dir":          false, // a directory
		"missing":      false,
		"in/../in/f":   true,
		"../elsewhere": false,
	} {
		if got := regularFileInClone(root, filepath.Join(root, filepath.FromSlash(rel))); got != want {
			t.Errorf("regularFileInClone(%q) = %v, want %v", rel, got, want)
		}
	}
	if regularFileInClone(root, filepath.Join(outside, "f")) {
		t.Error("an absolute path outside the clone was accepted")
	}
}
