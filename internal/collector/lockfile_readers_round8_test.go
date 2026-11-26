// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 8, the lockfile readers (R8-8, and the pin for M44).

// R8-8. Two keys that decode to one string are one key, holding the later
// value (round 7). The EARLIER value is still part of the document: the
// decoder these readers replaced read it before the later one replaced it,
// and refused the file for a key written twice inside it. It was dropped
// unread, and such a file was read.
func TestAYAMLValueThatALaterKeyReplacesIsStillRead(t *testing.T) {
	readers := map[string]func([]byte) (parsedLockfileData, error){"pnpm-lock.yaml": parsePnpmLock, "pubspec.lock": parsePubspecLock}
	for name, c := range map[string]struct {
		doc     string
		refused bool
	}{
		"a key twice inside the replaced value":       {"packages:\n  a: {version: 1, version: 2}\n  !!binary YQ==: {version: 3}\n", true},
		"a key twice deep inside the replaced value":  {"packages:\n  a: {version: 1, more: [{k: 1, k: 2}]}\n  !!binary YQ==: {version: 3}\n", true},
		"an alias inside the replaced value":          {"anchor: &x {k: 1}\npackages:\n  a: {version: 1, more: *x}\n  !!binary YQ==: {version: 3}\n", true},
		"a key twice inside the value that replaces":  {"packages:\n  a: {version: 1}\n  !!binary YQ==: {version: 3, version: 4}\n", true},
		"nothing wrong in either":                     {"packages:\n  a: {version: 1, more: [{k: 1}]}\n  !!binary YQ==: {version: 3}\n", false},
		"the same, replaced twice":                    {"packages:\n  a: {version: 1}\n  'a': {version: 2, version: 2}\n  !!binary YQ==: {version: 3}\n", true},
		"the replaced value is a scalar":              {"packages:\n  a: 5\n  !!binary YQ==: {version: 3}\n", false},
		"the replaced value is a sequence with a dup": {"packages:\n  a: [{k: 1, k: 1}]\n  !!binary YQ==: {version: 3}\n", true},
	} {
		for file, read := range readers {
			if _, err := read([]byte(c.doc)); (err != nil) != c.refused {
				t.Errorf("%s (%s): err = %v; refused should be %v", name, file, err, c.refused)
			}
		}
	}
}

// A package-lock v1 "dependencies" that is null empties what an earlier
// "dependencies" of the same object gave, the index of names with it: a name
// written again afterwards is a new entry, in its own place (M44: with the
// index kept, it was written over whatever then stood where it first was).
func TestPackageLockV1NamesAfterANullAreNewEntries(t *testing.T) {
	data := []byte(`{"dependencies": {"a": {"version": "1.0.0"}, "z": {"version": "9.0.0"}},
		"dependencies": null,
		"dependencies": {"b": {"version": "2.0.0"}, "a": {"version": "3.0.0"}}}`)
	var got []string
	for _, d := range npmV1Tree(data) {
		got = append(got, fmt.Sprintf("%s@%s", d.name, d.node.version))
	}
	if want := "[b@2.0.0 a@3.0.0]"; fmt.Sprint(got) != want {
		t.Errorf("after a null, dependencies read as %v, want %s", got, want)
	}
}

// The reasons a declaration may carry are listed in one place
// (model.VersionReasons), and the parent accepts from the child exactly
// those. Round 8 added three; the parent's own list of three refused the
// whole analysis of a repository whose pom has a profile that manages a
// dependency. Each new reason is produced here by the real child and
// arrives.
func TestEveryReasonTheReadersGiveCrossesTheChildBoundary(t *testing.T) {
	dir := writeClone(t, map[string]string{
		// A profile that is active by default declares g:x with a version:
		// that declaration is the build's, so the managed one is not given
		// (0.29.216: a profile's dependencyManagement alone no longer
		// withholds; the default build applies it).
		"profiled/pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version>" +
			"<dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId><version>1</version></dependency></dependencies></dependencyManagement>" +
			"<profiles><profile><id>p</id><activation><activeByDefault>true</activeByDefault></activation><dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId><version>9</version></dependency></dependencies></profile></profiles>" +
			"<dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies></project>",
		"tied/build.gradle": "dependencies {\n implementation platform('t:a:1')\n implementation platform('t:b:1')\n implementation 'g:y'\n}\n",
		"long/build.gradle": "ext.v = ''" + strings.Repeat(" + ''", 6000) + " + '1'\ndependencies {\n implementation \"g:z:$v\"\n}\n",
	})
	// The two platforms are published ones (0.29.216: a pom of the clone is
	// never a Gradle platform).
	bom := func(artifact, version string) *manifest.RemotePom {
		return &manifest.RemotePom{Group: "t", Artifact: artifact, Version: "1", Managed: []manifest.RemoteManaged{{Group: "g", Artifact: "y", Version: version}}}
	}
	src := &mapPomSource{poms: map[string]*manifest.RemotePom{"t:a:1": bom("a", "1.0"), "t:b:1": bom("b", "1-0")}, other: manifest.RemoteNotFound}
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 10}, testLimits(), src, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range run.decl.Deps {
		if d.VersionReason != "" && !d.ManagedEntry {
			got[d.Name] = d.VersionReason
		}
	}
	want := map[string]string{"g:x": model.VersionReasonProfileManaged, "g:y": model.VersionReasonPlatformsDisagree, "g:z": model.VersionReasonValueNotRead}
	for name, reason := range want {
		if got[name] != reason {
			t.Errorf("%s arrived with reason %q, want %q (all that arrived: %v)", name, got[name], reason, got)
		}
	}
	listed := map[string]bool{}
	for _, r := range model.VersionReasons() {
		listed[r] = true
	}
	for _, reason := range want {
		if !listed[reason] {
			t.Errorf("%q is not in model.VersionReasons()", reason)
		}
	}
}
