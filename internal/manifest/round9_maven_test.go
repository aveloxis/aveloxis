// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Round 9 (R9-1). A pom found by its coordinates is found in the BUILD of
// the pom that names them, the poms joined by <modules>, and nowhere else in
// the clone. 0.29.215 took any one pom of the clone: a test fixture that
// called itself by a published parent's coordinates was read for it.
func TestAPomOutsideTheBuildIsNoCandidate(t *testing.T) {
	published := &RemotePom{Group: "org.pub", Artifact: "parent", Version: "3",
		Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "2.2"}, {Group: "g", Artifact: "y", Version: "5"}}}
	src := func() *fakePoms { return &fakePoms{poms: map[string]*RemotePom{"org.pub:parent:3": published}} }
	fixture := "<project><groupId>org.pub</groupId><artifactId>parent</artifactId><version>3</version><dependencyManagement><dependencies>" +
		r4dep("g", "x", "1.4") + "</dependencies></dependencyManagement></project>"
	app := "<project><parent><groupId>org.pub</groupId><artifactId>parent</artifactId><version>3</version><relativePath/></parent><artifactId>app</artifactId><dependencies>" +
		r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>"
	importer := "<project><groupId>t</groupId><artifactId>imp</artifactId><version>1</version><dependencyManagement><dependencies>" +
		r4import("org.pub", "parent", "3") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>"
	for name, files := range map[string]map[string]string{
		"a fixture under src/test":        {"pom.xml": app, "src/test/resources/fixture/pom.xml": fixture},
		"a fixture, the pom in a module":  {"pom.xml": "<project><groupId>t</groupId><artifactId>agg</artifactId><version>1</version><modules><module>m</module></modules></project>", "m/pom.xml": app, "examples/pom.xml": fixture},
		"an import, the same":             {"pom.xml": importer, "it/pom.xml": fixture},
		"two fixtures (no candidate, so)": {"pom.xml": app, "a/pom.xml": fixture, "b/pom.xml": fixture},
	} {
		rel := "pom.xml"
		if _, has := files["m/pom.xml"]; has {
			rel = "m/pom.xml"
		}
		s := src()
		got := r6Versions(t, files, rel, s)
		if x, y := got["g:x"], got["g:y"]; x.Version != "2.2" || y.Version != "5" || x.VersionReason != "" || y.VersionReason != "" {
			t.Errorf("%s: g:x at %q (%s), g:y at %q (%s); want the published pom's 2.2 and 5 (the pom of the clone with its coordinates is part of another build)",
				name, x.Version, x.VersionReason, y.Version, y.VersionReason)
		}
		if len(s.asked) == 0 {
			t.Errorf("%s: the repositories were not asked", name)
		}
	}
	// The neighbour: the same pom as a module of the build IS the one.
	got := r6Versions(t, oneBuild(map[string]string{"pom.xml": app, "local/pom.xml": fixture}), "pom.xml", src())
	if x, y := got["g:x"], got["g:y"]; x.Version != "1.4" || y.Version != "" {
		t.Errorf("the parent is a module of the build: g:x at %q, g:y at %q; want the module's 1.4 and no version", x.Version, y.Version)
	}
}

// The parent of a pom that was FETCHED is asked of the repositories: a
// fetched pom is part of no build of the clone.
func TestAFetchedPomFindsNothingInTheClone(t *testing.T) {
	fetched := &RemotePom{Group: "org.pub", Artifact: "starter", Version: "3", Parent: &RemotePomParent{Group: "org.pub", Artifact: "deps", Version: "3"},
		Managed: []RemoteManaged{{Group: "org.pub", Artifact: "bom", Version: "3", Import: true}}}
	deps := &RemotePom{Group: "org.pub", Artifact: "deps", Version: "3", Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "2.2"}}}
	bom := &RemotePom{Group: "org.pub", Artifact: "bom", Version: "3", Managed: []RemoteManaged{{Group: "g", Artifact: "y", Version: "5"}}}
	local := func(artifact, dep, version string) string {
		return "<project><groupId>org.pub</groupId><artifactId>" + artifact + "</artifactId><version>3</version><dependencyManagement><dependencies>" +
			r4dep("g", dep, version) + "</dependencies></dependencyManagement></project>"
	}
	files := oneBuild(map[string]string{
		"pom.xml": "<project><parent><groupId>org.pub</groupId><artifactId>starter</artifactId><version>3</version><relativePath/></parent><artifactId>app</artifactId><dependencies>" +
			r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>",
		"deps/pom.xml": local("deps", "x", "1.4"),
		"bom/pom.xml":  local("bom", "y", "0.1"),
	})
	got := r6Versions(t, files, "pom.xml", &fakePoms{poms: map[string]*RemotePom{"org.pub:starter:3": fetched, "org.pub:deps:3": deps, "org.pub:bom:3": bom}})
	if x, y := got["g:x"], got["g:y"]; x.Version != "2.2" || y.Version != "5" {
		t.Errorf("g:x at %q (%s), g:y at %q (%s); want 2.2 from the fetched pom's parent and 5 from its import, both fetched", x.Version, x.VersionReason, y.Version, y.VersionReason)
	}
}

// Round 9 (R9-4). What is remembered of an import that led nowhere does
// not carry the verdict of the pom that asked first: a module that declares
// a repository never asked withholds, and its parent, which declares none,
// takes the next BOM's answer, whichever is read first. (Round 8's fix was
// pinned by nothing: handing the import's lookup the asker's own verdict
// passed every test and the reference.)
func TestAnImportThatLedNowhereIsRememberedWithoutItsAsker(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><modules><module>m</module><module>second</module></modules><dependencyManagement><dependencies>" +
			r4import("t", "gone", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m</artifactId>" +
			"<repositories><repository><id>old</id><url>http://insecure.example/repo</url></repository></repositories><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	for _, order := range [][]string{{"pom.xml", "m/pom.xml"}, {"m/pom.xml", "pom.xml"}} {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(&r8Counting{})
		got := map[string]string{}
		for _, rel := range order {
			for _, d := range w.Read(filepath.Join(root, rel), Options{}).Deps {
				if d.Name == "g:x" {
					got[rel] = d.Version + "(" + d.VersionReason + ")"
				}
			}
		}
		if got["pom.xml"] != "2.0()" || got["m/pom.xml"] != "("+model.VersionReasonInheritedNotFound+")" {
			t.Errorf("read order %v: the root has g:x at %s, the module at %s; want the root at 2.0 and the module withheld, in either order", order, got["pom.xml"], got["m/pom.xml"])
		}
	}
}

// Round 9 (R9-9). A pom whose version is a property set by a parent found
// only by ITS coordinates is indexed however deep that goes and wherever
// the directories sort: two passes, the second adding to the index as it
// went, answered for three such levels by the order of the walk.
func TestCoordinatesBehindCoordinatesDoNotDependOnTheWalk(t *testing.T) {
	managing := func(parent, parentVersion, artifact, version, dep, depVersion string) string {
		return "<project><parent><groupId>t</groupId><artifactId>" + parent + "</artifactId><version>" + parentVersion + "</version></parent><artifactId>" + artifact +
			"</artifactId><version>" + version + "</version><dependencyManagement><dependencies>" + r4dep("g", dep, depVersion) + "</dependencies></dependencyManagement></project>"
	}
	for _, dirs := range [][4]string{{"a/mid", "b/c", "d/e", "f/g"}, {"z/mid", "y/c", "x/e", "w/g"}, {"b/mid", "a/c", "d/e", "c/g"}, {"y/mid", "z/c", "a/e", "b/g"}} {
		files := oneBuild(map[string]string{
			"pom.xml":            "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><properties><r1>7</r1><r2>8</r2><r3>9</r3></properties></project>",
			dirs[0] + "/pom.xml": managing("root", "1", "mid", "${r1}", "x", "1.0"),
			dirs[1] + "/pom.xml": managing("mid", "7", "c", "${r2}", "y", "2.0"),
			dirs[2] + "/pom.xml": managing("c", "8", "e", "${r3}", "z", "3.0"),
			dirs[3] + "/pom.xml": "<project><parent><groupId>t</groupId><artifactId>e</artifactId><version>9</version></parent><artifactId>leaf</artifactId><dependencies>" +
				r4dep("g", "x", "") + r4dep("g", "y", "") + r4dep("g", "z", "") + "</dependencies></project>",
		})
		got := r6Versions(t, files, dirs[3]+"/pom.xml", &r8Counting{})
		if got["g:x"].Version != "1.0" || got["g:y"].Version != "2.0" || got["g:z"].Version != "3.0" {
			t.Errorf("directories %v: g:x %q, g:y %q, g:z %q; want 1.0, 2.0 and 3.0 from three parents each found by coordinates its own parent sets",
				dirs, got["g:x"].Version, got["g:y"].Version, got["g:z"].Version)
		}
	}
}

// Round 9 (R9-2). The build that is read is Maven's default one. A profile
// that is active by default is part of it: its properties, its managed
// entries and its imports are the pom's. Any other profile is not, and
// changes nothing. (0.29.215 withheld a version when ANY profile managed the
// artifact, and every managed version of the pom and of the modules below
// when a profile imported a BOM or wrote a coordinate as a property.)
func TestTheDefaultBuildsProfilesAreApplied(t *testing.T) {
	profile := func(activation, inner string) string {
		return "<profiles><profile><id>p</id>" + activation + inner + "</profile></profiles>"
	}
	const byDefault = "<activation><activeByDefault>true</activeByDefault></activation>"
	pom := func(profiles string) map[string]string {
		return oneBuild(map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><properties><v>1.1</v></properties><dependencyManagement><dependencies>" +
				r4dep("g", "own", "5.0") + r4dep("g", "prop", "${v}") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
				r4dep("g", "x", "") + r4dep("g", "z", "") + r4dep("g", "own", "") + r4dep("g", "prop", "") + r4dep("g", "w", "") + "</dependencies>" + profiles + "</project>",
			"second/pom.xml": r6Bom("second", "g", "x", "2.0", "g", "z", "3.0"),
			"third/pom.xml":  r6Bom("third", "g", "z", "7.0", "g", "w", "8.0"),
		})
	}
	inner := "<properties><v>1.2</v></properties><dependencyManagement><dependencies>" + r4dep("g", "x", "9") + r4dep("g", "own", "6.0") +
		r4dep("${some.group}", "thing", "1") + r4import("t", "third", "1") + "</dependencies></dependencyManagement>"
	for name, c := range map[string]struct {
		activation string
		want       map[string]string
	}{
		// Its entries over the pom's own and its imports; its import AFTER
		// the pom's (g:z stays the first BOM's, g:w is the profile's BOM's).
		"active by default":       {byDefault, map[string]string{"g:x": "9", "g:own": "6.0", "g:prop": "1.2", "g:z": "3.0", "g:w": "8.0"}},
		"no activation":           {"", map[string]string{"g:x": "2.0", "g:own": "5.0", "g:prop": "1.1", "g:z": "3.0", "g:w": ""}},
		"activeByDefault false":   {"<activation><activeByDefault>false</activeByDefault></activation>", map[string]string{"g:x": "2.0", "g:own": "5.0", "g:prop": "1.1", "g:z": "3.0", "g:w": ""}},
		"activated by a JDK":      {"<activation><jdk>[11,)</jdk></activation>", map[string]string{"g:x": "2.0", "g:own": "5.0", "g:prop": "1.1", "g:z": "3.0", "g:w": ""}},
		"activated by a property": {"<activation><property><name>!skip</name></property></activation>", map[string]string{"g:x": "2.0", "g:own": "5.0", "g:prop": "1.1", "g:z": "3.0", "g:w": ""}},
	} {
		got := r6Versions(t, pom(profile(c.activation, inner)), "pom.xml", &r8Counting{})
		for dep, want := range c.want {
			if d := got[dep]; d.Version != want || d.VersionReason == model.VersionReasonProfileManaged {
				t.Errorf("a profile %s: %s at %q (%s), want %q", name, dep, d.Version, d.VersionReason, want)
			}
		}
	}
	// A module below takes the parent's model with the profile in it, and
	// its own entry still outranks it.
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + r4dep("g", "y", "1.0") + "</dependencies></dependencyManagement>" +
			profile(byDefault, "<dependencyManagement><dependencies>"+r4dep("g", "x", "9")+r4dep("g", "y", "9")+"</dependencies></dependencyManagement>") + "</project>",
		"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencyManagement><dependencies>" +
			r4dep("g", "x", "4.0") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>",
	}
	got := r6Versions(t, oneBuild(files), "m/pom.xml", &r8Counting{})
	if x, y := got["g:x"], got["g:y"]; x.Version != "4.0" || y.Version != "9" {
		t.Errorf("below a parent whose default profile manages g:x and g:y at 9: g:x at %q, g:y at %q; want the module's own 4.0 and the profile's 9", x.Version, y.Version)
	}
}

// A profile, active by default, that DECLARES the dependency with a version:
// that declaration is a row of its own, and the declaration with no version
// of the pom that HOLDS the profile is the same dependency in the build; it
// is given no managed version. A module below declares the dependency
// itself, which replaces what it inherits, the profile's with it: it takes
// the managed version, as with no profile (0.29.217, review round 10: the
// module's was withheld too, which 0.29.214 stored). A profile that is not
// active leaves the pom's own declaration to its management.
func TestADefaultProfilesDeclarationWithholdsTheManagedVersion(t *testing.T) {
	rows := func(activation, ownManagement string) []string {
		root := writeTree(t, oneBuild(map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "x", "2.0") + r4dep("g", "z", "3.0") +
				"</dependencies></dependencyManagement><profiles><profile><id>p</id>" + activation + "<dependencies>" + r4dep("g", "x", "9") + "</dependencies></profile></profiles></project>",
			"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencyManagement><dependencies>" + ownManagement +
				"</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + r4dep("g", "z", "") + "</dependencies></project>",
		}))
		w := NewWalk(root, nil)
		w.SetPomSource(&r8Counting{})
		var out []string
		for _, rel := range []string{"pom.xml", "m/pom.xml"} {
			for _, d := range w.Read(filepath.Join(root, rel), Options{}).Deps {
				if !d.ManagedEntry {
					out = append(out, rel+" "+d.Name+"@"+d.Version+"("+d.VersionReason+")")
				}
			}
		}
		return out
	}
	const byDefault = "<activation><activeByDefault>true</activeByDefault></activation>"
	for name, c := range map[string]struct {
		activation, own string
		want            []string
	}{
		"active by default":                        {byDefault, "", []string{"pom.xml g:x@9()", "m/pom.xml g:x@2.0()", "m/pom.xml g:z@3.0()"}},
		"active by default, the module manages it": {byDefault, r4dep("g", "x", "4.0"), []string{"pom.xml g:x@9()", "m/pom.xml g:x@4.0()", "m/pom.xml g:z@3.0()"}},
		"not active":                               {"", "", []string{"pom.xml g:x@9()", "m/pom.xml g:x@2.0()", "m/pom.xml g:z@3.0()"}},
	} {
		if got := rows(c.activation, c.own); strings.Join(got, ", ") != strings.Join(c.want, ", ") {
			t.Errorf("a profile %s: rows %v, want %v", name, got, c.want)
		}
	}
	// The pom that holds the profile: its own declaration with no version.
	held := oneBuild(map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "x", "2.0") +
			"</dependencies></dependencyManagement></project>",
		"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencies>" + r4dep("g", "x", "") +
			"</dependencies><profiles><profile><id>p</id>" + byDefault + "<dependencies>" + r4dep("g", "x", "9") + "</dependencies></profile></profiles></project>",
	})
	heldRoot := writeTree(t, held)
	w := NewWalk(heldRoot, nil)
	w.SetPomSource(&r8Counting{})
	withheld := false
	for _, d := range w.Read(filepath.Join(heldRoot, "m/pom.xml"), Options{}).Deps {
		if d.Name == "g:x" && d.Version == "" {
			withheld = d.VersionReason == model.VersionReasonProfileManaged
		}
	}
	if !withheld {
		t.Error("a pom whose own default profile declares g:x at 9 and that declares g:x with no version: want that declaration with no version and profile_managed")
	}
}

// The file's own reader follows the same build (round 9, D2): a property a
// default profile sets is the pom's, over the pom's own; one that only a
// profile that is not active sets is not set, except for what that profile
// declares itself. (Every <properties> element of the document was one
// list, the first to set a name winning.)
func TestAPomsOwnPropertiesAreThoseOfTheDefaultBuild(t *testing.T) {
	pom := func(activation string) string {
		return "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><properties><v>1.1</v></properties><dependencies>" +
			r4dep("g", "x", "${v}") + r4dep("g", "y", "${only}") + "</dependencies><profiles><profile><id>p</id>" + activation +
			"<properties><v>1.2</v><only>3.3</only></properties><dependencies>" + r4dep("g", "inside", "${v}-${only}") + "</dependencies></profile>" +
			"<profile><id>q</id><dependencies>" + r4dep("g", "other", "${v}") + "</dependencies></profile></profiles></project>"
	}
	for name, c := range map[string]struct {
		activation                string
		x, y, inside, other, bare string
	}{
		"active by default": {"<activation><activeByDefault>true</activeByDefault></activation>", "1.2", "3.3", "1.2-3.3", "1.2", "1.2"},
		// "${only}" as written: the file's reader leaves a reference it
		// cannot resolve for the tree, which has no value for it either.
		"not active":            {"", "1.1", "${only}", "1.2-3.3", "1.1", "1.1"},
		"activeByDefault false": {"<activation><activeByDefault>false</activeByDefault></activation>", "1.1", "${only}", "1.2-3.3", "1.1", "1.1"},
	} {
		got := map[string]string{}
		for _, d := range ParsePomXMLVersions(pom(c.activation)) {
			got[d.Name] = d.Version
		}
		if got["g:x"] != c.x || got["g:y"] != c.y || got["g:inside"] != c.inside || got["g:other"] != c.other {
			t.Errorf("a profile %s, the file alone: g:x %q, g:y %q, g:inside %q, g:other %q; want %q, %q, %q, %q", name, got["g:x"], got["g:y"], got["g:inside"], got["g:other"], c.x, c.y, c.inside, c.other)
		}
		// And in the tree, where a module below inherits the property.
		files := oneBuild(map[string]string{"pom.xml": pom(c.activation),
			"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>app</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencies>" + r4dep("g", "bare", "${v}") + "</dependencies></project>"})
		if bare := r6Versions(t, files, "m/pom.xml", &r8Counting{})["g:bare"]; bare.Version != c.bare {
			t.Errorf("a profile %s, a module below: g:bare at %q, want %q", name, bare.Version, c.bare)
		}
	}
}

// Round 9 (the boundary review's mutant M26). A pom of the build that the
// reading process had no room to read (the read bound refused it) is noted
// as unread like one that is not a regular file: a parent the repositories
// do not have, and that the unread pom may be, is not known, and is not
// "not found".
func TestAPomTheReadBoundRefusedIsNotedAsUnread(t *testing.T) {
	files := map[string]string{
		"pom.xml":     "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><packaging>pom</packaging><modules><module>par</module><module>app</module></modules></project>",
		"par/pom.xml": "<project><groupId>t</groupId><artifactId>par</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" + r4dep("g", "x", "4.0") + "</dependencies></dependencyManagement></project>",
		"app/pom.xml": "<project><parent><groupId>t</groupId><artifactId>par</artifactId><version>1</version><relativePath>../none/pom.xml</relativePath></parent><artifactId>app</artifactId><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
	}
	// The neighbour: read whole, the parent of the build answers.
	if x := r6Versions(t, files, "app/pom.xml", outcomeSource{outcome: RemoteNotFound})["g:x"]; x.Version != "4.0" {
		t.Fatalf("with room: g:x at %q (%s); want 4.0 from the parent in the build", x.Version, x.VersionReason)
	}
	restore := SetReadBound(func(path string, size int64) int64 {
		if strings.HasSuffix(filepath.ToSlash(path), "par/pom.xml") {
			return 1
		}
		return 1 << 20
	})
	defer restore()
	if x := r6Versions(t, files, "app/pom.xml", outcomeSource{outcome: RemoteNotFound})["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("the parent could not be read: g:x at %q (%s); want no version and %s", x.Version, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
}
