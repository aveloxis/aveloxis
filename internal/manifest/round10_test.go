// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Round 10 (RD10-1). A parent reached by relativePath that no pom lists as
// a module imports a BOM that IS a module of the build its child is in: the
// BOM is found (0.29.216 looked only in the parent's own build, which is
// the parent alone; 0.29.214 found it). A child of the same parent in a
// build that has no such module does not find it, and neither answer
// depends on which child is read first: what one child's question finds or
// misses is not remembered for the other.
func TestAParentOutsideTheModulesFindsTheBOMOfTheBuildBeingRead(t *testing.T) {
	child := func(artifact string) string {
		return "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><relativePath>../parent</relativePath></parent><artifactId>" + artifact +
			"</artifactId><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>"
	}
	files := map[string]string{
		"pom.xml":        "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><packaging>pom</packaging><modules><module>bom</module><module>app</module></modules></project>",
		"bom/pom.xml":    "<project><groupId>ex</groupId><artifactId>bom</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" + r4dep("g", "x", "31") + "</dependencies></dependencyManagement></project>",
		"parent/pom.xml": "<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies><dependency><groupId>ex</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement></project>",
		"app/pom.xml":    child("app"),
		"solo/pom.xml":   child("solo"),
	}
	for _, order := range [][]string{{"app/pom.xml", "solo/pom.xml"}, {"solo/pom.xml", "app/pom.xml"}} {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(&r8Counting{})
		got := map[string]model.LibyearDep{}
		for _, rel := range order {
			for _, d := range w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{}).Deps {
				if d.Name == "g:x" {
					got[rel] = d
				}
			}
		}
		if app := got["app/pom.xml"]; app.Version != "31" {
			t.Errorf("read in the order %v: the module's g:x at %q (%s), want 31 from the BOM of its build", order, app.Version, app.VersionReason)
		}
		if solo := got["solo/pom.xml"]; solo.Version != "" || solo.VersionReason != model.VersionReasonInheritedNotFound {
			t.Errorf("read in the order %v: g:x of the pom that is in no build with the BOM at %q (%s), want none and inherited_not_found", order, solo.Version, solo.VersionReason)
		}
	}
	// The whole-tree verdict (round 11, RD11-6). The tree is NOT whole, and
	// that agrees with the questions: the parent is a pom of the tree and
	// is asked as itself too, and read as itself it is a build of its own
	// with no such BOM (its own rows say inherited_not_found). So the
	// tree's managed entries stay inventoried. What the module's question
	// finds through the build being read does not make the tree whole.
	delete(files, "solo/pom.xml")
	_, settled := readTree(t, &r8Counting{}, files)
	if len(settled.NotWhole) != 1 || settled.NotWhole[0].Reason != "an imported BOM could not be obtained" {
		t.Errorf("the tree's verdict is %+v; want it not whole for the BOM the parent, read as itself, does not find", settled.NotWhole)
	}
}

// Round 10 (RD10-3). The file's own reader and the tree's read a profile
// alike: the activation is the profile's wherever it is written in it, in
// any case; a <properties> element inside the profile's <build> is a
// plugin's and not the profile's. (The file's reader cut the profile's text
// at its first <properties> or <build> to find the activation, and took
// every <properties> element inside an active profile for the profile's.)
func TestBothPomReadersReadAProfilesActivationAndPropertiesAlike(t *testing.T) {
	pom := func(profile string) string {
		return "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><properties><gv>30</gv></properties><dependencyManagement><dependencies>" + r4dep("g", "m", "${gv}") +
			"</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "${gv}") + r4dep("g", "m", "") + "</dependencies><profiles><profile><id>p</id>" + profile + "</profile></profiles></project>"
	}
	const on = "<activation><activeByDefault>true</activeByDefault></activation>"
	plugin := func(inside string) string {
		return "<build><plugins><plugin><artifactId>p</artifactId><configuration>" + inside + "</configuration></plugin></plugins></build>"
	}
	for name, c := range map[string]struct{ profile, want string }{
		"the neighbour: activation first":                {on + "<properties><gv>31</gv></properties>", "31"},
		"properties written before the activation":       {"<properties><gv>31</gv></properties>" + on, "31"},
		"a build written before the activation":          {plugin("<x>1</x>") + on + "<properties><gv>31</gv></properties>", "31"},
		"TRUE, with space around it":                     {"<activation><activeByDefault> TRUE </activeByDefault></activation><properties><gv>31</gv></properties>", "31"},
		"properties in a plugin's configuration only":    {on + plugin("<properties><gv>99</gv></properties>"), "30"},
		"an activation in a plugin's configuration only": {plugin("<activation><activeByDefault>true</activeByDefault></activation>") + "<properties><gv>31</gv></properties>", "30"},
		"not active": {"<properties><gv>31</gv></properties>", "30"},
		"the plugin's properties before the profile's own one": {on + plugin("<properties><gv>99</gv></properties>") + "<properties><gv>31</gv></properties>", "31"},
	} {
		alone := ""
		for _, d := range ParsePomXMLVersions(pom(c.profile)) {
			if d.Name == "g:x" {
				alone = d.Version
			}
		}
		// g:m has no version of its own: its version is the managed
		// entry's, which the tree's reader works out from the pom's model
		// (the decoded document), not from the file's text.
		tree := r6Versions(t, oneBuild(map[string]string{"pom.xml": pom(c.profile)}), "pom.xml", &r8Counting{})["g:m"].Version
		if alone != c.want || tree != c.want {
			t.Errorf("%s: the file alone gives g:x %q, the tree gives the managed g:m %q; want %q from both", name, alone, tree, c.want)
		}
	}
}

// Round 10 (RD10-8). A default profile's plain managed entry replaces the
// pom's plain entry for the artifact, and never the pom's IMPORT of a BOM
// that has the same group and artifact.
func TestAProfilesPlainEntryDoesNotReplaceAnImportOfTheSameCoordinates(t *testing.T) {
	files := oneBuild(map[string]string{
		"bom/pom.xml": "<project><groupId>g</groupId><artifactId>x</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" + r4dep("g", "y", "5") + "</dependencies></dependencyManagement></project>",
		"app/pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement><dependencies>" +
			r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies><profiles><profile><id>p</id><activation><activeByDefault>true</activeByDefault></activation><dependencyManagement><dependencies>" +
			r4dep("g", "x", "9") + "</dependencies></dependencyManagement></profile></profiles></project>",
	})
	got := r6Versions(t, files, "app/pom.xml", &r8Counting{})
	if x, y := got["g:x"], got["g:y"]; x.Version != "9" || y.Version != "5" {
		t.Errorf("g:x at %q (%s), g:y at %q (%s); want the profile's 9 and, through the import that still stands, 5", x.Version, x.VersionReason, y.Version, y.VersionReason)
	}
}

// Round 10 (RD10-4, RD10-5, RD10-6). Spring's plugin takes a project
// property over a BOM's only where the file surely sets it: at its top,
// once, to a literal. Set in any other way the build sets it and to what is
// not known: no version, and the reason says so (a BOM's own value was
// stored as if the build set none, or no version with no reason, or a
// value from inside a subprojects block or a condition). An entry of a map
// is no property: the BOM's value stands.
func TestASpringPropertyIsTakenOnlyWhereTheFileSurelySetsIt(t *testing.T) {
	const notRead = model.VersionReasonValueNotRead
	for name, c := range map[string]struct {
		files     map[string]string
		rel       string
		x, reason string
	}{
		"the neighbour: set at the top":    {r8Build(r9Plugins + "ext['x.version'] = '2.17.1'\n" + r9Deps), "build.gradle", "2.17.1", ""},
		"a value that is not a literal":    {r8Build(r9Plugins + "ext['x.version'] = System.getenv('X') ?: '2.17.1'\n" + r9Deps), "build.gradle", "", notRead},
		"a call":                           {r8Build(r9Plugins + "ext['x.version'] = computeIt()\n" + r9Deps), "build.gradle", "", notRead},
		"the two branches of a condition":  {r8Build(r9Plugins + "if (a) {\n ext['x.version'] = '2.17.1'\n} else {\n ext['x.version'] = '2.12.4'\n}\n" + r9Deps), "build.gradle", "", notRead},
		"under a condition":                {r8Build(r9Plugins + "if (a) {\n ext['x.version'] = '2.17.1'\n}\n" + r9Deps), "build.gradle", "", notRead},
		"twice at the top":                 {r8Build(r9Plugins + "ext['x.version'] = '2.17.1'\next['x.version'] = '2.18.0'\n" + r9Deps), "build.gradle", "", notRead},
		"inside subprojects, for the root": {r8Build(r9Plugins + "subprojects {\n ext['x.version'] = '2.17.1'\n}\n" + r9Deps), "build.gradle", "", notRead},
		"an entry of a map":                {r8Build(r9Plugins + "ext.x = [version: '9']\n" + r9Deps), "build.gradle", "2.14.1", ""},
	} {
		got := r8Gradle(t, r9Boot(), c.files, c.rel)
		if x, plain := got["g:x"], got["g:plain"]; x.Version != c.x || x.VersionReason != c.reason || plain.Version != "1.0" {
			t.Errorf("%s: g:x at %q (%q), want %q (%q); g:plain, which no property of the project touches, at %q, want 1.0", name, x.Version, x.VersionReason, c.x, c.reason, plain.Version)
		}
	}
}
