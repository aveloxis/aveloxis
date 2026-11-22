// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Round 11 (RD11-1). A <properties> element in the configuration of a
// plugin under a profile's <build> or <reporting> is the plugin's: it is no
// property of the profile and none of the pom. 0.29.217 read it into the
// pom's own properties, the first to set a name winning, so it stood over
// the pom's own <properties> wherever those are written after <profiles>,
// and gave a value to a name the pom never sets; whether the profile is
// active made no difference. The file's reader and the tree's agree.
func TestAPluginsPropertiesUnderAProfileAreNoPropertiesOfThePom(t *testing.T) {
	const on = "<activation><activeByDefault>true</activeByDefault></activation>"
	plugin := func(section, value string) string {
		return "<" + section + "><plugins><plugin><artifactId>p</artifactId><configuration><properties><gv>" + value + "</gv></properties></configuration></plugin></plugins></" + section + ">"
	}
	pom := func(profile, own string) string {
		return "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><profiles><profile><id>p</id>" + profile + "</profile></profiles>" + own +
			"<dependencyManagement><dependencies>" + r4dep("g", "m", "${gv}") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "${gv}") + r4dep("g", "m", "") + "</dependencies></project>"
	}
	const own = "<properties><gv>30</gv></properties>"
	alone := func(content string) string {
		for _, d := range ParsePomXMLVersions(content) {
			if d.Name == "g:x" {
				return d.Version
			}
		}
		return "(no g:x)"
	}
	// What a reference to a property the pom does not set reads as: the
	// neighbour every "the pom never sets it" case below is held to.
	unset := alone(pom("", ""))
	for name, c := range map[string]struct{ profile, own, want string }{
		"the neighbour: no plugin properties":                    {"", own, "30"},
		"an inactive profile's build, before the pom's own":      {plugin("build", "439"), own, "30"},
		"an inactive profile's reporting, before the pom's own":  {plugin("reporting", "439"), own, "30"},
		"an active profile's build, before the pom's own":        {on + plugin("build", "439"), own, "30"},
		"an inactive profile's build, and the pom sets no value": {plugin("build", "429"), "", unset},
		"an active profile's build, and the pom sets no value":   {on + plugin("build", "429"), "", unset},
		"an active profile's own property still stands":          {on + "<properties><gv>31</gv></properties>" + plugin("build", "439"), own, "31"},
	} {
		content := pom(c.profile, c.own)
		if got := alone(content); got != c.want {
			t.Errorf("%s: the file alone gives g:x %q, want %q", name, got, c.want)
		}
		if c.own == "" {
			continue // the tree's reader has no version to give g:m either way
		}
		if tree := r6Versions(t, oneBuild(map[string]string{"pom.xml": content}), "pom.xml", &r8Counting{})["g:m"].Version; tree != c.want {
			t.Errorf("%s: the tree gives the managed g:m %q, want %q", name, tree, c.want)
		}
	}
}

// Round 11 (RD11-3, RD11-7). A name is the entry of a map only when the
// file assigns it in no other way: a property assigned surely stands beside
// a map of the same spelling (ext.x = [version: ..] is the property x, not
// x.version), and an assignment that is not read stays not read beside one.
// 0.29.217 asked "is it a map entry" first, so the map discarded the
// property. The last two rows pin what the order of the questions rests
// on: a literal followed by a value that is not read is not read.
func TestAMapOfTheSameSpellingDoesNotHideAPropertyOfTheProject(t *testing.T) {
	const notRead = model.VersionReasonValueNotRead
	for name, c := range map[string]struct {
		body      string
		x, reason string
	}{
		"the neighbour: the map alone":             {"ext.x = [version: '9']\n", "2.14.1", ""},
		"a sure property, then the map":            {"ext['x.version'] = '2.17.2'\next.x = [version: '9']\n", "2.17.2", ""},
		"the map, then a sure property":            {"ext.x = [version: '9']\next['x.version'] = '2.17.2'\n", "2.17.2", ""},
		"the map and a property under a condition": {"ext.x = [version: '9']\nif (c) {\n ext['x.version'] = '2.17.2'\n}\n", "", notRead},
		"the map and a property that is not read":  {"ext.x = [version: '9']\next['x.version'] = computeIt()\n", "", notRead},
		"the map, a literal, then one not read":    {"ext.x = [version: '9']\next['x.version'] = '2.17.2'\next['x.version'] = computeIt()\n", "", notRead},
		"the map, an empty literal, one not read":  {"ext.x = [version: '9']\next['x.version'] = ''\next['x.version'] = computeIt()\n", "", notRead},
		"the map and two different literals":       {"ext.x = [version: '9']\next['x.version'] = '2.17.2'\next['x.version'] = '2.18.0'\n", "", notRead},
		"the map and the same literal twice":       {"ext.x = [version: '9']\next['x.version'] = '2.17.2'\next['x.version'] = '2.17.2'\n", "2.17.2", ""},
		"the map and a literal inside subprojects": {"ext.x = [version: '9']\nsubprojects {\n ext['x.version'] = '2.17.2'\n}\n", "", notRead},
		"a literal, then a value that is not read": {"ext['x.version'] = '2.17.1'\next['x.version'] = computeIt()\n", "", notRead},
		"a value that is not read, then a literal": {"ext['x.version'] = computeIt()\next['x.version'] = '2.17.1'\n", "", notRead},
	} {
		got := r8Gradle(t, r9Boot(), r8Build(r9Plugins+c.body+r9Deps), "build.gradle")
		if x, plain := got["g:x"], got["g:plain"]; x.Version != c.x || x.VersionReason != c.reason || plain.Version != "1.0" {
			t.Errorf("%s: g:x at %q (%q), want %q (%q); g:plain at %q, want 1.0", name, x.Version, x.VersionReason, c.x, c.reason, plain.Version)
		}
	}
}

// Round 11 (RD11-4, RD11-5). The property that is not read is the VERSION
// OF A BOM the Spring BOM imports (jackson-bom.version and its like): what
// that BOM manages has no version, and the reason is that the value was not
// read, as for a property that is an artifact's own version. 0.29.217 said
// "not found", as if a repository had been asked and had no such pom. And
// none is asked: a coordinate that holds the mark of an unread value is no
// coordinate.
func TestABOMWhoseVersionIsNotReadIsNotAskedForAndSaysSo(t *testing.T) {
	src := r9Boot()
	got := r8Gradle(t, src, r8Build(r9Plugins+"ext['j-bom.version'] = computeIt()\n"+r9Deps), "build.gradle")
	if j := got["g:j"]; j.Version != "" || j.VersionReason != model.VersionReasonValueNotRead {
		t.Errorf("g:j, managed by the BOM whose version is not read, at %q (%q); want no version and %q", j.Version, j.VersionReason, model.VersionReasonValueNotRead)
	}
	if x := got["g:x"]; x.Version != "2.14.1" || x.VersionReason != "" {
		t.Errorf("g:x, which the unread property does not touch, at %q (%q); want 2.14.1 from the BOM", x.Version, x.VersionReason)
	}
	if len(src.asked) == 0 {
		t.Fatal("no pom was asked for: the Boot BOM itself is, and this test would say nothing")
	}
	for _, key := range src.asked {
		if strings.ContainsRune(key, 0) {
			t.Errorf("a repository was asked for %q: the mark of a value that is not read is in the coordinate", key)
		}
	}
	// The neighbour: the same build with the property read asks for that BOM.
	src = r9Boot()
	if j := r8Gradle(t, src, r8Build(r9Plugins+"ext['j-bom.version'] = '2'\n"+r9Deps), "build.gradle")["g:j"]; j.Version != "2.2" {
		t.Errorf("with the property read, g:j at %q (%q); want 2.2 from j-bom 2", j.Version, j.VersionReason)
	}
}
