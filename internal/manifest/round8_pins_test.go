// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"strings"
	"testing"
)

// Review round 8, R8-11: changes the suite did not notice.

// M22. A pom's managed entries whose group holds a reference are filed by
// their artifact, and a list of more than pomRefListSmall is answered
// through a cache. The cache is keyed by the list: two long lists of one
// pom, for two artifacts, asked in one reading, each give their own entry.
func TestTwoLongReferenceKeyedListsOfOnePomEachAnswerForTheirOwn(t *testing.T) {
	var managed strings.Builder
	for _, artifact := range []string{"a1", "a2"} {
		for i := 0; i < pomRefListSmall+2; i++ {
			managed.WriteString(r4dep("${nope}", artifact, fmt.Sprintf("0.%d", i)))
		}
	}
	managed.WriteString(r4dep("${grp}", "a2", "2.2") + r4dep("${grp}", "a1", "1.1"))
	pom := "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><properties><grp>gg</grp></properties>" +
		"<dependencyManagement><dependencies>" + managed.String() + "</dependencies></dependencyManagement>" +
		"<dependencies>" + r4dep("gg", "a1", "") + r4dep("gg", "a2", "") + r4dep("gg", "a1", "") + "</dependencies></project>"
	root := writeTree(t, map[string]string{"pom.xml": pom})
	var got []string
	for _, d := range NewWalk(root, nil).Read(root+"/pom.xml", Options{}).Deps {
		if !d.ManagedEntry {
			got = append(got, d.Name+"@"+d.Version)
		}
	}
	if want := "[gg:a1@1.1 gg:a2@2.2 gg:a1@1.1]"; fmt.Sprint(got) != want {
		t.Errorf("read %v, want %s", got, want)
	}
}

// M26, restated in 0.29.216. No pom of the clone is a Gradle platform, so
// two of them with a platform's coordinates are not two candidates: the
// repositories answer, and their "not found" is an answer. (Until then the
// two made the platform not known, and one alone was taken for it.)
func TestTwoPomsOfTheCloneWithAPlatformsCoordinatesAreNotCandidates(t *testing.T) {
	files := map[string]string{
		"build.gradle": "dependencies {\n implementation platform('t:dup:1')\n implementation platform('t:second:1')\n implementation 'g:x'\n}\n",
		"dup1/pom.xml": r6Bom("dup", "g", "x", "1.0"),
		"dup2/pom.xml": r6Bom("dup", "g", "x", "3.0"),
	}
	second := &RemotePom{Group: "t", Artifact: "second", Version: "1", Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "2.0"}}}
	dup := &RemotePom{Group: "t", Artifact: "dup", Version: "1", Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "2.5"}}}
	for name, c := range map[string]struct {
		has  map[string]*RemotePom
		want string
	}{
		"on no repository": {map[string]*RemotePom{"t:second:1": second}, "2.0"},
		"published":        {map[string]*RemotePom{"t:second:1": second, "t:dup:1": dup}, "2.5"},
	} {
		if x := r6Versions(t, files, "build.gradle", outcomeSource{RemoteNotFound, c.has})["g:x"]; x.Version != c.want {
			t.Errorf("%s: g:x at %q (%s); want %s", name, x.Version, x.VersionReason, c.want)
		}
	}
	files["build.gradle"] = "dependencyManagement {\n imports {\n  mavenBom 't:second:1'\n  mavenBom 't:dup:1'\n }\n}\ndependencies {\n implementation 'g:x'\n}\n"
	if x := r6Versions(t, files, "build.gradle", outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:second:1": second}})["g:x"]; x.Version != "2.0" {
		t.Errorf("a later mavenBom two poms of the clone name: g:x at %q (%s); want 2.0", x.Version, x.VersionReason)
	}
}

// M42. A name a TOML table declares in several keys has one row, and its
// requirement is the declarations as written, joined with "; ": the text
// the inventory stores and the GUI shows.
func TestATOMLNameDeclaredInSeveralKeysKeepsEveryDeclaration(t *testing.T) {
	got := map[string]string{}
	for _, e := range scanTOMLDepTables("[dependencies]\nserde.version = \"1.0\"\nserde.features = [\"derive\"]\nplain = \"2\"\n", map[string]bool{"[dependencies]": true}) {
		got[e.Name] = e.Raw
	}
	if want := `serde.version = "1.0"; serde.features = ["derive"]`; got["serde"] != want {
		t.Errorf("serde's requirement is %q, want %q", got["serde"], want)
	}
	if want := `plain = "2"`; got["plain"] != want {
		t.Errorf("plain's requirement is %q, want %q", got["plain"], want)
	}
}
