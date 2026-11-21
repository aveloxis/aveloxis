// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 6 (M6-10): answers a wrong-answer mutant changed and no test
// noticed. Each test here is one of those mutants, by the reviewer's name
// for it.

func r6Read(t *testing.T, files map[string]string, rel string, src PomSource) map[string]model.LibyearDep {
	t.Helper()
	return r6Versions(t, files, rel, src)
}

// A2. A BOM is asked of the repositories the importing pom's chain
// declares, the pom's OWN first: the merged list of a chain in which two
// poms declare starts with the nearest.
func TestABOMIsAskedOfTheChildsOwnRepositoriesFirst(t *testing.T) {
	repo := func(u string) string {
		return "<repositories><repository><url>" + u + "</url></repository></repositories>"
	}
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version>" + repo("https://root.example/m2") + "</project>",
		"app/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>app</artifactId>" + repo("https://app.example/m2") +
			"<dependencyManagement><dependencies>" + r4import("org.b", "bom", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
	}
	root := writeTree(t, files)
	src := &declaredRecorder{}
	w := NewWalk(root, nil)
	w.SetPomSource(src)
	w.Read(filepath.Join(root, "app", "pom.xml"), Options{})
	if got, want := src.asked["org.b:bom:1"], []string{"https://app.example/m2", "https://root.example/m2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the BOM was asked of %v, want %v", got, want)
	}
}

// B1. A project's platforms are its own and the ones the build files above
// give every project, settled together: the highest recommendation wins,
// whichever of them gives it. (Until 0.29.214 the project's own were
// consulted first and the first to answer won, and until 0.29.215 this test
// still said so: its fixture had the higher version in the project's own
// platform, so both rules passed it. Round 8.)
func TestAProjectsOwnPlatformsAndTheOnesFromAboveAreSettledTogether(t *testing.T) {
	for name, c := range map[string]struct{ above, own, want string }{
		"the project's own is the higher":  {"1.0", "2.0", "2.0"},
		"the one from above is the higher": {"2.0", "1.0", "2.0"},
	} {
		files := map[string]string{
			"settings.gradle":  "rootProject.name = 'r'\n",
			"build.gradle":     "subprojects {\n  dependencies {\n    implementation platform('t:above:1')\n  }\n}\n",
			"app/build.gradle": "dependencies {\n  implementation platform('t:own:1')\n  implementation 'g:x'\n  implementation 'g:onlyabove'\n}\n",
			"above/pom.xml":    r6Bom("above", "g", "x", c.above, "g", "onlyabove", "5.0"),
			"own/pom.xml":      r6Bom("own", "g", "x", c.own),
		}
		got := r6Read(t, files, "app/build.gradle", nil)
		if got["g:x"].Version != c.want || got["g:onlyabove"].Version != "5.0" {
			t.Errorf("%s: g:x at %q and g:onlyabove at %q; want %s (the highest) and 5.0 (from above)", name, got["g:x"].Version, got["g:onlyabove"].Version, c.want)
		}
	}
}

// B7. The sources of a buildSrc are read twice, so a constant built from one
// declared in a file read after it is settled.
func TestABuildSrcConstantFromALaterFileIsSettled(t *testing.T) {
	files := map[string]string{
		"settings.gradle":               "rootProject.name = 'r'\n",
		"buildSrc/src/main/kotlin/A.kt": "object A {\n  const val coord = \"g:fromlater:${Z.v}\"\n}\n",
		"buildSrc/src/main/kotlin/Z.kt": "object Z {\n  const val v = \"4.5.6\"\n}\n",
		"app/build.gradle":              "dependencies {\n  implementation A.coord\n}\n",
	}
	got := r6Read(t, files, "app/build.gradle", nil)
	if got["g:fromlater"].Version != "4.5.6" {
		t.Errorf("g:fromlater at %q; want 4.5.6, a constant of the file read second", got["g:fromlater"].Version)
	}
}

// B9. A catalogue's rich version is the required one before the preferred.
func TestACatalogueVersionIsTheRequiredOneBeforeThePreferred(t *testing.T) {
	files := map[string]string{
		"settings.gradle": "rootProject.name = 'r'\n",
		"gradle/libs.versions.toml": "[versions]\nboth = { require = \"1.0\", prefer = \"2.0\" }\nstrict = { strictly = \"3.0\", require = \"1.0\", prefer = \"2.0\" }\nalone = { prefer = \"7.0\" }\n" +
			"[libraries]\nb = { module = \"g:both\", version.ref = \"both\" }\ns = { module = \"g:strict\", version.ref = \"strict\" }\na = { module = \"g:alone\", version.ref = \"alone\" }\n",
		"app/build.gradle": "dependencies {\n  implementation libs.b\n  implementation libs.s\n  implementation libs.a\n}\n",
	}
	got := r6Read(t, files, "app/build.gradle", nil)
	for name, want := range map[string]string{"g:both": "1.0", "g:strict": "3.0", "g:alone": "7.0"} {
		if got[name].Version != want {
			t.Errorf("%s at %q, want %q", name, got[name].Version, want)
		}
	}
}

// B10. The root of a build is the nearest directory with a settings file,
// settings.gradle.kts as much as settings.gradle: $rootDir in a nested build
// is that build's root.
func TestTheRootOfABuildIsWhereItsKotlinSettingsFileIs(t *testing.T) {
	files := map[string]string{
		"gradle/v.gradle":            "ext.v = 'outer'\n",
		"nested/settings.gradle.kts": "rootProject.name = \"nested\"\n",
		"nested/gradle/v.gradle":     "ext.v = '1.2.3'\n",
		"nested/app/build.gradle":    "apply from: \"$rootDir/gradle/v.gradle\"\ndependencies {\n  implementation \"g:x:$v\"\n}\n",
	}
	got := r6Read(t, files, "nested/app/build.gradle", nil)
	if got["g:x"].Version != "1.2.3" {
		t.Errorf("g:x at %q; want 1.2.3, from the script under the nested build's own root", got["g:x"].Version)
	}
}

// B12. The build file above a project is read with its directory's
// catalogue: a platform it gives every project by a catalogue accessor is
// applied.
func TestAPlatformNamedByTheCatalogueInABuildFileAboveIsApplied(t *testing.T) {
	files := map[string]string{
		"settings.gradle":           "rootProject.name = 'r'\n",
		"gradle/libs.versions.toml": "[libraries]\nbom = \"t:shared:1\"\n",
		"build.gradle":              "subprojects {\n  dependencies {\n    implementation platform(libs.bom)\n  }\n}\n",
		"app/build.gradle":          "dependencies {\n  implementation 'g:x'\n}\n",
		"shared/pom.xml":            r6Bom("shared", "g", "x", "6.0"),
	}
	got := r6Read(t, files, "app/build.gradle", nil)
	if got["g:x"].Version != "6.0" {
		t.Errorf("g:x at %q; want 6.0 from the platform the root names through its catalogue", got["g:x"].Version)
	}
}

// A catalogue's library may give its version as a table of its own, two
// tables deep (gradleCatalogueDepth is that depth and no less).
func TestACatalogueLibrarysRichVersionIsRead(t *testing.T) {
	files := map[string]string{
		"settings.gradle":           "rootProject.name = 'r'\n",
		"gradle/libs.versions.toml": "[libraries]\nrich = { module = \"g:rich\", version = { strictly = \"4.4\" } }\nplain = { group = \"g\", name = \"plain\", version = \"1.1\" }\n",
		"app/build.gradle":          "dependencies {\n  implementation libs.rich\n  implementation libs.plain\n}\n",
	}
	got := r6Read(t, files, "app/build.gradle", nil)
	if got["g:rich"].Version != "4.4" || got["g:plain"].Version != "1.1" {
		t.Errorf("g:rich at %q and g:plain at %q; want 4.4 and 1.1", got["g:rich"].Version, got["g:plain"].Version)
	}
}

// B13. What a platform gives a dependency is applied only when it is one
// version: a range is not. And a platform that manages the dependency at a
// range HAS answered for it (round 7): another platform's version is not
// the answer, which is whatever Gradle resolves the range and it to.
func TestAPlatformsRangeIsNotAVersion(t *testing.T) {
	files := map[string]string{
		"build.gradle":   "dependencies {\n  implementation platform('t:first:1')\n  implementation platform('t:second:1')\n  implementation 'g:ranged'\n  implementation 'g:only'\n}\n",
		"first/pom.xml":  r6Bom("first", "g", "ranged", "[1.0,2.0)", "g", "only", "[3,4)"),
		"second/pom.xml": r6Bom("second", "g", "ranged", "1.5"),
	}
	got := r6Read(t, files, "build.gradle", nil)
	if got["g:ranged"].Version != "" || got["g:only"].Version != "" {
		t.Errorf("g:ranged at %q and g:only at %q; want none for both", got["g:ranged"].Version, got["g:only"].Version)
	}
}

// A3 (as it now stands). A pom that was NOT ASKED FOR, because the walk had
// asked for as many as it may, is a lookup that failed and never "not
// found" (SR-5).
func TestAPomNotAskedForIsALookupThatFailed(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "b1", "1") + r4import("t", "b2", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	w.SetPomSource(outcomeSource{outcome: RemoteNotFound})
	w.SetRemotePomLimit(1)
	got := w.Read(filepath.Join(root, "pom.xml"), Options{})
	if len(got.Deps) == 0 {
		t.Fatal("nothing read")
	}
	for _, d := range got.Deps {
		if d.Name == "g:x" && d.VersionReason != model.VersionReasonInheritedLookupFailed {
			t.Errorf("g:x says %q; want %q: the second BOM was never asked about", d.VersionReason, model.VersionReasonInheritedLookupFailed)
		}
	}
	if w.RemotePomsNotAsked() != 1 {
		t.Errorf("%d poms counted as not asked for, want 1", w.RemotePomsNotAsked())
	}
}
