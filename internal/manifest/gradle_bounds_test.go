// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// The Gradle reader's work is bounded by the size of what it reads (review
// round 1 of the vulnerability program's first part). A build file is
// written by whoever owns the repository being collected, and several
// shapes made the reader's work grow with the SQUARE of the file: 60 KB of
// `{y(` took 15 seconds, 600 KB about 25 minutes, on a collection worker.

// gradleWithin reads one build file through the walk and fails the test if
// the read does not return in time.
func gradleWithin(t *testing.T, name, content string, limit time.Duration) Declarations {
	t.Helper()
	root := writeTree(t, map[string]string{name: content})
	done := make(chan Declarations, 1)
	go func() { done <- NewWalk(root, nil).Read(filepath.Join(root, name), Options{}) }()
	select {
	case d := <-done:
		return d
	case <-time.After(limit):
		t.Fatalf("%s (%d bytes) was not read within %s", name, len(content), limit)
		return Declarations{}
	}
}

func TestHostileGradleShapesAreReadInBoundedTime(t *testing.T) {
	const n = 150_000
	for name, content := range map[string]string{
		"an opening brace and an unclosed call, repeated": strings.Repeat("{y(", n),
		"a closing brace and an unclosed call, repeated":  strings.Repeat("}y(", n),
		"lines that each end in a comma":                  strings.Repeat("a,\n", n),
		"lines that each continue a concatenation":        strings.Repeat("a = 'x' +\n", n/2),
		"calls nested inside closing braces":              strings.Repeat("} a (", n) + strings.Repeat(")", n),
		"blocks nested without end":                       strings.Repeat("x {\n", n),
		"conditions nested inside a dependencies block":   "dependencies {\n" + strings.Repeat("if ( {\n", n),
		"allprojects nested without end":                  strings.Repeat("allprojects {\n", n/3) + "dependencies { implementation 'a:b:1' }\n",
		"one string with a long dotted reference":         `def v = "$` + strings.Repeat("a.", 1300) + `a"` + "\n" + strings.Repeat(`def w = "$`+strings.Repeat("a.", 1300)+`a"`+"\n", 200),
	} {
		t.Run(name, func(t *testing.T) {
			gradleWithin(t, "build.gradle", content, 20*time.Second)
		})
	}
}

// A variable that is another one twice over, line after line, doubles each
// time: forty lines would be sixteen terabytes. What comes out of an
// interpolation is bounded, as it is for a pom's properties. (Twenty-two
// lines here, 64 MB unbounded: enough to show it, and a run of this test
// against a reader with no bound still ends.)
func TestGradleInterpolationIsBoundedInSize(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("def v0 = 'xxxxxxxxxxxxxxxx'\n")
	const doublings = 22
	for i := 1; i <= doublings; i++ {
		fmt.Fprintf(&sb, "def v%d = \"${v%d}${v%d}\"\n", i, i-1, i-1)
	}
	sb.WriteString("def ok = '1.2.3'\ndependencies {\n  implementation \"com.example:bomb:${v22}\"\n  implementation \"com.example:fine:${ok}\"\n}\n")
	d := gradleWithin(t, "build.gradle", sb.String(), 20*time.Second)
	got := map[string]string{}
	for _, dep := range d.Deps {
		got[dep.Name] = dep.Version
	}
	if v, ok := got["com.example:bomb"]; !ok || len(v) > pkgid.MaxNameBytes {
		t.Errorf("com.example:bomb: present %v, version of %d bytes; want the dependency kept, with no version longer than %d bytes", ok, len(v), pkgid.MaxNameBytes)
	}
	if gradleInterpolatedLimit != pkgid.MaxNameBytes {
		t.Errorf("gradleInterpolatedLimit = %d, want the longest value that can be stored (%d)", gradleInterpolatedLimit, pkgid.MaxNameBytes)
	}
	if got["com.example:fine"] != "1.2.3" {
		t.Errorf("com.example:fine = %q, want 1.2.3", got["com.example:fine"])
	}
}

// A build whose reading passes the bound is reported as a manifest that
// could not be parsed: said, never a silent partial inventory.
func TestAGradleBuildPastItsWorkBoundIsAReadFailure(t *testing.T) {
	d := gradleWithin(t, "build.gradle", "dependencies { implementation 'a:b:1' }\n"+strings.Repeat("} a (", 50_000)+strings.Repeat(")", 50_000), 20*time.Second)
	if d.Failure == nil || d.Failure.Reason != model.ManifestFailedParse || !errors.Is(d.Failure.Err, errGradleWorkBound) {
		t.Fatalf("failure = %+v, want a parse failure for the work bound", d.Failure)
	}
	if len(d.Deps) != 0 {
		t.Errorf("a build that was not read whole reported %d dependencies; a partial list reads as a whole one", len(d.Deps))
	}
}

// Real builds use a small part of the bound: the factor is not a limit any
// of them is near. The corpus's build files and a large generated one.
func TestRealGradleBuildsAreFarFromTheWorkBound(t *testing.T) {
	builds := map[string]string{}
	for _, name := range []string{"build.gradle", "build.gradle.kts"} {
		if data, err := os.ReadFile(filepath.Join(corpusDir, name)); err == nil {
			builds[name] = string(data)
		}
	}
	if len(builds) == 0 {
		t.Fatal("the corpus has no Gradle build file: the test would examine nothing")
	}
	// A large multi-project root: nested blocks, a long ext map, many
	// declarations, continuation lines.
	var big strings.Builder
	big.WriteString("ext {\n  versions = [\n")
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&big, "    lib%d: '1.%d.0',\n", i, i)
	}
	big.WriteString("  ]\n}\nallprojects {\n  repositories { mavenCentral() }\n  subprojects {\n    configurations.all {\n      resolutionStrategy {\n        eachDependency { details ->\n          if (details.requested.group == 'x') { details.useVersion '1' }\n        }\n      }\n    }\n    dependencies {\n")
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&big, "      implementation group: 'com.example',\n                     name: 'lib%d',\n                     version: \"${versions.lib%d}\"\n", i, i)
		fmt.Fprintf(&big, "      testImplementation(\"com.example:t%d:${versions.lib%d}\") {\n        exclude group: 'junit'\n      }\n", i, i)
	}
	big.WriteString("    }\n  }\n}\n")
	builds["generated"] = big.String()
	for name, content := range builds {
		b := readGradleBuild(content, nil)
		if b.overBound {
			t.Fatalf("%s passed the work bound", name)
		}
		if b.work*gradleWorkHeadroom > b.workLimit {
			t.Errorf("%s used %d of its bound of %d: more than one part in %d", name, b.work, b.workLimit, gradleWorkHeadroom)
		}
		t.Logf("%s: %d bytes, work %d, bound %d", name, len(content), b.work, b.workLimit)
	}
	if b := readGradleBuild(builds["generated"], nil); len(b.decls) != 800 {
		t.Errorf("the generated build has %d declarations read, want 800", len(b.decls))
	}
}

// gradleWorkHeadroom is the margin the test above asks for.
const gradleWorkHeadroom = 4

// A build file larger than a build file is, is not read.
func TestAnOversizedBuildFileIsNotRead(t *testing.T) {
	huge := "ext.v = '9.9'\n" + strings.Repeat("// padding\n", int(MaxBuildFileBytes)/11+1)
	root := writeTree(t, map[string]string{
		"big.gradle":   huge,
		"build.gradle": "apply from: 'big.gradle'\ndependencies { implementation \"a:b:${v}\"\n implementation 'c:d:1' }\n",
		"sub/pom.xml":  `<project><parent><groupId>p</groupId><artifactId>parent</artifactId><version>1</version><relativePath>../big/pom.xml</relativePath></parent><artifactId>c</artifactId><dependencies><dependency><groupId>a</groupId><artifactId>b</artifactId><version>${v}</version></dependency></dependencies></project>`,
		"big/pom.xml":  `<project><groupId>p</groupId><artifactId>parent</artifactId><version>1</version><properties><v>9.9</v></properties><!--` + strings.Repeat("padding ", int(MaxBuildFileBytes)/8+1) + `--></project>`,
	})
	w := NewWalk(root, nil)
	for _, d := range w.Read(filepath.Join(root, "build.gradle"), Options{}).Deps {
		if d.Version == "9.9" {
			t.Errorf("a value from an oversized applied file was used: %+v", d)
		}
	}
	for _, d := range w.Read(filepath.Join(root, "sub", "pom.xml"), Options{}).Deps {
		if d.Version == "9.9" {
			t.Errorf("a value from an oversized parent pom was used: %+v", d)
		}
	}
	// One byte under the bound is read.
	if !regularBuildFile(root, filepath.Join(root, "build.gradle")) || regularBuildFile(root, filepath.Join(root, "big.gradle")) {
		t.Error("regularBuildFile: the small file must pass and the oversized one must not")
	}
}

// Many <build></build> blocks: removing each used to copy the rest of the
// document.
func TestPomPropertiesIsLinear(t *testing.T) {
	content := `<project><groupId>g</groupId><artifactId>a</artifactId><version>7</version>` + strings.Repeat("<build></build>", 60_000) + `<properties><x>1</x></properties></project>`
	done := make(chan map[string]string, 1)
	go func() { done <- pomProperties(content) }()
	select {
	case props := <-done:
		if props["project.version"] != "7" || props["x"] != "1" || props["project.artifactId"] != "a" {
			t.Errorf("properties = %v", props)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("pomProperties did not finish")
	}
	// What is inside a nested block is not the project's own.
	p := pomProperties(`<project><build><version>no</version></build><dependencies><groupId>no</groupId></dependencies><groupId>g</groupId><version>2</version><build><artifactId>no</artifactId></build><artifactId>a</artifactId></project>`)
	if p["project.version"] != "2" || p["project.groupId"] != "g" || p["project.artifactId"] != "a" {
		t.Errorf("own coordinates = %v", p)
	}
}

// A build file that was not read whole gives nothing to the files that
// build on it: a variable it assigned before the reader stopped is not a
// variable of the build. And those files are then not read either
// (0.29.209): a project read without the file that gives it its versions,
// platforms and repositories reads as a whole build with other versions.
// The project is reported as a manifest that could not be read, and both
// the report and the log name the file that was at fault.
func TestAnOverBoundBuildGivesNothingToTheProjectsBelow(t *testing.T) {
	hostile := "ext.v = '9.9'\nsubprojects { repositories { maven { url 'https://half.example/m2' } } }\n" + strings.Repeat("} a (", 50_000) + strings.Repeat(")", 50_000)
	root := writeTree(t, map[string]string{
		"build.gradle":     hostile,
		"app/build.gradle": "dependencies {\n  implementation \"a:b:${v}\"\n  implementation 'c:d:1'\n}\n",
	})
	var log bytes.Buffer
	w := NewWalk(root, slog.New(slog.NewTextHandler(&log, nil)))
	got := w.Read(filepath.Join(root, "app", "build.gradle"), Options{})
	if len(got.Deps) != 0 || got.Failure == nil || !strings.Contains(got.Failure.Error(), "build.gradle") {
		t.Errorf("the project below: %d dependencies, failure %v; want none, and a failure that names the root build file", len(got.Deps), got.Failure)
	}
	if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), filepath.Join(root, "build.gradle")) {
		t.Errorf("the log does not name the file that could not be read:\n%s", log.String())
	}
	for _, r := range w.DeclaredRepositories() {
		if strings.Contains(r.URL, "half.example") {
			t.Errorf("a repository from a build file that was not read whole was recorded: %+v", r)
		}
	}
}

// The same for every other file a build file is read with: the settings
// file, a script it applies, a buildSrc source and a catalogue, over the
// work bound or over the size bound. Each is named in the log, and the
// build file that depends on it is a read failure that names it. A build
// file beside them that depends on none of them is read as before.
func TestAFileOfTheBuildThatCannotBeReadFailsWhatDependsOnIt(t *testing.T) {
	hostile := strings.Repeat("} a (", 50_000) + strings.Repeat(")", 50_000)
	tooBig := "ext.v = '9.9'\n" + strings.Repeat("// padding\n", int(MaxBuildFileBytes)/11+1)
	app := "dependencies {\n  implementation 'c:d:1'\n}\n"
	for what, c := range map[string]struct {
		files map[string]string
		part  string
	}{
		"a settings file past the work bound": {map[string]string{"settings.gradle": hostile, "app/build.gradle": app}, "settings.gradle"},
		"a settings file over the size bound": {map[string]string{"settings.gradle.kts": tooBig, "app/build.gradle": app}, "settings.gradle.kts"},
		"an applied script past the work bound": {map[string]string{"gradle/versions.gradle": hostile,
			"app/build.gradle": "apply from: '../gradle/versions.gradle'\n" + app}, "gradle/versions.gradle"},
		"an applied script over the size bound": {map[string]string{"big.gradle": tooBig,
			"app/build.gradle": "apply from: '../big.gradle'\n" + app}, "big.gradle"},
		"a buildSrc source past the work bound": {map[string]string{"buildSrc/src/main/kotlin/V.kt": hostile, "app/build.gradle": app}, "buildSrc/src/main/kotlin/V.kt"},
		"a catalogue over the size bound": {map[string]string{"gradle/libs.versions.toml": "[versions]\n" + strings.Repeat("# padding\n", int(MaxBuildFileBytes)/10+1),
			"app/build.gradle": app}, "gradle/libs.versions.toml"},
	} {
		c.files["elsewhere/settings.gradle"] = "rootProject.name = 'other'\n"
		c.files["elsewhere/build.gradle"] = app
		root := writeTree(t, c.files)
		var log bytes.Buffer
		w := NewWalk(root, slog.New(slog.NewTextHandler(&log, nil)))
		got := w.Read(filepath.Join(root, "app", "build.gradle"), Options{})
		if len(got.Deps) != 0 || got.Failure == nil || !strings.Contains(got.Failure.Error(), c.part) {
			t.Errorf("%s: %d dependencies, failure %v; want none and a failure naming %s", what, len(got.Deps), got.Failure, c.part)
		}
		if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), filepath.Join(root, filepath.FromSlash(c.part))) {
			t.Errorf("%s: the log does not name %s:\n%.400s", what, c.part, log.String())
		}
		// A sibling under the same root shares the root's settings and
		// buildSrc; one that is elsewhere... is still under the clone's
		// root. What is not above it does not touch it.
		if strings.HasPrefix(c.part, "gradle/versions") || strings.HasPrefix(c.part, "big") {
			other := w.Read(filepath.Join(root, "elsewhere", "build.gradle"), Options{})
			if other.Failure != nil || len(other.Deps) != 1 {
				t.Errorf("%s: a build file that applies nothing: %d dependencies, failure %v; want it read", what, len(other.Deps), other.Failure)
			}
		}
	}
}

// What the projects directly below a directory start from is worked out
// once for that directory, not once for each of them.
func TestAParentBuildFileIsReadOnceForItsChildren(t *testing.T) {
	files := map[string]string{"build.gradle": "ext.v = '4.2'\n"}
	const children = 5
	for i := 0; i < children; i++ {
		files[fmt.Sprintf("m%d/build.gradle", i)] = "dependencies {\n  implementation \"a:b:${v}\"\n}\n"
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	for i := 0; i < children; i++ {
		deps := w.Read(filepath.Join(root, fmt.Sprintf("m%d", i), "build.gradle"), Options{}).Deps
		if len(deps) != 1 || deps[0].Version != "4.2" {
			t.Fatalf("m%d: %+v, want a:b at 4.2 from the root's ext", i, deps)
		}
	}
	if len(w.gradle.dirs) != children+1 || len(w.gradle.belows) != 1 {
		t.Errorf("%d directories known and %d parents worked out; want %d and 1", len(w.gradle.dirs), len(w.gradle.belows), children+1)
	}
}

// A pom.xml over the bound is a manifest that could not be read, as a
// Gradle file is: the tree refuses it, and the reader of the file's own
// declarations must not then read it whole on its own.
func TestAnOversizedPomIsAReadFailure(t *testing.T) {
	pom := `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>1.0</version></dependency></dependencies><!--` +
		strings.Repeat("x", int(MaxBuildFileBytes)) + `--></project>`
	root := writeTree(t, map[string]string{"pom.xml": pom})
	got := NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{})
	if got.Failure == nil || len(got.Deps) != 0 {
		t.Errorf("an oversized pom: %d dependencies, failure %v; want none and a failure", len(got.Deps), got.Failure)
	}
}
