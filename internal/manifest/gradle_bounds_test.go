// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"bytes"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// The same for every other file a build file is read with: the settings
// file, a script it applies and a catalogue, over the size bound. Each is
// named in the log, and the
// build file that depends on it is a read failure that names it. A build
// file beside them that depends on none of them is read as before.
func TestAFileOfTheBuildThatCannotBeReadFailsWhatDependsOnIt(t *testing.T) {
	tooBig := "ext.v = '9.9'\n" + strings.Repeat("// padding\n", int(MaxBuildFileBytes)/11+1)
	app := "dependencies {\n  implementation 'c:d:1'\n}\n"
	for what, c := range map[string]struct {
		files map[string]string
		part  string
	}{
		"a settings file over the size bound": {map[string]string{"settings.gradle.kts": tooBig, "app/build.gradle": app}, "settings.gradle.kts"},
		"an applied script over the size bound": {map[string]string{"big.gradle": tooBig,
			"app/build.gradle": "apply from: '../big.gradle'\n" + app}, "big.gradle"},
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
