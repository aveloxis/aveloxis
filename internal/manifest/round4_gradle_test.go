// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Review round 4 of the vulnerability program's first part: the Gradle tree.

// R4-M3. Directories whose build files all apply ONE script that itself
// applies many small scripts: every directory took a layer for every small
// script (and a check of each), k directories times k scripts. What the
// shared script and the scripts it applies leave behind is built once and
// taken by reference (gradleBundle): one link per directory.
func r4Fanout(k int) (files map[string]string, reads []string) {
	var all strings.Builder
	files = map[string]string{}
	for i := 0; i < k; i++ {
		fmt.Fprintf(&all, "apply from: 's%d.gradle'\n", i)
		files[fmt.Sprintf("gradle/s%d.gradle", i)] = fmt.Sprintf("ext.a%d = '1.%d'\n", i, i)
	}
	files["gradle/all.gradle"] = all.String()
	for d := 0; d < k; d++ {
		files[fmt.Sprintf("p%d/build.gradle", d)] = "apply from: \"$rootDir/gradle/all.gradle\"\n"
		rel := fmt.Sprintf("p%d/c/build.gradle", d)
		// The last small script's variable: what the scripts assign must
		// reach the project below, through the shared run of layers.
		files[rel] = fmt.Sprintf("dependencies { implementation \"g:a:$a%d\" }\n", k-1)
		reads = append(reads, rel)
	}
	return files, reads
}

func TestDirectoriesTimesTheScriptsOfASharedScriptIsNotAProduct(t *testing.T) {
	cost := func(k int) (kept uint64, took time.Duration) {
		files, reads := r4Fanout(k)
		root := writeTree(t, files)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		w := NewWalk(root, nil)
		for _, rel := range reads {
			got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			if got.Failure != nil || len(got.Deps) != 1 || got.Deps[0].Version != fmt.Sprintf("1.%d", k-1) {
				t.Fatalf("k=%d %s: %+v, failure %v; want the version the last applied script assigns", k, rel, got.Deps, got.Failure)
			}
		}
		took = time.Since(start)
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(w)
		if after.HeapAlloc > before.HeapAlloc {
			kept = after.HeapAlloc - before.HeapAlloc
		}
		return kept, took
	}
	small, smallTook := cost(300)
	large, largeTook := cost(1200)
	t.Logf("kept %d KB in %v for 300 x 300; %d KB in %v for 1200 x 1200", small>>10, smallTook, large>>10, largeTook)
	// Four times the directories and four times the scripts. Linear in what
	// was read is four times; the pairs are sixteen.
	if large > 4<<20 && float64(large) > scalingLimit*float64(max(small, 1<<20)) {
		t.Errorf("1200 directories x 1200 scripts kept %d KB, 300 x 300 kept %d KB: what is kept grows with the pairs", large>>10, small>>10)
	}
}

// r4Publishing is an honest build of the kind that reads a script again for
// every module: each module sets a property of its own (its artifact's
// name) that the shared publishing script reads.
func r4Publishing(modules, scriptBytes int) (files map[string]string, reads []string) {
	var script strings.Builder
	script.WriteString("apply plugin: 'maven-publish'\ngroup = GROUP\nversion = VERSION_NAME\next.artifact = \"$POM_ARTIFACT_ID\"\n")
	for i := 0; script.Len() < scriptBytes; i++ {
		fmt.Fprintf(&script, "afterEvaluate { project ->\n  publishing {\n    publications {\n      release%d(MavenPublication) {\n        groupId = GROUP\n        artifactId = project.name\n        version = VERSION_NAME\n      }\n    }\n  }\n}\n", i)
	}
	files = map[string]string{
		"settings.gradle":       "rootProject.name = 'app'\n",
		"gradle.properties":     "GROUP=com.example\nVERSION_NAME=1.2.3\nkotlin_version=1.9.0\n",
		"build.gradle":          "allprojects {\n repositories { google(); mavenCentral() }\n}\n",
		"gradle/publish.gradle": script.String(),
	}
	for i := 0; i < modules; i++ {
		var deps strings.Builder
		for d := 0; d < 12; d++ {
			fmt.Fprintf(&deps, "    implementation \"com.example.lib%d:artifact-%d:1.%d.0\"\n", d, i+d, d)
		}
		files[fmt.Sprintf("module%d/gradle.properties", i)] = fmt.Sprintf("POM_ARTIFACT_ID=module-%d\nPOM_NAME=Module %d\n", i, i)
		rel := fmt.Sprintf("module%d/build.gradle", i)
		files[rel] = "plugins {\n  id 'com.android.library'\n  id 'org.jetbrains.kotlin.android'\n}\napply from: \"$rootDir/gradle/publish.gradle\"\n" +
			"android {\n  namespace 'com.example.module'\n  compileSdk 34\n  defaultConfig { minSdk 24 }\n}\ndependencies {\n" + deps.String() +
			"    implementation \"org.jetbrains.kotlin:kotlin-stdlib:$kotlin_version\"\n}\n"
		reads = append(reads, rel)
	}
	return files, reads
}

// R4-M13. An honest build of that kind is read: 1,000 modules of about a
// kilobyte, each reading its own property through a 60 KB script. (Under
// the work allowances of 0.29.210 and 0.29.211, 138 modules were refused.)
func TestAnHonestPublishingBuildIsRead(t *testing.T) {
	files, reads := r4Publishing(1000, 60<<10)
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	for _, rel := range reads {
		got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		if got.Failure != nil || len(got.Deps) != 13 {
			t.Fatalf("%s: %d dependencies, failure %v", rel, len(got.Deps), got.Failure)
		}
	}
}

// R4-M6 (M7 of the mutants). A script's reading records every name it
// looked for outside itself, the ones it did NOT find included: a project
// that defines such a name is given its own reading. Recording only what
// was found handed the next project the reading of one that lacked it.
func TestAScriptThatFoundANameMissingIsReadAgainWhereItIsDefined(t *testing.T) {
	files := map[string]string{
		"gradle/versions.gradle": "ext.lib = \"${base}.5\"\n",
		"without/build.gradle":   "apply from: \"$rootDir/gradle/versions.gradle\"\ndependencies { implementation \"g:a:$lib\" }\n",
		"with/gradle.properties": "base=7\n",
		"with/build.gradle":      "apply from: \"$rootDir/gradle/versions.gradle\"\ndependencies { implementation \"g:a:$lib\" }\n",
	}
	root := writeTree(t, files)
	for _, order := range [][]string{{"without", "with"}, {"with", "without"}} {
		w := NewWalk(root, nil)
		got := map[string]string{}
		for _, dir := range order {
			for _, d := range w.Read(filepath.Join(root, dir, "build.gradle"), Options{}).Deps {
				got[dir] = d.Version
			}
		}
		if got["with"] != "7.5" || got["without"] != "" {
			t.Errorf("read in the order %v: with %q, without %q; want 7.5 and no version", order, got["with"], got["without"])
		}
	}
}

// A script applied through a shared script is applied once by a build file
// that names it again itself, whether the shared script's run was built for
// this file or taken from another's reading.
func TestAScriptInsideASharedRunIsNotAppliedAgain(t *testing.T) {
	build := "apply from: \"$rootDir/gradle/all.gradle\"\napply from: \"$rootDir/gradle/s.gradle\"\ndependencies { implementation \"g:a:$n\" }\n"
	root := writeTree(t, map[string]string{
		"gradle.properties": "n=1\n",
		"gradle/all.gradle": "apply from: 's.gradle'\n",
		"gradle/s.gradle":   "ext.n = \"${n}.5\"\n",
		"p0/build.gradle":   build,
		"p1/build.gradle":   build,
	})
	w := NewWalk(root, nil)
	var versions []string
	for _, dir := range []string{"p0", "p1"} {
		got := w.Read(filepath.Join(root, dir, "build.gradle"), Options{})
		if got.Failure != nil || len(got.Deps) != 1 {
			t.Fatalf("%s: %+v, failure %v", dir, got.Deps, got.Failure)
		}
		versions = append(versions, got.Deps[0].Version)
	}
	if versions[0] != "1.5" || versions[1] != versions[0] {
		t.Errorf("versions %v; want 1.5 for both: the second project took the run the first built", versions)
	}
	if len(w.gradle.bundles) == 0 {
		t.Error("no run was kept: the premise does not hold")
	}
}
