// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5 (M5-5, M5-6): the pins the review's mutants asked for that
// the oracle's random trees do not reach. Each test names the change it
// fails under.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// r5Versions reads the build files named, in order, with one walk, and
// returns "name=version" for every declaration of each, and what the naive
// reader (round5_oracle_test.go) gives for the same file.
func r5Versions(t *testing.T, root string, rels ...string) (got, want map[string]string) {
	t.Helper()
	got, want = map[string]string{}, map[string]string{}
	w := NewWalk(root, nil)
	o := &oracleGradle{root: root, paths: newGradleTree(root, nil)}
	line := func(deps []model.LibyearDep) string {
		var parts []string
		for _, d := range deps {
			parts = append(parts, d.Name+"="+d.Version)
		}
		return strings.Join(parts, " ")
	}
	for _, rel := range rels {
		path := filepath.Join(root, filepath.FromSlash(rel))
		r := w.Read(path, Options{})
		got[rel] = line(r.Deps)
		if r.Failure != nil {
			got[rel] = "REFUSED"
		}
		want[rel] = line(notPackages(o.declared(path)))
	}
	return got, want
}

// G19. A run that stands in for a script takes with it the names the runs
// INSIDE it read: a.gradle applies b.gradle, which reads X. Kept for a
// project where X is 1.0 without X among its names, a.gradle's run stood in
// for a project where X is 2.0, with b.gradle's answer for 1.0.
func TestARunKeepsTheNamesTheRunsInsideItRead(t *testing.T) {
	deps := "dependencies {\n  implementation \"g:d:$derived\"\n}\n"
	root := writeTree(t, map[string]string{
		"settings.gradle":       "rootProject.name='r'\n",
		"gradle/b.gradle":       "ext.derived = \"$X\"\n",
		"gradle/a.gradle":       "apply from: 'b.gradle'\next.fromA = 'a'\n",
		"p1/gradle.properties":  "X=1.0\n",
		"p1/build.gradle":       "apply from: \"$rootDir/gradle/b.gradle\"\n" + deps,
		"p1b/gradle.properties": "X=1.0\n",
		"p1b/build.gradle":      "apply from: \"$rootDir/gradle/a.gradle\"\n" + deps,
		"p2/gradle.properties":  "X=2.0\n",
		"p2/build.gradle":       "apply from: \"$rootDir/gradle/a.gradle\"\n" + deps,
	})
	got, want := r5Versions(t, root, "p1/build.gradle", "p1b/build.gradle", "p2/build.gradle")
	if got["p2/build.gradle"] != "g:d=2.0" || got["p1b/build.gradle"] != "g:d=1.0" {
		t.Errorf("p1b reads %q and p2 %q; want g:d=1.0 and g:d=2.0 (the naive reader: %q, %q)", got["p1b/build.gradle"], got["p2/build.gradle"], want["p1b/build.gradle"], want["p2/build.gradle"])
	}
}

// G6, G23, G24. A kept run is not used where it would reach deeper than an
// apply is followed (gradleApplyDepth), and a run's depth includes the runs
// inside it, built or reused. c0 applies c1 ... applies c9. Applied at c5
// and at c3 the whole chain below is read and kept; applied at c0 the chain
// is followed to c7 and no further, whatever was kept.
func TestAKeptRunIsNotUsedPastTheApplyDepth(t *testing.T) {
	files := map[string]string{"settings.gradle": "rootProject.name='r'\n"}
	for i := 0; i <= 9; i++ {
		c := fmt.Sprintf("ext.v%d = '%d.0'\n", i, i)
		if i < 9 {
			c += fmt.Sprintf("apply from: 'c%d.gradle'\n", i+1)
		}
		files[fmt.Sprintf("gradle/c%d.gradle", i)] = c
	}
	deps := "dependencies {\n  implementation \"g:x7:$v7\"\n  implementation \"g:x8:$v8\"\n  implementation \"g:x9:$v9\"\n}\n"
	for _, p := range []struct{ dir, from string }{{"pa", "c5"}, {"pb", "c3"}, {"pc", "c0"}} {
		files[p.dir+"/build.gradle"] = "apply from: \"$rootDir/gradle/" + p.from + ".gradle\"\n" + deps
	}
	root := writeTree(t, files)
	if gradleApplyDepth != 8 {
		t.Fatalf("gradleApplyDepth is %d: this chain is written for 8", gradleApplyDepth)
	}
	for _, order := range [][]string{{"pa", "pb", "pc"}, {"pb", "pc"}, {"pc"}, {"pa", "pc", "pb"}} {
		var rels []string
		for _, d := range order {
			rels = append(rels, d+"/build.gradle")
		}
		got, want := r5Versions(t, root, rels...)
		for _, rel := range rels {
			if got[rel] != want[rel] {
				t.Errorf("read order %v: %s reads %q; the naive reader %q", order, rel, got[rel], want[rel])
			}
		}
		if got["pc/build.gradle"] != "g:x7=7.0 g:x8= g:x9=" {
			t.Errorf("read order %v: applied at c0 the chain gives %q; want c7 read and c8, c9 not (g:x7=7.0 g:x8= g:x9=)", order, got["pc/build.gradle"])
		}
	}
}

// G22. A run with a script inside that could not be read is not kept: the
// next file that applies it must be refused too, by name, and not read as a
// whole build with that script's assignments missing.
func TestARunWithAnUnreadScriptInsideIsNotKept(t *testing.T) {
	// A script that is not read: larger than a build file is.
	hostile := "ext.v = '9.9'\n" + strings.Repeat("// padding\n", int(MaxBuildFileBytes)/11+1)
	build := "apply from: \"$rootDir/gradle/a.gradle\"\ndependencies {\n  implementation 'g:d:1.0'\n}\n"
	root := writeTree(t, map[string]string{
		"settings.gradle": "rootProject.name='r'\n",
		"gradle/a.gradle": "ext.one = '1'\napply from: 'b.gradle'\n",
		"gradle/b.gradle": hostile,
		"p1/build.gradle": build,
		"p2/build.gradle": build,
	})
	w := NewWalk(root, nil)
	for _, rel := range []string{"p1/build.gradle", "p2/build.gradle"} {
		got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		if got.Failure == nil || !strings.Contains(got.Failure.Err.Error(), "gradle/b.gradle") {
			t.Errorf("%s: failure %v; want it refused, naming gradle/b.gradle", rel, got.Failure)
		}
	}
}

// G21. A script is read twice when it applies others: before them, for what
// it applies, and after. The second reading can look up a name the first did
// not (a name the applied script made unknown is looked for under its
// shorter spelling), and that name is one of the run's.
func TestARunKeepsTheNamesItsSecondReadingRead(t *testing.T) {
	deps := "dependencies {\n  implementation \"g:d:$d\"\n}\n"
	root := writeTree(t, map[string]string{
		"settings.gradle":      "rootProject.name='r'\n",
		"gradle/b.gradle":      "ext.lib.ver = '1'\next.lib.ver = '2'\n",
		"gradle/a.gradle":      "apply from: 'b.gradle'\next.d = \"$lib.ver\"\n",
		"p1/gradle.properties": "lib.ver=5\nlib=7\n",
		"p1/build.gradle":      "apply from: \"$rootDir/gradle/a.gradle\"\n" + deps,
		"p2/gradle.properties": "lib.ver=5\nlib=8\n",
		"p2/build.gradle":      "apply from: \"$rootDir/gradle/a.gradle\"\n" + deps,
	})
	got, want := r5Versions(t, root, "p1/build.gradle", "p2/build.gradle")
	for _, rel := range []string{"p1/build.gradle", "p2/build.gradle"} {
		if got[rel] != want[rel] {
			t.Errorf("%s reads %q; the naive reader %q", rel, got[rel], want[rel])
		}
	}
	if got["p1/build.gradle"] == got["p2/build.gradle"] {
		t.Fatalf("both projects read %q: the shape does not tell their scopes apart (the premise)", got["p1/build.gradle"])
	}
}

// G17, G18. What a kept run counts against the memo's allowance is its
// links and every assignment they hold, not its names alone.
func TestAKeptRunCountsItsLinksAndTheirAssignments(t *testing.T) {
	w := NewWalk(t.TempDir(), nil)
	tree := w.gradle
	tree.memoLimit = 1 << 20
	var vars *gradleVars
	text := 0
	const links = 5
	for i := 0; i < links; i++ {
		k, v := fmt.Sprintf("name%d", i), fmt.Sprintf("value-%d", i)
		vars = vars.push(map[string]string{k: v}, map[string]bool{"gone" + k: true})
		text += len(k) + len(v) + len("gone"+k)
	}
	tree.keepBundle("a.gradle", &gradleBundleBuild{scripts: []string{"a.gradle"}}, nil, vars)
	if len(tree.bundles["a.gradle"]) != 1 {
		t.Fatal("the run was not kept (the premise)")
	}
	if want := 2*gradleMemoRefBytes + links*gradleBundleLinkBytes + text; tree.memoBytes != want {
		t.Errorf("a run of %d links holding %d bytes of assignments counts %d bytes in the memo, want %d", links, text, tree.memoBytes, want)
	}
}

type failingSource struct{}

func (failingSource) RemotePom(string, string, string, []string) (*RemotePom, RemoteOutcome) {
	return nil, RemoteFailed
}

// P3, P5. A question takes the reason of every import it passes before its
// answer, also when an earlier question found the import dead and it was
// dropped from the list (the reason is carried to the import after it, and
// through the pass that shortens the list). The dependency g:own has an
// entry of its own whose version cannot be read, so it asks no import
// itself: its reason is what the question before it (answered by bomA,
// behind an import that could not be looked up) met.
func TestTheReasonsOfImportsPassedBeforeAnAnswerAreKept(t *testing.T) {
	parent := "<parent><groupId>t</groupId><artifactId>p</artifactId><version>1</version></parent>"
	root := writeTree(t, map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "gone", "1") + r4import("t", "bomA", "1") + r4import("t", "gone2", "1") + r4import("t", "bomB", "1") + "</dependencies></dependencyManagement></project>",
		"f4/pom.xml":   "<project>" + parent + "<artifactId>f4</artifactId><dependencies>" + r4dep("g", "nowhere3", "") + "</dependencies></project>",
		"bomA/pom.xml": "<project><groupId>t</groupId><artifactId>bomA</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "inbom", "5") + "</dependencies></dependencyManagement></project>",
		"bomB/pom.xml": "<project><groupId>t</groupId><artifactId>bomB</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "inbomB", "6") + "</dependencies></dependencyManagement></project>",
		"f1/pom.xml":   "<project>" + parent + "<artifactId>f1</artifactId><dependencies>" + r4dep("g", "nowhere", "") + r4dep("g", "nowhere2", "") + "</dependencies></project>",
		"f2/pom.xml": "<project>" + parent + "<artifactId>f2</artifactId><dependencyManagement><dependencies>" + r4dep("g", "own", "${nope}") +
			"</dependencies></dependencyManagement><dependencies>" + r4dep("g", "inbom", "") + r4dep("g", "own", "") + "</dependencies></project>",
		"f3/pom.xml": "<project>" + parent + "<artifactId>f3</artifactId><dependencyManagement><dependencies>" + r4dep("g", "own", "${nope}") +
			"</dependencies></dependencyManagement><dependencies>" + r4dep("g", "inbomB", "") + r4dep("g", "own", "") + "</dependencies></project>",
	})
	for _, order := range [][]string{{"f2"}, {"f1", "f2"}, {"f1", "f1", "f2"}, {"f3"}, {"f1", "f3"}, {"f1", "f2", "f3"}, {"f3", "f1", "f3"}} {
		w := NewWalk(root, nil)
		w.SetPomSource(failingSource{})
		var last Declarations
		for _, f := range order {
			last = w.Read(filepath.Join(root, f, "pom.xml"), Options{})
		}
		target := order[len(order)-1]
		seen := 0
		for _, d := range last.Deps {
			if d.ManagedEntry {
				continue
			}
			switch d.Name {
			case "g:inbom", "g:inbomB":
				// Round 6 (M6-3): the BOM that manages it comes AFTER an
				// import that could not be looked up, which might manage it
				// too and would win. Its version is not determined, and the
				// reason is the one of the import passed.
				seen++
				if d.Version != "" || d.VersionSource != "" || d.VersionReason != model.VersionReasonInheritedLookupFailed {
					t.Errorf("read order %v: %s of %s has version %q from %q, reason %q; want no version and %q", order, d.Name, target, d.Version, d.VersionSource, d.VersionReason, model.VersionReasonInheritedLookupFailed)
				}
			case "g:own":
				seen++
				if d.Version != "" || d.VersionReason != model.VersionReasonInheritedLookupFailed {
					t.Errorf("read order %v: g:own of %s has version %q and reason %q; want none, and %q: the question before it passed an import that could not be looked up", order, target, d.Version, d.VersionReason, model.VersionReasonInheritedLookupFailed)
				}
			}
		}
		if seen != 2 {
			t.Fatalf("read order %v: %d of the two dependencies of %s were found: %+v", order, seen, target, last.Deps)
		}
	}
	// P5b. And a question no import answers takes the reason of every import
	// that leads nowhere, also when an earlier pom's questions found them
	// dead and the list was shortened since.
	for _, order := range [][]string{{"f4"}, {"f1", "f4"}, {"f1", "f2", "f4"}} {
		w := NewWalk(root, nil)
		w.SetPomSource(failingSource{})
		var last Declarations
		for _, f := range order {
			last = w.Read(filepath.Join(root, f, "pom.xml"), Options{})
		}
		if len(last.Deps) != 1 || last.Deps[0].VersionReason != model.VersionReasonInheritedLookupFailed {
			t.Errorf("read order %v: f4's dependency: %+v; want one, with reason %q", order, last.Deps, model.VersionReasonInheritedLookupFailed)
		}
	}
}

// P8. A question whose group or artifact holds a colon cannot be filed by
// either half (the colon is not told apart from the one between them): it
// compares every reference-keyed entry, whole key against whole key, as
// every question did before the entries were filed.
func TestAQuestionWithAColonComparesEveryReferenceKeyedEntry(t *testing.T) {
	root := writeTree(t, map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><properties><x>g:a</x><y>b:c</y></properties><dependencyManagement><dependencies>" +
			r4dep("${x}", "b", "3") + r4dep("g", "${y}", "4") + "</dependencies></dependencyManagement></project>",
	})
	tree := NewWalk(root, nil).pom
	m := tree.model(filepath.Join(root, "pom.xml"))
	if m == nil {
		t.Fatal("the pom was not read")
	}
	c := tree.context(m)
	for _, q := range []struct{ group, artifact, want string }{{"g", "a:b", "3"}, {"g:a", "b", "3"}, {"g", "b:c", "4"}, {"g:b", "c", "4"}} {
		if v, found, _, _, _ := c.managed(&pomReading{}, q.group, q.artifact); !found || v != q.want {
			t.Errorf("%q : %q is managed at %q (found %v), want %q", q.group, q.artifact, v, found, q.want)
		}
	}
}

// G4c. What a reading took from outside is each name's value AND whether it
// was there at all: a name assigned the empty string is known, and a name
// that is not assigned is not, though both read as "". A run kept where X
// was not known does not stand for a scope where X is "".
func TestWhetherANameWasKnownIsPartOfWhatARunRead(t *testing.T) {
	deps := "apply from: \"$rootDir/gradle/s.gradle\"\ndependencies {\n  implementation \"g:d:$d\"\n}\n"
	files := map[string]string{
		"settings.gradle":     "rootProject.name='r'\n",
		"gradle/s.gradle":     "ext.d = \"1.0${X}\"\n",
		"p1/build.gradle":     deps,
		"sub/build.gradle":    "ext.X = ''\n",
		"sub/p2/build.gradle": deps,
	}
	root := writeTree(t, files)
	for _, order := range [][]string{{"p1/build.gradle", "sub/p2/build.gradle"}, {"sub/p2/build.gradle", "p1/build.gradle"}} {
		got, want := r5Versions(t, root, order...)
		if got["p1/build.gradle"] != "g:d=" || got["sub/p2/build.gradle"] != "g:d=1.0" {
			t.Errorf("read order %v: p1 reads %q and sub/p2 %q; want g:d= (X is not known) and g:d=1.0 (X is the empty string); the naive reader: %q, %q",
				order, got["p1/build.gradle"], got["sub/p2/build.gradle"], want["p1/build.gradle"], want["sub/p2/build.gradle"])
		}
	}
}

// G22, shown equivalent. A run is kept only when nothing inside it was left
// unread, and applyOne says so three ways; the count of files noted unread
// since the run began is the third, and no path reaches it alone: a script
// that is not read ends the run before it is kept (and marks the run it is
// part of). These are those paths, each
// applied twice: the second application must be refused as the first was.
func TestEveryWayAScriptIsLeftUnreadKeepsItsRunFromBeingKept(t *testing.T) {
	// A script that is not read: larger than a build file is.
	hostile := "ext.v = '9.9'\n" + strings.Repeat("// padding\n", int(MaxBuildFileBytes)/11+1)
	build := func(script string) string {
		return "apply from: \"$rootDir/gradle/" + script + "\"\ndependencies {\n  implementation 'g:d:1.0'\n}\n"
	}
	for name, c := range map[string]struct {
		scripts map[string]string
		apply   string
		unread  string
	}{
		"the script itself":        {map[string]string{"h.gradle": hostile}, "h.gradle", "gradle/h.gradle"},
		"a script it applies":      {map[string]string{"a.gradle": "ext.one = '1'\napply from: 'h.gradle'\n", "h.gradle": hostile}, "a.gradle", "gradle/h.gradle"},
		"a script two levels down": {map[string]string{"a.gradle": "apply from: 'b.gradle'\n", "b.gradle": "ext.two = '2'\napply from: 'h.gradle'\n", "h.gradle": hostile}, "a.gradle", "gradle/h.gradle"},
	} {
		files := map[string]string{"settings.gradle": "rootProject.name='r'\n", "p1/build.gradle": build(c.apply), "p2/build.gradle": build(c.apply), "p3/build.gradle": "dependencies {\n  implementation 'g:d:1.0'\n}\n"}
		for s, text := range c.scripts {
			files["gradle/"+s] = text
		}
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		for _, rel := range []string{"p1/build.gradle", "p2/build.gradle"} {
			got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			if got.Failure == nil || !strings.Contains(got.Failure.Err.Error(), c.unread) {
				t.Errorf("%s: %s: failure %v; want it refused, naming %s", name, rel, got.Failure, c.unread)
			}
		}
		for path, runs := range w.gradle.bundles {
			if len(runs) > 0 && strings.HasSuffix(path, c.apply) {
				t.Errorf("%s: a run was kept for %s with a script inside it unread", name, path)
			}
		}
		if got := w.Read(filepath.Join(root, "p3", "build.gradle"), Options{}); got.Failure != nil || len(got.Deps) != 1 {
			t.Errorf("%s: a project that applies none of it: %d dependencies, failure %v", name, len(got.Deps), got.Failure)
		}
	}
}
