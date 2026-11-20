// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5, the Gradle tree (M5-3, M5-7): work is the bytes touched,
// and a file's own bound is a fact about the file.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// r5SharedScript is k projects that each apply one script, which reads a
// property every project sets differently (so each project reads it anew)
// and is otherwise filler.
func r5SharedScript(k, scriptBytes int, filler string) (files map[string]string, reads []string) {
	var script strings.Builder
	script.WriteString("ext.artifact = \"$POM_ARTIFACT_ID\"\n")
	for script.Len() < scriptBytes {
		script.WriteString(filler)
	}
	files = map[string]string{"settings.gradle": "rootProject.name = 'app'\n", "gradle/publish.gradle": script.String()}
	for i := 0; i < k; i++ {
		files[fmt.Sprintf("m%d/gradle.properties", i)] = fmt.Sprintf("POM_ARTIFACT_ID=m%d\n", i)
		rel := fmt.Sprintf("m%d/build.gradle", i)
		files[rel] = "apply from: \"$rootDir/gradle/publish.gradle\"\ndependencies {\n  implementation 'g:a:1.0'\n}\n"
		reads = append(reads, rel)
	}
	return files, reads
}

// M5-3. A reading of a script is charged the bytes it tokenises, whatever
// they are: blank lines and comments cost time and were free, so 600
// projects applying an 8 MiB script of them took 44 s for 0.02% of the walk's
// allowance. Every byte tokenised is now in the units (gradleScanBytes of
// them to a unit); and a script whose tokens the walk's account has room for
// is tokenised ONCE, and read again from its tokens.
func TestEveryByteTokenisedIsCharged(t *testing.T) {
	for _, c := range []struct {
		name, filler  string
		tokenisedOnce bool
	}{
		{"blank lines (a token each: no room to keep them)", "\n", false},
		{"comments (no tokens: kept, and tokenised once)", "// a line of commentary that says nothing at all about anything\n", true},
	} {
		const k, size = 40, 1 << 20
		files, reads := r5SharedScript(k, size, c.filler)
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		start := time.Now()
		for _, rel := range reads {
			if got := w.Read(filepath.Join(root, rel), Options{}); got.Failure != nil {
				t.Fatalf("%s: %s was refused: %v", c.name, rel, got.Failure)
			}
		}
		took := time.Since(start)
		g := w.gradle
		t.Logf("%s: %d projects, a %d KB script: %d bytes tokenised, %d units of %d (%.1f%%), %s, %.0f ns a unit",
			c.name, k, size>>10, g.scanned, g.work.used, g.work.limit, 100*float64(g.work.used)/float64(g.work.limit), took.Round(time.Millisecond), float64(took.Nanoseconds())/float64(g.work.used))
		if g.work.used*gradleScanBytes < g.scanned {
			t.Errorf("%s: %d bytes were tokenised for %d units: at %d bytes a unit that is at least %d", c.name, g.scanned, g.work.used, gradleScanBytes, g.scanned/gradleScanBytes)
		}
		if once := g.scanned < 2*r5Bytes(files); once != c.tokenisedOnce {
			t.Errorf("%s: %d bytes tokenised for %d bytes of files: tokenised once is %v, want %v", c.name, g.scanned, r5Bytes(files), once, c.tokenisedOnce)
		}
		if w.acct.kept > w.acct.limit() {
			t.Errorf("%s: accounted %d bytes kept of %d allowed", c.name, w.acct.kept, w.acct.limit())
		}
	}
}

// M5-7. Whether a file is inside its OWN bound does not depend on the scope
// it is read under: a lookup costs the layers it passes, and that was
// charged to the file, so a script inside its bound under a shallow scope
// was past it under a deep one, and which projects were refused depended on
// the order they were read in. The layers are the walk's.
func TestAFilesOwnBoundDoesNotDependOnTheScopeItIsReadUnder(t *testing.T) {
	var s strings.Builder
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&s, "ext.r%d = \"$a%d$b%d$c%d$d%d$e%d$f%d$g%d$h%d\"\n", i, i, i, i, i, i, i, i, i)
	}
	files := map[string]string{
		"settings.gradle":            "rootProject.name='r'\n",
		"gradle/s.gradle":            s.String(),
		"shallow/build.gradle":       "apply from: \"$rootDir/gradle/s.gradle\"\ndependencies {\n  implementation 'g:a:1.0'\n}\n",
		"shallow2/gradle.properties": "a0=7\n",
		"shallow2/build.gradle":      "apply from: \"$rootDir/gradle/s.gradle\"\ndependencies {\n  implementation 'g:b:1.0'\n}\n",
	}
	var deep strings.Builder
	for i := 0; i < 300; i++ {
		files[fmt.Sprintf("gradle/t/t%d.gradle", i)] = fmt.Sprintf("ext.t%d = '1'\n", i)
		fmt.Fprintf(&deep, "apply from: \"$rootDir/gradle/t/t%d.gradle\"\n", i)
	}
	deep.WriteString("apply from: \"$rootDir/gradle/s.gradle\"\ndependencies {\n  implementation 'g:c:1.0'\n}\n")
	files["deep/build.gradle"] = deep.String()
	root := writeTree(t, files)
	first := ""
	for _, order := range [][]string{{"shallow", "shallow2", "deep"}, {"shallow", "deep", "shallow2"}, {"deep", "shallow", "shallow2"}, {"deep", "shallow2", "shallow"}} {
		w := NewWalk(root, nil)
		got := map[string]string{}
		for _, d := range order {
			r := w.Read(filepath.Join(root, d, "build.gradle"), Options{})
			got[d] = fmt.Sprintf("%d deps", len(r.Deps))
			if r.Failure != nil {
				got[d] = "REFUSED"
			}
		}
		out := fmt.Sprintf("shallow: %s, shallow2: %s, deep: %s", got["shallow"], got["shallow2"], got["deep"])
		if len(w.gradle.pastBound) != 0 {
			t.Errorf("order %v: %d files were found past their own bound; the script is the same text under every scope", order, len(w.gradle.pastBound))
		}
		if first == "" {
			first = out
		}
		if out != first || strings.Contains(out, "REFUSED") {
			t.Errorf("order %v gives %q; the first order gave %q, and none is refused", order, out, first)
		}
	}
}

// r5UnitCeiling is the most one unit of either walk's work may be measured
// to cost, in nanoseconds: far above every primitive (the dearest is a pom's
// look, some 300 ns; this allows for a race detector and a busy machine),
// and far below what an uncounted path costs per unit (round 5 found 570,975
// ns for a look that hashed a long coordinate, and a reading of blank lines
// that cost no units at all).
const r5UnitCeiling = 250 * walkUnitNanos

// The units are units of TIME: each primitive that is charged, driven at
// its dearest, costs about the same per unit. The log is the measurement
// the bounds are stated from.
func TestTheUnitsOfBothWalksAreUnitsOfTime(t *testing.T) {
	type measured struct {
		name  string
		units int
		took  time.Duration
	}
	var all []measured
	gradle := func(name string, files map[string]string, reads []string) {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		start := time.Now()
		for _, rel := range reads {
			w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		}
		all = append(all, measured{"gradle: " + name, w.gradle.work.used, time.Since(start)})
	}
	blank, blankReads := r5SharedScript(30, 1<<20, "\n")
	gradle("tokenising line ends (a token a byte)", blank, blankReads)
	comments, commentReads := r5SharedScript(30, 1<<20, "// a line of commentary that says nothing at all about anything\n")
	gradle("tokenising comments (no tokens; read once)", comments, commentReads)
	publishing, publishingReads := r4Publishing(300, 60<<10)
	gradle("statements of a publishing script, read again per project", publishing, publishingReads)
	// Lookups through a deep scope: 300 scripts' layers under each name.
	deep := map[string]string{"settings.gradle": "rootProject.name='r'\n"}
	var applies, uses strings.Builder
	for i := 0; i < 300; i++ {
		deep[fmt.Sprintf("gradle/t%d.gradle", i)] = fmt.Sprintf("ext.t%d = '1'\n", i)
		fmt.Fprintf(&applies, "apply from: \"$rootDir/gradle/t%d.gradle\"\n", i)
	}
	uses.WriteString("dependencies {\n")
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&uses, "  implementation \"g:a%d:$t0\"\n", i)
	}
	deep["p/build.gradle"] = applies.String() + uses.String() + "}\n"
	gradle("lookups through 300 layers", deep, []string{"p/build.gradle"})

	pom := func(name string, files map[string]string, reads []string) {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		start := time.Now()
		for _, rel := range reads {
			w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		}
		all = append(all, measured{"maven: " + name, w.pom.walkWork().used, time.Since(start)})
	}
	// Looks past imports with short coordinates.
	var imports, deps strings.Builder
	for i := 0; i < 300; i++ {
		imports.WriteString(r4import("org.elsewhere", fmt.Sprintf("bom-%d", i), "${v}"))
	}
	for i := 0; i < 300; i++ {
		deps.WriteString(r4dep("org.unmanaged", fmt.Sprintf("dep-%d", i), ""))
	}
	pom("looks past imports with short coordinates", map[string]string{"pom.xml": pomOf("<properties><v>1</v></properties><dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")}, []string{"pom.xml"})
	// The bytes of a long literal coordinate.
	long := strings.Repeat("g", 200<<10)
	pom("looks past an import with a 200 KB coordinate", map[string]string{
		"pom.xml":     "<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><dependencyManagement><dependencies>" + r4import(long, "bom", "1") + "</dependencies></dependencyManagement></project>",
		"bom/pom.xml": "<project><groupId>" + long + "</groupId><artifactId>bom</artifactId><version>1</version></project>",
		"m/pom.xml":   "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencies>" + deps.String() + "</dependencies></project>",
	}, []string{"m/pom.xml"})
	// Interpolation of long properties into every entry compared.
	var entries strings.Builder
	// (A reference in both halves: every question compares every entry.
	// The pom ends at its bound; what it spent until then is the measure.)
	for i := 0; i < 150; i++ {
		entries.WriteString(r4dep("${L}", "${A}", "1"))
	}
	pom("entries interpolated through a 2,600-byte property", map[string]string{"pom.xml": pomOf("<properties><A>a</A><L>" + strings.Repeat("L", 2600) + "</L></properties><dependencyManagement><dependencies>" + entries.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")}, []string{"pom.xml"})

	for _, m := range all {
		per := float64(m.took.Nanoseconds()) / float64(max(m.units, 1))
		t.Logf("%-62s %9d units in %8s: %6.0f ns a unit", m.name, m.units, m.took.Round(time.Millisecond), per)
		if m.units < 1000 {
			t.Errorf("%s: only %d units were charged: the shape is not driving what it names", m.name, m.units)
		}
		if per > r5UnitCeiling {
			t.Errorf("%s: %.0f ns a unit; the ceiling is %d: work is being done that is not charged", m.name, per, r5UnitCeiling)
		}
	}
}
