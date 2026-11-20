// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 4 of the vulnerability program's first part: the Maven tree.

func r4dep(g, a, v string) string {
	s := "<dependency><groupId>" + g + "</groupId><artifactId>" + a + "</artifactId>"
	if v != "" {
		s += "<version>" + v + "</version>"
	}
	return s + "</dependency>"
}

func r4import(g, a, v string) string {
	return "<dependency><groupId>" + g + "</groupId><artifactId>" + a + "</artifactId><version>" + v + "</version><type>pom</type><scope>import</scope></dependency>"
}

// R4-M1 (wrong data). A BOM import that cannot be obtained makes its tree
// not whole, and the tree's managed entries stay inventoried: a module that
// was not seen may use one. Round 3 dropped such an import from the list
// the lookups walk once a question had found it dead, and the pass that
// judges wholeness read that same list: with two version-less dependencies
// (the second question drops it) the tree was whole, and its entries and
// the import itself left the inventory as "unused".
func TestAnImportThatCannotBeObtainedKeepsItsTreeNotWhole(t *testing.T) {
	for deps, n := range map[string]int{
		r4dep("x", "y", "1"):                      0,
		r4dep("x", "y", ""):                       1,
		r4dep("x", "y", "") + r4dep("x", "z", ""): 2,
		r4dep("x", "y", "") + r4dep("x", "z", "") + r4dep("x", "w", ""): 3,
	} {
		files := map[string]string{"pom.xml": "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("ext", "missing-bom", "1") + r4dep("com.acme", "unused", "3.0") +
			"</dependencies></dependencyManagement><dependencies>" + deps + "</dependencies></project>"}
		_, res := readTree(t, nil, files)
		if len(res.NotWhole) != 1 || res.NotWhole[0].Reason != "an imported BOM could not be obtained" {
			t.Errorf("%d version-less dependencies: not whole = %+v, want the missing BOM", n, res.NotWhole)
		}
		if len(res.Unused) != 0 {
			t.Errorf("%d version-less dependencies: %v left the inventory as unused; the tree is not whole, so they stay", n, names(res.Unused))
		}
		if !has(names(res.Kept), "pom.xml com.acme:unused@3.0") {
			t.Errorf("%d version-less dependencies: the managed entry is not kept: %v", n, names(res.Kept))
		}
	}
}

// R4-M8. Why a version-less dependency has no version is a fact about the
// file and the question, never about which file was read before it. Round
// 3 moved the reason onto the question but took, at the start of a pom's
// imports, the reasons of ALL the imports already known dead, the ones
// after the import that answers included.
func TestAReasonDoesNotDependOnWhatWasReadBefore(t *testing.T) {
	files := map[string]string{
		// p imports a BOM that manages g:a1, then one no repository answers
		// for; it manages g:a2 at a version nothing defines.
		"p/pom.xml": "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" +
			r4import("t", "bom", "1") + r4import("ext", "unreachable", "1") + r4dep("g", "a2", "${undefined}") +
			"</dependencies></dependencyManagement><dependencies>" + r4dep("g", "a1", "") + r4dep("g", "a2", "") + "</dependencies></project>",
		"bom/pom.xml": "<project><groupId>t</groupId><artifactId>bom</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" +
			r4dep("g", "a1", "1.0") + "</dependencies></dependencyManagement></project>",
		// q is p's child, with a dependency nothing manages: its question
		// passes every import of p and finds the second one dead.
		"q/pom.xml": "<project><parent><groupId>t</groupId><artifactId>p</artifactId><version>1</version><relativePath>../p</relativePath></parent><artifactId>q</artifactId>" +
			"<dependencies>" + r4dep("g", "nowhere", "") + "</dependencies></project>",
	}
	root := writeTree(t, files)
	read := func(order ...string) map[string]string {
		w := NewWalk(root, nil)
		w.SetPomSource(failingPomSource{})
		out := map[string]string{}
		for _, rel := range order {
			for _, d := range w.Read(filepath.Join(root, rel), Options{}).Deps {
				out[rel+" "+d.Name] = d.Version + "|" + d.VersionReason
			}
		}
		return out
	}
	alone := read("p/pom.xml")
	after := read("q/pom.xml", "p/pom.xml")
	for _, k := range []string{"p/pom.xml g:a1", "p/pom.xml g:a2"} {
		if alone[k] != after[k] {
			t.Errorf("%s: read alone %q, read after its child %q", k, alone[k], after[k])
		}
	}
	// g:a1 is answered by the first import: the second, after it, is not
	// part of its answer. g:a2's entry is p's own.
	if alone["p/pom.xml g:a1"] != "1.0|" || alone["p/pom.xml g:a2"] != "|" {
		t.Errorf("p read alone: %v", alone)
	}
	// The child's question did pass the dead import, and says so.
	if after["q/pom.xml g:nowhere"] != "|"+model.VersionReasonInheritedLookupFailed {
		t.Errorf("q's unmanaged dependency: %q", after["q/pom.xml g:nowhere"])
	}
}

// failingPomSource answers nothing: every coordinate asked is a failed
// lookup.
type failingPomSource struct{}

func (failingPomSource) RemotePom(string, string, string, []string) (*RemotePom, RemoteOutcome) {
	return nil, RemoteFailed
}

// r4Reactor is a reactor the way real ones are written: a parent that
// manages its own modules by `${project.groupId}` and many others plainly,
// and modules that take their versions from it.
func r4Reactor(modules, deps, sibling, plain int) map[string]string {
	var parent strings.Builder
	parent.WriteString("<project><groupId>com.example.app</groupId><artifactId>parent</artifactId><version>1.0</version><packaging>pom</packaging><dependencyManagement><dependencies>")
	for i := 0; i < sibling; i++ {
		parent.WriteString(r4dep("${project.groupId}", fmt.Sprintf("module-%d", i), "${project.version}"))
	}
	for i := 0; i < plain; i++ {
		parent.WriteString(r4dep("org.lib", fmt.Sprintf("lib-%d", i), "2."+fmt.Sprint(i)))
	}
	parent.WriteString("</dependencies></dependencyManagement></project>")
	files := map[string]string{"pom.xml": parent.String()}
	for m := 0; m < modules; m++ {
		var sb strings.Builder
		sb.WriteString("<project><parent><groupId>com.example.app</groupId><artifactId>parent</artifactId><version>1.0</version></parent><artifactId>" + fmt.Sprintf("module-%d", m) + "</artifactId><dependencies>")
		for d := 0; d < deps; d++ {
			switch d % 3 {
			case 0: // a sibling module, managed by a reference-keyed entry
				sb.WriteString(r4dep("com.example.app", fmt.Sprintf("module-%d", (m+d+1)%sibling), ""))
			case 1: // a library, managed plainly
				sb.WriteString(r4dep("org.lib", fmt.Sprintf("lib-%d", (m*7+d)%plain), ""))
			default: // one nothing manages
				sb.WriteString(r4dep("org.other", fmt.Sprintf("other-%d", d), ""))
			}
		}
		sb.WriteString("</dependencies></project>")
		files[fmt.Sprintf("m%d/pom.xml", m)] = sb.String()
	}
	return files
}

// R4-M4. An honest reactor is read, with room to spare: 600 modules, each
// with 30 version-less dependencies, under a parent with 300 entries keyed
// by `${project.groupId}` and 2,200 plain ones. Round 3 compared every
// reference-keyed entry for every question and refused 570 of the 600.
func TestAnHonestReactorIsFarInsideTheWorkBound(t *testing.T) {
	const modules, deps, sibling, plain = 600, 30, 300, 2200
	root := writeTree(t, r4Reactor(modules, deps, sibling, plain))
	w := NewWalk(root, nil)
	refused, managed := 0, 0
	for m := 0; m < modules; m++ {
		got := w.Read(filepath.Join(root, fmt.Sprintf("m%d", m), "pom.xml"), Options{})
		if got.Failure != nil {
			refused++
			continue
		}
		for _, d := range got.Deps {
			if d.VersionSource == SourceManaged {
				managed++
			}
		}
	}
	if refused != 0 {
		t.Fatalf("%d of %d modules of an honest reactor were refused", refused, modules)
	}
	if want := modules * deps * 2 / 3; managed != want {
		t.Errorf("%d dependencies took a managed version, want %d (the sibling modules and the libraries)", managed, want)
	}
	work := w.pom.walkWork()
	if work.used*pomHonestMargin > work.limit {
		t.Errorf("the reactor used %d of the walk's %d units: less than a %d-fold margin", work.used, work.limit, pomHonestMargin)
	}
	t.Logf("honest reactor: %d units used of %d allowed (%.0f-fold margin)", work.used, work.limit, float64(work.limit)/float64(max(work.used, 1)))
}

// pomHonestMargin is the room an honest build must have under the work
// bound: tenfold.
const pomHonestMargin = 10

// r4LongImports is k modules under a parent that imports k BOMs, each by
// coordinates that hold a reference to a long property and to the module's
// own artifactId: every (module, import) pair interpolates to another
// string of several kilobytes.
func r4LongImports(k int) map[string]string {
	long := strings.Repeat("L", 2600)
	var parent strings.Builder
	parent.WriteString("<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><properties><L>" + long + "</L></properties><dependencyManagement><dependencies>")
	for i := 0; i < k; i++ {
		parent.WriteString(r4import("${L}${project.artifactId}", fmt.Sprintf("${L}b%d", i), "${L}"))
	}
	parent.WriteString("</dependencies></dependencyManagement></project>")
	files := map[string]string{"pom.xml": parent.String()}
	for m := 0; m < k; m++ {
		files[fmt.Sprintf("m%d/pom.xml", m)] = "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>" +
			fmt.Sprintf("m%d", m) + "</artifactId><dependencies>" + r4dep("x", "y", "") + "</dependencies></project>"
	}
	return files
}

// R4-M2. What a walk keeps does not grow with (poms) x (imports whose
// coordinates hold a reference). Each such import means a different BOM in
// each pom that inherits it, and the tree kept a key, up to three
// coordinates long, for every pair: 200 modules under a parent with 200
// such imports, 92 KB of poms, kept 330 MB.
func TestWhatAWalkKeepsOfReferenceKeyedImportsIsNotAProduct(t *testing.T) {
	build := r4LongImports
	kept := func(k int) uint64 {
		root := writeTree(t, build(k))
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		w := NewWalk(root, nil)
		for m := 0; m < k; m++ {
			w.Read(filepath.Join(root, fmt.Sprintf("m%d", m), "pom.xml"), Options{})
		}
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(w)
		if after.HeapAlloc < before.HeapAlloc {
			return 0
		}
		return after.HeapAlloc - before.HeapAlloc
	}
	small, large := kept(60), kept(240)
	t.Logf("kept: %d KB for 60 x 60, %d KB for 240 x 240", small>>10, large>>10)
	// Four times the files and four times the imports: sixteen times the
	// pairs. Linear in the bytes read is about four times; eight is the
	// harness's line between the two.
	if large > 8<<20 && large > 8*max(small, 1<<20) {
		t.Errorf("240 modules x 240 reference-keyed imports kept %d MB (60 x 60 kept %d KB): what is kept grows with the pairs", large>>20, small>>10)
	}
}

// BenchmarkPomLook measures what a unit of the pom work bound costs in
// time, parsing included, for the cheapest kind of look and the dearest:
// looks that pass BOM imports with literal coordinates, and looks that
// interpolate a managed entry's group through a chain of long properties.
// The two are the reason interpolation is counted by its bytes
// (pomLookBytes): the metric is ns/unit, and the bound is a bound on time
// when the two are of one order.
func BenchmarkPomLook(b *testing.B) {
	cheap := map[string]string{}
	var imports, deps strings.Builder
	for i := 0; i < 40; i++ {
		cheap[fmt.Sprintf("boms/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>org.example</groupId><artifactId>bom-%d</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>%s</dependencies></dependencyManagement></project>", i, r4dep("o", fmt.Sprintf("e%d", i), "1"))
		imports.WriteString(r4import("org.example", fmt.Sprintf("bom-%d", i), "1"))
	}
	for i := 0; i < 3000; i++ {
		deps.WriteString(r4dep("org.unmanaged", fmt.Sprintf("dep-%d", i), ""))
	}
	cheap["pom.xml"] = "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies></project>"

	var managed, asked strings.Builder
	for i := 0; i < 300; i++ {
		managed.WriteString(r4dep("${A}"+fmt.Sprint(i), "y", "1"))
		asked.WriteString(r4dep(fmt.Sprintf("x%d", i), "y", ""))
	}
	dear := map[string]string{"pom.xml": "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><properties><A>${B}</A><B>${C}</B><C>" + strings.Repeat("L", 2600) + "</C></properties><dependencyManagement><dependencies>" + managed.String() + "</dependencies></dependencyManagement><dependencies>" + asked.String() + "</dependencies></project>"}

	for _, c := range []struct {
		name  string
		files map[string]string
	}{{"literal imports", cheap}, {"long interpolation", dear}} {
		b.Run(c.name, func(b *testing.B) {
			root := b.TempDir()
			for rel, content := range c.files {
				p := filepath.Join(root, filepath.FromSlash(rel))
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
					b.Fatal(err)
				}
			}
			units := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := NewWalk(root, nil)
				w.Read(filepath.Join(root, "pom.xml"), Options{})
				units += w.pom.work.used
			}
			if units == 0 {
				b.Fatal("no unit of work was counted")
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(units), "ns/unit")
			b.ReportMetric(float64(units)/float64(b.N), "units/read")
		})
	}
}

// R4-M6 (M20). Passing an import whose coordinates hold a reference is a
// look like passing any other: a pom of n version-less dependencies and n
// such imports looks at n squared of them, and ends at its bound. (What the
// coordinates mean is worked out once for the file; the passing is what is
// counted here.)
func TestPassingAReferenceKeyedImportIsALook(t *testing.T) {
	const n = 800
	files := map[string]string{}
	var imports, deps strings.Builder
	for i := 0; i < n; i++ {
		files[fmt.Sprintf("boms/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>t</groupId><artifactId>bom%d</artifactId><version>1</version><packaging>pom</packaging></project>", i)
		imports.WriteString(r4import("t", fmt.Sprintf("bom%d", i), "${bv}"))
		deps.WriteString(r4dep("x", fmt.Sprintf("d%d", i), ""))
	}
	files["pom.xml"] = "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><properties><bv>1</bv></properties><dependencyManagement><dependencies>" +
		imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies></project>"
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	got := w.Read(filepath.Join(root, "pom.xml"), Options{})
	if got.Failure == nil {
		t.Errorf("%d version-less dependencies against %d reference-keyed imports were read (%d units of %d): the passing of such an import is not counted", n, n, w.pom.work.used, w.pom.work.limit)
	}
	// A tenth of it on each side is a hundredth of the looks, and is read.
	small := map[string]string{}
	imports.Reset()
	deps.Reset()
	for i := 0; i < n/10; i++ {
		small[fmt.Sprintf("boms/%d/pom.xml", i)] = files[fmt.Sprintf("boms/%d/pom.xml", i)]
		imports.WriteString(r4import("t", fmt.Sprintf("bom%d", i), "${bv}"))
		deps.WriteString(r4dep("x", fmt.Sprintf("d%d", i), ""))
	}
	small["pom.xml"] = "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><properties><bv>1</bv></properties><dependencyManagement><dependencies>" +
		imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies></project>"
	root = writeTree(t, small)
	if got := NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{}); got.Failure != nil {
		t.Errorf("the same shape at a tenth of the size was refused: %v", got.Failure)
	}
}

// R4-M9. Interpolation is counted by its bytes. A pom whose dependencies
// each compare against managed entries keyed by a long property asks a
// number of questions its size allows, and copies kilobytes for each: it
// ends at its bound for the copying.
func TestLongInterpolationIsCountedByItsBytes(t *testing.T) {
	build := func(n int) string {
		var managed, asked strings.Builder
		for i := 0; i < n; i++ {
			managed.WriteString(r4dep("${A}"+fmt.Sprint(i), "y", "1"))
			asked.WriteString(r4dep(fmt.Sprintf("x%d", i), "y", ""))
		}
		// Padding: the file's size allows every one of the n*n looks.
		pad := "<!--" + strings.Repeat("p", 2*n*n) + "-->"
		return "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>" + pad + "<properties><A>${B}</A><B>${C}</B><C>" + strings.Repeat("L", 2600) +
			"</C></properties><dependencyManagement><dependencies>" + managed.String() + "</dependencies></dependencyManagement><dependencies>" + asked.String() + "</dependencies></project>"
	}
	const n = 200
	root := writeTree(t, map[string]string{"pom.xml": build(n)})
	w := NewWalk(root, nil)
	got := w.Read(filepath.Join(root, "pom.xml"), Options{})
	t.Logf("%d questions against %d entries keyed by a 2,600-byte property: %d units of %d", n, n, w.pom.work.used, w.pom.work.limit)
	if w.pom.work.limit < n*n {
		t.Fatalf("the allowance %d does not cover the %d looks: the premise does not hold", w.pom.work.limit, n*n)
	}
	if got.Failure == nil {
		t.Errorf("%d looks of several kilobytes each were inside the bound (%d units of %d): the bytes are not counted", n*n, w.pom.work.used, w.pom.work.limit)
	}
}
