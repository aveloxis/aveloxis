// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5 (M5-1, M5-2): what a walk keeps, and what it asks of the
// source of poms, measured where the review measured them: with a source
// installed and SettleMavenManaged run.

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aveloxis/aveloxis/internal/model"
)

// countingPomSource answers that it has no such pom, and counts the
// requests. It keeps nothing of what it is asked: the tests measure what the
// WALK keeps.
type countingPomSource struct{ asked int }

func (s *countingPomSource) RemotePom(string, string, string, []string) (*RemotePom, RemoteOutcome) {
	s.asked++
	return nil, RemoteNotFound
}

// r5HeapNoise is what two readings of the heap differ by when nothing was
// kept between them (the test's own log lines and bookkeeping).
const r5HeapNoise = 256 << 10

func r5Bytes(files map[string]string) int {
	n := 0
	for _, c := range files {
		n += len(c)
	}
	return n
}

// r5Walk reads rels in order, runs SettleMavenManaged, and returns the walk,
// how many trees settling found not whole, and the heap the WALK keeps
// afterwards: what was read and what settling returned are the caller's,
// and are dropped before the heap is measured.
func r5Walk(t *testing.T, files map[string]string, rels []string, src PomSource) (w *Walk, notWhole int, kept uint64, refused int) {
	t.Helper()
	root := writeTree(t, files)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	w = NewWalk(root, nil)
	if src != nil {
		w.SetPomSource(src)
	}
	var all []model.LibyearDep
	for _, rel := range rels {
		got := w.Read(filepath.Join(root, rel), Options{})
		if got.Failure != nil {
			refused++
		}
		for i := range got.Deps {
			got.Deps[i].ManifestPath = rel
		}
		all = append(all, got.Deps...)
	}
	notWhole = len(w.SettleMavenManaged(all).NotWhole)
	all = nil
	_ = all
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(w)
	if after.HeapAlloc > before.HeapAlloc {
		kept = after.HeapAlloc - before.HeapAlloc
	}
	return w, notWhole, kept, refused
}

// r5PairReactor is k modules under a parent with k BOM imports whose
// coordinates hold a reference each module answers differently
// (${project.artifactId}): every (module, import) pair is another coordinate.
// long pads each coordinate through a property.
func r5PairReactor(k, long int) (files map[string]string, rels []string) {
	pad, props := "", ""
	if long > 0 {
		pad, props = "${L}", "<properties><L>"+strings.Repeat("L", long)+"</L></properties>"
	}
	var parent strings.Builder
	parent.WriteString("<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>" + props + "<dependencyManagement><dependencies>")
	for i := 0; i < k; i++ {
		parent.WriteString(r4import(pad+"org.x.${project.artifactId}", fmt.Sprintf("%sb%d", pad, i), "1"))
	}
	parent.WriteString("</dependencies></dependencyManagement></project>")
	files = map[string]string{"pom.xml": parent.String()}
	rels = []string{"pom.xml"}
	for m := 0; m < k; m++ {
		rel := fmt.Sprintf("m%d/pom.xml", m)
		files[rel] = "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>" +
			fmt.Sprintf("m%d", m) + "</artifactId><dependencies>" + r4dep("x", "y", "1.0") + "</dependencies></project>"
		rels = append(rels, rel)
	}
	return files, rels
}

// M5-1, requests. The modules times the imports they inherit is not what the
// source is asked: the requests of a walk are bounded by what it read
// (pomRequestBytes of pom to a request). Before, 480 modules under 480 such
// imports (194 KB) asked the source 199,651 times.
func TestTheSourceIsAskedInProportionToWhatWasRead(t *testing.T) {
	for _, k := range []int{120, 480} {
		files, rels := r5PairReactor(k, 0)
		src := &countingPomSource{}
		w, notWhole, _, _ := r5Walk(t, files, rels, src)
		budget := w.acct.read / pomRequestBytes
		t.Logf("k=%d: %d KB of poms, the source asked %d times (allowed %d), %d trees not whole", k, r5Bytes(files)>>10, src.asked, budget, notWhole)
		if src.asked > budget {
			t.Errorf("k=%d: the source was asked %d times for %d bytes of poms; the walk's request allowance is %d", k, src.asked, r5Bytes(files), budget)
		}
		if got := w.acct.asked; got != src.asked {
			t.Errorf("the walk counted %d requests and the source %d: a request went round the counter", got, src.asked)
		}
		// Not asking is not an answer: the trees whose imports were not all
		// asked about are not whole, and say why.
		if k*k > budget && notWhole == 0 {
			t.Errorf("k=%d: imports were not asked about, and no tree says it was not read whole", k)
		}
	}
}

// M5-1, memory. Nothing is kept per (pom, import): 120 modules under 120
// imports with 2,600-byte coordinates (199 KB) kept 29,240 answers, 223 MB.
func TestSettlingKeepsNoAnswerPerPomAndImport(t *testing.T) {
	for _, k := range []int{60, 120} {
		files, rels := r5PairReactor(k, 2600)
		w, _, kept, _ := r5Walk(t, files, rels, &countingPomSource{})
		allowed := w.acct.limit()
		t.Logf("k=%d: %d KB of poms; the walk keeps %d KB; accounted %d KB of %d KB allowed; %d answers kept", k, r5Bytes(files)>>10, kept>>10, w.acct.kept>>10, allowed>>10, len(w.pom.remoteAnswers))
		if int(kept) > allowed+r5HeapNoise {
			t.Errorf("k=%d: the walk keeps %d bytes for %d bytes of poms; its memory allowance is %d", k, kept, r5Bytes(files), allowed)
		}
		if w.acct.kept > allowed {
			t.Errorf("k=%d: accounted %d bytes kept, past the allowance %d", k, w.acct.kept, allowed)
		}
	}
}

// M5-2. The repositories a chain declares are the declaring pom's list,
// shared: 2,000 modules under a parent declaring 2,000 repositories (574 KB)
// kept a copy per module, 83 MB.
func TestModulesTimesDeclaredRepositoriesIsNotKept(t *testing.T) {
	for _, versionless := range []bool{true, false} {
		k := 1000
		var parent strings.Builder
		parent.WriteString("<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><repositories>")
		for i := 0; i < k; i++ {
			fmt.Fprintf(&parent, "<repository><url>https://r.example/%d</url></repository>", i)
		}
		parent.WriteString("</repositories><dependencyManagement><dependencies>" + r4import("org.x", "bom", "1") + "</dependencies></dependencyManagement></project>")
		files := map[string]string{"pom.xml": parent.String()}
		v := "1.0"
		if versionless {
			v = ""
		}
		var rels []string
		for m := 0; m < k; m++ {
			rel := fmt.Sprintf("m%d/pom.xml", m)
			files[rel] = "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>" +
				fmt.Sprintf("m%d", m) + "</artifactId><dependencies>" + r4dep("x", "y", v) + "</dependencies></project>"
			rels = append(rels, rel)
		}
		w, _, kept, _ := r5Walk(t, files, rels, &countingPomSource{})
		allowed := w.acct.limit()
		t.Logf("version-less=%v: %d KB of poms; the walk keeps %d KB; accounted %d KB of %d KB allowed", versionless, r5Bytes(files)>>10, kept>>10, w.acct.kept>>10, allowed>>10)
		if int(kept) > allowed+r5HeapNoise {
			t.Errorf("the walk keeps %d bytes for %d bytes of poms; its memory allowance is %d", kept, r5Bytes(files), allowed)
		}
	}
}

// The declared repositories a request is given are what they were: the
// nearest pom's first, each once, a fetched pom's not followed.
func TestTheDeclaredRepositoriesOfAChainAreSharedAndUnchanged(t *testing.T) {
	repo := func(urls ...string) string {
		s := "<repositories>"
		for _, u := range urls {
			s += "<repository><url>" + u + "</url></repository>"
		}
		return s + "</repositories>"
	}
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version>" + repo("https://a.example/r", "https://b.example/r", "http://plain.example/r") + "</project>",
		"mid/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>mid</artifactId>" +
			repo("https://c.example/r", "https://a.example/r") + "</project>",
		"mid/leaf/pom.xml":  "<project><parent><groupId>t</groupId><artifactId>mid</artifactId><version>1</version></parent><artifactId>leaf</artifactId></project>",
		"mid/leaf2/pom.xml": "<project><parent><groupId>t</groupId><artifactId>mid</artifactId><version>1</version></parent><artifactId>leaf2</artifactId></project>",
		"side/pom.xml":      "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>side</artifactId></project>",
		"side2/pom.xml":     "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>side2</artifactId></project>",
	}
	root := writeTree(t, files)
	tree := NewWalk(root, nil).pom
	list := func(rel string) []string {
		m := tree.model(filepath.Join(root, rel))
		if m == nil {
			t.Fatalf("%s was not read", rel)
		}
		c := tree.context(m)
		got, _ := c.declaredFor()
		if want := fetchableDeclared(c.chain); strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("%s: declared() is %v, the chain declares %v", rel, got, want)
		}
		return got
	}
	if got := strings.Join(list("mid/leaf/pom.xml"), " "); got != "https://c.example/r https://a.example/r https://b.example/r" {
		t.Errorf("leaf: %s", got)
	}
	// Two poms that declare nothing themselves under one that does hold ITS
	// list, not a copy each.
	a, b := list("side/pom.xml"), list("side2/pom.xml")
	if len(a) != 2 || len(b) != 2 || &a[0] != &b[0] {
		t.Errorf("two modules of one parent do not share its list of declared repositories: %v %v", a, b)
	}
}

// The source is asked in ONE place, pomTree.remotePom, which counts the
// request: a second call site would be a request the walk does not count.
func TestOnlyRemotePomAsksTheSource(t *testing.T) {
	fset := token.NewFileSet()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	funcs := 0
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			funcs++
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "RemotePom" {
						sites = append(sites, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if funcs < 300 {
		t.Fatalf("only %d functions examined: the package was not parsed", funcs)
	}
	if len(sites) != 1 || sites[0] != "remotePom" {
		t.Errorf("the PomSource is called in %v; the one place is pomTree.remotePom, where the request is counted", sites)
	}
}

// What a build WRITES can always be asked about: a reactor whose every
// module names a parent and two BOMs that are not in the clone asks for each
// of them once, and nothing is refused.
func TestWhatABuildWritesIsAlwaysAsked(t *testing.T) {
	const k = 300
	files := map[string]string{}
	var rels []string
	for m := 0; m < k; m++ {
		rel := fmt.Sprintf("m%d/pom.xml", m)
		files[rel] = fmt.Sprintf("<project><parent><groupId>g</groupId><artifactId>p%d</artifactId><version>1</version><relativePath/></parent><artifactId>m%d</artifactId>", m, m) +
			"<dependencyManagement><dependencies>" + r4import("g", fmt.Sprintf("a%d", m), "1") + r4import("g", fmt.Sprintf("b%d", m), "1") + "</dependencies></dependencyManagement>" +
			"<dependencies>" + r4dep("x", "y", "") + "</dependencies></project>"
		rels = append(rels, rel)
	}
	src := &countingPomSource{}
	w, _, _, refused := r5Walk(t, files, rels, src)
	if src.asked != 3*k || w.acct.askRefused != 0 || refused != 0 {
		t.Errorf("%d modules that each write a parent and two imports: asked %d times (want %d), %d requests refused, %d files refused; allowance %d", k, src.asked, 3*k, w.acct.askRefused, refused, w.acct.read/pomRequestBytes)
	}
	if got := w.acct.read / pomRequestBytes; got < 2*3*k {
		t.Errorf("the allowance of requests is %d for %d written requests: less than twice what the build writes", got, 3*k)
	}
}

// Honest builds use a small part of the memory allowance, and what the walk
// keeps is what the account says (the heap is measured).
func TestHonestBuildsAreFarInsideTheMemoryAllowance(t *testing.T) {
	publishing, reads := r4Publishing(1000, 60<<10)
	reactor := r4Reactor(600, 30, 300, 2200)
	var rels []string
	for rel := range reactor {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, c := range []struct {
		name  string
		files map[string]string
		rels  []string
	}{
		{"a reactor of 600 modules under a parent of 2,500 managed entries", reactor, rels},
		{"1,000 projects that each apply a 60 KB publishing script", publishing, reads},
	} {
		w, _, kept, refused := r5Walk(t, c.files, c.rels, nil)
		t.Logf("%s: %d KB read; accounted %d KB of %d KB allowed (%.0f%%); the heap keeps %d KB; %d structures not kept", c.name, r5Bytes(c.files)>>10, w.acct.kept>>10, w.acct.limit()>>10,
			100*float64(w.acct.kept)/float64(w.acct.limit()), kept>>10, w.acct.notKept)
		if refused != 0 || w.acct.notKept != 0 {
			t.Errorf("%s: %d files refused, %d structures not kept: an honest build is inside every allowance", c.name, refused, w.acct.notKept)
		}
		if w.acct.kept*r5HonestMemoryMargin > w.acct.limit() {
			t.Errorf("%s: accounted %d of %d: less than a %d-fold margin", c.name, w.acct.kept, w.acct.limit(), r5HonestMemoryMargin)
		}
		if int(kept) > w.acct.kept+r5HeapNoise {
			t.Errorf("%s: the heap keeps %d bytes and the account says %d: something is kept that is not registered", c.name, kept, w.acct.kept)
		}
	}
}

// r5HonestMemoryMargin is the margin the honest shapes are held to: a
// quarter of the allowance (walkMemFactor's derivation).
const r5HonestMemoryMargin = 4

// What hostile shapes make a walk keep is inside the allowance, measured on
// the heap, and not only by the account's own count.
func TestWhatAWalkKeepsIsInsideItsAllowanceOnTheHeap(t *testing.T) {
	fan, fanReads := r4Fanout(400)
	blank, blankReads := r5SharedScript(40, 1<<20, "\n")
	// 1,500 directories, each with a line of its own, under a
	// gradle.properties of 1,500 lines.
	deepProps := map[string]string{"settings.gradle": "rootProject.name='r'\n"}
	var props strings.Builder
	for i := 0; i < 1500; i++ {
		fmt.Fprintf(&props, "p%d=%d\n", i, i)
	}
	deepProps["gradle.properties"] = props.String()
	var propReads []string
	for i := 0; i < 1500; i++ {
		deepProps[fmt.Sprintf("d%d/gradle.properties", i)] = fmt.Sprintf("q=%d\n", i)
		rel := fmt.Sprintf("d%d/build.gradle", i)
		deepProps[rel] = "dependencies {\n  implementation \"g:a:$q\"\n}\n"
		propReads = append(propReads, rel)
	}
	pairs, pairRels := r5PairReactor(100, 2600)
	for _, c := range []struct {
		name  string
		files map[string]string
		rels  []string
		src   PomSource
	}{
		{"a script applied by 400 directories that applies 400 scripts", fan, fanReads, nil},
		{"40 projects applying a megabyte of line ends", blank, blankReads, nil},
		{"1,500 directories under 1,500 properties", deepProps, propReads, nil},
		{"100 modules under 100 imports with long coordinates", pairs, pairRels, &countingPomSource{}},
	} {
		w, _, kept, _ := r5Walk(t, c.files, c.rels, c.src)
		t.Logf("%s: %d KB read; accounted %d KB of %d KB allowed; the heap keeps %d KB; %d structures not kept", c.name, r5Bytes(c.files)>>10, w.acct.kept>>10, w.acct.limit()>>10, kept>>10, w.acct.notKept)
		if w.acct.kept > w.acct.limit() {
			t.Errorf("%s: accounted %d past the allowance %d", c.name, w.acct.kept, w.acct.limit())
		}
		if int(kept) > w.acct.limit()+r5HeapNoise {
			t.Errorf("%s: the heap keeps %d bytes; the allowance is %d", c.name, kept, w.acct.limit())
		}
	}
}

// The sizes the account's estimates use are the structs' sizes.
func TestTheAccountsSizesAreTheStructs(t *testing.T) {
	for _, c := range []struct {
		name      string
		estimated int
		actual    uintptr
	}{
		{"gradleDeclBytes", gradleDeclBytes, unsafe.Sizeof(gradleDecl{})},
		{"gradleRepoBytes", gradleRepoBytes, unsafe.Sizeof(gradleRepo{})},
		{"gradleTokBytes", gradleTokBytes, unsafe.Sizeof(gradleTok{})},
		{"gradleKnownBytes", gradleKnownBytes, unsafe.Sizeof(gradleKnown{})},
		{"pomContextBytes", pomContextBytes, unsafe.Sizeof(pomContext{})},
		{"pomModelBytes", pomModelBytes, unsafe.Sizeof(pomModel{}) + unsafe.Sizeof(pomIndex{}) + unsafe.Sizeof(pomParentRef{})},
		{"pomEntryBytes", pomEntryBytes, unsafe.Sizeof(pomManaged{})},
		{"gradleMemoEntryBytes", gradleMemoEntryBytes, unsafe.Sizeof(gradleMemo{})},
		{"gradleMemoEntryBytes (a run)", gradleMemoEntryBytes, unsafe.Sizeof(gradleBundle{})},
		{"gradleBundleLinkBytes", gradleBundleLinkBytes, unsafe.Sizeof(gradleVars{})},
		{"a layer in keepKnown (four words)", 4 * walkWord, unsafe.Sizeof(gradleVars{})},
	} {
		if uintptr(c.estimated) < c.actual {
			t.Errorf("%s is %d and the struct it stands for is %d bytes", c.name, c.estimated, c.actual)
		}
	}
}

// fakeClock is a clock a test moves.
type fakeClock struct{ at time.Time }

func (c *fakeClock) now() time.Time { return c.at }

// The backstop. It never fires on the clock the suite runs on; moved past
// its allowance, it ends the walk's build-tree readers: the file being read
// and every one after it is refused with a reason that says it was the
// backstop, the trees are not whole, and the log says at Warn that a path
// was not accounted for.
func TestTheWallClockBackstopRefusesWhatIsLeftByName(t *testing.T) {
	files := r4Reactor(20, 30, 10, 40)
	gradle, gradleReads := r4Publishing(3, 4<<10)
	for rel, c := range gradle {
		files["g/"+rel] = c
	}
	root := writeTree(t, files)
	read := func(late bool) (failures []string, gaps []MavenTreeGap, logged string) {
		var buf bytes.Buffer
		w := NewWalk(root, slog.New(slog.NewTextHandler(&buf, nil)))
		clock := &fakeClock{at: time.Unix(1_000_000, 0)}
		w.acct.now = clock.now
		var all []model.LibyearDep
		var rels []string
		for rel := range files {
			if filepath.Base(rel) == "pom.xml" {
				rels = append(rels, rel)
			}
		}
		// The parent first: its managed entries are what a tree's gap is
		// reported for.
		sort.Slice(rels, func(i, j int) bool {
			if (rels[i] == "pom.xml") != (rels[j] == "pom.xml") {
				return rels[i] == "pom.xml"
			}
			return rels[i] < rels[j]
		})
		if len(rels) != 21 {
			t.Fatalf("the reactor has %d poms, want 21", len(rels))
		}
		for _, r := range gradleReads {
			rels = append(rels, "g/"+r)
		}
		for i, rel := range rels {
			if late && i == 3 {
				// The readers have been at it for far longer than what was
				// read allows, and the next charge of work looks at the clock.
				w.acct.spent = w.acct.backstop() + time.Second
				w.acct.sinceCheck = walkClockEvery
			}
			got := w.Read(filepath.Join(root, rel), Options{})
			if got.Failure != nil {
				failures = append(failures, rel+": "+got.Failure.Err.Error())
			}
			for j := range got.Deps {
				got.Deps[j].ManifestPath = rel
			}
			all = append(all, got.Deps...)
		}
		return failures, w.SettleMavenManaged(all).NotWhole, buf.String()
	}
	if failures, gaps, logged := read(false); len(failures) != 0 || len(gaps) != 0 || strings.Contains(logged, "backstop") {
		t.Fatalf("on time: %d failures %v, gaps %v, log %q", len(failures), failures, gaps, logged)
	}
	failures, gaps, logged := read(true)
	// The fourth file and every build file after it: 18 poms and 3 Gradle
	// files.
	if len(failures) != 21 {
		t.Errorf("%d files were refused after the backstop, want the 21 that were still to be read: %v", len(failures), failures)
	}
	for _, f := range failures {
		if !strings.Contains(f, "wall-clock backstop") {
			t.Errorf("a file refused after the backstop does not say so: %s", f)
		}
	}
	if len(gaps) != 1 || gaps[0].Reason != mavenGapBackstop {
		t.Errorf("the tree is not reported as cut short by the backstop: %v", gaps)
	}
	if strings.Count(logged, "level=WARN") < 1 || !strings.Contains(logged, "NOT ACCOUNTED FOR") || strings.Count(logged, "NOT ACCOUNTED FOR") != 1 {
		t.Errorf("the log must say once, at Warn, that a path was not accounted for: %q", logged)
	}
}

// The backstop's allowance is far above the stated bound on time, and grows
// with what is read.
func TestTheBackstopIsFarAboveTheTimeBound(t *testing.T) {
	a := newWalkAccount(nil)
	if a.backstop() != walkBackstopFloor {
		t.Errorf("with nothing read the allowance is %s, want the floor %s", a.backstop(), walkBackstopFloor)
	}
	a.readFile("", int(MaxBuildFileBytes))
	bound := time.Duration(walkUnitNanos*gradleWalkFactor) * time.Duration(MaxBuildFileBytes)
	if got := a.backstop(); got < walkBackstopMargin*bound || got > walkBackstopMargin*bound+walkBackstopFloor {
		t.Errorf("for %d bytes the allowance is %s; the stated bound is %s and the margin %d", MaxBuildFileBytes, got, bound, walkBackstopMargin)
	}
	// The clock is not looked at outside a reader, nor between two looks.
	a.now = func() time.Time { t.Fatal("the clock was read outside a reader's entry"); return time.Time{} }
	if a.tick(10 * walkClockEvery) {
		t.Error("the backstop fired outside a reader")
	}
}

// M5-4. A look costs the bytes of the coordinates it joins, hashes and
// compares: an import written with a very long groupId was one unit a
// question whatever its length (a 6 MiB groupId: 570,975 ns a unit, where a
// unit is some 300). Every question that passes it is now charged its
// length, and a pom whose questions pass it too often is past its bound.
func TestALookCostsTheBytesOfItsCoordinates(t *testing.T) {
	const long, questions = 200 << 10, 300
	group := strings.Repeat("g", long)
	var deps strings.Builder
	for d := 0; d < questions; d++ {
		deps.WriteString(r4dep("x", fmt.Sprintf("y%d", d), ""))
	}
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>" +
			r4import(group, "bom", "1") + "</dependencies></dependencyManagement></project>",
		"bom/pom.xml": "<project><groupId>" + group + "</groupId><artifactId>bom</artifactId><version>1</version><packaging>pom</packaging></project>",
		"m/pom.xml":   "<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencies>" + deps.String() + "</dependencies></project>",
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	start := time.Now()
	got := w.Read(filepath.Join(root, "m", "pom.xml"), Options{})
	took := time.Since(start)
	work := w.pom.walkWork()
	t.Logf("%d questions past an import with a %d KB groupId: %d units of %d, %d looks, %s (%.0f ns a unit), failure %v", questions, long>>10, work.used, work.limit, w.pom.looks, took.Round(time.Millisecond), float64(took.Nanoseconds())/float64(max(work.used, 1)), got.Failure != nil)
	if want := questions * long / pomLookBytes; got.Failure == nil && work.used < want {
		t.Errorf("%d questions each passed an import of %d bytes for %d units; its bytes alone are %d", questions, long, work.used, want)
	}
	if work.used > work.limit {
		t.Errorf("%d units used of %d allowed", work.used, work.limit)
	}
}

// What a file cannot be read without, and there is no room to keep: the
// file is refused, by name, and is not read as if what was not kept said
// nothing. (No file reaches this by what it holds: the allowance covers the
// densest content there is. The account is filled here to show the path.)
func TestWithNoRoomLeftAFileIsRefusedByName(t *testing.T) {
	files := map[string]string{
		"pom.xml":             "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("x", "y", "2.0") + "</dependencies></dependencyManagement><dependencies>" + r4dep("x", "y", "") + "</dependencies></project>",
		"settings.gradle":     "rootProject.name='r'\n",
		"g/gradle.properties": "v=1.0\n",
		"g/build.gradle":      "dependencies {\n  implementation \"a:b:$v\"\n}\n",
	}
	root := writeTree(t, files)
	for _, rel := range []string{"pom.xml", "g/build.gradle"} {
		var buf bytes.Buffer
		w := NewWalk(root, slog.New(slog.NewTextHandler(&buf, nil)))
		if got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{}); got.Failure != nil {
			t.Fatalf("%s with room: %v", rel, got.Failure)
		}
		buf.Reset()
		w = NewWalk(root, slog.New(slog.NewTextHandler(&buf, nil)))
		w.acct.kept = 1 << 40 // everything the walk may keep is kept already
		got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		if got.Failure == nil {
			t.Errorf("%s with no room left: read, with %d dependencies; want it refused", rel, len(got.Deps))
		}
		if !strings.Contains(buf.String(), "memory allowance") || !strings.Contains(buf.String(), "level=WARN") {
			t.Errorf("%s with no room left: the log does not say so at Warn: %q", rel, buf.String())
		}
	}
}

// The Gradle tree looks at the backstop's clock itself, where its work is
// charged: a walk of Gradle files alone is ended by it too.
func TestTheBackstopEndsAGradleWalk(t *testing.T) {
	files, reads := r4Publishing(4, 4<<10)
	root := writeTree(t, files)
	var buf bytes.Buffer
	w := NewWalk(root, slog.New(slog.NewTextHandler(&buf, nil)))
	clock := &fakeClock{at: time.Unix(1_000_000, 0)}
	w.acct.now = clock.now
	refused := 0
	for i, rel := range reads {
		if i == 1 {
			w.acct.spent = w.acct.backstop() + time.Second
			w.acct.sinceCheck = walkClockEvery
		}
		got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		switch {
		case i == 0 && got.Failure != nil:
			t.Fatalf("on time: %v", got.Failure)
		case i > 0 && (got.Failure == nil || !strings.Contains(got.Failure.Err.Error(), "wall-clock backstop")):
			t.Errorf("%s, after the backstop: failure %v; want it refused, saying so", rel, got.Failure)
		case i > 0:
			refused++
		}
	}
	if refused != 3 || strings.Count(buf.String(), "NOT ACCOUNTED FOR") != 1 {
		t.Errorf("%d files refused (want 3); log %q", refused, buf.String())
	}
}
