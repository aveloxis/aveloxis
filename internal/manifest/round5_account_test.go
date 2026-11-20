// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5 (M5-1, M5-2): what a walk keeps, and what it asks of the
// source of poms, measured where the review measured them: with a source
// installed and SettleMavenManaged run.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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

// r5KeptFactor is the measure the two shapes below are held to: what the
// walk keeps is no more than this many times the bytes of the poms. It was
// the walk's memory allowance (walkMemFactor, 0.29.212), which is gone; here
// it only says that these two shapes are not products any more (they kept
// 1,100 and 145 times their poms).
const r5KeptFactor = 16

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
	return r5WalkLimited(t, files, rels, src, 0)
}

func r5WalkLimited(t *testing.T, files map[string]string, rels []string, src PomSource, limit int) (w *Walk, notWhole int, kept uint64, refused int) {
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
	w.SetRemotePomLimit(limit)
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

// M5-1. An import whose coordinates hold a reference each module answers
// differently is another coordinate for every module, and modules times
// imports of them are asked for and kept: 120 modules under 120 imports with
// 2,600-byte coordinates (199 KB) asked 14,520 times and kept 223 MB. The
// walk's limit on the coordinates it asks for bounds both: the source is
// asked that many times, that many answers are kept, the rest are counted as
// not asked, and every tree that wanted one is not whole.
func TestTheRemotePomLimitBoundsWhatIsAskedAndKept(t *testing.T) {
	const limit = 50
	for _, k := range []int{60, 120} {
		files, rels := r5PairReactor(k, 2600)
		src := &countingPomSource{}
		w, notWhole, kept, _ := r5WalkLimited(t, files, rels, src, limit)
		t.Logf("k=%d: %d KB of poms; the source was asked %d times; %d answers kept, %d KB; %d questions not asked", k, r5Bytes(files)>>10, src.asked, len(w.pom.remoteAnswers), kept>>10, w.RemotePomsNotAsked())
		if src.asked != limit || len(w.pom.remoteAnswers) != limit {
			t.Errorf("k=%d: the source was asked %d times and %d answers are kept; the limit is %d", k, src.asked, len(w.pom.remoteAnswers), limit)
		}
		if w.RemotePomsNotAsked() == 0 {
			t.Errorf("k=%d: no question is counted as not asked", k)
		}
		if notWhole != 1 {
			t.Errorf("k=%d: %d trees not whole, want the one: an import that was not asked about is not known to be there", k, notWhole)
		}
		// What k squared answers took is not kept: the answers kept are the
		// limit's, each a key of three padded coordinates.
		if most := uint64(limit*3*2700*2 + r5HeapNoise + r5KeptFactor*r5Bytes(files)); kept > most {
			t.Errorf("k=%d: the walk keeps %d bytes; %d answers of at most 8 KB and the poms themselves are %d", k, kept, limit, most)
		}
	}
	// Without a limit of its own a walk asks for every coordinate, once.
	files, rels := r5PairReactor(20, 0)
	src := &countingPomSource{}
	w, _, _, _ := r5Walk(t, files, rels, src)
	if src.asked != 21*20 || w.RemotePomsNotAsked() != 0 {
		t.Errorf("with no limit: asked %d times (want 420: each of the 20 imports once for the parent and once for each of the 20 modules), %d not asked", src.asked, w.RemotePomsNotAsked())
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
		_, _, kept, _ := r5Walk(t, files, rels, &countingPomSource{})
		allowed := r5KeptFactor * r5Bytes(files)
		t.Logf("version-less=%v: %d KB of poms; the walk keeps %d KB (the measure is %d KB)", versionless, r5Bytes(files)>>10, kept>>10, allowed>>10)
		if int(kept) > allowed+r5HeapNoise {
			t.Errorf("the walk keeps %d bytes for %d bytes of poms; the measure of this shape is %d", kept, r5Bytes(files), allowed)
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
		got := c.declaredFor()
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
	if funcs < 250 {
		t.Fatalf("only %d functions examined: the package was not parsed", funcs)
	}
	if len(sites) != 1 || sites[0] != "remotePom" {
		t.Errorf("the PomSource is called in %v; the one place is pomTree.remotePom, where the request is counted", sites)
	}
}
