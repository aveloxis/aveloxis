// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// Review round 6 (M6-4, M6-5). A substitution resolves each NAME once,
// however many times the string holds it and however many passes it takes:
// resolving walks a chain of up to pomChainLimit poms, and a property that
// names another five hundred times walked it for each, in each pass, for
// each dependency (9 microseconds for every byte of pom read), and for the
// coordinates of every pom of the tree when the tree was indexed.
func TestASubstitutionResolvesEachNameOnce(t *testing.T) {
	calls := map[string]int{}
	values := map[string]string{"a": "${b}${b}${c}", "b": "x", "c": "${b}-${d}", "d": "end"}
	resolve := func(name string) (string, bool) {
		calls[name]++
		v, ok := values[name]
		return v, ok
	}
	in := strings.Repeat("${a}", 100) + "${unknown}${a}${unknown}"
	got := pomSubstitute(in, resolve)
	if want := strings.Repeat("xxx-end", 100) + "${unknown}xxx-end${unknown}"; got != want {
		t.Fatalf("substituted to %.80q..., want %.80q...", got, want)
	}
	for _, name := range []string{"a", "b", "c", "d", "unknown"} {
		if calls[name] != 1 {
			t.Errorf("%q was resolved %d times in one substitution; want once", name, calls[name])
		}
	}
	// Nothing is remembered from one call to the next: the value is the
	// model's, and another model may answer otherwise.
	values["b"] = "y"
	if got := pomSubstitute("${a}", resolve); got != "yyy-end" {
		t.Errorf("a second substitution gave %q; want it resolved afresh (yyy-end)", got)
	}
	// And the usual string, one reference, is resolved as before.
	if got := pomSubstitute("${d}", resolve); got != "end" {
		t.Errorf("one reference: %q", got)
	}
}

// The shape the reviewer measured, counted where a clock cannot tell: a leaf
// under thirty parents whose dependencies take their version from a property
// that names another several hundred times. Each dependency's version walks
// the chain for each NAME, not for each occurrence.
func TestALongPropertyUnderADeepChainWalksTheChainOncePerName(t *testing.T) {
	const depth, repeats, deps = 30, 400, 50
	files := map[string]string{}
	rel := func(i int) string { return strings.Repeat("d/", i) + "pom.xml" }
	for i := 0; i < depth; i++ {
		props := ""
		if i == 0 {
			props = "<properties><aa>1</aa><p0>" + strings.Repeat("${aa}", repeats) + "</p0></properties>"
		}
		parent := ""
		if i > 0 {
			parent = fmt.Sprintf("<parent><groupId>t</groupId><artifactId>p%d</artifactId><version>1</version></parent>", i-1)
		}
		files[rel(i)] = fmt.Sprintf("<project>%s<groupId>t</groupId><artifactId>p%d</artifactId><version>1</version><packaging>pom</packaging>%s</project>", parent, i, props)
	}
	var leaf strings.Builder
	fmt.Fprintf(&leaf, "<project><parent><groupId>t</groupId><artifactId>p%d</artifactId><version>1</version></parent><artifactId>leaf</artifactId><dependencies>", depth-1)
	for i := 0; i < deps; i++ {
		fmt.Fprintf(&leaf, "<dependency><groupId>g</groupId><artifactId>a%d</artifactId><version>${p0}</version></dependency>", i)
	}
	leaf.WriteString("</dependencies></project>")
	files[rel(depth)] = leaf.String()
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	got := w.Read(filepath.Join(root, filepath.FromSlash(rel(depth))), Options{})
	if got.Failure != nil || len(got.Deps) != deps {
		t.Fatalf("%d dependencies, failure %v", len(got.Deps), got.Failure)
	}
	for _, d := range got.Deps {
		if d.Version != strings.Repeat("1", repeats) {
			t.Fatalf("%s has version %.40q; want the property's value", d.Name, d.Version)
		}
	}
	m := w.pom.model(filepath.Join(root, filepath.FromSlash(rel(depth))))
	if c := w.pom.context(m); len(c.chain) != depth+1 {
		t.Fatalf("the leaf's chain is %d poms, want %d (the premise)", len(c.chain), depth+1)
	}
}
