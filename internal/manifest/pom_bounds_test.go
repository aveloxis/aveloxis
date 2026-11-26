// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// The Maven reader bounds its WORK, not only its steps (review round 1 of
// the vulnerability program's first part, both High). A pom is written by
// whoever owns the repository being collected.

// readWithin reads one pom and fails the test if the read does not return
// in time: an unbounded read is the defect, and a hung test says nothing.
func readWithin(t *testing.T, root, rel string, limit time.Duration) []model.LibyearDep {
	t.Helper()
	done := make(chan []model.LibyearDep, 1)
	go func() { done <- NewWalk(root, nil).Read(filepath.Join(root, rel), Options{}).Deps }()
	select {
	case deps := <-done:
		return deps
	case <-time.After(limit):
		t.Fatalf("reading %s did not finish within %s", rel, limit)
		return nil
	}
}

// Property references multiply: p0 is p1 twenty times over, p1 is p2 twenty
// times over, five levels down. Ten passes are plenty to expand it all: a
// 1,290-byte pom of this shape with forty repetitions produced a 102 MB
// version string and allocated 3.7 GB. The bound is on what comes out.
func TestPomInterpolationIsBoundedInSize(t *testing.T) {
	const levels, fan = 5, 20
	var props strings.Builder
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&props, "<p%d>%s</p%d>", i, strings.Repeat(fmt.Sprintf("${p%d}", i+1), fan), i)
	}
	fmt.Fprintf(&props, "<p%d>x</p%d><ok>1.2.3</ok>", levels, levels)
	pom := `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><properties>` + props.String() + `</properties>
<dependencies>
  <dependency><groupId>x</groupId><artifactId>bomb</artifactId><version>${p0}</version></dependency>
  <dependency><groupId>x</groupId><artifactId>fine</artifactId><version>${ok}</version></dependency>
</dependencies></project>`
	root := writeTree(t, map[string]string{"pom.xml": pom})
	deps := readWithin(t, root, "pom.xml", 20*time.Second)
	got := map[string]string{}
	for _, d := range deps {
		got[d.Name] = d.Version
	}
	// What cannot be expanded within the bound is not a version: the
	// dependency is kept, with none.
	if v, ok := got["x:bomb"]; !ok || len(v) > pkgid.MaxNameBytes {
		t.Errorf("x:bomb: present %v, version of %d bytes; want the dependency kept and no version longer than %d bytes", ok, len(v), pkgid.MaxNameBytes)
	}
	// And what is left still holds a reference, which is how this reader
	// has always marked a version it could not resolve (a `${undefined}`
	// stays as written, and every later step reads it as no version).
	if v := got["x:bomb"]; v != "" && !strings.Contains(v, "${") {
		t.Errorf("x:bomb carries a fully expanded string as its version (%d bytes): %.60q", len(v), v)
	}
	// An ordinary property beside it still resolves.
	if got["x:fine"] != "1.2.3" {
		t.Errorf("x:fine = %q, want 1.2.3", got["x:fine"])
	}
}

// Two poms that import each other as BOMs, each listing the import twice.
// With a depth limit and no memory of what was visited, the lookup of a
// dependency neither manages doubles at every level: 2^32 calls.
func TestBOMImportCyclesEnd(t *testing.T) {
	bom := func(artifact, other string) string {
		imp := `<dependency><groupId>g</groupId><artifactId>` + other + `</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>`
		return `<project><groupId>g</groupId><artifactId>` + artifact + `</artifactId><version>1</version><packaging>pom</packaging>
<dependencyManagement><dependencies>` + imp + imp + `</dependencies></dependencyManagement>
<dependencies><dependency><groupId>x</groupId><artifactId>unmanaged</artifactId></dependency></dependencies></project>`
	}
	root := writeTree(t, map[string]string{"a/pom.xml": bom("a", "b"), "b/pom.xml": bom("b", "a")})
	deps := readWithin(t, root, "a/pom.xml", 20*time.Second)
	found := false
	for _, d := range deps {
		if d.Name == "x:unmanaged" {
			found = true
			if d.Version != "" {
				t.Errorf("x:unmanaged has version %q; nothing manages it", d.Version)
			}
		}
	}
	if !found {
		t.Errorf("x:unmanaged is not among the dependencies: %+v", deps)
	}
}

// A BOM reached twice by two different routes (a diamond) is not a cycle,
// and what it manages is still found, whichever route is tried first and
// however many routes lead to it.
func TestABOMReachedByTwoRoutesStillAnswers(t *testing.T) {
	imp := func(artifact string) string {
		return `<dependency><groupId>g</groupId><artifactId>` + artifact + `</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>`
	}
	bom := func(artifact, managed string) string {
		return `<project><groupId>g</groupId><artifactId>` + artifact + `</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>` + managed + `</dependencies></dependencyManagement></project>`
	}
	files := map[string]string{
		"pom.xml": `<project><groupId>g</groupId><artifactId>app</artifactId><version>1</version>
<dependencyManagement><dependencies>` + imp("left") + imp("right") + `</dependencies></dependencyManagement>
<dependencies>
  <dependency><groupId>x</groupId><artifactId>deep</artifactId></dependency>
  <dependency><groupId>x</groupId><artifactId>onlyright</artifactId></dependency>
  <dependency><groupId>x</groupId><artifactId>nowhere</artifactId></dependency>
</dependencies></project>`,
		"left/pom.xml":  bom("left", imp("base")),
		"right/pom.xml": bom("right", imp("base")+`<dependency><groupId>x</groupId><artifactId>onlyright</artifactId><version>2.0</version></dependency>`),
		"base/pom.xml":  bom("base", `<dependency><groupId>x</groupId><artifactId>deep</artifactId><version>9.9</version></dependency>`),
	}
	// A wide, deep lattice beside it: every level imports both poms of the
	// next. Sixteen levels are 65,536 routes to the bottom.
	const depth = 16
	for i := 0; i < depth; i++ {
		next := imp(fmt.Sprintf("l%da", i+1)) + imp(fmt.Sprintf("l%db", i+1))
		if i == depth-1 {
			next = `<dependency><groupId>x</groupId><artifactId>bottom</artifactId><version>7</version></dependency>`
		}
		files[fmt.Sprintf("l%da/pom.xml", i)] = bom(fmt.Sprintf("l%da", i), next)
		files[fmt.Sprintf("l%db/pom.xml", i)] = bom(fmt.Sprintf("l%db", i), next)
	}
	files["lattice/pom.xml"] = `<project><groupId>g</groupId><artifactId>lattice</artifactId><version>1</version>
<dependencyManagement><dependencies>` + imp("l0a") + imp("l0b") + `</dependencies></dependencyManagement>
<dependencies>
  <dependency><groupId>x</groupId><artifactId>bottom</artifactId></dependency>
  <dependency><groupId>x</groupId><artifactId>absent</artifactId></dependency>
</dependencies></project>`
	root := writeTree(t, oneBuild(files))
	got := map[string]string{}
	for _, d := range readWithin(t, root, "pom.xml", 20*time.Second) {
		got[d.Name] = d.Version
	}
	for name, want := range map[string]string{"x:deep": "9.9", "x:onlyright": "2.0", "x:nowhere": ""} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
	lat := map[string]string{}
	for _, d := range readWithin(t, root, "lattice/pom.xml", 20*time.Second) {
		lat[d.Name] = d.Version
	}
	if lat["x:bottom"] != "7" || lat["x:absent"] != "" {
		t.Errorf("the lattice: bottom %q (want 7), absent %q (want none)", lat["x:bottom"], lat["x:absent"])
	}
}
