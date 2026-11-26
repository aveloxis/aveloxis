// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 3 of the vulnerability program's first part. Two rounds had
// bounded each FILE; the cost that was left is a product across files: what
// is built per file from what files share. The rule since 0.29.210: a
// walk's work and what it retains are linear in the bytes it reads.
// scaling_test.go measures that; these pin the mechanisms and the answers.

// What is known from outside a file is a chain of layers, looked up nearest
// first, with the meaning the copied maps had.
func TestGradleVarsAreLayersWithTheMeaningOfTheCopies(t *testing.T) {
	var base *gradleVars
	if _, ok, layers := base.get("x"); ok || layers != 0 {
		t.Error("nothing known: a name was found, or a layer counted")
	}
	one := base.push(map[string]string{"a": "1", "b": "1"}, nil)
	if one.push(nil, nil) != one {
		t.Error("a source that assigns nothing added a layer")
	}
	// A nearer layer overrides; a name its source left with two values is
	// not known below it, whatever a layer above says ...
	two := one.push(map[string]string{"a": "2"}, map[string]bool{"b": true})
	for name, want := range map[string]string{"a": "2", "b": "", "c": ""} {
		v, ok, _ := two.get(name)
		if v != want || ok != (want != "") {
			t.Errorf("%s = %q (%v), want %q", name, v, ok, want)
		}
	}
	// ... until a nearer source assigns it again.
	three := two.push(map[string]string{"b": "3"}, nil)
	if v, ok, layers := three.get("b"); v != "3" || !ok || layers != 1 {
		t.Errorf("b after a later assignment = %q (%v) in %d layers", v, ok, layers)
	}
	if _, _, layers := three.get("zz"); layers != 3 {
		t.Errorf("a name nobody sets was looked for in %d layers, want all 3", layers)
	}
	// The layers above are untouched: every directory shares them.
	if v, ok, _ := one.get("b"); v != "1" || !ok {
		t.Error("a layer in front changed the one behind it")
	}

	// What a file leaves below it: its assignments, less the names it
	// assigned twice differently.
	b := readGradleBuild("ext.a = '1'\next.c = '1'\next.c = '2'\n", one)
	own, gone := b.result()
	if !reflect.DeepEqual(own, map[string]string{"a": "1"}) || !reflect.DeepEqual(gone, map[string]bool{"c": true}) {
		t.Errorf("result = %v, %v", own, gone)
	}
}

// The lists a build shares with the projects below it are linked, not
// copied, and read out in the order the copies had.
func TestGradleListKeepsItsOrder(t *testing.T) {
	var l *gradleList[string]
	if l.len() != 0 || l.flat() != nil {
		t.Error("an empty list is not empty")
	}
	root := l.with(nil, []string{"settings"})                   // a settings file's: after
	sub := root.with([]string{"near1", "near2"}, nil)           // a build file above: in front
	deeper := sub.with([]string{"nearest"}, []string{"deeper"}) // both
	if got, want := deeper.flat(), []string{"nearest", "near1", "near2", "settings", "deeper"}; !reflect.DeepEqual(got, want) || deeper.len() != 5 {
		t.Errorf("flat = %v (len %d), want %v", got, deeper.len(), want)
	}
	if sub.with(nil, nil) != sub {
		t.Error("adding nothing made a link")
	}
	if got := root.flat(); !reflect.DeepEqual(got, []string{"settings"}) {
		t.Errorf("a link below changed the list above: %v", got)
	}
}

// A script that many projects apply is read once for all of them, when what
// it takes from outside is the same for each.
func TestAScriptAppliedByManyProjectsIsReadOnce(t *testing.T) {
	const projects = 60
	files := map[string]string{
		"gradle.properties":      "base=1\n",
		"gradle/versions.gradle": "ext.v = \"${base}.5\"\next.w = '2.0'\n",
	}
	for i := 0; i < projects; i++ {
		// Each project has a variable of its own that the script does not
		// look at: their scopes differ, and it does not matter.
		files[fmt.Sprintf("p%d/gradle.properties", i)] = fmt.Sprintf("mine=%d\n", i)
		files[fmt.Sprintf("p%d/build.gradle", i)] = "apply from: \"$rootDir/gradle/versions.gradle\"\ndependencies { implementation \"g:a:$v\" }\n"
	}
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	for i := 0; i < projects; i++ {
		got := w.Read(filepath.Join(root, fmt.Sprintf("p%d/build.gradle", i)), Options{})
		if got.Failure != nil || len(got.Deps) != 1 || got.Deps[0].Version != "1.5" {
			t.Fatalf("project %d: %+v, failure %v", i, got.Deps, got.Failure)
		}
	}
	// Each build file is read twice (for what it applies, then for what it
	// declares) and the script once.
	if want := 2*projects + 1; w.gradle.fullReads != want {
		t.Errorf("%d readings for %d projects and one script; want %d: the script is read again for each project", w.gradle.fullReads, projects, want)
	}
	// A project whose scope answers differently for a name the script
	// looks up gets its own reading, and its own answer.
	files["other/gradle.properties"] = "base=7\n"
	files["other/build.gradle"] = files["p0/build.gradle"]
	root2 := writeTree(t, files)
	w2 := NewWalk(root2, nil)
	w2.Read(filepath.Join(root2, "p0", "build.gradle"), Options{})
	got := w2.Read(filepath.Join(root2, "other", "build.gradle"), Options{})
	if got.Failure != nil || len(got.Deps) != 1 || got.Deps[0].Version != "7.5" {
		t.Errorf("a project with another value for what the script reads: %+v, failure %v; want 7.5", got.Deps, got.Failure)
	}
}

// A pom's reasons do not depend on what was read before it (round 3): the
// effective model of a pom was cached with the reasons of every question
// ever put to it, so a pom read after another that imports it took that
// other's.
func TestAPomsReasonsDoNotDependOnReadOrder(t *testing.T) {
	files := map[string]string{
		// It manages g:pinned itself (at a version that cannot be worked
		// out) and imports a BOM that no repository has.
		"bom/pom.xml": `<project><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>pinned</artifactId><version>${undefined.version}</version></dependency>
<dependency><groupId>ext</groupId><artifactId>missing-bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>pinned</artifactId></dependency>
<dependency><groupId>g</groupId><artifactId>other</artifactId></dependency></dependencies></project>`,
		"app/pom.xml": `<project><groupId>g</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>
<dependency><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>elsewhere</artifactId></dependency></dependencies></project>`,
	}
	reasons := func(order ...string) map[string]string {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(&declaredRecorder{})
		out := map[string]string{}
		// What a question could not obtain is the question's. Nothing of
		// it may be left on a pom's effective model, which every later
		// file shares: both chains here are whole, and stay so.
		defer func() {
			for _, c := range w.pom.contexts {
				if c.chainGap != "" {
					t.Errorf("after reading %v, the effective model of %s carries the reason %q", order, c.chain[0].path, c.chainGap)
				}
			}
		}()
		for _, rel := range order {
			got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			if rel != "bom/pom.xml" {
				continue
			}
			for _, d := range got.Deps {
				if !d.ManagedEntry {
					out[d.Name] = d.VersionReason
				}
			}
		}
		return out
	}
	alone, after := reasons("bom/pom.xml"), reasons("app/pom.xml", "bom/pom.xml")
	// g:pinned is answered by the pom's own entry before any import is
	// looked at: no pom that might have answered was missing. g:other is
	// looked for in the missing BOM.
	want := map[string]string{"g:pinned": "", "g:other": model.VersionReasonInheritedNotFound}
	if !reflect.DeepEqual(alone, want) || !reflect.DeepEqual(after, want) {
		t.Errorf("read alone %v; read after the pom that imports it %v; want %v both times", alone, after, want)
	}
}

// A context holds its chain and nothing that grows with what the chain
// manages; a pom's entries are indexed once, on the pom.
func TestAPomsEntriesAreIndexedOnceAndShared(t *testing.T) {
	var managed strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&managed, "<dependency><groupId>org.x</groupId><artifactId>a%d</artifactId><version>1.%d</version></dependency>", i, i)
	}
	// One entry whose group is a property the CHILD sets: what it manages
	// depends on the model it is read through. It is written before the
	// plain entry for the same coordinate, so it wins there.
	files := map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><properties><grp>org.none</grp></properties><dependencyManagement><dependencies>
<dependency><groupId>${grp}</groupId><artifactId>a7</artifactId><version>9.9</version></dependency>` + managed.String() + `</dependencies></dependencyManagement></project>`}
	child := func(name, props string) string {
		return `<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>` + name + `</artifactId>` + props + `<dependencies><dependency><groupId>org.x</groupId><artifactId>a7</artifactId></dependency></dependencies></project>`
	}
	files["plain/pom.xml"] = child("plain", "")
	files["override/pom.xml"] = child("override", "<properties><grp>org.x</grp></properties>")
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	version := func(rel string) string {
		for _, d := range w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{}).Deps {
			if d.Name == "org.x:a7" && !d.ManagedEntry {
				return d.Version
			}
		}
		return "(absent)"
	}
	if got := version("plain/pom.xml"); got != "1.7" {
		t.Errorf("a child that leaves the property alone: org.x:a7 at %q, want 1.7", got)
	}
	if got := version("override/pom.xml"); got != "9.9" {
		t.Errorf("a child that sets the property: org.x:a7 at %q, want 9.9 (the entry written first, which now names it)", got)
	}
	parent := w.pom.model(filepath.Join(root, "pom.xml"))
	idx := parent.idx
	if idx == nil || len(idx.plain) != 500 || len(idx.dynamic) != 1 {
		t.Fatalf("the parent's index: %+v", idx)
	}
	for _, rel := range []string{"plain/pom.xml", "override/pom.xml"} {
		c := w.pom.context(w.pom.model(filepath.Join(root, filepath.FromSlash(rel))))
		if len(c.chain) != 2 || c.chain[1] != parent || c.chain[1].idx != idx {
			t.Errorf("%s: its context does not share the parent's model and index", rel)
		}
	}
	// The struct itself: a field that could hold a copy of what the chain
	// says (a map) would bring the product back.
	typ := reflect.TypeOf(pomContext{})
	for i := 0; i < typ.NumField(); i++ {
		if k := typ.Field(i).Type.Kind(); k == reflect.Map {
			t.Errorf("pomContext.%s is a map: an effective model holds its chain, not a copy of what the chain says", typ.Field(i).Name)
		}
	}
}

// A coordinate is not built past what a coordinate can be (round 3: the
// cut could be deleted with the suite green).
func TestACoordinateIsNotBuiltPastItsLength(t *testing.T) {
	long := "dependencies {\n implementation 'g:" + strings.Repeat("a", 100) + "'" + strings.Repeat(" + '"+strings.Repeat("b", 100)+"'", 4*gradleInterpolatedLimit/100+10) + " + ':1'\n implementation 'c:d:1'\n}\n"
	root := writeTree(t, map[string]string{"build.gradle": long})
	got := NewWalk(root, nil).Read(filepath.Join(root, "build.gradle"), Options{})
	if got.Failure != nil {
		t.Fatalf("failure %v", got.Failure)
	}
	for _, d := range got.Deps {
		if len(d.Name)+len(d.Version)+len(d.Requirement) > 4*gradleInterpolatedLimit {
			t.Errorf("a dependency of %d bytes was built from a concatenation", len(d.Name)+len(d.Version)+len(d.Requirement))
		}
	}
	if len(got.Deps) != 1 || got.Deps[0].Name != "c:d" {
		t.Errorf("dependencies = %d; want the ordinary one beside it, and nothing from the long one", len(got.Deps))
	}
}
