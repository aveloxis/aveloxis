// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 8 (R8-2, R8-3, R8-6, R8-7, R8-9): what the pom tree answered
// for poms it could have known more about, and for ones it knew less about
// than it took itself to.

// r8Counting answers "not found" for every coordinate and counts the asks.
type r8Counting struct{ asked []string }

func (s *r8Counting) RemotePom(g, a, v string, _ []string) (*RemotePom, RemoteOutcome) {
	s.asked = append(s.asked, g+":"+a+":"+v)
	return nil, RemoteNotFound
}

func r8Module(parentGAV [3]string, relativePath, management, deps string) string {
	rel := ""
	switch relativePath {
	case "-": // no element: Maven's default, ../pom.xml
	case "":
		rel = "<relativePath/>"
	default:
		rel = "<relativePath>" + relativePath + "</relativePath>"
	}
	return "<project><parent><groupId>" + parentGAV[0] + "</groupId><artifactId>" + parentGAV[1] + "</artifactId><version>" + parentGAV[2] + "</version>" + rel + "</parent>" +
		"<artifactId>m</artifactId><dependencyManagement><dependencies>" + management + "</dependencies></dependencyManagement><dependencies>" + deps + "</dependencies></project>"
}

// A parent that is in the build is the parent, wherever it stands: Maven
// finds a module's parent among the poms of the build by its coordinates
// when relativePath does not lead to it (R8-2). It was looked for at
// relativePath alone and otherwise asked of a repository, which answered
// "not found" (a release) or was never asked (a snapshot).
func TestAParentInTheBuildIsFoundByItsCoordinates(t *testing.T) {
	for _, version := range []string{"1", "1-SNAPSHOT"} {
		for _, rel := range []string{"-", ""} {
			name := fmt.Sprintf("version %s, relativePath %q", version, rel)
			files := map[string]string{
				"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>" + version + "</version><modules><module>svc/m</module></modules>" +
					"<dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement></project>",
				"svc/m/pom.xml":  r8Module([3]string{"t", "root", version}, rel, r4import("t", "second", "1"), r4dep("g", "x", "")+r4dep("g", "y", "")),
				"second/pom.xml": r6Bom("second", "g", "x", "2.0", "g", "y", "2.0"),
			}
			src := &r8Counting{}
			got := r6Versions(t, files, "svc/m/pom.xml", src)
			if x := got["g:x"]; x.Version != "1.0" || x.VersionReason != "" {
				t.Errorf("%s: g:x at %q (%s); the parent of the build manages it at 1.0", name, x.Version, x.VersionReason)
			}
			if y := got["g:y"]; y.Version != "2.0" || y.VersionReason != "" {
				t.Errorf("%s: g:y at %q (%s); the parent is in hand and says nothing of it, so the BOM's 2.0 stands", name, y.Version, y.VersionReason)
			}
			if len(src.asked) != 0 {
				t.Errorf("%s: a repository was asked for %v; the parent is a pom of the clone", name, src.asked)
			}
		}
	}
}

// Two poms of the clone under the parent's coordinates: neither is chosen,
// no repository is asked (its pom would be a third candidate), and what the
// parent says is not known.
func TestAParentTwoPomsOfTheCloneCouldBeIsNotKnown(t *testing.T) {
	root := "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement></project>"
	files := map[string]string{
		"a/pom.xml":      root,
		"b/pom.xml":      root,
		"svc/m/pom.xml":  r8Module([3]string{"t", "root", "1"}, "-", r4import("t", "second", "1"), r4dep("g", "y", "")),
		"second/pom.xml": r6Bom("second", "g", "y", "2.0"),
	}
	src := &r8Counting{}
	y := r6Versions(t, files, "svc/m/pom.xml", src)["g:y"]
	if y.Version != "" || y.VersionReason != model.VersionReasonInheritedNotFound {
		t.Errorf("g:y at %q (%s); want no version and inherited_not_found: the parent is one of two poms and neither was read as it", y.Version, y.VersionReason)
	}
	if len(src.asked) != 0 {
		t.Errorf("a repository was asked for %v", src.asked)
	}
}

// The parent's own coordinates may come from properties its own chain sets
// (CI-friendly versions): such a pom is still indexed under what they mean.
func TestAParentInTheBuildIsFoundWhenItsVersionIsAProperty(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>${revision}</version><properties><revision>3.1</revision></properties>" +
			"<dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement></project>",
		"deep/svc/m/pom.xml": r8Module([3]string{"t", "root", "3.1"}, "-", "", r4dep("g", "x", "")),
	}
	if x := r6Versions(t, files, "deep/svc/m/pom.xml", &r8Counting{})["g:x"]; x.Version != "1.0" {
		t.Errorf("g:x at %q (%s), want 1.0 from the parent found by its coordinates", x.Version, x.VersionReason)
	}
}

// A pom of the clone that is there and cannot be read has said nothing that
// is known (R8-3). It was treated as absent, the repositories then answered
// "not found", and a later BOM's version was stored.
func TestAPomOfTheCloneThatCannotBeReadIsNotKnown(t *testing.T) {
	big := "<!--" + strings.Repeat("x", int(MaxBuildFileBytes)+1) + "-->"
	manages := "<dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement>"
	for name, parent := range map[string]string{
		"not XML":            "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><oops></project>",
		"larger than a pom":  "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version>" + big + manages + "</project>",
		"an encoding nobody": `<?xml version="1.0" encoding="x-nobody-has-this"?><project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><name>Caf` + "\xe9" + `</name>` + manages + "</project>",
	} {
		files := map[string]string{
			"app/pom.xml": "<project><parent><groupId>t</groupId><artifactId>p</artifactId><version>1</version><relativePath>../p</relativePath></parent><artifactId>app</artifactId>" +
				"<dependencyManagement><dependencies>" + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"p/pom.xml":      parent,
			"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
		}
		src := &r8Counting{}
		x := r6Versions(t, files, "app/pom.xml", src)["g:x"]
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
			t.Errorf("parent %s: g:x at %q (%s); want no version and inherited_lookup_failed", name, x.Version, x.VersionReason)
		}
		if len(src.asked) != 0 {
			t.Errorf("parent %s: a repository was asked for %v; the parent is the file at relativePath", name, src.asked)
		}
	}
	// By coordinates: an import that no pom READ answers to, in a clone that
	// holds a pom which could not be read and which, by what can be made out
	// of it, is that import. A repository is still asked (its pom would be
	// taken); its "not found" is not an answer while the clone's own may be
	// the one.
	for name, first := range map[string]string{
		"not XML":           "<project><groupId>t</groupId><artifactId>first</artifactId><version>1</version><oops></project>",
		"larger than a pom": "<project><groupId>t</groupId><artifactId>first</artifactId><version>1</version>" + big + manages + "</project>",
	} {
		files := map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
				r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"first/pom.xml":  first,
			"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
		}
		x := r6Versions(t, files, "pom.xml", &r8Counting{})["g:x"]
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
			t.Errorf("first BOM %s: g:x at %q (%s); want no version and inherited_lookup_failed", name, x.Version, x.VersionReason)
		}
	}
	// A pom that cannot be read and is, by what can be made out of it,
	// ANOTHER artifact puts nothing in doubt.
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"fixture/pom.xml": "<project><groupId>t</groupId><artifactId>somethingelse</artifactId><version>1</version><oops></project>",
		"second/pom.xml":  r6Bom("second", "g", "x", "2.0"),
	}
	if x := r6Versions(t, files, "pom.xml", &r8Counting{})["g:x"]; x.Version != "2.0" {
		t.Errorf("an unreadable pom of another artifact: g:x at %q (%s), want the second BOM's 2.0 (the first is on no repository)", x.Version, x.VersionReason)
	}
}

// What Maven decodes is decoded: the encoding a pom declares, and an XML 1.1
// prolog (Go's decoder refuses both; the poms were treated as absent).
func TestAPomIsReadInTheEncodingItDeclares(t *testing.T) {
	body := func(name string) string {
		return "<project><groupId>t</groupId><artifactId>p</artifactId><version>1</version><name>" + name + "</name><dependencyManagement><dependencies>" +
			r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement></project>"
	}
	utf16 := func(s string, bigEndian bool) string {
		out := []byte{0xFF, 0xFE}
		if bigEndian {
			out = []byte{0xFE, 0xFF}
		}
		for _, r := range s {
			if bigEndian {
				out = append(out, byte(r>>8), byte(r))
			} else {
				out = append(out, byte(r), byte(r>>8))
			}
		}
		return string(out)
	}
	for name, parent := range map[string]string{
		"ISO-8859-1":                           `<?xml version="1.0" encoding="ISO-8859-1"?>` + body("Caf\xe9"),
		"iso-8859-1, lower":                    `<?xml version='1.0' encoding='iso-8859-1'?>` + body("Caf\xe9"),
		"latin1":                               `<?xml version="1.0" encoding="latin1"?>` + body("Caf\xe9"),
		"windows-1252":                         `<?xml version="1.0" encoding="windows-1252"?>` + body("\x93quoted\x94 \x80"),
		"Cp1252":                               `<?xml version="1.0" encoding="Cp1252"?>` + body("\x93quoted\x94"),
		"US-ASCII":                             `<?xml version="1.0" encoding="US-ASCII"?>` + body("plain"),
		"a label nobody has, over plain ASCII": `<?xml version="1.0" encoding="x-nobody-has-this"?>` + body("plain"),
		"UTF-16 little endian":                 utf16(`<?xml version="1.0" encoding="UTF-16"?>`+body("Café"), false),
		"UTF-16 big endian":                    utf16(`<?xml version="1.0" encoding="UTF-16"?>`+body("Café"), true),
		"XML 1.1":                              `<?xml version="1.1" encoding="UTF-8"?>` + body("Café"),
		"XML 1.1, single quote":                `<?xml version='1.1'?>` + body("plain"),
	} {
		files := map[string]string{
			"app/pom.xml": "<project><parent><groupId>t</groupId><artifactId>p</artifactId><version>1</version><relativePath>../p</relativePath></parent><artifactId>app</artifactId>" +
				"<dependencyManagement><dependencies>" + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"p/pom.xml":      parent,
			"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
		}
		if x := r6Versions(t, files, "app/pom.xml", &r8Counting{})["g:x"]; x.Version != "1.0" {
			t.Errorf("parent in %s: g:x at %q (%s), want the parent's 1.0", name, x.Version, x.VersionReason)
		}
	}
	// The pom's own reader too (the file read alone).
	files := map[string]string{"pom.xml": `<?xml version="1.0" encoding="ISO-8859-1"?><project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><name>Caf` + "\xe9" + `</name><dependencies>` + r4dep("g", "x", "3.3") + "</dependencies></project>"}
	if x := r6Versions(t, files, "pom.xml", nil)["g:x"]; x.Version != "3.3" {
		t.Errorf("a pom in ISO-8859-1 read by itself: g:x at %q, want 3.3", x.Version)
	}
}

// r8ParentChain is the app and n-1 parents in the clone; the last manages
// g:x at 1.0, and the app imports a BOM that manages it at 2.0.
func r8ParentChain(n int) map[string]string {
	files := map[string]string{"second/pom.xml": r6Bom("second", "g", "x", "2.0")}
	for i := 1; i < n; i++ {
		parent, management := "", ""
		if i < n-1 {
			parent = fmt.Sprintf("<parent><groupId>t</groupId><artifactId>c%d</artifactId><version>1</version><relativePath>../c%d</relativePath></parent>", i+1, i+1)
		} else {
			management = "<dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + "</dependencies></dependencyManagement>"
		}
		files[fmt.Sprintf("c%d/pom.xml", i)] = "<project>" + parent + fmt.Sprintf("<groupId>t</groupId><artifactId>c%d</artifactId><version>1</version>", i) + management + "</project>"
	}
	files["pom.xml"] = "<project><parent><groupId>t</groupId><artifactId>c1</artifactId><version>1</version><relativePath>c1</relativePath></parent><artifactId>app</artifactId>" +
		"<dependencyManagement><dependencies>" + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>"
	return files
}

// A chain of parents that was not followed to its end (it is longer than
// pomChainLimit, or it is a cycle) is a chain whose far end is not known:
// what a BOM would give behind it is withheld (R8-6). The BOM's version was
// stored.
func TestBehindAChainThatWasNotReadToItsEndNothingIsTaken(t *testing.T) {
	for _, n := range []int{pomChainLimit - 1, pomChainLimit} {
		if x := r6Versions(t, r8ParentChain(n), "pom.xml", &r8Counting{})["g:x"]; x.Version != "1.0" {
			t.Errorf("a chain of %d poms read to its end: g:x at %q (%s), want the last pom's 1.0", n, x.Version, x.VersionReason)
		}
	}
	for _, n := range []int{pomChainLimit + 1, pomChainLimit + 2} {
		x := r6Versions(t, r8ParentChain(n), "pom.xml", &r8Counting{})["g:x"]
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedNotFound {
			t.Errorf("a chain of %d poms, longer than is followed: g:x at %q (%s); want no version and inherited_not_found", n, x.Version, x.VersionReason)
		}
	}
	// A cycle of two, and a pom that is its own parent.
	cycle := map[string]string{
		"a/pom.xml": "<project><parent><groupId>t</groupId><artifactId>b</artifactId><version>1</version><relativePath>../b</relativePath></parent><groupId>t</groupId><artifactId>a</artifactId><version>1</version>" +
			"<dependencyManagement><dependencies>" + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"b/pom.xml":      "<project><parent><groupId>t</groupId><artifactId>a</artifactId><version>1</version><relativePath>../a</relativePath></parent><groupId>t</groupId><artifactId>b</artifactId><version>1</version></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	if x := r6Versions(t, cycle, "a/pom.xml", &r8Counting{})["g:x"]; x.Version != "" || x.VersionReason == "" {
		t.Errorf("a cycle of parents: g:x at %q (%s); Maven refuses such a build, and no version is taken from behind it", x.Version, x.VersionReason)
	}
	self := map[string]string{
		"pom.xml": "<project><parent><groupId>t</groupId><artifactId>app</artifactId><version>1</version><relativePath>.</relativePath></parent><groupId>t</groupId><artifactId>app</artifactId><version>1</version>" +
			"<dependencyManagement><dependencies>" + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	if x := r6Versions(t, self, "pom.xml", &r8Counting{})["g:x"]; x.Version != "" || x.VersionReason == "" {
		t.Errorf("a pom that is its own parent: g:x at %q (%s)", x.Version, x.VersionReason)
	}
}

// r8ImportChain: the app imports d0, which imports d1, ... to depth BOMs;
// the deepest manages g:x at 1.0; then the app imports a BOM with 2.0.
func r8ImportChain(depth int) map[string]string {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "d0", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	for i := 0; i < depth; i++ {
		entry := r4import("t", fmt.Sprintf("d%d", i+1), "1")
		if i == depth-1 {
			entry = r4dep("g", "x", "1.0")
		}
		files[fmt.Sprintf("d%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>t</groupId><artifactId>d%d</artifactId><version>1</version><dependencyManagement><dependencies>", i) + entry + "</dependencies></dependencyManagement></project>"
	}
	return files
}

// BOMs that import BOMs are followed pomChainLimit deep. Deeper is not
// followed, which is not known to say nothing: a later BOM's version is
// withheld, and the tree is not whole, at the SAME depth (R8-7: the lookup
// passed the cut silently, and settle counted one level more, so at exactly
// one depth the later version was stored in a tree reported whole).
func TestImportsNestedDeeperThanIsFollowedAreNotKnown(t *testing.T) {
	read := func(depth int) (model.LibyearDep, bool) {
		root := writeTree(t, r8ImportChain(depth))
		w := NewWalk(root, nil)
		w.SetPomSource(&r8Counting{})
		got := w.Read(root+"/pom.xml", Options{})
		var x model.LibyearDep
		for i := range got.Deps {
			got.Deps[i].ManifestPath = "pom.xml"
			if got.Deps[i].Name == "g:x" && !got.Deps[i].ManagedEntry {
				x = got.Deps[i]
			}
		}
		return x, len(w.SettleMavenManaged(got.Deps).NotWhole) == 0
	}
	for _, depth := range []int{1, 5, pomChainLimit - 1, pomChainLimit} {
		if x, whole := read(depth); x.Version != "1.0" || !whole {
			t.Errorf("imports nested %d deep: g:x at %q (%s), whole %v; want the deepest BOM's 1.0 in a whole tree", depth, x.Version, x.VersionReason, whole)
		}
	}
	for _, depth := range []int{pomChainLimit + 1, pomChainLimit + 2, pomChainLimit + 9} {
		x, whole := read(depth)
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedNotFound || whole {
			t.Errorf("imports nested %d deep: g:x at %q (%s), whole %v; want no version, inherited_not_found, and a tree that is not whole", depth, x.Version, x.VersionReason, whole)
		}
	}
}

// Profiles are not applied, and what a profile manages is therefore not
// known to be what the build takes (R8-9): the version the pom, its parents
// or its BOMs would give for THAT artifact is withheld; other artifacts are
// answered as before.
func TestWhatAProfileManagesIsNotAnsweredForWithoutIt(t *testing.T) {
	profile := func(inner string) string {
		return "<profiles><profile><id>p</id><activation><activeByDefault>true</activeByDefault></activation>" + inner + "</profile></profiles>"
	}
	pom := func(profiles string) map[string]string {
		return map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
				r4dep("g", "own", "5.0") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
				r4dep("g", "x", "") + r4dep("g", "z", "") + r4dep("g", "own", "") + "</dependencies>" + profiles + "</project>",
			"second/pom.xml": r6Bom("second", "g", "x", "2.0", "g", "z", "3.0"),
		}
	}
	got := r6Versions(t, pom(profile("<dependencyManagement><dependencies>"+r4dep("g", "x", "9")+r4dep("g", "own", "6.0")+"</dependencies></dependencyManagement>")), "pom.xml", &r8Counting{})
	for _, name := range []string{"g:x", "g:own"} {
		if d := got[name]; d.Version != "" || d.VersionReason != model.VersionReasonProfileManaged {
			t.Errorf("%s, which a profile manages: at %q (%s); want no version and profile_managed", name, d.Version, d.VersionReason)
		}
	}
	if z := got["g:z"]; z.Version != "3.0" || z.VersionReason != "" {
		t.Errorf("g:z, which no profile names: at %q (%s), want the BOM's 3.0", z.Version, z.VersionReason)
	}
	// A profile that DECLARES the dependency with a version: the profile's
	// declaration is a row of its own (at 9); the pom's own, with no
	// version, is not given the BOM's.
	root := writeTree(t, pom(profile("<dependencies>"+r4dep("g", "x", "9")+"</dependencies>")))
	w := NewWalk(root, nil)
	w.SetPomSource(&r8Counting{})
	var xs []string
	for _, d := range w.Read(root+"/pom.xml", Options{}).Deps {
		switch d.Name {
		case "g:x":
			xs = append(xs, d.Version+"("+d.VersionReason+")")
		case "g:z":
			if d.Version != "3.0" {
				t.Errorf("g:z beside a profile's dependency: at %q (%s)", d.Version, d.VersionReason)
			}
		}
	}
	if len(xs) != 2 || xs[0] != "("+model.VersionReasonProfileManaged+")" || xs[1] != "9()" {
		t.Errorf("g:x, declared with no version and by a profile at 9: rows %v; want the first with no version and profile_managed, the second at 9", xs)
	}
	// A profile that imports a BOM could manage anything: nothing managed is
	// taken, the pom's own entry included.
	got = r6Versions(t, pom(profile("<dependencyManagement><dependencies>"+r4import("t", "other", "1")+"</dependencies></dependencyManagement>")), "pom.xml", &r8Counting{})
	for _, name := range []string{"g:x", "g:z", "g:own"} {
		if d := got[name]; d.Version != "" || d.VersionReason != model.VersionReasonProfileManaged {
			t.Errorf("%s beside a profile that imports a BOM: at %q (%s)", name, d.Version, d.VersionReason)
		}
	}
	// A profile in a PARENT does not put the child's own entry in doubt (the
	// child's entry outranks everything of its parent), and does the
	// parent's.
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies>" + r4dep("g", "x", "1.0") + r4dep("g", "y", "1.0") + "</dependencies></dependencyManagement>" +
			profile("<dependencyManagement><dependencies>"+r4dep("g", "x", "9")+r4dep("g", "y", "9")+"</dependencies></dependencyManagement>") + "</project>",
		"m/pom.xml": "<project><parent><groupId>t</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m</artifactId><dependencyManagement><dependencies>" +
			r4dep("g", "x", "4.0") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>",
	}
	got = r6Versions(t, files, "m/pom.xml", &r8Counting{})
	if x := got["g:x"]; x.Version != "4.0" {
		t.Errorf("g:x, managed by the module itself above a parent's profile: at %q (%s), want 4.0", x.Version, x.VersionReason)
	}
	if y := got["g:y"]; y.Version != "" || y.VersionReason != model.VersionReasonProfileManaged {
		t.Errorf("g:y, managed by the parent and by the parent's profile: at %q (%s)", y.Version, y.VersionReason)
	}
}

// "Not found" is an answer only when every repository the build names
// answered (R8-4). A build that declares a repository which is never asked
// (http, credentials in the address, an address that cannot be read) has a
// place the pom may be that nobody looked in: what a later BOM would give is
// withheld. A plugin repository serves no parent or BOM and changes nothing.
func TestNotFoundIsNoAnswerWhileADeclaredRepositoryWasNotAsked(t *testing.T) {
	pom := func(repositories string) map[string]string {
		return map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version>" + repositories +
				"<dependencyManagement><dependencies>" + r4import("corp", "corp-bom", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement>" +
				"<dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
		}
	}
	for name, repositories := range map[string]string{
		"http":        "<repositories><repository><id>corp</id><url>http://nexus.corp.example/repo</url></repository></repositories>",
		"credentials": "<repositories><repository><id>corp</id><url>https://user:secret@nexus.corp.example/repo</url></repository></repositories>",
		"a property":  "<repositories><repository><id>corp</id><url>${corp.url}</url></repository></repositories>",
	} {
		x := r6Versions(t, pom(repositories), "pom.xml", &r8Counting{})["g:x"]
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedNotFound {
			t.Errorf("a repository declared with %s, never asked: g:x at %q (%s); want no version and inherited_not_found", name, x.Version, x.VersionReason)
		}
	}
	for name, repositories := range map[string]string{
		"no repository":          "",
		"an https one, asked":    "<repositories><repository><id>corp</id><url>https://nexus.corp.example/repo</url></repository></repositories>",
		"a plugin repository":    "<pluginRepositories><pluginRepository><id>corp</id><url>http://nexus.corp.example/plugins</url></pluginRepository></pluginRepositories>",
		"an empty repository id": "<repositories><repository><id>corp</id><url></url></repository></repositories>",
	} {
		if x := r6Versions(t, pom(repositories), "pom.xml", &r8Counting{})["g:x"]; x.Version != "2.0" {
			t.Errorf("%s: g:x at %q (%s), want the second BOM's 2.0: every repository the build names answered", name, x.Version, x.VersionReason)
		}
	}
	// A parent's declaration counts for the child that inherits it.
	files := pom("")
	files["pom.xml"] = strings.Replace(files["pom.xml"], "<groupId>t</groupId><artifactId>app</artifactId><version>1</version>",
		"<parent><groupId>t</groupId><artifactId>up</artifactId><version>1</version><relativePath>up</relativePath></parent><artifactId>app</artifactId>", 1)
	files["up/pom.xml"] = "<project><groupId>t</groupId><artifactId>up</artifactId><version>1</version><repositories><repository><url>http://nexus.corp.example/repo</url></repository></repositories></project>"
	if x := r6Versions(t, files, "pom.xml", &r8Counting{})["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedNotFound {
		t.Errorf("a parent declares the repository that is never asked: g:x at %q (%s)", x.Version, x.VersionReason)
	}
}

// BOMs that import each other in a cycle are a build Maven refuses (as this
// reader understands its model builder: UNSURE, see the reference's M5), so
// what lies behind the cycle is not known. A BOM reached by two routes is
// no cycle, and answers. Until 0.29.215 a cycle was passed in silence and a
// later BOM's version was stored.
func TestBehindBOMsThatImportEachOtherNothingIsTaken(t *testing.T) {
	bom := func(artifact string, entries ...string) string {
		return "<project><groupId>t</groupId><artifactId>" + artifact + "</artifactId><version>1</version><dependencyManagement><dependencies>" +
			strings.Join(entries, "") + "</dependencies></dependencyManagement></project>"
	}
	app := func(imports ...string) string {
		s := "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>"
		for _, i := range imports {
			s += r4import("t", i, "1")
		}
		return s + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>"
	}
	second := bom("second", r4dep("g", "x", "2.0"))
	for name, c := range map[string]struct {
		files map[string]string
		want  string
		why   string
	}{
		"a BOM that imports itself, then one that manages it": {map[string]string{"pom.xml": app("self", "second"),
			"self/pom.xml": bom("self", r4import("t", "self", "1")), "second/pom.xml": second}, "", model.VersionReasonInheritedNotFound},
		"two BOMs that import each other, then one that manages it": {map[string]string{"pom.xml": app("a", "second"),
			"a/pom.xml": bom("a", r4import("t", "b", "1")), "b/pom.xml": bom("b", r4import("t", "a", "1")), "second/pom.xml": second}, "", model.VersionReasonInheritedNotFound},
		"the one that manages it first, then the cycle": {map[string]string{"pom.xml": app("second", "a"),
			"a/pom.xml": bom("a", r4import("t", "b", "1")), "b/pom.xml": bom("b", r4import("t", "a", "1")), "second/pom.xml": second}, "2.0", ""},
		"a cycle whose own entry manages it": {map[string]string{"pom.xml": app("a", "second"),
			"a/pom.xml": bom("a", r4dep("g", "x", "1.0"), r4import("t", "a", "1")), "second/pom.xml": second}, "1.0", ""},
		"one BOM reached by two routes is no cycle": {map[string]string{"pom.xml": app("a", "b", "second"),
			"a/pom.xml": bom("a", r4import("t", "shared", "1")), "b/pom.xml": bom("b", r4import("t", "shared", "1")),
			"shared/pom.xml": bom("shared"), "second/pom.xml": second}, "2.0", ""},
	} {
		root := writeTree(t, c.files)
		w := NewWalk(root, nil)
		w.SetPomSource(outcomeSource{outcome: RemoteNotFound})
		got := w.Read(root+"/pom.xml", Options{})
		var x model.LibyearDep
		for i := range got.Deps {
			got.Deps[i].ManifestPath = "pom.xml"
			if got.Deps[i].Name == "g:x" && !got.Deps[i].ManagedEntry {
				x = got.Deps[i]
			}
		}
		if x.Version != c.want || x.VersionReason != c.why {
			t.Errorf("%s: g:x at %q (%s), want %q (%s)", name, x.Version, x.VersionReason, c.want, c.why)
		}
		if whole := len(w.SettleMavenManaged(got.Deps).NotWhole) == 0; whole != (strings.HasPrefix(name, "one BOM") || !(strings.Contains(name, "cycle") || strings.Contains(name, "import"))) {
			t.Errorf("%s: the tree is whole: %v; a tree with a cycle of imports is not", name, whole)
		}
	}
}

// LATEST and RELEASE are Maven's names for "whichever version is newest":
// like a range, neither names one pom, and none is asked for. A BOM imported
// at one of them is not known, and nothing is taken from behind it.
func TestAnImportAtLATESTOrRELEASEIsNotKnown(t *testing.T) {
	for _, version := range []string{"LATEST", "RELEASE"} {
		files := map[string]string{
			"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
				r4import("t", "first", version) + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
		}
		src := &r8Counting{}
		x := r6Versions(t, files, "pom.xml", src)["g:x"]
		if x.Version != "" || x.VersionReason != model.VersionReasonInheritedNotFound {
			t.Errorf("behind an import at %s: g:x at %q (%s); want no version and inherited_not_found", version, x.Version, x.VersionReason)
		}
		if slices.Contains(src.asked, "t:first:"+version) {
			t.Errorf("t:first:%s was asked of a repository (%v); it names no pom", version, src.asked)
		}
	}
}
