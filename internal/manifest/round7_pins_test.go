// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"strings"
	"testing"
)

// Review round 7 (R7-11): bounds that no test held. Each was changed by a
// mutant (32 to 4096, 10 to 1000, a guard removed) and the suite passed.

// r7Chain is a chain of n poms in the clone, c1 the nearest: cK's parent is
// c(K+1). setAt (1-based) is the pom that sets the property v.
func r7Chain(n, setAt int) map[string]string {
	files := map[string]string{}
	for i := 1; i <= n; i++ {
		parent, props := "", ""
		if i < n {
			parent = fmt.Sprintf("<parent><groupId>t</groupId><artifactId>c%d</artifactId><version>1</version><relativePath>../c%d</relativePath></parent>", i+1, i+1)
		}
		if i == setAt {
			props = "<properties><v>4.2</v></properties>"
		}
		files[fmt.Sprintf("c%d/pom.xml", i)] = "<project>" + parent + fmt.Sprintf("<groupId>t</groupId><artifactId>c%d</artifactId><version>1</version>", i) + props + "</project>"
	}
	files["pom.xml"] = "<project><parent><groupId>t</groupId><artifactId>c1</artifactId><version>1</version><relativePath>c1</relativePath></parent>" +
		"<artifactId>app</artifactId><dependencyManagement><dependencies>" + r4dep("g", "kept", "1.0") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "${v}") + "</dependencies></project>"
	return files
}

// A chain of parents is followed for pomChainLimit poms, the pom itself
// counted, and no further: what the last of them sets is read, what the
// next one sets is not, and a chain that is longer is not whole.
func TestAChainOfParentsIsFollowedToItsLimitAndNoFurther(t *testing.T) {
	// The pom is the first of the chain, so c(pomChainLimit-1) is the last.
	last := pomChainLimit - 1
	if x := r6Versions(t, r7Chain(last, last), "pom.xml", nil)["g:x"]; x.Version != "4.2" {
		t.Errorf("a property set by the last pom of a chain of exactly pomChainLimit (%d): g:x at %q, want 4.2", pomChainLimit, x.Version)
	}
	if x := r6Versions(t, r7Chain(last+3, last), "pom.xml", nil)["g:x"]; x.Version != "4.2" {
		t.Errorf("a property set by the last pom the limit reaches, in a longer chain: g:x at %q, want 4.2", x.Version)
	}
	if x := r6Versions(t, r7Chain(last+3, last+1), "pom.xml", nil)["g:x"]; x.Version == "4.2" {
		t.Errorf("a property set by the pom one past the limit was read: g:x at %q", x.Version)
	}
	whole := func(files map[string]string) bool {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		got := w.Read(root+"/pom.xml", Options{})
		for i := range got.Deps {
			got.Deps[i].ManifestPath = "pom.xml"
		}
		return len(w.SettleMavenManaged(got.Deps).NotWhole) == 0
	}
	if !whole(r7Chain(last, last)) {
		t.Errorf("a chain of exactly pomChainLimit poms that ends at a pom with no parent is whole")
	}
	if whole(r7Chain(last+1, last)) {
		t.Errorf("a chain one pom longer than pomChainLimit was not followed to its end, and is reported whole")
	}
}

// A cycle of parents ends at the same limit: it is read, answers as its
// first turn does, and is not whole. (Round 7: two guards that each ended a
// cycle sooner were removed; nothing but this limit ends one now.)
func TestACycleOfParentsEndsAtTheChainLimit(t *testing.T) {
	cycle := func(a, b string) map[string]string {
		return map[string]string{
			"pom.xml": "<project><parent><groupId>t</groupId><artifactId>b</artifactId><version>1</version><relativePath>b</relativePath></parent>" +
				"<groupId>t</groupId><artifactId>a</artifactId><version>1</version><properties><v>" + a + "</v></properties><dependencies>" + r4dep("g", "x", "${v}") + r4dep("g", "y", "${w}") + "</dependencies></project>",
			"b/pom.xml": "<project><parent><groupId>t</groupId><artifactId>a</artifactId><version>1</version><relativePath>..</relativePath></parent>" +
				"<groupId>t</groupId><artifactId>b</artifactId><version>1</version><properties><v>9.9</v><w>" + b + "</w></properties></project>",
		}
	}
	got := r6Versions(t, cycle("1.1", "2.2"), "pom.xml", nil)
	if got["g:x"].Version != "1.1" || got["g:y"].Version != "2.2" {
		t.Errorf("in a cycle of two poms: g:x at %q (want 1.1, the pom's own) and g:y at %q (want 2.2, its parent's)", got["g:x"].Version, got["g:y"].Version)
	}
	root := writeTree(t, cycle("1.1", "2.2"))
	tree := newPomTree(root, nil)
	chain, _, _, whole := tree.chainWhole(tree.model(root + "/pom.xml"))
	if whole || len(chain) != pomChainLimit {
		t.Errorf("a cycle's chain is %d poms, whole %v; want pomChainLimit (%d) and not whole", len(chain), whole, pomChainLimit)
	}
	// A relativePath that leads back to a pom of the chain whose
	// coordinates are NOT the parent's is no cycle: the parent is asked of
	// a repository, as Maven asks.
	files := map[string]string{
		"pom.xml": "<project><parent><groupId>t</groupId><artifactId>up</artifactId><version>1</version><relativePath>pom.xml</relativePath></parent>" +
			"<artifactId>a</artifactId><dependencies>" + r4dep("g", "x", "${v}") + "</dependencies></project>",
	}
	up := &RemotePom{Group: "t", Artifact: "up", Version: "1", Properties: map[string]string{"v": "7.7"}, ServedBy: "repo.example"}
	if x := r6Versions(t, files, "pom.xml", outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:up:1": up}})["g:x"]; x.Version != "7.7" {
		t.Errorf("a relativePath that names the pom itself, for a parent with other coordinates: g:x at %q; want 7.7 from the repository's pom", x.Version)
	}
}

// BOMs that import BOMs are followed pomChainLimit deep and no deeper.
func TestBOMsImportingBOMsAreFollowedToTheLimit(t *testing.T) {
	chain := func(depth int) map[string]string {
		files := map[string]string{"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "b1", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>"}
		for i := 1; i <= depth; i++ {
			body := r4import("t", fmt.Sprintf("b%d", i+1), "1")
			if i == depth {
				body = r4dep("g", "x", "3.3")
			}
			files[fmt.Sprintf("b%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>t</groupId><artifactId>b%d</artifactId><version>1</version><dependencyManagement><dependencies>%s</dependencies></dependencyManagement></project>", i, body)
		}
		return files
	}
	if x := r6Versions(t, oneBuild(chain(pomChainLimit)), "pom.xml", nil)["g:x"]; x.Version != "3.3" {
		t.Errorf("a BOM %d imports deep manages g:x: read at %q, want 3.3", pomChainLimit, x.Version)
	}
	if x := r6Versions(t, oneBuild(chain(pomChainLimit+1)), "pom.xml", nil)["g:x"]; x.Version != "" {
		t.Errorf("a BOM %d imports deep, one past the limit, was reached: g:x at %q", pomChainLimit+1, x.Version)
	}
}

// A property that is another property is followed pomSubstitutePasses deep
// by one substitution, and no deeper. (A version is substituted twice on its
// way through the readers, by the file's reader and then the tree's, so the
// bound is held here, on the substitution itself.)
func TestAChainOfPropertiesIsFollowedToItsLimit(t *testing.T) {
	resolver := func(depth int) func(string) (string, bool) {
		return func(name string) (string, bool) {
			var n int
			if _, err := fmt.Sscanf(name, "p%d", &n); err != nil || n < 1 || n > depth {
				return "", false
			}
			if n == depth {
				return "5.5", true
			}
			return fmt.Sprintf("${p%d}", n+1), true
		}
	}
	if got := pomSubstitute("${p1}", resolver(pomSubstitutePasses)); got != "5.5" {
		t.Errorf("a property %d properties deep: %q, want 5.5", pomSubstitutePasses, got)
	}
	if got := pomSubstitute("${p1}", resolver(pomSubstitutePasses+1)); !strings.Contains(got, "${") {
		t.Errorf("a property %d properties deep, one past pomSubstitutePasses, was resolved to %q", pomSubstitutePasses+1, got)
	}
}

// The two bounds are the reference's too, which states them on its own
// (oracleChainLimit, and the ten passes of oracleSubstitute): a change to
// one side alone is a disagreement. (The tests above follow the reader's
// constants, so that they hold at any value: this is what holds the value.)
func TestTheChainAndSubstitutionBoundsAreTheReferences(t *testing.T) {
	if pomChainLimit != oracleChainLimit {
		t.Errorf("pomChainLimit is %d and the reference's bound on a chain is %d", pomChainLimit, oracleChainLimit)
	}
	for depth := 1; depth <= 14; depth++ {
		resolve := func(name string) (string, bool) {
			var n int
			if _, err := fmt.Sscanf(name, "p%d", &n); err != nil || n < 1 || n > depth {
				return "", false
			}
			if n == depth {
				return "5.5", true
			}
			return fmt.Sprintf("${p%d}", n+1), true
		}
		if got, want := pomSubstitute("${p1}", resolve), oracleSubstitute("${p1}", resolve); got != want {
			t.Errorf("a property %d properties deep: the reader gives %q and the reference %q", depth, got, want)
		}
	}
}

// A long list of entries keyed by a reference is worked out once for the
// file (pomContext.firstRef), and answers as a short one does: the FIRST
// entry written that means the coordinate, and each list for itself.
func TestALongListOfReferenceKeyedEntriesAnswersAsItIsWritten(t *testing.T) {
	var props, entries strings.Builder
	// Twelve entries under the artifact x, each with a group that is a
	// reference: the first two both mean g:x.
	props.WriteString("<ga>g</ga><gb>g</gb><q>other</q>")
	entries.WriteString(r4dep("${ga}", "x", "1.0") + r4dep("${gb}", "x", "2.0"))
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&props, "<h%d>h%d</h%d><r%d>a%d</r%d>", i, i, i, i, i, i)
		entries.WriteString(r4dep(fmt.Sprintf("${h%d}", i), "x", fmt.Sprintf("9.%d", i)))
	}
	// Ten under the ARTIFACT k (group a reference), and ten under the GROUP
	// k (artifact a reference): two lists that share the word and nothing
	// else.
	for i := 0; i < 10; i++ {
		entries.WriteString(r4dep(fmt.Sprintf("${h%d}", i), "k", fmt.Sprintf("3.%d", i)))
		entries.WriteString(r4dep("k", fmt.Sprintf("${r%d}", i), fmt.Sprintf("4.%d", i)))
	}
	entries.WriteString(r4dep("${q}", "k", "5.0"))
	if pomRefListSmall >= 10 {
		t.Fatalf("the lists here have ten entries and more, and pomRefListSmall is %d: they would be read one by one", pomRefListSmall)
	}
	files := map[string]string{"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><properties>" + props.String() +
		"</properties><dependencyManagement><dependencies>" + entries.String() + "</dependencies></dependencyManagement><dependencies>" +
		r4dep("g", "x", "") + r4dep("h7", "x", "") + r4dep("other", "k", "") + r4dep("h3", "k", "") + r4dep("k", "a3", "") + r4dep("k", "a9", "") + r4dep("k", "none", "") +
		"</dependencies></project>"}
	got := r6Versions(t, files, "pom.xml", nil)
	for name, want := range map[string]string{"g:x": "1.0", "h7:x": "9.7", "other:k": "5.0", "h3:k": "3.3", "k:a3": "4.3", "k:a9": "4.9", "k:none": ""} {
		if got[name].Version != want {
			t.Errorf("%s at %q, want %q", name, got[name].Version, want)
		}
	}
}
