// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Managed entries and BOM imports stop being inventoried (summary/70, 4.5,
// D9). A dependencyManagement entry is not a dependency: it says which
// version a dependency WOULD get. Inventoried as one, an entry nothing uses
// had findings no module was exposed to. Only for a tree read whole: if any
// pom of the tree could not be read, a module that was not seen may be the
// user, so that tree's entries stay inventoried.

// readTree reads every pom.xml under root the way the analysis does and
// settles the managed entries.
func readTree(t *testing.T, src PomSource, files map[string]string) (root string, res MavenManaged) {
	t.Helper()
	root = writeTree(t, files)
	w := NewWalk(root, nil)
	if src != nil {
		w.SetPomSource(src)
	}
	var all []model.LibyearDep
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Base(path) != "pom.xml" {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		d := w.Read(path, Options{})
		for i := range d.Deps {
			d.Deps[i].ManifestPath = filepath.ToSlash(rel)
		}
		all = append(all, d.Deps...)
		return nil
	})
	return root, w.SettleMavenManaged(all)
}

func names(deps []model.LibyearDep) []string {
	var out []string
	for _, d := range deps {
		out = append(out, d.ManifestPath+" "+d.Name+"@"+d.Version)
	}
	sort.Strings(out)
	return out
}

func has(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

const reactorRoot = `<project><groupId>org.r</groupId><artifactId>root</artifactId><version>1.0</version><packaging>pom</packaging>
<modules><module>app</module></modules>
<dependencyManagement><dependencies>
  <dependency><groupId>com.acme</groupId><artifactId>used</artifactId><version>2.0</version></dependency>
  <dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
  <dependency><groupId>com.acme</groupId><artifactId>overridden</artifactId><version>4.0</version></dependency>
  <dependency><groupId>org.bom</groupId><artifactId>platform</artifactId><version>9.0</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement></project>`

const reactorApp = `<project><parent><groupId>org.r</groupId><artifactId>root</artifactId><version>1.0</version></parent><artifactId>app</artifactId>
<dependencies>
  <dependency><groupId>com.acme</groupId><artifactId>used</artifactId></dependency>
  <dependency><groupId>com.acme</groupId><artifactId>overridden</artifactId><version>4.5</version></dependency>
  <dependency><groupId>com.acme</groupId><artifactId>plain</artifactId><version>1.1</version></dependency>
</dependencies></project>`

const platformBOM = `<project><groupId>org.bom</groupId><artifactId>platform</artifactId><version>9.0</version><packaging>pom</packaging></project>`

func TestManagedEntriesOfAWholeTreeAreNotInventoried(t *testing.T) {
	_, res := readTree(t, nil, oneBuild(map[string]string{"pom.xml": reactorRoot, "app/pom.xml": reactorApp, "bom/pom.xml": platformBOM}))
	kept, unused := names(res.Kept), names(res.Unused)

	// The dependencies stay, at the versions they really get.
	for _, want := range []string{"app/pom.xml com.acme:used@2.0", "app/pom.xml com.acme:overridden@4.5", "app/pom.xml com.acme:plain@1.1"} {
		if !has(kept, want) {
			t.Errorf("kept %v; missing the dependency %q", kept, want)
		}
	}
	// No managed entry is a row any more.
	for _, k := range kept {
		if strings.HasPrefix(k, "pom.xml ") {
			t.Errorf("a managed entry of a whole tree is still inventoried: %q", k)
		}
	}
	// An entry nothing uses, an entry used at an OVERRIDING version (the
	// managed version was applied to nothing), and the BOM import's own
	// coordinate each leave a note.
	for _, want := range []string{"pom.xml com.acme:unused@3.0", "pom.xml com.acme:overridden@4.0", "pom.xml org.bom:platform@9.0"} {
		if !has(unused, want) {
			t.Errorf("unused %v; missing %q", unused, want)
		}
	}
	// An entry whose version WAS applied leaves neither a row nor a note:
	// the dependency that took it is its row.
	if has(unused, "pom.xml com.acme:used@2.0") {
		t.Errorf("unused %v; an entry whose version a dependency took is not unused", unused)
	}
	if len(res.NotWhole) != 0 {
		t.Errorf("not whole: %+v; this tree was read whole", res.NotWhole)
	}
}

// Two reactors and a broken fixture (the plan's test). A reactor with one
// module that cannot be read keeps its managed entries inventoried and gets
// no note; the other reactor, and the broken fixture standing alone, do not
// affect each other.
func TestOnlyATreeReadWholeLosesItsManagedEntries(t *testing.T) {
	// Each reactor has the BOM it imports as a module: a pom is found by its
	// coordinates in the build of the pom that names them, and the two BOMs,
	// one coordinate in two builds, are not two candidates (round 9).
	withBOM := func(root string) string {
		return strings.Replace(root, "<module>app</module>", "<module>app</module><module>bom</module>", 1)
	}
	second := strings.NewReplacer("org.r", "org.s", "<module>app</module>", "<module>app</module><module>gone</module>").Replace(reactorRoot)
	secondApp := strings.ReplaceAll(reactorApp, "org.r", "org.s")
	_, res := readTree(t, nil, map[string]string{
		"one/pom.xml": withBOM(reactorRoot), "one/app/pom.xml": reactorApp, "one/bom/pom.xml": platformBOM,
		"two/pom.xml": withBOM(second), "two/app/pom.xml": secondApp, "two/bom/pom.xml": platformBOM, // its second module does not exist
		"fixtures/broken/pom.xml": "<project><groupId>",
	})
	kept, unused := names(res.Kept), names(res.Unused)
	if !has(unused, "one/pom.xml com.acme:unused@3.0") || has(kept, "one/pom.xml com.acme:unused@3.0") {
		t.Errorf("reactor one was read whole: kept %v unused %v", kept, unused)
	}
	if !has(kept, "two/pom.xml com.acme:unused@3.0") {
		t.Errorf("reactor two has a module that could not be read; its managed entries stay inventoried. kept %v", kept)
	}
	for _, u := range unused {
		if strings.HasPrefix(u, "two/") {
			t.Errorf("a note was written for a tree that is not whole: %q", u)
		}
	}
	why := map[string]string{}
	for _, g := range res.NotWhole {
		why[g.Root] = g.Reason
	}
	if why["two/pom.xml"] == "" {
		t.Errorf("not whole = %+v; reactor two must be named, with its reason", res.NotWhole)
	}
	if _, named := why["one/pom.xml"]; named {
		t.Errorf("reactor one is whole and was named: %+v", res.NotWhole)
	}
}

// The other ways a tree is not whole.
func TestWhatMakesAMavenTreeNotWhole(t *testing.T) {
	entry := `<dependencyManagement><dependencies><dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency></dependencies></dependencyManagement>`
	for name, files := range map[string]map[string]string{
		"a parent that could not be obtained": {"pom.xml": `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>` + entry + `</project>`},
		"management inside a profile": {"pom.xml": `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>` + entry + `
			<profiles><profile><id>p</id><dependencyManagement><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>1</version></dependency></dependencies></dependencyManagement></profile></profiles></project>`},
		"an ambiguous BOM import": {
			"pom.xml": `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><dependencyManagement><dependencies>
				<dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
				<dependency><groupId>org.bom</groupId><artifactId>platform</artifactId><version>9.0</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement></project>`,
			"x/pom.xml": platformBOM, "y/pom.xml": platformBOM},
		"a BOM import that could not be obtained": {
			"pom.xml": `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><dependencyManagement><dependencies>
				<dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
				<dependency><groupId>org.bom</groupId><artifactId>platform</artifactId><version>9.0</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement></project>`},
	} {
		_, res := readTree(t, nil, files)
		if !has(names(res.Kept), "pom.xml com.acme:unused@3.0") || len(res.Unused) != 0 {
			t.Errorf("%s: kept %v unused %v; the tree is not whole, so its entries stay and no note is written", name, names(res.Kept), names(res.Unused))
		}
		if len(res.NotWhole) == 0 {
			t.Errorf("%s: the tree was not named as not whole", name)
		}
	}
	// A standalone pom is its own tree, and whole. And with a source that
	// obtains the parent, that tree is whole too.
	_, res := readTree(t, nil, map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>` + entry + `</project>`})
	if len(res.Kept) != 0 || !has(names(res.Unused), "pom.xml com.acme:unused@3.0") {
		t.Errorf("a standalone pom: kept %v unused %v", names(res.Kept), names(res.Unused))
	}
	src := &fakePoms{poms: map[string]*RemotePom{"far:away:1": {Group: "far", Artifact: "away", Version: "1"}}}
	_, res = readTree(t, src, map[string]string{"pom.xml": `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>` + entry + `</project>`})
	if len(res.NotWhole) != 0 || !has(names(res.Unused), "pom.xml com.acme:unused@3.0") {
		t.Errorf("a parent obtained from a repository: not whole %+v, unused %v", res.NotWhole, names(res.Unused))
	}
}

// What is not Maven is untouched.
func TestSettleMavenManagedLeavesOtherEcosystemsAlone(t *testing.T) {
	w := NewWalk(t.TempDir(), nil)
	in := []model.LibyearDep{{Name: "express", Manager: "npm", Version: "4.0.0", ManifestPath: "package.json"}}
	res := w.SettleMavenManaged(in)
	if len(res.Kept) != 1 || len(res.Unused) != 0 || len(res.NotWhole) != 0 {
		t.Errorf("%+v", res)
	}
}
