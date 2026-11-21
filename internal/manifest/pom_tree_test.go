// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Maven read as a tree (summary/70, 4.4). A dependency with no <version> is
// not unversioned: Maven takes its version from dependencyManagement, in the
// same pom, a parent or an imported BOM, with properties interpolated across
// the chain. Read one file at a time, such a dependency had no version, was
// asked of OSV with none, and got back every advisory the package ever had.
// (Sample of 300 repositories, summary/69: 73% of version-less pom
// dependencies resolve in the tree.)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func pomDeps(t *testing.T, root, rel string) map[string]model.LibyearDep {
	t.Helper()
	d := NewWalk(root, nil).Read(filepath.Join(root, rel), Options{DevBuildDeps: true})
	if d.Failure != nil {
		t.Fatalf("%s: %v", rel, d.Failure)
	}
	out := map[string]model.LibyearDep{}
	for _, dep := range d.Deps {
		out[dep.Name] = dep
	}
	return out
}

const parentPom = `<project>
  <groupId>org.example</groupId><artifactId>parent</artifactId><version>2.5.0</version><packaging>pom</packaging>
  <properties><jackson.version>2.15.2</jackson.version><shared.version>1.0.0</shared.version></properties>
  <dependencyManagement><dependencies>
    <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId><version>${jackson.version}</version></dependency>
    <dependency><groupId>org.example</groupId><artifactId>sibling</artifactId><version>${project.version}</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>overridden</artifactId><version>1.0</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>parent-entry-child-property</artifactId><version>${shared.version}</version></dependency>
    <dependency><groupId>org.example.bom</groupId><artifactId>platform-bom</artifactId><version>7.0.0</version><type>pom</type><scope>import</scope></dependency>
  </dependencies></dependencyManagement>
</project>`

const childPom = `<project>
  <parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>2.5.0</version></parent>
  <artifactId>app</artifactId>
  <properties><shared.version>9.9.9</shared.version></properties>
  <dependencyManagement><dependencies>
    <dependency><groupId>com.acme</groupId><artifactId>overridden</artifactId><version>2.0</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>own-managed</artifactId><version>3.3.3</version></dependency>
  </dependencies></dependencyManagement>
  <dependencies>
    <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency>
    <dependency><groupId>org.example</groupId><artifactId>sibling</artifactId></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>overridden</artifactId></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>own-managed</artifactId></dependency>
    <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>explicit</artifactId><version>4.4.4</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>by-inherited-property</artifactId><version>${jackson.version}</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>by-overridden-property</artifactId><version>${shared.version}</version></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>nowhere</artifactId></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>parent-entry-child-property</artifactId></dependency>
  </dependencies>
</project>`

const bomPom = `<project>
  <groupId>org.example.bom</groupId><artifactId>platform-bom</artifactId><version>7.0.0</version><packaging>pom</packaging>
  <dependencyManagement><dependencies>
    <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId><version>4.1.100.Final</version></dependency>
  </dependencies></dependencyManagement>
</project>`

func TestAVersionlessPomDependencyTakesItsVersionFromTheTree(t *testing.T) {
	root := writeTree(t, oneBuild(map[string]string{"pom.xml": parentPom, "app/pom.xml": childPom, "bom/pom.xml": bomPom}))
	deps := pomDeps(t, root, "app/pom.xml")
	for _, c := range []struct{ name, version, source, why string }{
		{"com.fasterxml.jackson.core:jackson-databind", "2.15.2", SourceManaged, "the parent's management, through the parent's property"},
		{"org.example:sibling", "2.5.0", SourceManaged, "${project.version} is resolved in the CHILD's effective model, as Maven does; the child inherits 2.5.0"},
		{"com.acme:overridden", "2.0", SourceManaged, "the child's own management wins over the parent's"},
		{"com.acme:own-managed", "3.3.3", SourceManaged, "management in the same pom"},
		{"io.netty:netty-all", "4.1.100.Final", SourceManaged, "a BOM the parent imports, found in the tree by its coordinates"},
		{"com.acme:explicit", "4.4.4", "", "an explicit version is the declaration's"},
		{"com.acme:by-inherited-property", "2.15.2", "", "a property inherited from the parent; still the declaration's version"},
		{"com.acme:by-overridden-property", "9.9.9", "", "the child's property overrides the parent's"},
		{"com.acme:nowhere", "", "", "managed nowhere in the tree: left for the remote resolver"},
		{"com.acme:parent-entry-child-property", "9.9.9", SourceManaged, "the PARENT's entry names a property the CHILD overrides: Maven builds the child's model, so the child's value"},
	} {
		got, ok := deps[c.name]
		if !ok {
			t.Errorf("%s: not read", c.name)
			continue
		}
		if got.Version != c.version || got.VersionSource != c.source {
			t.Errorf("%s: version %q source %q, want %q %q (%s)", c.name, got.Version, got.VersionSource, c.version, c.source, c.why)
		}
	}
}

// A parent is found by <relativePath> and VERIFIED by its coordinates: a pom
// at the expected place that is another project is not the parent (SR-6).
func TestThePomParentIsVerifiedByItsCoordinates(t *testing.T) {
	child := func(relativePath string) string {
		return `<project><parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>2.5.0</version>` + relativePath + `</parent>
		<artifactId>app</artifactId><dependencies>
		<dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency></dependencies></project>`
	}
	const jackson = "com.fasterxml.jackson.core:jackson-databind"
	impostor := `<project><groupId>org.other</groupId><artifactId>parent</artifactId><version>2.5.0</version>
		<dependencyManagement><dependencies><dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId><version>0.0.1</version></dependency></dependencies></dependencyManagement></project>`

	for name, c := range map[string]struct {
		files   map[string]string
		version string
	}{
		"the default ../pom.xml":                {map[string]string{"pom.xml": parentPom, "app/pom.xml": child("")}, "2.15.2"},
		"a relativePath to a file":              {map[string]string{"build/parent.xml": parentPom, "app/pom.xml": child("<relativePath>../build/parent.xml</relativePath>")}, "2.15.2"},
		"a relativePath to a directory":         {map[string]string{"build/pom.xml": parentPom, "app/pom.xml": child("<relativePath>../build</relativePath>")}, "2.15.2"},
		"another project at the expected place": {map[string]string{"pom.xml": impostor, "app/pom.xml": child("")}, ""},
		"the same group, another artifact":      {map[string]string{"pom.xml": strings.Replace(parentPom, "<artifactId>parent</artifactId>", "<artifactId>other-parent</artifactId>", 1), "app/pom.xml": child("")}, ""},
		// 0.29.215: an empty relativePath says "do not look at ../pom.xml";
		// Maven still finds a parent that is part of the build by its
		// coordinates, and so does this (round 8). With no pom of the clone
		// under the coordinates there is none.
		"an empty relativePath, the parent in the clone":        {map[string]string{"pom.xml": parentPom, "app/pom.xml": child("<relativePath/>")}, "2.15.2"},
		"an empty relativePath, no such pom in the clone":       {map[string]string{"pom.xml": impostor, "app/pom.xml": child("<relativePath/>")}, ""},
		"the parent elsewhere in the clone, by its coordinates": {map[string]string{"deep/down/pom.xml": parentPom, "app/pom.xml": child("")}, "2.15.2"},
		"a relativePath above the clone":                        {map[string]string{"app/pom.xml": child("<relativePath>../../../../etc/pom.xml</relativePath>")}, ""},
		"the parent's version differs from the ref's":           {map[string]string{"pom.xml": `<project><groupId>org.example</groupId><artifactId>parent</artifactId><version>9.0.0</version></project>`, "app/pom.xml": child("")}, ""},
	} {
		root := writeTree(t, oneBuild(c.files))
		if got := pomDeps(t, root, "app/pom.xml")[jackson].Version; got != c.version {
			t.Errorf("%s: version %q, want %q", name, got, c.version)
		}
	}
}

// Two poms in the tree with the same coordinates and version are ambiguous:
// neither is chosen for a BOM import (fixture and template poms are common).
func TestAnAmbiguousBOMIsNotChosen(t *testing.T) {
	app := `<project><groupId>org.example</groupId><artifactId>app</artifactId><version>1.0</version>
	<dependencyManagement><dependencies><dependency><groupId>org.example.bom</groupId><artifactId>platform-bom</artifactId><version>7.0.0</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
	<dependencies><dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId></dependency></dependencies></project>`
	other := `<project><groupId>org.example.bom</groupId><artifactId>platform-bom</artifactId><version>7.0.0</version>
	<dependencyManagement><dependencies><dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId><version>3.0.0</version></dependency></dependencies></dependencyManagement></project>`
	one := writeTree(t, oneBuild(map[string]string{"pom.xml": app, "bom/pom.xml": bomPom}))
	if got := pomDeps(t, one, "pom.xml")["io.netty:netty-all"]; got.Version != "4.1.100.Final" {
		t.Errorf("one BOM in the tree: %+v", got)
	}
	two := writeTree(t, oneBuild(map[string]string{"pom.xml": app, "bom/pom.xml": bomPom, "templates/bom/pom.xml": other}))
	if got := pomDeps(t, two, "pom.xml")["io.netty:netty-all"]; got.Version != "" || got.VersionSource != "" {
		t.Errorf("two poms with the BOM's coordinates: %+v; want no version chosen", got)
	}
}

// The tree is read under the walk's own rules: a symlinked parent is
// refused, an unparseable one is no parent, and a cycle ends.
func TestThePomTreeIsReadSafely(t *testing.T) {
	const jackson = "com.fasterxml.jackson.core:jackson-databind"
	child := `<project><parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>2.5.0</version></parent><artifactId>app</artifactId>
	<dependencies><dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency></dependencies></project>`

	root := writeTree(t, map[string]string{"real/pom.xml": parentPom, "app/pom.xml": child})
	if err := os.Symlink(filepath.Join(root, "real", "pom.xml"), filepath.Join(root, "pom.xml")); err != nil {
		t.Skip("symlinks unavailable")
	}
	if got := pomDeps(t, root, "app/pom.xml")[jackson].Version; got != "" {
		t.Errorf("a symlinked parent was followed: version %q", got)
	}

	root = writeTree(t, map[string]string{"pom.xml": "<project><groupId>org.example</groupId><artifactId>parent", "app/pom.xml": child})
	if got := pomDeps(t, root, "app/pom.xml")[jackson].Version; got != "" {
		t.Errorf("an unparseable parent answered: version %q", got)
	}

	// a's parent is b, b's parent is a.
	a := `<project><parent><groupId>g</groupId><artifactId>b</artifactId><version>1</version><relativePath>../b/pom.xml</relativePath></parent><artifactId>a</artifactId><version>1</version>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	b := `<project><parent><groupId>g</groupId><artifactId>a</artifactId><version>1</version><relativePath>../a/pom.xml</relativePath></parent><artifactId>b</artifactId><version>1</version></project>`
	root = writeTree(t, map[string]string{"a/pom.xml": a, "b/pom.xml": b})
	if got := pomDeps(t, root, "a/pom.xml")["x:y"]; got.Version != "" {
		t.Errorf("a parent cycle: %+v", got)
	}
}

// A plugin's dependencies are build tooling: kept, with build scope.
func TestPomPluginDependenciesAreBuildScope(t *testing.T) {
	root := writeTree(t, map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>lib</artifactId><version>1.0</version></dependency></dependencies>
	<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-checkstyle-plugin</artifactId><version>3.3.0</version>
	  <dependencies><dependency><groupId>com.puppycrawl.tools</groupId><artifactId>checkstyle</artifactId><version>10.12.4</version></dependency></dependencies>
	</plugin></plugins></build></project>`})
	deps := pomDeps(t, root, "pom.xml")
	if d := deps["com.puppycrawl.tools:checkstyle"]; d.Version != "10.12.4" || d.Type != model.ScopeBuild {
		t.Errorf("a plugin's dependency = %+v; want it kept with build scope", d)
	}
	if d := deps["com.acme:lib"]; d.Type != "runtime" {
		t.Errorf("an ordinary dependency = %+v; want runtime scope", d)
	}
}

// Nothing is read above the clone, even when a pom is really there: the
// clone sits inside a directory that holds one at the default parent path.
func TestThePomTreeNeverReadsAboveTheClone(t *testing.T) {
	outer := writeTree(t, map[string]string{
		"pom.xml": parentPom,
		"clone/pom.xml": `<project><parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>2.5.0</version></parent><artifactId>app</artifactId>
		<dependencies><dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency></dependencies></project>`,
	})
	root := filepath.Join(outer, "clone")
	if got := pomDeps(t, root, "pom.xml")["com.fasterxml.jackson.core:jackson-databind"]; got.Version != "" {
		t.Errorf("a pom above the clone answered: %+v", got)
	}
}
