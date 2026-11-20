// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The remote half of the Maven tree (summary/70, 4.5): a parent or a BOM
// that is not in the repository is asked of a PomSource. About a quarter of
// version-less pom dependencies need one (summary/69's sample).

// fakePoms answers from a table and records what it was asked.
type fakePoms struct {
	poms    map[string]*RemotePom
	outcome map[string]RemoteOutcome // for coordinates with no pom
	asked   []string
}

func (f *fakePoms) RemotePom(group, artifact, version string, _ []string) (*RemotePom, RemoteOutcome) {
	key := group + ":" + artifact + ":" + version
	f.asked = append(f.asked, key)
	// A stated outcome wins over a pom in the table: "this one fails".
	if o, ok := f.outcome[key]; ok {
		return nil, o
	}
	if p, ok := f.poms[key]; ok {
		return p, RemoteFound
	}
	return nil, RemoteNotFound
}

func remoteDeps(t *testing.T, src PomSource, files map[string]string, rel string) map[string]model.LibyearDep {
	t.Helper()
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	w.SetPomSource(src)
	d := w.Read(filepath.Join(root, rel), Options{})
	if d.Failure != nil {
		t.Fatal(d.Failure)
	}
	out := map[string]model.LibyearDep{}
	for _, dep := range d.Deps {
		out[dep.Name] = dep
	}
	return out
}

const springChild = `<project>
  <parent><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-parent</artifactId><version>3.2.0</version><relativePath/></parent>
  <artifactId>app</artifactId>
  <dependencies>
    <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency>
    <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId></dependency>
    <dependency><groupId>com.acme</groupId><artifactId>unmanaged</artifactId></dependency>
  </dependencies>
</project>`

func springPoms() map[string]*RemotePom {
	return map[string]*RemotePom{
		// The starter parent's parent holds the management, through a
		// property, and imports a BOM that is also remote.
		"org.springframework.boot:spring-boot-starter-parent:3.2.0": {
			Group: "org.springframework.boot", Artifact: "spring-boot-starter-parent", Version: "3.2.0",
			Parent: &RemotePomParent{Group: "org.springframework.boot", Artifact: "spring-boot-dependencies", Version: "3.2.0"},
		},
		"org.springframework.boot:spring-boot-dependencies:3.2.0": {
			Group: "org.springframework.boot", Artifact: "spring-boot-dependencies", Version: "3.2.0",
			Properties: map[string]string{"jackson.version": "2.15.3", "netty.version": "4.1.101.Final"},
			Managed: []RemoteManaged{
				{Group: "com.fasterxml.jackson.core", Artifact: "jackson-databind", Version: "${jackson.version}"},
				{Group: "io.netty", Artifact: "netty-bom", Version: "${netty.version}", Import: true},
			},
		},
		"io.netty:netty-bom:4.1.101.Final": {
			Group: "io.netty", Artifact: "netty-bom", Version: "4.1.101.Final",
			Managed: []RemoteManaged{{Group: "io.netty", Artifact: "netty-all", Version: "${project.version}"}},
		},
	}
}

func TestARemoteParentAndBOMManageVersions(t *testing.T) {
	src := &fakePoms{poms: springPoms()}
	deps := remoteDeps(t, src, map[string]string{"pom.xml": springChild}, "pom.xml")
	if d := deps["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "2.15.3" || d.VersionSource != SourceManaged || d.VersionReason != "" {
		t.Errorf("managed by the remote parent's parent, through its property: %+v", d)
	}
	// A BOM's ${project.version} is the BOM's own.
	if d := deps["io.netty:netty-all"]; d.Version != "4.1.101.Final" || d.VersionSource != SourceManaged {
		t.Errorf("managed by a BOM the remote chain imports: %+v", d)
	}
	// The chain was obtained whole and nothing in it manages this one: it
	// has no version, and no reason to give (it simply declares none).
	if d := deps["com.acme:unmanaged"]; d.Version != "" || d.VersionReason != "" {
		t.Errorf("managed nowhere in a chain read whole: %+v", d)
	}
	// Each coordinate is asked once per walk, however many dependencies
	// need it.
	seen := map[string]int{}
	for _, k := range src.asked {
		seen[k]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s was asked %d times in one walk", k, n)
		}
	}
}

// When a parent or BOM cannot be obtained, a dependency it might manage has
// no version AND says why: that is not the same as "declares no version".
func TestAMissingRemoteParentIsAReason(t *testing.T) {
	const parent = "org.springframework.boot:spring-boot-starter-parent:3.2.0"
	for name, c := range map[string]struct {
		outcome RemoteOutcome
		reason  string
	}{
		"on no repository":          {RemoteNotFound, model.VersionReasonInheritedNotFound},
		"the lookup failed":         {RemoteFailed, model.VersionReasonInheritedLookupFailed},
		"the repository is refused": {RemoteRefused, model.VersionReasonInheritedRepositoryRefused},
	} {
		src := &fakePoms{outcome: map[string]RemoteOutcome{parent: c.outcome}}
		d := remoteDeps(t, src, map[string]string{"pom.xml": springChild}, "pom.xml")["io.netty:netty-all"]
		if d.Version != "" || d.VersionReason != c.reason {
			t.Errorf("%s: %+v; want no version and the reason %q", name, d, c.reason)
		}
	}
	// A dependency with its own version is untouched by a missing parent.
	explicit := `<project><parent><groupId>g</groupId><artifactId>p</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>1.2.3</version></dependency></dependencies></project>`
	if d := remoteDeps(t, &fakePoms{}, map[string]string{"pom.xml": explicit}, "pom.xml")["x:y"]; d.Version != "1.2.3" || d.VersionReason != "" {
		t.Errorf("an explicit version under a missing parent: %+v", d)
	}
	// The worst reason in a chain is the one given: a failed lookup may yet
	// succeed, so it outranks "not found".
	src := &fakePoms{poms: springPoms(), outcome: map[string]RemoteOutcome{"io.netty:netty-bom:4.1.101.Final": RemoteFailed}}
	if d := remoteDeps(t, src, map[string]string{"pom.xml": springChild}, "pom.xml")["io.netty:netty-all"]; d.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("a BOM whose lookup failed: %+v", d)
	}
}

// What is not asked remotely.
func TestWhatThePomTreeDoesNotAskRemotely(t *testing.T) {
	child := func(version, relativePath string) string {
		return `<project><parent><groupId>g</groupId><artifactId>p</artifactId><version>` + version + `</version>` + relativePath + `</parent><artifactId>a</artifactId>
		<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	}
	for name, c := range map[string]struct {
		files  map[string]string
		reason string
	}{
		// A snapshot is whatever was built last; a range is not one pom.
		"a snapshot parent":            {map[string]string{"pom.xml": child("1.0-SNAPSHOT", "<relativePath/>")}, model.VersionReasonInheritedNotFound},
		"a ranged parent":              {map[string]string{"pom.xml": child("[1.0,2.0)", "<relativePath/>")}, model.VersionReasonInheritedNotFound},
		"an unresolved parent version": {map[string]string{"pom.xml": child("${revision}", "<relativePath/>")}, model.VersionReasonInheritedNotFound},
	} {
		src := &fakePoms{}
		d := remoteDeps(t, src, c.files, "pom.xml")["x:y"]
		if len(src.asked) != 0 {
			t.Errorf("%s: asked remotely for %v", name, src.asked)
		}
		if d.VersionReason != c.reason {
			t.Errorf("%s: reason %q, want %q", name, d.VersionReason, c.reason)
		}
	}
	// A parent that IS in the tree is read from the tree and not asked for.
	src := &fakePoms{}
	parent := `<project><groupId>g</groupId><artifactId>p</artifactId><version>1</version>
	<dependencyManagement><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>9</version></dependency></dependencies></dependencyManagement></project>`
	d := remoteDeps(t, src, map[string]string{"pom.xml": parent, "app/pom.xml": child("1", "")}, "app/pom.xml")["x:y"]
	if d.Version != "9" || len(src.asked) != 0 {
		t.Errorf("a parent in the tree: %+v, asked %v", d, src.asked)
	}
	// Two tree poms with a BOM's coordinates: ambiguous, and NOT settled by
	// asking a repository (it may hold a third thing under that name).
	app := `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
	<dependencyManagement><dependencies><dependency><groupId>b</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	bom := `<project><groupId>b</groupId><artifactId>bom</artifactId><version>1</version></project>`
	src = &fakePoms{}
	remoteDeps(t, src, map[string]string{"pom.xml": app, "one/pom.xml": bom, "two/pom.xml": bom}, "pom.xml")
	if len(src.asked) != 0 {
		t.Errorf("an ambiguous BOM was asked for remotely: %v", src.asked)
	}
	// With no source at all, nothing is asked and nothing is claimed: no
	// reason is recorded for a lookup that was never made.
	root := writeTree(t, map[string]string{"pom.xml": child("1", "<relativePath/>")})
	got := NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{})
	if got.Deps[0].VersionReason != "" {
		t.Errorf("no source installed: reason %q", got.Deps[0].VersionReason)
	}
}

// A remote pom is extracted to the same model a tree pom is.
func TestParseRemotePom(t *testing.T) {
	p, err := ParseRemotePom(parentPom)
	if err != nil {
		t.Fatal(err)
	}
	if p.Group != "org.example" || p.Artifact != "parent" || p.Version != "2.5.0" || p.Properties["jackson.version"] != "2.15.2" {
		t.Errorf("coordinates and properties: %+v", p)
	}
	imports := 0
	for _, m := range p.Managed {
		if m.Import {
			imports++
		}
	}
	if len(p.Managed) != 5 || imports != 1 {
		t.Errorf("%d managed entries, %d of them imports; want 5 and 1", len(p.Managed), imports)
	}
	if _, err := ParseRemotePom("<project><groupId>"); err == nil {
		t.Error("an unparseable pom was accepted")
	}
}

// A BOM that was obtained, but whose OWN parent was not, is not whole: a
// dependency it does not manage itself may be managed by that parent.
func TestAGapInsideARemoteBOMIsAReason(t *testing.T) {
	poms := springPoms()
	poms["io.netty:netty-bom:4.1.101.Final"] = &RemotePom{Group: "io.netty", Artifact: "netty-bom", Version: "4.1.101.Final",
		Parent: &RemotePomParent{Group: "io.netty", Artifact: "netty-parent", Version: "4.1.101.Final"}}
	src := &fakePoms{poms: poms, outcome: map[string]RemoteOutcome{"io.netty:netty-parent:4.1.101.Final": RemoteFailed}}
	d := remoteDeps(t, src, map[string]string{"pom.xml": springChild}, "pom.xml")["io.netty:netty-all"]
	if d.Version != "" || d.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("%+v; want no version, and the failed lookup inside the BOM's chain as the reason", d)
	}
}

// Building the tree's coordinate index reads every pom.xml of the clone. It
// must ask a repository for none of their parents: a clone with a thousand
// modules would send a thousand requests to index itself.
func TestIndexingTheTreeAsksNothingRemotely(t *testing.T) {
	app := `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
	<dependencyManagement><dependencies><dependency><groupId>b</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	bom := `<project><groupId>b</groupId><artifactId>bom</artifactId><version>1</version>
	<dependencyManagement><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>7</version></dependency></dependencies></dependencyManagement></project>`
	bystander := `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent><artifactId>bystander</artifactId></project>`
	src := &fakePoms{}
	d := remoteDeps(t, src, map[string]string{"pom.xml": app, "bom/pom.xml": bom, "other/pom.xml": bystander}, "pom.xml")["x:y"]
	if d.Version != "7" {
		t.Fatalf("the BOM in the tree did not answer: %+v", d)
	}
	for _, k := range src.asked {
		if k == "far:away:3" {
			t.Errorf("indexing the tree asked a repository for a bystander pom's parent: %v", src.asked)
		}
	}
}

// With two reasons in one model, the worse is given, whichever was met
// first: here the parent's lookup FAILED (met first) and a BOM is simply on
// no repository (met second). A failed lookup may yet succeed, so it is what
// the row must say.
func TestTheWorseOfTwoReasonsIsGiven(t *testing.T) {
	child := `<project><parent><groupId>g</groupId><artifactId>p</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>
	<dependencyManagement><dependencies><dependency><groupId>b</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	src := &fakePoms{outcome: map[string]RemoteOutcome{"g:p:1": RemoteFailed, "b:bom:1": RemoteNotFound}}
	if d := remoteDeps(t, src, map[string]string{"pom.xml": child}, "pom.xml")["x:y"]; d.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("reason %q, want the failed lookup", d.VersionReason)
	}
	for _, c := range [][3]string{
		{model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedNotFound, model.VersionReasonInheritedLookupFailed},
		{model.VersionReasonInheritedNotFound, model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedLookupFailed},
		{model.VersionReasonInheritedRepositoryRefused, model.VersionReasonInheritedNotFound, model.VersionReasonInheritedRepositoryRefused},
		{"", model.VersionReasonInheritedNotFound, model.VersionReasonInheritedNotFound},
		{model.VersionReasonInheritedNotFound, "", model.VersionReasonInheritedNotFound},
	} {
		if got := worseReason(c[0], c[1]); got != c[2] {
			t.Errorf("worseReason(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}
