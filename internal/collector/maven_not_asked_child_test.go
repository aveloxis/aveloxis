// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// "Not asked" is not "not found", all the way through (0.29.214): a pom
// whose parent the analysis declines to ask any repository for (its version
// is not text a request can be built from) has an in-tree BOM that manages
// the dependency. What the parent says is NOT KNOWN, and Maven consults it
// first, so the BOM's version is withheld. The answer travels from the real
// source, across the child's wire, to the reader: a layer that turned it
// back into "not found" would give the dependency the BOM's version.
func TestAParentThatIsNotAskedForWithholdsWhatALaterBOMWouldGive(t *testing.T) {
	_, src := pomSourceFixture(t, "not-asked-child")
	repo := newPomRepo(t, nil)
	withMavenRepositories(t, repo.srv.URL)

	const bom = `<project><groupId>t</groupId><artifactId>bom</artifactId><version>1</version><packaging>pom</packaging>
		<dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId><version>2.0</version></dependency></dependencies></dependencyManagement></project>`
	app := func(parentVersion string) string {
		return `<project><parent><groupId>org.remote</groupId><artifactId>parent</artifactId><version>` + parentVersion + `</version><relativePath/></parent>
		<artifactId>app</artifactId>
		<dependencyManagement><dependencies><dependency><groupId>t</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
		<dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies></project>`
	}
	read := func(parentVersion string) (version, reason string) {
		t.Helper()
		dir := writeClone(t, map[string]string{"bom/pom.xml": bom, "app/pom.xml": app(parentVersion)})
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: maxRemotePomCoordinates},
			testLimits(), src, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range run.decl.Deps {
			if d.Name == "g:x" && d.ManifestPath == "app/pom.xml" {
				return d.Version, d.VersionReason
			}
		}
		t.Fatalf("g:x of app/pom.xml is not among %d dependencies", len(run.decl.Deps))
		return "", ""
	}

	// The control: a parent every repository answers "not found" for is an
	// answer, and the BOM's version stands.
	before := repo.asked.Load()
	if v, reason := read("5.0"); v != "2.0" || reason != "" || repo.asked.Load() == before {
		t.Fatalf("a parent that is not found: version %q reason %q, asked %d more; want 2.0, no reason, and the repository asked", v, reason, repo.asked.Load()-before)
	}
	// Not askable: nothing is asked, and the version is withheld.
	before, unaskable := repo.asked.Load(), src.unaskable
	if out := func() manifest.RemoteOutcome { _, o := src.RemotePom("org.remote", "parent", "5.0~rc1", nil); return o }(); out != manifest.RemoteNotAsked {
		t.Fatalf("the source answers %v for a coordinate it cannot ask for; want not asked", out)
	}
	v, reason := read("5.0~rc1")
	if v != "" || reason != model.VersionReasonInheritedNotFound {
		t.Errorf("behind a parent that was not asked for: version %q reason %q; want no version and %s", v, reason, model.VersionReasonInheritedNotFound)
	}
	if repo.asked.Load() != before || src.unaskable <= unaskable {
		t.Errorf("asked %d more, counted %d unaskable; want none asked and the coordinate counted", repo.asked.Load()-before, src.unaskable-unaskable)
	}
}
