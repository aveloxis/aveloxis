// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// pomRepo is a Maven repository stand-in: it serves the poms given (keyed by
// "group:artifact:version"), answers 404 for the rest, and fails every
// request while down is set.
type pomRepo struct {
	srv   *httptest.Server
	asked atomic.Int64
	down  atomic.Bool
}

func newPomRepo(t *testing.T, poms map[string]string) *pomRepo {
	t.Helper()
	r := &pomRepo{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.asked.Add(1)
		if r.down.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		for coord, body := range poms {
			p := strings.Split(coord, ":")
			if req.URL.Path == "/"+strings.ReplaceAll(p[0], ".", "/")+"/"+p[1]+"/"+p[2]+"/"+p[1]+"-"+p[2]+".pom" {
				_, _ = io.WriteString(w, body)
				return
			}
		}
		http.NotFound(w, req)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

const remoteParentPom = `<project><groupId>org.remote</groupId><artifactId>parent</artifactId><version>5.0</version>
<properties><lib.version>7.7.7</lib.version></properties>
<dependencyManagement><dependencies>
  <dependency><groupId>com.acme</groupId><artifactId>lib</artifactId><version>${lib.version}</version></dependency>
</dependencies></dependencyManagement></project>`

func pomSourceFixture(t *testing.T, name string) (*wholeReplaceFixture, *mavenPomSource) {
	t.Helper()
	f, _ := scanFixture(t, name)
	clean := func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	}
	t.Cleanup(clean)
	clean()
	t.Cleanup(resetRegistryBreakers)
	src, err := newMavenPomSource(context.Background(), f.store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return f, src
}

// Resolve, then fail (the plan's first test): a pom that was obtained is
// stored, and is served from the store with no request, whatever the
// repository is doing.
func TestAFetchedPomIsStoredAndServedWithoutARequest(t *testing.T) {
	_, src := pomSourceFixture(t, "pom-stored")
	central := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, central.srv.URL)

	pom, outcome := src.RemotePom("org.remote", "parent", "5.0", nil)
	if outcome != manifest.RemoteFound || pom.Properties["lib.version"] != "7.7.7" || len(pom.Managed) != 1 {
		t.Fatalf("first resolution: %+v, outcome %v", pom, outcome)
	}
	central.down.Store(true)
	asked := central.asked.Load()
	pom, outcome = src.RemotePom("org.remote", "parent", "5.0", nil)
	if outcome != manifest.RemoteFound || pom.Properties["lib.version"] != "7.7.7" {
		t.Errorf("with the repository down: %+v, outcome %v; want the stored pom", pom, outcome)
	}
	if central.asked.Load() != asked {
		t.Error("a stored pom was asked for again")
	}
}

// A Central miss and a Jenkins hit; the miss expires; Central fails (the
// plan's second test). The stored chain answer stands: the pom still comes
// from Jenkins, and the due miss is used as it is.
func TestAStoredChainStandsWhenARefreshFails(t *testing.T) {
	f, src := pomSourceFixture(t, "pom-chain")
	central := newPomRepo(t, nil)
	jenkins := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, central.srv.URL, jenkins.srv.URL)

	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteFound {
		t.Fatalf("first resolution: outcome %v", outcome)
	}
	if central.asked.Load() != 1 || jenkins.asked.Load() != 1 {
		t.Fatalf("first resolution asked central %d and jenkins %d times; want once each", central.asked.Load(), jenkins.asked.Load())
	}
	// While the miss holds, Central is not asked again.
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteFound || central.asked.Load() != 1 || jenkins.asked.Load() != 1 {
		t.Errorf("second resolution: outcome %v, central asked %d, jenkins %d; want both answers from the store", outcome, central.asked.Load(), jenkins.asked.Load())
	}
	// The miss falls due and Central is failing.
	if _, err := f.store.Pool().Exec(context.Background(), `UPDATE aveloxis_data.maven_pom_results SET expires_at = NOW() - INTERVAL '1 second' WHERE repository_url = $1`, central.srv.URL); err != nil {
		t.Fatal(err)
	}
	central.down.Store(true)
	pom, outcome := src.RemotePom("org.remote", "parent", "5.0", nil)
	if outcome != manifest.RemoteFound || pom == nil || pom.Properties["lib.version"] != "7.7.7" {
		t.Errorf("Central's miss due and Central failing: %+v, outcome %v; want the stored chain answer (Jenkins's pom)", pom, outcome)
	}
	if central.asked.Load() != 2 {
		t.Errorf("Central was asked %d times in all; want the one refresh attempt", central.asked.Load())
	}
	// The same holds with no later repository to answer. A coordinate on
	// neither repository is "not found"; when Central's miss falls due and
	// Central is failing, the stored miss STANDS: still "not found", not
	// "unknown". Otherwise a repository's bad hour would turn every stored
	// miss into a failed lookup.
	central.down.Store(false)
	if _, outcome := src.RemotePom("org.remote", "absent", "1.0", nil); outcome != manifest.RemoteNotFound {
		t.Fatalf("on neither repository: outcome %v", outcome)
	}
	if _, err := f.store.Pool().Exec(context.Background(), `UPDATE aveloxis_data.maven_pom_results SET expires_at = NOW() - INTERVAL '1 second'
		WHERE repository_url = $1 AND artifact_id = 'absent'`, central.srv.URL); err != nil {
		t.Fatal(err)
	}
	central.down.Store(true)
	if _, outcome := src.RemotePom("org.remote", "absent", "1.0", nil); outcome != manifest.RemoteNotFound {
		t.Errorf("a due miss whose refresh failed: outcome %v; want the stored answer, not found", outcome)
	}
}

// A first-ever lookup that fails has nothing stored (the plan's third
// test): the coordinate is unknown, which is not "not found", and nothing
// is stored for it.
func TestAFirstLookupThatFailsIsUnknown(t *testing.T) {
	f, src := pomSourceFixture(t, "pom-first-fail")
	central := newPomRepo(t, nil)
	central.down.Store(true)
	withMavenRepositories(t, central.srv.URL)
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteFailed {
		t.Errorf("outcome %v, want failed (unknown)", outcome)
	}
	var n int
	if err := f.store.Pool().QueryRow(context.Background(), `SELECT COUNT(*) FROM aveloxis_data.maven_pom_results WHERE repository_url = $1`, central.srv.URL).Scan(&n); err != nil || n != 0 {
		t.Errorf("%d rows stored for a lookup that got no answer (err %v)", n, err)
	}
	// One repository failing and another answering "none" is still unknown.
	missing := newPomRepo(t, nil)
	withMavenRepositories(t, central.srv.URL, missing.srv.URL)
	if _, outcome := src.RemotePom("org.remote", "other", "1.0", nil); outcome != manifest.RemoteFailed {
		t.Errorf("a failure and a miss: outcome %v, want failed", outcome)
	}
}

// End to end: a pom whose parent is not in the clone gets its managed
// version from a configured repository, and the row says so. When the
// repository is then unreachable the row does not change.
func TestARemoteParentReachesTheInventory(t *testing.T) {
	f := lockFixture(t, "pom-e2e")
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	})
	t.Cleanup(resetRegistryBreakers)
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	f.write(t, "pom.xml", `<project><parent><groupId>org.remote</groupId><artifactId>parent</artifactId><version>5.0</version><relativePath/></parent>
	<artifactId>app</artifactId><dependencies>
	  <dependency><groupId>com.acme</groupId><artifactId>lib</artifactId></dependency>
	</dependencies></project>`)
	f.analyse(t)
	want := invRow{Version: "7.7.7", Source: model.VersionSourceManaged, Purl: "pkg:maven/com.acme/lib@7.7.7", Requirement: "com.acme:lib:"}
	if got := inventoryRows(t, f)["pom.xml:com.acme:lib"]; got != want {
		t.Fatalf("row = %+v, want %+v", got, want)
	}
	repo.down.Store(true)
	f.analyse(t)
	if got := inventoryRows(t, f)["pom.xml:com.acme:lib"]; got != want {
		t.Errorf("with the repository down the row changed to %+v; a transient failure must not change a version", got)
	}
}

// Managed entries stop being inventoried, end to end (summary/70, 4.5, D9):
// a root pom's unused entry leaves a note and no row, its findings close as
// never_applied, and the entry a module uses is that module's row.
func TestAnUnusedManagedEntryIsANoteAndItsFindingNeverApplied(t *testing.T) {
	f := lockFixture(t, "managed-unused")
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, f.repoID)
	})
	t.Cleanup(resetRegistryBreakers)
	ctx := context.Background()
	f.write(t, "pom.xml", `<project><groupId>org.r</groupId><artifactId>root</artifactId><version>1.0</version><modules><module>app</module></modules>
	<dependencyManagement><dependencies>
	  <dependency><groupId>com.acme</groupId><artifactId>used</artifactId><version>2.0</version></dependency>
	  <dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
	</dependencies></dependencyManagement></project>`)
	f.write(t, "app/pom.xml", `<project><parent><groupId>org.r</groupId><artifactId>root</artifactId><version>1.0</version></parent><artifactId>app</artifactId>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>used</artifactId></dependency></dependencies></project>`)
	// A finding recorded on the unused entry by an earlier release.
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-on-unused", PackageName: "com.acme:unused", PackagePurl: "pkg:maven/com.acme/unused@3.0", Ecosystem: "maven", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	f.analyse(t)
	rows := inventoryRows(t, f)
	if len(rows) != 1 || rows["app/pom.xml:com.acme:used"].Version != "2.0" {
		t.Errorf("inventory = %+v; want only the module's dependency, at its managed version", rows)
	}
	notes, err := f.store.GetRepoInventoryNotes(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || notes[0].Kind != model.NoRowManagedUnused || notes[0].Purl != "pkg:maven/com.acme/unused@3.0" {
		t.Errorf("notes = %+v; want one managed_unused note for the unused entry", notes)
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if cov.Inventory.DependenciesDeclared != 2 || cov.Inventory.DependenciesWithoutRow[model.NoRowManagedUnused] != 1 || len(cov.Inventory.MavenTreesNotWhole) != 0 {
		t.Errorf("coverage = %+v; want 2 declared, 1 without a row as managed_unused, every tree whole", cov.Inventory)
	}

	stubOSV(t, nil)
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, f.ac.logger, nil, true); err != nil {
		t.Fatal(err)
	}
	var reason *string
	var open bool
	if err := f.store.Pool().QueryRow(ctx, `SELECT resolved_at IS NULL, closed_reason FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND vuln_id = 'GHSA-on-unused'`, f.repoID).Scan(&open, &reason); err != nil {
		t.Fatal(err)
	}
	if open || reason == nil || *reason != model.ClosedNeverApplied {
		t.Errorf("the finding on the unused entry: open %v, reason %v; want closed as never_applied", open, reason)
	}

	// A tree that is not whole keeps its entries as rows, and says why.
	f.write(t, "pom.xml", `<project><groupId>org.r</groupId><artifactId>root</artifactId><version>1.0</version><modules><module>app</module><module>missing</module></modules>
	<dependencyManagement><dependencies>
	  <dependency><groupId>com.acme</groupId><artifactId>used</artifactId><version>2.0</version></dependency>
	  <dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
	</dependencies></dependencyManagement></project>`)
	f.analyse(t)
	if _, ok := inventoryRows(t, f)["pom.xml:com.acme:unused"]; !ok {
		t.Errorf("a tree with an unreadable module lost its managed entry's row: %+v", inventoryRows(t, f))
	}
	cov, _, _ = f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if len(cov.Inventory.MavenTreesNotWhole) != 1 || cov.Inventory.MavenTreesNotWhole[0].Root != "pom.xml" || cov.Inventory.MavenTreesNotWhole[0].Reason == "" {
		t.Errorf("coverage's trees not whole = %+v", cov.Inventory.MavenTreesNotWhole)
	}
}

func withDeclaredFetch(t *testing.T, on bool) {
	t.Helper()
	old := declaredRepositoryFetch.Load()
	SetDeclaredRepositoryFetch(on)
	t.Cleanup(func() { SetDeclaredRepositoryFetch(old) })
	// The test servers are http. The source's own https check is a seam;
	// its default is pinned by TestTheSourceActsOnlyOnHTTPSDeclaredAddresses.
	oldOK := mavenDeclaredSchemeOK
	mavenDeclaredSchemeOK = func(string) bool { return true }
	t.Cleanup(func() { mavenDeclaredSchemeOK = oldOK })
	// And they are on loopback, which a declared host is never trusted to
	// be: the rule's seam, its default pinned by
	// TestTheDeclaredHostPolicyIsStrictByDefault.
	oldPolicy := declaredHostPolicy
	declaredHostPolicy = currentOutboundPolicy
	t.Cleanup(func() { declaredHostPolicy = oldPolicy })
}

// A repository a build file declares is asked AFTER the configured ones,
// and only when collection.declared_repository_fetch is on.
func TestADeclaredRepositoryIsAskedAfterTheConfiguredOnes(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-order")
	central := newPomRepo(t, map[string]string{"org.remote:on-central:1.0": remoteParentPom})
	declared := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, central.srv.URL)
	withDeclaredFetch(t, true)

	if _, outcome := src.RemotePom("org.remote", "on-central", "1.0", []string{declared.srv.URL}); outcome != manifest.RemoteFound || declared.asked.Load() != 0 {
		t.Errorf("a pom the configured repository has: outcome %v, declared asked %d times; want found and the declared one not asked", outcome, declared.asked.Load())
	}
	pom, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{declared.srv.URL})
	if outcome != manifest.RemoteFound || pom.Properties["lib.version"] != "7.7.7" {
		t.Errorf("a pom only the declared repository has: %+v, outcome %v", pom, outcome)
	}
	// A declared address that is also a configured one is asked once.
	before := central.asked.Load()
	src.RemotePom("org.remote", "nowhere", "1.0", []string{central.srv.URL})
	if central.asked.Load()-before != 1 {
		t.Errorf("a repository both configured and declared was asked %d times", central.asked.Load()-before)
	}
}

// The knob gates fetching, not reading. Off: a declared host is not
// contacted, and a pom it already served is still used, so switching the
// knob re-keys nothing.
func TestTheDeclaredFetchKnobGatesFetchingNotReading(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-knob")
	declared := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t)
	withDeclaredFetch(t, false)

	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{declared.srv.URL}); outcome != manifest.RemoteNotFound || declared.asked.Load() != 0 {
		t.Errorf("knob off, nothing stored: outcome %v, asked %d; want not found and no request", outcome, declared.asked.Load())
	}
	SetDeclaredRepositoryFetch(true)
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{declared.srv.URL}); outcome != manifest.RemoteFound {
		t.Fatalf("knob on: outcome %v", outcome)
	}
	SetDeclaredRepositoryFetch(false)
	asked := declared.asked.Load()
	pom, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{declared.srv.URL})
	if outcome != manifest.RemoteFound || pom == nil || declared.asked.Load() != asked {
		t.Errorf("knob off again, a pom stored: outcome %v, requests %d; want the stored pom and no request", outcome, declared.asked.Load()-asked)
	}
}

// A declared repository that refuses (401, 403), or that the outbound rule
// refuses, is a terminal reason and not a failed lookup; and a redirect off
// https is not followed.
func TestADeclaredRepositoryThatRefusesIsRefused(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-refused")
	withMavenRepositories(t)
	withDeclaredFetch(t, true)
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer forbidden.Close()
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{forbidden.URL}); outcome != manifest.RemoteRefused {
		t.Errorf("a 403: outcome %v, want refused", outcome)
	}
	// A host on a private network (the test policy resolves *.internal so).
	if _, outcome := src.RemotePom("org.remote", "other", "1.0", []string{"http://nexus.internal/maven2"}); outcome != manifest.RemoteRefused {
		t.Errorf("a declared host at a private address: outcome %v, want refused", outcome)
	}
	// A redirect from a declared host to plain http is not followed.
	target := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.srv.URL+r.URL.Path, http.StatusFound)
	}))
	defer redirecting.Close()
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{redirecting.URL}); outcome == manifest.RemoteFound || target.asked.Load() != 0 {
		t.Errorf("a redirect off https from a declared host: outcome %v, target asked %d times; want it not followed", outcome, target.asked.Load())
	}
}

// The source acts only on an https declared address, whatever it is handed.
func TestTheSourceActsOnlyOnHTTPSDeclaredAddresses(t *testing.T) {
	if !mavenDeclaredSchemeOK("https://repo.example/m2") || mavenDeclaredSchemeOK("http://repo.example/m2") || mavenDeclaredSchemeOK("file:///m2") {
		t.Error("the source's own scheme check must accept https and nothing else")
	}
	_, src := pomSourceFixture(t, "declared-scheme")
	plain := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t)
	old := declaredRepositoryFetch.Load()
	SetDeclaredRepositoryFetch(true)
	t.Cleanup(func() { SetDeclaredRepositoryFetch(old) })
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{plain.srv.URL}); outcome != manifest.RemoteNotFound || plain.asked.Load() != 0 {
		t.Errorf("an http declared address: outcome %v, asked %d; want it not asked", outcome, plain.asked.Load())
	}
}

// What a repository's poms declare is recorded at analysis, fetched from or
// not, and replaced whole.
func TestDeclaredRepositoriesAreRecordedAtAnalysis(t *testing.T) {
	f := lockFixture(t, "declared-recorded")
	t.Cleanup(resetRegistryBreakers)
	withMavenRepositories(t)
	ctx := context.Background()
	f.write(t, "pom.xml", `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
	<repositories>
	  <repository><url>https://Nexus.Corp.Example/repository/public/</url></repository>
	  <repository><url>https://user:secret@private.example/m2</url></repository>
	</repositories></project>`)
	f.analyse(t)
	got, err := f.store.GetRepoDeclaredRepositories(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.DeclaredRepository{
		{ManifestPath: "pom.xml", URL: "https://Nexus.Corp.Example/repository/public", Host: "nexus.corp.example", Kind: manifest.RepositoryForDependencies},
		{ManifestPath: "pom.xml", URL: "https://private.example/m2", Host: "private.example", Kind: manifest.RepositoryForDependencies, NotFetched: manifest.NotFetchedCredentials},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("recorded = %+v\nwant %+v", got, want)
	}
	// Replaced whole: the declaration removed, the record goes.
	f.write(t, "pom.xml", `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version></project>`)
	f.analyse(t)
	if got, _ := f.store.GetRepoDeclaredRepositories(ctx, f.repoID); len(got) != 0 {
		t.Errorf("after the declarations were removed: %+v", got)
	}
}

// What depends on a host is recorded (summary/70, 4.5, D14), end to end:
// the row carries the hosts its version came through, the tree's hosts are
// in the coverage record, and the host is in the registry with the time it
// was first fetched from, which later answers do not move.
func TestTheHostsAVersionCameThroughAreStored(t *testing.T) {
	f := lockFixture(t, "version-hosts")
	ctx := context.Background()
	clean := func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.package_repository_hosts WHERE host LIKE '127.0.0.1:%'`)
	}
	t.Cleanup(clean)
	clean()
	t.Cleanup(resetRegistryBreakers)
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	host := hostOf(repo.srv.URL)
	f.write(t, "pom.xml", `<project><parent><groupId>org.remote</groupId><artifactId>parent</artifactId><version>5.0</version><relativePath/></parent>
	<artifactId>app</artifactId><dependencies>
	  <dependency><groupId>com.acme</groupId><artifactId>lib</artifactId></dependency>
	  <dependency><groupId>com.acme</groupId><artifactId>explicit</artifactId><version>1.0</version></dependency>
	</dependencies></project>`)
	f.analyse(t)

	hosts := map[string][]string{}
	rows, err := f.store.Pool().Query(ctx, `SELECT name, version_hosts FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		var h []string
		if err := rows.Scan(&n, &h); err != nil {
			t.Fatal(err)
		}
		hosts[n] = h
	}
	rows.Close()
	if got := hosts["com.acme:lib"]; len(got) != 1 || got[0] != host {
		t.Errorf("the managed row's hosts = %v, want [%s]", got, host)
	}
	if got := hosts["com.acme:explicit"]; got != nil {
		t.Errorf("a row with its own version carries hosts %v; want NULL", got)
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cov.Inventory.MavenTreeHosts) != 1 || cov.Inventory.MavenTreeHosts[0].Root != "pom.xml" || len(cov.Inventory.MavenTreeHosts[0].Hosts) != 1 || cov.Inventory.MavenTreeHosts[0].Hosts[0] != host {
		t.Errorf("the coverage record's tree hosts = %+v", cov.Inventory.MavenTreeHosts)
	}

	var first, last time.Time
	read := func() {
		t.Helper()
		if err := f.store.Pool().QueryRow(ctx, `SELECT first_fetched_at, last_answered_at FROM aveloxis_ops.package_repository_hosts WHERE host = $1`, host).Scan(&first, &last); err != nil {
			t.Fatalf("the host is not in the registry: %v", err)
		}
	}
	read()
	firstSeen := first
	// A later analysis that is served from the store asks the host nothing,
	// and so records nothing: the registry says when the host ANSWERED.
	f.analyse(t)
	read()
	if !first.Equal(firstSeen) {
		t.Errorf("first_fetched_at moved from %v to %v", firstSeen, first)
	}
	// And the row still carries the host when the pom came from the store.
	var h []string
	if err := f.store.Pool().QueryRow(ctx, `SELECT version_hosts FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = 'com.acme:lib'`, f.repoID).Scan(&h); err != nil || len(h) != 1 || h[0] != host {
		t.Errorf("served from the store, the row's hosts = %v (err %v)", h, err)
	}
}

func distrust(t *testing.T, f *wholeReplaceFixture, repoURL string) {
	t.Helper()
	host := hostOf(repoURL)
	if _, err := f.store.DistrustRepositoryHost(context.Background(), host, "test", ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = f.store.RestoreRepositoryHost(context.Background(), host) })
}

// A distrusted host is neither fetched from nor served from (summary/70,
// 4.5, D14): the decision is asked before the store is read, so a pom the
// host served earlier is not used either.
func TestADistrustedHostIsNeitherFetchedFromNorServedFrom(t *testing.T) {
	f, src := pomSourceFixture(t, "distrust-source")
	bad := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	good := newPomRepo(t, map[string]string{"org.remote:parent:5.0": strings.Replace(remoteParentPom, "7.7.7", "8.8.8", 1)})
	withMavenRepositories(t, bad.srv.URL, good.srv.URL)

	// Before the distrust: the first repository answers, and is stored.
	if pom, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteFound || pom.Properties["lib.version"] != "7.7.7" {
		t.Fatalf("before: %+v, %v", pom, outcome)
	}
	distrust(t, f, bad.srv.URL)
	// The list is read once per analysis: a source made before the distrust
	// still uses the host (the stated bound), a new one does not.
	fresh, err := newMavenPomSource(context.Background(), f.store, f.ac.logger)
	if err != nil {
		t.Fatal(err)
	}
	asked := bad.asked.Load()
	pom, outcome := fresh.RemotePom("org.remote", "parent", "5.0", nil)
	if outcome != manifest.RemoteFound || pom.Properties["lib.version"] != "8.8.8" {
		t.Errorf("after the distrust: %+v, %v; want the NEXT repository's pom, not the stored one from the distrusted host", pom, outcome)
	}
	if bad.asked.Load() != asked {
		t.Error("a distrusted host was asked")
	}
	// With no other repository, the coordinate is refused: not "not found".
	withMavenRepositories(t, bad.srv.URL)
	if _, outcome := fresh.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteRefused {
		t.Errorf("only a distrusted repository: outcome %v, want refused", outcome)
	}
}

// Distrust a configured host, then run an analysis (the plan's end-to-end
// test). A Maven dependency whose only repository is distrusted keeps its
// row and says inherited_repository_refused; it is never "not on a
// registry", which would be false. And an answer cached before the distrust
// is not served after it.
func TestDistrustingAConfiguredRepositoryThenAnalysing(t *testing.T) {
	f := lockFixture(t, "distrust-analysis")
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	})
	t.Cleanup(resetRegistryBreakers)
	only, _ := mavenRepo(t, 0, "lib")
	withMavenRepositories(t, only.URL)
	f.write(t, "pom.xml", `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>lib</artifactId><version>1.0</version></dependency></dependencies></project>`)
	status := func() (libyearStatus, reason string) {
		t.Helper()
		var s, r *string
		if err := f.store.Pool().QueryRow(ctx, `SELECT libyear_status, version_reason FROM aveloxis_data.repo_deps_libyear
			WHERE repo_id = $1 AND name = 'com.acme:lib'`, f.repoID).Scan(&s, &r); err != nil {
			t.Fatalf("the dependency lost its row: %v", err)
		}
		if s != nil {
			libyearStatus = *s
		}
		if r != nil {
			reason = *r
		}
		return
	}
	f.analyse(t)
	if s, r := status(); s != model.LibyearResolved || r != "" {
		t.Fatalf("before the distrust: status %q reason %q; want resolved", s, r)
	}
	distrust(t, f, only.URL)
	f.analyse(t)
	s, r := status()
	if r != model.VersionReasonInheritedRepositoryRefused {
		t.Errorf("after the distrust: reason %q, want %q", r, model.VersionReasonInheritedRepositoryRefused)
	}
	if s == model.LibyearNotOnRegistry || s == model.LibyearResolved {
		t.Errorf("after the distrust: status %q; it was not asked, so it is neither 'not on a registry' nor resolved (a cached answer from the distrusted host must not be served)", s)
	}
	// Restored, it resolves again at the next analysis.
	if _, err := f.store.RestoreRepositoryHost(ctx, hostOf(only.URL)); err != nil {
		t.Fatal(err)
	}
	f.analyse(t)
	if s, r := status(); s != model.LibyearResolved || r != "" {
		t.Errorf("after the restore: status %q reason %q; want resolved again", s, r)
	}
}

// A purge, then an analysis and a scan (summary/70, 4.5, D14; the plan's
// test): the findings at a version that came through the distrusted host
// close as version_lost and none as a fix; the tree whose parent can no
// longer be obtained is no longer whole, so its managed entry is a row again
// and the finding on it, closed as never_applied, reopens. The preview said
// so before anything changed.
func TestAPurgeThenAnAnalysisClosesAsVersionLostAndReopensManagedEntries(t *testing.T) {
	f := lockFixture(t, "purge-then-analyse")
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, f.repoID)
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	})
	t.Cleanup(resetRegistryBreakers)
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	host := hostOf(repo.srv.URL)
	t.Cleanup(func() { _, _ = f.store.RestoreRepositoryHost(context.Background(), host) })
	f.write(t, "pom.xml", `<project><parent><groupId>org.remote</groupId><artifactId>parent</artifactId><version>5.0</version></parent><artifactId>app</artifactId>
	<dependencyManagement><dependencies>
	  <dependency><groupId>com.acme</groupId><artifactId>unused</artifactId><version>3.0</version></dependency>
	</dependencies></dependencyManagement>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>lib</artifactId></dependency></dependencies></project>`)

	// OSV reports one advisory for each of the two packages, at any version.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/querybatch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Queries []struct {
				Package struct {
					Purl string `json:"purl"`
				} `json:"package"`
			} `json:"queries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, len(req.Queries))
		for i, q := range req.Queries {
			stubs := []map[string]string{}
			switch {
			case strings.Contains(q.Package.Purl, "com.acme/lib@"):
				stubs = append(stubs, map[string]string{"id": "ADV-LIB", "modified": "2026-01-01T00:00:00Z"})
			case strings.Contains(q.Package.Purl, "com.acme/unused@"):
				stubs = append(stubs, map[string]string{"id": "ADV-UNUSED", "modified": "2026-01-01T00:00:00Z"})
			}
			results[i] = map[string]any{"vulns": stubs}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	})
	mux.HandleFunc("GET /v1/vulns/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"id":"`+r.PathValue("id")+`","summary":"bad","database_specific":{"severity":"HIGH"}}`)
	})
	osv := httptest.NewServer(mux)
	t.Cleanup(osv.Close)
	oldBatch, oldVulns := osvBatchURL, osvVulnURLBase
	osvBatchURL, osvVulnURLBase = osv.URL+"/v1/querybatch", osv.URL+"/v1/vulns/"
	t.Cleanup(func() { osvBatchURL, osvVulnURLBase = oldBatch, oldVulns })

	// A finding an earlier release recorded on the managed entry.
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "ADV-UNUSED", PackageName: "com.acme:unused", PackagePurl: "pkg:maven/com.acme/unused@3.0", Ecosystem: "maven", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	finding := func(id string) (open bool, reason string) {
		t.Helper()
		var r *string
		if err := f.store.Pool().QueryRow(ctx, `SELECT resolved_at IS NULL, closed_reason FROM aveloxis_data.repo_deps_vulnerabilities
			WHERE repo_id = $1 AND vuln_id = $2 ORDER BY resolved_at IS NULL DESC LIMIT 1`, f.repoID, id).Scan(&open, &r); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if r != nil {
			reason = *r
		}
		return
	}
	scan := func() {
		t.Helper()
		f.analyse(t)
		if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, f.ac.logger, nil, true); err != nil {
			t.Fatal(err)
		}
	}

	scan()
	if rows := inventoryRows(t, f); len(rows) != 1 || rows["pom.xml:com.acme:lib"].Version != "7.7.7" {
		t.Fatalf("before the distrust the inventory = %+v; want the one dependency at the remote parent's version", rows)
	}
	if open, _ := finding("ADV-LIB"); !open {
		t.Fatal("before the distrust the dependency's finding is not open")
	}
	if open, reason := finding("ADV-UNUSED"); open || reason != model.ClosedNeverApplied {
		t.Fatalf("before the distrust the managed entry's finding: open %v reason %q; want closed as never_applied", open, reason)
	}

	// What the operator is shown.
	preview, err := f.store.PreviewRepositoryHostDistrust(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Findings != 1 || preview.Trees != 1 || preview.Repositories != 1 || preview.StoredResults < 1 {
		t.Fatalf("preview = %+v; want 1 finding to close, 1 tree no longer whole, 1 repository, and the stored parent", preview)
	}
	if _, changed, err := f.store.DistrustAndPurgeRepositoryHost(ctx, host, "test", ""); err != nil || !changed {
		t.Fatalf("distrust: changed %v err %v", changed, err)
	}
	asked := repo.asked.Load()
	scan()
	if n := repo.asked.Load() - asked; n != 0 {
		t.Errorf("the distrusted host was asked %d times after the distrust", n)
	}

	// The dependency's version is no longer known, and its finding closed
	// for that reason: not as a fix.
	rows := inventoryRows(t, f)
	if r := rows["pom.xml:com.acme:lib"]; r.Version != "" {
		t.Errorf("after the purge the dependency still has version %q", r.Version)
	}
	if open, reason := finding("ADV-LIB"); open || reason != model.ClosedVersionLost {
		t.Errorf("after the purge the dependency's finding: open %v reason %q; want closed as %s", open, reason, model.ClosedVersionLost)
	}
	var closedAsFix int
	if err := f.store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND resolved_at IS NOT NULL AND closed_reason IS DISTINCT FROM $2 AND closed_reason IS DISTINCT FROM $3`,
		f.repoID, model.ClosedVersionLost, model.ClosedNeverApplied).Scan(&closedAsFix); err != nil {
		t.Fatal(err)
	}
	if closedAsFix != 0 {
		t.Errorf("%d findings closed as a fix (or with no reason) by the distrust; want none", closedAsFix)
	}
	// The tree is no longer whole: the managed entry is a row again and its
	// finding is open again, as the preview's tree count said.
	if _, ok := rows["pom.xml:com.acme:unused"]; !ok {
		t.Errorf("the tree's managed entry did not return to the inventory: %+v", rows)
	}
	if open, _ := finding("ADV-UNUSED"); !open {
		t.Error("the managed entry's never_applied finding did not reopen")
	}
}

// A Gradle platform, end to end (summary/70, 4.6): the BOM is fetched like
// a pom's, the version-less dependency takes its version and records the
// host, and the platform leaves the inventory with a note.
func TestAGradlePlatformSuppliesVersionsThroughTheResolver(t *testing.T) {
	f := lockFixture(t, "gradle-platform")
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	})
	t.Cleanup(resetRegistryBreakers)
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	f.write(t, "build.gradle", `
dependencies {
    implementation platform('org.remote:parent:5.0')
    implementation 'com.acme:lib'
    implementation 'com.acme:unmanaged'
}
`)
	f.analyse(t)
	rows := inventoryRows(t, f)
	if r := rows["build.gradle:com.acme:lib"]; r.Version != "7.7.7" {
		t.Errorf("inventory = %+v; want the dependency at the platform's version", rows)
	}
	if _, kept := rows["build.gradle:org.remote:parent"]; kept {
		t.Errorf("the applied platform is still a row: %+v", rows)
	}
	if _, ok := rows["build.gradle:com.acme:unmanaged"]; !ok {
		t.Errorf("a dependency the platform does not manage lost its row: %+v", rows)
	}
	var source *string
	var hosts []string
	if err := f.store.Pool().QueryRow(ctx, `SELECT version_source, version_hosts FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1 AND name = 'com.acme:lib'`, f.repoID).Scan(&source, &hosts); err != nil {
		t.Fatal(err)
	}
	if source == nil || *source != model.VersionSourceManaged || len(hosts) != 1 || hosts[0] != hostOf(repo.srv.URL) {
		t.Errorf("the row's source = %v, hosts = %v; want managed, through the repository's host", source, hosts)
	}
	notes, err := f.store.GetRepoInventoryNotes(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || notes[0].Kind != model.NoRowManagedUnused || notes[0].Purl != "pkg:maven/org.remote/parent@5.0" || notes[0].ManifestPath != "build.gradle" {
		t.Errorf("notes = %+v; want one managed_unused note for the platform", notes)
	}

	// The repository down and nothing stored: the dependency says why it
	// has no version, and the platform is a row again.
	_, _ = f.store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url = $1`, repo.srv.URL)
	repo.down.Store(true)
	resetRegistryBreakers()
	f.analyse(t)
	rows = inventoryRows(t, f)
	if _, back := rows["build.gradle:org.remote:parent"]; !back {
		t.Errorf("a platform that could not be obtained is not a row: %+v", rows)
	}
	var reason *string
	if err := f.store.Pool().QueryRow(ctx, `SELECT version_reason FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1 AND name = 'com.acme:lib'`, f.repoID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == nil || *reason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("with the repository down the dependency's reason = %v; want %s", reason, model.VersionReasonInheritedLookupFailed)
	}
}

// A Gradle build's repositories are recorded at analysis like a pom's
// (summary/70, 4.6), the settings file's included, and replaced whole.
func TestGradleRepositoriesAreRecordedAtAnalysis(t *testing.T) {
	f := lockFixture(t, "gradle-declared-recorded")
	t.Cleanup(resetRegistryBreakers)
	withMavenRepositories(t)
	ctx := context.Background()
	f.write(t, "settings.gradle", "dependencyResolutionManagement { repositories { google() } }\n")
	f.write(t, "build.gradle", `
repositories {
    maven { url 'https://Nexus.Corp.Example/repository/public/' }
    mavenLocal()
    jcenter()
}
dependencies { implementation 'g:a:1' }
`)
	f.analyse(t)
	got, err := f.store.GetRepoDeclaredRepositories(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.DeclaredRepository{
		{ManifestPath: "build.gradle", URL: "https://jcenter.bintray.com", Host: "jcenter.bintray.com", Kind: manifest.RepositoryForDependencies, NotFetched: manifest.NotFetchedShutDown},
		{ManifestPath: "build.gradle", URL: "https://Nexus.Corp.Example/repository/public", Host: "nexus.corp.example", Kind: manifest.RepositoryForDependencies},
		{ManifestPath: "build.gradle", URL: "mavenLocal", Kind: manifest.RepositoryForDependencies, NotFetched: manifest.NotFetchedLocal},
		{ManifestPath: "settings.gradle", URL: "https://dl.google.com/dl/android/maven2", Host: "dl.google.com", Kind: manifest.RepositoryForDependencies},
	}
	if len(got) != len(want) {
		t.Fatalf("recorded = %+v\nwant %+v", got, want)
	}
	// By address: the store's order follows the database's collation.
	byURL := map[string]model.DeclaredRepository{}
	for _, d := range got {
		byURL[d.URL] = d
	}
	for _, w := range want {
		if byURL[w.URL] != w {
			t.Errorf("recorded for %s = %+v\nwant %+v", w.URL, byURL[w.URL], w)
		}
	}
	f.write(t, "build.gradle", "dependencies { implementation 'g:a:1' }\n")
	f.write(t, "settings.gradle", "rootProject.name = 'x'\n")
	f.analyse(t)
	if got, _ := f.store.GetRepoDeclaredRepositories(ctx, f.repoID); len(got) != 0 {
		t.Errorf("after the declarations were removed: %+v", got)
	}
}

// Gradle's precedence (summary/70, 4.6): the lockfile's version is the one
// in use, above a version written through a variable and above a
// platform's. Nothing downgrades a locked version.
func TestAGradleLockfileVersionWinsOverAVariableAndAPlatform(t *testing.T) {
	f := lockFixture(t, "gradle-lock-precedence")
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url LIKE 'http://127.0.0.1:%'`)
	})
	t.Cleanup(resetRegistryBreakers)
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	f.write(t, "build.gradle", `
ext.varVersion = '1.0.0'
dependencies {
    implementation platform('org.remote:parent:5.0')
    implementation 'com.acme:lib'
    implementation "com.acme:by-variable:$varVersion"
    implementation 'com.acme:not-locked:3.3.3'
}
`)
	f.write(t, "gradle.lockfile", "# This is a Gradle generated file for dependency locking.\ncom.acme:lib:8.8.8=compileClasspath,runtimeClasspath\ncom.acme:by-variable:2.2.2=compileClasspath,runtimeClasspath\nempty=\n")
	f.analyse(t)
	got := map[string][2]string{}
	rows, err := f.store.Pool().Query(ctx, `SELECT name, COALESCE(current_version, ''), COALESCE(version_source, '') FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, version, source string
		if err := rows.Scan(&name, &version, &source); err != nil {
			t.Fatal(err)
		}
		got[name] = [2]string{version, source}
	}
	if g := got["com.acme:lib"]; g[0] != "8.8.8" || g[1] != versionuse.SourceLocked {
		t.Errorf("a platform-managed dependency that is locked = %v; want the locked 8.8.8 (the platform says 7.7.7)", g)
	}
	if g := got["com.acme:by-variable"]; g[0] != "2.2.2" || g[1] != versionuse.SourceLocked {
		t.Errorf("a variable's version that is locked = %v; want the locked 2.2.2 (the variable says 1.0.0)", g)
	}
	if g := got["com.acme:not-locked"]; g[0] != "3.3.3" || g[1] == versionuse.SourceLocked {
		t.Errorf("a dependency the lockfile does not name = %v; want its written 3.3.3", g)
	}
}
