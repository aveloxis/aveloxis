// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"reflect"
	"testing"
)

// What depends on a host is recorded, not guessed (summary/70, 4.5, D14). A
// resolution is a chain: a parent from one host, a BOM from another. Each
// dependency whose version came through fetched poms carries the hosts its
// chain used, so that distrusting a host can say exactly what it touches.

func hostedSpringPoms() map[string]*RemotePom {
	poms := springPoms()
	poms["org.springframework.boot:spring-boot-starter-parent:3.2.0"].ServedBy = "repo1.maven.org"
	poms["org.springframework.boot:spring-boot-dependencies:3.2.0"].ServedBy = "repo1.maven.org"
	poms["io.netty:netty-bom:4.1.101.Final"].ServedBy = "repo.jenkins-ci.org"
	return poms
}

func TestAResolvedVersionCarriesTheHostsItCameThrough(t *testing.T) {
	deps := remoteDeps(t, &fakePoms{poms: hostedSpringPoms()}, map[string]string{"pom.xml": springChild}, "pom.xml")
	// Managed by the remote parent chain alone.
	if got := deps["com.fasterxml.jackson.core:jackson-databind"].VersionHosts; !reflect.DeepEqual(got, []string{"repo1.maven.org"}) {
		t.Errorf("jackson's hosts = %v, want the parent chain's", got)
	}
	// Managed by a BOM from another host, reached through the parent chain:
	// both, sorted.
	if got := deps["io.netty:netty-all"].VersionHosts; !reflect.DeepEqual(got, []string{"repo.jenkins-ci.org", "repo1.maven.org"}) {
		t.Errorf("netty's hosts = %v, want the BOM's host and the chain's", got)
	}
	// A dependency with no version found depends on no host for a version.
	if got := deps["com.acme:unmanaged"].VersionHosts; len(got) != 0 {
		t.Errorf("an unmanaged dependency carries hosts %v", got)
	}
}

// A version that comes from the repository's own poms depends on no host.
func TestATreeOnlyVersionCarriesNoHost(t *testing.T) {
	root := writeTree(t, map[string]string{"pom.xml": parentPom, "app/pom.xml": childPom, "bom/pom.xml": bomPom})
	for name, d := range pomDeps(t, root, "app/pom.xml") {
		if len(d.VersionHosts) != 0 {
			t.Errorf("%s carries hosts %v; its version came from the tree", name, d.VersionHosts)
		}
	}
}

// A tree can depend on a host with no row saying so: the parent was fetched,
// every version in the tree is explicit, and the managed entries are unused
// and so not inventoried. The tree's hosts are recorded for the tree.
func TestATreesHostsAreRecordedEvenWithNoResolvedRow(t *testing.T) {
	poms := map[string]*RemotePom{"far:away:1": {Group: "far", Artifact: "away", Version: "1", ServedBy: "nexus.corp.example",
		Managed: []RemoteManaged{{Group: "x", Artifact: "unused", Version: "9"}}}}
	child := `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>
	<dependencyManagement><dependencies><dependency><groupId>com.acme</groupId><artifactId>held</artifactId><version>3.0</version></dependency></dependencies></dependencyManagement>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>explicit</artifactId><version>1.2.3</version></dependency></dependencies></project>`
	_, res := readTree(t, &fakePoms{poms: poms}, map[string]string{"svc/pom.xml": child})
	for _, d := range res.Kept {
		if len(d.VersionHosts) != 0 {
			t.Fatalf("%s carries hosts; the fixture needs a tree with no resolved row", d.Name)
		}
	}
	want := []MavenTreeHosts{{Root: "svc/pom.xml", Hosts: []string{"nexus.corp.example"}}}
	if !reflect.DeepEqual(res.TreeHosts, want) {
		t.Errorf("tree hosts = %+v, want %+v", res.TreeHosts, want)
	}
}

// A tree with NO managed entry of its own still has hosts to record. (The
// settlement once returned early when it found no managed entry, and the
// tree's hosts were never computed; the collector's end-to-end test caught
// it, this one pins it where it lives.)
func TestATreeWithNoManagedEntryStillRecordsItsHosts(t *testing.T) {
	poms := map[string]*RemotePom{"far:away:1": {Group: "far", Artifact: "away", Version: "1", ServedBy: "nexus.corp.example"}}
	child := `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>1</version><relativePath/></parent><artifactId>a</artifactId>
	<dependencies><dependency><groupId>com.acme</groupId><artifactId>explicit</artifactId><version>1.2.3</version></dependency></dependencies></project>`
	_, res := readTree(t, &fakePoms{poms: poms}, map[string]string{"pom.xml": child})
	if len(res.TreeHosts) != 1 || res.TreeHosts[0].Root != "pom.xml" || !reflect.DeepEqual(res.TreeHosts[0].Hosts, []string{"nexus.corp.example"}) {
		t.Errorf("tree hosts = %+v", res.TreeHosts)
	}
}
