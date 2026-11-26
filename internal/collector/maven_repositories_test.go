// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// The configured Maven repositories (summary/70, 4.5). Maven Central is not
// the only public repository: a Jenkins plugin's artifacts live on
// repo.jenkins-ci.org. Asked of Central alone, such a dependency was stored
// as "not on its registry". The Maven resolver asks the configured list, in
// order.

const mavenMetadataDoc = `<metadata><versioning><release>2.0</release><versions><version>1.0</version><version>2.0</version></versions></versioning></metadata>`

// mavenRepo is a repository stand-in that holds the artifacts named, answers
// 404 for the rest, and counts what it was asked.
func mavenRepo(t *testing.T, status int, holds ...string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var asked atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		for _, h := range holds {
			if strings.Contains(r.URL.Path, "/"+h+"/") {
				if strings.HasSuffix(r.URL.Path, "maven-metadata.xml") {
					_, _ = io.WriteString(w, mavenMetadataDoc)
					return
				}
				w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &asked
}

func withMavenRepositories(t *testing.T, bases ...string) {
	t.Helper()
	old := mavenRepositories.Load()
	SetMavenRepositories(bases)
	t.Cleanup(func() { mavenRepositories.Store(old) })
	t.Cleanup(resetRegistryBreakers)
}

func TestTheMavenResolverAsksTheConfiguredRepositoriesInOrder(t *testing.T) {
	central, centralAsked := mavenRepo(t, 0, "on-central")
	jenkins, jenkinsAsked := mavenRepo(t, 0, "on-jenkins")
	withMavenRepositories(t, central.URL, jenkins.URL)
	ctx := context.Background()
	dep := func(artifact string) model.LibyearDep {
		return model.LibyearDep{Name: "org.example:" + artifact, Manager: "maven", Version: "1.0"}
	}

	// On the first repository: the second is not asked.
	row, err := resolveMavenLibyear(ctx, dep("on-central"))
	if err != nil || row.LatestVersion != "2.0" {
		t.Fatalf("an artifact on the first repository: %+v, %v", row, err)
	}
	if jenkinsAsked.Load() != 0 {
		t.Error("the second repository was asked for an artifact the first holds")
	}
	// On the second only: found there, where it used to be "not found".
	row, err = resolveMavenLibyear(ctx, dep("on-jenkins"))
	if err != nil || row.LatestVersion != "2.0" || row.CurrentReleaseDate == "" {
		t.Errorf("an artifact on the second repository only: %+v, %v", row, err)
	}
	// On neither: a definitive miss, after BOTH were asked.
	before := centralAsked.Load() + jenkinsAsked.Load()
	_, err = resolveMavenLibyear(ctx, dep("nowhere"))
	if !isDefinitiveRegistryMiss(err) {
		t.Errorf("an artifact on no repository: err = %v, want a definitive miss", err)
	}
	if centralAsked.Load()+jenkinsAsked.Load()-before != 2 {
		t.Error("a miss must have asked every repository once")
	}
}

// "Not on any repository" needs every repository to have ANSWERED that. A
// miss on one and a failure on another is a failed lookup: the artifact may
// be on the one that did not answer (SR-5).
func TestAMavenMissIsDefinitiveOnlyWhenEveryRepositoryAnswered(t *testing.T) {
	missing, _ := mavenRepo(t, 0)
	failing, _ := mavenRepo(t, http.StatusBadGateway)
	dep := model.LibyearDep{Name: "org.example:somewhere", Manager: "maven", Version: "1.0"}
	for name, order := range map[string][]string{
		"the miss first, then the failure": {missing.URL, failing.URL},
		"the failure first, then the miss": {failing.URL, missing.URL},
	} {
		withMavenRepositories(t, order...)
		_, err := resolveMavenLibyear(context.Background(), dep)
		if err == nil || isDefinitiveRegistryMiss(err) {
			t.Errorf("%s: err = %v; want a failed lookup, not 'not found'", name, err)
		}
	}
	// A repository that fails does not stop a later one from answering.
	holding, _ := mavenRepo(t, 0, "somewhere")
	withMavenRepositories(t, failing.URL, holding.URL)
	if row, err := resolveMavenLibyear(context.Background(), dep); err != nil || row.LatestVersion != "2.0" {
		t.Errorf("the first repository failing and the second holding it: %+v, %v", row, err)
	}
}

// With no repository configured there is none to ask: that is not "not on a
// registry".
func TestNoMavenRepositoryConfiguredCannotBeAsked(t *testing.T) {
	withMavenRepositories(t)
	_, err := resolveMavenLibyear(context.Background(), model.LibyearDep{Name: "org.example:x", Manager: "maven", Version: "1.0"})
	if !errors.Is(err, errRegistryCannotBeAsked) {
		t.Errorf("err = %v, want 'cannot be asked'", err)
	}
}

// A configured repository's host is trusted the way a fixed registry's is:
// the operator named it.
func TestConfiguredMavenRepositoriesAreTrustedByName(t *testing.T) {
	withMavenRepositories(t, "https://repo.jenkins-ci.org/public", "https://nexus.corp.example/repository/maven-public")
	private := func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("10.9.8.7")}, nil
	}
	p := registryPolicy().WithResolver(private)
	for _, host := range []string{"repo.jenkins-ci.org", "nexus.corp.example", "repo1.maven.org"} {
		if err := p.RefuseHost(context.Background(), host); err != nil {
			t.Errorf("%s: %v", host, err)
		}
	}
	if err := p.RefuseHost(context.Background(), "unlisted.example"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Errorf("an unlisted host: err = %v", err)
	}
	// The trust follows the list: a host taken off it is no longer trusted.
	SetMavenRepositories([]string{"https://repo1.maven.org/maven2"})
	if err := registryPolicy().WithResolver(private).RefuseHost(context.Background(), "nexus.corp.example"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Errorf("a host removed from the list is still trusted: err = %v", err)
	}
}
