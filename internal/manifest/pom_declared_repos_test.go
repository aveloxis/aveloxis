// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"reflect"
	"testing"
)

// The repositories a build's own files declare (summary/70, 4.5). A pom can
// name the repositories its dependencies live on. They are recorded, and a
// parent or BOM that the configured repositories do not have is asked of
// them.

const pomWithRepos = `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>
<parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent>
<repositories>
  <repository><id>corp</id><url> https://nexus.corp.example/repository/public/ </url></repository>
  <repository><id>plain</id><url>http://insecure.example/maven2</url></repository>
  <repository><id>creds</id><url>https://user:secret@private.example/maven2</url></repository>
  <repository><id>prop</id><url>${repo.url}/maven2</url></repository>
  <repository><id>local</id><url>file:///home/me/.m2/repository</url></repository>
  <repository><id>dup</id><url>https://nexus.corp.example/repository/public</url></repository>
</repositories>
<pluginRepositories><pluginRepository><id>plugins</id><url>https://plugins.example/m2</url></pluginRepository></pluginRepositories>
<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`

func TestDeclaredRepositoriesAreRecordedWithWhetherTheyMayBeFetched(t *testing.T) {
	root := writeTree(t, map[string]string{"pom.xml": pomWithRepos})
	w := NewWalk(root, nil)
	w.Read(filepath.Join(root, "pom.xml"), Options{})
	got := map[string]DeclaredRepository{}
	for _, d := range w.DeclaredRepositories() {
		got[d.URL] = d
	}
	for url, want := range map[string]DeclaredRepository{
		// Fetchable: https, no credentials, readable. The trailing slash and
		// the spaces are not part of it, and the duplicate is one entry.
		"https://nexus.corp.example/repository/public": {ManifestPath: "pom.xml", Kind: RepositoryForDependencies},
		// Recorded with the reason, and never fetched.
		"http://insecure.example/maven2": {ManifestPath: "pom.xml", Kind: RepositoryForDependencies, NotFetched: NotFetchedNotHTTPS},
		"https://private.example/maven2": {ManifestPath: "pom.xml", Kind: RepositoryForDependencies, NotFetched: NotFetchedCredentials},
		"${repo.url}/maven2":             {ManifestPath: "pom.xml", Kind: RepositoryForDependencies, NotFetched: NotFetchedUnreadable},
		"file:///home/me/.m2/repository": {ManifestPath: "pom.xml", Kind: RepositoryForDependencies, NotFetched: NotFetchedNotHTTPS},
		// A plugin repository is recorded and not used for dependencies.
		"https://plugins.example/m2": {ManifestPath: "pom.xml", Kind: RepositoryForPlugins, NotFetched: NotFetchedPluginsOnly},
	} {
		g, ok := got[url]
		if !ok {
			t.Errorf("%s: not recorded (recorded: %v)", url, got)
			continue
		}
		g.URL = ""
		if g != want {
			t.Errorf("%s: %+v, want %+v", url, g, want)
		}
	}
	if len(got) != 6 {
		t.Errorf("%d declared repositories, want 6: %v", len(got), got)
	}
	// The credentials are never stored.
	for url := range got {
		if url != "${repo.url}/maven2" && (reflect.ValueOf(url).String() == "" || containsAny(url, "secret", "user:")) {
			t.Errorf("a recorded URL carries credentials: %q", url)
		}
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// declaredRecorder is a PomSource that records the declared repositories it
// was handed with each question.
type declaredRecorder struct{ asked map[string][]string }

func (r *declaredRecorder) RemotePom(group, artifact, version string, declared []string) (*RemotePom, RemoteOutcome) {
	if r.asked == nil {
		r.asked = map[string][]string{}
	}
	r.asked[group+":"+artifact+":"+version] = declared
	return nil, RemoteNotFound
}

// The source is handed the FETCHABLE repositories the requesting pom's tree
// declares, the child's first; never one that may not be fetched, and never
// one a fetched pom declares.
func TestTheSourceIsHandedTheTreesFetchableRepositories(t *testing.T) {
	child := `<project><parent><groupId>g</groupId><artifactId>mid</artifactId><version>1</version></parent><artifactId>child</artifactId>
	<repositories><repository><url>https://child.example/m2</url></repository><repository><url>http://insecure.example/m2</url></repository></repositories>
	<dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`
	mid := `<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent>
	<groupId>g</groupId><artifactId>mid</artifactId><version>1</version>
	<repositories><repository><url>https://mid.example/m2</url></repository></repositories></project>`
	root := writeTree(t, map[string]string{"pom.xml": mid, "app/pom.xml": child})
	src := &declaredRecorder{}
	w := NewWalk(root, nil)
	w.SetPomSource(src)
	w.Read(filepath.Join(root, "app", "pom.xml"), Options{})
	if got, want := src.asked["far:away:3"], []string{"https://child.example/m2", "https://mid.example/m2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("declared repositories handed to the source = %v, want %v", got, want)
	}
}
