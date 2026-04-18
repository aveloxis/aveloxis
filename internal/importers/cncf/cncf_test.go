package cncf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestParseLandscapeFiltersByProjectField — the landscape.yml lists hundreds
// of projects, but only items with a `project:` value of graduated/incubating/
// sandbox are actual CNCF member projects. Everything else (ecosystem entries
// with no `project:`, `archived`, unknown values) must be excluded.
func TestParseLandscapeFiltersByProjectField(t *testing.T) {
	data, err := os.ReadFile("testdata/landscape_mini.yml")
	if err != nil {
		t.Fatal(err)
	}
	projects, err := ParseLandscape(data)
	if err != nil {
		t.Fatalf("ParseLandscape: %v", err)
	}

	// Expect exactly three: Kubernetes (graduated), Thanos (incubating),
	// OpenCost (sandbox). Docker Swarm has no project field; Example Archived
	// has project:archived; Weird Entry has an unknown status; NoRepo has a
	// valid status but no repo_url — all must be skipped.
	if len(projects) != 3 {
		names := make([]string, 0, len(projects))
		for _, p := range projects {
			names = append(names, p.Name+"("+p.Status+")")
		}
		t.Fatalf("got %d projects %v, want exactly 3: Kubernetes/Thanos/OpenCost — "+
			"items without a recognized project: status must be excluded",
			len(projects), names)
	}

	byName := map[string]Project{}
	for _, p := range projects {
		byName[p.Name] = p
	}

	// Kubernetes: graduated, includes primary + 2 additional_repos.
	k8s, ok := byName["Kubernetes"]
	if !ok {
		t.Fatal("Kubernetes must be in the result set")
	}
	if k8s.Status != "graduated" {
		t.Errorf("Kubernetes status = %q, want graduated", k8s.Status)
	}
	if k8s.Foundation != "cncf" {
		t.Errorf("Foundation = %q, want cncf", k8s.Foundation)
	}
	sort.Strings(k8s.RepoURLs)
	wantK8sRepos := []string{
		"https://github.com/kubernetes/kubectl",
		"https://github.com/kubernetes/kubernetes",
		"https://github.com/kubernetes/website",
	}
	if strings.Join(k8s.RepoURLs, ",") != strings.Join(wantK8sRepos, ",") {
		t.Errorf("Kubernetes RepoURLs = %v, want %v — additional_repos must be included alongside the primary repo_url so projects like k8s are covered end-to-end",
			k8s.RepoURLs, wantK8sRepos)
	}

	// Thanos: incubating, single repo.
	thanos, ok := byName["Thanos"]
	if !ok {
		t.Fatal("Thanos must be in the result set")
	}
	if thanos.Status != "incubating" {
		t.Errorf("Thanos status = %q, want incubating", thanos.Status)
	}

	// OpenCost: sandbox.
	opencost, ok := byName["OpenCost"]
	if !ok {
		t.Fatal("OpenCost must be in the result set")
	}
	if opencost.Status != "sandbox" {
		t.Errorf("OpenCost status = %q, want sandbox", opencost.Status)
	}

	// Ensure the excluded items really did not sneak in.
	for _, bad := range []string{"Docker Swarm", "Example Archived", "Weird Entry", "NoRepo"} {
		if _, ok := byName[bad]; ok {
			t.Errorf("%q must NOT be in the result set", bad)
		}
	}
}

// TestFetchLandscapeUsesProvidedURL — Fetch must hit the URL we configure,
// not a hard-coded one. Proves we can point the client at a test server
// (critical for offline CI) and at alternate mirrors if cncf/landscape
// ever moves.
func TestFetchLandscapeUsesProvidedURL(t *testing.T) {
	data, err := os.ReadFile("testdata/landscape_mini.yml")
	if err != nil {
		t.Fatal(err)
	}
	var serverHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverHits++
		w.Header().Set("Content-Type", "text/yaml")
		w.Write(data)
	}))
	defer server.Close()

	projects, err := Fetch(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if serverHits != 1 {
		t.Errorf("server hit %d times, want 1 — Fetch must not retry-storm on success", serverHits)
	}
	if len(projects) != 3 {
		t.Errorf("got %d projects, want 3 (from the fixture)", len(projects))
	}
}

// TestParseLandscapeUnknownSchema — a malformed YAML payload must return an
// error rather than silently producing zero projects. Silent zero would be
// indistinguishable from "the upstream source really has no projects" and
// hide real breakage when CNCF changes the schema.
func TestParseLandscapeUnknownSchema(t *testing.T) {
	garbage := []byte("this: is: not: { a: valid landscape.yml")
	_, err := ParseLandscape(garbage)
	if err == nil {
		t.Error("ParseLandscape must return error for malformed YAML so schema drift is caught loudly instead of producing silent empty imports")
	}
}

// Keep a reference to encoding/json so future tests that want to marshal
// compare structures don't need a separate import add.
var _ = json.Marshal
