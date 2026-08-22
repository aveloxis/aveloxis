// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package apache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestParseProjectsDerivesGitHubURL — Apache's projects.json doesn't have a
// `repository` field. Our parser must derive the GitHub URL from
// `bug-database` when it's a github.com URL (strip trailing /issues), and
// fall back to `https://github.com/apache/<pmc>` for projects that use Jira.
func TestParseProjectsDerivesGitHubURL(t *testing.T) {
	data, err := os.ReadFile("testdata/projects_mini.json")
	if err != nil {
		t.Fatal(err)
	}
	projects, err := ParseProjects(data)
	if err != nil {
		t.Fatalf("ParseProjects: %v", err)
	}
	if len(projects) != 3 {
		t.Fatalf("got %d projects, want 3 (accumulo, airflow, commons)", len(projects))
	}

	byName := map[string]Project{}
	for _, p := range projects {
		byName[p.Name] = p
	}

	// accumulo has bug-database on github.com — must be derived from it.
	accumulo, ok := byName["Apache Accumulo"]
	if !ok {
		t.Fatal("Apache Accumulo must be present")
	}
	if accumulo.Status != "graduated" {
		t.Errorf("Accumulo status = %q, want graduated — projects.json contains TLPs which are all graduated", accumulo.Status)
	}
	if accumulo.Foundation != "apache" {
		t.Errorf("Foundation = %q, want apache", accumulo.Foundation)
	}
	if !reposContain(accumulo.RepoURLs, "https://github.com/apache/accumulo") {
		t.Errorf("Accumulo RepoURLs = %v, want to contain https://github.com/apache/accumulo — derivation from bug-database (strip /issues) must work", accumulo.RepoURLs)
	}

	// airflow uses Jira for bugs — fall back to https://github.com/apache/<pmc>.
	airflow, ok := byName["Apache Airflow"]
	if !ok {
		t.Fatal("Apache Airflow must be present")
	}
	if !reposContain(airflow.RepoURLs, "https://github.com/apache/airflow") {
		t.Errorf("Airflow RepoURLs = %v, want the /apache/<pmc> fallback when bug-database is Jira", airflow.RepoURLs)
	}

	// commons has no bug-database at all — same /apache/<pmc> fallback.
	commons, ok := byName["Apache Commons"]
	if !ok {
		t.Fatal("Apache Commons must be present")
	}
	if !reposContain(commons.RepoURLs, "https://github.com/apache/commons") {
		t.Errorf("Commons RepoURLs = %v, want fallback to https://github.com/apache/commons when bug-database is missing", commons.RepoURLs)
	}
}

// TestParsePodlingsAllIncubating — every entry in podlings.json is an
// incubating podling. Status must be "incubating" and repo URL must follow
// the Apache INFRA convention: https://github.com/apache/<slug>.
func TestParsePodlingsAllIncubating(t *testing.T) {
	data, err := os.ReadFile("testdata/podlings_mini.json")
	if err != nil {
		t.Fatal(err)
	}
	projects, err := ParsePodlings(data)
	if err != nil {
		t.Fatalf("ParsePodlings: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("got %d podlings, want 2 (amoro, burr)", len(projects))
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].Name < projects[j].Name })

	for _, p := range projects {
		if p.Status != "incubating" {
			t.Errorf("%s status = %q, want incubating", p.Name, p.Status)
		}
		if p.Foundation != "apache" {
			t.Errorf("%s foundation = %q, want apache", p.Name, p.Foundation)
		}
	}

	// v0.27.132: podling repos carry the incubator- prefix (Apache INFRA
	// convention) — the old apache/<slug> derivation seeded phantom rows
	// that later 404'd (the four wedged production PMC groups).
	if !reposContain(projects[0].RepoURLs, "https://github.com/apache/incubator-amoro") {
		t.Errorf("amoro RepoURLs = %v, want https://github.com/apache/incubator-amoro", projects[0].RepoURLs)
	}
	if !reposContain(projects[1].RepoURLs, "https://github.com/apache/incubator-burr") {
		t.Errorf("burr RepoURLs = %v, want https://github.com/apache/incubator-burr", projects[1].RepoURLs)
	}
}

// TestFetchHitsBothEndpoints — Fetch must combine projects.json + podlings.json
// results into a single list, using the URLs we pass in so tests can stub the
// server. Round-24: Fetch also PROBES podling repo URLs against the
// forge (via the podlingProbeBase seam — a bare run would otherwise
// hit live github.com from a unit test) and resolves the moving-target
// incubator- prefix: amoro is served as the GRADUATED-rename shape
// (incubator- 301s, plain 200s — the exact production example) and
// must come back PLAIN; burr stays incubator-.
func TestFetchHitsBothEndpoints(t *testing.T) {
	projectsJSON, _ := os.ReadFile("testdata/projects_mini.json")
	podlingsJSON, _ := os.ReadFile("testdata/podlings_mini.json")

	var jsonHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "podlings.json"):
			jsonHits++
			w.Header().Set("Content-Type", "application/json")
			w.Write(podlingsJSON)
		case strings.Contains(r.URL.Path, "projects.json"):
			jsonHits++
			w.Header().Set("Content-Type", "application/json")
			w.Write(projectsJSON)
		// Forge probes (HEAD): amoro graduated — the incubator- form
		// redirects, the plain form exists; burr is still incubating.
		case r.URL.Path == "/apache/incubator-amoro":
			w.Header().Set("Location", "/apache/amoro")
			w.WriteHeader(http.StatusMovedPermanently)
		case r.URL.Path == "/apache/amoro":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/apache/incubator-burr":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	oldBase := podlingProbeBase
	podlingProbeBase = server.URL
	t.Cleanup(func() { podlingProbeBase = oldBase })

	projects, err := Fetch(context.Background(), server.URL+"/projects.json", server.URL+"/podlings.json")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if jsonHits != 2 {
		t.Errorf("JSON endpoints hit %d times, want exactly 2 (projects + podlings)", jsonHits)
	}
	// 3 TLPs + 2 podlings = 5.
	if len(projects) != 5 {
		t.Errorf("got %d projects, want 5 (3 TLPs + 2 podlings)", len(projects))
	}
	byName := map[string][]string{}
	for _, p := range projects {
		byName[p.Name] = p.RepoURLs
	}
	if !reposContain(byName["Apache Amoro (Incubating)"], "https://github.com/apache/amoro") {
		t.Errorf("graduated-rename podling must resolve to the PLAIN form, got %v", byName["Apache Amoro (Incubating)"])
	}
	if !reposContain(byName["Apache Burr (Incubating)"], "https://github.com/apache/incubator-burr") {
		t.Errorf("in-incubation podling must keep the incubator- form, got %v", byName["Apache Burr (Incubating)"])
	}
}

// TestFetchDropsUnresolvablePodlingURLs — a podling neither variant of
// which exists on the forge must move its URL to UnresolvedRepoURLs
// (the importer skips it) instead of shipping a phantom catalog row.
func TestFetchDropsUnresolvablePodlingURLs(t *testing.T) {
	podlingsJSON := []byte(`{"ghostling": {"name": "Apache Ghostling (Incubating)", "podling": true}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "podlings.json"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(podlingsJSON)
		case strings.Contains(r.URL.Path, "projects.json"):
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound) // no variant exists
		}
	}))
	defer server.Close()

	oldBase := podlingProbeBase
	podlingProbeBase = server.URL
	t.Cleanup(func() { podlingProbeBase = oldBase })

	projects, err := Fetch(context.Background(), server.URL+"/projects.json", server.URL+"/podlings.json")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var ghost *Project
	for i := range projects {
		if projects[i].Name == "Apache Ghostling (Incubating)" {
			ghost = &projects[i]
		}
	}
	if ghost == nil {
		t.Fatal("podling project missing from Fetch result")
	}
	if len(ghost.RepoURLs) != 0 {
		t.Errorf("unresolvable podling must ship ZERO repo URLs (no phantom rows), got %v", ghost.RepoURLs)
	}
	if len(ghost.UnresolvedRepoURLs) != 1 {
		t.Errorf("the guessed URL must be surfaced in UnresolvedRepoURLs, got %v", ghost.UnresolvedRepoURLs)
	}
}

// TestParseProjectsInvalidJSON — malformed JSON must return an error.
func TestParseProjectsInvalidJSON(t *testing.T) {
	if _, err := ParseProjects([]byte("{ not valid")); err == nil {
		t.Error("ParseProjects must return error for malformed JSON")
	}
}

func reposContain(urls []string, want string) bool {
	for _, u := range urls {
		if u == want {
			return true
		}
	}
	return false
}

// Round-25 (SR-5 in the resolver itself): only DEFINITIVE probe
// responses decide. A transient forge failure (5xx, 403/429,
// transport error) must ABORT the import — never silently demote a
// valid podling to UnresolvedRepoURLs.
func TestFetchAbortsOnNonDefinitiveProbeResponse(t *testing.T) {
	podlingsJSON := []byte(`{"flaky": {"name": "Apache Flaky (Incubating)", "podling": true}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "podlings.json"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(podlingsJSON)
		case strings.Contains(r.URL.Path, "projects.json"):
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusInternalServerError) // forge outage
		}
	}))
	defer server.Close()

	oldBase := podlingProbeBase
	podlingProbeBase = server.URL
	t.Cleanup(func() { podlingProbeBase = oldBase })

	_, err := Fetch(context.Background(), server.URL+"/projects.json", server.URL+"/podlings.json")
	if err == nil {
		t.Fatal("a 5xx probe must ABORT Fetch — treating it as 'repo does not exist' drops valid podlings from the import")
	}
	if !strings.Contains(err.Error(), "not a definitive answer") {
		t.Errorf("error must name the non-definitive-response rule, got: %v", err)
	}
}
