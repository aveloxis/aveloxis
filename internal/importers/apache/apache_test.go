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

	// amoro → github.com/apache/amoro
	if !reposContain(projects[0].RepoURLs, "https://github.com/apache/amoro") {
		t.Errorf("amoro RepoURLs = %v, want https://github.com/apache/amoro", projects[0].RepoURLs)
	}
	// burr → github.com/apache/burr
	if !reposContain(projects[1].RepoURLs, "https://github.com/apache/burr") {
		t.Errorf("burr RepoURLs = %v, want https://github.com/apache/burr", projects[1].RepoURLs)
	}
}

// TestFetchHitsBothEndpoints — Fetch must combine projects.json + podlings.json
// results into a single list, using the URLs we pass in so tests can stub the
// server.
func TestFetchHitsBothEndpoints(t *testing.T) {
	projectsJSON, _ := os.ReadFile("testdata/projects_mini.json")
	podlingsJSON, _ := os.ReadFile("testdata/podlings_mini.json")

	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "podlings"):
			w.Write(podlingsJSON)
		default:
			w.Write(projectsJSON)
		}
	}))
	defer server.Close()

	projects, err := Fetch(context.Background(), server.URL+"/projects.json", server.URL+"/podlings.json")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if hits != 2 {
		t.Errorf("server hit %d times, want exactly 2 (projects + podlings)", hits)
	}
	// 3 TLPs + 2 podlings = 5.
	if len(projects) != 5 {
		t.Errorf("got %d projects, want 5 (3 TLPs + 2 podlings)", len(projects))
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
