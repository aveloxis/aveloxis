// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// libyear_registry_client_test.go — v0.29.56. Two hours of the
// 2026-09-17 chaoss.tv log (v0.29.55) showed the libyear registry layer
// losing data on every axis:
//   - crates.io: 40 WARN lines of HTTP 429 (3,153 deps). Its data-access
//     policy allows 1 request per second; 70 workers each resolving
//     sequentially had no shared pacing and a 429 failed the dep at once.
//   - search.maven.org: 75 lines of timeouts + 11 of 403 (5,163 deps);
//     one repo's libyear phase took 40 minutes. Its answers also never
//     carried a release date for the version in use, so every Maven
//     libyear was 0 (0 of 51,510 rows in the prior 7 days).
//   - api.github.com (Go module licenses, SwiftPM releases): anonymous
//     requests, 60/hour per IP. 99.5% of 821,230 Go rows had no license,
//     and the license lookup failed without a log line.
//   - No reuse: 168,510 libyear rows in 24 h came from 34,434 distinct
//     (name, version) pairs — about 80% of registry calls were repeats.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// noRegistrySleep swaps the registry wait seam for a recorder so pacing
// and Retry-After tests assert the waits without spending wall clock.
func noRegistrySleep(t *testing.T) *[]time.Duration {
	t.Helper()
	var mu sync.Mutex
	var waits []time.Duration
	old := registrySleep
	registrySleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		// A wait the code should never have agreed to is reported as such
		// rather than slept: the test asserts the decision.
		if d > time.Hour {
			return fmt.Errorf("registry layer asked to sleep %v — far beyond any lookup budget", d)
		}
		return ctx.Err()
	}
	t.Cleanup(func() { registrySleep = old })
	return &waits
}

func TestRegistryFetchRetriesTooManyRequestsWithinBudget(t *testing.T) {
	waits := noRegistrySleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= 2 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	body, err := fetchRegistryJSON(context.Background(), srv.URL+"/api/v1/crates/serde")
	if err != nil {
		t.Fatalf("a 429 with Retry-After inside the budget must be retried: %v", err)
	}
	if string(body) != `{"ok":true}` || hits.Load() != 3 {
		t.Errorf("body=%q hits=%d, want the third attempt's body", body, hits.Load())
	}
	if len(*waits) != 2 || (*waits)[0] != 3*time.Second {
		t.Errorf("waits = %v, want two Retry-After waits of 3s", *waits)
	}
}

func TestRegistryFetchGivesUpWhenRetryAfterExceedsBudget(t *testing.T) {
	noRegistrySleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	_, err := fetchRegistryJSON(context.Background(), srv.URL+"/x")
	if err == nil {
		t.Fatal("a Retry-After past the lookup budget must fail, not sleep an hour")
	}
	if hits.Load() != 1 {
		t.Errorf("hits = %d, want 1", hits.Load())
	}
	if isDefinitiveRegistryMiss(err) {
		t.Error("a rate limit is not a definitive miss (it must never be cached)")
	}
}

// TestRegistryFetchGivesUpOnZeroRetryAfter: a registry (or its CDN) may
// answer 429/503 with `Retry-After: 0`, or an HTTP date already in the past
// by the time it is parsed (clock skew, round trip). Both are legal HTTP and
// both mean "wait zero" — which must still end the lookup rather than
// re-requesting at full speed forever, holding a collection worker until
// `stop serve`.
func TestRegistryFetchGivesUpOnZeroRetryAfter(t *testing.T) {
	headers := []string{
		"0",
		time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat),
		// A date centuries out: time.Until saturates at the maximum
		// Duration, and adding it to the elapsed wait overflows int64 —
		// the sum goes NEGATIVE, passes a "> budget" check, and sleeps
		// ~292 years, holding the worker until `stop serve`.
		"Mon, 01 Jan 2400 00:00:00 GMT",
		"9223372036",
	}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			waits := noRegistrySleep(t)
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The first answer is an ordinary short wait so some budget
				// is already spent: the overflow arithmetic only bites once
				// `waited` is non-zero.
				if hits.Add(1) == 1 {
					w.Header().Set("Retry-After", "1")
				} else {
					w.Header().Set("Retry-After", header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer srv.Close()
			// Cancellable: on the timeout arm the goroutine must stop, or it
			// keeps requesting while srv.Close() waits for it and a clean
			// failure becomes a hung test binary.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := fetchRegistryJSON(ctx, srv.URL+"/x")
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("a registry answering 429 forever must fail the lookup")
				}
				if isDefinitiveRegistryMiss(err) {
					t.Error("a rate limit is not a definitive miss")
				}
			case <-time.After(10 * time.Second):
				cancel()
				<-done
				t.Fatalf("fetchRegistryJSON never returned: %d requests sent — the retry loop makes no progress on Retry-After: %q", hits.Load(), header)
			}
			if n := hits.Load(); n > 40 {
				t.Errorf("sent %d requests before giving up — the wait must advance the budget", n)
			}
			// The decision, not the sleep: the loop must never AGREE to a
			// wait longer than the lookup's whole budget. A far-future or
			// saturating Retry-After overflowed `waited + wait`, so the sum
			// went negative, passed the budget check and slept ~292 years.
			for _, w := range *waits {
				if w > registryRetryBudget {
					t.Errorf("agreed to wait %v for Retry-After %q — the budget is %v", w, header, registryRetryBudget)
				}
			}
		})
	}
}

func TestRegistryNotFoundIsDefinitiveAndServerErrorIsNot(t *testing.T) {
	noRegistrySleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/missing":
			http.NotFound(w, r)
		case "/gone":
			w.WriteHeader(http.StatusGone)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	for _, p := range []string{"/missing", "/gone"} {
		_, err := fetchRegistryJSON(ctx, srv.URL+p)
		if !isDefinitiveRegistryMiss(err) {
			t.Errorf("%s: %v must be a definitive miss", p, err)
		}
		if err == nil || !strings.Contains(err.Error(), "registry "+srv.URL+p+": HTTP") {
			t.Errorf("%s: error text %v must keep the `registry URL: HTTP n` form operators grep for", p, err)
		}
	}
	if _, err := fetchRegistryJSON(ctx, srv.URL+"/boom"); err == nil || isDefinitiveRegistryMiss(err) {
		t.Errorf("a 500 must fail and must not be a definitive miss, got %v", err)
	}
}

// TestRegistryPacingSpacesRequestsToAPacedHost: a host with a published
// request-rate policy gets its requests spaced across ALL callers.
func TestRegistryPacingSpacesRequestsToAPacedHost(t *testing.T) {
	waits := noRegistrySleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	restore := setRegistryHostIntervalForTest(u.Host, time.Second)
	defer restore()

	for i := 0; i < 3; i++ {
		if _, err := fetchRegistryJSON(context.Background(), srv.URL+"/x"); err != nil {
			t.Fatal(err)
		}
	}
	var paced int
	for _, w := range *waits {
		if w > 0 {
			paced++
		}
	}
	if paced < 2 {
		t.Errorf("waits = %v, want the 2nd and 3rd request to wait for the host's interval", *waits)
	}
}

func TestCratesIOIsPacedAtItsPublishedRate(t *testing.T) {
	if got := registryHostInterval("crates.io"); got != time.Second {
		t.Errorf("crates.io interval = %v, want 1s (crates.io data-access policy: at most 1 request per second)", got)
	}
	if got := registryHostInterval("registry.npmjs.org"); got != 0 {
		t.Errorf("npm interval = %v, want none", got)
	}
}

func TestLibyearCacheReusesAnswersAndMisses(t *testing.T) {
	c := newTTLCache[*db.LibyearRow](time.Hour)
	var calls int
	dep := libyearDep{Name: "serde", Version: "1.0.0", Manager: "cargo", Requirement: "1.0", Type: "runtime"}
	resolve := func(ctx context.Context, d libyearDep) (*db.LibyearRow, error) {
		calls++
		return &db.LibyearRow{Name: d.Name, CurrentVersion: d.Version, Requirement: d.Requirement, Type: d.Type, LatestVersion: "1.0.200", PackageManager: "cargo"}, nil
	}
	ctx := context.Background()
	if _, err := resolveLibyearCached(ctx, c, dep, resolve); err != nil {
		t.Fatal(err)
	}
	// Same package and version from another manifest: same answer, this
	// manifest's requirement and scope.
	dep2 := dep
	dep2.Requirement, dep2.Type = "=1.0.0", "dev"
	row, err := resolveLibyearCached(ctx, c, dep2, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("resolver calls = %d, want 1 (the second lookup is a cache hit)", calls)
	}
	if row.Requirement != "=1.0.0" || row.Type != "dev" || row.LatestVersion != "1.0.200" {
		t.Errorf("hit row = %+v, want this dep's requirement/type with the cached registry answer", row)
	}
	row.LatestVersion = "mutated"
	again, _ := resolveLibyearCached(ctx, c, dep, resolve)
	if again.LatestVersion != "1.0.200" {
		t.Error("callers must get copies: mutating a returned row changed the cache")
	}

	// A definitive miss is cached; a transient failure is not (SR-5).
	missCalls, transientCalls := 0, 0
	miss := func(ctx context.Context, d libyearDep) (*db.LibyearRow, error) {
		missCalls++
		return nil, &registryStatusError{url: "u", status: http.StatusNotFound}
	}
	transient := func(ctx context.Context, d libyearDep) (*db.LibyearRow, error) {
		transientCalls++
		return nil, &registryStatusError{url: "u", status: http.StatusServiceUnavailable}
	}
	ghost := libyearDep{Name: "no-such-crate", Version: "0.1.0", Manager: "cargo"}
	flaky := libyearDep{Name: "flaky", Version: "0.1.0", Manager: "cargo"}
	for i := 0; i < 2; i++ {
		if _, err := resolveLibyearCached(ctx, c, ghost, miss); err == nil {
			t.Error("a cached miss must still return its error")
		}
		_, _ = resolveLibyearCached(ctx, c, flaky, transient)
	}
	if missCalls != 1 {
		t.Errorf("miss resolver calls = %d, want 1 (definitive misses are cached)", missCalls)
	}
	if transientCalls != 2 {
		t.Errorf("transient resolver calls = %d, want 2 (a failure without an answer is never cached)", transientCalls)
	}
}

func TestTTLCacheExpires(t *testing.T) {
	c := newTTLCache[string](time.Minute)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	c.put("k", "v", nil)
	if v, _, ok := c.get("k"); !ok || v != "v" {
		t.Fatal("fresh entry must hit")
	}
	now = now.Add(2 * time.Minute)
	if _, _, ok := c.get("k"); ok {
		t.Error("an entry older than the TTL must miss")
	}
	c.put("k2", "v2", nil)
	if c.len() != 1 {
		t.Errorf("len = %d, want 1 (expired entries are swept)", c.len())
	}
}

// fakeGitHubAPI records paths and answers from a table; it stands in for
// the key-pooled platform client.
type fakeGitHubAPI struct {
	mu      sync.Mutex
	paths   []string
	answers map[string]string
	errs    map[string]error
}

func (f *fakeGitHubAPI) GetJSON(ctx context.Context, path string, dest any) error {
	f.mu.Lock()
	f.paths = append(f.paths, path)
	f.mu.Unlock()
	if err, ok := f.errs[path]; ok {
		return err
	}
	body, ok := f.answers[path]
	if !ok {
		return platform.ErrNotFound
	}
	return json.Unmarshal([]byte(body), dest)
}

func TestGoModuleLicenseUsesKeyedGitHubClientAndCaches(t *testing.T) {
	gh := &fakeGitHubAPI{answers: map[string]string{
		"/repos/spf13/cobra/license": `{"license":{"spdx_id":"Apache-2.0"}}`,
	}, errs: map[string]error{
		"/repos/flaky/mod/license": errors.New("getting API key: no API keys configured"),
	}}
	cache := newTTLCache[string](time.Hour)
	ctx := context.Background()

	lic, err := githubModuleLicense(ctx, gh, cache, "github.com/spf13/cobra/v2")
	if err != nil || lic != "Apache-2.0" {
		t.Fatalf("license = %q err = %v, want Apache-2.0", lic, err)
	}
	if lic, _ := githubModuleLicense(ctx, gh, cache, "github.com/spf13/cobra"); lic != "Apache-2.0" {
		t.Errorf("second module of the same repo: %q", lic)
	}
	if len(gh.paths) != 1 {
		t.Errorf("GitHub calls = %v, want one per owner/repo", gh.paths)
	}
	// 404 = the repo has no license file: a definitive empty answer.
	if lic, err := githubModuleLicense(ctx, gh, cache, "github.com/nolicense/repo"); err != nil || lic != "" {
		t.Errorf("no-license repo: %q, %v — want an empty answer, not an error", lic, err)
	}
	// A failure without an answer is an error (counted and logged by the caller).
	if _, err := githubModuleLicense(ctx, gh, cache, "github.com/flaky/mod"); err == nil {
		t.Error("a lookup that failed without an answer must return an error")
	}
	// Non-GitHub modules have no lookup.
	if lic, err := githubModuleLicense(ctx, gh, cache, "golang.org/x/mod"); err != nil || lic != "" {
		t.Errorf("non-GitHub module: %q, %v", lic, err)
	}
	// No client (no GitHub keys): an error, never a silent empty.
	if _, err := githubModuleLicense(ctx, nil, newTTLCache[string](time.Hour), "github.com/a/b"); err == nil {
		t.Error("a missing GitHub client must be reported, not read as 'no license'")
	}
}

func TestSwiftPMResolverUsesKeyedClientAndFindsCurrentRelease(t *testing.T) {
	gh := &fakeGitHubAPI{answers: map[string]string{
		"/repos/Alamofire/Alamofire/releases/latest":     `{"tag_name":"5.10.2","published_at":"2024-11-14T18:00:00Z"}`,
		"/repos/Alamofire/Alamofire/releases/tags/5.8.0": `{"tag_name":"5.8.0","published_at":"2023-09-18T18:00:00Z"}`,
	}}
	row, err := resolveSwiftPMLibyear(context.Background(), gh, libyearDep{
		Name: "Alamofire", Version: "5.8.0", Manager: "swiftpm",
		Requirement: "https://github.com/Alamofire/Alamofire.git"})
	if err != nil {
		t.Fatal(err)
	}
	if row.LatestVersion != "5.10.2" || row.CurrentReleaseDate == "" || row.Libyear <= 0 {
		t.Errorf("row = %+v, want latest 5.10.2 with a current release date and libyear > 0", row)
	}
	if _, err := resolveSwiftPMLibyear(context.Background(), nil, libyearDep{
		Name: "Alamofire", Version: "5.8.0", Manager: "swiftpm",
		Requirement: "https://github.com/Alamofire/Alamofire.git"}); err == nil {
		t.Error("no GitHub client must be an error")
	}
}

func TestMavenResolverUsesRepositoryMetadataAndReleaseDates(t *testing.T) {
	noRegistrySleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/maven2/org/apache/commons/commons-lang3/maven-metadata.xml":
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<metadata><groupId>org.apache.commons</groupId><artifactId>commons-lang3</artifactId>
<versioning><latest>3.20.0</latest><release>3.20.0</release>
<versions><version>3.12.0</version><version>3.20.0</version></versions>
<lastUpdated>20251112000000</lastUpdated></versioning></metadata>`)
		case "/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.pom":
			w.Header().Set("Last-Modified", "Fri, 26 Feb 2021 20:40:52 GMT")
		case "/maven2/org/apache/commons/commons-lang3/3.20.0/commons-lang3-3.20.0.pom":
			w.Header().Set("Last-Modified", "Wed, 12 Nov 2025 10:00:00 GMT")
		case "/maven2/org/gone/artifact/maven-metadata.xml":
			_, _ = io.WriteString(w, `<metadata><versioning><release>2.0</release><versions><version>2.0</version></versions></versioning></metadata>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := mavenRepositoryBase
	mavenRepositoryBase = srv.URL + "/maven2"
	defer func() { mavenRepositoryBase = old }()

	row, err := resolveMavenLibyear(context.Background(), libyearDep{
		Name: "org.apache.commons:commons-lang3", Version: "3.12.0", Manager: "maven"})
	if err != nil {
		t.Fatal(err)
	}
	if row.LatestVersion != "3.20.0" || row.CurrentReleaseDate == "" || row.LatestReleaseDate == "" {
		t.Errorf("row = %+v, want latest 3.20.0 with both release dates", row)
	}
	if row.Libyear < 4 || row.Libyear > 5 {
		t.Errorf("libyear = %v, want about 4.7 (Feb 2021 → Nov 2025)", row.Libyear)
	}
	if want := "pkg:maven/org.apache.commons/commons-lang3@3.12.0"; row.Purl != want {
		t.Errorf("purl = %q, want %q", row.Purl, want)
	}

	// A version the metadata lists whose .pom is absent (retracted file,
	// classifier-only artifact) costs that DATE, not the dependency: the
	// artifact exists, and failing here would also cache a definitive miss
	// for something Maven Central has.
	row, err = resolveMavenLibyear(context.Background(), libyearDep{
		Name: "org.apache.commons:commons-lang3", Version: "3.9", Manager: "maven"})
	if err != nil {
		t.Fatalf("a version outside the metadata list must not fail the dependency: %v", err)
	}
	if row.LatestVersion != "3.20.0" {
		t.Errorf("row = %+v, want the latest version even without the pinned version's date", row)
	}

	_, err = resolveMavenLibyear(context.Background(), libyearDep{Name: "org.nope:nothing", Version: "1", Manager: "maven"})
	if !isDefinitiveRegistryMiss(err) {
		t.Errorf("an artifact Maven Central does not have must be a definitive miss, got %v", err)
	}
	// The latest version's .pom is absent too: the row still carries the
	// latest version, with no date.
	row, err = resolveMavenLibyear(context.Background(), libyearDep{Name: "org.gone:artifact", Version: "2.0", Manager: "maven"})
	if err != nil {
		t.Fatalf("a missing .pom for the latest version must not fail the dependency: %v", err)
	}
	if row.LatestVersion != "2.0" || row.LatestReleaseDate != "" {
		t.Errorf("row = %+v, want latest 2.0 with no date", row)
	}

	for _, name := range []string{"${project.groupId}:pki-server", "org.x:${artifact.id}"} {
		if _, err := resolveMavenLibyear(context.Background(), libyearDep{Name: name, Version: "1", Manager: "maven"}); err == nil {
			t.Errorf("%q: an unresolved property must never be sent to the repository", name)
		}
	}
}

// TestRegistryRequestsIdentifyThemselves replaces the source pin on
// fetchRegistryJSON's body: every registry request, GET or HEAD, carries
// the identifying User-Agent (crates.io 403s anonymous defaults).
func TestRegistryRequestsIdentifyThemselves(t *testing.T) {
	noRegistrySleep(t)
	var mu sync.Mutex
	var agents []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		agents = append(agents, r.Method+" "+r.Header.Get("User-Agent"))
		mu.Unlock()
		w.Header().Set("Last-Modified", "Fri, 26 Feb 2021 20:40:52 GMT")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	ctx := context.Background()
	if _, err := fetchRegistryJSON(ctx, srv.URL+"/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := fetchRegistryLastModified(ctx, srv.URL+"/b"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, a := range agents {
		if !strings.Contains(a, " aveloxis/") {
			t.Errorf("request %q lacks the aveloxis User-Agent", a)
		}
	}
	if len(agents) != 2 || !strings.HasPrefix(agents[1], "HEAD ") {
		t.Errorf("requests = %v, want a GET then a HEAD", agents)
	}
}

// TestScanLibyearWiresKeyedGitHubClient: the scheduler hands analysis the
// key-pooled GitHub client; no registry lookup reaches api.github.com
// anonymously.
func TestScanLibyearWiresKeyedGitHubClient(t *testing.T) {
	src := srctest.Read(t, "internal/collector/analysis.go")
	if strings.Contains(srctest.StripGoComments(src), "api.github.com") {
		t.Error("analysis.go must not build api.github.com URLs for anonymous fetches; GitHub lookups go through AnalysisCollector.GitHubAPI (the key pool)")
	}
	sched := srctest.Read(t, "internal/scheduler/scheduler.go")
	if !strings.Contains(sched, "ac.GitHubAPI = ") {
		t.Error("runFacadeAndAnalysis must set ac.GitHubAPI to the key-pooled GitHub client")
	}
}

// TestRegistryLayerIsSafeUnderConcurrency: the cache and the host pacer are
// shared by every collection worker in the process (70 on chaoss.tv), so
// they are exercised from many goroutines at once under -race.
func TestRegistryLayerIsSafeUnderConcurrency(t *testing.T) {
	noRegistrySleep(t)
	cache := newTTLCache[*db.LibyearRow](time.Hour)
	var calls atomic.Int32
	resolve := func(ctx context.Context, d libyearDep) (*db.LibyearRow, error) {
		calls.Add(1)
		if d.Name == "ghost" {
			return nil, &registryStatusError{url: "u", status: http.StatusNotFound}
		}
		return &db.LibyearRow{Name: d.Name, CurrentVersion: d.Version, LatestVersion: "9.9.9"}, nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	restore := setRegistryHostIntervalForTest(u.Host, time.Millisecond)
	defer restore()

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				name := []string{"serde", "tokio", "ghost"}[j%3]
				dep := libyearDep{Name: name, Version: "1.0.0", Manager: "cargo", Type: "runtime"}
				row, err := resolveLibyearCached(ctx, cache, dep, resolve)
				switch {
				case name == "ghost":
					if err == nil {
						t.Errorf("ghost must keep returning its miss")
					}
				case err != nil:
					t.Errorf("%s: %v", name, err)
				case row.LatestVersion != "9.9.9":
					t.Errorf("%s: row = %+v", name, row)
				}
				if _, err := fetchRegistryJSON(ctx, srv.URL+"/x"); err != nil {
					t.Errorf("paced fetch: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	// Two packages and one miss; a few duplicate resolves are fine (there is
	// no single-flight), but it must be far below the 480 lookups requested.
	if n := calls.Load(); n > 100 {
		t.Errorf("resolver calls = %d for 480 lookups of 3 packages — the cache is not being shared", n)
	}
	if cache.len() != 3 {
		t.Errorf("cache holds %d entries, want 3", cache.len())
	}
}

// TestRegistryPacingReportsThroughTheCollectionLogger: the deep-queue line
// must go to the collection's logger. slog.Default() is not it — nothing in
// this codebase calls slog.SetDefault, so the default logger writes
// unformatted lines that ignore `log_level`.
func TestRegistryPacingReportsThroughTheCollectionLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	waits := noRegistrySleep(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	// An interval past one request's whole timeout makes the second caller's
	// wait deep enough to report.
	restore := setRegistryHostIntervalForTest(u.Host, registryHTTPClient.Timeout+time.Second)
	defer restore()

	ctx := withRegistryLogger(context.Background(), logger)
	for i := 0; i < 2; i++ {
		if _, err := fetchRegistryJSON(ctx, srv.URL+"/x"); err != nil {
			t.Fatal(err)
		}
	}
	if len(*waits) == 0 {
		t.Fatal("the second request must wait for its slot")
	}
	if !strings.Contains(buf.String(), "registry pacing queue is deep") {
		t.Errorf("the deep-queue line did not reach the collection logger; got %q", buf.String())
	}
	if !strings.Contains(buf.String(), u.Host) {
		t.Errorf("the line must name the host; got %q", buf.String())
	}
}

// TestScanLibyearSeedsTheRegistryLogger is the wiring half.
func TestScanLibyearSeedsTheRegistryLogger(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/collector/analysis.go"),
		"func (ac *AnalysisCollector) scanLibyear(")
	if !strings.Contains(body, "withRegistryLogger(ctx, ac.logger)") {
		t.Error("scanLibyear must seed the registry layer's logger with the collection's own")
	}
}

// TestRegistryRetryAfterRefusesOverflowingDeltaSeconds pins the guard that
// the budget check alone does not: `time.Duration(secs) * time.Second`
// wraps int64 past ~9.22e9 seconds, and the one-second floor would then
// MASK the wrapped negative into a 1s wait — silently ignoring a registry
// that asked for an enormous back-off, while a value one second smaller
// correctly gives up. Deleting the guard leaves every other test passing.
func TestRegistryRetryAfterRefusesOverflowingDeltaSeconds(t *testing.T) {
	budgetSecs := int(registryRetryBudget / time.Second)
	cases := []struct {
		header string
		want   time.Duration
		ok     bool
	}{
		{"0", 0, true},
		{"1", time.Second, true},
		{strconv.Itoa(budgetSecs), registryRetryBudget, true},
		{strconv.Itoa(budgetSecs + 1), 0, false},
		// The first value whose nanosecond conversion wraps, and one that
		// wraps to a small POSITIVE duration (the dangerous shape: it would
		// look like a perfectly reasonable wait).
		{"9223372037", 0, false},
		{"18446744074", 0, false},
		{"-5", 0, false},
		{"garbage", 0, false},
	}
	for _, tc := range cases {
		got, ok := registryRetryAfter(tc.header, 0)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("registryRetryAfter(%q) = (%v, %v), want (%v, %v)", tc.header, got, ok, tc.want, tc.ok)
		}
	}
}
