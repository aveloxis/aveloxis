// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Phase C (v0.27.21) tests: the C0 OSV cache and the C1 transitive
// machinery (summary/14).

package collector

import (
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/scantarget"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// ---------------------------------------------------------------------------
// C0 — OSV cache
// ---------------------------------------------------------------------------

func TestOSVCacheQueryRoundTrip(t *testing.T) {
	c := NewOSVCache()
	if _, ok := c.GetQuery("pkg:npm/lodash@4.17.21"); ok {
		t.Fatal("empty cache must miss")
	}
	c.PutQuery("pkg:npm/lodash@4.17.21", []string{"GHSA-x", "GHSA-y"})
	ids, ok := c.GetQuery("pkg:npm/lodash@4.17.21")
	if !ok || len(ids) != 2 {
		t.Fatalf("cached answer must round-trip, got ok=%v ids=%v", ok, ids)
	}
	// Negative results (no vulns) are cached too — that's most of the
	// win: most packages have no advisories.
	c.PutQuery("pkg:npm/left-pad@1.3.0", []string{})
	ids, ok = c.GetQuery("pkg:npm/left-pad@1.3.0")
	if !ok || len(ids) != 0 {
		t.Fatalf("negative answers must be cached, got ok=%v ids=%v", ok, ids)
	}
}

func TestOSVCacheDetailRoundTripAndNilSafety(t *testing.T) {
	c := NewOSVCache()
	c.PutDetail("GHSA-x", &osvVuln{ID: "GHSA-x", Summary: "s"})
	v, ok := c.GetDetail("GHSA-x")
	if !ok || v.Summary != "s" {
		t.Fatal("detail must round-trip")
	}
	// nil detail must never be cached (a failed fetch has nothing to
	// put — caching it would mask a later success, the v0.27.4
	// prefer-nonempty lesson).
	c.PutDetail("GHSA-fail", nil)
	if _, ok := c.GetDetail("GHSA-fail"); ok {
		t.Fatal("nil details must not be cached")
	}
	// A nil *OSVCache disables caching entirely — every method no-ops.
	var nc *OSVCache
	if _, ok := nc.GetQuery("x"); ok {
		t.Fatal("nil cache GetQuery must miss")
	}
	nc.PutQuery("x", nil) // must not panic
	nc.PutDetail("y", &osvVuln{})
	if _, ok := nc.GetDetail("y"); ok {
		t.Fatal("nil cache GetDetail must miss")
	}
}

func TestOSVCacheTTLExpiry(t *testing.T) {
	c := NewOSVCache()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	c.PutQuery("pkg:npm/a@1", []string{"GHSA-a"})
	c.PutDetail("GHSA-a", &osvVuln{ID: "GHSA-a"})

	now = now.Add(23 * time.Hour)
	if _, ok := c.GetQuery("pkg:npm/a@1"); !ok {
		t.Fatal("23h-old entry must still hit (TTL is 24h)")
	}
	now = now.Add(2 * time.Hour) // 25h total
	if _, ok := c.GetQuery("pkg:npm/a@1"); ok {
		t.Fatal("25h-old query entry must expire — vuln knowledge about an immutable version still changes")
	}
	if _, ok := c.GetDetail("GHSA-a"); ok {
		t.Fatal("25h-old detail entry must expire")
	}
}

func TestOSVCacheConcurrentAccess(t *testing.T) {
	c := NewOSVCache()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.PutQuery("pkg:npm/p@1", []string{"GHSA-1"})
				c.GetQuery("pkg:npm/p@1")
				c.PutDetail("GHSA-1", &osvVuln{ID: "GHSA-1"})
				c.GetDetail("GHSA-1")
			}
		}(i)
	}
	wg.Wait()
}

// TestScanRoutesThroughCache pins the C0 wiring inside
// ScanVulnerabilities: purls consult GetQuery before the batch call,
// fresh answers are PutQuery'd (negatives included), details consult
// GetDetail, and ONLY successfully fetched details are PutDetail'd.
func TestScanRoutesThroughCache(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// 0.29.168: the query half moved into the fetcher, which reads the
	// cache through lookupQuery (GetQuery plus a cached rejection).
	fetch := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func fetchOSVQueryAnswers("))
	for _, needle := range []string{"cache.lookupQuery(", "cache.putAnswer(", "cache.putRejectedQuery("} {
		if !strings.Contains(fetch, needle) {
			t.Errorf("fetchOSVQueryAnswers must route OSV queries through the C0 cache — missing %s", needle)
		}
	}
	if !strings.Contains(s, "fetchOSVQueryAnswers(ctx, cache, ") {
		t.Error("ScanVulnerabilities must ask through fetchOSVQueryAnswers with the scan's cache")
	}
	for _, needle := range []string{
		"cache.GetDetail(", "cache.PutDetail(",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("ScanVulnerabilities must route OSV traffic through the C0 cache — missing %s", needle)
		}
	}
	// PutDetail must be fed from fetchOSVDetails' RESULT map (which
	// omits failures) — never from the id set.
	if !strings.Contains(s, "for id, v := range fetchOSVDetails(") {
		t.Error("details must be cached only from the successful-fetch result map (failed fetches are never cached)")
	}
}

// ---------------------------------------------------------------------------
// C1 — transitive machinery
// ---------------------------------------------------------------------------

func TestPurlForPackageMapping(t *testing.T) {
	cases := []struct{ eco, name, ver, want string }{
		{"npm", "lodash", "4.17.21", "pkg:npm/lodash@4.17.21"},
		// v0.27.29: spec-canonical — npm scope '@' percent-encodes to
		// %40 (purl-spec test-suite case). The pre-v0.27.29 pin
		// asserted the bare '@' form back at the implementation.
		{"npm", "@babel/core", "7.0.0", "pkg:npm/%40babel/core@7.0.0"},
		{"pypi", "flask", "2.5.0", "pkg:pypi/flask@2.5.0"},
		{"cargo", "serde", "1.0.0", "pkg:cargo/serde@1.0.0"},
		{"gem", "rails", "7.0.0", "pkg:gem/rails@7.0.0"},
		{"maven", "org.apache.logging.log4j:log4j-core", "2.14.1",
			"pkg:maven/org.apache.logging.log4j/log4j-core@2.14.1"},
		{"haskell", "aeson", "2.0.0", "pkg:hackage/aeson@2.0.0"},
		{"unknown-eco", "x", "1", ""}, // honest omission over a malformed purl
		{"npm", "", "1", ""},
		{"npm", "x", "", ""},
	}
	for _, tc := range cases {
		if got := pkgid.PurlForPackage(tc.eco, "", tc.name, tc.ver); got != tc.want {
			t.Errorf("pkgid.PurlForPackage(%q,%q,%q) = %q, want %q", tc.eco, tc.name, tc.ver, got, tc.want)
		}
	}
}

func TestNPMLockV2CarriesDevScope(t *testing.T) {
	data := []byte(`{
		"lockfileVersion": 2,
		"packages": {
			"": {"dependencies": {"express": "^4.0.0"}, "devDependencies": {"jest": "^29.0.0"}},
			"node_modules/express": {"version": "4.18.2"},
			"node_modules/jest": {"version": "29.5.0", "dev": true},
			"node_modules/tmpl": {"version": "1.0.5", "dev": true}
		}
	}`)
	parsed, err := parsePackageLockJSON(data)
	if err != nil || !parsed.DirectKnown {
		t.Fatalf("parse: err=%v directKnown=%v", err, parsed.DirectKnown)
	}
	byName := map[string]LockfileEntry{}
	for _, e := range parsed.Entries {
		byName[e.Name] = e
	}
	if byName["express"].Scope != "" {
		t.Errorf("runtime dep must have empty scope, got %q", byName["express"].Scope)
	}
	if byName["jest"].Scope != "dev" || byName["tmpl"].Scope != "dev" {
		t.Errorf("dev-flagged entries must carry scope='dev': jest=%q tmpl=%q",
			byName["jest"].Scope, byName["tmpl"].Scope)
	}
	if !byName["express"].Direct || !byName["jest"].Direct || byName["tmpl"].Direct {
		t.Error("direct flags: express+jest direct (root deps), tmpl transitive")
	}
}

func TestPoetryLockCarriesDevCategory(t *testing.T) {
	data := []byte(`
[[package]]
name = "flask"
version = "2.3.0"
category = "main"

[[package]]
name = "pytest"
version = "7.4.0"
category = "dev"
`)
	parsed, err := parsePoetryStyleTOML(data)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]LockfileEntry{}
	for _, e := range parsed.Entries {
		byName[e.Name] = e
	}
	if byName["flask"].Scope != "" {
		t.Errorf("main-category entry must have empty scope, got %q", byName["flask"].Scope)
	}
	if byName["pytest"].Scope != "dev" {
		t.Errorf("dev-category entry must carry scope='dev', got %q", byName["pytest"].Scope)
	}
}

// TestScanLockfilesTransitiveGate pins the knob semantics in
// scanLockfiles: the transitive branch is gated on
// ac.TransitiveLockfiles (knob off = the pre-C1 declared-only row
// set), stored transitive rows carry Direct=false, and declared
// matches stay Direct=true.
func TestScanLockfilesTransitiveGate(t *testing.T) {
	src, err := os.ReadFile("lockfile_scan.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// 0.29.215: what the gate gives is pinned by what comes out
	// (TestRowsOfALockfileUnderTheTransitiveKnob: knob off keeps the
	// declared-only row set, transitive rows are Direct=false, declared
	// resolutions Direct=true). This holds the wiring: the phase builds a
	// lockfile's rows by that function, with the knob, and the flag is the
	// declared match.
	s = srctest.StripGoComments(s)
	if !strings.Contains(s, "rowsOfLockfile(pl, declared, ac.TransitiveLockfiles)") {
		t.Error("the transitive storage branch must be gated on ac.TransitiveLockfiles — " +
			"knob off must keep the pre-C1 declared-only row set")
	}
	if !regexp.MustCompile(`Direct:\s+isDeclared,`).MatchString(s) {
		t.Error("a stored row is Direct exactly when it is the resolution of a declared dependency")
	}
}

// TestVulnScanTargetsCarryKind pins that every direct-target
// constructor stamps dependency_kind='direct' — a target without a
// kind would store the empty string and read as a pre-C1 row forever.
func TestVulnScanTargetsCarryKind(t *testing.T) {
	// Behaviour (0.29.154; this counted a struct-literal line in the source):
	// a Go dependency, a lockfile resolution and a floor all yield direct
	// targets.
	locked := map[string][]string{pkgid.MatchKey("npm", "lodash"): {"4.17.21"}}
	for _, d := range []model.VulnScanDep{
		{Name: "github.com/pkg/errors", PackageManager: "go", CurrentVersion: "v0.9.1", Purl: "pkg:golang/github.com/pkg/errors@v0.9.1"},
		{Name: "lodash", PackageManager: "npm", CurrentVersion: "4.17.20", Purl: "pkg:npm/lodash@4.17.20"},
		{Name: "express", PackageManager: "npm", CurrentVersion: "4.17.1", Requirement: "^4.17.1", Purl: "pkg:npm/express@4.17.1"},
	} {
		targets := scantarget.DirectTargets(d, locked)
		if len(targets) != 1 || targets[0].Kind != model.DependencyKindDirect {
			t.Errorf("%s: targets %+v; want one, of kind direct", d.Name, targets)
		}
	}
}

// TestTransitiveScanFailsClosed pins the fail-closed posture: a
// transitive-read error must fail the scan (a partial target set
// would flip resolved_at back and forth across scans — the v0.27.11
// lesson).
func TestTransitiveScanFailsClosed(t *testing.T) {
	// The read, and failing closed on its error, are scantarget.Build's
	// (TestBuildFailsClosedOnEveryRead there drives each read's failure).
	// What is the scan's is to return Build's error and go no further.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ScanVulnerabilities("))
	wiring := regexp.MustCompile(`set, err := scantarget\.Build\(ctx, store, repoID, transitive\)\s*if err != nil \{\s*return nil, err\s*\}`)
	if !wiring.MatchString(body) {
		t.Error("ScanVulnerabilities must build its targets with scantarget.Build(ctx, store, repoID, transitive) and return its error")
	}
}
