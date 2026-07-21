// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.11 — unit tests for the pure per-dependency scan decisions:
// self-dep exclusion, lockfile-version precedence, the Go
// locked-by-construction rule, and purl version substitution.

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestIsSelfDependency(t *testing.T) {
	selfSet := map[string]bool{
		"apache-airflow":                  true,
		"apache-airflow-providers-google": true,
		"airflow":                         true,
	}
	cases := []struct {
		name string
		want bool
	}{
		{"apache-airflow", true},
		{"Apache-Airflow", true}, // case-insensitive
		{"apache_airflow", true}, // '_'→'-' folded on the dep side
		{"apache-airflow-providers-google", true},
		{"apache-airflow-providers-amazon", false}, // exact only — NO prefix rule
		{"flask", false},
		{"airflow-lookalike", false},
	}
	for _, c := range cases {
		if got := isSelfDependency(c.name, selfSet); got != c.want {
			t.Errorf("isSelfDependency(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// Go needs NO lockfile: go.mod versions are exact under MVS, so Go
// deps classify 'locked' by construction.
func TestGoDepsAreLockedByConstruction(t *testing.T) {
	dep := db.VulnScanDep{Name: "golang.org/x/text", CurrentVersion: "v0.3.0",
		PackageManager: "go", Purl: "pkg:golang/golang.org/x/text@v0.3.0",
		Requirement: "golang.org/x/text v0.3.0"}
	targets := vulnScanTargets(dep, nil)
	if len(targets) != 1 || targets[0].Resolution != resolutionLocked {
		t.Fatalf("go deps must be 'locked' by construction, got %+v", targets)
	}
	if targets[0].Purl != dep.Purl {
		t.Errorf("go purl must stay the go.mod version: %s", targets[0].Purl)
	}
}

func TestLockedVersionsWinOverFloor(t *testing.T) {
	// v0.27.29: the stored purl is SPEC-CANONICAL (lowercase,
	// '_'→'-') — the pre-v0.27.29 version of this test pinned
	// "pkg:pypi/Flask_SQLAlchemy" as correct, the wrong-answer-tests
	// audit's headline instance. The display Name keeps the
	// manifest's spelling; only the purl canonicalizes. Pre-existing
	// DB rows with non-canonical purls heal at each repo's next
	// analysis pass.
	dep := db.VulnScanDep{Name: "Flask_SQLAlchemy", CurrentVersion: "2.0",
		PackageManager: "pypi", Purl: "pkg:pypi/flask-sqlalchemy@2.0",
		Requirement: "Flask_SQLAlchemy>=2.0"}
	locked := map[string][]string{
		// PEP 503 folding on both sides: the lockfile spelled it
		// flask-sqlalchemy.
		lockfileMatchKey("pypi", "flask-sqlalchemy"): {"2.5.1", "3.1.1"},
	}
	targets := vulnScanTargets(dep, locked)
	if len(targets) != 2 {
		t.Fatalf("every distinct locked version yields a purl, got %+v", targets)
	}
	for _, tgt := range targets {
		if tgt.Resolution != resolutionLocked {
			t.Errorf("locked resolution class wrong: %+v", tgt)
		}
		if tgt.Requirement != "Flask_SQLAlchemy>=2.0" {
			t.Errorf("raw requirement must ride along: %+v", tgt)
		}
	}
	got := map[string]bool{targets[0].Purl: true, targets[1].Purl: true}
	if !got["pkg:pypi/flask-sqlalchemy@2.5.1"] || !got["pkg:pypi/flask-sqlalchemy@3.1.1"] {
		t.Errorf("locked purls must substitute the resolved versions onto the canonical purl: %v", got)
	}
}

func TestNoLockfileFallsBackToFloorClassification(t *testing.T) {
	dep := db.VulnScanDep{Name: "apache-airflow", CurrentVersion: "3.0.0",
		PackageManager: "pypi", Purl: "pkg:pypi/apache-airflow@3.0.0",
		Requirement: "apache-airflow>=3.0.0"}
	targets := vulnScanTargets(dep, map[string][]string{})
	if len(targets) != 1 {
		t.Fatalf("got %+v", targets)
	}
	if targets[0].Purl != dep.Purl {
		t.Errorf("the FLOOR stays the scanned version (operator rejected latest-satisfying resolution): %s", targets[0].Purl)
	}
	if targets[0].Resolution != resolutionRangeFloor {
		t.Errorf("class must be range-floor, got %s", targets[0].Resolution)
	}
	// A dep with no purl yields no targets (unchanged behavior).
	if got := vulnScanTargets(db.VulnScanDep{Name: "x"}, nil); got != nil {
		t.Errorf("purl-less deps must yield no targets: %+v", got)
	}
}

func TestPurlWithVersion(t *testing.T) {
	cases := []struct{ purl, version, want string }{
		{"pkg:npm/express@4.18.0", "4.19.2", "pkg:npm/express@4.19.2"},
		{"pkg:npm/@scope/name@1.0.0", "2.0.0", "pkg:npm/@scope/name@2.0.0"},
		{"pkg:npm/@scope/name", "2.0.0", "pkg:npm/@scope/name@2.0.0"}, // scope @ is not a version separator
		{"pkg:pypi/flask@", "2.3.3", "pkg:pypi/flask@2.3.3"},
	}
	for _, c := range cases {
		if got := purlWithVersion(c.purl, c.version); got != c.want {
			t.Errorf("purlWithVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
		}
	}
}

// ---------- source pins ----------

// The scan applies self-set exclusion FIRST, then locked lookup, and
// reads deps through GetRepoDepsForVulnScan (the DB-backed path that
// `aveloxis heal-vulnerabilities` — no fresh analysis pass — uses too).
func TestScanWiringOrderAndSources(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"GetRepoDepsForVulnScan(", "GetRepoSelfPackageNames(", "GetRepoLockedVersions(",
		"isSelfDependency(", "vulnScanTargets(",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("ScanVulnerabilities must call %s", needle)
		}
	}
	// Order: self-exclusion before target/locked selection, both before
	// the OSV batch request is built.
	selfIdx := strings.Index(s, "isSelfDependency(")
	targetIdx := strings.Index(s, "vulnScanTargets(")
	batchIdx := strings.Index(s, "buildOSVBatchRequest(")
	if !(selfIdx < targetIdx && targetIdx < batchIdx) {
		t.Error("purl construction order must be: self-set exclusion → locked/classified targets → OSV batch")
	}
	// The legacy libyear-read entry point must be GONE from the scan —
	// the vuln path reads requirement+purl through its own method.
	if strings.Contains(s, "GetRepoLibyearDeps(") {
		t.Error("vulnerability.go must not use GetRepoLibyearDeps — the scan reads via GetRepoDepsForVulnScan (carries the raw requirement)")
	}
	// One INFO line per scan for exclusions, with samples.
	if !strings.Contains(s, "self") || !strings.Contains(s, "samples") {
		t.Error("the self-dep exclusion must log ONE INFO line per scan with count + samples")
	}
}

// HARD-RULE tripwires: the self-dep/lockfile machinery applies to the
// VULNERABILITY path only. Libyear (analysis.go's scanLibyear + the
// registry resolvers) keeps every dep — including self-deps — at the
// floor convention, completely untouched.
func TestLibyearPathUntouchedBySelfDepAndLockfileLogic(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, forbidden := range []string{
		"isSelfDependency(", "GetRepoSelfPackageNames(", "vulnScanTargets(",
		"GetRepoLockedVersions(", "classifyRequirement(",
	} {
		if strings.Contains(s, forbidden) {
			t.Errorf("analysis.go must NOT reference %s — self-dep exclusion and lockfile "+
				"resolution apply ONLY to the vulnerability scan; libyear is untouched "+
				"(v0.27.11 hard rule)", forbidden)
		}
	}
	// cleanVersion's floor convention is load-bearing for libyear and
	// for the vuln floor purls alike — pin its presence unchanged.
	if !strings.Contains(s, "func cleanVersion(") {
		t.Error("cleanVersion (the floor convention) must remain in analysis.go")
	}
}
