// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Each finding records when it ENTERED EXPOSURE in its repository
// (summary/70, 4.3). The digest mails findings by that time. First detection
// cannot serve: since 0.29.165 a successor carries its predecessor's first
// detection, so a finding that has only just become exposure (its package's
// version was worked out) carries an old date and would never be mailed.

func enteredExposure(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64, vulnID string) *time.Time {
	t.Helper()
	var at *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT entered_exposure_at FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND vuln_id = $2`, repoID, vulnID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func upsertOne(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64, v model.VulnerabilityRow) {
	t.Helper()
	v.PackageName, v.Source = "p", "osv.dev"
	if v.Ecosystem == "" {
		v.Ecosystem = "PyPI"
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{&v}); err != nil {
		t.Fatal(err)
	}
}

func TestEnteredExposureIsSetWhenTheClassBecomesExposure(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "entered-exposure")
	start := time.Now().Add(-time.Second)

	// By insert: an exposure row enters exposure now; a row of another class
	// (no declared version; the repository's own package) has no such time.
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "exposure", PackagePurl: "pkg:pypi/a@1.0", DependencyKind: "direct", VersionResolution: "locked"})
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "unpinned", PackagePurl: "pkg:pypi/b", DependencyKind: "direct", VersionResolution: "unpinned"})
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "self", PackagePurl: "pkg:pypi/own", DependencyKind: "self"})
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "action", PackagePurl: "pkg:githubactions/o/r@v4", Ecosystem: "githubactions", DependencyKind: "direct", VersionResolution: "unpinned"})
	entered := enteredExposure(t, store, ctx, repoID, "exposure")
	if entered == nil || entered.Before(start) {
		t.Fatalf("an exposure row inserted now entered exposure at %v", entered)
	}
	if at := enteredExposure(t, store, ctx, repoID, "action"); at == nil {
		t.Error("a GitHub Actions finding at a floating ref is exposure and must have the time")
	}
	for _, id := range []string{"unpinned", "self"} {
		if at := enteredExposure(t, store, ctx, repoID, id); at != nil {
			t.Errorf("%s is not exposure and entered exposure at %v", id, at)
		}
	}

	// Seen again, still exposure: the time does not move.
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "exposure", PackagePurl: "pkg:pypi/a@1.0", DependencyKind: "direct", VersionResolution: "locked"})
	if at := enteredExposure(t, store, ctx, repoID, "exposure"); !at.Equal(*entered) {
		t.Errorf("seen again: entered exposure moved from %v to %v", entered, at)
	}

	// By a change in place: the requirement was edited at an unchanged purl
	// (no version -> a version is declared), and the repository's own
	// package became an ordinary dependency.
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "unpinned", PackagePurl: "pkg:pypi/b", DependencyKind: "direct", VersionResolution: "range-floor"})
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "self", PackagePurl: "pkg:pypi/own", DependencyKind: "direct", VersionResolution: "exact"})
	for _, id := range []string{"unpinned", "self"} {
		if at := enteredExposure(t, store, ctx, repoID, id); at == nil || at.Before(start) {
			t.Errorf("%s became exposure in place; entered exposure at %v, want now", id, at)
		}
	}

	// By reopening: closed, then reported again.
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, nil, model.DecisionGuard{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "exposure", PackagePurl: "pkg:pypi/a@1.0", DependencyKind: "direct", VersionResolution: "locked"})
	if at := enteredExposure(t, store, ctx, repoID, "exposure"); at == nil || !at.After(*entered) {
		t.Errorf("reopened: entered exposure %v, want later than the first time %v", at, entered)
	}
}

// A successor of a finding that was itself exposure when it closed (a version
// bump to a still-affected version) takes its predecessor's time, so it is
// not mailed again. A predecessor with no time (written before this release)
// leaves the successor's NULL, and the digest reads first detection instead.
func TestEnteredExposureIsCarried(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "entered-carried")
	carried := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "carried", PackagePurl: "pkg:pypi/a@2.0", DependencyKind: "direct", VersionResolution: "locked",
		EnteredExposureCarried: true, EnteredExposureAt: &carried})
	if at := enteredExposure(t, store, ctx, repoID, "carried"); at == nil || !at.Equal(carried) {
		t.Errorf("a successor handed its predecessor's time: %v, want %v", at, carried)
	}
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "carried-unknown", PackagePurl: "pkg:pypi/b@2.0", DependencyKind: "direct", VersionResolution: "locked",
		EnteredExposureCarried: true})
	if at := enteredExposure(t, store, ctx, repoID, "carried-unknown"); at != nil {
		t.Errorf("a successor of a predecessor with no time: %v, want NULL (the digest reads first detection)", at)
	}

	// Reopened AND the target of a carry (a bump, then a revert): the
	// carried time wins over "now".
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "reverted", PackagePurl: "pkg:pypi/c@1.0", DependencyKind: "direct", VersionResolution: "locked"})
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"carried|pkg:pypi/a@2.0", "carried-unknown|pkg:pypi/b@2.0"}, nil, false, nil, model.DecisionGuard{}); err != nil {
		t.Fatal(err)
	}
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "reverted", PackagePurl: "pkg:pypi/c@1.0", DependencyKind: "direct", VersionResolution: "locked",
		EnteredExposureCarried: true, EnteredExposureAt: &carried})
	if at := enteredExposure(t, store, ctx, repoID, "reverted"); at == nil || !at.Equal(carried) {
		t.Errorf("reopened and carried: %v, want the carried time %v", at, carried)
	}

	// A carry onto a row that is ALREADY open exposure does not overwrite
	// its own time (both versions were targets; one went away).
	own := enteredExposure(t, store, ctx, repoID, "carried")
	later := carried.Add(48 * time.Hour)
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "carried", PackagePurl: "pkg:pypi/a@2.0", DependencyKind: "direct", VersionResolution: "locked",
		EnteredExposureCarried: true, EnteredExposureAt: &later})
	if at := enteredExposure(t, store, ctx, repoID, "carried"); !at.Equal(*own) {
		t.Errorf("a carry onto an open exposure row moved its time from %v to %v", own, at)
	}

	// A carry never gives the time to a row that is not exposure.
	upsertOne(t, store, ctx, repoID, model.VulnerabilityRow{VulnID: "not-exposure", PackagePurl: "pkg:pypi/d", DependencyKind: "direct", VersionResolution: "unpinned",
		EnteredExposureCarried: true, EnteredExposureAt: &carried})
	if at := enteredExposure(t, store, ctx, repoID, "not-exposure"); at != nil {
		t.Errorf("a row that is not exposure was given %v", at)
	}
}

// The digest's window reads the time a finding entered exposure, or first
// detection where there is none.
func TestTheDigestReadsEnteredExposure(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "digest-entered")
	longAgo := time.Now().Add(-90 * 24 * time.Hour)
	since := time.Now().Add(-time.Hour)

	base := model.VulnerabilityRow{PackagePurl: "pkg:pypi/a@1.0", DependencyKind: "direct", VersionResolution: "locked", Severity: "CRITICAL", DependencyScope: "runtime"}
	// A successor: detected long ago (carried), in exposure only now. MAILED.
	v := base
	v.VulnID, v.FirstDetectedAt = "DG-newly-exposure", longAgo
	upsertOne(t, store, ctx, repoID, v)
	// A version bump: detected and in exposure long ago (both carried). NOT mailed.
	v = base
	v.VulnID, v.FirstDetectedAt, v.EnteredExposureCarried, v.EnteredExposureAt = "DG-bump", longAgo, true, &longAgo
	upsertOne(t, store, ctx, repoID, v)
	// A row from before this release: no entered time, detected now. MAILED.
	v = base
	v.VulnID = "DG-pre-release-new"
	upsertOne(t, store, ctx, repoID, v)
	// A row from before this release, detected long ago. NOT mailed.
	v = base
	v.VulnID, v.FirstDetectedAt = "DG-pre-release-old", longAgo
	upsertOne(t, store, ctx, repoID, v)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_vulnerabilities SET entered_exposure_at = NULL
		WHERE repo_id = $1 AND vuln_id LIKE 'DG-pre-release-%'`, repoID)

	items, err := store.GetNewVulnerabilityFindings(ctx, since, "LOW", true, true)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, it := range items {
		if it.RepoID == repoID {
			got[it.VulnID] = true
		}
	}
	want := map[string]bool{"DG-newly-exposure": true, "DG-pre-release-new": true}
	for _, id := range []string{"DG-newly-exposure", "DG-bump", "DG-pre-release-new", "DG-pre-release-old"} {
		if got[id] != want[id] {
			t.Errorf("%s: in the digest %v, want %v", id, got[id], want[id])
		}
	}
}

// The predicate's constants and its builder are one spelling.
func TestExposurePredicateHasOneSpelling(t *testing.T) {
	if want := " AND " + exposureExprSQL("dependency_kind", "version_resolution", "ecosystem"); exposurePredicateSQL != want {
		t.Errorf("exposurePredicateSQL = %q\nbuilder gives %q", exposurePredicateSQL, want)
	}
	if want := " AND " + exposureExprSQL("v.dependency_kind", "v.version_resolution", "v.ecosystem"); exposurePredicateSQLv != want {
		t.Errorf("exposurePredicateSQLv = %q\nbuilder gives %q", exposurePredicateSQLv, want)
	}
}

// The upsert spells its three class expressions as text (the write-policy
// analyzer cannot read a concatenated statement); each is the builder's.
func TestTheUpsertSpellsTheExposureClassesAsTheBuilderDoes(t *testing.T) {
	body := srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) InsertVulnerabilityBatch(")
	for name, expr := range map[string]string{
		"the class an inserted row has":      enteredNewClassInsertSQL,
		"the class an updated row will have": enteredNewClassUpdateSQL,
		"the class an updated row had":       enteredOldClassSQL,
	} {
		if n := strings.Count(body, expr); n != 1 {
			t.Errorf("%s appears %d time(s) in the upsert, want 1:\n%s", name, n, expr)
		}
	}
	if n := strings.Count(body, "<> 'self'"); n != 3 {
		t.Errorf("the upsert spells the predicate %d times; the three above are all it may", n)
	}
}
