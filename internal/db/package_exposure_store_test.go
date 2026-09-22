// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.60: the supply-chain package view. The numbers the profile reports
// are what a seeded cohort actually contains, through the live path, the
// view-absent fallback and the views built by their owner
// (supply_chain_views.go, whose body is the same Go SQL — SR-17).

// TestPackageExposureProfileNumbers seeds one package's findings across
// four repositories (three known, one carrying only stub rows) and checks
// every figure the GUI draws, through the live cohort path, the
// view-absent fallback and a freshly built matview.
func TestPackageExposureProfileNumbers(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const pkg = "aveloxis-test-minimatch"
	r1 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "exposure-one")
	r2 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "exposure-two")
	r3 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "exposure-three")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name = $1`, pkg)
	})
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name = $1`, pkg)

	old := time.Now().Add(-60 * 24 * time.Hour)
	recent := time.Now().Add(-10 * 24 * time.Hour)
	seed := func(repo int64, vuln, cve, ver, sev string, cvss float64, fixed, kind string, first time.Time, resolved *time.Time) {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			  (repo_id, vuln_id, cve_id, package_name, package_purl, ecosystem, severity, cvss_score,
			   fixed_version, dependency_kind, first_detected_at, last_seen_at, resolved_at, tool_source, data_source)
			VALUES ($1, $2, $3, $4, $5, 'npm', $6, $7, $8, $9, $10, NOW(), $11, 'aveloxis', 'test')`,
			repo, vuln, cve, pkg, "pkg:npm/"+pkg+"@"+ver, sev, cvss, fixed, kind, first, resolved)
	}
	yesterday := time.Now().Add(-24 * time.Hour)
	seed(r1, "GHSA-A", "CVE-1", "3.1.2", "HIGH", 7.5, "10.2.3", "transitive", old, nil)
	seed(r1, "GHSA-B", "", "3.1.2", "MEDIUM", 5.0, "9.0.0", "transitive", old, &yesterday) // resolved
	seed(r2, "GHSA-A", "CVE-1", "7.4.6", "HIGH", 7.5, "10.2.3", "direct", recent, nil)
	seed(r3, "GHSA-A", "CVE-1", "3.1.2", "HIGH", 7.5, "10.2.3", "transitive", old, nil)
	// Review round 1: two STUB rows (a failed detail fetch stores severity
	// UNKNOWN, no fixed version, cvss 0) on a fourth repository — more stub
	// rows than known ones for GHSA-A. The profile must still report the
	// KNOWN severity and fixed version; frequency voting let the stubs win.
	r4 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "exposure-four")
	seed(r4, "GHSA-A", "", "1.0.0", "UNKNOWN", 0, "", "transitive", old, nil)
	seed(r4, "GHSA-A", "", "1.0.1", "", 0, "", "transitive", old, nil)
	seed(r4, "GHSA-A", "", "1.0.2", "", 0, "", "transitive", old, nil) // three stubs vs three known: a tie

	check := func(label string, e *PackageExposure) {
		t.Helper()
		if e.CohortRepos != 4 || e.ReposUnresolved != 4 || e.Findings != 7 || e.FindingsUnresolved != 6 {
			t.Errorf("%s: repos=%d unresolved_repos=%d findings=%d unresolved=%d, want 4/4/7/6", label, e.CohortRepos, e.ReposUnresolved, e.Findings, e.FindingsUnresolved)
		}
		if math.Abs(e.PctUnresolved-85.7) > 0.05 || math.Abs(e.PctTransitive-85.7) > 0.05 {
			t.Errorf("%s: pct_unresolved=%v pct_transitive=%v, want 85.7/85.7 (6 of 7 rows each)", label, e.PctUnresolved, e.PctTransitive)
		}
		if e.WorstSeverity != "HIGH" || e.MaxCVSS != 7.5 || e.Advisories != 2 {
			t.Errorf("%s: worst=%q max_cvss=%v advisories=%d, want HIGH/7.5/2", label, e.WorstSeverity, e.MaxCVSS, e.Advisories)
		}
		if e.DistinctVersionsInUse != 5 || e.ModalVersion != "3.1.2" || e.ModalVersionSharePct == nil || *e.ModalVersionSharePct != 50 {
			t.Errorf("%s: versions=%d modal=%q share=%v, want 5 / 3.1.2 / 50 (2 of 4 repos with a current version)", label, e.DistinctVersionsInUse, e.ModalVersion, e.ModalVersionSharePct)
		}
		// The two stub rows must not downgrade the lead: severity by RANK
		// (HIGH outranks UNKNOWN and ''), fixed version from the non-empty rows.
		if e.LeadAdvisory != "GHSA-A" || e.LeadCVE != "CVE-1" || e.LeadFixedVersion != "10.2.3" || e.LeadAdvisoryRepos != 4 || e.LeadSeverity != "HIGH" {
			t.Errorf("%s: lead=%s/%s fixed=%s repos=%d sev=%s, want GHSA-A/CVE-1/10.2.3/4/HIGH (stub rows must not outvote known labels)", label, e.LeadAdvisory, e.LeadCVE, e.LeadFixedVersion, e.LeadAdvisoryRepos, e.LeadSeverity)
		}
		// Median age of the CURRENT findings: 60, 10, 60, 60, 60, 60 days → 60.
		if e.MedianDaysOpen == nil || math.Abs(*e.MedianDaysOpen-60) > 0.5 {
			t.Errorf("%s: median_days_open=%v, want ~60 (the resolved one is out)", label, e.MedianDaysOpen)
		}
	}

	// Live cohort path: the four seeded repositories.
	live, isLive, err := store.GetPackageExposure(ctx, "npm", pkg, []int64{r1, r2, r3, r4})
	if err != nil {
		t.Fatalf("live profile: %v", err)
	}
	if !isLive {
		t.Error("a cohort profile must report itself as live")
	}
	check("live", live)

	// A narrower cohort changes the numbers: r1 + r2 only.
	narrow, _, err := store.GetPackageExposure(ctx, "npm", pkg, []int64{r1, r2})
	if err != nil {
		t.Fatal(err)
	}
	if narrow.CohortRepos != 2 || narrow.LeadAdvisoryRepos != 2 || narrow.ModalVersionSharePct == nil || *narrow.ModalVersionSharePct != 50 {
		t.Errorf("narrow cohort: repos=%d lead_repos=%d modal_share=%v, want 2/2/50", narrow.CohortRepos, narrow.LeadAdvisoryRepos, narrow.ModalVersionSharePct)
	}
	// An empty cohort is the typed not-found, never a zero profile.
	if _, _, err := store.GetPackageExposure(ctx, "npm", pkg, []int64{}); !errors.Is(err, ErrPackageNotFound) {
		t.Errorf("empty cohort: err=%v, want ErrPackageNotFound", err)
	}

	// With NO fleet view built (a materialized_views-off deployment, or a
	// fleet before its migrate), the fleet path aggregates live over the
	// whole table instead of failing: same figures.
	for _, stmt := range []string{
		`DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_advisory`,
		`DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_exposure`,
	} {
		mustExecRetry(ctx, t, store, stmt)
	}
	fallback, fbLive, err := store.GetPackageExposure(ctx, "npm", pkg, nil)
	if err != nil {
		t.Fatalf("fleet path without the view must fall back to the live aggregate: %v", err)
	}
	if !fbLive {
		t.Error("the fallback must report itself as live, not as the view")
	}
	check("fallback", fallback)
	if advFB, err := store.GetPackageAdvisories(ctx, "npm", pkg, nil); err != nil || len(advFB) != 2 {
		t.Fatalf("advisories without the view: %v (%d rows)", err, len(advFB))
	}
	if pg, err := store.ListPackageExposure(ctx, PackageExposureQuery{Search: pkg}); err != nil || pg.Total != 1 || !pg.Live {
		t.Fatalf("leaderboard without the view: %v (page %+v)", err, pg)
	}

	// The matview built from the same SQL reports the same figures (the
	// per-package test database holds only this test's findings for pkg).
	if err := CreateSupplyChainViews(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("building the supply-chain views: %v", err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_advisory`)
		cleanupExecRetry(context.Background(), store, `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_exposure`)
	})
	fleet, fleetLive, err := store.GetPackageExposure(ctx, "npm", pkg, nil)
	if err != nil {
		t.Fatalf("matview profile: %v", err)
	}
	if fleetLive {
		t.Error("with the view built the fleet profile must report the view, not live")
	}
	check("matview", fleet)

	// Leaderboard, search and sort through the matview.
	pg, err := store.ListPackageExposure(ctx, PackageExposureQuery{Ecosystem: "npm", Search: "TEST-MINIMATCH", Sort: "repos", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if pg.Total != 1 || len(pg.Rows) != 1 || pg.Rows[0].PackageName != pkg || pg.Live {
		t.Errorf("leaderboard: total=%d rows=%d live=%v, want the one seeded package from the view", pg.Total, len(pg.Rows), pg.Live)
	}
	// A page past the end still reports the total (the window count rides
	// on the rows; with no rows the total comes from a count of the set).
	if pg, err := store.ListPackageExposure(ctx, PackageExposureQuery{Search: "TEST-MINIMATCH", Limit: 10, Offset: 50}); err != nil || pg.Total != 1 || len(pg.Rows) != 0 {
		t.Errorf("leaderboard past the end: page=%+v err=%v, want total 1 and no rows", pg, err)
	}
	// An unknown sort key falls back rather than interpolating.
	if _, err := store.ListPackageExposure(ctx, PackageExposureQuery{RepoIDs: []int64{r1, r2, r3}, Sort: "1; DROP TABLE x"}); err != nil {
		t.Errorf("unknown sort key must fall back to the default: %v", err)
	}

	// Advisories, versions and repositories.
	adv, err := store.GetPackageAdvisories(ctx, "npm", pkg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(adv) != 2 || adv[0].VulnID != "GHSA-A" || adv[0].Repos != 4 || adv[0].ReposUnresolved != 4 || adv[1].ReposUnresolved != 0 {
		t.Errorf("advisories = %+v, want GHSA-A on 4 repos (4 unresolved) then GHSA-B (0 unresolved)", adv)
	}
	if adv[0].Severity != "HIGH" || adv[0].FixedVersion != "10.2.3" || adv[0].MaxCVSS != 7.5 {
		t.Errorf("GHSA-A advisory row = %+v, want HIGH / 10.2.3 / 7.5 despite two stub rows", adv[0])
	}
	vers, err := store.GetPackageVersionsInUse(ctx, "npm", pkg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(vers) != 5 || vers[0].Version != "3.1.2" || vers[0].Repos != 2 {
		t.Errorf("versions = %+v, want 3.1.2×2 first, then four singletons", vers)
	}
	repos, err := store.GetPackageExposedRepos(ctx, "npm", pkg, []int64{r1, r2, r3, r4}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(repos) != 4 {
		t.Fatalf("exposed repos = %+v, want 4", repos)
	}
	for _, r := range repos {
		if r.RepoID == r2 && (r.Transitive || len(r.Versions) != 1 || r.Versions[0] != "7.4.6") {
			t.Errorf("repo two is a DIRECT exposure at 7.4.6, got %+v", r)
		}
		if r.RepoID == r1 && (!r.Transitive || r.FindingsUnresolved != 1) {
			t.Errorf("repo one: transitive with one current finding (the resolved one is out), got %+v", r)
		}
	}
}

// TestPackageVersionIsReadAfterTheLastAt — review round 1: a legacy raw
// scoped npm purl (pkg:npm/@scope/name@1.0.0, preserved by the scan for
// rows minted before v0.27.29) has an '@' before the version; the first
// '@' is the scope marker. Both spellings must report version 1.0.0, and a
// versionless raw scoped purl reports no version at all.
func TestPackageVersionIsReadAfterTheLastAt(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const pkg = "aveloxis-test-scoped"
	r1 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "scoped-one")
	r2 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "scoped-two")
	r3 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "scoped-three")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name = $1`, pkg)
	})
	for _, row := range []struct {
		repo int64
		purl string
	}{
		{r1, "pkg:npm/%40scope/" + pkg + "@1.0.0"}, // canonical (buildPurl escapes the scope's @)
		{r2, "pkg:npm/@scope/" + pkg + "@1.0.0"},   // legacy raw scoped purl
		{r3, "pkg:npm/@scope/" + pkg},              // legacy, versionless
	} {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			  (repo_id, vuln_id, package_name, package_purl, ecosystem, severity, cvss_score, dependency_kind, tool_source, data_source)
			VALUES ($1, 'GHSA-S', $2, $3, 'npm', 'HIGH', 7.0, 'direct', 'aveloxis', 'test')`, row.repo, pkg, row.purl)
	}
	vers, err := store.GetPackageVersionsInUse(ctx, "npm", pkg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(vers) != 1 || vers[0].Version != "1.0.0" || vers[0].Repos != 2 {
		t.Fatalf("versions = %+v, want exactly 1.0.0 on 2 repositories (the scope is not a version)", vers)
	}
	e, _, err := store.GetPackageExposure(ctx, "npm", pkg, []int64{r1, r2, r3})
	if err != nil {
		t.Fatal(err)
	}
	if e.DistinctVersionsInUse != 1 || e.ModalVersion != "1.0.0" || e.ReposWithKnownVersions != 2 {
		t.Errorf("profile versions=%d modal=%q known=%d, want 1 / 1.0.0 / 2", e.DistinctVersionsInUse, e.ModalVersion, e.ReposWithKnownVersions)
	}
	repos, err := store.GetPackageExposedRepos(ctx, "npm", pkg, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range repos {
		if r.RepoID == r3 && len(r.Versions) != 0 {
			t.Errorf("versionless raw purl must yield no version, got %v", r.Versions)
		}
		if r.RepoID == r2 && (len(r.Versions) != 1 || r.Versions[0] != "1.0.0") {
			t.Errorf("raw scoped purl must yield 1.0.0, got %v", r.Versions)
		}
	}
}

// TestGroupScopeRequiresAnExistingGroup — review round 1: an admin asking
// for an unknown group id got an empty cohort (200) instead of the typed
// not-found; a non-admin must also own the group.
func TestGroupScopeRequiresAnExistingGroup(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if _, err := store.GetGroupRepoIDsForUser(ctx, 999999999, 1, true); !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("admin + unknown group: err=%v, want ErrGroupNotFound", err)
	}
	if _, err := store.GetGroupRepoIDsForUser(ctx, 999999999, 1, false); !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("user + unknown group: err=%v, want ErrGroupNotFound", err)
	}
}

// TestLeaderboardAggregatesOnce — review round 2 (L17d): the single-pass
// page (the total rides on the rows as a window count) is an optimization
// a body-only assertion cannot defend. The pin: one window count in the
// page query, and the only standalone COUNT(*) lives inside the
// past-the-end guard.
func TestLeaderboardAggregatesOnce(t *testing.T) {
	src, err := os.ReadFile("package_exposure_store.go")
	if err != nil {
		t.Fatal(err)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(src), "func (s *PostgresStore) ListPackageExposure("))
	if n := strings.Count(body, "COUNT(*) OVER ()"); n != 1 {
		t.Errorf("ListPackageExposure must carry the total as one window count, found %d", n)
	}
	count := strings.Index(body, "SELECT COUNT(*) FROM")
	guard := strings.Index(body, "len(page.Rows) == 0 && offset > 0")
	if strings.Count(body, "SELECT COUNT(*) FROM") != 1 || guard < 0 || count < guard {
		t.Error("the only standalone COUNT(*) must sit inside the past-the-end guard; a second count re-runs the whole aggregate")
	}
}
