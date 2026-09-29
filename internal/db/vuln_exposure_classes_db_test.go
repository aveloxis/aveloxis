// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.70 (operator, 2026-09-29, the pytorch vulnerabilities page): an
// UNPINNED dependency declares no version, so the scanner queried OSV without
// one and OSV answered with every advisory ever published for the package
// (PyYAML's 2017 CVE, fixed in 5.1, on a project that installs PyYAML 6).
// Those rows say "this package has had advisories", not "this repository is
// exposed" — the VEX under_investigation class. Like the project's own
// release advisories (kind 'self', v0.27.29), they leave every exposure
// count and read, and are counted apart. One predicate for all of them
// (SR-17): exposurePredicateSQL.

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestUnpinnedFindingsAreNotExposure(t *testing.T) {
	store, ctx := v0251Connect(t)
	suffix := time.Now().UnixNano()
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avvulnclass', $2, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avvulnclass/r%d", suffix), fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	mk := func(id, pkg, purl, sev, resolution, kind string) *VulnerabilityRow {
		eco := "PyPI"
		if strings.HasPrefix(purl, "pkg:githubactions/") {
			eco = "githubactions"
		}
		return &VulnerabilityRow{VulnID: id, PackageName: pkg, PackagePurl: purl, Ecosystem: eco,
			Severity: sev, CVSSScore: map[string]float64{"CRITICAL": 9.8, "HIGH": 7.5}[sev], Source: "osv.dev",
			VersionResolution: resolution, DependencyKind: kind, DependencyScope: "runtime"}
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{
		mk("GHSA-lock-0001", "lockedpkg", "pkg:pypi/lockedpkg@1.0", "CRITICAL", "locked", "direct"),
		mk("GHSA-flor-0001", "floorpkg", "pkg:pypi/floorpkg@2.0", "HIGH", "range-floor", "direct"),
		mk("GHSA-unpn-0001", "unpinnedpkg", "pkg:pypi/unpinnedpkg", "CRITICAL", "unpinned", "direct"),
		mk("GHSA-unpn-0002", "unpinnedpkg", "pkg:pypi/unpinnedpkg", "HIGH", "unpinned", "direct"),
		mk("PYSEC-self-0001", "selfpkg", "pkg:pypi/selfpkg", "CRITICAL", "", "self"),
		// Review round 6 F1: a floating GitHub Actions ref (@v4, @main) is
		// stored 'unpinned', but its advisory was MATCHED against the ref
		// (actionRefAffected) — real exposure, not an unknown version.
		mk("GHSA-actn-0001", "tj-actions/changed-files", "pkg:githubactions/tj-actions/changed-files@v35", "HIGH", "unpinned", "direct"),
	}); err != nil {
		t.Fatal(err)
	}

	// The repository tile and every CountRepoVulnerabilities caller.
	c, err := store.CountRepoVulnerabilityClasses(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Exposure != 3 || c.Critical != 1 || c.UnknownVersion != 2 || c.UnknownVersionCritical != 1 {
		t.Errorf("classes = %+v; want exposure 3 (the Actions ref counts), critical 1, unknown-version 2 (1 critical)", c)
	}
	if total, critical, err := store.CountRepoVulnerabilities(ctx, repoID); err != nil || total != 3 || critical != 1 {
		t.Errorf("CountRepoVulnerabilities = %d, %d, %v; want 3, 1 — unpinned (not Actions) and self are not exposure", total, critical, err)
	}
	// The single and batch stats agree (the v0.28.7 rule).
	st, err := store.GetRepoStats(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Vulnerabilities != 3 || st.CriticalVulns != 1 || st.VulnsVersionUnknown != 2 {
		t.Errorf("single stats = %d/%d/%d; want 3/1/2", st.Vulnerabilities, st.CriticalVulns, st.VulnsVersionUnknown)
	}
	batch, err := store.GetRepoStatsBatch(ctx, []int64{repoID})
	if err != nil {
		t.Fatal(err)
	}
	if b := batch[repoID]; b == nil || b.Vulnerabilities != 3 || b.CriticalVulns != 1 || b.VulnsVersionUnknown != 2 {
		t.Errorf("batch stats = %+v; want 3/1/2", b)
	}
	// The operator digest never mails an unknown-version advisory.
	items, err := store.GetNewVulnerabilityFindings(ctx, time.Now().Add(-time.Hour), "LOW", true, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.RepoID == repoID && strings.HasPrefix(it.VulnID, "GHSA-unpn") {
			t.Errorf("the digest carries unknown-version advisory %s", it.VulnID)
		}
	}
	// The supply-chain package reads do not count the repository as exposed.
	if _, _, err := store.GetPackageExposure(ctx, "PyPI", "unpinnedpkg", []int64{repoID}); !errors.Is(err, ErrPackageNotFound) {
		t.Errorf("GetPackageExposure(unpinnedpkg) err=%v; want ErrPackageNotFound — an unpinned repository is not exposed", err)
	}
	if _, _, err := store.GetPackageExposure(ctx, "PyPI", "floorpkg", []int64{repoID}); err != nil {
		t.Errorf("GetPackageExposure(floorpkg) err=%v; a range floor is still exposure", err)
	}
	if _, _, err := store.GetPackageExposure(ctx, "githubactions", "tj-actions/changed-files", []int64{repoID}); err != nil {
		t.Errorf("GetPackageExposure(action) err=%v; a floating Actions ref matched against its ref is exposure", err)
	}
	found := false
	for _, it := range items {
		found = found || (it.RepoID == repoID && it.VulnID == "GHSA-actn-0001")
	}
	if !found {
		t.Error("the digest must carry the floating Actions ref's advisory")
	}
}

// TestEveryExposureReadUsesThePredicate: no internal/db statement may spell
// the self exclusion by hand — a hand spelling is how a new read would
// forget the unpinned half (SR-17). The migrate.go scope backfill is the one
// reviewed exception: it writes scope, it does not read exposure.
func TestEveryExposureReadUsesThePredicate(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 20)
	examined := 0
	for name, src := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		code := srctest.StripGoComments(src)
		n := strings.Count(code, "<> 'self'")
		if n == 0 {
			continue
		}
		examined += n
		allowed := 0
		if strings.HasSuffix(name, "vuln_exposure.go") {
			allowed = 3 // the two predicate spellings and unknownVersionSQL
		}
		if strings.HasSuffix(name, "migrate.go") {
			allowed = 1 // the v0.27.51 scope backfill
		}
		if n > allowed {
			t.Errorf("%s spells the self exclusion by hand %d time(s) — use exposurePredicateSQL so unpinned findings leave the read too", name, n-allowed)
		}
	}
	if examined < 2 {
		t.Fatalf("examined %d spellings — the scan broke (the predicate and the migrate backfill alone are 2)", examined)
	}
}

// TestStatsAgreeComparesEveryClass — review round 7: data-verify's
// batch-vs-single check compared total and critical only; the
// unknown-version count must agree too.
func TestStatsAgreeComparesEveryClass(t *testing.T) {
	c := VulnClassCounts{Exposure: 3, Critical: 1, UnknownVersion: 104}
	if !statsAgree(&RepoStats{Vulnerabilities: 3, CriticalVulns: 1, VulnsVersionUnknown: 104}, c) {
		t.Error("identical counts must agree")
	}
	for _, b := range []*RepoStats{
		{Vulnerabilities: 2, CriticalVulns: 1, VulnsVersionUnknown: 104},
		{Vulnerabilities: 3, CriticalVulns: 0, VulnsVersionUnknown: 104},
		{Vulnerabilities: 3, CriticalVulns: 1, VulnsVersionUnknown: 103},
	} {
		if statsAgree(b, c) {
			t.Errorf("%+v must disagree with %+v", b, c)
		}
	}
	if statsAgree(nil, c) {
		t.Error("a missing batch row must disagree")
	}
}
