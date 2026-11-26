// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A closure with a reason is not a resolution (summary/70, 4.2). The package
// reads leave such rows out of every existing figure, in the live path and in
// the views alike, and count them in one new one. A package or an advisory
// whose only findings are such closures has no row.
func TestPackageReadsLeaveOutReasonedClosures(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const pkg, only = "aveloxis-test-reasoned", "aveloxis-test-reasoned-only"
	r1 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "reasoned-one")
	r2 := seedRepoForDeps(t, store, ctx, "aveloxis-it", "reasoned-two")
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name IN ($1, $2)`, pkg, only)
	}
	t.Cleanup(clean)
	clean()

	old := time.Now().Add(-60 * 24 * time.Hour)
	closed := time.Now().Add(-24 * time.Hour)
	seed := func(repo int64, name, vuln, ver, sev string, cvss float64, resolved *time.Time, reason *string) {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			  (repo_id, vuln_id, cve_id, package_name, package_purl, ecosystem, severity, cvss_score,
			   fixed_version, dependency_kind, first_detected_at, last_seen_at, resolved_at, closed_reason, tool_source, data_source)
			VALUES ($1, $2, '', $3, $4, 'npm', $5, $6, '', 'direct', $7, NOW(), $8, $9, 'aveloxis', 'test')`,
			repo, vuln, name, "pkg:npm/"+name+"@"+ver, sev, cvss, old, resolved, reason)
	}
	changed, determined := "version_changed", "version_determined"
	seed(r1, pkg, "GHSA-A", "2.0.0", "HIGH", 7.5, nil, nil)                 // open
	seed(r1, pkg, "GHSA-A", "1.0.0", "HIGH", 7.5, &closed, &changed)        // its predecessor: a bump, not a fix
	seed(r1, pkg, "GHSA-B", "1.0.0", "MEDIUM", 5.0, &closed, nil)           // a real fix
	seed(r2, pkg, "GHSA-C", "1.0.0", "CRITICAL", 9.8, &closed, &determined) // only ever a reasoned closure
	seed(r2, only, "GHSA-D", "1.0.0", "HIGH", 7.5, &closed, &determined)    // a package with nothing else

	check := func(label string, e *PackageExposure) {
		t.Helper()
		// Rows counted: the open one and the real fix. r2 has only a reasoned
		// closure, so it was never exposed; GHSA-C is no advisory of this
		// profile, and its CRITICAL is not the worst severity.
		if e.CohortRepos != 1 || e.ReposUnresolved != 1 || e.Findings != 2 || e.FindingsUnresolved != 1 || e.PctUnresolved != 50 {
			t.Errorf("%s: repos=%d unresolved_repos=%d findings=%d unresolved=%d pct=%v; want 1/1/2/1/50", label, e.CohortRepos, e.ReposUnresolved, e.Findings, e.FindingsUnresolved, e.PctUnresolved)
		}
		if e.Advisories != 2 || e.WorstSeverity != "HIGH" || e.MaxCVSS != 7.5 {
			t.Errorf("%s: advisories=%d worst=%q max_cvss=%v; want 2/HIGH/7.5 (the reasoned CRITICAL is out)", label, e.Advisories, e.WorstSeverity, e.MaxCVSS)
		}
		if e.LeadAdvisory != "GHSA-A" || e.LeadAdvisoryRepos != 1 {
			t.Errorf("%s: lead=%s over %d repos; want GHSA-A over 1", label, e.LeadAdvisory, e.LeadAdvisoryRepos)
		}
		if e.ClosedWithReason != 2 {
			t.Errorf("%s: closed_with_reason=%d, want 2", label, e.ClosedWithReason)
		}
	}
	cohort := []int64{r1, r2}

	live, _, err := store.GetPackageExposure(ctx, "npm", pkg, cohort)
	if err != nil {
		t.Fatal(err)
	}
	check("live", live)
	if _, _, err := store.GetPackageExposure(ctx, "npm", only, cohort); !errors.Is(err, ErrPackageNotFound) {
		t.Errorf("live: a package with only reasoned closures: err %v, want ErrPackageNotFound", err)
	}
	advisories, err := store.GetPackageAdvisories(ctx, "npm", pkg, cohort)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]PackageAdvisory{}
	for _, a := range advisories {
		ids[a.VulnID] = a
	}
	if len(ids) != 2 || ids["GHSA-A"].Repos != 1 || ids["GHSA-B"].Repos != 1 {
		t.Errorf("live advisories = %+v; want GHSA-A and GHSA-B, and no row for GHSA-C", advisories)
	}

	// The views: the same bodies, and the new column is the LAST one (a
	// reader selecting by position keeps what it had).
	var viewBody = PackageExposureMatviewSQL()
	rows, err := store.pool.Query(ctx, "SELECT * FROM ("+viewBody+") x WHERE package_name = ANY($1)", []string{pkg, only})
	if err != nil {
		t.Fatal(err)
	}
	fields := rows.FieldDescriptions()
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if last := fields[len(fields)-1].Name; last != "closed_with_reason" {
		t.Errorf("the view's last column is %q, want closed_with_reason", last)
	}
	if n != 1 {
		t.Errorf("the view body has %d rows for the two packages, want 1 (none for the one with only reasoned closures)", n)
	}
	var advisoryRows int
	if err := store.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ("+PackageAdvisoryMatviewSQL()+") x WHERE package_name = ANY($1)", []string{pkg, only}).Scan(&advisoryRows); err != nil {
		t.Fatal(err)
	}
	if advisoryRows != 2 {
		t.Errorf("the advisory view body has %d rows, want 2", advisoryRows)
	}
}
