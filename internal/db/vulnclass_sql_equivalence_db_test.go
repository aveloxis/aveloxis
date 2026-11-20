// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// TestVulnClassAgreesWithTheSQLPredicates holds the two forms of the
// classification equal (summary/70, 4.1.3). The counts the database computes
// use exposurePredicateSQL, unknownVersionSQL and criticalSQL; the API's
// counts and both SBOM annotators use internal/vulnclass over the rows the
// store reads. A repository must not read one way on its tile and another in
// its list, so every combination of the columns the rule reads is stored, and
// each row is classified both ways.
//
// NULL is not in the matrix because it cannot be stored: the columns the rule
// reads are NOT NULL (the first insert of one here was refused), so the
// COALESCEs in the SQL form have nothing to fold and "" is the only "unset".
func TestVulnClassAgreesWithTheSQLPredicates(t *testing.T) {
	store, ctx := v0251Connect(t)
	suffix := time.Now().UnixNano()
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avvulnclasseq', $2, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avvulnclasseq/r%d", suffix), fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	kinds := []string{"", "direct", "transitive", "self"}
	resolutions := []string{"", "locked", "exact", "bounded-range", "range-floor", "unpinned"}
	ecosystems := []string{"", "PyPI", "npm", "githubactions", "GitHubActions"}
	severities := []struct {
		label string
		score float64
	}{{"CRITICAL", 9.8}, {"CRITICAL", 0}, {"HIGH", 9.0}, {"HIGH", 8.9}, {"", 0}, {"critical", 0}}
	want := 0
	for ki, kind := range kinds {
		for ri, res := range resolutions {
			for ei, eco := range ecosystems {
				for si, sev := range severities {
					for _, resolved := range []bool{false, true} {
						id := fmt.Sprintf("EQ-%d-%d-%d-%d-%v", ki, ri, ei, si, resolved)
						var resolvedAt any
						if resolved {
							resolvedAt = time.Now()
						}
						mustExecRetry(ctx, t, store, `
							INSERT INTO aveloxis_data.repo_deps_vulnerabilities
								(repo_id, vuln_id, package_name, package_purl, ecosystem, severity, cvss_score,
								 dependency_kind, version_resolution, resolved_at)
							VALUES ($1, $2, 'p', $3, $4, $5, $6, $7, $8, $9)`,
							repoID, id, "pkg:generic/"+id, eco, sev.label, sev.score,
							kind, res, resolvedAt)
						want++
					}
				}
			}
		}
	}

	// What SQL says of each row.
	type verdict struct{ exposure, unknown, critical bool }
	sqlSays := map[string]verdict{}
	rows, err := store.pool.Query(ctx, `
		SELECT vuln_id,
		       (resolved_at IS NULL`+exposurePredicateSQL+`),
		       (`+unknownVersionSQL+`),
		       `+criticalSQL+`
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var v verdict
		if err := rows.Scan(&id, &v.exposure, &v.unknown, &v.critical); err != nil {
			t.Fatal(err)
		}
		sqlSays[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// What Go says of the same rows, as the store's reader hands them over.
	found, err := store.GetRepoVulnerabilities(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != want || len(sqlSays) != want {
		t.Fatalf("read %d rows in Go and %d in SQL, want %d each", len(found), len(sqlSays), want)
	}
	seen := map[vulnclass.Class]int{}
	for _, v := range found {
		sql := sqlSays[v.VulnID]
		class := vulnclass.Of(v)
		seen[class]++
		if got := class == vulnclass.Exposure; got != sql.exposure {
			t.Errorf("%s (kind %q, resolution %q, ecosystem %q): class %q, SQL exposure %v",
				v.VulnID, v.DependencyKind, v.VersionResolution, v.Ecosystem, class, sql.exposure)
		}
		if got := vulnclass.IsVersionNotDetermined(v); got != sql.unknown {
			t.Errorf("%s (kind %q, resolution %q, ecosystem %q): IsVersionNotDetermined %v, SQL %v",
				v.VulnID, v.DependencyKind, v.VersionResolution, v.Ecosystem, got, sql.unknown)
		}
		if got := vulnclass.IsCritical(v); got != sql.critical {
			t.Errorf("%s (severity %q, score %v): IsCritical %v, SQL %v", v.VulnID, v.Severity, v.CVSSScore, got, sql.critical)
		}
	}
	// Guard the denominator: every class must have been exercised.
	for _, c := range []vulnclass.Class{vulnclass.Closed, vulnclass.Self, vulnclass.VersionNotDetermined, vulnclass.Exposure} {
		if seen[c] == 0 {
			t.Errorf("no row of class %q was examined", c)
		}
	}
}
