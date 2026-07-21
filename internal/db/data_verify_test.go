// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.43 (summary/18 Phase 5) tests: the data-verify probe battery
// plus tripwires (3) batch==single stats and (4) bridge-intersection
// containment, both run against the real schema.

package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRunDataVerificationOnScratchDB (AVELOXIS_TEST_DB): the battery
// must complete on a real database, produce findings for every probe,
// and contain no FAILs on the healthy scratch schema. This is also the
// tripwire-(4) home: "new cross-kind collisions" verifies bridge
// intersection ⊆ heal worklist against real data.
func TestRunDataVerificationOnScratchDB(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })

	results := store.RunDataVerification(ctx, VerifyOptions{Sample: 50})
	if len(results) < 8 {
		t.Fatalf("expected the full probe battery (>=8 findings), got %d: %+v", len(results), results)
	}
	seen := map[string]bool{}
	for _, r := range results {
		seen[r.Check] = true
		if r.Severity == "FAIL" {
			t.Errorf("healthy scratch DB must not FAIL: %s — %s", r.Check, r.Detail)
		}
	}
	for _, want := range []string{
		"case-duplicate repos", "default repo_group singleton",
		"stale collecting locks", "stranded repos",
		"message heal worklist", "new cross-kind collisions",
		"cached counts vs actual", "gathered vs metadata drift",
		"batch vs single stats",
	} {
		if !seen[want] {
			t.Errorf("probe %q missing from the battery", want)
		}
	}
}

// TestBatchSingleStatsAgreementSeeded (AVELOXIS_TEST_DB): Phase 5c
// tripwire (3) — seed one current, one resolved, and one self finding
// and assert batch == single. Pre-v0.27.36 the batch path counted all
// three while single counted one.
func TestBatchSingleStatsAgreementSeeded(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, '_avbss', $2, 1, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avbss/r%d", suffix), fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at, last_collected)
		VALUES ($1, 'queued', 0, NOW(), NOW())`, repoID); err != nil {
		t.Fatal(err)
	}
	seed := func(vulnID, kind string, resolved bool) {
		resolvedAt := "NULL"
		if resolved {
			resolvedAt = "NOW()"
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO aveloxis_data.repo_deps_vulnerabilities
				(repo_id, vuln_id, package_name, severity, dependency_kind, resolved_at)
			VALUES ($1, $2, 'pkg', 'HIGH', $3, %s)`, resolvedAt), repoID, vulnID, kind); err != nil {
			t.Fatal(err)
		}
	}
	seed(fmt.Sprintf("GHSA-cur-%d", suffix), "direct", false)
	seed(fmt.Sprintf("GHSA-res-%d", suffix), "direct", true)
	seed(fmt.Sprintf("GHSA-self-%d", suffix), "self", false)
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	singleTotal, singleCrit, err := store.CountRepoVulnerabilities(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if singleTotal != 1 {
		t.Fatalf("single path must count ONLY the current direct finding, got %d", singleTotal)
	}
	batch, err := store.GetRepoStatsBatch(ctx, []int64{repoID})
	if err != nil {
		t.Fatal(err)
	}
	b := batch[repoID]
	if b == nil || b.Vulnerabilities != singleTotal || b.CriticalVulns != singleCrit {
		t.Fatalf("batch (%+v) must agree with single (%d/%d) — resolved and self rows must be excluded from BOTH paths", b, singleTotal, singleCrit)
	}

	// And the battery's own agreement probe sees a healthy pair.
	res := store.verifyBatchSingleAgreement(ctx, 10)
	if res.Severity == "FAIL" {
		t.Errorf("agreement probe reports FAIL on agreeing data: %s", res.Detail)
	}

	_ = context.Background
}
