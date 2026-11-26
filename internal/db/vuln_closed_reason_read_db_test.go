// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The finding list carries each closed finding's reason (0.29.196): the API
// labels and counts closures from it, so a read that dropped it would report
// every reasoned closure as a fix.
func TestGetRepoVulnerabilitiesCarriesTheClosedReason(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "closed-reason-read")
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "open", PackageName: "p", PackagePurl: "pkg:npm/p@1", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "fixed", PackageName: "p", PackagePurl: "pkg:npm/p@1", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "lost", PackageName: "p", PackagePurl: "pkg:npm/p@1", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_vulnerabilities SET resolved_at = NOW() WHERE repo_id = $1 AND vuln_id = 'fixed'`, repoID)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_vulnerabilities SET resolved_at = NOW(), closed_reason = $2 WHERE repo_id = $1 AND vuln_id = 'lost'`, repoID, model.ClosedVersionLost)
	rows, err := store.GetRepoVulnerabilities(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.VulnID] = r.ClosedReason
	}
	if len(got) != 3 || got["open"] != "" || got["fixed"] != "" || got["lost"] != model.ClosedVersionLost {
		t.Errorf("closed reasons read = %v; want only the reasoned closure to carry one", got)
	}
}

// A failed analysis attempt moves the repository's cache state (0.29.197):
// the attempt is shown on the coverage object's second line, which cached
// answers carry, so a page cached before the attempt must be rebuilt.
func TestRecordInventoryAttemptMovesTheCacheState(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "attempt-stamps")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
	})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET data_changed_at = NOW() - INTERVAL '1 hour' WHERE repo_id = $1`, repoID)
	if err := store.RecordInventoryAttempt(ctx, repoID, model.InventoryAttemptCloneUnavailable); err != nil {
		t.Fatal(err)
	}
	var fresh bool
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at > NOW() - INTERVAL '1 minute' FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if !fresh {
		t.Error("a recorded analysis attempt did not move the repository's cache state")
	}
	in, err := store.GetRepoCoverageInputs(ctx, []int64{repoID, -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 1 || !in[repoID].HasRow || in[repoID].Coverage.LastAttemptResult != model.InventoryAttemptCloneUnavailable || in[repoID].HasFindings || in[repoID].ScanStamp != nil {
		t.Errorf("coverage inputs = %+v; want the one repository, with its record and attempt, and no entry for an id that does not exist", in)
	}
}

// The SBOM generator's first read carries the coverage it will mark the
// document with (0.29.199): decided by the one function, from the stored
// record, the stamp and the exposure count.
func TestGetRepoForSBOMCarriesCoverage(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "sbom-coverage")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
	})
	r, err := store.GetRepoForSBOM(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Coverage == nil || r.Coverage.State != "not_scanned" {
		t.Fatalf("a repository with nothing recorded: coverage %+v", r.Coverage)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET vuln_scan_last_run = NOW() WHERE repo_id = $1`, repoID)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, last_scan_attempt_at, scan_outcome, dependencies_unknown,
		manifests_failed) VALUES ($1, NOW(), 'sent', TRUE, '[{"path":"a/pom.xml","kind":"pom.xml","reason":"read","rows_carried":1}]')`, repoID)
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "E-1", PackageName: "p", PackagePurl: "pkg:npm/p@1", Source: "osv.dev", DependencyKind: "direct", VersionResolution: "locked"}}); err != nil {
		t.Fatal(err)
	}
	r, err = store.GetRepoForSBOM(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	c := r.Coverage
	if c == nil || c.State != "scanned" || !c.DependenciesUnknown || c.ManifestsFailed != 1 || c.ManifestsCarried != 1 || c.CleanResult {
		t.Errorf("coverage = %+v; want scanned, dependencies unknown, one failed and carried manifest, and not a clean result", c)
	}
	// Analysed, so the dependencies are known: the one thing between this
	// repository and a clean result is its exposure finding, which the
	// read must count.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_dependency_coverage SET analysed_at = NOW() WHERE repo_id = $1`, repoID)
	if r, err = store.GetRepoForSBOM(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if r.Coverage.DependenciesUnknown || r.Coverage.CleanResult {
		t.Errorf("analysed, with an exposure finding: %+v; want dependencies known and NOT a clean result", r.Coverage)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_deps_vulnerabilities SET resolved_at = NOW() WHERE repo_id = $1`, repoID)
	if r, err = store.GetRepoForSBOM(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if !r.Coverage.CleanResult {
		t.Errorf("analysed, the finding closed: %+v; want a clean result", r.Coverage)
	}
}
