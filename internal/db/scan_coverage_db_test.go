// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

func intp(i int) *int    { return &i }
func boolp(b bool) *bool { return &b }

// The scan writes its half of the coverage record in one statement of its
// own, which also moves the repository's cache state (summary/70, 4.7).
func TestRecordScanCoverage(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "scan-half")

	var changedBefore *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&changedBefore); err != nil {
		t.Fatal(err)
	}

	// A failed scan on a repository with no record: the row is created with
	// the scan half alone, and the fields unknown at the point of failure
	// are NULL.
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: model.ScanFailed}); err != nil {
		t.Fatal(err)
	}
	cov, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil || !found {
		t.Fatalf("found %v err %v", found, err)
	}
	if cov.ScanAttemptAt == nil || cov.Scan.Outcome != model.ScanFailed || cov.Scan.TargetsSent != nil || cov.Scan.DependenciesUnknown != nil || cov.AnalysedAt != nil || cov.FirstScanAt != nil {
		t.Errorf("after a failed scan: %+v", cov)
	}
	var changedAfter *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&changedAfter); err != nil {
		t.Fatal(err)
	}
	if changedAfter == nil || (changedBefore != nil && !changedAfter.After(*changedBefore)) {
		t.Errorf("data_changed_at %v -> %v; the scan's coverage write must move the cache state", changedBefore, changedAfter)
	}

	// A completed first scan.
	firstAttempt := *cov.ScanAttemptAt
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{
		Outcome: model.ScanSent, Partial: true, TargetsSent: intp(7), TargetsDropped: intp(1),
		DependenciesUnknown: boolp(false), FirstScan: true,
	}); err != nil {
		t.Fatal(err)
	}
	cov, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if cov.Scan.Outcome != model.ScanSent || !cov.Scan.Partial || *cov.Scan.TargetsSent != 7 || *cov.Scan.TargetsDropped != 1 || *cov.Scan.DependenciesUnknown {
		t.Errorf("scan half = %+v", cov.Scan)
	}
	if cov.FirstScanAt == nil || !cov.ScanAttemptAt.After(firstAttempt) {
		t.Errorf("first_scan_at %v, attempt %v; want the first scan's time set and the attempt moved on", cov.FirstScanAt, cov.ScanAttemptAt)
	}
	first := *cov.FirstScanAt

	// first_scan_at is written once.
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: model.ScanNothingToSend, TargetsSent: intp(0), TargetsDropped: intp(0),
		DependenciesUnknown: boolp(false), FirstScan: true}); err != nil {
		t.Fatal(err)
	}
	cov, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if !cov.FirstScanAt.Equal(first) || cov.Scan.Partial {
		t.Errorf("first_scan_at moved from %v to %v, or partial stayed set: %+v", first, cov.FirstScanAt, cov.Scan)
	}

	// A scan whose target reads did not complete leaves the stored
	// dependencies_unknown as it was.
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: model.ScanFailed}); err != nil {
		t.Fatal(err)
	}
	cov, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if cov.Scan.DependenciesUnknown == nil || *cov.Scan.DependenciesUnknown || cov.Scan.TargetsSent != nil {
		t.Errorf("after a scan that failed before its reads: %+v; want dependencies_unknown kept (false) and the counts NULL", cov.Scan)
	}

	// The scan half never touches the analysis half or the attempt columns.
	half := model.InventoryCoverage{ManifestsFound: 1, ManifestsParsed: 1, DependenciesDeclared: 1}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, half); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: model.ScanSent, TargetsSent: intp(1), TargetsDropped: intp(0), DependenciesUnknown: boolp(false)}); err != nil {
		t.Fatal(err)
	}
	cov, _, _ = store.GetRepoDependencyCoverage(ctx, repoID)
	if cov.AnalysedAt == nil || cov.Inventory.ManifestsFound != 1 || cov.Scan.Outcome != model.ScanSent {
		t.Errorf("the two halves disturbed each other: %+v", cov)
	}

	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: "made up"}); err == nil {
		t.Error("an outcome that is not one of the five must be refused")
	}
}

// One read gives the scan what is stored about a repository's dependencies
// and earlier scans.
func TestGetRepoInventoryEvidence(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "evidence")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`, repoID)
	})

	ev, err := store.GetRepoInventoryEvidence(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if ev != (model.InventoryEvidence{}) || !ev.DependenciesUnknown() {
		t.Errorf("a repository nothing is stored for: %+v; want nothing, and dependencies unknown", ev)
	}

	// An analysis that found nothing still says what the repository depends
	// on: nothing.
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	ev, _ = store.GetRepoInventoryEvidence(ctx, repoID)
	if !ev.AnalysisHalf || ev.InventoryRows != 0 || ev.DependenciesUnknown() {
		t.Errorf("after an analysis that found nothing: %+v; the analysis half makes the (empty) inventory known", ev)
	}

	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0"), libyearRow("b", "1.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID,
		[]*RepoLockfileInfo{{Ecosystem: "npm", LockfilePath: "package-lock.json", LockfileKind: "package-lock.json", EntryCount: 1, DirectCount: 1}},
		[]*model.RepoLockfilePackage{{Ecosystem: "npm", PackageName: "a", Direct: true, ResolvedVersion: "1.0.0", LockfilePath: "package-lock.json"}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{{VulnID: "GHSA-ev-1", PackageName: "a", PackagePurl: "pkg:npm/a@1.0.0", Source: "osv.dev"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetVulnScanLastRun(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	ev, err = store.GetRepoInventoryEvidence(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	want := model.InventoryEvidence{InventoryRows: 2, LockfileRows: 1, AnalysisHalf: true, ScanStamp: true, Findings: true}
	if ev != want {
		t.Errorf("evidence = %+v, want %+v", ev, want)
	}
}

// A repository about which nothing is known may still close findings about
// its OWN package (they do not come from the inventory): the closing
// statement can be limited to those.
func TestMarkStaleVulnerabilitiesResolvedSelfOnly(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "self-only")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
	})
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-dep-1", PackageName: "dep", PackagePurl: "pkg:npm/dep@1.0.0", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-old-1", PackageName: "legacy", PackagePurl: "pkg:npm/legacy@1.0.0", Source: "osv.dev"}, // kind not recorded
		{VulnID: "PYSEC-self-1", PackageName: "own", PackagePurl: "pkg:pypi/own", Source: "osv.dev", DependencyKind: "self"},
		{VulnID: "PYSEC-self-2", PackageName: "own", PackagePurl: "pkg:pypi/own", Source: "osv.dev", DependencyKind: "self"},
	}); err != nil {
		t.Fatal(err)
	}
	n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"PYSEC-self-2|pkg:pypi/own"}, nil, true, nil, model.DecisionGuard{})
	if err != nil || n != 1 {
		t.Fatalf("self-only close: closed %d, err %v; want exactly the one self finding no longer reported", n, err)
	}
	var open int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL`, repoID).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 3 {
		t.Errorf("%d findings still open, want 3: both dependency findings and the reported self one", open)
	}
	// Not limited: the dependency findings close as before.
	n, err = store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"PYSEC-self-2|pkg:pypi/own"}, nil, false, nil, model.DecisionGuard{})
	if err != nil || n != 2 {
		t.Errorf("unlimited close: closed %d, err %v; want the 2 dependency findings", n, err)
	}
}
