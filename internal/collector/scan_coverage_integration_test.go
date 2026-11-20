// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The vulnerability scan records its half of the coverage record on every
// exit, and applies the closing rule's precondition (summary/70, 4.7 and
// section 3): no dependency finding closes for a repository about which
// nothing is known.

// stubOSV answers every purl with no advisories and counts the queries; fail
// makes the batch endpoint answer 500.
func stubOSV(t *testing.T, fail *bool) *int {
	t.Helper()
	queries := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail != nil && *fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		var req struct {
			Queries []json.RawMessage `json:"queries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		queries += len(req.Queries)
		results := make([]map[string]any, len(req.Queries))
		for i := range results {
			results[i] = map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(srv.Close)
	old := osvBatchURL
	osvBatchURL = srv.URL
	t.Cleanup(func() { osvBatchURL = old })
	return &queries
}

func (f *wholeReplaceFixture) openFindings(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.store.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL`, f.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *wholeReplaceFixture) scanStamped(t *testing.T) bool {
	t.Helper()
	var stamped bool
	if err := f.store.Pool().QueryRow(context.Background(),
		`SELECT vuln_scan_last_run IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $1`, f.repoID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	return stamped
}

func scanFixture(t *testing.T, name string) (*wholeReplaceFixture, *slog.Logger) {
	f := newWholeReplaceFixture(t, name, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), "DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1", f.repoID)
	})
	return f, slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestNothingKnownMeansNothingCloses is the red test for the precondition. A
// repository with findings from an earlier release, and no stored inventory,
// no lockfile rows and no completed analysis (its clone was unavailable, say)
// is scanned by heal-vulnerabilities. Before 0.29.161 the scan closed every
// finding and marked the repository "scanned, clean".
func TestNothingKnownMeansNothingCloses(t *testing.T) {
	f, logger := scanFixture(t, "unknown")
	stubOSV(t, nil)
	ctx := context.Background()
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-unk-1", PackageName: "left-pad", PackagePurl: "pkg:npm/left-pad@1.0.0", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-unk-2", PackageName: "express", PackagePurl: "pkg:npm/express@4.0.0", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.openFindings(t); got != 2 || res.VulnsResolved != 0 {
		t.Errorf("%d findings open, %d reported closed; nothing has ever said what this repository depends on, so both must stay open", got, res.VulnsResolved)
	}
	if f.scanStamped(t) {
		t.Error("the repository was marked scanned; \"scanned, clean\" would be a claim with nothing behind it")
	}
	cov, found := f.coverage(t)
	if !found || cov.Scan.Outcome != model.ScanInventoryIncomplete || cov.Scan.DependenciesUnknown == nil || !*cov.Scan.DependenciesUnknown || cov.ScanAttemptAt == nil {
		t.Errorf("coverage = %+v (found %v); want outcome inventory_incomplete with dependencies_unknown true", cov, found)
	}
	if cov.FirstScanAt != nil {
		t.Error("a scan that sent nothing and knew nothing is not the repository's first scan")
	}
}

// Once an analysis HAS run and found nothing, the same scan closes the stale
// findings and marks the repository, as it always did.
func TestAKnownEmptyInventoryClosesStaleFindings(t *testing.T) {
	f, logger := scanFixture(t, "knownempty")
	stubOSV(t, nil)
	ctx := context.Background()
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-gone-1", PackageName: "left-pad", PackagePurl: "pkg:npm/left-pad@1.0.0", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	// An analysis of a clone with no manifest.
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	if got := f.openFindings(t); got != 0 || !f.scanStamped(t) {
		t.Errorf("%d findings open, stamped %v; an analysed repository that declares nothing has no live findings and is marked scanned", got, f.scanStamped(t))
	}
	cov, _ := f.coverage(t)
	if cov.Scan.Outcome != model.ScanNothingToSend || *cov.Scan.DependenciesUnknown || *cov.Scan.TargetsSent != 0 {
		t.Errorf("scan half = %+v; want nothing_to_send, dependencies known, 0 targets", cov.Scan)
	}
	// It had findings before this scan began, so this is not its first scan.
	if cov.FirstScanAt != nil {
		t.Error("first_scan_at was set for a repository that already had findings")
	}
}

// A repository first analysed and first scanned by this release: sent, with
// its first-scan time; a second scan does not move that time.
func TestAFirstScanIsRecordedOnce(t *testing.T) {
	f, logger := scanFixture(t, "firstscan")
	queries := stubOSV(t, nil)
	ctx := context.Background()
	f.manifest(t, map[string]string{uniq(t, "scan-a"): "1.0.0", uniq(t, "scan-b"): "1.0.0"})
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	cov, _ := f.coverage(t)
	if cov.Scan.Outcome != model.ScanSent || cov.Scan.Partial || *cov.Scan.TargetsSent != 2 || *cov.Scan.TargetsDropped != 0 || *queries != 2 {
		t.Errorf("scan half = %+v after %d queries; want sent, 2 targets", cov.Scan, *queries)
	}
	if cov.FirstScanAt == nil || !f.scanStamped(t) {
		t.Fatalf("first_scan_at %v, stamped %v", cov.FirstScanAt, f.scanStamped(t))
	}
	first := *cov.FirstScanAt
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	cov, _ = f.coverage(t)
	if !cov.FirstScanAt.Equal(first) {
		t.Errorf("first_scan_at moved from %v to %v", first, cov.FirstScanAt)
	}
}

// A failed scan records "failed" with what it knew, closes nothing and does
// not mark; and a shutdown records nothing at all.
func TestAFailedScanIsRecordedAndAShutdownIsNot(t *testing.T) {
	f, logger := scanFixture(t, "failedscan")
	fail := true
	stubOSV(t, &fail)
	ctx := context.Background()
	f.manifest(t, map[string]string{uniq(t, "fail-a"): "1.0.0"})
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err == nil {
		t.Fatal("the scan must fail when OSV answers 500")
	}
	cov, _ := f.coverage(t)
	if cov.Scan.Outcome != model.ScanFailed || cov.Scan.TargetsSent == nil || *cov.Scan.TargetsSent != 1 || cov.FirstScanAt != nil || f.scanStamped(t) {
		t.Errorf("after a failed scan: %+v, stamped %v; want failed, the 1 target it had built, no first-scan time, no stamp", cov.Scan, f.scanStamped(t))
	}
	attempt := *cov.ScanAttemptAt

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = ScanVulnerabilities(cancelled, f.store, f.repoID, logger, nil, true)
	cov, _ = f.coverage(t)
	if !cov.ScanAttemptAt.Equal(attempt) {
		t.Error("a scan under a cancelled context wrote its outcome; a shutdown writes nothing")
	}
}

// A closure that is not a fix is labelled, end to end (summary/70, section
// 3). A dependency declared with no version has a finding at its versionless
// purl; then its version becomes known (here, a pinned requirement). The old
// finding closes because its purl is no longer a target, and it must not read
// as a fix: the advisory does not affect the version now known.
func TestAReKeyedFindingClosesWithAReason(t *testing.T) {
	f, logger := scanFixture(t, "rekey")
	stubOSV(t, nil) // no advisory affects anything
	ctx := context.Background()
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0", Requirement: "requests==2.31.0", Purl: "pkg:pypi/requests@2.31.0", Type: "runtime"},
		{Name: "express", PackageManager: "npm", CurrentVersion: "4.19.0", Requirement: "4.19.0", Purl: "pkg:npm/express@4.19.0", Type: "runtime"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		// Found while `requests` had no declared version.
		{VulnID: "GHSA-old", PackageName: "requests", PackagePurl: "pkg:pypi/requests", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct", VersionResolution: "unpinned"},
		// Found at an older version of express: a real fix.
		{VulnID: "GHSA-fixed", PackageName: "express", PackagePurl: "pkg:npm/express@4.17.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
		// A dependency that is gone.
		{VulnID: "GHSA-gone", PackageName: "left-pad", PackagePurl: "pkg:npm/left-pad@1.0.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	rows, err := f.store.Pool().Query(ctx, `
		SELECT vuln_id, resolved_at IS NOT NULL, COALESCE(closed_reason, '')
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for rows.Next() {
		var id, reason string
		var closed bool
		if err := rows.Scan(&id, &closed, &reason); err != nil {
			t.Fatal(err)
		}
		if !closed {
			reason = "OPEN"
		}
		got[id] = reason
	}
	rows.Close()
	want := map[string]string{
		"GHSA-old":   model.ClosedVersionDetermined, // not a fix: the version was worked out
		"GHSA-fixed": "",                            // a fix: no label
		"GHSA-gone":  "",                            // gone: no label
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s: closed with reason %q, want %q (all: %v)", id, got[id], w, got)
		}
	}
}

// A query OSV refuses no longer fails the repository's scan, and is not
// evidence of a fix (summary/70, 4.2), end to end.
func TestARejectedQueryHoldsItsPackagesFindings(t *testing.T) {
	f, logger := scanFixture(t, "rejected-query")
	ctx := context.Background()
	osv := &osvRejecting{reject: map[string]bool{"pkg:pypi/requests@2.31.0": true}}
	osv.serve(t)
	withRejectedQueryCap(t, 5)
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0", Requirement: "requests==2.31.0", Purl: "pkg:pypi/requests@2.31.0", Type: "runtime"},
		{Name: "express", PackageManager: "npm", CurrentVersion: "4.19.0", Requirement: "4.19.0", Purl: "pkg:npm/express@4.19.0", Type: "runtime"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-at-rejected", PackageName: "requests", PackagePurl: "pkg:pypi/requests@2.31.0", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-old-purl", PackageName: "requests", PackagePurl: "pkg:pypi/requests", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct", VersionResolution: "unpinned"},
		{VulnID: "GHSA-fixed", PackageName: "express", PackagePurl: "pkg:npm/express@4.17.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true)
	if err != nil {
		t.Fatalf("a rejected query failed the scan: %v", err)
	}
	if res.OSVQueriesRejected != 1 || res.VulnsFound != 1 {
		t.Errorf("rejected %d, found %d; want 1 and 1 (express's advisory)", res.OSVQueriesRejected, res.VulnsFound)
	}
	open := map[string]bool{}
	rows, err := f.store.Pool().Query(ctx, `SELECT vuln_id FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		open[id] = true
	}
	rows.Close()
	if !open["GHSA-at-rejected"] || !open["GHSA-old-purl"] {
		t.Errorf("open findings %v; both findings on the package OSV would not answer for must stay open", open)
	}
	if open["GHSA-fixed"] {
		t.Error("the finding on an answered package, at a version no longer in use, must close as before")
	}
	cov, found, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil || !found {
		t.Fatalf("coverage: found %v err %v", found, err)
	}
	if cov.Scan.Outcome != model.ScanSent || !cov.Scan.Partial {
		t.Errorf("outcome %q partial %v; want sent and partial", cov.Scan.Outcome, cov.Scan.Partial)
	}
	if cov.Scan.TargetsRejected == nil || *cov.Scan.TargetsRejected != 1 || cov.Scan.ClosuresHeld == nil || *cov.Scan.ClosuresHeld != 2 {
		t.Errorf("rejected %v held %v; want 1 and 2", cov.Scan.TargetsRejected, cov.Scan.ClosuresHeld)
	}
	if !f.scanStamped(t) {
		t.Error("OSV answered for another dependency: the repository is scanned")
	}
}

// Every query refused: OSV said nothing about the repository. Its findings
// stay and it is not marked scanned.
func TestEveryQueryRejectedDoesNotStamp(t *testing.T) {
	f, logger := scanFixture(t, "all-rejected")
	ctx := context.Background()
	osv := &osvRejecting{reject: map[string]bool{"pkg:pypi/requests@2.31.0": true}}
	osv.serve(t)
	withRejectedQueryCap(t, 5)
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0", Requirement: "requests==2.31.0", Purl: "pkg:pypi/requests@2.31.0", Type: "runtime"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-1", PackageName: "requests", PackagePurl: "pkg:pypi/requests@2.31.0", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	if f.openFindings(t) != 1 {
		t.Error("the finding closed although OSV answered nothing")
	}
	if f.scanStamped(t) {
		t.Error("the repository was marked scanned although OSV answered nothing")
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	// Its dependency WAS sent. "No dependency could be sent" would be false
	// (0.29.208): the outcome is that nothing was answered.
	if cov.Scan.Outcome != model.ScanUnanswered || cov.Scan.TargetsRejected == nil || *cov.Scan.TargetsRejected != 1 {
		t.Errorf("outcome %q rejected %v; want unanswered and 1", cov.Scan.Outcome, cov.Scan.TargetsRejected)
	}
}

// An answer cut short is not a complete answer (summary/70, 4.2), end to
// end: the advisories on the pages that were read are written, the finding
// whose advisory was on a page that was not read stays open, and the scan is
// partial.
func TestATruncatedAnswerClosesNothingForItsPackage(t *testing.T) {
	f, logger := scanFixture(t, "truncated-answer")
	ctx := context.Background()
	osv := &osvPaged{pages: map[string][][]string{
		"pkg:pypi/requests@2.31.0": {{"GHSA-page1"}, {"GHSA-page2"}, {"GHSA-page3"}},
	}}
	osv.serve(t)
	withPagesPerQuery(t, 2)
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0", Requirement: "requests==2.31.0", Purl: "pkg:pypi/requests@2.31.0", Type: "runtime"},
		{Name: "express", PackageManager: "npm", CurrentVersion: "4.19.0", Requirement: "4.19.0", Purl: "pkg:npm/express@4.19.0", Type: "runtime"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-page3", PackageName: "requests", PackagePurl: "pkg:pypi/requests@2.31.0", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-express-fixed", PackageName: "express", PackagePurl: "pkg:npm/express@4.19.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.OSVQueriesTruncated != 1 || res.VulnsFound != 2 {
		t.Errorf("truncated %d, found %d; want 1 and 2 (the two pages that were read)", res.OSVQueriesTruncated, res.VulnsFound)
	}
	open := map[string]bool{}
	rows, err := f.store.Pool().Query(ctx, `SELECT vuln_id FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		open[id] = true
	}
	rows.Close()
	if !open["GHSA-page1"] || !open["GHSA-page2"] || !open["GHSA-page3"] {
		t.Errorf("open findings %v; the advisory on the unread page must stay open beside the two that were read", open)
	}
	if open["GHSA-express-fixed"] {
		t.Error("express's answer was complete and omits the advisory: that finding closes")
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if cov.Scan.Outcome != model.ScanSent || !cov.Scan.Partial || cov.Scan.TargetsTruncated == nil || *cov.Scan.TargetsTruncated != 1 ||
		cov.Scan.ClosuresHeld == nil || *cov.Scan.ClosuresHeld != 1 {
		t.Errorf("coverage %+v; want sent, partial, 1 truncated, 1 held", cov.Scan)
	}
}

// Present but not evaluable is not absent (summary/70, 4.2), end to end. OSV
// names an advisory for a GitHub Action and the advisory's detail cannot be
// fetched, so the pinned ref cannot be matched against it. Before 0.29.170 no
// row was written and the existing finding closed by absence, as if fixed.
func TestAnActionsAdvisoryWithNoDetailKeepsItsFinding(t *testing.T) {
	f, logger := scanFixture(t, "not-evaluable")
	ctx := context.Background()
	osv := &osvPaged{pages: map[string][][]string{"o/act|GitHub Actions": {{"GHSA-nodetail"}}}}
	osv.serve(t)
	withPagesPerQuery(t, 10)
	detail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(detail.Close)
	oldVulns := osvVulnURLBase
	osvVulnURLBase = detail.URL + "/v1/vulns/"
	t.Cleanup(func() { osvVulnURLBase = oldVulns })

	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "o/act", PackageManager: "githubactions", CurrentVersion: "v4", Requirement: "v4", Purl: "pkg:githubactions/o/act@v4", Type: "build"},
		{Name: "o/pinned", PackageManager: "githubactions", CurrentVersion: "0123456789abcdef0123456789abcdef01234567", Requirement: "0123456789abcdef0123456789abcdef01234567",
			Purl: "pkg:githubactions/o/pinned@0123456789abcdef0123456789abcdef01234567", Type: "build"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	osv.pages["o/pinned|GitHub Actions"] = [][]string{{"GHSA-sha"}}
	if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-nodetail", PackageName: "o/act", PackagePurl: "pkg:githubactions/o/act@v4", Ecosystem: "githubactions", Source: "osv.dev", DependencyKind: "direct", VersionResolution: "unpinned"},
		// A finding at a commit SHA: a SHA is never matched, with or
		// without the detail, so this one closes as before.
		{VulnID: "GHSA-sha", PackageName: "o/pinned", PackagePurl: "pkg:githubactions/o/pinned@0123456789abcdef0123456789abcdef01234567", Ecosystem: "githubactions", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	var open []string
	rows, err := f.store.Pool().Query(ctx, `SELECT vuln_id FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND resolved_at IS NULL ORDER BY 1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		open = append(open, id)
	}
	rows.Close()
	if len(open) != 1 || open[0] != "GHSA-nodetail" {
		t.Errorf("open findings %v; want only GHSA-nodetail (held), the SHA-pinned one closed", open)
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !cov.Scan.Partial || cov.Scan.ClosuresHeld == nil || *cov.Scan.ClosuresHeld != 1 {
		t.Errorf("coverage %+v; want partial with 1 closure held", cov.Scan)
	}
}

// The same with NO finding open for the advisory (review round 1): nothing
// is held, and the scan is still not a complete answer about that Action.
// OSV named an advisory for it and whether it applies could not be decided.
// It used to read "scanned" with nothing found.
func TestAnActionsAdvisoryWithNoDetailAndNoFindingMakesTheScanPartial(t *testing.T) {
	f, logger := scanFixture(t, "not-evaluable-first")
	ctx := context.Background()
	osv := &osvPaged{pages: map[string][][]string{"o/act|GitHub Actions": {{"GHSA-nodetail"}}}}
	osv.serve(t)
	withPagesPerQuery(t, 10)
	detail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(detail.Close)
	oldVulns := osvVulnURLBase
	osvVulnURLBase = detail.URL + "/v1/vulns/"
	t.Cleanup(func() { osvVulnURLBase = oldVulns })
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "o/act", PackageManager: "githubactions", CurrentVersion: "v4", Requirement: "v4", Purl: "pkg:githubactions/o/act@v4", Type: "build"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if cov.Scan.Outcome != model.ScanSent || !cov.Scan.Partial || cov.Scan.ClosuresHeld == nil || *cov.Scan.ClosuresHeld != 0 {
		t.Errorf("coverage %+v; want sent, partial, and no closure held", cov.Scan)
	}
}

// An older answer does not act (summary/70, 4.2 and 5.4), end to end, in
// both directions. This process holds an answer in its day-long cache that
// was asked BEFORE another process's decision on the same finding. The
// cached answer must change nothing, and the package is asked about again,
// live; what OSV says now decides.
func TestAnOvertakenCachedAnswerIsAskedAgain(t *testing.T) {
	const purl = "pkg:pypi/requests@2.31.0"
	seedDep := func(t *testing.T, f *wholeReplaceFixture) {
		t.Helper()
		if err := f.store.ReplaceRepoLibyear(context.Background(), f.repoID, []*model.LibyearRow{
			{Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0", Requirement: "requests==2.31.0", Purl: purl, Type: "runtime"},
		}, nil, model.InventoryCoverage{}); err != nil {
			t.Fatal(err)
		}
	}
	state := func(t *testing.T, f *wholeReplaceFixture) (open bool, decided time.Time) {
		t.Helper()
		var at *time.Time
		if err := f.store.Pool().QueryRow(context.Background(), `SELECT resolved_at IS NULL, decided_asked_at
			FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND vuln_id = 'GHSA-1'`, f.repoID).Scan(&open, &at); err != nil {
			t.Fatal(err)
		}
		if at != nil {
			decided = *at
		}
		return open, decided
	}

	t.Run("a stale 'absent' does not close what a newer answer opened", func(t *testing.T) {
		f, logger := scanFixture(t, "overtaken-close")
		ctx := context.Background()
		seedDep(t, f)
		now, err := f.store.DatabaseNow(ctx)
		if err != nil {
			t.Fatal(err)
		}
		longAgo, recently := now.Add(-20*time.Hour), now.Add(-time.Hour)
		// Another process opened the finding from an answer asked an hour ago.
		if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
			{VulnID: "GHSA-1", PackageName: "requests", PackagePurl: purl, Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct", AskedAt: recently, TargetsReadAt: recently},
		}); err != nil {
			t.Fatal(err)
		}
		// This process's cache says "no advisories", asked twenty hours ago.
		cache := NewOSVCache()
		cache.putAnswer(purl, []string{}, longAgo)
		// OSV, asked now, still reports the advisory.
		osv := &osvPaged{pages: map[string][][]string{purl: {{"GHSA-1"}}}}
		osv.serve(t)
		withPagesPerQuery(t, 10)

		if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, cache, true); err != nil {
			t.Fatal(err)
		}
		open, decided := state(t, f)
		if !open {
			t.Fatal("a cached answer older than the finding's last decision closed it")
		}
		if len(osv.requests) != 1 {
			t.Errorf("OSV was asked %d times; want once, live, for the overtaken answer", len(osv.requests))
		}
		if !decided.After(recently) {
			t.Errorf("decided_asked_at = %v; want the live re-ask's time, after %v", decided, recently)
		}
	})

	t.Run("a stale 'present' does not reopen what a newer answer closed", func(t *testing.T) {
		f, logger := scanFixture(t, "overtaken-reopen")
		ctx := context.Background()
		seedDep(t, f)
		now, err := f.store.DatabaseNow(ctx)
		if err != nil {
			t.Fatal(err)
		}
		longAgo, recently := now.Add(-20*time.Hour), now.Add(-time.Hour)
		if err := f.store.InsertVulnerabilityBatch(ctx, f.repoID, []*model.VulnerabilityRow{
			{VulnID: "GHSA-1", PackageName: "requests", PackagePurl: purl, Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "direct", AskedAt: longAgo, TargetsReadAt: longAgo},
		}); err != nil {
			t.Fatal(err)
		}
		// Another process closed it on an answer asked an hour ago.
		if n, err := f.store.MarkStaleVulnerabilitiesResolved(ctx, f.repoID, nil, nil, false, nil,
			model.DecisionGuard{TargetsReadAt: recently, AnswerAskedAt: map[string]time.Time{purl: recently}}); err != nil || n != 1 {
			t.Fatalf("closed %d, err %v", n, err)
		}
		// This process's cache still says the advisory applies, asked twenty
		// hours ago. OSV, asked now, no longer reports it.
		cache := NewOSVCache()
		cache.putAnswer(purl, []string{"GHSA-1"}, longAgo)
		osv := &osvPaged{pages: map[string][][]string{purl: {}}}
		osv.serve(t)
		withPagesPerQuery(t, 10)
		detail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) }))
		t.Cleanup(detail.Close)
		oldVulns := osvVulnURLBase
		osvVulnURLBase = detail.URL + "/v1/vulns/"
		t.Cleanup(func() { osvVulnURLBase = oldVulns })

		if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, cache, true); err != nil {
			t.Fatal(err)
		}
		if open, _ := state(t, f); open {
			t.Error("a cached answer older than the finding's last decision reopened it")
		}
		if len(osv.requests) != 1 {
			t.Errorf("OSV was asked %d times; want once, live", len(osv.requests))
		}
		// The pass after the re-ask is the one that marks the repository.
		if !f.scanStamped(t) {
			t.Error("the repository is not marked scanned after a scan that asked again and finished")
		}
		// The fresh answer replaced the stale cache entry.
		if ids, ok := cache.GetQuery(purl); !ok || len(ids) != 0 {
			t.Errorf("the cache holds %v (ok %v) after the re-ask; want the fresh, empty answer", ids, ok)
		}
	})
}

// Zero targets, one rule (summary/70, 4.2), end to end. A repository whose
// only dependency has a name the identity check refused has no inventory row
// and no target. Before 0.29.174 that read as "declares nothing": it was
// marked scanned, clean.
func TestARepositoryOfRejectedNamesIsNotScannedClean(t *testing.T) {
	f, logger := scanFixture(t, "rejected-names-only")
	ctx := context.Background()
	stubOSV(t, nil)
	notes := []model.InventoryNote{{Kind: model.NoRowNameRejected, PackageManager: "pypi", Name: "not a name", ManifestPath: "requirements.txt"}}
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, nil, notes, model.InventoryCoverage{ManifestsFound: 1, ManifestsParsed: 1, DependenciesDeclared: 1,
		DependenciesWithoutRow: map[string]int{model.NoRowNameRejected: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	if f.scanStamped(t) {
		t.Error("a repository none of whose dependencies could be sent was marked scanned")
	}
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if cov.Scan.Outcome != model.ScanUnsendable {
		t.Errorf("outcome %q, want unsendable", cov.Scan.Outcome)
	}

	// With a note of another kind only, it declares nothing sendable by
	// design and IS scanned, as before.
	g, logger := scanFixture(t, "non-registry-only")
	notes = []model.InventoryNote{{Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "local-lib", ManifestPath: "package.json"}}
	if err := g.store.ReplaceRepoLibyear(ctx, g.repoID, nil, notes, model.InventoryCoverage{ManifestsFound: 1, ManifestsParsed: 1, DependenciesDeclared: 1,
		DependenciesWithoutRow: map[string]int{model.NoRowNonRegistry: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanVulnerabilities(ctx, g.store, g.repoID, logger, nil, true); err != nil {
		t.Fatal(err)
	}
	if !g.scanStamped(t) {
		t.Error("a repository whose only dependency is one no registry holds declares nothing to send, and is scanned")
	}
}
