// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A closure that is not a fix carries its reason (summary/70, section 3).

func closedReasons(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) map[string]string {
	t.Helper()
	rows, err := store.pool.Query(ctx, `
		SELECT vuln_id || '|' || package_purl,
		       CASE WHEN resolved_at IS NULL THEN 'OPEN' ELSE COALESCE(closed_reason, '') END
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func vulnRepo(t *testing.T, store *PostgresStore, ctx context.Context, name string) int64 {
	t.Helper()
	repoID := seedLibyearRotationRepo(t, store, ctx, name)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
	})
	return repoID
}

func TestClosingStampsTheReason(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "closed-reason")
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-a", PackageName: "requests", PackagePurl: "pkg:pypi/requests", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-b", PackageName: "lodash", PackagePurl: "pkg:npm/lodash@4.17.20", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-c", PackageName: "gone", PackagePurl: "pkg:npm/gone@1.0.0", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-d", PackageName: "kept", PackagePurl: "pkg:npm/kept@1.0.0", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	reasons := map[string]string{
		"GHSA-a|pkg:pypi/requests":      model.ClosedVersionDetermined,
		"GHSA-b|pkg:npm/lodash@4.17.20": model.ClosedVersionChanged,
		// A reason offered for a finding that does NOT close is not applied.
		"GHSA-d|pkg:npm/kept@1.0.0": model.ClosedUnsendable,
	}
	n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"GHSA-d|pkg:npm/kept@1.0.0"}, nil, false, reasons, model.DecisionGuard{})
	if err != nil || n != 3 {
		t.Fatalf("closed %d, err %v; want 3", n, err)
	}
	got := closedReasons(t, store, ctx, repoID)
	want := map[string]string{
		"GHSA-a|pkg:pypi/requests":      model.ClosedVersionDetermined,
		"GHSA-b|pkg:npm/lodash@4.17.20": model.ClosedVersionChanged,
		"GHSA-c|pkg:npm/gone@1.0.0":     "", // closed, no reason: gone or fixed
		"GHSA-d|pkg:npm/kept@1.0.0":     "OPEN",
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %q, want %q", k, got[k], w)
		}
	}

	// A finding that reappears is open again and its reason is cleared: the
	// label belonged to the closure.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-a", PackageName: "requests", PackagePurl: "pkg:pypi/requests", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	var reason *string
	var open bool
	if err := store.pool.QueryRow(ctx, `SELECT closed_reason, resolved_at IS NULL FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND vuln_id = 'GHSA-a'`, repoID).Scan(&reason, &open); err != nil {
		t.Fatal(err)
	}
	if !open || reason != nil {
		t.Errorf("after reappearing: open %v, reason %v; want open with the reason cleared", open, reason)
	}

	// A reason that is not one of the six is refused, and nothing closes.
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil, false, map[string]string{"GHSA-d|pkg:npm/kept@1.0.0": "fixed"}, model.DecisionGuard{}); err == nil {
		t.Error("an unknown reason must be refused")
	}
	if closedReasons(t, store, ctx, repoID)["GHSA-d|pkg:npm/kept@1.0.0"] != "OPEN" {
		t.Error("a refused close closed a finding")
	}
}

// The open findings, as the closing rule reads them before a scan writes.
func TestGetOpenVulnerabilityFindings(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "open-findings")
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-a", PackageName: "a", PackagePurl: "pkg:npm/a@1.0.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
		{VulnID: "GHSA-b", PackageName: "b", PackagePurl: "pkg:pypi/b", Ecosystem: "PyPI", Source: "osv.dev", DependencyKind: "self"},
		{VulnID: "GHSA-c", PackageName: "c", PackagePurl: "pkg:npm/c@1.0.0", Ecosystem: "npm", Source: "osv.dev", DependencyKind: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"GHSA-a|pkg:npm/a@1.0.0", "GHSA-b|pkg:pypi/b"}, nil, false, nil, model.DecisionGuard{}); err != nil {
		t.Fatal(err)
	}
	open, err := store.GetOpenVulnerabilityFindings(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 2 {
		t.Fatalf("open findings = %+v; want the two that are not closed", open)
	}
	byID := map[string]model.OpenFinding{}
	for _, f := range open {
		byID[f.VulnID] = f
	}
	if f := byID["GHSA-b"]; f.Purl != "pkg:pypi/b" || f.Kind != "self" || f.Ecosystem != "PyPI" || f.FirstDetectedAt.IsZero() {
		t.Errorf("GHSA-b = %+v", f)
	}
}

// A successor keeps its predecessor's first detection: the store keeps the
// EARLIER of the date it is handed and the one it holds, and a row handed no
// date is first detected now.
func TestFirstDetectionIsTheEarliest(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := vulnRepo(t, store, ctx, "first-detected")
	carried := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	before := time.Now().Add(-time.Second)
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-new", PackageName: "a", PackagePurl: "pkg:npm/a@2.0.0", Source: "osv.dev", FirstDetectedAt: carried},
		{VulnID: "GHSA-plain", PackageName: "b", PackagePurl: "pkg:npm/b@1.0.0", Source: "osv.dev"},
	}); err != nil {
		t.Fatal(err)
	}
	read := func(id string) time.Time {
		var at time.Time
		if err := store.pool.QueryRow(ctx, `SELECT first_detected_at FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND vuln_id = $2`, repoID, id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	if got := read("GHSA-new"); !got.Equal(carried) {
		t.Errorf("a new row handed a date: first detected %v, want %v", got, carried)
	}
	plain := read("GHSA-plain")
	if plain.Before(before) {
		t.Errorf("a new row handed no date: first detected %v, want now", plain)
	}
	// Seen again with no date: unchanged. Seen again with a LATER date:
	// unchanged. Seen again with an EARLIER date: moved back.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-new", PackageName: "a", PackagePurl: "pkg:npm/a@2.0.0", Source: "osv.dev"},
		{VulnID: "GHSA-plain", PackageName: "b", PackagePurl: "pkg:npm/b@1.0.0", Source: "osv.dev", FirstDetectedAt: time.Now().Add(time.Hour)},
	}); err != nil {
		t.Fatal(err)
	}
	if got := read("GHSA-new"); !got.Equal(carried) {
		t.Errorf("seen again with no date: first detected moved to %v", got)
	}
	if got := read("GHSA-plain"); !got.Equal(plain) {
		t.Errorf("seen again with a later date: first detected moved to %v", got)
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*model.VulnerabilityRow{
		{VulnID: "GHSA-plain", PackageName: "b", PackagePurl: "pkg:npm/b@1.0.0", Source: "osv.dev", FirstDetectedAt: carried},
	}); err != nil {
		t.Fatal(err)
	}
	if got := read("GHSA-plain"); !got.Equal(carried) {
		t.Errorf("seen again with an earlier date: first detected %v, want %v", got, carried)
	}
}
