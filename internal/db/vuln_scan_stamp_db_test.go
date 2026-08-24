// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// v0.28.1 (A4) — schema + store pins for the completed-vuln-scan
// stamp (repos.vuln_scan_last_run, the scancode_last_run pattern).

func TestSchemaDeclaresVulnScanLastRun(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	if !strings.Contains(schema, "vuln_scan_last_run      TIMESTAMPTZ") {
		t.Error("schema.sql must declare repos.vuln_scan_last_run TIMESTAMPTZ")
	}
}

func TestMigrateAddsVulnScanLastRun(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	if !strings.Contains(src, `"aveloxis_data.repos", "vuln_scan_last_run", "TIMESTAMPTZ"`) {
		t.Error("migrate.go must addColumnIfMissing repos.vuln_scan_last_run for existing fleets")
	}
}

func TestVulnScanStampStoreMethods(t *testing.T) {
	src := readSourceFile(t, "vulnerability_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) SetVulnScanLastRun(") {
		t.Error("SetVulnScanLastRun missing")
	}
	if !strings.Contains(src, "SET vuln_scan_last_run = NOW()") {
		t.Error("SetVulnScanLastRun must stamp NOW() (AlwaysRefresh policy)")
	}
	if !strings.Contains(src, "func (s *PostgresStore) GetVulnScanLastRun(") {
		t.Error("GetVulnScanLastRun missing")
	}
}

// The API envelope carries the stamp so the GUI can render
// "Last scanned <date>" — null must stay distinguishable from a
// dated clean scan.
func TestVulnEnvelopeCarriesScannedAt(t *testing.T) {
	data, err := os.ReadFile("../api/vulnerabilities.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"scanned_at"`) {
		t.Error("/repos/{id}/vulnerabilities envelope must carry scanned_at")
	}
	if !strings.Contains(src, "GetVulnScanLastRun(") {
		t.Error("the handler must read the stamp via GetVulnScanLastRun")
	}
}

// v0.28.7 (Copilot round 3) — the migration backfill pin: upgraded
// fleets have NULL stamps everywhere, but stored findings PROVE a
// scan ran (their last_seen_at is when OSV was consulted). Without
// the backfill, a repo serves active findings alongside
// scanned_at:null ("never scanned") until its next scan.
func TestVulnScanStampBackfillRegistered(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	for _, needle := range []string{
		"v0.28.7 backfill vuln_scan_last_run from finding evidence (a scan provably ran)",
		"MAX(last_seen_at) AS last_seen",
		"AND r.vuln_scan_last_run IS NULL", // fill-empty-only: never clobber a real stamp
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go missing vuln-stamp backfill needle %q", needle)
		}
	}
}

// Behavioral (AVELOXIS_TEST_DB): findings-bearing repo gets the
// evidence-derived stamp via the operator-replay flow; a clean repo
// (no findings — no evidence) honestly stays NULL.
func TestVulnScanStampBackfillEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	withFindings := seedRepoForDeps(t, store, ctx, "_avvstamp", "evidence")
	clean := seedRepoForDeps(t, store, ctx, "_avvstamp", "clean")
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repo_deps_vulnerabilities
			(repo_id, vuln_id, package_name, severity, dependency_kind, last_seen_at)
		VALUES ($1, 'GHSA-avvstamp-1', 'pkg', 'HIGH', 'direct', '2026-06-01T12:00:00Z')`, withFindings)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, withFindings)
	})

	// Replay the ledgered step exactly the way an operator would.
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label LIKE 'v0.28.7 backfill vuln_scan_last_run%'`)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("backfill migrate: %v", err)
	}

	got, err := store.GetVulnScanLastRun(ctx, withFindings)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.UTC().Format("2006-01-02") != "2026-06-01" {
		t.Errorf("findings-bearing repo must get the evidence-derived stamp, got %v", got)
	}
	cleanStamp, err := store.GetVulnScanLastRun(ctx, clean)
	if err != nil {
		t.Fatal(err)
	}
	if cleanStamp != nil {
		t.Errorf("evidence-less repo must honestly stay NULL (unknown), got %v", cleanStamp)
	}
}
