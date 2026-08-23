// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
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
