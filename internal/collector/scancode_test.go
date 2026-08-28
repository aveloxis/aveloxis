// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestScancodeOutputParsing verifies we can parse ScanCode JSON output.
func TestScancodeOutputParsing(t *testing.T) {
	raw := `{
		"headers": [{"tool_name": "scancode-toolkit", "tool_version": "32.5.0", "duration": 12.5, "extra_data": {"files_count": 42}}],
		"files": [
			{
				"path": "src/main.go",
				"type": "file",
				"programming_language": "Go",
				"detected_license_expression": "apache-2.0",
				"detected_license_expression_spdx": "Apache-2.0",
				"percentage_of_license_text": 3.8,
				"copyrights": [{"copyright": "Copyright 2024 Example Inc.", "start_line": 1, "end_line": 1}],
				"holders": [{"holder": "Example Inc.", "start_line": 1, "end_line": 1}],
				"license_detections": [{"license_expression": "apache-2.0"}],
				"package_data": [],
				"scan_errors": []
			}
		]
	}`
	var output scancodeOutput
	if err := json.Unmarshal([]byte(raw), &output); err != nil {
		t.Fatalf("failed to parse scancode JSON: %v", err)
	}
	if len(output.Headers) != 1 {
		t.Fatalf("Headers = %d, want 1", len(output.Headers))
	}
	if output.Headers[0].ToolVersion != "32.5.0" {
		t.Errorf("ToolVersion = %q, want 32.5.0", output.Headers[0].ToolVersion)
	}
	if output.Headers[0].Duration != 12.5 {
		t.Errorf("Duration = %f, want 12.5", output.Headers[0].Duration)
	}
	if len(output.Files) != 1 {
		t.Fatalf("Files = %d, want 1", len(output.Files))
	}
	f := output.Files[0]
	if f.Path != "src/main.go" {
		t.Errorf("Path = %q", f.Path)
	}
	if f.ProgrammingLanguage != "Go" {
		t.Errorf("ProgrammingLanguage = %q", f.ProgrammingLanguage)
	}
	if f.DetectedLicenseExpressionSPDX != "Apache-2.0" {
		t.Errorf("DetectedLicenseExpressionSPDX = %q", f.DetectedLicenseExpressionSPDX)
	}
	if f.PercentageOfLicenseText != 3.8 {
		t.Errorf("PercentageOfLicenseText = %f", f.PercentageOfLicenseText)
	}
	if len(f.Copyrights) != 1 {
		t.Fatalf("Copyrights = %d, want 1", len(f.Copyrights))
	}
	// Copyrights are json.RawMessage — verify the raw JSON content.
	var cr struct {
		Copyright string `json:"copyright"`
	}
	if err := json.Unmarshal(f.Copyrights[0], &cr); err != nil {
		t.Fatalf("unmarshal copyright: %v", err)
	}
	if cr.Copyright != "Copyright 2024 Example Inc." {
		t.Errorf("Copyright = %q", cr.Copyright)
	}
}

// v0.21.0 — TestScancodeResultStruct / TestRunScanCodeFunctionExists /
// TestRunScanCodeUsesCorrectFlags / TestScancodeConcurrencySemaphore /
// TestAnalysisCallsScanCode were retired in this release. They
// pinned the pre-v0.21.0 architecture: per-job RunScanCode call
// from AnalysisCollector with a 2-slot package-level semaphore.
// That architecture was the root cause of the 2026-05-14 incident
// (177 of 180 workers parked on the semaphore for 7+ hours). The
// equivalent invariants for the new architecture are pinned by:
//
//   - TestAnalyzeRepoNoLongerInvokesScancode      (analysis_no_scancode_test.go)
//   - TestScancodeSemaphoreNoLongerExists         (analysis_no_scancode_test.go)
//   - TestScancodeNoLongerHas30DaySkipCheck       (analysis_no_scancode_test.go)
//   - TestScancodeWorker* family                  (scancode_worker_test.go)
//   - TestClaim* family                           (scancode_worker_test.go)

// TestAnalysisResultHasScancodeFiles verifies AnalysisResult tracks scancode findings.
func TestAnalysisResultHasScancodeFiles(t *testing.T) {
	r := AnalysisResult{
		Dependencies:  10,
		ScancodeFiles: 5,
	}
	if r.ScancodeFiles != 5 {
		t.Errorf("ScancodeFiles = %d, want 5", r.ScancodeFiles)
	}
}

// TestScancodeToolRegistered verifies scancode is in ExternalTools().
func TestScancodeToolRegistered(t *testing.T) {
	tools := ExternalTools()
	found := false
	for _, tool := range tools {
		if tool.Name == "scancode" {
			found = true
			if tool.CheckBinary != "scancode" {
				t.Errorf("scancode CheckBinary = %q, want 'scancode'", tool.CheckBinary)
			}
			if tool.InstallCmd == "" {
				t.Error("scancode must have an InstallCmd")
			}
			if tool.InstallFunc == nil {
				t.Error("scancode must have an InstallFunc (tries pipx then pip)")
			}
			break
		}
	}
	if !found {
		t.Error("scancode must be registered in ExternalTools()")
	}
}

// TestScancodeSchemaExists verifies schema.sql contains the aveloxis_scan schema
// and scancode tables.
func TestScancodeSchemaExists(t *testing.T) {
	src, err := os.ReadFile("../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "aveloxis_scan") {
		t.Error("schema.sql must create aveloxis_scan schema")
	}
	if !strings.Contains(code, "scancode_scans") {
		t.Error("schema.sql must create scancode_scans table")
	}
	if !strings.Contains(code, "scancode_file_results") {
		t.Error("schema.sql must create scancode_file_results table")
	}
	if !strings.Contains(code, "scancode_scans_history") {
		t.Error("schema.sql must create scancode_scans_history table")
	}
	if !strings.Contains(code, "scancode_file_results_history") {
		t.Error("schema.sql must create scancode_file_results_history table")
	}
}

// TestScancodeStoreMethodsExist verifies store methods for scancode data.
func TestScancodeStoreMethodsExist(t *testing.T) {
	storeSrc, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	storeCode := string(storeSrc)

	// Methods in scancode_store.go.
	// Round 8: InsertScancodeScan / InsertScancodeFileResultBatch were
	// DELETED — the v0.28.19 fusion made them unreachable, and pinning
	// their existence made this test assert dead code and describe an
	// ingest path that no longer exists.
	for _, fn := range []string{"ReplaceScancodeSnapshot", "ScancodeLastRun"} {
		if !strings.Contains(storeCode, fn) {
			t.Errorf("scancode_store.go must contain %s", fn)
		}
	}

	// The rotation statements live in history.go as one shared helper.
	historySrc, err := os.ReadFile("../db/history.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(historySrc), "func rotateScancodeRows(") {
		t.Error("history.go must contain rotateScancodeRows")
	}
}

// TestScancodeLastRunReturnsTime verifies the 30-day skip check method signature.
func TestScancodeLastRunReturnsTime(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must query the scancode_scans table for last run time.
	if !strings.Contains(code, "scancode_scans") {
		t.Error("ScancodeLastRun must query scancode_scans table")
	}
	if !strings.Contains(code, "data_collection_date") {
		t.Error("ScancodeLastRun must check data_collection_date")
	}
}

// v0.21.0 — TestScancode30DaySkipLogic was retired in this release.
// The 30-day inline skip check in scancode.go was replaced by the
// configurable cadence (default 180 days) enforced at claim time
// in db.ClaimNextScancodeRepo. The equivalent invariant is now
// pinned by TestClaimGatesOnCadenceAndStaleLock in
// scancode_worker_test.go and the
// collection.scancode_cadence_days knob in
// scancode_knobs_test.go.

// TestScancodeStoreHasSBOMMethod verifies the DB has a method to retrieve
// scancode data for SBOM enrichment.
func TestScancodeStoreHasSBOMMethod(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetScancodeForSBOM") {
		t.Error("scancode_store.go must contain GetScancodeForSBOM for SBOM enrichment")
	}
}

// TestScancodeStoreHasSourceLicensesMethod verifies the DB has a method to
// retrieve aggregated source code license counts for the web dashboard.
func TestScancodeStoreHasSourceLicensesMethod(t *testing.T) {
	src, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetScancodeSourceLicenses") {
		t.Error("scancode_store.go must contain GetScancodeSourceLicenses for dashboard")
	}
}

// TestScancodeAPIEndpointExists verifies the API server has a scancode
// licenses endpoint.
func TestScancodeAPIEndpointExists(t *testing.T) {
	src, err := os.ReadFile("../api/server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "scancode-licenses") {
		t.Error("API server must have a /api/v1/repos/{id}/scancode-licenses endpoint")
	}
}

// TestScancodeHistoryRotation verifies history.go has scancode rotation.
func TestScancodeHistoryRotation(t *testing.T) {
	src, err := os.ReadFile("../db/history.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "func rotateScancodeRows(") {
		t.Error("history.go must contain rotateScancodeRows")
	}
	if !strings.Contains(code, "scancode_scans_history") {
		t.Error("rotateScancodeRows must reference scancode_scans_history")
	}
	if !strings.Contains(code, "scancode_file_results_history") {
		t.Error("RotateScancodeToHistory must reference scancode_file_results_history")
	}
}

// v0.28.1 (A5) — scancode paths are stored repository-root-relative:
// the clone-dir segment (repo_<id>_<nanos>, the scan root's basename
// that scancode emits relative to the root's PARENT) is stripped at
// ingest. v0.28.5 (Copilot round): the strip is the ONE shared
// db.StripScancodeRootPrefix (SR-17), also applied at the API read
// path so historical prefixed rows serve clean to every consumer.
func TestStripScanRootPrefix(t *testing.T) {
	cases := map[string]string{
		"repo_4366_1780813842500810892/.ci/docker/install_cuda.sh": ".ci/docker/install_cuda.sh",
		"repo_1_2/LICENSE":       "LICENSE",
		"src/main.go":            "src/main.go",            // no prefix — untouched
		"src/repo_1_2/x.go":      "src/repo_1_2/x.go",      // mid-path lookalike — untouched
		"repo_abc_123/notreally": "repo_abc_123/notreally", // non-numeric — untouched
	}
	for in, want := range cases {
		if got := db.StripScancodeRootPrefix(in); got != want {
			t.Errorf("StripScancodeRootPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// The ingest path must actually apply the strip.
func TestIngestStripsScanRootPrefix(t *testing.T) {
	data, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "db.StripScancodeRootPrefix(f.Path)") {
		t.Error("ingest must store db.StripScancodeRootPrefix(f.Path), not the raw scancode path")
	}
	// And the READ path serves historical prefixed rows clean too.
	storeSrc, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(storeSrc), "Path:      StripScancodeRootPrefix(path)") {
		t.Error("GetScancodeFileEntries must strip the scan-root prefix at read time — historical rows keep it in storage until their next rescan")
	}
}
