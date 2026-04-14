package collector

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
	var cr struct{ Copyright string `json:"copyright"` }
	if err := json.Unmarshal(f.Copyrights[0], &cr); err != nil {
		t.Fatalf("unmarshal copyright: %v", err)
	}
	if cr.Copyright != "Copyright 2024 Example Inc." {
		t.Errorf("Copyright = %q", cr.Copyright)
	}
}

// TestScancodeResultStruct verifies ScancodeResult has expected fields.
func TestScancodeResultStruct(t *testing.T) {
	r := ScancodeResult{
		ScancodeVersion:  "32.5.0",
		FilesScanned:     42,
		FilesWithFindings: 10,
		DurationSecs:     12.5,
		FileResults: []ScancodeFileResult{
			{
				Path:                         "main.go",
				ProgrammingLanguage:          "Go",
				DetectedLicenseExpressionSPDX: "MIT",
			},
		},
	}
	if r.ScancodeVersion != "32.5.0" {
		t.Errorf("ScancodeVersion = %q", r.ScancodeVersion)
	}
	if r.FilesScanned != 42 {
		t.Errorf("FilesScanned = %d", r.FilesScanned)
	}
	if r.FilesWithFindings != 10 {
		t.Errorf("FilesWithFindings = %d", r.FilesWithFindings)
	}
	if len(r.FileResults) != 1 {
		t.Fatalf("FileResults = %d", len(r.FileResults))
	}
}

// TestRunScanCodeFunctionExists verifies RunScanCode is defined with expected signature.
func TestRunScanCodeFunctionExists(t *testing.T) {
	src, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "func RunScanCode(") {
		t.Error("RunScanCode function must exist in scancode.go")
	}
	// Must accept a local path to scan.
	if !strings.Contains(code, "localPath string") {
		t.Error("RunScanCode must accept a localPath parameter")
	}
}

// TestRunScanCodeUsesCorrectFlags verifies the CLI flags used.
func TestRunScanCodeUsesCorrectFlags(t *testing.T) {
	src, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must use license, copyright, package, and info flags.
	if !strings.Contains(code, "-clpi") {
		t.Error("RunScanCode must use -clpi flags (copyright, license, package, info)")
	}
	// Must use --only-findings to reduce output size.
	if !strings.Contains(code, "--only-findings") {
		t.Error("RunScanCode must use --only-findings to reduce output size")
	}
	// Must use --json for machine-readable output.
	if !strings.Contains(code, "--json") {
		t.Error("RunScanCode must use --json output format")
	}
	// Must use --quiet to suppress progress output.
	if !strings.Contains(code, "--quiet") {
		t.Error("RunScanCode must use --quiet flag")
	}
	// Must limit internal Python process parallelism.
	if !strings.Contains(code, "--processes") {
		t.Error("RunScanCode must use --processes to limit Python thread/process count")
	}
	// Must limit in-memory file count for large repos.
	if !strings.Contains(code, "--max-in-memory") {
		t.Error("RunScanCode must use --max-in-memory to cap memory usage")
	}
}

// TestScancodeConcurrencySemaphore verifies that a package-level semaphore
// limits concurrent ScanCode invocations.
func TestScancodeConcurrencySemaphore(t *testing.T) {
	src, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "scancodeSem") {
		t.Error("scancode.go must define scancodeSem to limit concurrent invocations")
	}
	// The semaphore must be used in RunScanCode to acquire/release slots.
	if !strings.Contains(code, "scancodeSem <-") || !strings.Contains(code, "<-scancodeSem") {
		t.Error("RunScanCode must acquire and release scancodeSem")
	}
}

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

// TestAnalysisCallsScanCode verifies analysis.go includes scancode as a phase.
func TestAnalysisCallsScanCode(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "scanScanCode") || !strings.Contains(code, "RunScanCode") {
		t.Error("analysis.go must call scancode as a phase (scanScanCode or RunScanCode)")
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
	for _, fn := range []string{"InsertScancodeScan", "InsertScancodeFileResultBatch", "ScancodeLastRun"} {
		if !strings.Contains(storeCode, fn) {
			t.Errorf("scancode_store.go must contain %s", fn)
		}
	}

	// RotateScancodeToHistory lives in history.go alongside other rotation methods.
	historySrc, err := os.ReadFile("../db/history.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(historySrc), "RotateScancodeToHistory") {
		t.Error("history.go must contain RotateScancodeToHistory")
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

// TestScancode30DaySkipLogic verifies scancode checks last-run date and skips
// if within 30 days.
func TestScancode30DaySkipLogic(t *testing.T) {
	src, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ScancodeLastRun") {
		t.Error("RunScanCode must check ScancodeLastRun to implement 30-day skip")
	}
	if !strings.Contains(code, "30") {
		t.Error("RunScanCode must reference 30-day interval")
	}
}

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

	if !strings.Contains(code, "RotateScancodeToHistory") {
		t.Error("history.go must contain RotateScancodeToHistory")
	}
	if !strings.Contains(code, "scancode_scans_history") {
		t.Error("RotateScancodeToHistory must reference scancode_scans_history")
	}
	if !strings.Contains(code, "scancode_file_results_history") {
		t.Error("RotateScancodeToHistory must reference scancode_file_results_history")
	}
}
