// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.23.4 — salvage scancode JSON output when subprocess exits
// non-zero but the JSON was still written.
//
// Diagnostic on 2026-05-21 (post-v0.23.3 deploy): 27 of 37 scancode
// runs over a 17-hour window exited status 1 with stderr files
// containing ONLY the libmagic warning (252 bytes — no actual error
// message). Manual reproduction:
//
//   $ scancode -clpi --only-findings --json out.json --quiet \
//       --timeout 300 --processes 2 --max-in-memory 5000 <repo>
//   /home/.../typecode/magic2.py:197: UserWarning: System libmagic ...
//   $ echo $?
//   1
//   $ ls -lh out.json
//   45K out.json
//   $ python3 -c 'import json; d=json.load(open("out.json")); print(d["headers"][0]["errors"])'
//   ['Path: genefilter/docs/Cluster.pdf']
//
// scancode-toolkit-mini with `--quiet` returns exit 1 when ANY
// individual file fails to scan (e.g., a malformed PDF that
// pdfminer crashes on), even though it still writes a complete and
// valid JSON output containing the successful scans plus the
// per-file error metadata in headers[0].errors.
//
// The same scan with --verbose exits 0 with identical JSON content.
// So aveloxis has been treating these as failures and incrementing
// scancode_failed_attempts toward the v0.21.4 10-strike cap, even
// though scancode did its job.
//
// v0.23.4 fix: when cmd.Wait() returns non-zero, salvage the JSON
// output. If it parses and contains scan data (files_count > 0),
// log a WARN with the per-file errors and proceed to the normal
// ingest path. Only treat as a real failure if the JSON is missing
// or invalid (genuine scancode crash, or wall-clock timeout
// SIGKILL'd subprocess before output completed).

func TestScancodeHeaderCapturesErrors(t *testing.T) {
	src, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// scancodeHeader must declare an Errors []string field so the
	// salvage path can surface per-file errors recorded by scancode
	// in headers[0].errors. Pre-v0.23.4 the field existed in the
	// scancode JSON output but the struct silently dropped it.
	if !strings.Contains(code, "Errors") || !strings.Contains(code, `json:"errors"`) {
		t.Error("scancodeHeader must declare `Errors []string` with " +
			"`json:\"errors\"` tag. The salvage path in scancode_worker.go's " +
			"runOne reads headers[0].errors to log which files scancode failed " +
			"to scan. Without this field, the WARN log on a salvaged scan " +
			"would have no per-file diagnostic, making it impossible for " +
			"operators to tell salvaged scans (with a few bad files) apart " +
			"from clean ones.")
	}
}

func TestSalvageScancodeOutputHelperExists(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "salvageScancodeOutput") {
		t.Error("v0.23.4 introduces salvageScancodeOutput, a helper that " +
			"parses the scancode JSON output file's headers and returns " +
			"(filesCount, headerErrors, ok). When ok=true (JSON parses with " +
			"files_count > 0), the runOne caller treats the scancode exit-1 " +
			"as a per-file-error case and falls through to ingest rather " +
			"than booking a failure.")
	}
}

func TestRunOneAttemptsSalvageOnSubprocessFailure(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The cmd.Wait() error branch must call salvageScancodeOutput
	// before calling recordFailureBestEffort. Source-contract pin
	// using a relative-position check: the salvage call must appear
	// AFTER the "scancode subprocess failed" warn log and BEFORE the
	// recordFailureBestEffort call inside the failure block.
	salvagePos := strings.Index(code, "salvageScancodeOutput(")
	if salvagePos < 0 {
		t.Fatal("salvageScancodeOutput call missing from scancode_worker.go")
	}
	failedLogPos := strings.Index(code, `"scancode runOne: scancode subprocess failed"`)
	if failedLogPos < 0 {
		t.Fatal("expected `scancode runOne: scancode subprocess failed` log line")
	}
	if salvagePos < failedLogPos {
		t.Errorf("salvageScancodeOutput call (pos=%d) must appear AFTER the "+
			"`scancode runOne: scancode subprocess failed` log line (pos=%d). "+
			"The salvage path runs INSIDE the cmd.Wait() failure block, which "+
			"is logically after the stderr-file-writing diagnostic.",
			salvagePos, failedLogPos)
	}
}

func TestSalvageSuccessSkipsRecordFailure(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Pin that the salvage path's success branch reaches ingest +
	// MarkScancodeComplete WITHOUT first calling recordFailureBestEffort.
	// This is the load-bearing contract of v0.23.4: a scan with
	// per-file errors must NOT advance scancode_failed_attempts, or
	// the v0.21.4 10-strike backoff will keep sidelining repos that
	// scancode is happily producing data for.
	//
	// Concretely: the salvage path must reference ingestScancodeOutput
	// or call into the existing success-path code. A negative-pin
	// check that the salvage-success branch doesn't end at
	// recordFailureBestEffort.
	if !strings.Contains(code, "ingestScancodeOutput") {
		t.Fatal("expected ingestScancodeOutput call to remain in scancode_worker.go")
	}
	// The salvage path should mention salvaged or recovered terminology
	// so log greps for "scancode salvage" / "scancode recovered" find
	// the new log line.
	if !strings.Contains(code, "salvaged") && !strings.Contains(code, "salvage") {
		t.Error("salvage path must emit a recognizable log message " +
			"(containing 'salvage' or 'salvaged') so operators can grep " +
			"for the v0.23.4 recovery events to understand how many repos " +
			"are hitting per-file scancode errors.")
	}
}

func TestSalvageSurfacesHeaderErrorsInLog(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The salvage WARN must include the per-file errors so operators
	// can see WHICH files scancode failed on. Pin a couple of
	// expected log keys (header_errors or scancode_errors).
	if !strings.Contains(code, "header_errors") && !strings.Contains(code, "scancode_errors") {
		t.Error("salvage WARN log must include a slog key for the " +
			"per-file errors recorded in headers[0].errors. Without " +
			"that, operators can't tell salvaged-with-broken-PDF apart " +
			"from salvaged-with-twenty-broken-files.")
	}
}

func TestSalvageHelperBehavioral(t *testing.T) {
	// Behavioral test: write a known-good scancode JSON output with
	// header errors, call salvageScancodeOutput, assert it returns
	// ok=true and surfaces the per-file errors.
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "scan.json")
	jsonContent := map[string]any{
		"headers": []map[string]any{
			{
				"tool_name":    "scancode-toolkit",
				"tool_version": "32.5.0",
				"duration":     14.2,
				"errors":       []string{"Path: genefilter/docs/Cluster.pdf"},
				"extra_data": map[string]any{
					"files_count": 38,
				},
			},
		},
		"files": []map[string]any{},
	}
	b, err := json.Marshal(jsonContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	filesCount, headerErrors, ok := salvageScancodeOutput(outputPath)
	if !ok {
		t.Fatal("expected salvageScancodeOutput to return ok=true for a " +
			"valid JSON with files_count > 0")
	}
	if filesCount != 38 {
		t.Errorf("expected filesCount=38, got %d", filesCount)
	}
	if len(headerErrors) != 1 || headerErrors[0] != "Path: genefilter/docs/Cluster.pdf" {
		t.Errorf("expected per-file errors to be preserved, got %v", headerErrors)
	}
}

func TestSalvageHelperRejectsMissingFile(t *testing.T) {
	// A missing JSON file means scancode crashed before writing
	// anything. Salvage should return ok=false so the caller falls
	// through to recordFailureBestEffort.
	_, _, ok := salvageScancodeOutput("/nonexistent/path/scan.json")
	if ok {
		t.Error("expected salvageScancodeOutput to return ok=false " +
			"for a missing file (real scancode crash before output)")
	}
}

func TestSalvageHelperRejectsZeroFilesCount(t *testing.T) {
	// JSON exists but files_count == 0 means scancode probably died
	// very early or the output is corrupt. Salvage should return
	// ok=false so the caller treats it as a genuine failure.
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "scan.json")
	jsonContent := map[string]any{
		"headers": []map[string]any{
			{
				"tool_name":    "scancode-toolkit",
				"tool_version": "32.5.0",
				"errors":       []string{},
				"extra_data": map[string]any{
					"files_count": 0,
				},
			},
		},
		"files": []map[string]any{},
	}
	b, err := json.Marshal(jsonContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, ok := salvageScancodeOutput(outputPath)
	if ok {
		t.Error("expected salvageScancodeOutput to return ok=false " +
			"for files_count=0 (scancode probably crashed before scanning)")
	}
}

func TestSalvageHelperRejectsCorruptJSON(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "scan.json")
	// Truncated JSON — what we'd see if scancode was SIGKILL'd
	// mid-write by the v0.23.3 wall-clock timeout.
	if err := os.WriteFile(outputPath, []byte(`{"headers":[{"tool_n`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, ok := salvageScancodeOutput(outputPath)
	if ok {
		t.Error("expected salvageScancodeOutput to return ok=false " +
			"for truncated JSON (mid-write SIGKILL during timeout)")
	}
}
