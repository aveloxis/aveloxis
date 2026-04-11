// Package collector — scancode.go runs ScanCode Toolkit against a local
// repository checkout to detect licenses, copyrights, and packages per file.
//
// ScanCode (https://github.com/aboutcode-org/scancode-toolkit) is a Python
// tool installed via `pipx install scancode-toolkit`. If not installed on
// PATH, this phase is silently skipped.
//
// ScanCode only needs to run every 30 days per repo — license and copyright
// data changes infrequently. The last-run timestamp is checked via
// ScancodeLastRun before invoking the tool.
//
// Results are stored in the aveloxis_scan schema:
//   - scancode_scans: one row per scan run (metadata, duration, file count)
//   - scancode_file_results: per-file license, copyright, and package findings
//   - History tables rotate previous results before each new scan.
//
// Assumptions:
//   - The `scancode` binary is installed and on PATH
//   - ScanCode is invoked with -clpi (copyright, license, package, info)
//   - --only-findings reduces output to files with actual detections
//   - --quiet suppresses progress output
//   - --timeout 300 gives 5 min per file (some files are pathological)
//   - Output goes to a temp JSON file, parsed after completion
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
)

// ScancodeRunInterval is how often ScanCode should run per repo.
// License and copyright data changes infrequently, so 30 days is sufficient.
const ScancodeRunInterval = 30 * 24 * time.Hour

// ScancodeResult holds the parsed summary from a ScanCode run.
type ScancodeResult struct {
	ScancodeVersion   string
	FilesScanned      int
	FilesWithFindings int
	DurationSecs      float64
	FileResults       []ScancodeFileResult
	Errors            []string
}

// ScancodeFileResult holds per-file findings from ScanCode.
type ScancodeFileResult struct {
	Path                             string
	FileType                         string
	ProgrammingLanguage              string
	DetectedLicenseExpression        string
	DetectedLicenseExpressionSPDX    string
	PercentageOfLicenseText          float64
	Copyrights                       json.RawMessage // JSONB array of {copyright, start_line, end_line}
	Holders                          json.RawMessage // JSONB array of {holder, start_line, end_line}
	LicenseDetections                json.RawMessage // JSONB array of detection details
	PackageData                      json.RawMessage // JSONB array of package metadata
	ScanErrors                       json.RawMessage // JSONB array of error strings
}

// RunScanCode executes ScanCode Toolkit against a local checkout and stores
// results in the aveloxis_scan schema. Skips if scancode is not installed or
// if the last scan was within 30 days.
//
// The localPath must point to an existing checkout (the temp analysis clone).
func RunScanCode(ctx context.Context, store *db.PostgresStore, repoID int64, localPath string, logger *slog.Logger) (*ScancodeResult, error) {
	// Check if scancode is installed.
	scancodePath, err := exec.LookPath("scancode")
	if err != nil {
		logger.Info("scancode not installed, skipping ScanCode analysis",
			"install", "pipx install scancode-toolkit")
		return nil, nil
	}

	// Check if we ran scancode within the last 30 days for this repo.
	lastRun, err := store.ScancodeLastRun(ctx, repoID)
	if err == nil && !lastRun.IsZero() && time.Since(lastRun) < ScancodeRunInterval {
		logger.Info("scancode ran recently, skipping",
			"repo_id", repoID,
			"last_run", lastRun,
			"next_due", lastRun.Add(ScancodeRunInterval).Format("2006-01-02"))
		return nil, nil
	}

	if localPath == "" {
		logger.Warn("scancode skipped: no local clone path", "repo_id", repoID)
		return nil, nil
	}

	logger.Info("running ScanCode", "repo_id", repoID, "path", localPath)

	// Write output to a temp file — scancode writes JSON to a file, not stdout.
	outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("aveloxis-scancode-%d-%d.json", repoID, time.Now().UnixNano()))
	defer os.Remove(outputFile)

	cmd := exec.CommandContext(ctx, scancodePath,
		"-clpi",
		"--only-findings",
		"--json", outputFile,
		"--quiet",
		"--timeout", "300",
		localPath,
	)
	var stderrBuf []byte
	cmd.Stderr = nil // scancode writes progress to stderr; --quiet suppresses it

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("scancode failed: %w", err)
	}

	// Read and parse the output file.
	data, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, fmt.Errorf("reading scancode output: %w", err)
	}
	// stderrBuf is declared for future error context capture but currently unused.
	if len(stderrBuf) > 0 {
		logger.Debug("scancode stderr output", "repo_id", repoID, "stderr", string(stderrBuf))
	}

	var raw scancodeOutput
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing scancode output: %w", err)
	}

	// Build result from parsed output.
	result := &ScancodeResult{}
	if len(raw.Headers) > 0 {
		result.ScancodeVersion = raw.Headers[0].ToolVersion
		result.DurationSecs = raw.Headers[0].Duration
		if raw.Headers[0].ExtraData.FilesCount > 0 {
			result.FilesScanned = raw.Headers[0].ExtraData.FilesCount
		}
	}

	// Rotate previous results to history before inserting.
	if err := store.RotateScancodeToHistory(ctx, repoID); err != nil {
		logger.Warn("failed to rotate scancode history", "repo_id", repoID, "error", err)
	}

	// Insert scan metadata.
	for _, f := range raw.Files {
		if f.Type != "file" {
			continue // skip directory entries
		}
		if hasFindings(f) {
			result.FilesWithFindings++
		}
	}

	scanID, err := store.InsertScancodeScan(ctx, repoID, result.ScancodeVersion,
		result.FilesScanned, result.FilesWithFindings, result.DurationSecs, nil)
	if err != nil {
		return result, fmt.Errorf("inserting scancode scan: %w", err)
	}
	logger.Debug("scancode scan recorded", "repo_id", repoID, "scan_id", scanID)

	// Collect file results for batch insert.
	var fileResults []ScancodeFileResult
	var dbRows []*db.ScancodeFileRow
	for _, f := range raw.Files {
		if f.Type != "file" {
			continue
		}
		if !hasFindings(f) {
			continue // --only-findings should filter, but double-check
		}

		copyrightsJSON, err := json.Marshal(f.Copyrights)
		if err != nil {
			logger.Warn("failed to marshal copyrights", "path", f.Path, "error", err)
		}
		holdersJSON, err := json.Marshal(f.Holders)
		if err != nil {
			logger.Warn("failed to marshal holders", "path", f.Path, "error", err)
		}
		licenseDetJSON, err := json.Marshal(f.LicenseDetections)
		if err != nil {
			logger.Warn("failed to marshal license detections", "path", f.Path, "error", err)
		}
		packageJSON, err := json.Marshal(f.PackageData)
		if err != nil {
			logger.Warn("failed to marshal package data", "path", f.Path, "error", err)
		}
		errorsJSON, err := json.Marshal(f.ScanErrors)
		if err != nil {
			logger.Warn("failed to marshal scan errors", "path", f.Path, "error", err)
		}

		fileResults = append(fileResults, ScancodeFileResult{
			Path:                          f.Path,
			FileType:                      f.FileType,
			ProgrammingLanguage:           f.ProgrammingLanguage,
			DetectedLicenseExpression:     f.DetectedLicenseExpression,
			DetectedLicenseExpressionSPDX: f.DetectedLicenseExpressionSPDX,
			PercentageOfLicenseText:       f.PercentageOfLicenseText,
			Copyrights:                    copyrightsJSON,
			Holders:                       holdersJSON,
			LicenseDetections:             licenseDetJSON,
			PackageData:                   packageJSON,
			ScanErrors:                    errorsJSON,
		})

		dbRows = append(dbRows, &db.ScancodeFileRow{
			Path:                          f.Path,
			FileType:                      f.FileType,
			ProgrammingLanguage:           f.ProgrammingLanguage,
			DetectedLicenseExpression:     f.DetectedLicenseExpression,
			DetectedLicenseExpressionSPDX: f.DetectedLicenseExpressionSPDX,
			PercentageOfLicenseText:       f.PercentageOfLicenseText,
			Copyrights:                    copyrightsJSON,
			Holders:                       holdersJSON,
			LicenseDetections:             licenseDetJSON,
			PackageData:                   packageJSON,
			ScanErrors:                    errorsJSON,
		})
	}

	result.FileResults = fileResults

	// Batch insert file results.
	if err := store.InsertScancodeFileResultBatch(ctx, repoID, dbRows); err != nil {
		return result, fmt.Errorf("inserting scancode file results: %w", err)
	}

	logger.Info("scancode complete",
		"repo_id", repoID,
		"version", result.ScancodeVersion,
		"files_scanned", result.FilesScanned,
		"files_with_findings", result.FilesWithFindings,
		"duration_secs", result.DurationSecs)

	return result, nil
}

// hasFindings returns true if a scancode file entry has any detections.
func hasFindings(f scancodeFile) bool {
	return f.DetectedLicenseExpression != "" ||
		len(f.Copyrights) > 0 ||
		len(f.PackageData) > 0
}

// scancodeOutput is the top-level JSON structure from `scancode --json`.
type scancodeOutput struct {
	Headers []scancodeHeader `json:"headers"`
	Files   []scancodeFile   `json:"files"`
}

type scancodeHeader struct {
	ToolName    string  `json:"tool_name"`
	ToolVersion string  `json:"tool_version"`
	Duration    float64 `json:"duration"`
	ExtraData   struct {
		FilesCount int `json:"files_count"`
	} `json:"extra_data"`
}

type scancodeFile struct {
	Path                          string            `json:"path"`
	Type                          string            `json:"type"`
	FileType                      string            `json:"file_type"`
	ProgrammingLanguage           string            `json:"programming_language"`
	DetectedLicenseExpression     string            `json:"detected_license_expression"`
	DetectedLicenseExpressionSPDX string            `json:"detected_license_expression_spdx"`
	PercentageOfLicenseText       float64           `json:"percentage_of_license_text"`
	Copyrights                    []json.RawMessage `json:"copyrights"`
	Holders                       []json.RawMessage `json:"holders"`
	LicenseDetections             []json.RawMessage `json:"license_detections"`
	PackageData                   []json.RawMessage `json:"package_data"`
	ScanErrors                    []json.RawMessage `json:"scan_errors"`
}
