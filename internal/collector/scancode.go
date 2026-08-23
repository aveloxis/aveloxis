// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — scancode.go provides scancode JSON output
// parsing and ingest into aveloxis_scan tables.
//
// The actual scancode subprocess invocation lives in
// scancode_worker.go (v0.21.0). Pre-v0.21.0 this file also owned
// the subprocess invocation + a 2-slot package-level semaphore +
// a 30-day inline skip check; all of that moved into the
// ScancodeWorker pool after the 2026-05-14 incident showed the
// semaphore stalled 177 of 180 collection workers for 7+ hours.
// See docs/architecture/scancode.md for the architectural
// rationale and the four-state recovery table.
//
// What stays here:
//   - The scancode JSON output struct definitions (scancodeOutput,
//     scancodeHeader, scancodeFile) — these are the wire format
//     scancode emits to its --json output file.
//   - hasFindings(f) — returns true if a parsed file entry has any
//     detection worth persisting.
//   - ingestScancodeOutput — parses the JSON output file at the
//     given path and writes the results to aveloxis_scan tables
//     (with history rotation). Called from scancode_worker.runOne
//     after cmd.Wait() succeeds, and from the recovery monitor for
//     orphaned scans that exited while aveloxis was down.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
)

// ingestScancodeOutput parses a scancode JSON output file and
// writes scancode_scans + scancode_file_results, rotating any
// previous results to the *_history tables first. Returns the
// scancode binary version that produced the output (from the
// JSON's headers).
//
// Safe to call from multiple goroutines on different repoIDs
// because all writes target rows keyed by repoID with appropriate
// per-row history rotation.
func ingestScancodeOutput(ctx context.Context, store *db.PostgresStore, repoID int64, outputPath string, logger *slog.Logger) (string, error) {
	data, err := os.ReadFile(outputPath)
	if err != nil {
		return "", fmt.Errorf("reading scancode output: %w", err)
	}
	var raw scancodeOutput
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("parsing scancode output: %w", err)
	}

	var version string
	var duration float64
	var filesScanned int
	if len(raw.Headers) > 0 {
		version = raw.Headers[0].ToolVersion
		duration = raw.Headers[0].Duration
		if raw.Headers[0].ExtraData.FilesCount > 0 {
			filesScanned = raw.Headers[0].ExtraData.FilesCount
		}
	}

	if err := store.RotateScancodeToHistory(ctx, repoID); err != nil {
		logger.Warn("failed to rotate scancode history", "repo_id", repoID, "error", err)
	}

	var filesWithFindings int
	for _, f := range raw.Files {
		if f.Type != "file" {
			continue
		}
		if hasFindings(f) {
			filesWithFindings++
		}
	}

	scanID, err := store.InsertScancodeScan(ctx, repoID, version,
		filesScanned, filesWithFindings, duration, nil)
	if err != nil {
		return version, fmt.Errorf("inserting scancode scan: %w", err)
	}
	logger.Debug("scancode scan recorded", "repo_id", repoID, "scan_id", scanID)

	var dbRows []*db.ScancodeFileRow
	for _, f := range raw.Files {
		if f.Type != "file" || !hasFindings(f) {
			continue
		}
		copyrightsJSON, _ := json.Marshal(f.Copyrights)
		holdersJSON, _ := json.Marshal(f.Holders)
		licenseDetJSON, _ := json.Marshal(f.LicenseDetections)
		packageJSON, _ := json.Marshal(f.PackageData)
		errorsJSON, _ := json.Marshal(f.ScanErrors)
		dbRows = append(dbRows, &db.ScancodeFileRow{
			Path:                          db.StripScancodeRootPrefix(f.Path),
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

	if err := store.InsertScancodeFileResultBatch(ctx, repoID, dbRows); err != nil {
		return version, fmt.Errorf("inserting scancode file results: %w", err)
	}
	return version, nil
}

// hasFindings returns true if a scancode file entry has any
// detection worth recording. Pre-v0.21.0 this was conservative
// (excluded files with only license-text-percentage signals); kept
// as-is for behavioral parity with existing scancode_file_results
// rows.
func hasFindings(f scancodeFile) bool {
	return f.DetectedLicenseExpression != "" ||
		len(f.Copyrights) > 0 ||
		len(f.PackageData) > 0
}

// scancodeOutput is the top-level JSON structure from
// `scancode --json`.
type scancodeOutput struct {
	Headers []scancodeHeader `json:"headers"`
	Files   []scancodeFile   `json:"files"`
}

type scancodeHeader struct {
	ToolName    string   `json:"tool_name"`
	ToolVersion string   `json:"tool_version"`
	Duration    float64  `json:"duration"`
	Errors      []string `json:"errors"`
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
