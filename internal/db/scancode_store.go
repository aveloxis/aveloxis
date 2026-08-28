// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ScancodeFileRow holds per-file scancode data for batch insertion.
type ScancodeFileRow struct {
	Path                          string
	FileType                      string
	ProgrammingLanguage           string
	DetectedLicenseExpression     string
	DetectedLicenseExpressionSPDX string
	PercentageOfLicenseText       float64
	Copyrights                    json.RawMessage
	Holders                       json.RawMessage
	LicenseDetections             json.RawMessage
	PackageData                   json.RawMessage
	ScanErrors                    json.RawMessage
}

// ScancodeLastRun returns the most recent scan date for a repo, or zero time
// if no scan exists. Used to implement the 30-day skip interval.
func (s *PostgresStore) ScancodeLastRun(ctx context.Context, repoID int64) (time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT data_collection_date FROM aveloxis_scan.scancode_scans
		WHERE repo_id = $1
		ORDER BY data_collection_date DESC
		LIMIT 1`, repoID).Scan(&t)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// ScancodeFreshness returns the v0.21.0 scancode_last_run timestamp
// and scancode_version string off aveloxis_data.repos. Both values
// are NULL-tolerant: a repo with no scancode_last_run returns
// (zero-time, "", nil) — the API surfaces "not yet run" to the
// user instead of an error.
//
// Reads from aveloxis_data.repos (NOT aveloxis_scan.scancode_scans)
// because the ScancodeWorker writes the freshness columns
// atomically alongside the scan inserts in MarkScancodeComplete.
// This gives a single source of truth for "when did this repo last
// successfully complete scancode" that the dashboard can rely on
// without joining scancode_scans + max(date) on every render.
func (s *PostgresStore) ScancodeFreshness(ctx context.Context, repoID int64) (time.Time, string, error) {
	var lastRun *time.Time
	var version *string
	err := s.pool.QueryRow(ctx, `
		SELECT scancode_last_run, scancode_version
		FROM aveloxis_data.repos
		WHERE repo_id = $1`, repoID).Scan(&lastRun, &version)
	if err != nil {
		return time.Time{}, "", err
	}
	var t time.Time
	if lastRun != nil {
		t = *lastRun
	}
	var v string
	if version != nil {
		v = *version
	}
	return t, v, nil
}

// InsertScancodeScan inserts a scan metadata row and returns the scan_id.
func (s *PostgresStore) InsertScancodeScan(ctx context.Context, repoID int64, scancodeVersion string, filesScanned, filesWithFindings int, durationSecs float64, scanErrors json.RawMessage) (int64, error) {
	var scanID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_scan.scancode_scans
			(repo_id, scancode_version, files_scanned, files_with_findings,
			 scan_duration_secs, scan_errors,
			 tool_source, data_source, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, $6,
			'aveloxis-scancode', 'scancode-toolkit', NOW())
		RETURNING scan_id`,
		repoID, scancodeVersion, filesScanned, filesWithFindings, durationSecs, scanErrors).Scan(&scanID)
	return scanID, err
}

// InsertScancodeFileResultBatch inserts per-file scancode results in a single
// round-trip using pgx batch. A scan of a large repo can produce thousands of
// file results, so batching is important for performance.
func (s *PostgresStore) InsertScancodeFileResultBatch(ctx context.Context, repoID int64, rows []*ScancodeFileRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO aveloxis_scan.scancode_file_results
				(repo_id, path, file_type, programming_language,
				 detected_license_expression, detected_license_expression_spdx,
				 percentage_of_license_text,
				 copyrights, holders, license_detections, package_data, scan_errors,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
				'aveloxis-scancode', 'scancode-toolkit', NOW())`,
			repoID, row.Path, row.FileType, row.ProgrammingLanguage,
			row.DetectedLicenseExpression, row.DetectedLicenseExpressionSPDX,
			row.PercentageOfLicenseText,
			row.Copyrights, row.Holders, row.LicenseDetections, row.PackageData, row.ScanErrors)
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

// ScancodeForSBOM holds aggregated scancode data for SBOM enrichment.
type ScancodeForSBOM struct {
	ConcludedLicenseSPDX string   // aggregated SPDX expression (e.g., "Apache-2.0 AND MIT")
	Copyrights           []string // distinct copyright holders
	// ScancodeVersion is the toolkit version of the scan that produced
	// this evidence (v0.27.23). Recorded in the SBOM's tool metadata so
	// the document says WHICH scanner version concluded the licenses —
	// load-bearing for attestability because the external tools are
	// installed unpinned and auto-updated monthly. Empty when the
	// version wasn't recorded (very old scans).
	ScancodeVersion string
}

// GetScancodeForSBOM returns aggregated scancode data for SBOM enrichment:
// the concluded license expression and copyright holders for the repo's source.
func (s *PostgresStore) GetScancodeForSBOM(ctx context.Context, repoID int64) (*ScancodeForSBOM, error) {
	result := &ScancodeForSBOM{}

	// v0.27.23: the version of the latest scan — provenance for the
	// evidence fields below. No rows = repo never scanned = empty
	// version; any other error surfaces.
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(scancode_version, '')
		FROM aveloxis_scan.scancode_scans
		WHERE repo_id = $1
		ORDER BY data_collection_date DESC
		LIMIT 1`, repoID).Scan(&result.ScancodeVersion)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}

	// Get distinct SPDX license expressions from source files.
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT detected_license_expression_spdx
		FROM aveloxis_scan.scancode_file_results
		WHERE repo_id = $1
			AND detected_license_expression_spdx != ''
			AND detected_license_expression_spdx IS NOT NULL
		ORDER BY detected_license_expression_spdx`, repoID)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	var licenses []string
	for rows.Next() {
		var lic string
		if err := rows.Scan(&lic); err != nil {
			return nil, err
		}
		if lic != "" {
			licenses = append(licenses, lic)
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if len(licenses) > 0 {
		// Combine into a single SPDX expression with AND.
		result.ConcludedLicenseSPDX = strings.Join(licenses, " AND ")
	}

	// Get distinct copyright holders.
	holderRows, err := s.pool.Query(ctx, `
		SELECT DISTINCT h->>'holder' AS holder
		FROM aveloxis_scan.scancode_file_results,
			jsonb_array_elements(holders) AS h
		WHERE repo_id = $1
			AND holders IS NOT NULL
			AND jsonb_array_length(holders) > 0
		ORDER BY holder`, repoID)
	if err != nil {
		return result, err
	}
	defer holderRows.Close()

	for holderRows.Next() {
		var holder string
		if err := holderRows.Scan(&holder); err != nil {
			return nil, err
		}
		if holder != "" {
			result.Copyrights = append(result.Copyrights, holder)
		}
	}

	return result, holderRows.Err()
}

// ScancodeSourceLicense is a license detected in source code files by ScanCode.
type ScancodeSourceLicense struct {
	License   string `json:"license"`
	FileCount int    `json:"file_count"`
	IsOSI     bool   `json:"is_osi"`
}

// GetScancodeSourceLicenses returns aggregated per-license file counts from
// ScanCode source code analysis. Used by the web dashboard.
func (s *PostgresStore) GetScancodeSourceLicenses(ctx context.Context, repoID int64) ([]ScancodeSourceLicense, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(TRIM(detected_license_expression_spdx), ''), 'Unknown') AS lic,
			COUNT(*) AS cnt
		FROM aveloxis_scan.scancode_file_results
		WHERE repo_id = $1
		GROUP BY lic
		ORDER BY cnt DESC`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var lic string
		var cnt int
		if err := rows.Scan(&lic, &cnt); err != nil {
			return nil, err
		}
		counts[normalizeLicense(lic)] += cnt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []ScancodeSourceLicense
	for lic, cnt := range counts {
		result = append(result, ScancodeSourceLicense{
			License:   lic,
			FileCount: cnt,
			IsOSI:     isOSILicense(lic),
		})
	}
	slices.SortFunc(result, func(a, b ScancodeSourceLicense) int {
		if a.FileCount != b.FileCount {
			return b.FileCount - a.FileCount
		}
		return strings.Compare(a.License, b.License)
	})
	return result, nil
}

// ScancodeSourceCopyright is a copyright holder detected by ScanCode.
type ScancodeSourceCopyright struct {
	Holder    string `json:"holder"`
	FileCount int    `json:"file_count"`
}

// GetScancodeCopyrights returns distinct copyright holders with file counts.
func (s *PostgresStore) GetScancodeCopyrights(ctx context.Context, repoID int64) ([]ScancodeSourceCopyright, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT h->>'holder' AS holder, COUNT(DISTINCT result_id) AS cnt
		FROM aveloxis_scan.scancode_file_results,
			jsonb_array_elements(holders) AS h
		WHERE repo_id = $1
			AND holders IS NOT NULL
			AND jsonb_array_length(holders) > 0
		GROUP BY holder
		ORDER BY cnt DESC`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ScancodeSourceCopyright
	for rows.Next() {
		var c ScancodeSourceCopyright
		if err := rows.Scan(&c.Holder, &c.FileCount); err != nil {
			return nil, err
		}
		if c.Holder != "" {
			result = append(result, c)
		}
	}
	return result, rows.Err()
}

// ScancodeFileEntry is a single file's license/copyright summary for the web GUI.
// Instead of dumping raw license text, this provides a compact table row:
// filename | detected SPDX license | first copyright holder.
type ScancodeFileEntry struct {
	Path      string `json:"path"`
	License   string `json:"license"`
	Copyright string `json:"copyright"`
}

// scancodeRootSegRe matches the leading clone-directory segment of a
// scancode path: the worker clones into repo_<repoID>_<unixNanos>
// (scancode_worker.go) and scancode emits paths relative to the scan
// root's PARENT, so pre-v0.28.1 stored paths carry that noise segment.
var scancodeRootSegRe = regexp.MustCompile(`^repo_\d+_\d+/`)

// StripScancodeRootPrefix makes scancode paths repository-root-relative
// (v0.28.1 item 5; hoisted here in v0.28.5 as the ONE shared strip,
// SR-17). Applied at BOTH boundaries: collector ingest (new scans
// store clean paths) and the API read below (historical prefixed rows
// serve clean to every consumer without waiting for their next
// 180-day-cadence rescan). The aveloxis-gui display-side strip
// remains as a belt for cached payloads.
func StripScancodeRootPrefix(p string) string {
	return scancodeRootSegRe.ReplaceAllString(p, "")
}

// GetScancodeFileEntries returns per-file license and copyright data for the
// web GUI table. Each row is: file path, SPDX license expression, first
// copyright holder (truncated). Sorted by path for deterministic display
// (historical rows all share one scan-root prefix per scan, so stripping
// preserves the sorted order).
func (s *PostgresStore) GetScancodeFileEntries(ctx context.Context, repoID int64) ([]ScancodeFileEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT path,
		       COALESCE(NULLIF(TRIM(detected_license_expression_spdx), ''), 'Unknown') AS lic,
		       COALESCE(copyrights, '[]'::jsonb) AS copyrights_json
		FROM aveloxis_scan.scancode_file_results
		WHERE repo_id = $1
		ORDER BY path`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ScancodeFileEntry
	for rows.Next() {
		var path, lic string
		var copyrightsJSON []byte
		if err := rows.Scan(&path, &lic, &copyrightsJSON); err != nil {
			return nil, err
		}
		copyright := truncateCopyright(extractFirstCopyrightHolder(copyrightsJSON), 120)
		result = append(result, ScancodeFileEntry{
			Path:      StripScancodeRootPrefix(path),
			License:   NormalizeLicenseToSPDX(lic),
			Copyright: copyright,
		})
	}
	return result, rows.Err()
}

// extractFirstCopyrightHolder extracts the first copyright holder's "value" field
// from a ScanCode copyrights JSON array like:
//
//	[{"value":"Copyright 2024 ACME Corp","start_line":1}, ...]
//
// Returns the first value, with a "(+N more)" suffix if multiple exist.
// Returns empty string for empty/nil/malformed JSON.
func extractFirstCopyrightHolder(jsonData []byte) string {
	if len(jsonData) == 0 {
		return ""
	}
	var entries []struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(jsonData, &entries); err != nil || len(entries) == 0 {
		return ""
	}
	first := entries[0].Value
	if len(entries) > 1 {
		first += fmt.Sprintf(" (+%d more)", len(entries)-1)
	}
	return first
}

// truncateCopyright cuts a copyright string to maxLen characters, appending "..."
// if truncated. Returns the original string if it fits.
func truncateCopyright(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ScancodeScanMeta is the scancode_scans row for one completed scan.
type ScancodeScanMeta struct {
	Version           string
	FilesScanned      int
	FilesWithFindings int
	DurationSecs      float64
	ScanErrors        json.RawMessage
}

// ReplaceScancodeSnapshot rotates the repo's current scancode rows to
// history and writes the new snapshot — rotation and BOTH inserts in
// ONE transaction.
//
// v0.28.19. The ingest used to call RotateScancodeToHistory,
// InsertScancodeScan and InsertScancodeFileResultBatch as three
// independent transactions, guarded only by a ctx check BEFORE the
// first. That guard prevents STARTING the sequence under a done ctx;
// it does nothing about a cancellation (or any failure) landing
// between them, which is a wide window — the file batch is one Exec
// per finding. The repo was then left with its previous snapshot
// deleted and no current rows, or with scan metadata and no file
// rows, until a full re-scan 180 days later. Fusing them is the same
// fix ReplaceRepoLaborSnapshot got in v0.27.7: the rotation can
// neither be skipped nor half-applied, and a mid-insert failure rolls
// the rotation back so the PREVIOUS snapshot stays current.
func (s *PostgresStore) ReplaceScancodeSnapshot(ctx context.Context, repoID int64, meta ScancodeScanMeta, rows []*ScancodeFileRow) (int64, error) {
	var scanID int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if err := rotateScancodeRows(ctx, tx, repoID); err != nil {
			return fmt.Errorf("rotating scancode history: %w", err)
		}

		if err := tx.QueryRow(ctx, `
			INSERT INTO aveloxis_scan.scancode_scans
				(repo_id, scancode_version, files_scanned, files_with_findings,
				 scan_duration_secs, scan_errors,
				 tool_source, data_source, data_collection_date)
			VALUES ($1, $2, $3, $4, $5, $6,
				'aveloxis-scancode', 'scancode-toolkit', NOW())
			RETURNING scan_id`,
			repoID, meta.Version, meta.FilesScanned, meta.FilesWithFindings,
			meta.DurationSecs, meta.ScanErrors).Scan(&scanID); err != nil {
			return fmt.Errorf("inserting scancode scan: %w", err)
		}

		for _, row := range rows {
			if _, err := tx.Exec(ctx, `
				INSERT INTO aveloxis_scan.scancode_file_results
					(repo_id, path, file_type, programming_language,
					 detected_license_expression, detected_license_expression_spdx,
					 percentage_of_license_text,
					 copyrights, holders, license_detections, package_data, scan_errors,
					 tool_source, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
					'aveloxis-scancode', 'scancode-toolkit', NOW())`,
				repoID, row.Path, row.FileType, row.ProgrammingLanguage,
				row.DetectedLicenseExpression, row.DetectedLicenseExpressionSPDX,
				row.PercentageOfLicenseText,
				row.Copyrights, row.Holders, row.LicenseDetections, row.PackageData,
				row.ScanErrors); err != nil {
				return fmt.Errorf("inserting scancode file results: %w", err)
			}
		}
		return tx.Commit(ctx)
	})
	return scanID, err
}
