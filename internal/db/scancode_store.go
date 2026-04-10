package db

import (
	"context"
	"encoding/json"
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
}

// GetScancodeForSBOM returns aggregated scancode data for SBOM enrichment:
// the concluded license expression and copyright holders for the repo's source.
func (s *PostgresStore) GetScancodeForSBOM(ctx context.Context, repoID int64) (*ScancodeForSBOM, error) {
	result := &ScancodeForSBOM{}

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
		if rows.Scan(&lic) == nil && lic != "" {
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
		if holderRows.Scan(&holder) == nil && holder != "" {
			result.Copyrights = append(result.Copyrights, holder)
		}
	}

	return result, holderRows.Err()
}

// ScancodeSourceLicense is a license detected in source code files by ScanCode.
type ScancodeSourceLicense struct {
	License  string `json:"license"`
	FileCount int   `json:"file_count"`
	IsOSI    bool   `json:"is_osi"`
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
		if rows.Scan(&lic, &cnt) == nil {
			normalized := normalizeLicense(lic)
			counts[normalized] += cnt
		}
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
		if rows.Scan(&c.Holder, &c.FileCount) == nil && c.Holder != "" {
			result = append(result, c)
		}
	}
	return result, rows.Err()
}
