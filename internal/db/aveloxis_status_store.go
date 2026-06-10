// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// aveloxis_status_store.go is the store layer for aveloxis_ops.aveloxis_status:
// one row per subsystem (status_name), upserted by startup preflights so a
// system-level failure is recorded for the operator instead of silently
// degrading. First subsystem: scancode (a corrupt system libmagic makes every
// scan spam GB of warnings and wedge workers — the 2026-06-09 incident).

// Subsystem status values.
const (
	StatusOK           = "ok"
	StatusBroken       = "broken"
	StatusNotInstalled = "not_installed"
)

// AveloxisStatus is one subsystem's recorded health.
type AveloxisStatus struct {
	StatusName         string
	Status             string
	StatusDetail       string
	ToolVersion        string
	DataSource         string
	DataCollectionDate string
}

// SetAveloxisStatus upserts a subsystem's status (one row per status_name).
// tool_source and tool_version are stamped automatically. detail carries the
// human-readable reason + remediation hint.
func (s *PostgresStore) SetAveloxisStatus(ctx context.Context, statusName, status, detail, dataSource string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.aveloxis_status
			(status_name, status, status_detail, tool_source, tool_version, data_source, data_collection_date)
		VALUES ($1, $2, $3, 'aveloxis', $4, $5, NOW())
		ON CONFLICT (status_name) DO UPDATE SET
			status               = EXCLUDED.status,
			status_detail        = EXCLUDED.status_detail,
			tool_version         = EXCLUDED.tool_version,
			data_source          = EXCLUDED.data_source,
			data_collection_date = NOW()`,
		statusName, status, detail, ToolVersion, dataSource)
	if err != nil {
		return fmt.Errorf("set aveloxis_status %q: %w", statusName, err)
	}
	return nil
}

// GetAveloxisStatus returns one subsystem's status, or (nil, nil) if unrecorded.
func (s *PostgresStore) GetAveloxisStatus(ctx context.Context, statusName string) (*AveloxisStatus, error) {
	var a AveloxisStatus
	err := s.pool.QueryRow(ctx, `
		SELECT status_name, status, COALESCE(status_detail,''), COALESCE(tool_version,''),
		       COALESCE(data_source,''), COALESCE(data_collection_date::text,'')
		FROM aveloxis_ops.aveloxis_status WHERE status_name = $1`, statusName).
		Scan(&a.StatusName, &a.Status, &a.StatusDetail, &a.ToolVersion, &a.DataSource, &a.DataCollectionDate)
	if err != nil {
		return nil, nil //nolint:nilerr // unrecorded subsystem is not an error
	}
	return &a, nil
}
