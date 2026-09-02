// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// deploy_ack records, per binary version, that an operator confirmed
// the release's manual deploy/heal steps were run. The `aveloxis start
// serve` / `start all` gate consults it so a release's healing steps
// can't be silently skipped (v0.29.0 operator ask — there is enough
// data-side healing here that a missed step would go unnoticed).

// ensureDeployAckTable creates the table if it does not exist. Called
// by RecordDeployAck because `start` can run BEFORE the current
// binary's migrate (the previous version's schema has no such table).
func (s *PostgresStore) ensureDeployAckTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS aveloxis_ops.deploy_ack (
			tool_version    TEXT PRIMARY KEY,
			acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			note            TEXT NOT NULL DEFAULT ''
		)`)
	if err != nil {
		return fmt.Errorf("ensure deploy_ack table: %w", err)
	}
	return nil
}

// DeployAckExists reports whether the given binary version's deploy
// steps have been acknowledged. A MISSING table (the first start after
// a version bump, before this version's migrate created it) is treated
// as "not acknowledged" — never an error — so the gate fires exactly
// when it should.
func (s *PostgresStore) DeployAckExists(ctx context.Context, version string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM aveloxis_ops.deploy_ack WHERE tool_version = $1)`,
		version).Scan(&exists)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
			return false, nil
		}
		return false, fmt.Errorf("deploy ack lookup %q: %w", version, err)
	}
	return exists, nil
}

// RecordDeployAck stamps the acknowledgement for a version (idempotent).
func (s *PostgresStore) RecordDeployAck(ctx context.Context, version, note string) error {
	if err := s.ensureDeployAckTable(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.deploy_ack (tool_version, note)
		VALUES ($1, $2)
		ON CONFLICT (tool_version) DO UPDATE SET
			acknowledged_at = NOW(), note = EXCLUDED.note`, version, note)
	if err != nil {
		return fmt.Errorf("record deploy ack %q: %w", version, err)
	}
	return nil
}

// FleetHasCollectedData reports whether this database is an EXISTING
// fleet (has ever collected) vs a fresh install. The deploy gate only
// applies to existing fleets — a fresh DB has nothing to heal. A
// missing queue table (pathological) is treated as fresh.
func (s *PostgresStore) FleetHasCollectedData(ctx context.Context) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM aveloxis_ops.collection_queue LIMIT 1)`).Scan(&exists)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return false, nil
		}
		return false, fmt.Errorf("fleet-has-data probe: %w", err)
	}
	return exists, nil
}
