// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// The operator's 2026-10-17 migrate on 0.29.96 (v0.29.97): a schema born
// on 0.29.82–0.29.88 declares api_token_settings.default_rate_limit_per_hour
// NOT NULL with no default, and the seed step stopped supplying the column
// in 0.29.89 — so on such a fleet with no row 1 yet the seed failed with
// SQLSTATE 23502 and the migrate reported an error. The fixture puts the
// package database into that shape (default dropped, row 1 gone, the
// hourly quota edited) and runs the seed: row 1 exists afterwards, its
// mirror equals the stored quota, and the column carries the default
// schema.sql declares.
func TestAPITokenSettingsSeedSurvivesTheOldSchemaWithoutRowOne(t *testing.T) {
	store, ctx := openSR5Store(t)
	// The pre-test row comes back whole (review R2-5: cleanups run LIFO and
	// a re-seed is DO NOTHING on the row the test made).
	var prevHour, prevDays int
	var prevBy *int
	if err := store.pool.QueryRow(ctx, `SELECT default_rate_limit_per_hour, default_lifetime_days, updated_by FROM aveloxis_ops.api_token_settings WHERE id = 1`).Scan(&prevHour, &prevDays, &prevBy); err != nil {
		t.Fatalf("row 1 before the test: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `ALTER TABLE aveloxis_ops.api_token_settings ALTER COLUMN default_rate_limit_per_hour SET DEFAULT 1000`)
		var errs []error
		seedAPITokenSettings(bg, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
		cleanupExecRetry(bg, store, `UPDATE aveloxis_ops.api_token_settings SET default_rate_limit_per_hour = $1, default_lifetime_days = $2, updated_by = $3 WHERE id = 1`, prevHour, prevDays, prevBy)
	})
	mustExecRetry(ctx, t, store, `ALTER TABLE aveloxis_ops.api_token_settings ALTER COLUMN default_rate_limit_per_hour DROP DEFAULT`)
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.api_token_settings WHERE id = 1`)
	// An administrator edited the hourly quota before this migrate: the
	// mirror is born equal to it (review B3), not to the shipped value.
	var prevAllowed int
	if err := store.pool.QueryRow(ctx, `SELECT allowed FROM aveloxis_ops.capacity_quotas WHERE name = 'token_requests_per_hour'`).Scan(&prevAllowed); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = $1 WHERE name = 'token_requests_per_hour'`, prevAllowed)
	})
	const edited = 777
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = $1 WHERE name = 'token_requests_per_hour'`, edited)

	var errs []error
	seedAPITokenSettings(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) != 0 {
		t.Fatalf("the seed failed on the 0.29.82 schema without row 1: %v", errs)
	}
	var hour, days int
	if err := store.pool.QueryRow(ctx, `SELECT default_rate_limit_per_hour, default_lifetime_days FROM aveloxis_ops.api_token_settings WHERE id = 1`).Scan(&hour, &days); err != nil {
		t.Fatalf("row 1 after the seed: %v", err)
	}
	if hour != edited || days != DefaultAPITokenLifetimeDays {
		t.Errorf("row 1 = (hour %d, days %d); want the stored hourly quota %d (the mirror) and the lifetime %d", hour, days, edited, DefaultAPITokenLifetimeDays)
	}

	var def *string
	if err := store.pool.QueryRow(ctx, `SELECT column_default FROM information_schema.columns WHERE table_schema = 'aveloxis_ops' AND table_name = 'api_token_settings' AND column_name = 'default_rate_limit_per_hour'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if def == nil || *def != "1000" {
		t.Errorf("default_rate_limit_per_hour default = %v; want 1000 (schema.sql's declaration, restored by the migrate)", def)
	}
	// Idempotent: a second run changes nothing.
	seedAPITokenSettings(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) != 0 {
		t.Fatalf("second seed: %v", errs)
	}
	// Without a quota row at all (a database the capacity seed has not
	// reached) the mirror falls back to the shipped allowance.
	var mode string
	var qBy *int
	if err := store.pool.QueryRow(ctx, `SELECT mode, updated_by FROM aveloxis_ops.capacity_quotas WHERE name = 'token_requests_per_hour'`).Scan(&mode, &qBy); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.capacity_quotas WHERE name = 'token_requests_per_hour'`)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `INSERT INTO aveloxis_ops.capacity_quotas (name, allowed, mode, updated_by) VALUES ('token_requests_per_hour', $1, $2, $3) ON CONFLICT (name) DO UPDATE SET allowed = EXCLUDED.allowed, mode = EXCLUDED.mode, updated_by = EXCLUDED.updated_by`, prevAllowed, mode, qBy)
	})
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.api_token_settings WHERE id = 1`)
	seedAPITokenSettings(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) != 0 {
		t.Fatalf("seed without a quota row: %v", errs)
	}
	if err := store.pool.QueryRow(ctx, `SELECT default_rate_limit_per_hour FROM aveloxis_ops.api_token_settings WHERE id = 1`).Scan(&hour); err != nil {
		t.Fatal(err)
	}
	if hour != capacity.Shipped[capacity.QuotaTokenRequestsPerHour].Allowed {
		t.Errorf("without a quota row the mirror = %d, want the shipped %d", hour, capacity.Shipped[capacity.QuotaTokenRequestsPerHour].Allowed)
	}
}
