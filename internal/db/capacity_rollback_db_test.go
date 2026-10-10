// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// v0.29.89 (operator, 2026-10-10: "can't the column just be handled in the
// migrate?"): a rollback runs the OLDER binary's migrate, which writes
// api_token_settings.default_rate_limit_per_hour and cannot be taught to
// re-add it. So this release keeps the column and mirrors the token hour
// into it (SetCapacityQuota, the one writer): a rollback needs no command.
// The one-time move of an administrator's value into the quota is
// ledgered, so a later migrate never overwrites a Capacity-page edit.
func TestTokenHourDefaultIsMirroredForRollback(t *testing.T) {
	store, ctx := openSR5Store(t)
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		q := before[capacity.QuotaTokenRequestsPerHour]
		_ = store.SetCapacityQuota(context.Background(), capacity.QuotaTokenRequestsPerHour, q.Allowed, q.Mode, 0)
	})
	mirrored := func() int {
		t.Helper()
		var v int
		if err := store.pool.QueryRow(ctx, `SELECT default_rate_limit_per_hour FROM aveloxis_ops.api_token_settings WHERE id = 1`).Scan(&v); err != nil {
			t.Fatalf("the rollback column must exist after migrate: %v", err)
		}
		return v
	}
	if err := store.SetCapacityQuota(ctx, capacity.QuotaTokenRequestsPerHour, 1234, capacity.Enforce, 0); err != nil {
		t.Fatal(err)
	}
	if got := mirrored(); got != 1234 {
		t.Errorf("after a Capacity-page save the rollback column = %d; want 1234", got)
	}
	if err := store.SetAPITokenSettings(ctx, APITokenSettings{DefaultRateLimitPerHour: 4321, DefaultLifetimeDays: 30}, 0); err != nil {
		t.Fatal(err)
	}
	if got := mirrored(); got != 4321 {
		t.Errorf("after an API-tokens-page save the rollback column = %d; want 4321", got)
	}
	// A rerun of migrate keeps the page's value (the move is ledgered).
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	q, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if q[capacity.QuotaTokenRequestsPerHour].Allowed != 4321 {
		t.Errorf("a migrate rerun changed the token hour to %d; want the saved 4321", q[capacity.QuotaTokenRequestsPerHour].Allowed)
	}
	var done bool
	if err := store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.migration_ledger WHERE step_label = $1)`, "v0.29.89 move the API-token hourly default into the token_requests_per_hour quota").Scan(&done); err != nil || !done {
		t.Errorf("the one-time move is not in the ledger: %v, %v", done, err)
	}
}

// L10 r2 F1/F2: the one-time move takes an administrator's saved hourly
// default into the quota — never the old shipped 5,000 (a lifetime-only
// edit stored it with updated_by set; the operator lowered the default to
// 1,000), and never over a quota row someone already saved (a pre-release
// 0.29.89 moved it without a ledger entry).
func TestTokenHourMoveTakesOnlyADeliberateUnmovedValue(t *testing.T) {
	store, ctx := openSR5Store(t)
	const label = "v0.29.89 move the API-token hourly default into the token_requests_per_hour quota"
	var admin int
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, admin) VALUES ('_avmove_admin', 'github', TRUE)
		ON CONFLICT (login_name) DO UPDATE SET admin = TRUE RETURNING user_id`).Scan(&admin); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = 1000, updated_by = NULL WHERE name = 'token_requests_per_hour'`)
		cleanupExecRetry(bg, store, `UPDATE aveloxis_ops.api_token_settings SET default_rate_limit_per_hour = 1000, updated_by = NULL WHERE id = 1`)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, admin)
	})
	replay := func(column int, quotaSaved bool) int {
		t.Helper()
		mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
		by := any(nil)
		if quotaSaved {
			by = admin
		}
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = 1000, updated_by = $1 WHERE name = 'token_requests_per_hour'`, by)
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.api_token_settings SET default_rate_limit_per_hour = $1, updated_by = $2 WHERE id = 1`, column, admin)
		if err := store.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		q, err := store.GetCapacityQuotas(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return q[capacity.QuotaTokenRequestsPerHour].Allowed
	}
	if got := replay(5000, false); got != 1000 {
		t.Errorf("the old default 5,000 moved into the quota (%d); want the new 1,000", got)
	}
	if got := replay(2500, false); got != 2500 {
		t.Errorf("a deliberately saved 2,500 = %d after the move; want 2,500", got)
	}
	if got := replay(2500, true); got != 1000 {
		t.Errorf("the move overwrote a saved quota row (%d); want it left at 1,000", got)
	}
}
