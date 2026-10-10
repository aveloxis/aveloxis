// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// v0.29.89 (operator 2026-10-10): a new account's address is kept only
// sealed to the operator's offline key; the server cannot open it, the
// private key can, and another key cannot.
func TestSignupAddressIsSealedToTheEscrowKey(t *testing.T) {
	store, ctx := openSR5Store(t)
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetSignupEscrowRecipient(id.Recipient().String()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.SetSignupEscrowRecipient("") })
	if err := store.SetSignupEscrowRecipient("not-a-key"); err == nil {
		t.Error("an invalid recipient must be refused")
	}
	_ = store.SetSignupEscrowRecipient(id.Recipient().String())
	login := fmt.Sprintf("_avescrow%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.account_signups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	})
	uid, created, err := store.SignInOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github", GHUserID: time.Now().UnixNano(), SignupAddr: netip.MustParseAddr("2001:db8:7:8::42")})
	if err != nil || !created {
		t.Fatalf("sign-up = %v, %v", created, err)
	}
	recs, err := store.SealedSignupsForUser(ctx, uid)
	if err != nil || len(recs) != 1 {
		t.Fatalf("sealed records = %d, %v; want 1", len(recs), err)
	}
	addr, err := OpenSignupAddress(recs[0].Sealed, id)
	if err != nil || addr.String() != "2001:db8:7:8::42" {
		t.Fatalf("opened with the private key = %v, %v; want the full address", addr, err)
	}
	other, _ := age.GenerateX25519Identity()
	if _, err := OpenSignupAddress(recs[0].Sealed, other); err == nil {
		t.Error("another key opened the envelope")
	}
}

// The quota's secret lasts its UTC day: the prune deletes past days'
// secrets and clears past days' keys, and deletes records past the
// retention; today's are untouched. With the quota off, no key is kept and
// no secret is made (ASVS review I6: off is decided before any secret).
func TestSignupPrivacyPrune(t *testing.T) {
	store, ctx := openSR5Store(t)
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prev := before[capacity.QuotaSignupsPerAddressPerDay]
	tag := fmt.Sprintf("_avprune%d", time.Now().UnixNano())
	t.Cleanup(func() {
		bg := context.Background()
		_ = store.SetCapacityQuota(bg, capacity.QuotaSignupsPerAddressPerDay, prev.Allowed, prev.Mode, 0)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.account_signups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, tag+"%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE $1`, tag+"%")
	})
	var uid int
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ($1, 'github') RETURNING user_id`, tag+"-old").Scan(&uid); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	yesterday := now.Truncate(24 * time.Hour).Add(-time.Hour)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.signup_key_secrets (day, secret) VALUES ($1::date, 'x') ON CONFLICT (day) DO NOTHING`, yesterday.Format(time.DateOnly))
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.account_signups (address_key, address_sealed, user_id, created_at) VALUES ('k-yesterday', 'sealed', $1, $2)`, uid, yesterday)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.account_signups (address_key, address_sealed, user_id, created_at) VALUES ('k-ancient', 'sealed', $1, $2)`, uid, now.Add(-SignupRecordRetention-time.Hour))
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.account_signups (address_key, user_id, created_at) VALUES ('k-today', $1, $2)`, uid, now)
	if _, err := store.PruneSignupPrivacy(ctx, now); err != nil {
		t.Fatal(err)
	}
	var secrets, yKey, ancient, todayKey int
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.signup_key_secrets WHERE day < $1::date`, now.Format(time.DateOnly)).Scan(&secrets)
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups WHERE user_id = $1 AND address_key = 'k-yesterday'`, uid).Scan(&yKey)
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups WHERE user_id = $1 AND address_sealed IS NOT NULL AND created_at < $2`, uid, now.Add(-SignupRecordRetention)).Scan(&ancient)
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups WHERE user_id = $1 AND address_key = 'k-today'`, uid).Scan(&todayKey)
	if secrets != 0 || yKey != 0 || ancient != 0 || todayKey != 1 {
		t.Errorf("after the prune: past secrets %d, yesterday's key %d, records past retention %d, today's key %d; want 0, 0, 0, 1", secrets, yKey, ancient, todayKey)
	}
	// Off: no key, no secret.
	if err := store.SetCapacityQuota(ctx, capacity.QuotaSignupsPerAddressPerDay, 3, capacity.Off, 0); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_ops.signup_key_secrets WHERE day = $1::date`, now.Format(time.DateOnly))
	nid, created, err := store.SignInOAuthUser(ctx, OAuthUserInfo{Login: tag + "-off", Provider: "github", GHUserID: time.Now().UnixNano(), SignupAddr: netip.MustParseAddr("198.51.100.99")})
	if err != nil || !created {
		t.Fatalf("sign-up while off = %v, %v", created, err)
	}
	var keys, todaySecrets int
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(address_key) FROM aveloxis_ops.account_signups WHERE user_id = $1`, nid).Scan(&keys)
	_ = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.signup_key_secrets WHERE day = $1::date`, now.Format(time.DateOnly)).Scan(&todaySecrets)
	if keys != 0 || todaySecrets != 0 {
		t.Errorf("with the quota off: keys %d, today's secret %d; want 0, 0", keys, todaySecrets)
	}
}
