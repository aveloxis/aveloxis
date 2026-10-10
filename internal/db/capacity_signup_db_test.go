// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// v0.29.89 (operator, 2026-10-10): at most 3 new accounts per network
// address per UTC day (summary/53), decided where the account is created.
// The address is kept only as a keyed hash of the IPv4 address or the IPv6
// /64; allowlisted networks are exempt; signing in to an existing account
// is never a sign-up.
func TestSignupsPerAddressAreCappedAtAccountCreation(t *testing.T) {
	store, ctx := openSR5Store(t)
	tag := fmt.Sprintf("_avsignup%d", time.Now().UnixNano())
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		if q, ok := before[capacity.QuotaSignupsPerAddressPerDay]; ok {
			_ = store.SetCapacityQuota(bg, capacity.QuotaSignupsPerAddressPerDay, q.Allowed, q.Mode, 0)
		}
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.account_signups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, tag+"%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE $1`, tag+"%")
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.signup_allowlist WHERE note = $1`, tag)
	})
	if err := store.SetCapacityQuota(ctx, capacity.QuotaSignupsPerAddressPerDay, 2, capacity.Enforce, 0); err != nil {
		t.Fatal(err)
	}
	n := 0
	signUp := func(addr string) (bool, error) {
		n++
		info := OAuthUserInfo{Login: fmt.Sprintf("%s-%d", tag, n), Provider: "github", GHUserID: time.Now().UnixNano() + int64(n)}
		if addr != "" {
			info.SignupAddr = netip.MustParseAddr(addr)
		}
		_, created, err := store.SignInOAuthUser(ctx, info)
		return created, err
	}
	for i := 0; i < 2; i++ {
		if created, err := signUp("198.51.100.7"); err != nil || !created {
			t.Fatalf("sign-up %d of 2 from one address = %v, %v", i+1, created, err)
		}
	}
	_, err = signUp("198.51.100.7")
	ex, ok := capacity.AsExceeded(err)
	if !ok || ex.Quota != capacity.QuotaSignupsPerAddressPerDay || ex.Allowed != 2 || ex.Window != capacity.UTCDay ||
		!strings.Contains(ex.Message(), "try again tomorrow") {
		t.Fatalf("the third sign-up from one address = %v; want the signups refusal", err)
	}
	var users int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.users WHERE login_name LIKE $1`, tag+"%").Scan(&users); err != nil {
		t.Fatal(err)
	}
	if users != 2 {
		t.Errorf("a refused sign-up left an account: %d accounts, want 2", users)
	}
	// Another address is unaffected; one IPv6 /64 is one address.
	if created, err := signUp("198.51.100.8"); err != nil || !created {
		t.Errorf("another address = %v, %v", created, err)
	}
	for i, a := range []string{"2001:db8:1:2::5", "2001:db8:1:2:ffff::9"} {
		if created, err := signUp(a); err != nil || !created {
			t.Fatalf("IPv6 sign-up %d = %v, %v", i+1, created, err)
		}
	}
	if _, err := signUp("2001:db8:1:2::77"); err == nil {
		t.Error("a third sign-up from the same IPv6 /64 must be refused")
	}
	// The stored key never carries the address.
	var leaked int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.account_signups s JOIN aveloxis_ops.users u USING (user_id)
		WHERE u.login_name LIKE $1 AND (s.address_key LIKE '%198.51%' OR s.address_key LIKE '%2001%' OR length(s.address_key) <> 64)`, tag+"%").Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Errorf("%d sign-up rows carry the raw address or a malformed key", leaked)
	}
	// An allowlisted network is exempt.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.signup_allowlist (cidr, note) VALUES ('198.51.100.0/24', $1)`, tag)
	if created, err := signUp("198.51.100.7"); err != nil || !created {
		t.Errorf("an allowlisted address = %v, %v; want created", created, err)
	}
	// Signing in to an existing account at the cap is not a sign-up.
	existing := OAuthUserInfo{Login: tag + "-1", Provider: "github", SignupAddr: netip.MustParseAddr("2001:db8:1:2::5")}
	var ghID int64
	if err := store.pool.QueryRow(ctx, `SELECT gh_user_id FROM aveloxis_ops.users WHERE login_name = $1`, existing.Login).Scan(&ghID); err != nil {
		t.Fatal(err)
	}
	existing.GHUserID = ghID
	if _, created, err := store.SignInOAuthUser(ctx, existing); err != nil || created {
		t.Errorf("an existing account signing in at the cap = %v, %v; want signed in", created, err)
	}
	// Shadow: served (and logged); unknown address: not counted.
	if err := store.SetCapacityQuota(ctx, capacity.QuotaSignupsPerAddressPerDay, 2, capacity.Shadow, 0); err != nil {
		t.Fatal(err)
	}
	if created, err := signUp("2001:db8:1:2::78"); err != nil || !created {
		t.Errorf("shadow mode = %v, %v; want created", created, err)
	}
	if err := store.SetCapacityQuota(ctx, capacity.QuotaSignupsPerAddressPerDay, 2, capacity.Enforce, 0); err != nil {
		t.Fatal(err)
	}
	if created, err := signUp(""); err != nil || !created {
		t.Errorf("an unknown address = %v, %v; want created (not counted)", created, err)
	}
	// Every creation is recorded for the per-day count.
	days, err := store.SignupsPerDay(ctx, 2)
	if err != nil || len(days) == 0 || days[len(days)-1].Accounts < 8 {
		t.Errorf("SignupsPerDay = %+v, %v; want today with at least this test's 8 accounts", days, err)
	}
}
