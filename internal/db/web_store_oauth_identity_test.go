// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestUpsertOAuthUserMatchesByForgeIdentity (final whole-tree review F1,
// 2026-09-28): the login matched an existing account by login_name alone,
// so a GitLab user who registered a GitHub admin's username — or a GitHub
// user who registered a renamed-away login — was handed that account and,
// through IsUserAdmin, an admin session. The forge's numeric user ID is the
// identity (SR-6); a login name is claimed only by a row that carries no
// contradicting identity, and a collision no ID settles is refused.
func TestUpsertOAuthUserMatchesByForgeIdentity(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	const prefix = "_avid_"
	clean := func() {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '\_avid\_%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)

	ids := func(login string) (gh, gl int64) {
		t.Helper()
		if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(gh_user_id, 0), COALESCE(gl_user_id, 0)
			FROM aveloxis_ops.users WHERE login_name = $1`, login).Scan(&gh, &gl); err != nil {
			t.Fatalf("read %s: %v", login, err)
		}
		return gh, gl
	}

	// A GitHub admin.
	alice := prefix + "alice"
	aliceID, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: alice, GHUserID: 900101, GHLogin: alice, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, aliceID); err != nil {
		t.Fatal(err)
	}

	t.Run("a GitLab user with the admin's username is refused", func(t *testing.T) {
		got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: alice, GLUserID: 900202, GLUsername: alice, Provider: "gitlab"})
		if got == aliceID {
			t.Fatalf("a GitLab login named %s was handed the GitHub admin's account %d", alice, aliceID)
		}
		if !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("err = %v; want ErrLoginNameTaken", err)
		}
		if _, gl := ids(alice); gl != 0 {
			t.Errorf("the admin's row was linked to GitLab user %d", gl)
		}
	})

	t.Run("a different GitHub account with a reused login is refused", func(t *testing.T) {
		got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: alice, GHUserID: 900999, GHLogin: alice, Provider: "github"})
		if got == aliceID || !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("GitHub user 900999 as %s: got user %d, err %v; want refusal with ErrLoginNameTaken", alice, got, err)
		}
		if gh, _ := ids(alice); gh != 900101 {
			t.Errorf("the admin's gh_user_id is %d; want 900101 unchanged", gh)
		}
	})

	t.Run("the same GitHub account under a new login keeps its row", func(t *testing.T) {
		renamed := prefix + "alice_renamed"
		got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: renamed, GHUserID: 900101, GHLogin: renamed, Provider: "github"})
		if err != nil || got != aliceID {
			t.Fatalf("GitHub user 900101 renamed to %s: got user %d, err %v; want %d", renamed, got, err, aliceID)
		}
		var ghLogin string
		if err := store.Pool().QueryRow(ctx, `SELECT gh_login FROM aveloxis_ops.users WHERE user_id = $1`, aliceID).Scan(&ghLogin); err != nil {
			t.Fatal(err)
		}
		if ghLogin != renamed {
			t.Errorf("gh_login = %q; want the current handle %q", ghLogin, renamed)
		}
	})

	t.Run("a legacy row without an ID is claimed once and stamped", func(t *testing.T) {
		bob := prefix + "bob"
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gh_user_id, gl_user_id)
			VALUES ($1, 'github', NULL, 0)`, bob); err != nil {
			t.Fatal(err)
		}
		first, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: bob, GHUserID: 900055, GHLogin: bob, Provider: "github"})
		if err != nil {
			t.Fatalf("the legacy row's first GitHub login: %v", err)
		}
		if gh, _ := ids(bob); gh != 900055 {
			t.Errorf("gh_user_id = %d after the claim; want 900055 stamped", gh)
		}
		if again, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: bob, GHUserID: 900055, Provider: "github"}); err != nil || again != first {
			t.Errorf("the same account again: user %d, err %v; want %d", again, err, first)
		}
		if other, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: bob, GHUserID: 900056, Provider: "github"}); other == first || !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("another GitHub account as %s after the claim: user %d, err %v; want ErrLoginNameTaken", bob, other, err)
		}
	})

	t.Run("a pre-provider row is a GitHub row", func(t *testing.T) {
		old := prefix + "old"
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name) VALUES ($1)`, old); err != nil {
			t.Fatal(err)
		}
		if got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: old, GLUserID: 900303, Provider: "gitlab"}); !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("a GitLab login claimed a pre-provider row: user %d, err %v", got, err)
		}
		if _, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: old, GHUserID: 900304, Provider: "github"}); err != nil {
			t.Errorf("a GitHub login of a pre-provider row: %v", err)
		}
	})

	t.Run("a GitLab row is found by its GitLab ID", func(t *testing.T) {
		dave := prefix + "dave"
		first, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: dave, GLUserID: 900707, GLUsername: dave, Provider: "gitlab"})
		if err != nil {
			t.Fatal(err)
		}
		if gh, gl := ids(dave); gh != 0 || gl != 900707 {
			t.Errorf("gh_user_id=%d gl_user_id=%d; want 0 and 900707", gh, gl)
		}
		if again, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: dave, GLUserID: 900707, Provider: "gitlab"}); err != nil || again != first {
			t.Errorf("the same GitLab account again: user %d, err %v; want %d", again, err, first)
		}
		if got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: dave, Provider: "github"}); got == first || !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("a GitHub login without an ID claimed the GitLab row: user %d, err %v", got, err)
		}
	})

	t.Run("an ID-less login repeats onto its own ID-less row", func(t *testing.T) {
		carol := prefix + "carol"
		a, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: carol, Provider: "github"})
		if err != nil {
			t.Fatal(err)
		}
		if b, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: carol, Provider: "github"}); err != nil || b != a {
			t.Errorf("second ID-less login: user %d, err %v; want %d", b, err, a)
		}
	})

	t.Run("the audit counts a cross-linked row", func(t *testing.T) {
		var before, after int
		if err := store.Pool().QueryRow(ctx, CrossProviderUserAuditSQL()).Scan(&before); err != nil {
			t.Fatalf("the audit SQL does not run: %v", err)
		}
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gh_user_id, gl_user_id)
			VALUES ($1, 'gitlab', 900801, 900802)`, prefix+"linked"); err != nil {
			t.Fatal(err)
		}
		if err := store.Pool().QueryRow(ctx, CrossProviderUserAuditSQL()).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before+1 {
			t.Errorf("audit went %d -> %d; want one more for the cross-linked row", before, after)
		}
	})

	t.Run("an unknown provider is refused", func(t *testing.T) {
		if _, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "x", Provider: "bitbucket"}); err == nil {
			t.Error("a login from an unknown provider was accepted")
		}
	})
}
