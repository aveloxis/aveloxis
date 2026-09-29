// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
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
	var logs syncBuffer
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(&logs, nil)))
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
		// Round 2 F2: the account's name follows the rename, so the old
		// name is free for whoever registers it next and the first-signup
		// probe does not read the renamed user as new at every login.
		var name string
		if err := store.Pool().QueryRow(ctx, `SELECT login_name FROM aveloxis_ops.users WHERE user_id = $1`, aliceID).Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != renamed {
			t.Errorf("login_name = %q after the rename; want %q", name, renamed)
		}
	})

	t.Run("a rename onto a name another account holds keeps the old name", func(t *testing.T) {
		holder, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "held", GHUserID: 900111, Provider: "github"})
		if err != nil {
			t.Fatal(err)
		}
		mover, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "mover", GHUserID: 900112, Provider: "github"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "held", GHUserID: 900112, Provider: "github"})
		if err != nil || got != mover {
			t.Fatalf("GitHub user 900112 renamed onto a held name: user %d, err %v; want %d", got, err, mover)
		}
		var a, b string
		if err := store.Pool().QueryRow(ctx, `SELECT (SELECT login_name FROM aveloxis_ops.users WHERE user_id = $1), (SELECT login_name FROM aveloxis_ops.users WHERE user_id = $2)`, holder, mover).Scan(&a, &b); err != nil {
			t.Fatal(err)
		}
		if a != prefix+"held" || b != prefix+"mover" {
			t.Errorf("names after the blocked rename: holder %q, mover %q; want both unchanged", a, b)
		}
	})

	t.Run("of duplicate rows for one ID the most recently used wins, with a WARN", func(t *testing.T) {
		// <= 0.29.68 inserted a second row when a user renamed.
		var before, after int
		if err := store.Pool().QueryRow(ctx, DuplicateForgeIDUserAuditSQL()).Scan(&before); err != nil {
			t.Fatalf("the duplicate audit SQL does not run: %v", err)
		}
		defer func() {
			if err := store.Pool().QueryRow(ctx, DuplicateForgeIDUserAuditSQL()).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if after != before+1 {
				t.Errorf("duplicate audit went %d -> %d; want GitHub user 900121 counted once", before, after)
			}
		}()
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gh_user_id, data_collection_date)
			VALUES ($1, 'github', 900121, NOW() - interval '30 days'), ($2, 'github', 900121, NOW() - interval '1 day')`, prefix+"dup_old", prefix+"dup_new"); err != nil {
			t.Fatal(err)
		}
		var newer int
		if err := store.Pool().QueryRow(ctx, `SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1`, prefix+"dup_new").Scan(&newer); err != nil {
			t.Fatal(err)
		}
		got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "dup_now", GHUserID: 900121, Provider: "github"})
		if err != nil || got != newer {
			t.Errorf("duplicate rows for GitHub user 900121: user %d, err %v; want the most recently used %d", got, err, newer)
		}
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "900121") {
			t.Errorf("duplicate rows for one forge ID were not logged:\n%s", logs.String())
		}
	})

	t.Run("a GitLab ID is an identity only on its own instance", func(t *testing.T) {
		const hostA, hostB = "https://gitlab.a.example", "https://gitlab.b.example"
		a, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "gl_alice", GLUserID: 900131, GLHost: hostA, Provider: "gitlab"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "gl_mallory", GLUserID: 900131, GLHost: hostB, Provider: "gitlab"})
		if err != nil {
			t.Fatal(err)
		}
		if b == a {
			t.Fatalf("GitLab user 900131 on %s was handed %s's account %d", hostB, hostA, a)
		}
		if again, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "gl_alice", GLUserID: 900131, GLHost: hostA + "/", Provider: "gitlab"}); err != nil || again != a {
			t.Errorf("the same instance (trailing slash): user %d, err %v; want %d", again, err, a)
		}
		// A row from before the host was recorded matches NO instance
		// (round 3, decided as a class: a NULL host matching any instance
		// handed an unstamped account to the next instance's user with the
		// same ID). Web start stamps such rows from the configured instance
		// (StampLegacyGitLabHost); until then the login is refused.
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gl_user_id) VALUES ($1, 'gitlab', 900132)`, prefix+"gl_legacy"); err != nil {
			t.Fatal(err)
		}
		if got, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "gl_legacy", GLUserID: 900132, GLHost: hostB, Provider: "gitlab"}); !errors.Is(err, ErrLoginNameTaken) {
			t.Errorf("an unstamped GitLab row was matched from %s: user %d, err %v; want ErrLoginNameTaken", hostB, got, err)
		}
		n, err := store.StampLegacyGitLabHost(ctx, hostA+"/")
		if err != nil || n < 1 {
			t.Fatalf("StampLegacyGitLabHost: %d rows, %v; want the legacy row stamped", n, err)
		}
		var host string
		if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(gl_oauth_host, '') FROM aveloxis_ops.users WHERE login_name = $1`, prefix+"gl_legacy").Scan(&host); err != nil {
			t.Fatal(err)
		}
		if host != hostA {
			t.Errorf("gl_oauth_host = %q after the stamp; want %q", host, hostA)
		}
		if _, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: prefix + "gl_legacy", GLUserID: 900132, GLHost: hostA, Provider: "gitlab"}); err != nil {
			t.Errorf("the stamped row's own instance: %v", err)
		}
		if again, err := store.StampLegacyGitLabHost(ctx, hostB); err != nil || again != 0 {
			t.Errorf("a second stamp touched %d rows (%v); a recorded host is never replaced", again, err)
		}
		// An empty host is gitlab.com, the callback's default.
		if got, want := GitLabOAuthHost(""), "https://gitlab.com"; got != want {
			t.Errorf("GitLabOAuthHost(\"\") = %q; want %q", got, want)
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

	t.Run("the sign-in says whether it created the account", func(t *testing.T) {
		who := prefix + "created"
		_, created, err := store.SignInOAuthUser(ctx, OAuthUserInfo{Login: who, GHUserID: 900141, Provider: "github"})
		if err != nil || !created {
			t.Fatalf("first sign-in: created=%v err=%v; want created", created, err)
		}
		if _, created, err := store.SignInOAuthUser(ctx, OAuthUserInfo{Login: who, GHUserID: 900141, Provider: "github"}); err != nil || created {
			t.Errorf("second sign-in: created=%v err=%v; want not created", created, err)
		}
		if _, created, err := store.SignInOAuthUser(ctx, OAuthUserInfo{Login: who + "_renamed", GHUserID: 900141, Provider: "github"}); err != nil || created {
			t.Errorf("sign-in after a rename: created=%v err=%v; want not created", created, err)
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

	t.Run("the audit counts a takeover in the shape the old code wrote", func(t *testing.T) {
		// Through 0.29.68 the INSERT stored the other provider's ID as 0 and
		// the takeover UPDATE kept it (COALESCE(gl_user_id, $6) over a 0):
		// a GitHub row taken over by a GitLab login carries gh 111, gl 0,
		// a GitLab user name and provider gitlab (round 2 F1).
		var before, after int
		if err := store.Pool().QueryRow(ctx, CrossProviderUserAuditSQL()).Scan(&before); err != nil {
			t.Fatalf("the audit SQL does not run: %v", err)
		}
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gh_user_id, gh_login, gl_user_id, gl_username)
			VALUES ($1, 'github', 900811, $1, 0, '')`, prefix+"taken"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.users SET gl_user_id = COALESCE(gl_user_id, 900812),
			gl_username = $1, oauth_provider = 'gitlab' WHERE login_name = $1`, prefix+"taken"); err != nil {
			t.Fatal(err)
		}
		if err := store.Pool().QueryRow(ctx, CrossProviderUserAuditSQL()).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before+1 {
			t.Errorf("audit went %d -> %d; want the old-shape takeover counted", before, after)
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

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}
