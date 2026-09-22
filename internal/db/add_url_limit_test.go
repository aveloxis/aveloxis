// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestAddURLByteLimit (AVELOXIS_TEST_DB): MaxAddURLBytes is the derivation its
// comment gives, computed from this database's block size; a URL of exactly
// that many bytes goes through every add path and every index it is written
// into — the request item, the repos row with its lower(repo_git) indexes, the
// org registration — even when it cannot be compressed or lower() lengthens it;
// one byte more is refused with ErrURLTooLong before anything is written, also
// when it is fewer characters than the limit (round-27 review: the database
// refused over-long URLs with SQLSTATE 54000, which also means a server-wide
// stop, and one of its two index-size errors names no index; round 28: a wrong
// constant, or a limit counted in characters, passed).
func TestAddURLByteLimit(t *testing.T) {
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
	testMigrate(ctx, t, store)

	const userLogin, adminLogin = "_avurl_limit_user_probe", "_avurl_limit_admin_probe"
	const host = "https://git.example.org/_avurl-limit/"
	const hubOwner = "https://github.com/_avurl-limit-owner/"
	const orgHost = "https://github.com/_avurl-limit-org-"
	clean := func() {
		for _, login := range []string{userLogin, adminLogin} {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		}
		for _, prefix := range []string{host, hubOwner} {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, prefix)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, prefix)
		}
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: userLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	adminID, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: adminLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAdmin(ctx, adminID, true); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "url limit probe")
	if err != nil {
		t.Fatal(err)
	}
	adminGID, err := store.CreateUserGroup(ctx, adminID, "url limit probe, admin")
	if err != nil {
		t.Fatal(err)
	}

	// sized returns prefix padded to exactly n bytes with pseudo-random
	// characters that compress poorly. When expanding, only the first
	// randomHead bytes are random and the rest is "Ⱥ" (U+023A, 2 bytes), which
	// lower() turns into the 3-byte U+2C65: the random head keeps Postgres from
	// compressing the lowered value, which otherwise shrinks a run of "Ⱥ" enough
	// to fit (author probe: without the halving, a 2684-byte URL built this way
	// made a 3488-byte lower() index row, refused with 54000).
	const randomHead = 1100
	seed := uint64(88172645463325252)
	sized := func(prefix string, expanding bool, n int) string {
		t.Helper()
		const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_"
		var b strings.Builder
		b.WriteString(prefix)
		for b.Len() < n {
			if expanding && b.Len() >= randomHead && b.Len()+len("Ⱥ") <= n {
				b.WriteString("Ⱥ")
				continue
			}
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			b.WriteByte(alphabet[seed%uint64(len(alphabet))])
		}
		if b.Len() != n {
			t.Fatalf("built a %d-byte URL; want %d", b.Len(), n)
		}
		return b.String()
	}
	// The limit is Postgres's btree row limit for this block size (BTMaxItemSize
	// in nbtree.h: a third of the page after the page header with three line
	// pointers and the btree trailer, aligned down, less an item pointer), less
	// 20 bytes of row overhead, halved for lower().
	var blockSize int
	if err := store.pool.QueryRow(ctx, `SELECT current_setting('block_size')::int`).Scan(&blockSize); err != nil {
		t.Fatalf("read block_size: %v", err)
	}
	maxalign := func(n int) int { return (n + 7) &^ 7 }
	btreeMaxItem := (blockSize-maxalign(24+3*4)-maxalign(16))/3&^7 - 8
	if want := (btreeMaxItem - 20) / 2; MaxAddURLBytes != want || btreeMaxItem != 2704 {
		t.Fatalf("MaxAddURLBytes = %d, btree row limit %d; the derivation from block_size %d gives %d (and 2704, the limit its comment names)", MaxAddURLBytes, btreeMaxItem, blockSize, want)
	}

	requests := func(userID int) int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, userID).Scan(&n); err != nil {
			t.Fatalf("count requests: %v", err)
		}
		return n
	}

	// At the limit, every path writes every index.
	pendingURLs := []string{sized(host+"pending-a/", false, MaxAddURLBytes), sized(host+"pending-b/", true, MaxAddURLBytes)}
	out, err := store.AddReposToGroup(ctx, uid, gid, pendingURLs, 0)
	if err != nil || out.Pending != 2 {
		t.Fatalf("non-admin add of two URLs at the limit = %+v, %v; want 2 pending", out, err)
	}
	if _, _, err := store.DecideAddRequest(ctx, out.RequestID, adminID, true, ""); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if n, err := store.ProcessApprovedAddRequest(ctx, out.RequestID); err != nil || n != 2 {
		t.Errorf("processing the approved URLs at the limit = %d, %v; want 2, nil", n, err)
	}
	// Surrounding whitespace is trimmed before the limit applies.
	if out, err := store.AddReposToGroup(ctx, uid, gid, []string{" " + sized(hubOwner+"auto-", true, MaxAddURLBytes) + "\n"}, 5); err != nil || out.Enqueued != 1 {
		t.Errorf("auto-approved add of a GitHub URL at the limit, with whitespace around it = %+v, %v; want 1 enqueued", out, err)
	}
	if out, err := store.AddReposToGroup(ctx, adminID, adminGID, []string{sized(host+"admin-", true, MaxAddURLBytes)}, 0); err != nil || out.Enqueued != 1 {
		t.Errorf("admin add of a URL at the limit = %+v, %v; want 1 enqueued", out, err)
	}
	if out, err := store.AddOrgToGroup(ctx, adminID, adminGID, sized(orgHost+"admin-", true, MaxAddURLBytes), ""); err != nil || !out.Registered {
		t.Errorf("admin org add at the limit = %+v, %v; want registered", out, err)
	}
	orgOut, err := store.AddOrgToGroup(ctx, uid, gid, sized(orgHost+"user-", false, MaxAddURLBytes), "")
	if err != nil || orgOut.RequestID == 0 {
		t.Fatalf("non-admin org add at the limit = %+v, %v; want a pending request", orgOut, err)
	}
	if _, _, err := store.DecideAddRequest(ctx, orgOut.RequestID, adminID, true, ""); err != nil {
		t.Errorf("approving an org at the limit: %v", err)
	}

	// One byte over is refused before anything is written, even next to a
	// URL that fits.
	over := MaxAddURLBytes + 1
	before, adminBefore := requests(uid), requests(adminID)
	for _, tc := range []struct {
		name string
		add  func() error
	}{
		{"non-admin repos", func() error {
			_, err := store.AddReposToGroup(ctx, uid, gid, []string{host + "short-ok", sized(host+"over-", false, over)}, 0)
			return err
		}},
		{"repos, fewer characters than the limit", func() error {
			u := sized(host+"over-multibyte-", true, over)
			if n := len([]rune(u)); n >= MaxAddURLBytes {
				t.Fatalf("the multibyte URL has %d characters; want fewer than %d", n, MaxAddURLBytes)
			}
			_, err := store.AddReposToGroup(ctx, uid, gid, []string{u}, 0)
			return err
		}},
		{"org, fewer characters than the limit", func() error {
			_, err := store.AddOrgToGroup(ctx, uid, gid, sized(orgHost+"over-multibyte-", true, over), "")
			return err
		}},
		{"auto-approved repos", func() error {
			_, err := store.AddReposToGroup(ctx, uid, gid, []string{sized(host+"over-auto-", false, over)}, 5)
			return err
		}},
		{"admin repos", func() error {
			_, err := store.AddReposToGroup(ctx, adminID, adminGID, []string{host + "admin-short-ok", sized(host+"over-admin-", false, over)}, 0)
			return err
		}},
		{"admin org", func() error {
			_, err := store.AddOrgToGroup(ctx, adminID, adminGID, sized(orgHost+"over-admin-", false, over), "")
			return err
		}},
		{"non-admin org", func() error {
			_, err := store.AddOrgToGroup(ctx, uid, gid, sized(orgHost+"over-user-", false, over), "")
			return err
		}},
	} {
		if err := tc.add(); !errors.Is(err, ErrURLTooLong) {
			t.Errorf("%s one byte over the limit: %v; want ErrURLTooLong", tc.name, err)
		}
	}
	if after := requests(uid); after != before {
		t.Errorf("refused non-admin adds created %d requests", after-before)
	}
	if after := requests(adminID); after != adminBefore {
		t.Errorf("refused admin adds created %d requests", after-adminBefore)
	}
	var written int
	if err := store.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM aveloxis_data.repos WHERE repo_git IN ($1, $2))
		     + (SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE org_url LIKE $3 || '%')`,
		host+"short-ok", host+"admin-short-ok", orgHost+"over-").Scan(&written); err != nil {
		t.Fatalf("count writes: %v", err)
	}
	if written != 0 {
		t.Errorf("refused adds wrote %d repos or org registrations; want 0", written)
	}
}
