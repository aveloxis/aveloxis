// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Worklist item 82: the forge's own message for a blocked or disabled
// repository is stored on the repos row and served with the stats the
// repository page reads. The store owns the limits (SR-18): the text is
// capped, the notice link is kept only when it is https, and every path
// that proves the repository answers again clears both.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func unavailableStore(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
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
	return store, ctx
}

func seedUnavailableRepo(ctx context.Context, t *testing.T, store *PostgresStore, name string, queued bool) int64 {
	t.Helper()
	var id int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, $2, '_avunavail', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
		    repo_gone_at = NULL, repo_gone_checked_at = NULL
		RETURNING repo_id`, "https://github.com/_avunavail/"+name, name).Scan(&id); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	if queued {
		if err := store.EnqueueRepo(ctx, id, 100); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func unavailableCols(ctx context.Context, t *testing.T, store *PostgresStore, id int64) (reason, url *string) {
	t.Helper()
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_unavailable_reason, repo_unavailable_url FROM aveloxis_data.repos WHERE repo_id = $1`, id).
		Scan(&reason, &url); err != nil {
		t.Fatal(err)
	}
	return reason, url
}

func TestRepoUnavailableStoredAndServed(t *testing.T) {
	store, ctx := unavailableStore(t)
	queued := seedUnavailableRepo(ctx, t, store, "queued", true)
	queueless := seedUnavailableRepo(ctx, t, store, "queueless", false)
	const text = "Repository access blocked (dmca)"
	const link = "https://github.com/github/dmca/blob/master/2025/11/x.md"
	for _, id := range []int64{queued, queueless} {
		if err := store.SetRepoUnavailable(ctx, id, text, link); err != nil {
			t.Fatal(err)
		}
	}

	// The single-repo stats (the repository page's read), for both a
	// repository that keeps its queue row (disabled by staff) and one
	// prelim dequeued (a legal block).
	for _, id := range []int64{queued, queueless} {
		st, err := store.GetRepoStats(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if st.UnavailableReason != text || st.UnavailableURL != link {
			t.Errorf("repo %d stats = %q %q, want the stored notice", id, st.UnavailableReason, st.UnavailableURL)
		}
	}
	// The batch endpoint answers the same (the v0.28.7 rule: the two
	// never disagree), on the tracked read and the queueless fallback.
	batch, err := store.GetRepoStatsBatch(ctx, []int64{queued, queueless})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{queued, queueless} {
		if st := batch[id]; st == nil || st.UnavailableReason != text || st.UnavailableURL != link {
			t.Errorf("batch repo %d = %+v, want the stored notice", id, st)
		}
	}

	if err := store.ClearRepoUnavailable(ctx, queued); err != nil {
		t.Fatal(err)
	}
	if r, u := unavailableCols(ctx, t, store, queued); r != nil || u != nil {
		t.Errorf("after ClearRepoUnavailable: %v %v, want NULL NULL", r, u)
	}
}

// TestRepoUnavailableLinkIsHTTPSOnly: the page renders the link, so the
// store keeps it only when it is an absolute https URL.
func TestRepoUnavailableLinkIsHTTPSOnly(t *testing.T) {
	store, ctx := unavailableStore(t)
	id := seedUnavailableRepo(ctx, t, store, "link", false)
	for _, bad := range []string{
		"http://github.com/notice",
		"javascript:alert(1)",
		"//github.com/notice",
		"https://",
		"/relative",
		"",
	} {
		if err := store.SetRepoUnavailable(ctx, id, "m", bad); err != nil {
			t.Fatal(err)
		}
		r, u := unavailableCols(ctx, t, store, id)
		if u != nil {
			t.Errorf("link %q stored as %q, want NULL", bad, *u)
		}
		if r == nil || *r != "m" {
			t.Errorf("link %q: the text must still be stored, got %v", bad, r)
		}
	}
	// Empty text is not a notice.
	if err := store.SetRepoUnavailable(ctx, id, "  ", "https://github.com/n"); err != nil {
		t.Fatal(err)
	}
	if r, _ := unavailableCols(ctx, t, store, id); r != nil {
		t.Errorf("blank text stored as %q, want NULL", *r)
	}
}

// TestRepoUnavailableTextIsCapped: a git server can print any amount of
// remote text; the store keeps at most MaxUnavailableReasonRunes runes and
// never splits a character.
func TestRepoUnavailableTextIsCapped(t *testing.T) {
	store, ctx := unavailableStore(t)
	id := seedUnavailableRepo(ctx, t, store, "cap", false)
	long := strings.Repeat("é", MaxUnavailableReasonRunes+50)
	if err := store.SetRepoUnavailable(ctx, id, long, ""); err != nil {
		t.Fatal(err)
	}
	r, _ := unavailableCols(ctx, t, store, id)
	if r == nil {
		t.Fatal("capped text not stored")
	}
	if n := utf8.RuneCountInString(*r); n != MaxUnavailableReasonRunes || !utf8.ValidString(*r) {
		t.Errorf("stored %d runes (valid %v), want exactly %d", n, utf8.ValidString(*r), MaxUnavailableReasonRunes)
	}
	// Exactly the cap is kept whole.
	exact := strings.Repeat("a", MaxUnavailableReasonRunes)
	if err := store.SetRepoUnavailable(ctx, id, exact, ""); err != nil {
		t.Fatal(err)
	}
	if r, _ := unavailableCols(ctx, t, store, id); r == nil || *r != exact {
		t.Error("text of exactly the cap was altered")
	}
}

// TestGoneClearersClearTheNotice: both paths that prove a gone repository
// answers again (prelim's 2xx, the recheck's resurrection) clear the
// notice with the gone stamp — a lifted block must not keep its banner.
func TestGoneClearersClearTheNotice(t *testing.T) {
	store, ctx := unavailableStore(t)
	a := seedUnavailableRepo(ctx, t, store, "clear-a", false)
	b := seedUnavailableRepo(ctx, t, store, "clear-b", false)
	for _, id := range []int64{a, b} {
		if err := store.MarkRepoGone(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := store.SetRepoUnavailable(ctx, id, "blocked", "https://github.com/n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ClearRepoGone(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := store.ResurrectRepo(ctx, b, 100); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{a, b} {
		if r, u := unavailableCols(ctx, t, store, id); r != nil || u != nil {
			t.Errorf("repo %d: notice survived the gone clear: %v %v", id, r, u)
		}
	}
}

// TestRepoUnavailableTextIsSanitized — review round 1 F3: git passes a
// text/plain refusal's bytes through as they are (a latin-1 message is
// invalid UTF-8) and JSON's \u0000 decodes to NUL; PostgreSQL rejects both,
// so the UPDATE failed every cycle. The store sanitizes before it caps.
func TestRepoUnavailableTextIsSanitized(t *testing.T) {
	store, ctx := unavailableStore(t)
	id := seedUnavailableRepo(ctx, t, store, "sanitize", false)
	for _, raw := range []string{"blocked\x00 here", "d\xe9sactiv\xe9", "ok\x00"} {
		if err := store.SetRepoUnavailable(ctx, id, raw, ""); err != nil {
			t.Fatalf("SetRepoUnavailable(%q): %v — the text must be sanitized before the UPDATE", raw, err)
		}
		r, _ := unavailableCols(ctx, t, store, id)
		if r == nil || strings.ContainsRune(*r, 0) || !utf8.ValidString(*r) {
			t.Errorf("SetRepoUnavailable(%q) stored %v, want valid UTF-8 without NUL", raw, r)
		}
	}
}

// TestRepoUnavailableKeepsTheLinkWhenTheNewNoticeHasNone — review round 1
// F5: in one job the API phase stores GitHub's block object (message and
// notice link) and the refused clone then stores its remote text, which
// has no link. The clone's text replaces the message; the link stays.
func TestRepoUnavailableKeepsTheLinkWhenTheNewNoticeHasNone(t *testing.T) {
	store, ctx := unavailableStore(t)
	id := seedUnavailableRepo(ctx, t, store, "keeplink", false)
	const link = "https://github.com/github/dmca/x.md"
	if err := store.SetRepoUnavailable(ctx, id, "Repository access blocked (dmca)", link); err != nil {
		t.Fatal(err)
	}
	if err := store.SetRepoUnavailable(ctx, id, "Access to this repository has been disabled by GitHub staff.", ""); err != nil {
		t.Fatal(err)
	}
	r, u := unavailableCols(ctx, t, store, id)
	if r == nil || *r != "Access to this repository has been disabled by GitHub staff." || u == nil || *u != link {
		t.Errorf("stored %v %v; want the newer message and the kept link", r, u)
	}
	// A newer link replaces the older one.
	if err := store.SetRepoUnavailable(ctx, id, "m", "https://github.com/n2"); err != nil {
		t.Fatal(err)
	}
	if _, u := unavailableCols(ctx, t, store, id); u == nil || *u != "https://github.com/n2" {
		t.Errorf("link = %v, want the newer one", u)
	}
}
