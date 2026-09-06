// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// sender_backfill_keyset_test.go — Part A of the email-attribution
// program: BackfillMailingListSenderIDs moves from a LIMIT-rescan loop
// (measured 3.9s/5,000 rows at 8.5:1 scan waste → ~31 days to converge)
// to keyset windows (measured 15,445 rows/s email arm + 87,848 rows/1.0s
// alias arm → full pass in minutes), and gains the contributors_aliases
// join arm the old query lacked (163,236 attributable messages on the
// production aveloxis DB were reachable ONLY through an alias).
package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSenderBackfillKeysetShape pins the rewritten query shape:
// keyset window predicate, the alias arm, withRetry (the drain loop's
// UpsertMailingListMessageBody DO-UPDATEs the same rows — 40P01 between
// these writers is observed history, v0.27.112/114), and the ABSENCE of
// the LIMIT-rescan form ([[feedback_keyset_windows_for_bulk_backfills]]).
func TestSenderBackfillKeysetShape(t *testing.T) {
	src := srctest.Read(t, "internal/db/email_message_store.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src,
		"func (s *PostgresStore) BackfillMailingListSenderIDs("))

	for _, needle := range []string{
		"msg_id > $",           // keyset lower bound
		"contributors_aliases", // the alias arm (the 2.8% hole)
		"withRetry",            // 40P01 protection
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("BackfillMailingListSenderIDs must contain %q — the Part A keyset+alias contract", needle)
		}
	}
	if strings.Contains(body, "LIMIT") {
		t.Error("BackfillMailingListSenderIDs must NOT use LIMIT — the LIMIT-rescan loop re-pays the full join per batch (measured 8.5:1 waste, ~31-day convergence); keyset windows are the house rule")
	}
}

// TestSenderBackfillBoundsHelpersExist pins the floor/ceiling pair the
// pass loop terminates on. Termination is cursor >= ceiling, NEVER
// rows-affected (sparse windows legally resolve 0 — the runKeysetWindows
// rule). The floor exists because MIN(msg_id) over platform-6 rows costs
// ~17.5s (forward pkey scan through ~40M non-email rows, measured) and
// so is cached by callers per process; the ceiling is a ~26ms backward
// scan and refreshes per pass.
func TestSenderBackfillBoundsHelpersExist(t *testing.T) {
	src := srctest.Read(t, "internal/db/email_message_store.go")
	for _, sig := range []string{
		"func (s *PostgresStore) MailingListMsgIDFloor(",
		"func (s *PostgresStore) MailingListMsgIDCeiling(",
	} {
		if !strings.Contains(src, sig) {
			t.Errorf("missing %q — the pass-bounds helper pair", sig)
		}
	}
}

// --- behavioral tier (AVELOXIS_TEST_DB) --------------------------------

const (
	sbRepoID    = int64(944_147_010)
	sbMsgBase   = int64(944_147_100) // platform_msg_id range, platform 6 kind 4
	sbLoginA    = "_avsb-email-match"
	sbLoginB    = "_avsb-alias-target"
	sbLoginC    = "_avsb-alias-only"
	sbCntrbA    = "944e0000-0000-4000-8000-0000000000aa"
	sbCntrbB    = "944e0000-0000-4000-8000-0000000000bb"
	sbCntrbC    = "944e0000-0000-4000-8000-0000000000cc"
	sbEmailBoth = "_avsb-both@example.org"  // matches A by email AND B by alias
	sbEmailAli  = "_avsb-alias@example.org" // matches C ONLY via contributors_aliases
)

func sbConnect(t *testing.T) (*PostgresStore, context.Context) {
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
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)
	return store, ctx
}

// sbSeed seeds a repo, three contributors (A email-matchable, B
// alias-target of the same address A matches, C alias-only), one alias
// row per target, and unattributed platform-6 message bodies joined to
// email_message rows by node_id = message_id_header. Returns the msg_ids
// in seed order.
func sbSeed(t *testing.T, ctx context.Context, store *PostgresStore, senders []string) []int64 {
	t.Helper()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avsb/sender-backfill', '_avsb', 'sender-backfill', 1)
		ON CONFLICT (repo_id) DO NOTHING`, sbRepoID)
	for _, c := range []struct{ id, login, email string }{
		{sbCntrbA, sbLoginA, sbEmailBoth},
		{sbCntrbB, sbLoginB, ""},
		{sbCntrbC, sbLoginC, ""},
	} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (cntrb_id) DO UPDATE SET cntrb_email = EXCLUDED.cntrb_email`, c.id, c.login, c.email)
	}
	// B is an alias target for the address A ALSO matches by email —
	// the priority discriminator. C is reachable only via its alias.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, $2, $2) ON CONFLICT (alias_email) DO UPDATE SET cntrb_id = EXCLUDED.cntrb_id`, sbCntrbB, sbEmailBoth)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, $2, $2) ON CONFLICT (alias_email) DO UPDATE SET cntrb_id = EXCLUDED.cntrb_id`, sbCntrbC, sbEmailAli)

	ids := make([]int64, 0, len(senders))
	for i, sender := range senders {
		mid := fmt.Sprintf("<_avsb-%d@test.invalid>", i)
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message (repo_id, platform_id, message_id_header, sender_email, msg_class)
			VALUES ($1, 6, $2, $3, 'discuss')
			ON CONFLICT (message_id_header) DO UPDATE SET sender_email = EXCLUDED.sender_email`, sbRepoID, mid, sender)
		var msgID int64
		err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, node_id, msg_text, cntrb_id)
			VALUES ($1, $2, 6, $3, $4, 'body', NULL)
			ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET cntrb_id = NULL, node_id = EXCLUDED.node_id
			RETURNING msg_id`, sbRepoID, sbMsgBase+int64(i), MsgKindEmail, mid).Scan(&msgID)
		if err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
		ids = append(ids, msgID)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.messages WHERE platform_id = 6 AND platform_msg_id >= $1 AND platform_msg_id < $1 + 50`, sbMsgBase)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.email_message WHERE message_id_header LIKE '<_avsb-%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email LIKE '_avsb-%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avsb-%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, sbRepoID)
	})
	return ids
}

func sbCntrbOf(t *testing.T, ctx context.Context, store *PostgresStore, msgID int64) string {
	t.Helper()
	var id *string
	if err := store.pool.QueryRow(ctx, `SELECT cntrb_id::text FROM aveloxis_data.messages WHERE msg_id = $1`, msgID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id == nil {
		return ""
	}
	return *id
}

// sbWindowCovering runs the backfill over one window covering all the
// given msg_ids.
func sbWindowCovering(t *testing.T, ctx context.Context, store *PostgresStore, ids []int64) int64 {
	t.Helper()
	lo, hi := ids[0], ids[0]
	for _, id := range ids {
		if id < lo {
			lo = id
		}
		if id > hi {
			hi = id
		}
	}
	n, err := store.BackfillMailingListSenderIDs(ctx, lo-1, hi-lo+1)
	if err != nil {
		t.Fatalf("backfill window: %v", err)
	}
	return n
}

// TestBackfillMailingListSenderIDsAliasOnly — the 2.8% hole: a sender
// reachable ONLY through contributors_aliases must attribute. Also pins
// SR-17 parity: the point-probe ResolveContributorIDByEmail and the set
// join must agree on the same input.
func TestBackfillMailingListSenderIDsAliasOnly(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli})
	sbWindowCovering(t, ctx, store, ids)
	got := sbCntrbOf(t, ctx, store, ids[0])
	if got != sbCntrbC {
		t.Fatalf("alias-only sender: cntrb_id = %q, want %q (the contributors_aliases arm)", got, sbCntrbC)
	}
	probe, ok, err := store.ResolveContributorIDByEmail(ctx, sbEmailAli)
	if err != nil || !ok || probe != got {
		t.Fatalf("SR-17 parity: point-probe = (%q,%v,%v), set join = %q — the two identity paths must agree", probe, ok, err, got)
	}
}

// TestBackfillMailingListSenderIDsEmailBeatsAlias — the same address
// matches contributor A by cntrb_email AND contributor B via an alias
// row; the email/canonical arm must win (it runs first; the alias arm
// only touches rows still NULL). Matches ResolveContributorIDByEmail's
// own probe order.
func TestBackfillMailingListSenderIDsEmailBeatsAlias(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailBoth})
	sbWindowCovering(t, ctx, store, ids)
	if got := sbCntrbOf(t, ctx, store, ids[0]); got != sbCntrbA {
		t.Fatalf("both-match sender: cntrb_id = %q, want %q (email arm must beat alias arm)", got, sbCntrbA)
	}
}

// TestBackfillMailingListSenderIDsWindowScoped — a row OUTSIDE the
// (after, after+window] range must not be touched.
func TestBackfillMailingListSenderIDsWindowScoped(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli, sbEmailAli})
	// Window covers only the first row.
	if _, err := store.BackfillMailingListSenderIDs(ctx, ids[0]-1, 1); err != nil {
		t.Fatal(err)
	}
	if got := sbCntrbOf(t, ctx, store, ids[0]); got != sbCntrbC {
		t.Fatalf("in-window row unresolved: %q", got)
	}
	if got := sbCntrbOf(t, ctx, store, ids[1]); got != "" {
		t.Fatalf("out-of-window row was touched: %q — the window predicate is broken", got)
	}
}

// TestSenderBackfillFullPassConverges — the SR-19 driving test: walking
// floor→ceiling windows resolves every resolvable fixture row, and a
// SECOND pass over the same range leaves them untouched and re-resolves
// nothing of ours ("cntrb_id IS NULL is the resume state" — scoped to
// the fixture's rows; the shared scratch DB may carry residue).
func TestSenderBackfillFullPassConverges(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli, sbEmailBoth, sbEmailAli})

	floor, err := store.MailingListMsgIDFloor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ceil, err := store.MailingListMsgIDCeiling(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if floor <= 0 || ceil < floor {
		t.Fatalf("bounds: floor=%d ceiling=%d", floor, ceil)
	}
	for _, id := range ids {
		if id < floor || id > ceil {
			t.Fatalf("fixture msg_id %d outside bounds [%d,%d]", id, floor, ceil)
		}
	}

	const window = int64(1_000_000)
	pass := func() {
		t.Helper()
		for after := floor - 1; after < ceil; after += window {
			if _, err := store.BackfillMailingListSenderIDs(ctx, after, window); err != nil {
				t.Fatalf("window after=%d: %v", after, err)
			}
		}
	}
	pass()
	want := []string{sbCntrbC, sbCntrbA, sbCntrbC}
	got1 := make([]string, len(ids))
	for i, id := range ids {
		got1[i] = sbCntrbOf(t, ctx, store, id)
		if got1[i] != want[i] {
			t.Fatalf("after pass 1, row %d cntrb = %q, want %q", i, got1[i], want[i])
		}
	}
	pass() // convergence: values unchanged
	for i, id := range ids {
		if got := sbCntrbOf(t, ctx, store, id); got != got1[i] {
			t.Fatalf("pass 2 changed row %d: %q → %q — the pass must be idempotent", i, got1[i], got)
		}
	}
}

// TestMailingListMsgIDBoundsSane — floor/ceiling bracket the seeded rows
// and behave on the shared DB.
func TestMailingListMsgIDBoundsSane(t *testing.T) {
	store, ctx := sbConnect(t)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli})
	floor, err := store.MailingListMsgIDFloor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ceil, err := store.MailingListMsgIDCeiling(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if floor <= 0 || ceil < ids[0] || floor > ids[0] {
		t.Fatalf("bounds [%d,%d] must bracket seeded msg_id %d", floor, ceil, ids[0])
	}
}
