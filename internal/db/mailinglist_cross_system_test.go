// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Part G layer-3 finds (2026-09-01, five-project main-vs-branch run):
//
//  1. CROSS-SYSTEM DRAIN — ListsWithStaging took no system parameter, so
//     EVERY system's drain goroutine drained EVERY list with staged rows.
//     The per-list guard (Processor.inflight) is per-PROCESSOR, so the
//     apache_ponymail and lore_public_inbox processors interleaved batches
//     on the same apache list — and the lore processor carries
//     projectionClean=false (projection_policy "none"), so its batches got
//     NO issue projection, NO tracker actions, NO thread inheritance, and
//     a wrong ml_system stamp. Measured on the Part G scratch DBs: 92% of
//     the released side's 621K apache emails and 90% of the branch side's
//     were lore-drained. Pre-existing on main since the lore pool shipped.
//  2. PHANTOM MINT — the sender-resolve candidate query had no automation
//     filter; the only gates were the Go-side IsAutomationEmail belts in
//     the ticker loop, which structurally CANNOT know registered list
//     addresses (DB-side knowledge). DMARC-munged From headers
//     ("Foo Bar via dev" <dev@beam.apache.org>) made list addresses look
//     like human senders with >= 6 messages, and CreateEmailOnlyContributor
//     minted them (dev@beam.apache.org + dev@kafka.apache.org observed).
package db

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func xsRegisterList(t *testing.T, store *PostgresStore, group, list, system string) int64 {
	t.Helper()
	ctx := t.Context()
	gid, err := store.UpsertRepoGroup(ctx, group, "test", "")
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	if err := store.RegisterMailingList(ctx, gid, list, system); err != nil {
		t.Fatalf("register: %v", err)
	}
	var rgls int64
	if err := store.pool.QueryRow(ctx,
		`SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve
		 WHERE repo_group_id = $1 AND rgls_email = $2`, gid, list).Scan(&rgls); err != nil {
		t.Fatalf("rgls lookup: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE rgls_id = $1`, rgls)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1`, rgls)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, rgls)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, gid)
	})
	return rgls
}

// TestListsWithStagingFiltersBySystem is the fix-1 driver: a list registered
// under apache_ponymail must be INVISIBLE to the lore pool's drain (and vice
// versa), so a system's processor can only ever drain its own lists.
func TestListsWithStagingFiltersBySystem(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	apacheRgls := xsRegisterList(t, store, "_avxsys_apache", "dev@xsysa.apache.org", "apache_ponymail")
	loreRgls := xsRegisterList(t, store, "_avxsys_lore", "xsysb@vger.kernel.org", "lore_public_inbox")

	stage := func(rgls int64, mid string) {
		t.Helper()
		if err := store.StageMailingListMessage(ctx, rgls, nil, nil, model.MailingListStagedMessage{
			MessageID: mid, ListAddress: "x@y", Subject: "s", SenderEmail: "h@example.org",
			SentAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), MsgClass: "discuss", Body: "b",
		}); err != nil {
			t.Fatalf("stage: %v", err)
		}
	}
	stage(apacheRgls, "<xsys-a@example>")
	stage(loreRgls, "<xsys-b@example>")

	has := func(ids []int64, want int64) bool {
		for _, id := range ids {
			if id == want {
				return true
			}
		}
		return false
	}

	apacheLists, err := store.ListsWithStaging(ctx, "apache_ponymail", 1000)
	if err != nil {
		t.Fatalf("apache ListsWithStaging: %v", err)
	}
	if !has(apacheLists, apacheRgls) {
		t.Errorf("apache pool must see its own staged list %d", apacheRgls)
	}
	if has(apacheLists, loreRgls) {
		t.Errorf("apache pool must NOT see the lore list %d", loreRgls)
	}

	loreLists, err := store.ListsWithStaging(ctx, "lore_public_inbox", 1000)
	if err != nil {
		t.Fatalf("lore ListsWithStaging: %v", err)
	}
	if !has(loreLists, loreRgls) {
		t.Errorf("lore pool must see its own staged list %d", loreRgls)
	}
	if has(loreLists, apacheRgls) {
		t.Errorf("lore pool must NOT see the apache list %d — the cross-system drain find: 90-92%% of apache mail was drained by the lore processor (projectionClean=false), silently losing issue projection", apacheRgls)
	}
}

// TestSenderResolveCandidatesExcludeAutomation is the fix-2 driver: the
// candidate query must consult the SQL is_automation_email twin — the ONLY
// predicate that knows "sender IS a registered list address" — so a
// DMARC-munged list-address From can never reach the mint path.
func TestSenderResolveCandidatesExcludeAutomation(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	rgls := xsRegisterList(t, store, "_avxsys_cand", "dev@xsysc.apache.org", "apache_ponymail")

	seed := func(sender string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			em := &model.EmailMessage{
				RglsID: &rgls, PlatformID: model.Platform(MailingListPlatformID),
				MLSystem: "apache_ponymail", ListAddress: "dev@xsysc.apache.org",
				MessageIDHeader: "<cand-" + sender + "-" + string(rune('a'+i)) + "@example>",
				Subject:         "hello", SenderEmail: sender,
				SentAt:   time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
				MsgClass: "discuss", ProjectedKind: "mailing_list_only",
				DataSource: "dev@xsysc.apache.org",
			}
			if _, err := store.UpsertEmailMessage(ctx, em); err != nil {
				t.Fatalf("seed em: %v", err)
			}
		}
	}
	human := "xsys-human@example.org"
	seed(human, 7)
	seed("dev@xsysc.apache.org", 7) // the list address itself (DMARC-munged From)
	seed("jira@apache.org", 7)      // static relay
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_sender_resolve
			WHERE sender_email IN ($1, 'dev@xsysc.apache.org', 'jira@apache.org')`, human)
	})

	cands, err := store.GetMailingListSenderResolveCandidates(ctx, 6, 0, 10000)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	var sawHuman bool
	for _, c := range cands {
		switch c.SenderEmail {
		case human:
			sawHuman = true
		case "dev@xsysc.apache.org":
			t.Errorf("registered list address must never be a sender-resolve candidate (the I5 phantom-mint find)")
		case "jira@apache.org":
			t.Errorf("static relay must never be a sender-resolve candidate")
		}
	}
	if !sawHuman {
		t.Errorf("human sender %s must remain a candidate", human)
	}
}

// TestHealMisdrainedMailingListRows drives the ledgered heal: rows whose
// ml_system disagrees with their list's registered mlls_system are restamped,
// and UNLINKED mailing_list_only rows among them are reset to the pending
// sentinel (empty string) so backfill-mailing-list-projection re-runs the keyed +
// thread-inheritance passes over exactly the mis-drained cohort. Rows the
// wrong-system drain still managed to LINK (the mirror path is not gated on
// projectionClean) keep their kind.
func TestHealMisdrainedMailingListRows(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	rgls := xsRegisterList(t, store, "_avxsys_heal", "dev@xsysd.apache.org", "apache_ponymail")

	mk := func(mid, kind string) int64 {
		t.Helper()
		em := &model.EmailMessage{
			RglsID: &rgls, PlatformID: model.Platform(MailingListPlatformID),
			MLSystem:    "lore_public_inbox", // the mis-drain stamp
			ListAddress: "dev@xsysd.apache.org", MessageIDHeader: mid,
			Subject: "[jira] [Created] (XSYS-1) t", SenderEmail: "h@example.org",
			SentAt:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			MsgClass: "issue_event", LinkedExternalKey: "XSYS-1",
			ProjectedKind: kind, DataSource: "dev@xsysd.apache.org",
		}
		id, err := store.UpsertEmailMessage(ctx, em)
		if err != nil {
			t.Fatalf("seed em: %v", err)
		}
		return id
	}
	unlinked := mk("<heal-unlinked@example>", "mailing_list_only")
	linked := mk("<heal-linked@example>", "pr")

	if err := store.HealMisdrainedMailingListRows(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("heal: %v", err)
	}

	var sys, kind string
	if err := store.pool.QueryRow(ctx, `SELECT ml_system, COALESCE(projected_kind,'')
		FROM aveloxis_data.email_message WHERE email_message_id = $1`, unlinked).Scan(&sys, &kind); err != nil {
		t.Fatalf("read unlinked: %v", err)
	}
	if sys != "apache_ponymail" {
		t.Errorf("unlinked row: ml_system = %q, want the list's registered system", sys)
	}
	if kind != "" {
		t.Errorf("unlinked mailing_list_only row must reset to the pending sentinel '', got %q", kind)
	}
	if err := store.pool.QueryRow(ctx, `SELECT ml_system, COALESCE(projected_kind,'')
		FROM aveloxis_data.email_message WHERE email_message_id = $1`, linked).Scan(&sys, &kind); err != nil {
		t.Fatalf("read linked: %v", err)
	}
	if sys != "apache_ponymail" {
		t.Errorf("linked row: ml_system = %q, want restamp", sys)
	}
	if kind != "pr" {
		t.Errorf("already-linked row must KEEP its kind, got %q", kind)
	}

	// Idempotent: a second pass touches nothing further.
	if err := store.HealMisdrainedMailingListRows(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("heal rerun: %v", err)
	}
}

// TestMisdrainHealLedgered pins the migrate wiring: the heal runs as a
// LEDGERED step (one-shot per fleet; minutes-scale keyset walk on a
// mailing-list deployment must not re-run on every version bump).
func TestMisdrainHealLedgered(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	if !strings.Contains(src, "HealMisdrainedMailingListRows") {
		t.Fatal("migrate.go must invoke HealMisdrainedMailingListRows")
	}
	idx := strings.Index(src, "HealMisdrainedMailingListRows")
	window := src[max(0, idx-600):idx]
	if !strings.Contains(window, "runOnce(") {
		t.Error("the mis-drain heal must be wrapped in runOnce (ledgered)")
	}
}

// TestTrackerActionRankGuard pins the C3a provider-precedence rule at the
// state writer (SR-18): a MAIL-rank action may only advance a row the Jira
// API has written when its event time is STRICTLY newer. Part G found 53
// API-closed synthetics flipped open because Pony Mail rounds sent_at to the
// minute — a same-minute [Reopened]+[Resolved] pair ties the <= guard and
// batch order picked the winner. Mail-owned rows keep <= (replay safety).
func TestTrackerActionRankGuard(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	repo := &model.Repo{GitURL: "https://github.com/avxsys/rank-fixture", Owner: "avxsys",
		Name: "rank-fixture", Platform: model.PlatformGitHub}
	repoID, err := store.UpsertRepo(ctx, repo)
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	at := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	mkIssue := func(key, source string) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.issues
				(repo_id, platform_issue_id, issue_number, issue_title, issue_state,
				 external_key, closed_at, updated_at, data_source, tool_source, tool_version, data_collection_date)
			VALUES ($5, $1, 1, 't', 'closed', $2, $3, $3, $4, 'test', '0', NOW())
			RETURNING issue_id`,
			syntheticIssueID(key), key, at, source, repoID).Scan(&id); err != nil {
			t.Fatalf("seed issue: %v", err)
		}
		t.Cleanup(func() {
			store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, id)
		})
		return id
	}

	apiOwned := mkIssue("XSYSRANK-1", JiraAPIDataSource)
	mailOwned := mkIssue("XSYSRANK-2", "JIRA")

	state := func(id int64) string {
		t.Helper()
		var st string
		if err := store.pool.QueryRow(ctx,
			`SELECT issue_state FROM aveloxis_data.issues WHERE issue_id = $1`, id).Scan(&st); err != nil {
			t.Fatalf("state: %v", err)
		}
		return st
	}

	// TIE on an API-owned row: mail must NOT flip it.
	if err := store.ApplyTrackerAction(ctx, apiOwned, "Reopened", at); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := state(apiOwned); got != "closed" {
		t.Errorf("equal-timestamp mail action flipped an API-owned row to %q — rank guard missing", got)
	}
	// STRICTLY newer mail event on an API-owned row: freshness wins.
	if err := store.ApplyTrackerAction(ctx, apiOwned, "Reopened", at.Add(time.Minute)); err != nil {
		t.Fatalf("apply newer: %v", err)
	}
	if got := state(apiOwned); got != "open" {
		t.Errorf("strictly-newer mail action must still advance an API-owned row, got %q", got)
	}
	// TIE on a mail-owned row: <= preserved (replay safety unchanged).
	if err := store.ApplyTrackerAction(ctx, mailOwned, "Reopened", at); err != nil {
		t.Fatalf("apply mail-owned: %v", err)
	}
	if got := state(mailOwned); got != "open" {
		t.Errorf("equal-timestamp action on a MAIL-owned row must still apply, got %q", got)
	}
}
