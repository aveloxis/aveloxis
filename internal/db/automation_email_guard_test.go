// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// automation_email_guard_test.go — C1-pre, DB side: the SQL twin of
// collector.IsAutomationEmail, the guard on both attribution arms, and
// the heal for the phantom relay contributors the ungated minting
// created (2026-08-31 production find).
package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSchemaDeclaresAutomationEmailFunction pins the SQL predicate —
// schema.sql runs every migrate, and CREATE OR REPLACE FUNCTION is
// idempotent there. The list-as-sender clause is DB-only knowledge
// (a sender that IS a registered list address is automation).
func TestSchemaDeclaresAutomationEmailFunction(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	if !strings.Contains(schema, "CREATE OR REPLACE FUNCTION aveloxis_data.is_automation_email") {
		t.Fatal("schema.sql must declare aveloxis_data.is_automation_email")
	}
	for _, needle := range []string{"jira@apache.org", "gitbox@apache.org", "repo_groups_list_serve"} {
		if !strings.Contains(schema, needle) {
			t.Errorf("is_automation_email must cover %q", needle)
		}
	}
}

// TestSenderBackfillArmsExcludeAutomation — both attribution arms and
// the dry-run count must refuse automation senders. Without the guard,
// the first fast pass attributes 5.48M relay bodies to whatever
// phantom row matches the relay address.
func TestSenderBackfillArmsExcludeAutomation(t *testing.T) {
	src := srctest.Read(t, "internal/db/email_message_store.go")
	for _, fn := range []string{
		"func (s *PostgresStore) BackfillMailingListSenderIDs(",
		"func (s *PostgresStore) CountResolvableMailingListSenders(",
	} {
		body := srctest.FuncBody(t, src, fn)
		if strings.Count(body, "is_automation_email") < 1 {
			t.Errorf("%s must gate on aveloxis_data.is_automation_email", fn)
		}
	}
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) BackfillMailingListSenderIDs(")
	if strings.Count(body, "is_automation_email") < 2 {
		t.Error("BOTH backfill arms must carry the automation guard")
	}
}

// TestAutomationEmailSQLParity — the Go and SQL predicates agree on
// the fixture set (SR-17: one vocabulary, two enforced spellings).
func TestAutomationEmailSQLParity(t *testing.T) {
	store, ctx := sbConnect(t)
	cases := []struct {
		email string
		want  bool
	}{
		{"jira@apache.org", true},
		{"jira+amqnet@apache.org", true},
		{"git@apache.org", true},
		{"gitbox@apache.org", true},
		{"notifications@github.com", true},
		{"x[bot]@users.noreply.github.com", true},
		{"noreply@example.org", true},
		{"12345+alice@users.noreply.github.com", false},
		{"alice@example.org", false},
		{"jirasmith@example.org", false},
	}
	for _, tc := range cases {
		var got bool
		if err := store.pool.QueryRow(ctx, `SELECT aveloxis_data.is_automation_email($1)`, tc.email).Scan(&got); err != nil {
			t.Fatalf("%q: %v", tc.email, err)
		}
		if got != tc.want {
			t.Errorf("is_automation_email(%q) = %v, want %v (must match collector.IsAutomationEmail)", tc.email, got, tc.want)
		}
	}
	// The DB-only clause: a registered list address is automation.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_groups (repo_group_id, rg_name)
		VALUES (944147300, '_avauto-group') ON CONFLICT (repo_group_id) DO NOTHING`)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email)
		VALUES (944147300, '_avauto-list@x.apache.org')
		ON CONFLICT DO NOTHING`)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email = '_avauto-list@x.apache.org'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = 944147300`)
	})
	var got bool
	if err := store.pool.QueryRow(ctx, `SELECT aveloxis_data.is_automation_email('_avauto-list@x.apache.org')`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Error("a registered list address must classify as automation (the dev@myfaces phantom class)")
	}
}

// TestBackfillRefusesAutomationSender — behavioral: a relay-sender
// message with a MATCHING phantom contributor must stay unattributed.
func TestBackfillRefusesAutomationSender(t *testing.T) {
	store, ctx := sbConnect(t)
	// Phantom: an email-only contributor row carrying the relay address
	// (the exact production shape, cntrb_login empty).
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, cntrb_canonical)
		VALUES ('944e0000-0000-4000-8000-0000000000dd'::uuid, '', 'jira@apache.org', 'jira@apache.org')
		ON CONFLICT (cntrb_id) DO UPDATE SET cntrb_email = EXCLUDED.cntrb_email, cntrb_deleted = 0`)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = '944e0000-0000-4000-8000-0000000000dd'::uuid`)
	})
	ids := sbSeed(t, ctx, store, []string{"jira@apache.org"})
	sbWindowCovering(t, ctx, store, ids)
	if got := sbCntrbOf(t, ctx, store, ids[0]); got != "" {
		t.Fatalf("relay-sender message was attributed to %q — the automation guard is missing", got)
	}
}

// TestHealAutomationPhantomContributors — the ledgered heal:
// soft-delete phantom automation contributors, drop their alias rows,
// and NULL the message attributions they hold. Legit contributors and
// attributions are untouched.
func TestHealAutomationPhantomContributors(t *testing.T) {
	store, ctx := sbConnect(t)
	const phantom = "944e0000-0000-4000-8000-0000000000ee"
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, cntrb_canonical)
		VALUES ($1::uuid, '', 'gitbox@apache.org', 'gitbox@apache.org')
		ON CONFLICT (cntrb_id) DO UPDATE SET cntrb_deleted = 0, gh_login = ''`, phantom)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, 'gitbox@apache.org', 'gitbox@apache.org')
		ON CONFLICT (alias_email) DO UPDATE SET cntrb_id = EXCLUDED.cntrb_id`, phantom)
	ids := sbSeed(t, ctx, store, []string{sbEmailAli})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.messages SET cntrb_id = $1::uuid WHERE msg_id = $2`, phantom, ids[0])
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = 'gitbox@apache.org'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, phantom)
	})

	if err := store.HealAutomationPhantomContributors(ctx); err != nil {
		t.Fatal(err)
	}
	var deleted int
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(cntrb_deleted, 0) FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, phantom).Scan(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Error("phantom automation contributor must be soft-deleted (R10: never physically deleted)")
	}
	var aliasCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email = 'gitbox@apache.org'`).Scan(&aliasCount); err != nil {
		t.Fatal(err)
	}
	if aliasCount != 0 {
		t.Error("the phantom's relay alias row must be removed")
	}
	if got := sbCntrbOf(t, ctx, store, ids[0]); got != "" {
		t.Errorf("falsely attributed message must be re-NULLed, got %q", got)
	}
	// The legit alias-only contributor from the shared seed is untouched.
	var legit int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid AND COALESCE(cntrb_deleted,0) = 0`, sbCntrbC).Scan(&legit); err != nil {
		t.Fatal(err)
	}
	if legit != 1 {
		t.Error("legit contributors must survive the heal")
	}
}
