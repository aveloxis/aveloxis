// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDeployProbesHandleMissingSchema (Copilot round 23): a fresh DB has
// no aveloxis_ops schema, so the reference fails with 3F000
// (invalid_schema_name) before 42P01 — both must be treated as fresh.
func TestDeployProbesHandleMissingSchema(t *testing.T) {
	src := srctest.Read(t, "internal/db/deploy_ack.go")
	for _, fn := range []string{
		"func (s *PostgresStore) DeployAckExists(",
		"func (s *PostgresStore) FleetHasCollectedData(",
	} {
		body := srctest.FuncBody(t, src, fn)
		if !strings.Contains(body, `"3F000"`) {
			t.Errorf("%s must treat 3F000 (invalid_schema_name) as fresh, not just 42P01 (round 23)", fn)
		}
	}
}

// TestDeriveJiraProjectsCarriesRepoCount (Copilot round 23): the
// synthetic-derived candidates must carry a distinct-repo count so the
// register command can refuse to auto-pick a repo for a key that spans
// several.
func TestDeriveJiraProjectsCarriesRepoCount(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/jira_store.go"),
		"func (s *PostgresStore) DeriveJiraProjectsFromSynthetics(")
	if !strings.Contains(body, "count(DISTINCT repo_id)") {
		t.Error("DeriveJiraProjectsFromSynthetics must select count(DISTINCT repo_id) — the min(repo_id) guess must be detectable as ambiguous (round 23)")
	}
}

// TestSenderResolverAndDryRunRejectAmbiguous (Copilot round 23, SR-6):
// both the ingest-time point resolver AND the dry-run count must require
// an unambiguous direct match, not LIMIT 1 / bare EXISTS.
func TestSenderResolverAndDryRunRejectAmbiguous(t *testing.T) {
	src := srctest.Read(t, "internal/db/email_message_store.go")
	resolver := srctest.FuncBody(t, src, "func (s *PostgresStore) ResolveContributorIDByEmail(")
	if !strings.Contains(resolver, "count(DISTINCT cntrb_id) AS n") || !strings.Contains(resolver, "WHERE t.n = 1") {
		t.Error("ResolveContributorIDByEmail's direct arm must require exactly one distinct contributor (SR-6, round 23) — not LIMIT 1")
	}
	dry := srctest.FuncBody(t, src, "func (s *PostgresStore) CountResolvableMailingListSenders(")
	if !strings.Contains(dry, "count(DISTINCT c.cntrb_id)") || !strings.Contains(dry, ") = 1") {
		t.Error("CountResolvableMailingListSenders' direct arm must require exactly one distinct contributor (matches the backfill, round 23)")
	}
}

// TestResolveEmailIsUnambiguous (Copilot round 23, SR-6, AVELOXIS_TEST_DB):
// an email shared by two active contributors resolves to "" (not an
// arbitrary pick); a single-contributor email resolves to that id.
func TestResolveEmailIsUnambiguous(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	ambig := "r23ambig-" + tag + "@example.org"
	solo := "r23solo-" + tag + "@example.org"
	var cA, cB, cS string
	for _, seed := range []struct {
		id           *string
		login, email string
	}{
		{&cA, "r23a-" + tag, ambig}, {&cB, "r23b-" + tag, ambig}, {&cS, "r23s-" + tag, solo},
	} {
		mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, cntrb_email, cntrb_deleted) VALUES (gen_random_uuid(), $1, $2, 0)
			RETURNING cntrb_id::text`, seed.id, seed.login, seed.email)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text IN ($1,$2,$3)`, cA, cB, cS)
	})

	id, ok, err := store.ResolveContributorIDByEmail(ctx, ambig)
	if err != nil {
		t.Fatal(err)
	}
	if ok || id != "" {
		t.Errorf("ambiguous email resolved to %q — SR-6 requires unresolved (round 23)", id)
	}
	id, ok, err = store.ResolveContributorIDByEmail(ctx, solo)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || id != cS {
		t.Errorf("single-contributor email must resolve to %s, got %q ok=%v", cS, id, ok)
	}
}
