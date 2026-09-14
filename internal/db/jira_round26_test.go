// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRegisterJiraProjectCanonicalizesTrailingSlash (Copilot round 26,
// PR #193): the one-instance guard compared RAW base_url, but jira.New
// canonicalizes with strings.TrimRight(baseURL, "/"). So the SAME instance
// spelled with vs without a trailing slash read as two different Jira
// instances and the second registration was wrongly refused. Both the
// incoming URL (Go) and the stored side (rtrim) are now canonicalized.
func TestRegisterJiraProjectCanonicalizesTrailingSlash(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	cleanup := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key LIKE '_AVR26%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Arm 1 (incoming canonicalization): register WITHOUT a slash, then a
	// different project on the same instance WITH a slash — must be accepted.
	if err := store.RegisterJiraProject(ctx, "_AVR26A", "https://issues.example.org/jira", nil); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := store.RegisterJiraProject(ctx, "_AVR26B", "https://issues.example.org/jira/", nil); err != nil {
		t.Fatalf("same instance with a trailing slash must NOT read as a different Jira instance: %v", err)
	}
	var got string
	if err := store.pool.QueryRow(ctx,
		`SELECT base_url FROM aveloxis_ops.jira_project_serve WHERE project_key='_AVR26B'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "https://issues.example.org/jira" {
		t.Errorf("stored base_url = %q, want the canonical (trailing slash trimmed) form", got)
	}

	// A genuinely different instance is still refused.
	if err := store.RegisterJiraProject(ctx, "_AVR26C", "https://other.example.org/jira", nil); err == nil {
		t.Fatal("a genuinely different instance must still be refused")
	}

	// Arm 2 (stored canonicalization): a LEGACY non-canonical stored row
	// (inserted directly, bypassing RegisterJiraProject) must still compare
	// equal to a canonical incoming URL via rtrim.
	cleanup()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.jira_project_serve
		(project_key, base_url, tool_version) VALUES ('_AVR26L', 'https://legacy.example.org/jira/', '0.0.0')`)
	if err := store.RegisterJiraProject(ctx, "_AVR26M", "https://legacy.example.org/jira", nil); err != nil {
		t.Fatalf("a canonical URL must match a legacy trailing-slash stored row (rtrim on the stored side): %v", err)
	}
}

// TestCreateEmailOnlyContributorRepairsMissingAlias (Copilot round 26): a
// prior run may have created the contributor row but lost the best-effort
// alias write; the by-email probe used to return early forever without
// repairing it. The existing-contributor path now ensures the alias too.
func TestCreateEmailOnlyContributorRepairsMissingAlias(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	email := fmt.Sprintf("avr26+%d@example.org", time.Now().UnixNano())

	var id string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', $1, $1, 'test', 'Mailing List', NOW()) RETURNING cntrb_id::text`, &id, email)
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id::text = $1`, id)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, id)
	})

	var cnt int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("precondition: expected 0 aliases, got %d", cnt)
	}

	gotID, err := store.CreateEmailOnlyContributor(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id {
		t.Fatalf("expected the existing contributor %s, got %s", id, gotID)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("the existing-contributor path must repair the missing convergence alias; got %d aliases", cnt)
	}
}

// TestCreateEmailOnlyContributorPropagatesAliasError (Copilot round 26): the
// create path must not silently discard the alias write — a lost alias would
// never be repaired (the caller marks the sender terminally resolved).
func TestCreateEmailOnlyContributorPropagatesAliasError(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/mailinglist_sender_resolve_store.go"),
		"func (s *PostgresStore) CreateEmailOnlyContributor("))
	// Round 28 (PR #193): the alias write moved into the transaction
	// (ensureAliasTx) under a per-email advisory lock. It must still be
	// error-propagating on both the existing-by-email and create paths, and
	// the discard form must never return.
	if strings.Contains(body, "_ = ensureAliasTx") || strings.Contains(body, "_ = s.EnsureContributorAlias") {
		t.Error("CreateEmailOnlyContributor must PROPAGATE the alias error, not discard it (round 26/28)")
	}
	if strings.Count(body, "ensureAliasTx(ctx, tx") < 2 {
		t.Error("CreateEmailOnlyContributor must ensure the alias on BOTH the existing-by-email and the create paths (round 26/28)")
	}
	// Round 28: the create must be serialized by a per-email advisory lock.
	if !strings.Contains(body, "pg_advisory_xact_lock") {
		t.Error("CreateEmailOnlyContributor must serialize concurrent creators with a per-email advisory lock (round 28)")
	}
}
