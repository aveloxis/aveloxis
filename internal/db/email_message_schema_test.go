// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.26.0 — mailing-list ingestion. email_message is a first-class entity
// (peer to issues / pull_requests / pull_request_reviews); its body lives in
// messages, linked by email_message_ref. These source-contract tests pin the
// schema so a refactor can't silently drop a load-bearing column or the
// idempotent dedup keys.

func TestSchemaSeedsMailingListPlatform(t *testing.T) {
	src := readSchema(t)
	if !strings.Contains(src, "(6, 'Mailing List')") {
		t.Error("schema.sql platforms seed must include (6, 'Mailing List') — the transport platform for mailing-list-sourced rows")
	}
}

func TestSchemaDeclaresEmailMessageTable(t *testing.T) {
	src := readSchema(t)
	if !strings.Contains(src, "CREATE TABLE IF NOT EXISTS aveloxis_data.email_message (") {
		t.Fatal("schema.sql must declare the email_message entity table")
	}
	for _, col := range []string{
		"message_id_header", "ml_system", "list_address", "subject", "sender_email",
		"sent_at", "in_reply_to", "thread_root_id", "msg_class", "classification_source",
		"is_mirror", "mirrors_url", "signaled_repo_url", "signaled_repo_id",
		"linked_issue_id", "linked_pull_request_id", "linked_external_key", "linked_commit_hash",
	} {
		if !strings.Contains(src, col) {
			t.Errorf("email_message must declare column %s", col)
		}
	}
	// message_id_header is the idempotent dedup key (RFC-822 global uniqueness).
	if !strings.Contains(src, "UNIQUE (message_id_header)") {
		t.Error("email_message must declare UNIQUE (message_id_header) — re-collecting a month must not duplicate")
	}
	// signaled_repo_id must be an FK to repos (resolved per §5c).
	if !strings.Contains(src, "signaled_repo_id  BIGINT REFERENCES aveloxis_data.repos(repo_id)") {
		t.Error("signaled_repo_id must reference repos(repo_id)")
	}
}

func TestSchemaDeclaresEmailMessageRef(t *testing.T) {
	src := readSchema(t)
	if !strings.Contains(src, "CREATE TABLE IF NOT EXISTS aveloxis_data.email_message_ref (") {
		t.Fatal("schema.sql must declare the email_message_ref bridge table")
	}
	if !strings.Contains(src, "REFERENCES aveloxis_data.email_message(email_message_id)") ||
		!strings.Contains(src, "REFERENCES aveloxis_data.messages(msg_id)") {
		t.Error("email_message_ref must bridge email_message ↔ messages")
	}
	if !strings.Contains(src, "UNIQUE (email_message_id, msg_id)") {
		t.Error("email_message_ref must declare UNIQUE (email_message_id, msg_id)")
	}
}

func TestSchemaDeclaresIssuesExternalKey(t *testing.T) {
	src := readSchema(t)
	if !strings.Contains(src, "external_key     TEXT DEFAULT ''") {
		t.Error("issues must declare external_key — holds 'LUCENE-1' parsed from Pattern-A imported GitHub issue titles")
	}
}

func TestSchemaDeclaresMllsColumns(t *testing.T) {
	src := readSchema(t)
	for _, col := range []string{
		"mlls_system", "mlls_last_month", "mlls_scan_complete",
		"mlls_failed_attempts", "mlls_last_failed_at", "mlls_last_run",
		"mlls_locked_at", "mlls_locked_pid", "mlls_locked_boot_id",
	} {
		if !strings.Contains(src, col) {
			t.Errorf("repo_groups_list_serve must declare MailingListWorker claim/checkpoint column %s", col)
		}
	}
}

func TestMigrateAddsMailingListColumnsAndIndexes(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{
		`"aveloxis_data.issues", "external_key"`,
		`"aveloxis_data.repo_groups_list_serve", "mlls_system"`,
		`"aveloxis_data.repo_groups_list_serve", "mlls_scan_complete"`,
		`"aveloxis_data.repo_groups_list_serve", "mlls_locked_boot_id"`,
		"idx_issues_external_key",
		"idx_rgls_group_email",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must contain %q so existing DBs get the v0.26.0 columns/indexes", needle)
		}
	}
	// The external_key unique index must be partial (empty key excluded).
	if !strings.Contains(src, "external_key <> ''") {
		t.Error("idx_issues_external_key must be partial WHERE external_key <> '' so native rows don't collide on empty key")
	}
}

func readSchema(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
