// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestLinkedMsgIndexIsMigrationOnlyConcurrentAndPartial pins the fourth
// sibling of the v0.25.34 linked-* trio: idx_email_message_linked_msg on
// email_message (linked_msg_id) WHERE linked_msg_id IS NOT NULL.
//
// The analytics native>notification dedup (Part D) excludes a messages row
// when it is the body of a notification whose native twin was collected
// (email_message.linked_msg_id IS NOT NULL). That exclusion enumerates the
// linked cohort; without this index the enumeration is a 13M-row seq scan of
// email_message per analytics query on the mailing-list deployment.
//
// Deliberately PARTIAL — unlike the node-id indexes (v0.27.54: join-variable
// probes cannot prove a partial predicate), the consumer's predicate is the
// LITERAL `linked_msg_id IS NOT NULL`, which matches the index predicate
// verbatim (the same reasoning as its three v0.25.34 siblings).
//
// Migration-only per SR-2 (the v0.28.20 precedent on this very table): the
// base DDL runs before migration steps, so a schema.sql declaration would
// block-build on 13M rows during an upgrading fleet's startup migrate. The
// v0.25.34 trio predates that rule and is grandfathered in schema.sql.
func TestLinkedMsgIndexIsMigrationOnlyConcurrentAndPartial(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	migrate := srctest.Read(t, "internal/db/migrate.go")

	const ix = "idx_email_message_linked_msg"
	if strings.Contains(schema, ix) {
		t.Errorf("schema.sql declares %s — SR-2: migration-owned via execCreateIndexConcurrently only", ix)
	}
	if !strings.Contains(migrate, ix) {
		t.Fatalf("migrate.go must create %s (the Part D native>notification dedup enumeration)", ix)
	}
	if !srctest.ContainsNormalized(migrate, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+ix) {
		t.Errorf("%s must be built CONCURRENTLY (blocking build stalls mailing-list writers)", ix)
	}
	if !srctest.ContainsNormalized(migrate,
		"ON aveloxis_data.email_message (linked_msg_id) WHERE linked_msg_id IS NOT NULL") {
		t.Errorf("%s must be partial on linked_msg_id IS NOT NULL — the consumer's literal predicate", ix)
	}
}

// TestLinkedMsgIndexNameIsNotADropTarget enforces SR-4: DROP INDEX steps run
// on every migrate, so reusing a dropped name would rebuild the index forever.
func TestLinkedMsgIndexNameIsNotADropTarget(t *testing.T) {
	migrate := srctest.Read(t, "internal/db/migrate.go")
	stripped := srctest.StripGoComments(migrate)
	for _, line := range strings.Split(stripped, "\n") {
		if strings.Contains(line, "DROP INDEX") && strings.Contains(line, "idx_email_message_linked_msg") {
			t.Fatalf("idx_email_message_linked_msg appears in a DROP INDEX step — SR-4: dropped names are never recreated: %s", line)
		}
	}
}
