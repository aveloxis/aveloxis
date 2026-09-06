// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestLinkedMsgUniqueIndexIsMigrationOnlyConcurrentAndPartial pins the
// Copilot-round-20 (PR #193) UNIQUE partial index on
// email_message (linked_msg_id) WHERE linked_msg_id IS NOT NULL.
//
// It serves TWO purposes: (a) the analytics native>notification dedup
// (Part D) enumerates the linked cohort — the same read the retired
// non-unique index served; (b) it is the HARD BACKSTOP for
// one-notification-per-native-comment. The two writers
// (UpsertJiraComment / LinkCommentNotificationToNative) use a NOT EXISTS
// anti-join that is check-then-act and races under concurrent
// mailing_list_processor drains; the unique index rejects the duplicate
// with 23505, which the writers handle.
//
// Deliberately PARTIAL — the consumer's predicate is the LITERAL
// `linked_msg_id IS NOT NULL`, matching the index predicate verbatim
// (the v0.25.34 sibling reasoning). Migration-only per SR-2 (the
// v0.28.20 precedent on this 13M-row table): a schema.sql declaration
// would block-build during an upgrading fleet's startup migrate.
func TestLinkedMsgUniqueIndexIsMigrationOnlyConcurrentAndPartial(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	migrate := srctest.Read(t, "internal/db/migrate.go")

	const ix = "uq_email_message_linked_msg"
	if strings.Contains(schema, ix) {
		t.Errorf("schema.sql declares %s — SR-2: migration-owned via execCreateIndexConcurrently only", ix)
	}
	if !srctest.ContainsNormalized(migrate, "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "+ix) {
		t.Errorf("%s must be a UNIQUE index built CONCURRENTLY — it is the one-notification-per-native backstop", ix)
	}
	if !srctest.ContainsNormalized(migrate,
		"ON aveloxis_data.email_message (linked_msg_id) WHERE linked_msg_id IS NOT NULL") {
		t.Errorf("%s must be partial on linked_msg_id IS NOT NULL — the consumer's literal predicate", ix)
	}
}

// TestRetiredNonUniqueLinkedMsgIndexIsDroppedNeverRecreated enforces
// SR-4 for the RETIRED non-unique idx_email_message_linked_msg: Copilot
// round 20 replaced it with the unique index above, so it must be
// DROPPED on existing fleets (to remove the redundant non-unique index)
// AND must never be CREATE'd again (a reused DROP+CREATE would rebuild
// forever, since DROP steps run every migrate).
func TestRetiredNonUniqueLinkedMsgIndexIsDroppedNeverRecreated(t *testing.T) {
	migrate := srctest.StripGoComments(srctest.Read(t, "internal/db/migrate.go"))
	const old = "idx_email_message_linked_msg"

	var dropped, created bool
	for _, line := range strings.Split(migrate, "\n") {
		if !strings.Contains(line, old) {
			continue
		}
		if strings.Contains(line, "DROP INDEX") {
			dropped = true
		}
		if strings.Contains(line, "CREATE INDEX") || strings.Contains(line, "CREATE UNIQUE INDEX") {
			created = true
		}
	}
	if !dropped {
		t.Errorf("%s must be DROP INDEX CONCURRENTLY IF EXISTS'd — it is retired in favor of the unique index", old)
	}
	if created {
		t.Errorf("%s must NOT be recreated — SR-4: a dropped name reused in a CREATE rebuilds forever", old)
	}
}
