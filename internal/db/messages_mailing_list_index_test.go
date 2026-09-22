// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestMailingListMsgIDIndexIsMigrationOwned pins the v0.29.58 partial
// index that serves MailingListMsgIDFloor/Ceiling: migrate.go builds it
// CONCURRENTLY with the platform-6 predicate on aveloxis_data.messages,
// and schema.sql does NOT declare it (SR-2: a fleet-scale index is
// migration-owned, or the base DDL block-builds it on the first upgrade).
func TestMailingListMsgIDIndexIsMigrationOwned(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	// Comment-stripped so prose cannot satisfy the pin (review round 2).
	code := srctest.StripGoComments(string(src))
	call := regexp.MustCompile(`(?s)execCreateIndexConcurrently\([^)]*?"aveloxis_data",\s*"idx_messages_mailing_list_msg_id",\s*` + "`" + `\s*CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_mailing_list_msg_id\s+ON aveloxis_data\.messages \(msg_id\) WHERE platform_id = 6` + "`")
	if !call.MatchString(code) {
		t.Fatal("migrate.go must build idx_messages_mailing_list_msg_id via execCreateIndexConcurrently " +
			"as CREATE INDEX CONCURRENTLY IF NOT EXISTS ... ON aveloxis_data.messages (msg_id) WHERE platform_id = 6 " +
			"— the mailing-list msg_id bounds full-scanned 141M rows (943 s each) without it (2026-09-22 log review)")
	}
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "idx_messages_mailing_list_msg_id") {
		t.Fatal("schema.sql must NOT declare idx_messages_mailing_list_msg_id (SR-2): the base DDL would block-build it on an existing fleet's first upgrade")
	}
}
