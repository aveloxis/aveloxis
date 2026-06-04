// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestMailingListStatsReportsMissedLinkDuplicates pins #2: the command surfaces
// synthetic/native external_key collisions and points the operator at the fix.
func TestMailingListStatsReportsMissedLinkDuplicates(t *testing.T) {
	data, err := os.ReadFile("mailing_list_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "store.MailingListProjectionDuplicates(") {
		t.Error("mailing-list-stats must call MailingListProjectionDuplicates for the missed-LINK guard")
	}
	if !strings.Contains(src, "backfill-mailing-list-projection") {
		t.Error("the missed-LINK message must point the operator at backfill-mailing-list-projection")
	}
}
