// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

func TestMailingListOpsCommandsRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, c := range []string{"mailingListStatsCmd(", "backfillExternalKeysCmd("} {
		if !strings.Contains(src, c) {
			t.Errorf("main.go must register %s", c)
		}
	}
}

func TestMailingListStatsCmdUsesStore(t *testing.T) {
	data, err := os.ReadFile("mailing_list_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "MailingListStats(") {
		t.Error("mailing-list-stats must call store.MailingListStats")
	}
}
