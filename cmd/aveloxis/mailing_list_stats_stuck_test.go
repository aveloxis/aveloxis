// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestMailingListStatsReportsStuckLists pins that the coverage command surfaces
// lists stuck awaiting a repo for their org group (summary/12 §11), so an
// operator sees which PMCs need org-population instead of grepping logs.
func TestMailingListStatsReportsStuckLists(t *testing.T) {
	data, err := os.ReadFile("mailing_list_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "store.StuckMailingLists(") {
		t.Error("mailing-list-stats must call store.StuckMailingLists to surface stuck lists")
	}
	if !strings.Contains(src, "stuck") {
		t.Error("mailing-list-stats must label the stuck-list section so operators recognize it")
	}
}
