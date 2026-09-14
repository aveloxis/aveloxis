// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// tracker_action_test.go — C1: the ONE Go spelling of the Jira
// notification action vocabulary, pinned against the systems.yaml
// classification rule so the two cannot drift (SR-17). Today the
// action is captured at classification time and then DISCARDED — all
// 485,892 synthetic issues sit permanently 'open' while 358,384
// Resolved notifications are already in the database.
package mailinglist

import "testing"

func TestTrackerActionFromSubject(t *testing.T) {
	cases := []struct {
		subject string
		want    string
	}{
		{"[jira] [Created] (KAFKA-123) add a thing", "Created"},
		{"[jira] [Resolved] (ARROW-99) fix the thing", "Resolved"},
		{"[jira] [Closed] (HBASE-1) old ticket", "Closed"},
		{"[jira] [Reopened] (BEAM-7) regression", "Reopened"},
		{"[jira] [Commented] (SPARK-5) note", "Commented"},
		{"[jira] [Work logged] (HIVE-2) hours", "Work logged"},
		{"Re: [jira] [Created] (KAFKA-123) reply", ""}, // replies are not the notification
		{"[DISCUSS] design", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := TrackerActionFromSubject(tc.subject); got != tc.want {
			t.Errorf("TrackerActionFromSubject(%q) = %q, want %q", tc.subject, got, tc.want)
		}
	}
}

// TestTrackerActionParityWithSystemsYAML — the classifier's own rule
// regex (systems.yaml capture group 1) and TrackerActionFromSubject
// must extract the SAME action on the notification shapes. A yaml rule
// edit that changes the vocabulary without touching the Go helper (or
// vice versa) fails here.
func TestTrackerActionParityWithSystemsYAML(t *testing.T) {
	systems, err := LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	apache := systems["apache_ponymail"]
	for _, subject := range []string{
		"[jira] [Created] (KAFKA-123) add a thing",
		"[jira] [Resolved] (ARROW-99) fix",
		"[jira] [Work logged] (HIVE-2) hours",
	} {
		cls := apache.Classify(Message{Subject: subject})
		if cls.Class != ClassIssueEvent {
			t.Fatalf("fixture %q did not classify issue_event", subject)
		}
		if got := TrackerActionFromSubject(subject); got != cls.Captures["action"] {
			t.Errorf("%q: Go helper = %q, yaml capture = %q — one action vocabulary (SR-17)", subject, got, cls.Captures["action"])
		}
	}
}
