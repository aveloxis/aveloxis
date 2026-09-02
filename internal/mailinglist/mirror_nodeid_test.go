// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import "testing"

// TestNodeIDFromMessageID pins the GitBox mirror Message-ID → GitHub GraphQL
// node-ID extraction. Apache's GitBox relay uses the node ID as the local part
// of the Message-ID (root message) and appends a UUID for each reply, so the
// node ID is recoverable from provenance alone — which matters because the
// default mirror_handling ("metadata_only") never stores the body, so the
// body-URL capture path cannot be used to heal historical rows.
//
// Inputs are verbatim production shapes from the `aveloxis` DB (2026-08-29).
func TestNodeIDFromMessageID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"pr root", "PR_kwDOBCyuKc8AAAABBMldbw@gitbox.apache.org", "PR_kwDOBCyuKc8AAAABBMldbw"},
		{"pr reply with uuid suffix",
			"PR_kwDOBCyuKc8AAAABBMldbw-f9312bc0-9406-4f3d-95fd-e0ae8915b4e9@gitbox.apache.org",
			"PR_kwDOBCyuKc8AAAABBMldbw"},
		{"issue root", "I_kwDOBCyuKc6XYZabc@gitbox.apache.org", "I_kwDOBCyuKc6XYZabc"},
		{"angle brackets stripped", "<PR_kwDOAQXtWs5nynym@gitbox.apache.org>", "PR_kwDOAQXtWs5nynym"},
		// Round 15 (Copilot): the host is part of the trust decision —
		// a Message-ID is SENDER-controlled, and without the GitBox
		// domain gate any list participant could link their message to
		// an arbitrary stored PR/issue by pasting a public node ID.
		{"no at sign is untrusted", "PR_kwDOBCyuKc8AAAABBMldbw", ""},
		{"attacker host is untrusted", "PR_kwDOBCyuKc8AAAABBMldbw@evil.example.org", ""},
		{"host case-insensitive", "PR_kwDOBCyuKc8AAAABBMldbw@GitBox.Apache.ORG", "PR_kwDOBCyuKc8AAAABBMldbw"},

		// Node IDs are base64url: a trailing '-' segment that is NOT a UUID
		// must survive, or we would truncate a legitimate identifier.
		{"hyphen in node id preserved", "PR_kwDO-abc-def@gitbox.apache.org", "PR_kwDO-abc-def"},

		// Non-mirror / human mail must yield nothing rather than a bogus key.
		{"human message id", "CADkQ1tYq@mail.gmail.com", ""},
		{"jira notification", "JIRA.13579.1234567890@Atlassian.JIRA", ""},
		{"empty", "", ""},
		{"at only", "@gitbox.apache.org", ""},
		{"wrong type prefix", "MDExOlB1bGxSZXF1ZXN0@gitbox.apache.org", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NodeIDFromMessageID(tc.in); got != tc.want {
				t.Errorf("NodeIDFromMessageID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
