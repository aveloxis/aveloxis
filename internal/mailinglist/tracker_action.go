// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import "regexp"

// trackerActionRe is the ONE Go spelling of the Jira notification
// action extraction — the same shape as the systems.yaml issue_event
// rule's first capture ([\w ]+ because actions like "Work logged"
// carry spaces). TestTrackerActionParityWithSystemsYAML pins the two
// spellings together (SR-17); internal/db's state backfill carries a
// third (SQL) spelling pinned by the same fixture set.
var trackerActionRe = regexp.MustCompile(`^\[jira\] \[([\w ]+)\] \([A-Z][A-Z0-9]+-\d+\)`)

// TrackerActionFromSubject returns the Jira notification action
// ("Created", "Resolved", "Work logged", …) from a subject line, or
// "" when the subject is not a Jira notification (replies included —
// only the bot's own "[jira] …" shape counts).
func TrackerActionFromSubject(subject string) string {
	if m := trackerActionRe.FindStringSubmatch(subject); m != nil {
		return m[1]
	}
	return ""
}
