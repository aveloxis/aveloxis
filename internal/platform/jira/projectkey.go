// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package jira

import "regexp"

// projectKeyRe is the Jira project-key shape (the same [A-Z][A-Z0-9]+
// the mail parser extracts from "[jira] [Action] (KEY-N)" subjects).
// Copilot round 27 (PR #193): the key is interpolated into `project = …`
// JQL in WalkProjectByUpdated / ProjectTotal, so a value like
// `X OR project = Y` would widen the scan and stage another project's
// issues under the registered repo. Validating the shape at the JQL
// boundary (SR-18) makes injection structurally impossible — a valid
// key carries no space, quote, or operator.
var projectKeyRe = regexp.MustCompile(`^[A-Z][A-Z0-9]+$`)

// ValidProjectKey reports whether key is a well-formed Jira project key
// (safe to interpolate into JQL). Shared by the walk, the count probe,
// and the registration CLI (SR-17: one spelling).
func ValidProjectKey(key string) bool {
	return projectKeyRe.MatchString(key)
}
