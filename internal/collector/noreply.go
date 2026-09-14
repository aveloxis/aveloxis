// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strconv"
	"strings"
)

// noreplyPattern matches GitHub noreply email addresses:
//
//	12345+username@users.noreply.github.com  -> (username, 12345)
//	username@users.noreply.github.com        -> (username, 0)
var noreplyPattern = regexp.MustCompile(
	`^(?:(\d+)\+)?([a-zA-Z0-9][a-zA-Z0-9._-]*)@users\.noreply\.github\.com$`,
)

// NoreplyInfo holds the parsed result of a GitHub noreply email.
type NoreplyInfo struct {
	Login  string
	UserID int64 // 0 if not present in the email
	HasID  bool  // true if the numeric prefix was present
}

// ParseNoreplyEmail extracts GitHub login and optional user ID from a
// noreply email address. Returns nil if the email is not a noreply format.
//
// GitHub noreply emails come in two formats:
//
//	12345+username@users.noreply.github.com  (includes numeric user ID)
//	username@users.noreply.github.com        (login only)
//
// The numeric prefix is the gh_user_id, which is the stable identifier.
// The login can change (users can rename), but gh_user_id is permanent.
func ParseNoreplyEmail(email string) *NoreplyInfo {
	email = strings.TrimSpace(email)
	m := noreplyPattern.FindStringSubmatch(email)
	if m == nil {
		return nil
	}

	info := &NoreplyInfo{
		Login: m[2],
	}
	if m[1] != "" {
		id, err := strconv.ParseInt(m[1], 10, 64)
		if err == nil {
			info.UserID = id
			info.HasID = true
		}
	}
	return info
}

// IsNoreplyEmail returns true if the email is a GitHub noreply address.
func IsNoreplyEmail(email string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(email)), "@users.noreply.github.com")
}

// IsAutomationEmail reports whether an email address belongs to an
// automated sender: everything IsBotEmail catches PLUS the Apache
// notification relays (jira@ / jira+<project>@ / git@ / gitbox@
// apache.org). Found 2026-08-31: IsBotEmail alone let the relays
// classify as human — the sender-resolve ticker minted email-only
// CONTRIBUTOR rows for them and 83,746 messages were attributed to the
// jira@apache.org phantom. The SQL twin is
// aveloxis_data.is_automation_email (schema.sql), which additionally
// knows "sender IS a registered list address" (DB-side knowledge);
// TestAutomationEmailSQLParity pins the two spellings together
// (SR-17). Callers with the list address in hand should also treat
// sender == list address as automation.
func IsAutomationEmail(email string) bool {
	if IsBotEmail(email) {
		return true
	}
	lower := strings.ToLower(email)
	if lower == "git@apache.org" || lower == "gitbox@apache.org" {
		return true
	}
	if strings.HasSuffix(lower, "@apache.org") &&
		(lower == "jira@apache.org" || strings.HasPrefix(lower, "jira+")) {
		return true
	}
	// Bugzilla relays (v0.29.0 pre-release, summary/27 B0): ASF's
	// bugzilla@ / bugzilla-daemon@ (+ per-list daemon variants) send
	// ~193K notifications already in the corpus — IsBotEmail did not
	// know them, so the sender-resolve ticker was minting phantom
	// contributor rows for them exactly like the jira@ phantom. Kept
	// in lockstep with the SQL twin (TestAutomationEmailSQLParity).
	if strings.HasSuffix(lower, "@apache.org") &&
		(lower == "bugzilla@apache.org" ||
			strings.HasPrefix(lower, "bugzilla-") ||
			strings.HasPrefix(lower, "bugzilla+")) {
		return true
	}
	return false
}

// IsBotEmail reports whether the email looks like an automated/bot
// email that shouldn't be resolved to a human contributor. Production
// gates use IsAutomationEmail (the superset); this narrower predicate
// remains for the SQL-parity split and as IsAutomationEmail's first arm.
func IsBotEmail(email string) bool {
	lower := strings.ToLower(email)
	return strings.Contains(lower, "[bot]") ||
		strings.Contains(lower, "noreply") && !strings.Contains(lower, "users.noreply.github.com") ||
		strings.HasSuffix(lower, "@github.com") && !strings.Contains(lower, "users.noreply")
}
