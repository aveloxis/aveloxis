// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import "strings"

// logSafe neutralizes a user-controlled string before it reaches a log
// entry (CodeQL: go/log-injection). Newlines and carriage returns are
// removed so a crafted query parameter cannot forge log lines, other
// control characters are dropped, and the value is length-bounded so a
// huge parameter cannot flood the log. slog's Text/JSON handlers
// already quote-escape attribute values — this is defense in depth for
// any future handler change, and it makes the sanitization visible at
// the call site.
func logSafe(s string) string {
	// Explicit ReplaceAll for \n and \r first: these are the log-forging
	// characters, and this form is what taint analyzers recognize as the
	// sanitization step.
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	const maxLen = 200
	if len(s) > maxLen {
		return s[:maxLen] + "...(truncated)"
	}
	return s
}
