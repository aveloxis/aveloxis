// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "strings"

// scrubLogValue strips CR/LF and other control characters from a
// user-influenced value before it is written to a log attribute.
//
// slog's TextHandler quote-escapes attribute values, so log forgery is
// not actually possible through these sites — this is the house
// defense-in-depth + scanner-hygiene pattern (v0.27.3's
// internal/api/logsafe.go and internal/web's truncateForLog scrub),
// applied to the collector package (v0.27.10). The \n/\r removals use
// strings.ReplaceAll specifically because taint analyzers recognize
// that form as a sanitizer.
func scrubLogValue(s string) string {
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
}
