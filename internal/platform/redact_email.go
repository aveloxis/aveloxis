// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"strings"
	"unicode/utf8"
)

// RedactEmail is the one spelling (SR-17) of an email address in a log
// line: the first character of the local part, "***", and the domain —
// enough to recognise which account a line means, not the address itself
// (v0.29.71, personal data in INFO logs: the mailer's startup line logged
// the SMTP account and the operator's address in full). A string without
// an "@" is "***"; an empty one stays empty (not configured).
func RedactEmail(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return "***"
	}
	first := ""
	if at > 0 {
		_, n := utf8.DecodeRuneInString(addr)
		first = addr[:n]
	}
	return first + "***" + addr[at:]
}
