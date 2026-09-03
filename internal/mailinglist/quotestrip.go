// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// quotestrip.go — the quoted-history pattern library (Part B of the
// email-attribution program). Mailing-list bodies overwhelmingly embed
// the thread they reply to: measured on 20,000 messages across 5 Apache
// lists (2026-08-31), 82.5% carry quoted history and 64% of body
// CHARACTERS are quotation, while GitHub comments average 274 chars
// with 2% quoted. StripQuotedHistory recovers the author's OWN text —
// median 4,774 → 302 chars on the beam corpus, with ~0.02% of messages
// emptied (a 100%-quote reply is a legal, meaningful result).
//
// The raw body is provenance and is NEVER mutated: callers store the
// stripped text beside it (messages.msg_text_clean) tagged with
// QuoteStripRuleVersion, and consumers read
// COALESCE(msg_text_clean, msg_text). A rule change bumps the version;
// `aveloxis strip-quoted-history --rule-rerun` re-walks rows stamped
// under an older rule.

package mailinglist

import (
	"regexp"
	"strings"
)

// QuoteStripRuleVersion identifies the rule set that produced a stored
// msg_text_clean value. Bump on ANY behavioral change to
// StripQuotedHistory so stored rows are distinguishable from
// current-rule output.
const QuoteStripRuleVersion = "qs-v1"

// Rule prevalence on the measured 20,000-message corpus is noted per
// pattern; ordering inside StripQuotedHistory is drop-to-end markers
// first (they end the message), then per-line drops.
var (
	// 77.5% — classic `>` quotation.
	qsQuoted = regexp.MustCompile(`^\s*>`)
	// 69.1% — "On <date> ... <someone> wrote:" attribution. Copilot
	// round 24 (PR #193): the start line must carry a DATE shape (a
	// digit — day/year/time), not merely begin with "On ". The cleaned
	// body feeds analytics across the historical corpus, and authored
	// prose like "On Windows" followed within two lines by "...what
	// Alice wrote:" must NOT be deleted as quoted history. Real
	// attributions ("On Mon, Jan 5, 2026 at 3:14 PM, Alice wrote:")
	// always carry a digit; a digit-less "On ..." falls through to the
	// default arm and is kept. Single-line form requires the digit
	// before the "wrote:" tail; the wrapped two/three-line form pairs
	// qsAttrStart (digit) with the qsWroteTail lookahead.
	qsAttrOne   = regexp.MustCompile(`^On .*[0-9].*wrote:\s*$`)
	qsAttrStart = regexp.MustCompile(`^On .*[0-9]`)
	qsWroteTail = regexp.MustCompile(`wrote:\s*$`)
	// 16.8% — the RFC 3676 signature delimiter ("-- ", tolerating the
	// space-stripped "--" variant mailers emit).
	qsSigDelim = regexp.MustCompile(`^-- ?$`)
	// 0.6% — Outlook-style inline forward/reply marker.
	qsOrigMsg = regexp.MustCompile(`^-+ ?Original Message ?-+$`)
	// 0.8% / 9.7% — bare separator rules (Outlook underscores, ezmlm
	// dashes). Dropped as LINES only — the text after them is often the
	// author's own (unlike the drop-to-end markers above).
	qsUnderscores = regexp.MustCompile(`^_{10,}\s*$`)
	qsDashes      = regexp.MustCompile(`^-{20,}\s*$`)
	// 0.7% — ezmlm list footer; nothing after it is authored text.
	qsUnsub = regexp.MustCompile(`^To unsubscribe, e-?mail:`)
	// Jira notification trailer (22.3% of the corpus carries Jira
	// URLs); the version line follows it.
	qsAtlassian = regexp.MustCompile(`^This message was sent by Atlassian`)
)

// StripQuotedHistory returns body with quoted history, attribution
// lines, signature blocks, and footer boilerplate removed, plus the
// rule-set version that produced the result. Pure function; the input
// is never modified. An all-quote input legally returns "".
func StripQuotedHistory(body string) (clean string, rule string) {
	if body == "" {
		return "", QuoteStripRuleVersion
	}
	lines := strings.Split(body, "\n")
	out := make([]string, 0, len(lines))
scan:
	for i := 0; i < len(lines); i++ {
		ln := strings.TrimRight(lines[i], " \t\r")
		switch {
		case qsSigDelim.MatchString(ln),
			qsOrigMsg.MatchString(ln),
			qsUnsub.MatchString(ln),
			qsAtlassian.MatchString(ln):
			break scan // drop to end
		case qsQuoted.MatchString(ln),
			qsAttrOne.MatchString(ln),
			qsUnderscores.MatchString(ln),
			qsDashes.MatchString(ln):
			continue // drop line
		case qsAttrStart.MatchString(ln):
			// Wrapped attribution: "On <date> <name>" with the
			// "<email> wrote:" tail within the next two lines.
			for j := i + 1; j <= i+2 && j < len(lines); j++ {
				if qsWroteTail.MatchString(strings.TrimRight(lines[j], " \t\r")) {
					i = j // skip the whole attribution block
					continue scan
				}
			}
			out = append(out, lines[i])
		default:
			out = append(out, lines[i])
		}
	}
	// Collapse leading/trailing blank runs; internal spacing is the
	// author's own and stays.
	start, end := 0, len(out)
	for start < end && strings.TrimSpace(out[start]) == "" {
		start++
	}
	for end > start && strings.TrimSpace(out[end-1]) == "" {
		end--
	}
	return strings.Join(out[start:end], "\n"), QuoteStripRuleVersion
}
