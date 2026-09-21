// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"fmt"
	"strings"
)

// ErrURLUserinfo is returned for a URL that carries userinfo — credentials
// before the host, as in https://user:token@host/owner/name. A repository
// URL is stored, logged, shown in the GUI and handed to subprocesses
// (git, scorecard) on their command line, so credentials in it leak by
// every one of those paths. Refused at the URL parser, the web validator,
// every store writer of a URL column (UpsertRepo, UpdateRepoURLs,
// UpdateRepoURL, UpsertRepoGroup, UpsertFoundationMembership, the add
// paths), the redirect probe (input and target), the scheduler's job entry
// and every subprocess or
// third-party boundary (v0.29.57, Copilot review 5261384568: `aveloxis
// run-scorecard` forwarded a stored URL to `scorecard --repo` verbatim).
var ErrURLUserinfo = errors.New("URL carries credentials (userinfo before the host)")

// ErrRedirectTargetUserinfo is the redirect probe's answer when the STORED
// URL is clean but the forge's Location carries credentials: the redirect
// is not followed and nothing is written. It wraps ErrURLUserinfo, so every
// arm that refuses the class still fires, and a caller that wants to tell
// the operator the truth — "correct repo_git" would be wrong here — tests
// for it first (v0.29.57 fix-review round 7).
var ErrRedirectTargetUserinfo = fmt.Errorf("the redirect target carries credentials — not followed: %w", ErrURLUserinfo)

// RefuseURLUserinfo is the ONE check (SR-17) behind ErrURLUserinfo: nil for
// a URL without userinfo, ErrURLUserinfo otherwise. It reads the authority
// textually (userinfoBounds) rather than through url.Parse, so a URL that
// does not parse cannot smuggle credentials past it (fail closed), and so
// RedactURLUserinfo below cannot disagree with it — fix-review round 2 found
// the two as separate spellings: the redaction returned a scheme-relative
// //user:pw@host/x verbatim and panicked on //u@h/x://y. An "@" in the path,
// query or fragment is not userinfo. A schemeless string is refused when
// it is credential-shaped (`user:token@host/…`, `token@host/…` — what a web
// paste looks like before a scheme is added; the portal API and UpsertRepo
// see it raw, Copilot review 5267408933) and accepted when it is SCP-shaped
// (`git@host:path`, `user@host:path` — no ":" before the "@", a ":" after
// it), which is a clone URL git accepts and the facade allows.
func RefuseURLUserinfo(raw string) error {
	if _, _, ok := userinfoBounds(strings.TrimSpace(raw), false); ok {
		return ErrURLUserinfo
	}
	return nil
}

// RedactURLUserinfo is the ONE spelling (SR-17) of a URL that may carry a
// credential in a log line: userinfo, if any, becomes "***"; a string
// without it is returned unchanged. It is BROADER than the refusal on
// purpose: a schemeless `token@github.com/o/n` (what a web paste looks like
// before the validator prepends https://) is redacted too, because a false
// positive in a log line costs nothing and the miss put a token in the
// server log (fix-review round 2). Textual on purpose: url.URL.String()
// percent-escapes the marker and re-spells the rest.
func RedactURLUserinfo(raw string) string {
	raw = strings.TrimSpace(raw)
	start, end, ok := userinfoBounds(raw, true)
	if !ok {
		return raw
	}
	return raw[:start] + "***" + raw[end:]
}

// userinfoBounds locates the userinfo of raw: [start, end) covers it
// WITHOUT the "@" that ends it, so raw[:start] + "***" + raw[end:] is the
// redaction. The authority starts after a LEADING scheme's "://" (a "://"
// later in the string is path or query, not a scheme), or after a leading
// "//" (scheme-relative), or — when schemeless is set — at the start of the
// string; it ends at the first "/", "?" or "#"; the userinfo ends at the
// LAST "@" inside it (u@x@host is userinfo "u@x"). ok is false when there
// is no authority or no "@" in it.
func userinfoBounds(raw string, schemeless bool) (start, end int, ok bool) {
	switch scheme := leadingScheme(raw); {
	case strings.HasPrefix(raw, "//"):
		start = 2
	case scheme > 0:
		start = scheme + 3
	case schemeless, credentialShapedPaste(raw):
		start = 0
	default:
		return 0, 0, false
	}
	authority := raw[start:]
	if j := strings.IndexAny(authority, "/?#"); j >= 0 {
		authority = authority[:j]
	}
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return 0, 0, false
	}
	return start, start + at, true
}

// leadingScheme returns the length of the URL scheme raw starts with
// (letter, then letters, digits, "+", "-" or "."), when "://" follows it;
// 0 otherwise.
func leadingScheme(raw string) int {
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			continue
		case i > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'):
			continue
		case i > 0 && strings.HasPrefix(raw[i:], "://"):
			return i
		}
		return 0
	}
	return 0
}

// credentialShapedPaste reports whether a schemeless string's leading
// segment (up to the first "/", "?" or "#") carries an "@" in the web-paste
// shape — `user:token@host…` or `token@host/…` — as opposed to the SCP
// clone shape `[user@]host:path`, which has no ":" before its "@" and a ":"
// after it. Refusal (userinfoBounds with schemeless=false) uses it so a raw
// paste that reaches a store writer without the web validator is refused
// while `git@github.com:org/repo.git` is not.
func credentialShapedPaste(raw string) bool {
	seg := raw
	if j := strings.IndexAny(seg, "/?#"); j >= 0 {
		seg = seg[:j]
	}
	at := strings.LastIndex(seg, "@")
	if at < 0 {
		return false
	}
	scp := !strings.Contains(seg[:at], ":") && strings.Contains(seg[at+1:], ":")
	return !scp
}
