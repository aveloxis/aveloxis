// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostid

import (
	"os"
	"strings"
	"sync"
)

var (
	hostTagOnce sync.Once
	hostTagVal  string
)

// HostTag is this machine's marker for the `application_name` host
// suffix (v0.29.4 round 12): os.Hostname(), sanitized to the byte set
// that can never confuse the '@' split the readers key on, or "" when
// the kernel will not tell us.
//
// "" is load-bearing, not an oversight: an empty marker means the
// component tags UN-SUFFIXED and every reader falls back to the
// client-address rule — exactly today's behavior. A host that cannot
// name itself degrades to the pre-v0.29.4 verdict rather than
// fabricating a marker that could collide with another host's.
//
// Sanitizing keeps [A-Za-z0-9._-] and rewrites everything else to '-'.
// The one byte that MUST NOT survive is '@': the tag is parsed with
// split_part(application_name, '@', 1), so an '@' inside the hostname
// would move the component boundary and the tag would match nothing.
// A hostname cannot legally contain one, so this is a belt — but the
// readers' correctness rests on it, so it is enforced here rather than
// assumed. Leading and trailing '-' are trimmed so a fully-rewritten
// hostname degrades to "" instead of to a run of dashes that every
// such host would share.
//
// The rewrite is NOT injective — `user@host` and `user-host` both
// become `user-host` — and that is tolerable rather than a defect,
// because of how the marker is consumed. Since round 13 the marker can
// only VETO the client-address rule, never override it
// (db.sameHostAsProbeSQL), so two hosts whose markers collide fall
// back to the address rule: the pre-v0.29.4 verdict, never a
// pg_terminate_backend recipe for a machine we are not on. The same
// tolerance covers the larger collision this cannot fix at all —
// os.Hostname() is not unique across machines, and two container hosts
// running one compose file report the same name. Marker quality is a
// precision property here, not a safety one.
//
// Cached: a process's hostname does not change under it, and the tag
// is read on every pool connection.
func HostTag() string {
	hostTagOnce.Do(func() {
		h, err := os.Hostname()
		if err != nil {
			return
		}
		hostTagVal = SanitizeHostTag(h)
	})
	return hostTagVal
}

// SanitizeHostTag is HostTag's rule, exported so the tag composer and
// its tests share ONE spelling (SR-17) and so a hostname can be driven
// through it without an os.Hostname seam.
func SanitizeHostTag(h string) string {
	h = strings.TrimSpace(h)
	if h == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(h))
	for _, r := range h {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
