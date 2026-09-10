// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/hostid"
)

// AppNameHostSep separates a component's match prefix from this host's
// marker inside `application_name`: `aveloxis-serve@kate`. It is the
// one character the readers split on, and hostid.SanitizeHostTag
// guarantees it can never appear inside the marker itself.
const AppNameHostSep = "@"

// maxAppNameBytes is PostgreSQL's application_name ceiling,
// NAMEDATALEN-1. The server TRUNCATES silently past it, and a
// truncated marker is the one failure this whole change exists to
// remove: two hosts sharing a long prefix
// (`scancode-runner-01.internal.example.com` and
// `scancode-runner-02.internal.example.com`) would truncate to the
// SAME marker and read as one host — the incident's shape, restored by
// the fix meant to prevent it. AppNameForHost therefore never
// truncates a marker; it digests it.
const maxAppNameBytes = 63

// AppNameForHost composes the application_name a component tags its
// pool with: the component's match prefix (`aveloxis-serve`) plus this
// host's marker. It is the TAGGING half of the SR-17 pair whose
// reading half is appNamePrefixSQL / appNameHostSQL — the two must
// agree on the separator, so both live here.
//
// v0.29.4 round 12 (Copilot #1, second raise). The host verdict was
// client-address equality alone, which is not host identity: behind a
// transaction pooler, a database proxy or shared NAT every client
// collapses onto one address, and `aveloxis stop` on a scancode runner
// would classify the PRIMARY's backends as this host's and print
// pg_terminate_backend recipes for production — the 2026-09-09
// incident, reachable again through a topology the address rule cannot
// see. A marker carried IN the tag survives every such collapse
// because the server never rewrites application_name.
//
// Three degradations, all toward the pre-v0.29.4 verdict rather than
// toward a wrong one:
//
//   - no hostname (hostid.HostTag() == "") -> tag UN-suffixed, readers
//     fall back to the address rule.
//   - marker too long for the ceiling -> a stable 12-hex digest of the
//     marker, which is fixed-width, distinct across hosts, and stable
//     across restarts of the same host. Less readable than the
//     hostname; correct, which truncation is not.
//   - even the digest form over the ceiling (a prefix that long is not
//     reachable from resolveComponents, but the arithmetic is checked
//     rather than assumed) -> un-suffixed.
func AppNameForHost(prefix string) string {
	h := hostid.HostTag()
	if h == "" {
		return prefix
	}
	if tag := prefix + AppNameHostSep + h; len(tag) <= maxAppNameBytes {
		return tag
	}
	sum := sha256.Sum256([]byte(h))
	if tag := prefix + AppNameHostSep + "h" + hex.EncodeToString(sum[:])[:12]; len(tag) <= maxAppNameBytes {
		return tag
	}
	return prefix
}

// appNamePrefixSQL is the component half of a tag — everything before
// the separator. split_part returns the WHOLE string when the
// separator is absent, so an un-suffixed backend (a pre-round-12
// binary, or a host that cannot name itself) still matches its
// component: that identity is what makes the change additive and the
// upgrade window safe, and it is why the readers match on this rather
// than on `application_name LIKE $1 || '@%'`.
func appNamePrefixSQL(col string) string {
	return fmt.Sprintf(`split_part(%s, '%s', 1)`, col, AppNameHostSep)
}

// appNameHostSQL is the host marker half — NULL when the tag carries
// none, so a missing marker composes as "unknown" rather than as the
// empty string, which would make two un-suffixed backends on DIFFERENT
// hosts compare equal and read as one host.
func appNameHostSQL(col string) string {
	return fmt.Sprintf(`NULLIF(split_part(%s, '%s', 2), '')`, col, AppNameHostSep)
}
