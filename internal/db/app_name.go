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
// the fix meant to prevent it. The composer therefore never truncates
// a marker; it digests it.
const maxAppNameBytes = 63

// MaxHostMarkerBytes is how many bytes a host marker may occupy, and
// it is DERIVED, not chosen: maxAppNameBytes minus the longest
// component prefix (`aveloxis-scancode-worker`, 24 bytes) minus the
// separator. 63 - 24 - 1 = 38.
//
// The budget has to come from the longest prefix because the marker is
// PREFIX-INDEPENDENT, and that is a hard requirement rather than a
// tidiness preference: checkBlockersFrom matches
// `application_name LIKE 'aveloxis-%'` across EVERY component prefix
// while passing a single marker parameter, so a marker sized per
// prefix would be wrong for every prefix but one.
//
// cmd/aveloxis proves the arithmetic against the actual component list
// (TestEveryComponentPrefixFitsTheHostMarkerBudget) — internal/db
// cannot see that list, so a future component with a longer name fails
// the build there rather than silently shortening every host's marker
// here.
const MaxHostMarkerBytes = 38

// HostMarker is this host's marker: the half of `application_name`
// after the separator, and the value every reader passes as the
// probing session's `me.host_tag`. It is the READING half of the SR-17
// pair whose tagging half is AppNameForHost — one function computes
// the marker, both halves compose it.
//
// v0.29.4 round 14 (Copilot round 4, finding 1) is why this exists as
// a named function rather than as arithmetic inside the tag composer.
// Round 12 gave the composer a digest branch for hostnames that would
// not fit the ceiling — correct in itself — but no reader ever computed
// that digest: backendsByAppNameFrom, checkBlockersFrom and
// otherServeAddressesFrom each passed the RAW hostid.HostTag(). On any
// host whose name exceeds the budget the two halves disagreed, both
// markers were non-NULL and unequal, and sameHostAsProbeSQL's veto
// fired against THIS HOST'S OWN backends. `aveloxis stop` then saw an
// empty ThisHost and returned instantly without waiting out the drain
// (the round-8 finding-2 failure, reintroduced through a new path),
// printed the "running somewhere else" note for its own pool, and the
// blocker watcher denied this host's own lock holders their terminate
// recipe. Reachable on ordinary cloud hosts: AWS EC2's default private
// DNS name is 39 bytes.
//
// Returning "" is the honest degradation for a host that cannot name
// itself: the readers then pass NULL, sameHostAsProbeSQL's veto
// abstains, and the verdict is the pre-v0.29.4 address rule.
func HostMarker() string { return hostMarkerFor(hostid.HostTag()) }

// hostMarkerFor is HostMarker with the hostname supplied, so the
// tag/reader agreement is provable without a long-named machine.
//
// Two degradations, both toward the pre-v0.29.4 verdict rather than
// toward a wrong one:
//
//   - no hostname -> "", readers fall back to the address rule.
//   - over the budget -> a stable 12-hex digest of the hostname, which
//     is fixed-width, distinct across hosts, and stable across restarts
//     of the same host. Less readable than the hostname; correct, which
//     truncation is not.
func hostMarkerFor(h string) string {
	if h == "" {
		return ""
	}
	if len(h) <= MaxHostMarkerBytes {
		return h
	}
	sum := sha256.Sum256([]byte(h))
	return "h" + hex.EncodeToString(sum[:])[:12]
}

// AppNameForHost composes the application_name a component tags its
// pool with: the component's match prefix (`aveloxis-serve`) plus this
// host's marker. It is the TAGGING half of the SR-17 pair whose
// reading half is HostMarker + appNamePrefixSQL / appNameHostSQL — all
// of them must agree on the separator and on the marker, so they live
// together here.
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
// The marker is NOT host identity either, and round 13 stopped
// treating it as such: it can only VETO the address rule, never
// override it (see sameHostAsProbeSQL). That is why the degradations
// are safe to reason about locally — a marker this composer got wrong,
// or one two hosts happen to share, costs at worst the pre-v0.29.4
// verdict.
func AppNameForHost(prefix string) string {
	return appNameFromMarker(prefix, HostMarker())
}

// appNameForHostWith is AppNameForHost with the hostname supplied, so
// the tag/reader agreement is provable without a long-named machine.
// It composes through the SAME two helpers the production path uses,
// which is what makes it a seam rather than a second implementation.
func appNameForHostWith(prefix, h string) string {
	return appNameFromMarker(prefix, hostMarkerFor(h))
}

// appNameFromMarker is the ARITHMETIC BACKSTOP, and it is all that is
// left of the per-prefix sizing: the marker arrives already computed
// (prefix-independently) and is never recomputed here — that is round
// 14's whole point. A prefix so long that even a budget-sized marker
// will not fit tags un-suffixed rather than truncating; the row's
// marker then reads NULL, sameHostAsProbeSQL's veto abstains, and the
// verdict degrades to the address rule. Not reachable from
// resolveComponents (the cmd-side tripwire proves the arithmetic), but
// checked rather than assumed, because a truncated marker is the one
// failure mode that turns two hosts into one.
func appNameFromMarker(prefix, marker string) string {
	if marker == "" {
		return prefix
	}
	if tag := prefix + AppNameHostSep + marker; len(tag) <= maxAppNameBytes {
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
// empty string.
//
// NULL is what makes the marker ABSTAIN in sameHostAsProbeSQL's veto
// (round 13). Without it an un-suffixed backend would carry the empty
// string, which no marked probe can equal, so every pre-round-12
// backend on this host would be vetoed into OtherHosts — `aveloxis stop` would stop
// reporting a pre-round-12 serve's orphans and stop waiting out their
// drain. Under round 12's marker-DECIDES form the same mutation broke
// the opposite way (two un-marked hosts comparing equal and reading as
// one); the guard is load-bearing under both, for different reasons,
// and TestHostMarkerBeatsCollapsedClientAddress catches it either way.
func appNameHostSQL(col string) string {
	return fmt.Sprintf(`NULLIF(split_part(%s, '%s', 2), '')`, col, AppNameHostSep)
}
