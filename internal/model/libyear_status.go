// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

// Why a dependency's inventory row has a libyear, or lacks one (summary/70,
// 4.2). Exactly one applies to a row; collector's libyearStatus is the one
// function that decides which.
const (
	// LibyearResolved: the registry answered and the libyear was computed.
	LibyearResolved = "resolved"
	// LibyearNoVersion: no version could be worked out for the dependency
	// (a constraint with no lockfile is this), so there is nothing to date.
	LibyearNoVersion = "no_version"
	// LibyearNoReleaseDate: the package is known, and the registry gave no
	// date for the version; or the resolver cannot tell a version it does
	// not have from a version it has no date for.
	LibyearNoReleaseDate = "no_release_date"
	// LibyearNotOnRegistry: the registry's definitive answer is that the
	// package does not exist there.
	LibyearNotOnRegistry = "not_on_registry"
	// LibyearCannotBeAsked: the registry cannot be asked about this
	// dependency at all: an invalid coordinate, or a host the resolver does
	// not support.
	LibyearCannotBeAsked = "cannot_be_asked"
	// LibyearLookupFailed: no answer this cycle (a timeout, a rate limit, a
	// server error, an unreadable body). It is asked again next time.
	LibyearLookupFailed = "lookup_failed"
	// LibyearNoTimeline: the ecosystem has no registry timeline to measure
	// against (GitHub Actions). Not a failure; shown as "not applicable".
	LibyearNoTimeline = "no_timeline"
)

// libyearStatusWordings is the one wording of each status, on every surface
// that shows a dependency's libyear (0.29.198; summary/70, 4.8). A status
// with a libyear (resolved) has none: the number is shown.
var libyearStatusWordings = map[string]string{
	LibyearNoVersion:     "no version determined",
	LibyearNoReleaseDate: "no release date for this version",
	LibyearNotOnRegistry: "not on the registry",
	LibyearCannotBeAsked: "the registry cannot be asked about it",
	LibyearLookupFailed:  "lookup failed; it is asked again",
	LibyearNoTimeline:    "not applicable",
}

// LibyearStatusWording is the wording shown where a libyear would be; "" for
// resolved (the number is shown) and for a row with no status recorded.
func LibyearStatusWording(status string) string { return libyearStatusWordings[status] }

// LibyearIsUnknown reports whether a row's libyear is not known and could
// be: it has none, and its status is not "no timeline", the one status that
// means there is nothing to measure. The SQL twin is db.libyearUnknownSQL.
func LibyearIsUnknown(hasLibyear bool, status string) bool {
	return !hasLibyear && status != LibyearNoTimeline
}

// VersionFound is what a registry lookup established about the version a
// dependency declares, kept apart from whether that version has a date. The
// zero value is "not established": the dependency declares no version, or
// the resolver cannot tell (see collector's resolverCanProveVersionMiss).
type VersionFound int

// The three answers.
const (
	VersionNotEstablished VersionFound = iota
	VersionOnRegistry
	VersionNotOnRegistry
)
