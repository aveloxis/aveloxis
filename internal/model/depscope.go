// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Dependency-scope vocabulary (v0.27.44, summary/19 P0 — operator
// decision #5: FINE storage). The scope rides repo_deps_libyear.type
// and the dependency_scope columns; UI stays coarse (2–3-position
// toggles + per-row badges) and SBOM emission maps scopes to SPDX
// typed relationships / CycloneDX scope.
//
// THE CONTRACT (the P0 hardening): consumers must never branch on a
// literal scope value ("== \"dev\"") — they ask IsRuntimeScope. The
// pre-P0 consumers hard-coded the dev/runtime dichotomy, so the first
// parser to emit "test" or "build" would have silently fallen through
// every dev-handling branch (the Fix C/J wiring-gap class, applied to
// vocabulary instead of classification).

package model

// Dependency scope values. Empty string means "unknown" and is treated
// as runtime everywhere (the safest presentation: an unclassified dep
// counts toward exposure rather than being hidden).
const (
	ScopeRuntime  = "runtime"
	ScopeDev      = "dev"
	ScopeTest     = "test"
	ScopeBuild    = "build"
	ScopeOptional = "optional"
	ScopePeer     = "peer"
)

// NonRuntimeScopes lists every scope that is NOT part of the shipped
// artifact's runtime surface — the set the license table's default
// view and the libyear headline exclude, and the digest gates on.
var NonRuntimeScopes = []string{ScopeDev, ScopeTest, ScopeBuild, ScopeOptional, ScopePeer}

// IsRuntimeScope reports whether a stored scope value counts as
// runtime exposure. "" (unknown/legacy) and "runtime" are runtime;
// everything in NonRuntimeScopes is not. Unrecognized future values
// deliberately read as RUNTIME (fail toward visibility, never toward
// hiding a dependency).
func IsRuntimeScope(scope string) bool {
	switch scope {
	case ScopeDev, ScopeTest, ScopeBuild, ScopeOptional, ScopePeer:
		return false
	default:
		return true
	}
}

// StoredScope normalizes a scope for repo_deps_vulnerabilities'
// dependency_scope column: runtime (including "" and unknown-future
// values) stores as the WORD "runtime" — v0.27.51 operator decision:
// the old "" convention (v0.27.21-50) was uninterpretable for anyone
// reading the table directly. Non-runtime scopes store fine-grained
// (decision #5). Legacy "" rows still READ as runtime everywhere
// (IsRuntimeScope) and heal to 'runtime' on each repo's next scan
// via the always-refresh upsert.
//
// NOTE: repo_lockfile_packages.dependency_scope deliberately KEEPS
// the "" convention — its aggregation fold (MIN(CASE ...)) depends
// on "" sorting before every scope name; the conversion happens at
// the finding-write boundary (vulnScanTargets + the transitive
// target construction), never in the lockfile plumbing.
func StoredScope(scope string) string {
	if IsRuntimeScope(scope) {
		return ScopeRuntime
	}
	return scope
}

// SPDXRelationshipForScope maps a dependency scope to its SPDX 2.3
// relationship (v0.27.46, summary/19 P3). SPDX's typed dependency
// relationships are INVERTED relative to DEPENDS_ON: "A
// DEV_DEPENDENCY_OF B" reads "A is a dev dependency of B", so the
// package is the subject and the root the object. inverted=true
// tells the emitter to swap subject/object accordingly.
//
//   - runtime/unknown → root DEPENDS_ON pkg (the 2.3 baseline)
//   - dev             → pkg DEV_DEPENDENCY_OF root
//   - test            → pkg TEST_DEPENDENCY_OF root
//   - build           → pkg BUILD_DEPENDENCY_OF root
//   - optional        → pkg OPTIONAL_DEPENDENCY_OF root
//   - peer            → pkg PROVIDED_DEPENDENCY_OF root (npm peer
//     semantics: the consumer provides it at runtime — SPDX's
//     to-be-provided relationship is the honest match)
func SPDXRelationshipForScope(scope string) (relType string, inverted bool) {
	switch scope {
	case ScopeDev:
		return "DEV_DEPENDENCY_OF", true
	case ScopeTest:
		return "TEST_DEPENDENCY_OF", true
	case ScopeBuild:
		return "BUILD_DEPENDENCY_OF", true
	case ScopeOptional:
		return "OPTIONAL_DEPENDENCY_OF", true
	case ScopePeer:
		return "PROVIDED_DEPENDENCY_OF", true
	default:
		return "DEPENDS_ON", false
	}
}

// CycloneDXScopeForScope maps a dependency scope to CycloneDX's
// component scope enum (required | optional | excluded):
// runtime/unknown → required (in the deliverable); optional + peer →
// optional (may or may not be present at runtime); dev/test/build →
// excluded (never part of the deliverable).
func CycloneDXScopeForScope(scope string) string {
	switch scope {
	case ScopeOptional, ScopePeer:
		return "optional"
	case ScopeDev, ScopeTest, ScopeBuild:
		return "excluded"
	default:
		return "required"
	}
}
