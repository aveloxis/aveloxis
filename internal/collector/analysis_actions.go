// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// GitHub Actions dependency inventory + OSV matching (v0.27.47,
// summary/19 P4 — operator decision #2). Workflow `uses:` references
// are BUILD dependencies of the repository (numpy's 25-action set is
// real supply-chain surface: the tj-actions/changed-files compromise
// class). Rows land in repo_deps_libyear with
// manager='githubactions', type='build', libyear NULL (Actions have
// no registry timeline — NoLibyear keeps the snapshot honest).
//
// OSV matching — the 2026-07-21 live spike's verdict, pinned by the
// AVELOXIS_TEST_NETWORK canary:
//
//   - purl queries (pkg:githubactions/...) return NOTHING: OSV's
//     "GitHub Actions" advisories carry ecosystem+name only, no purl.
//   - VERSIONED {name, ecosystem} queries also return nothing — the
//     API has no version comparator for this ecosystem.
//   - VERSIONLESS {name, ecosystem:"GitHub Actions"} works (the
//     v0.27.29 self-advisory shape), returning every advisory ever.
//
// So the scan queries versionless and evaluates the pinned ref
// against each advisory's ECOSYSTEM ranges CLIENT-SIDE
// (actionRefAffected). Refs that cannot be evaluated (commit SHAs)
// produce NO findings — over-attribution is misinformation (the
// numpy lesson); a SHA pin is also the ecosystem's recommended
// mitigation posture.
//
// Gated on collection.github_actions_deps (its own knob — a new
// ecosystem, not a scope refinement; default FALSE until the small-DB
// canary sizes the wave).

package collector

import (
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// actionRefAffected evaluates a pinned action ref against an
// advisory's ECOSYSTEM ranges for the given action name. The policy,
// derived from how GitHub Actions tags actually behave:
//
//   - commit SHA → false (cannot be compared to version events; a
//     claim either way would be fabricated).
//   - full version tag (contains '.') → affected iff some range has
//     introduced ≤ v AND (no fixed event OR v < fixed). Standard OSV
//     event semantics.
//   - major-only tag (v4) → the tag FLOATS within its major: it
//     auto-heals when a fix lands in the same major, but a fix in a
//     LATER major never reaches it. Affected iff no fixed event
//     shares the pinned major (and the range's introduced major ≤
//     pinned major).
//   - non-numeric ref (branch names: main) → floats to latest:
//     affected only when the advisory has NO fix at all.
func actionRefAffected(ref string, affected []osvAffected, actionName string) bool {
	if versionuse.IsHexSHA(ref) {
		return false
	}
	v := strings.TrimPrefix(ref, "v")
	numeric := len(v) > 0 && v[0] >= '0' && v[0] <= '9'
	fullVersion := numeric && strings.Contains(v, ".")
	for _, aff := range affected {
		if !strings.EqualFold(aff.Package.Ecosystem, osvActionsEcosystem) ||
			!strings.EqualFold(aff.Package.Name, actionName) {
			continue
		}
		for _, rng := range aff.Ranges {
			if rng.Type != "ECOSYSTEM" && rng.Type != "SEMVER" {
				continue
			}
			introduced, fixed := "", ""
			for _, ev := range rng.Events {
				if ev.Introduced != "" {
					introduced = ev.Introduced
				}
				if ev.Fixed != "" {
					fixed = ev.Fixed
				}
			}
			switch {
			case fullVersion:
				if (introduced == "" || introduced == "0" || compareVersionish(v, introduced) >= 0) &&
					(fixed == "" || compareVersionish(v, fixed) < 0) {
					return true
				}
			case numeric: // major-only tag
				major, _ := strconv.Atoi(strings.SplitN(v, ".", 2)[0])
				introMajor := 0
				if introduced != "" && introduced != "0" {
					introMajor, _ = strconv.Atoi(strings.SplitN(strings.TrimPrefix(introduced, "v"), ".", 2)[0])
				}
				fixedInMajor := false
				if fixed != "" {
					fm, _ := strconv.Atoi(strings.SplitN(strings.TrimPrefix(fixed, "v"), ".", 2)[0])
					fixedInMajor = fm == major
				}
				if introMajor <= major && !fixedInMajor && (fixed == "" || !fixedWithinOrBeforeMajor(fixed, major)) {
					return true
				}
			default: // branch name — floats to latest
				if fixed == "" {
					return true
				}
			}
		}
	}
	return false
}

// fixedWithinOrBeforeMajor reports whether the fix version's major is
// at or below the pinned major — meaning the floating major tag has
// (or will) pick the fix up.
func fixedWithinOrBeforeMajor(fixed string, major int) bool {
	fm, err := strconv.Atoi(strings.SplitN(strings.TrimPrefix(fixed, "v"), ".", 2)[0])
	if err != nil {
		return false
	}
	return fm <= major
}

// compareVersionish is db.CompareVersionish, the one semantic-ish version
// order (moved there so the store's exposed-repositories list can sort by
// it too).
func compareVersionish(a, b string) int { return db.CompareVersionish(a, b) }
