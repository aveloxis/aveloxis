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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// isWorkflowPath reports whether path is a GitHub Actions workflow
// file (.github/workflows/*.yml|yaml).
func isWorkflowPath(path string) bool {
	dir := filepath.ToSlash(filepath.Dir(path))
	if !strings.HasSuffix(dir, ".github/workflows") {
		return false
	}
	return strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")
}

// parseWorkflowUses extracts action references from a workflow file.
// `uses: owner/repo@ref` and `uses: owner/repo/subdir@ref` both
// attribute to owner/repo (the repository carries the advisories);
// local (./...) and docker:// references are skipped — they are not
// marketplace actions.
func parseWorkflowUses(content string) []libyearDep {
	var deps []libyearDep
	seen := map[string]bool{}
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimPrefix(trimmed, "- ")
		if !strings.HasPrefix(trimmed, "uses:") {
			continue
		}
		ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "uses:"))
		// Strip trailing comments and quotes.
		if idx := strings.Index(ref, "#"); idx >= 0 {
			ref = strings.TrimSpace(ref[:idx])
		}
		ref = strings.Trim(ref, `"' `)
		if ref == "" || strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "docker://") {
			continue
		}
		name, version, found := strings.Cut(ref, "@")
		if !found || name == "" || version == "" {
			continue
		}
		// Subdirectory actions attribute to the owning repository.
		parts := strings.Split(name, "/")
		if len(parts) < 2 {
			continue
		}
		name = parts[0] + "/" + parts[1]
		key := name + "@" + version
		if seen[key] {
			continue
		}
		seen[key] = true
		deps = append(deps, libyearDep{
			Name: name, Version: version, Requirement: ref,
			Type: model.ScopeBuild, Manager: "githubactions",
		})
	}
	return deps
}

// classifyActionRef maps a pinned ref onto the version_resolution
// vocabulary: full commit SHAs are 'locked' (immutable), dotted
// version tags are 'exact', and floating refs (major-only tags,
// branch names) are 'unpinned'.
func classifyActionRef(ref string) string {
	if isHexSHA(ref) {
		return resolutionLocked
	}
	if strings.Contains(ref, ".") {
		return resolutionExact
	}
	return resolutionUnpinned
}

// isHexSHA reports whether ref looks like a git commit SHA
// (7–40 lowercase hex chars — the abbreviated-through-full range).
func isHexSHA(ref string) bool {
	if len(ref) < 7 || len(ref) > 40 {
		return false
	}
	for _, c := range ref {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

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
	if isHexSHA(ref) {
		return false
	}
	v := strings.TrimPrefix(ref, "v")
	numeric := len(v) > 0 && v[0] >= '0' && v[0] <= '9'
	fullVersion := numeric && strings.Contains(v, ".")
	for _, aff := range affected {
		if !strings.EqualFold(aff.Package.Ecosystem, "GitHub Actions") ||
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

// compareVersionish compares two dotted numeric versions segment-wise
// (-1/0/1). Non-numeric segments compare lexically; missing segments
// read as 0 ("4.1" == "4.1.0").
func compareVersionish(a, b string) int {
	as := strings.Split(strings.TrimPrefix(a, "v"), ".")
	bs := strings.Split(strings.TrimPrefix(b, "v"), ".")
	n := len(as)
	if len(bs) > n {
		n = len(bs)
	}
	for i := 0; i < n; i++ {
		av, bv := "0", "0"
		if i < len(as) {
			av = as[i]
		}
		if i < len(bs) {
			bv = bs[i]
		}
		ai, aerr := strconv.Atoi(av)
		bi, berr := strconv.Atoi(bv)
		if aerr == nil && berr == nil {
			if ai != bi {
				if ai < bi {
					return -1
				}
				return 1
			}
			continue
		}
		if av != bv {
			return strings.Compare(av, bv)
		}
	}
	return 0
}
