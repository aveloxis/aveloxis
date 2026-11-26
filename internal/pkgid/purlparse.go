// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "strings"

// TWO purl version rules live in this file, and they answer different
// questions (v0.29.56 — a review asked whether they could be one):
//
//   - SplitVersion — "where would OSV split this?" The receiver's
//     rule: the LAST '@', whatever follows it. Used by WireValid,
//     because what matters at the gate is what the RECEIVER will parse.
//     This is what catches a name region that OSV reads as empty
//     ("purl is missing name", the 2026-09-17 batch failure).
//   - ScopeAwareBase — "where do I replace the version?" Our rule
//     (versionStart): the first '@' that does not begin a path segment.
//     One that does is a scope marker, so "pkg:npm/@scope/name" keeps
//     its name. Used by the two rebuilders and KeyOfPurl; OSV's rule
//     would cut that purl to "pkg:npm/".
//
// They cannot be merged without breaking one of the two.
//
// Known, accepted consequence: a legacy row whose stored purl is already
// mis-split ("pkg:npm/registry.npmjs.org/@jest/transform/26.0.1", the shape
// v0.29.56's lockfile fix stops producing) is REJECTED by the gate as it
// stands, but once a lockfile supplies a resolved version the rebuilt purl
// ends in a real version and passes. It then queries OSV under a name no
// package has, which matches nothing — a wasted query, not a wrong finding
// — and the row heals on the repo's next analysis. Guarding the rebuilders
// on the gate was tried and reverted: ReplaceVersion exists to clean
// exactly these malformed stored purls at read time (v0.27.72, the
// heal-vulnerabilities path), so refusing to rebuild them defeats the heal.
func SplitVersion(purl string) (base, version string) {
	cut := purl
	if i := strings.LastIndexByte(cut, '#'); i >= 0 {
		cut = cut[:i]
	}
	if i := strings.IndexByte(cut, '?'); i >= 0 {
		cut = cut[:i]
	}
	// Both halves come out of `cut`, not the original (v0.29.57): slicing
	// the original handed the qualifiers and subpath back as part of the
	// version ("1.0?repository_url=x#src"), and as part of the base when
	// there was no version at all — the opposite of what stripping them
	// was for.
	at := strings.LastIndexByte(cut, '@')
	if at < 0 {
		return cut, ""
	}
	return cut[:at], cut[at+1:]
}

// ScopeAwareBase returns a purl HEAD (no qualifiers or subpath; see
// setVersion) without its version, treating an '@' that begins a path
// segment as a scope marker. See the note on SplitVersion.
//
// 0.29.208: the version starts at versionStart, which is not "the last '@'
// with no '/' after it". A version may hold a '/' (`dev-feature/x`), and
// such a purl then had no version at all for this function.
func ScopeAwareBase(purl string) string {
	if at := versionStart(purl); at >= 0 {
		return purl[:at]
	}
	return purl
}

// versionStart is THE place a purl head's version begins for this package's
// readers (ScopeAwareBase, KeyOfPurl): the index of the first '@' that does
// not begin a path segment, or -1. head has no qualifiers or subpath; it
// may or may not carry its "pkg:type/" prefix.
//
// An '@' that begins a segment is an npm scope as purls were written before
// the canonical builder ("pkg:npm/@scope/name"); the builder writes that
// one as %40, so in a purl built here the first '@' is always the version's.
// Everything after it is the version, '/' and '@' included.
func versionStart(head string) int {
	for i := 0; i < len(head); i++ {
		if head[i] == '@' && i > 0 && head[i-1] != '/' {
			return i
		}
	}
	return -1
}

// setVersion is the ONE version replacement both rebuilders use (SR-17,
// v0.29.71): the version is replaced in the purl's head — everything before
// the qualifiers ("?") and subpath ("#") — and the tail is kept as written.
// Before, the scope-aware base read the whole purl, so a qualifier value
// with a "/" (repository_url=https://…) left the old version in place and
// appended the new one after the qualifiers, and one with an "@" (a vcs_url
// at a commit) cut the purl inside the qualifier — the defect v0.29.57
// fixed in SplitVersion, which the v0.29.58 review found here too. An
// empty version drops it.
func setVersion(purl, version string) string {
	head, tail := purl, ""
	if i := strings.IndexAny(purl, "?#"); i >= 0 {
		head, tail = purl[:i], purl[i:]
	}
	base := ScopeAwareBase(head)
	if version == "" {
		return base + tail
	}
	return base + "@" + version + tail
}

// WithVersion swaps the version of a purl
// ("pkg:npm/express@4.18.0" → "pkg:npm/express@4.19.2").
//
// The version is escaped as Purl escapes it (0.29.208): the two are the
// only writers of a version, and a raw one here (`1.0@local`) gave a purl
// that Purl would never have built for the same package and version.
func WithVersion(purl, version string) string {
	return ReplaceVersion(purl, version)
}

// WireValid (v0.27.73) is the last-line syntactic gate before a
// purl goes to OSV. The 2026-08-01 heal run failed 81 repos with 400s
// (`invalid URL escape "%i["`, `"purl is missing name"`) because
// legacy dep rows carried RAW pre-v0.27.29 purls — built by string
// concatenation before the canonical escaper — and ONE malformed
// query 400s the repo's ENTIRE batch. OSV's error names only "query
// at index N", so the offending purl is invisible without production
// forensics. Anything failing this gate is dropped and NAMED in the
// log instead of sinking the repo.
//
// Rules (syntactic only — semantic hygiene is manifest.NormalizeParsedVersion's
// job upstream): "pkg:type/…" shape; no whitespace or control bytes;
// every '%' begins a valid two-hex escape; the name region (after the
// type, before any version separator) must be non-blank once %20s are
// discounted.
func WireValid(p string) bool {
	if !strings.HasPrefix(p, "pkg:") {
		return false
	}
	rest := p[len("pkg:"):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return false
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == ' ' || c == '\t' || c < 0x20 {
			return false
		}
		if c == '%' {
			if i+2 >= len(p) || !isHexByte(p[i+1]) || !isHexByte(p[i+2]) {
				return false
			}
		}
	}
	// Parse like the purl spec (and OSV): drop "#subpath" and "?qualifiers",
	// then split the version at the LAST '@'. What remains must be a
	// non-blank name with no empty path segment. v0.29.56: the gate kept an
	// '@' followed by '/' in the name, so a mis-split pnpm lockfile key
	// ("pkg:npm/registry.npmjs.org/@jest/transform/26.0.1") passed here and
	// OSV rejected the repository's whole batch: "purl is missing name".
	nameRegion, _ := SplitVersion(rest[slash+1:])
	if i := strings.LastIndexByte(nameRegion, '#'); i >= 0 {
		nameRegion = nameRegion[:i]
	}
	if i := strings.IndexByte(nameRegion, '?'); i >= 0 {
		nameRegion = nameRegion[:i]
	}
	if strings.Trim(strings.ReplaceAll(nameRegion, "%20", " "), " /") == "" {
		return false
	}
	if strings.HasSuffix(nameRegion, "/") || strings.Contains(nameRegion, "//") {
		return false
	}
	// v0.29.58: the namespace rule at the wire too — legacy dep rows
	// minted before PurlForPackage/analysis applied it still reach here.
	if NamespaceRequired(rest[:slash]) && !NameHasNamespace(nameRegion) {
		return false
	}
	return true
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// ReplaceVersion (v0.27.72) swaps a purl's version segment for a
// NORMALIZED one, or strips it entirely when version is "" (the
// unpinned pathway). This is the scan-side half of the v0.27.71
// version-hygiene fix: stored dep rows keep pre-fix garbage versions
// until each repo's analysis re-runs, so the scan rebuilds purls from
// manifest.NormalizeParsedVersion output at READ time — which is what lets
// `aveloxis heal-vulnerabilities` clean up the malformed-purl false
// positives immediately after deploy. Uses the scope-aware base — see the
// note on SplitVersion for why the gate and the rebuilders split
// differently.
func ReplaceVersion(purl, version string) string {
	if purl == "" {
		return ""
	}
	if version == "" {
		return setVersion(purl, "")
	}
	return setVersion(purl, EscapeSegment(version))
}
