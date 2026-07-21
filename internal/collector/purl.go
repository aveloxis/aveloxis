// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — purl.go: the ONE canonical purl builder
// (v0.27.29). Before this, 13 scattered sites built purls by raw
// concatenation and the tests asserted the non-canonical output
// (pkg:pypi/Flask_SQLAlchemy pinned as correct — the 2026-07-21
// wrong-answer-tests audit's headline). OSV happens to normalize
// server-side (verified live: Pillow/pillow and Flask_SQLAlchemy/
// flask-sqlalchemy return identical results), so no findings were
// lost — but our SBOMs carry these purls to consumers that may NOT
// normalize, and the purl spec is explicit about canonical forms.
//
// Per-type rules implemented (github.com/package-url/purl-spec,
// PURL-TYPES.md — the spec's own test-suite cases are committed at
// testdata/purl_spec_cases.json):
//   - pypi:  lowercase, '_' → '-'
//   - npm, golang, composer, nuget: lowercase
//   - all types: '/'-separated segments percent-encode reserved
//     characters ('@' → %40 — the npm scope case; '%', '?', '#',
//     space); the version component likewise.
package collector

import "strings"

// purlEscapeSegment percent-encodes the characters the purl spec
// reserves inside a namespace/name segment. Deliberately minimal —
// our package names are registry-validated, so only the characters
// that actually collide with purl syntax are handled.
func purlEscapeSegment(s string) string {
	r := strings.NewReplacer(
		"%", "%25", // first — never double-encode
		"@", "%40",
		"?", "%3F",
		"#", "%23",
		" ", "%20",
	)
	return r.Replace(s)
}

// buildPurl assembles a canonical purl. version may be empty
// (self-advisory scanning queries versionless). name may contain '/'
// separators (npm scopes, golang module paths, maven group/artifact
// after ':' folding) — each segment is escaped independently so the
// separators survive.
func buildPurl(typ, name, version string) string {
	if typ == "" || name == "" {
		return ""
	}
	switch typ {
	case "pypi":
		name = strings.ReplaceAll(strings.ToLower(name), "_", "-")
	case "npm", "golang", "composer", "nuget":
		name = strings.ToLower(name)
	}
	segs := strings.Split(name, "/")
	for i, s := range segs {
		segs[i] = purlEscapeSegment(s)
	}
	p := "pkg:" + typ + "/" + strings.Join(segs, "/")
	if version != "" {
		p += "@" + purlEscapeSegment(version)
	}
	return p
}
