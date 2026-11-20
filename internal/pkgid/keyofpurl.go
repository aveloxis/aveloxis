// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "strings"

// purlTypeEcosystems maps a purl type back to the ecosystem spelling Key
// folds to. It is the inverse of managerPurlTypes after FoldEcosystem.
var purlTypeEcosystems = map[string]string{
	"npm": "npm", "pypi": "pypi", "golang": "go", "cargo": "cargo", "gem": "gem",
	"maven": "maven", "composer": "composer", "hex": "hex", "nuget": "nuget",
	"pub": "pub", "swift": "swift", "hackage": "hackage", "githubactions": "githubactions",
	"cran": "cran",
	// Not a purl type of the specification: this program's own identity
	// for a finding of a package OSV is asked about by name (0.29.206).
	"bioconductor": "bioconductor",
}

// purlUnescape reverses EscapeSegment. '%25' is last so nothing is decoded
// twice.
var purlUnescape = strings.NewReplacer(
	"%40", "@",
	"%3F", "?",
	"%23", "#",
	"%20", " ",
	"%25", "%",
)

// KeyOfPurl returns the package key of a stored purl, and the purl's version
// ("" when it has none). It is the third invariant of this package: for a
// purl built here from a (package manager, name), the key of the purl is
// Key(manager, name) (TestKeyOfPurlIsTheKeyOfItsRow, over the production
// sample).
//
// It lets a finding, which stores only its purl, be compared with the scan's
// targets by PACKAGE rather than by exact purl: "is this the same package at
// another version, or with no version?" (the closing rule's reasons,
// summary/70 section 3).
//
// ok is false for a string that is not a purl of a type this package builds.
//
// Two things the key is not. It is not a perfect package identity: a SwiftPM
// purl's namespace (the repository's host and owner) is outside the key, as
// it is outside the key of a lockfile row, so two packages of one name under
// different owners share a key; and the key folds case. A reason chosen by
// key can therefore mislabel across two such packages, which is accepted
// because a reason closes nothing.
func KeyOfPurl(purl string) (key, version string, ok bool) {
	rest, isPurl := strings.CutPrefix(purl, "pkg:")
	if !isPurl {
		return "", "", false
	}
	typ, afterType, hasName := strings.Cut(rest, "/")
	eco, known := purlTypeEcosystems[typ]
	if !hasName || !known {
		return "", "", false
	}
	// Qualifiers and a subpath are not part of the name or the version.
	if i := strings.IndexAny(afterType, "?#"); i >= 0 {
		afterType = afterType[:i]
	}
	name := afterType
	// The version begins where every reader here says it does
	// (versionStart): a version may hold a '/'.
	if strings.HasPrefix(afterType, "@") && !strings.Contains(afterType, "/") {
		return "", "", false // a version with no name ("pkg:npm/@1.0.0")
	}
	if at := versionStart(afterType); at >= 0 {
		name, version = afterType[:at], purlUnescape.Replace(afterType[at+1:])
	}
	if name == "" {
		return "", "", false
	}
	name = purlUnescape.Replace(name)
	switch typ {
	case "maven":
		// group/artifact in the purl, group:artifact in the row's name.
		name = strings.Replace(name, "/", ":", 1)
	case "swift":
		// The namespace is outside the key: the row's name is the package's
		// own name.
		if i := strings.LastIndexByte(name, '/'); i >= 0 {
			name = name[i+1:]
		}
	}
	return Key(eco, name), version, true
}
