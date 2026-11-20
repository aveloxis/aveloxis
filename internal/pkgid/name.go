// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "regexp"

// The name grammar: whether a string can be the name of a package in an
// ecosystem at all.
//
// Until 0.29.164 the registry was the de facto name validator: a dependency
// had an inventory row only when its registry answered for it, so a line of
// prose, a template placeholder or a YAML fragment that a manifest reader
// took for a name simply got no row. Since 0.29.162 a dependency keeps its
// row when the registry cannot answer, so something else has to say "this is
// not a name". The grammar does, per ecosystem, from the ecosystem's own
// naming rules; and it is deliberately permissive within them, because its
// job is to refuse garbage, not to police style.
//
// It was checked, before it shipped, against every (package manager, name)
// that had an inventory row on both production databases on 2026-11-19:
// 156,490 names. It accepts all but seventeen, each of which is listed and
// explained in name_test.go. A change that makes it refuse a name that has a
// row is its own step, red first, listing the names it drops.

// MaxNameBytes is the longest name the grammar accepts, in bytes: the
// largest value a PostgreSQL B-tree index entry can hold on the default 8 kB
// page (its own error names the figure: "index row size ... exceeds btree
// version 4 maximum 2704"). A name longer than that cannot be a key in any
// index, whatever the ecosystem allows. The longest production name was 120
// bytes.
const MaxNameBytes = 2704

// nameGrammars holds one pattern per package manager, as the libyear
// writers spell it. Capitals are accepted throughout: several ecosystems
// fold case and all have older packages that predate their lower-case rule.
var nameGrammars = map[string]*regexp.Regexp{
	// An optional @scope/, then the name; npm's own character set.
	"npm": regexp.MustCompile(`^(@[A-Za-z0-9~-][A-Za-z0-9._~-]*/)?[A-Za-z0-9~-][A-Za-z0-9._~-]*$`),
	// PEP 508: letters and digits, with '.', '_' and '-' inside.
	"pypi":  regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`),
	"cargo": regexp.MustCompile(`^[A-Za-z0-9_-]+$`),
	// A module path: slash-separated elements of the proxy protocol's set.
	"go": regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~-]*(/[A-Za-z0-9._~-]+)*$`),
	// groupId:artifactId, both present.
	"maven":    regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*:[A-Za-z0-9_][A-Za-z0-9._-]*$`),
	"nuget":    regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`),
	"rubygems": regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`),
	// vendor/package, both present.
	"packagist": regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?/[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`),
	"pub":       regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`),
	"hex":       regexp.MustCompile(`^[a-z][a-z0-9_]*$`),
	"hackage":   regexp.MustCompile(`^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$`),
	"swiftpm":   regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`),
	// "Writing R Extensions": ASCII letters, digits and dots; at least two
	// characters; starts with a letter and does not end in a dot.
	"cran": regexp.MustCompile(`^[A-Za-z][A-Za-z0-9.]*[A-Za-z0-9]$`),
	// owner/repository, as the workflow reader attributes an action.
	"githubactions": regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$`),
}

// ValidName reports whether name can be the name of a package of this
// package manager: it matches the manager's grammar and is no longer than
// MaxNameBytes. A manager with no grammar has no valid names; every manager
// the inventory writes has one (TestEveryInventoryManagerHasAGrammar).
func ValidName(manager, name string) bool {
	if len(name) > MaxNameBytes {
		return false
	}
	g, ok := nameGrammars[manager]
	return ok && g.MatchString(name)
}
