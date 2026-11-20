// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"regexp"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// R (summary/70, 4.9; 0.29.204). An R package states what it needs in its
// DESCRIPTION, a file of "Field: value" lines in the Debian control format:
// a value continues on the lines after it that begin with white space.

// rBasePackages are R itself and the packages that ship with every R and
// are not on a registry ("base" priority). A DESCRIPTION names them like
// any other dependency; they are filtered here, as Composer's platform
// names (php, ext-json) are in its reader. The "recommended" packages
// (MASS, Matrix, survival, ...) are on CRAN and are kept.
var rBasePackages = map[string]bool{
	"R": true, "base": true, "compiler": true, "datasets": true, "grDevices": true, "graphics": true,
	"grid": true, "methods": true, "parallel": true, "splines": true, "stats": true, "stats4": true,
	"tcltk": true, "tools": true, "utils": true,
}

// IsRBasePackage reports whether name is R itself or a package that ships
// with every R: not a package of any registry.
func IsRBasePackage(name string) bool { return rBasePackages[name] }

// rDependencyFields are the fields that name dependencies, with the scope
// of each in the one scope vocabulary: what the package needs to be loaded
// or built against is runtime; what it can use when present is optional.
var rDependencyFields = map[string]string{
	"Depends": "runtime", "Imports": "runtime", "LinkingTo": "runtime",
	"Suggests": "optional", "Enhances": "optional",
}

// rFieldLine is a line that starts a field: a name at the start of the
// line, then a colon. Field names are case-sensitive in R.
var rFieldLine = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9@/._-]*):(.*)$`)

// rEntry is one entry of a dependency field: a package name, then
// optionally a version constraint in parentheses.
var rEntry = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9.]*[A-Za-z0-9])\s*(?:\(\s*(>=|==|<=|!=|>|<)\s*([^\s()]+)\s*\))?$`)

// rDescriptionFields reads the file into its fields, each value joined
// across its continuation lines. A '#' line is a comment.
func rDescriptionFields(content string) map[string]string {
	fields := map[string]string{}
	current := ""
	for _, line := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "#"):
		case line != "" && (line[0] == ' ' || line[0] == '\t'):
			if current != "" {
				fields[current] += " " + strings.TrimSpace(line)
			}
		default:
			current = ""
			if m := rFieldLine.FindStringSubmatch(line); m != nil {
				current = m[1]
				fields[current] = strings.TrimSpace(m[2])
			}
		}
	}
	return fields
}

// rRemoteNames are the package names a Remotes field says come from
// somewhere that is not a registry. An entry is [name=][type::]path[@ref],
// path being owner/repository[/more] for the forges. The name is the one
// given before "=", or, by the convention the remotes package relies on,
// the repository's: its second path element, and for a forge with nested
// groups its last. A dependency with one of these names is not the registry
// package of that name (SR-6).
func rRemoteNames(field string) map[string]bool {
	names := map[string]bool{}
	for _, entry := range strings.Split(field, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		given := ""
		if name, rest, ok := strings.Cut(entry, "="); ok && !strings.Contains(name, "/") && !strings.Contains(name, ":") {
			given = strings.TrimSpace(name)
			entry = strings.TrimSpace(rest)
		}
		kind := ""
		if k, path, ok := strings.Cut(entry, "::"); ok {
			kind, entry = strings.ToLower(strings.TrimSpace(k)), path
		}
		// The name given before "=" is marked with its entry's kind
		// (0.29.209): it was marked before the kind was read, so
		// `pkg=cran::pkg` took a CRAN package out of the checks.
		if given != "" && kind != "cran" && kind != "standard" {
			names[given] = true
		}
		switch kind {
		case "cran", "standard":
			// pak's spelling of "from the registry": it IS the registry's
			// package, and saying otherwise would take a real CRAN
			// dependency out of the checks (review round 1).
			continue
		case "git":
			// A git remote is an address: a URL, or user@host:path. Its
			// package is, by the same convention, the repository's name:
			// the last path element, without ".git" or a ref after it.
			// (The user of user@host:path was read as the name.)
			if name := rGitRemoteName(entry); name != "" {
				names[name] = true
			}
			continue
		}
		if strings.Contains(entry, "://") {
			continue // a URL: only a name given before "=" names the package
		}
		if i := strings.IndexAny(entry, "@#"); i >= 0 {
			entry = entry[:i]
		}
		parts := strings.Split(strings.Trim(entry, "/"), "/")
		if len(parts) >= 2 {
			names[parts[1]] = true
		}
		names[parts[len(parts)-1]] = true
	}
	return names
}

// rGitRemoteName is the repository name of a git remote's address:
// https://host/team/baz.git[@ref], ssh://git@host/owner/baz.git,
// git@host:owner/baz.git. Empty when there is none to read.
func rGitRemoteName(address string) string {
	address = strings.TrimSpace(address)
	if _, rest, ok := strings.Cut(address, "://"); ok {
		address = rest
	} else if _, path, ok := strings.Cut(address, ":"); ok {
		address = path // user@host:path
	}
	parts := strings.Split(strings.Trim(address, "/"), "/")
	last := parts[len(parts)-1]
	if i := strings.IndexAny(last, "@#"); i >= 0 {
		last = last[:i]
	}
	return strings.TrimSuffix(last, ".git")
}

// parseRDescriptionVersions reads the dependencies an R package's
// DESCRIPTION declares. A file with no Package field is not an R package's
// DESCRIPTION (the name is a common one) and declares nothing.
//
// A constraint gives a version when it has a floor or is a pin: (>= v) and
// (> v) are the lowest version allowed, (== v) the version. An upper bound
// or an exclusion ((<= v), (< v), (!= v)) says nothing about the version in
// use: the entry keeps its requirement as written and has no version, and a
// dependency with no version is "version not determined" (versionuse.Of).
func parseRDescriptionVersions(content string) []model.LibyearDep {
	fields := rDescriptionFields(content)
	if fields["Package"] == "" {
		return nil
	}
	remote := rRemoteNames(fields["Remotes"])
	var deps []model.LibyearDep
	at := map[string]int{}
	// In a fixed order, what is needed first: a package both needed and
	// suggested is needed.
	for _, field := range []string{"Depends", "Imports", "LinkingTo", "Suggests", "Enhances"} {
		for _, entry := range strings.Split(fields[field], ",") {
			m := rEntry.FindStringSubmatch(strings.TrimSpace(entry))
			if m == nil || rBasePackages[m[1]] {
				continue
			}
			dep := model.LibyearDep{Name: m[1], Type: rDependencyFields[field], Manager: "cran", NonRegistry: remote[m[1]]}
			if m[2] != "" {
				dep.Requirement = strings.TrimSpace(strings.TrimSuffix(strings.SplitN(strings.TrimSpace(entry), "(", 2)[1], ")"))
			}
			switch m[2] {
			case ">=", ">", "==":
				dep.Version = m[3]
			}
			if i, seen := at[dep.Name]; seen {
				// Named again (Imports and LinkingTo both name Rcpp): one
				// dependency, with the version of whichever entry gives one.
				if deps[i].Version == "" && dep.Version != "" {
					deps[i].Version, deps[i].Requirement = dep.Version, dep.Requirement
				}
				continue
			}
			at[dep.Name] = len(deps)
			deps = append(deps, dep)
		}
	}
	return deps
}
