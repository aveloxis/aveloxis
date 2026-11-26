// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import (
	"path"
	"strings"

	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// The source of an inventory row's version (repo_deps_libyear.version_source,
// v0.29.177). Empty means the declaration's own version, as before.
const (
	// SourceLocked: the nearest lockfile in the manifest's ancestry resolves
	// the dependency to exactly one version, and the row reports it.
	SourceLocked = "locked"
	// SourceLockedSeveral: the nearest lockfiles resolve it to more than one
	// version, or two lockfiles sit equally near and both hold it. The row
	// keeps its declared version; the scan still checks every locked one.
	SourceLockedSeveral = "locked-several"
)

// Lockfile is one lockfile of a repository as Nearest reads it: where it is,
// which ecosystem it locks, and the versions it resolves each package to,
// keyed by pkgid.MatchKey(Ecosystem, name).
type Lockfile struct {
	Path      string // relative to the clone's root, '/'-separated
	Ecosystem string
	Versions  map[string][]string
}

// Nearest answers, for a dependency declared in the manifest at
// manifestPath: what does the nearest lockfile say? "Nearest" is the
// lockfile, of the dependency's ecosystem, in the manifest's own directory
// or the closest directory above it. An application's manifest is locked by
// the lockfile beside it (or its workspace root's), not by another
// application's elsewhere in the repository.
//
// It returns the version and SourceLocked when that level resolves the name
// to exactly one version; "" and SourceLockedSeveral when it resolves it to
// several, or when two lockfiles at that level both hold it; and "", "" when
// there is no such lockfile or the nearest ones do not hold the name. In the
// last case nothing is claimed here: the scan's repository-wide match still
// applies, as it always has.
//
// It changes what an inventory row REPORTS, never what the scan targets: the
// scan takes a dependency's targets from the lockfile table whenever the
// repository's lockfiles hold it, whatever version the row carries.
func Nearest(manifestPath, manager, name string, lockfiles []Lockfile) (version, source string) {
	if manager == githubActions || manager == "go" {
		// An action is known by its ref and a Go module by go.mod: no
		// lockfile supplies the version an inventory row reports for
		// either. (Of still classes a Go row with NO version from the
		// lockfile map or as not determined, 0.29.209.)
		return "", ""
	}
	key := pkgid.MatchKey(manager, name)
	dir := path.Dir(filepathSlash(manifestPath))
	for {
		var holding, distinct int
		var found string
		present := false
		for _, lf := range lockfiles {
			if lf.Ecosystem != manager || path.Dir(filepathSlash(lf.Path)) != dir {
				continue
			}
			present = true
			versions := lf.Versions[key]
			if len(versions) == 0 {
				continue
			}
			holding++
			for _, v := range versions {
				if v != found {
					distinct++
					found = v
				}
			}
		}
		if present {
			switch {
			case holding == 0:
				return "", ""
			case holding == 1 && distinct == 1:
				return found, SourceLocked
			default:
				return "", SourceLockedSeveral
			}
		}
		if dir == "." || dir == "/" || dir == "" {
			return "", ""
		}
		dir = path.Dir(dir)
	}
}

// filepathSlash makes a stored relative path '/'-separated whatever host
// wrote it.
func filepathSlash(p string) string {
	return strings.ReplaceAll(p, "\\", "/")
}
