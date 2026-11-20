// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/textclean"
)

// The manifest passes (0.29.213): everything an analysis does that READS a
// clone's dependency manifests. (The third pass over a clone, the lockfiles,
// is walkLockfiles in lockfile_scan.go.) They are two walks over the clone,
// one for the dependency names (scanDependencies) and one for the
// declarations with their versions (scanLibyear), and they need nothing but
// the clone, the two knobs, and, for a Maven parent or BOM that is not in
// the clone, the PomSource.
//
// These two functions are those passes. The analysis does not call them: it runs
// them in a child process (manifest_parent.go, manifest_child.go), because
// what they read is written by whoever owns the repository being collected
// and six review rounds each found another way for such text to take a
// reader's time or memory without bound. The child's CPU time and heap are
// limited; this code is not asked to be.

// manifestNames is the first walk's result: language -> name -> count.
type manifestNames map[string]map[string]int

// manifestDeclared is the second walk's result.
type manifestDeclared struct {
	// Deps are the declarations to inventory, each with its manifest's path
	// relative to the clone, after version hygiene and after
	// dependencyManagement entries were taken out of the trees read whole.
	Deps []model.LibyearDep
	// Unused are those entries: notes, not rows.
	Unused []model.LibyearDep
	// Found and Parsed count the manifests claimed and the ones read.
	Found, Parsed int
	Failed        []model.ManifestFailure
	TreeHosts     []model.MavenTreeHosts
	NotWhole      []model.MavenTreeGap
	// Repositories are the package repositories the build files declare.
	Repositories []manifest.DeclaredRepository
	// RemoteNotAsked is how many parent or BOM poms were not asked for
	// because the walk had asked for as many as one analysis may.
	RemoteNotAsked int
}

// errManifestRoot: the clone's root itself could not be read, so nothing was
// visited.
type errManifestRoot struct{ err error }

func (e *errManifestRoot) Error() string { return e.err.Error() }
func (e *errManifestRoot) Unwrap() error { return e.err }

// skippedManifestDir reports whether the walks skip a directory. names is
// the name inventory's walk, which also skips __pycache__ (it always did).
func skippedManifestDir(base string, names bool) bool {
	switch base {
	case "vendor", "node_modules", ".git":
		return true
	case "__pycache__":
		return names
	}
	return false
}

// readManifestNames is the name inventory's walk (v0.29.158: walk first,
// write last).
func readManifestNames(workDir string) (manifestNames, error) {
	depCounts := manifestNames{}
	err := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == workDir {
				// The root itself could not be read: nothing will be
				// visited. Returned, so the previous snapshot stands.
				return err
			}
			return nil // one unreadable entry: skipped, the rest is walked
		}
		// Reject symlinks to prevent traversal attacks. A malicious repo can
		// symlink requirements.txt -> /etc/passwd and have host file contents
		// stored as dependency names or sent to package registries.
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			if skippedManifestDir(filepath.Base(path), true) {
				return filepath.SkipDir
			}
			return nil
		}
		// The roster says which files the name inventory counts and reads
		// them (manifest.Names): a file no kind claims, and one that cannot
		// be read or parsed, contribute nothing.
		lang, deps, matched, err := manifest.Names(path)
		if !matched || err != nil {
			return nil
		}
		if depCounts[lang] == nil {
			depCounts[lang] = make(map[string]int)
		}
		for _, dep := range deps {
			depCounts[lang][dep]++
		}
		return nil
	})
	if err != nil {
		return nil, &errManifestRoot{err}
	}
	return depCounts, nil
}

// readManifestDeclarations is the declarations' walk: every manifest the
// roster claims is read, versions are made storable, and Maven's managed
// entries are settled. src may be nil (nothing outside the clone is asked);
// remoteLimit is how many coordinates the walk may ask it for.
func readManifestDeclarations(workDir string, opts manifest.Options, src manifest.PomSource, remoteLimit int, logger *slog.Logger, repoID int64) (*manifestDeclared, error) {
	out := &manifestDeclared{}
	// One walk per scan: it holds the Cargo workspace index, so every
	// Cargo.toml of a workspace inherits from the same root, read once and
	// never above the clone (v0.29.71).
	walk := manifest.NewWalk(workDir, logger)
	if src != nil {
		// v0.29.182: a Maven parent or BOM that is not in the clone is
		// obtained from the configured Maven repositories, and stored.
		walk.SetPomSource(src)
		walk.SetRemotePomLimit(remoteLimit)
	}
	// v0.27.36: a root-stat failure makes Walk return without visiting
	// anything — silently producing ZERO dependencies for the repo.
	// Per-entry errors are still skipped inside the callback.
	if err := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == workDir {
				// The root itself could not be read: nothing will be
				// visited. Returned, so the phase stops instead of going on
				// with an inventory of nothing.
				return err
			}
			return nil // one unreadable entry: skipped, the rest is walked
		}
		// Reject symlinks to prevent traversal attacks (see readManifestNames).
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			if skippedManifestDir(filepath.Base(path), false) {
				return filepath.SkipDir
			}
			return nil
		}
		// The roster is the dispatch (manifest.Walk.Read): every kind that
		// claims the file reads it, under the two knobs. A file no kind
		// claims is not a manifest.
		read := walk.Read(path, opts)
		if !read.Claimed {
			return nil
		}
		out.Found++
		rel := relativeToClone(workDir, path)
		if textclean.WouldChange(rel) {
			// The path is what a manifest's rows are carried by, and two
			// paths the sanitizer would change could become one. Such a
			// manifest is recorded as unread under its sanitized path and
			// neither contributes rows nor carries any.
			out.Failed = append(out.Failed, model.ManifestFailure{
				Path: textclean.Sanitize(rel), Kind: read.Kind, Reason: model.ManifestFailedRead, NoCarry: true})
			logger.Warn("manifest path cannot be stored unchanged — not read", "kind", read.Kind, "path", textclean.Sanitize(rel))
			return nil
		}
		if f := read.Failure; f != nil {
			// Recorded, not silent: a manifest that contributes nothing is
			// otherwise indistinguishable from a project that dropped its
			// dependencies. It contributes no declarations this cycle; the
			// replace carries its previous rows by this path instead.
			out.Failed = append(out.Failed, model.ManifestFailure{Path: rel, Kind: f.Kind, Reason: f.Reason})
			if f.Reason == model.ManifestFailedParse {
				logger.Warn("failed to parse "+f.Kind+" versions", "path", path, "error", f.Err)
			} else {
				logger.Warn("failed to read manifest", "kind", f.Kind, "path", path, "error", f.Err)
			}
			return nil
		}
		out.Parsed++
		for i := range read.Deps {
			read.Deps[i].ManifestPath = rel
		}
		out.Deps = append(out.Deps, read.Deps...)
		return nil
	}); err != nil {
		return nil, &errManifestRoot{err}
	}

	// v0.27.71: central version-hygiene choke point. Every parser's
	// output passes through here before resolution, storage, and purl
	// construction — a malformed version (line-continuation backslash,
	// property interpolation, inline-table fragment, monorepo
	// protocol) becomes "" (the honest 'unpinned' pathway) instead of
	// silently defeating OSV version matching and registry lookups.
	// See version_normalize.go for the incident history.
	for i := range out.Deps {
		out.Deps[i].Version = manifest.NormalizeParsedVersion(out.Deps[i].Manager, out.Deps[i].Version)
	}

	// v0.29.183 (summary/70, 4.5, D9): a Maven dependencyManagement entry is
	// not a dependency. In a tree read whole it stops being inventoried: an
	// entry whose version a dependency took is represented by that
	// dependency; one whose version was applied to nothing, and a BOM
	// import's own coordinate, becomes a note. A tree that was not read
	// whole keeps its entries as rows, and is named with the reason.
	mavenManaged := walk.SettleMavenManaged(out.Deps)
	out.Deps, out.Unused = mavenManaged.Kept, mavenManaged.Unused
	for _, h := range mavenManaged.TreeHosts {
		out.TreeHosts = append(out.TreeHosts, model.MavenTreeHosts{Root: h.Root, Hosts: h.Hosts})
	}
	for _, g := range mavenManaged.NotWhole {
		out.NotWhole = append(out.NotWhole, model.MavenTreeGap{Root: g.Root, Reason: g.Reason})
	}
	if len(mavenManaged.Unused)+len(mavenManaged.NotWhole) > 0 {
		logger.Info("maven: dependencyManagement entries",
			"repo_id", repoID, "not_inventoried_unused_or_bom_import", len(mavenManaged.Unused),
			"trees_not_read_whole_entries_kept", len(mavenManaged.NotWhole))
	}
	// v0.29.184: the package repositories this repository's build files
	// declare.
	out.Repositories = walk.DeclaredRepositories()
	out.RemoteNotAsked = walk.RemotePomsNotAsked()
	return out, nil
}

// The passes over a clone, as the child is asked for them: the two manifest
// walks above, and the lockfile walk (lockfile_scan.go, walkLockfiles).
const (
	manifestPassNames        = "names"
	manifestPassDeclarations = "declarations"
	manifestPassLockfiles    = "lockfiles"
)

// manifestPassesAll is one analysis's reading of its clone, in the order one
// child does it. The lockfiles are last: they are the pass with no largest
// honest input, so a child stopped at its memory limit by a lockfile has
// already handed over the names and the declarations.
var manifestPassesAll = []string{manifestPassNames, manifestPassDeclarations, manifestPassLockfiles}
