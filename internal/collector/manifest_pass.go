// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"

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
		if cloneFileTooLarge(info.Size()) {
			return nil // as a file that cannot be read: it contributes nothing
		}
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
		rel := relativeToClone(workDir, path)
		if cloneFileTooLarge(info.Size()) {
			// Not read at all: the file alone is larger than the memory
			// this process has left to it (0.29.214; until then it was read
			// whole between two looks at the limit, and the limit was
			// passed by the size of the file). If it is a manifest it is
			// one that could not be read, and the rest are read.
			// Whether it is one is asked of the roster by the file's path
			// alone (manifest.DeclaredKind): nothing is opened.
			kind, claimed := manifest.DeclaredKind(path, opts)
			if !claimed {
				return nil
			}
			out.Found++
			logger.Warn("manifest is larger than the memory left to read it in (collection.manifest_read_memory_mb) — not read",
				"kind", kind, "path", textclean.Sanitize(rel), "bytes", info.Size())
			if !wirePathOK(rel) {
				out.Failed = append(out.Failed, model.ManifestFailure{Path: storablePath(rel), Kind: kind, Reason: model.ManifestFailedRead, NoCarry: true})
				return nil
			}
			out.Failed = append(out.Failed, model.ManifestFailure{Path: rel, Kind: kind, Reason: model.ManifestFailedRead})
			return nil
		}
		read := walk.Read(path, opts)
		if !read.Claimed {
			return nil
		}
		out.Found++
		if textclean.WouldChange(rel) {
			// The path is what a manifest's rows are carried by, and two
			// paths the sanitizer would change could become one. Such a
			// manifest is recorded as unread under its sanitized path and
			// neither contributes rows nor carries any.
			out.Failed = append(out.Failed, model.ManifestFailure{
				Path: storablePath(rel), Kind: read.Kind, Reason: model.ManifestFailedRead, NoCarry: true})
			logger.Warn("manifest path cannot be stored unchanged — not read", "kind", read.Kind, "path", storablePath(rel))
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
		if item := declarationTooLarge(read.Deps); item != "" {
			// One declaration too long to hand over (manifestItemLimit):
			// nothing could store it. The manifest failed, by name.
			out.Failed = append(out.Failed, model.ManifestFailure{Path: rel, Kind: read.Kind, Reason: model.ManifestFailedParse})
			logger.Warn("manifest holds a declaration too long to store — not read", "kind", read.Kind, "path", rel, "declaration", item)
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
	// A tree's root, and a declared repository's manifest, are paths of
	// files the walk read on its own (a parent pom): held to the same form
	// as every other path of the pass.
	for _, h := range mavenManaged.TreeHosts {
		out.TreeHosts = append(out.TreeHosts, model.MavenTreeHosts{Root: storablePath(h.Root), Hosts: h.Hosts})
	}
	for _, g := range mavenManaged.NotWhole {
		out.NotWhole = append(out.NotWhole, model.MavenTreeGap{Root: storablePath(g.Root), Reason: g.Reason})
	}
	if len(mavenManaged.Unused)+len(mavenManaged.NotWhole) > 0 {
		logger.Info("maven: dependencyManagement entries",
			"repo_id", repoID, "not_inventoried_unused_or_bom_import", len(mavenManaged.Unused),
			"trees_not_read_whole_entries_kept", len(mavenManaged.NotWhole))
	}
	// v0.27.45 (summary/19 P2, Go C1): go.mod deps whose packages are
	// imported ONLY from _test.go files are test-scope. Ungated: a relabel,
	// which adds no row. Here, in the pass, because it reads the clone's .go
	// files (0.29.214).
	out.Deps = classifyGoModTestOnlyDeps(workDir, out.Deps)
	// v0.29.184: the package repositories this repository's build files
	// declare.
	out.Repositories = walk.DeclaredRepositories()
	for i := range out.Repositories {
		out.Repositories[i].ManifestPath = storablePath(out.Repositories[i].ManifestPath)
	}
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

// cloneReadHeapLimit is the memory limit of the process reading a clone,
// when this process is the child that does (ManifestChildMain sets it); 0 in
// any other process, where no file is too large.
var cloneReadHeapLimit atomic.Int64

// cloneFileTooLarge reports whether a file of the clone is not to be read
// because of its size alone: it is larger than what the heap limit leaves
// this process (the limit less the heap now, after a collection when it
// looks too small).
//
// The watchdog looks at the heap every manifestWatchEvery, and a file is
// read in one call between two looks, so before 0.29.214 the limit was
// passed by the size of whatever file the repository held: a 6 GiB file of
// zeros named requirements.txt, a few megabytes in git, took the child to
// 2 GiB resident at a 256 MiB limit and at a 1,024 MiB one alike (review
// round 7). A file's size is known before it is read. With this, what the
// child holds is at most its limit, one file-reading step past it by what
// the readers allocate in one manifestWatchEvery while they parse (measured
// at about a gigabyte a second for the densest, so a tenth of a gibibyte),
// and what the Go runtime keeps beyond its heap objects.
func cloneFileTooLarge(size int64) bool {
	limit := cloneReadHeapLimit.Load()
	if limit <= 0 {
		return false
	}
	if size <= limit-processHeapBytes() {
		return false
	}
	runtime.GC()
	return size > limit-processHeapBytes()
}

// cloneReadRoom is how many bytes one read of a file of the clone may take
// in this process: what the heap limit leaves now. It is the bound the
// manifest package's own reads are held to (manifest.SetReadBound, installed
// by ManifestChildMain), so that a file a reader opens for itself (a crate's
// workspace root, a build's parent pom), which the walk's check above never
// sees, and a file that grew after that check, stop at the same line. It is
// the rule of cloneFileTooLarge, and like it collects before it refuses a
// file of the given size (-1: not known): the answer for one file must not
// change between the roster's read of it and its reader's a moment later
// because of what the first read left to be collected. 0 in a process with
// no limit, which never installs it.
func cloneReadRoom(size int64) int64 {
	limit := cloneReadHeapLimit.Load()
	if limit <= 0 {
		return 0
	}
	room := limit - processHeapBytes()
	if room <= 0 || size > room {
		runtime.GC()
		room = limit - processHeapBytes()
	}
	return room
}

// manifestItemLimit is the most text one item of a result may hold: a
// declaration, a name, a lockfile's entry or edge. It is the size the child
// cuts a result's pieces at (manifestPartTarget), so that a piece and one
// more item always fit a frame, whatever the item's bytes cost to encode
// (six for one, at worst). An item past it is text nothing downstream
// stores (a "dependency name" of megabytes); the file it stands in is
// recorded as one that could not be read, by name. Until 0.29.214 such an
// item made the child exit with the protocol status and say nothing.
const manifestItemLimit = int(manifestPartTarget)

// declarationTooLarge names a declaration longer than manifestItemLimit
// ("" when none is).
func declarationTooLarge(deps []model.LibyearDep) string {
	for _, d := range deps {
		if libyearDepBytes(d) > manifestItemLimit {
			name := textclean.Sanitize(d.Name)
			if len(name) > 80 {
				name = name[:80] + "…"
			}
			return name
		}
	}
	return ""
}

// wirePathOK reports whether p is a path inside the clone in the one form
// the passes write: relative, '/'-separated and clean (no empty, "." or
// ".." step), and text that is stored as it is. The parent holds every path
// it is sent to this (cloneRelative).
func wirePathOK(p string) bool {
	if p == "" || p == "." || strings.HasPrefix(p, "/") || filepath.IsAbs(p) || path.Clean(p) != p {
		return false
	}
	if p == ".." || strings.HasPrefix(p, "../") {
		return false
	}
	return !textclean.WouldChange(p)
}

// storablePath is p when it is in that form, and otherwise a spelling of it
// that is: for a record that names the file and carries no row by it.
func storablePath(p string) string {
	if wirePathOK(p) {
		return p
	}
	q := path.Clean(strings.TrimLeft(filepath.ToSlash(textclean.Sanitize(p)), "/"))
	for strings.HasPrefix(q, "../") {
		q = q[3:]
	}
	if q == "" || q == "." || q == ".." {
		return "unnamed"
	}
	return q
}

// presentLockfiles lists the lockfiles the clone holds, by the walk the
// lockfile pass makes and without reading one (0.29.214): the paths the
// stored lockfile rows are held to when the lockfiles themselves go unread.
func presentLockfiles(workDir string) ([]manifestWirePresent, error) {
	var out []manifestWirePresent
	err := filepath.Walk(workDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			if p == workDir {
				return err
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			if skippedManifestDir(filepath.Base(p), false) {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(p)
		if _, ok := lockfileKinds[base]; !ok {
			return nil
		}
		rel, relErr := filepath.Rel(workDir, p)
		if relErr != nil {
			return nil
		}
		if rel = filepath.ToSlash(rel); wirePathOK(rel) {
			out = append(out, manifestWirePresent{Path: rel, Kind: base})
		}
		return nil
	})
	return out, err
}
