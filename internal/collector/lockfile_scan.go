// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — lockfile_scan.go: the analysis-phase lockfile
// walk (v0.27.11). Runs right after the libyear phase (which refreshes
// repo_deps_libyear — the declared-direct-deps set the storage filter
// matches against) and snapshot-replaces the repo's lockfile inventory
// + direct-dep resolutions. The vulnerability scan then consumes the
// TABLES, not a fresh parse, so `aveloxis heal-vulnerabilities`
// benefits without an analysis pass.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/textclean"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// eachLockfileOfClone visits every lockfile of the clone the roster names:
// the ONE definition of "a lockfile of this repository", which the lockfile
// pass reads by (walkLockfiles) and the name pass lists by
// (presentLockfiles). They were two walks with two copies of the rules
// (0.29.214), and nothing held them equal: a lockfile under node_modules
// counted as present by one and unread by the other would have kept stored
// rows alive for ever (review round 8).
//
// The rules: a symbolic link is not followed and is not a lockfile; nothing
// under vendor, node_modules or .git is visited (skippedManifestDir, the
// manifest walks' rule); a path is given with whether it can be stored
// unchanged (wirePathOK). An error at the root ends the walk with that
// error (nothing is known of the repository's lockfiles); an unreadable
// entry below it is passed over.
func eachLockfileOfClone(workDir string, visit func(path, rel string, info os.FileInfo, storable bool) error) error {
	return filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == workDir {
				// The root itself: nothing is known about this
				// repository's lockfiles (0.29.176). Swallowed, this read
				// as "it has none" and the snapshot was replaced empty.
				return err
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			if skippedManifestDir(filepath.Base(path), false) {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(path)
		if _, ok := lockfileKinds[base]; !ok {
			return nil
		}
		rel, relErr := filepath.Rel(workDir, path)
		if relErr != nil {
			rel = base
		}
		rel = filepath.ToSlash(rel)
		return visit(path, rel, info, wirePathOK(rel))
	})
}

// parsedLockfile pairs a repo-relative path with its parse result.
type parsedLockfile struct {
	Path   string
	Result *LockfileResult
}

// walkLockfiles walks workDir for roster lockfiles and parses each.
// Best-effort throughout: an unreadable or malformed lockfile logs a
// WARN and is skipped — the walk itself never fails. Same traversal
// rules as the manifest walks in manifest_pass.go: symlinks rejected,
// vendor/node_modules/.git skipped.
//
// It is the lockfile pass, and an analysis runs it in the child process that
// reads its clone (0.29.213, manifest_child.go), not in the worker: a
// lockfile is read whole, there is no largest honest one, and what it holds
// is the repository owner's. The child's memory and CPU limits are its
// bound.
//
// Each lockfile that parses is handed to parsed as it is read, so that the
// child, which passes it on, holds one lockfile at a time.
func walkLockfiles(workDir string, logger *slog.Logger, parsed func(parsedLockfile)) ([]model.ManifestFailure, error) {
	var failed []model.ManifestFailure
	// The lockfiles at paths that cannot be stored, under their sanitized
	// paths, and every path the walk named: a sanitized path that is also a
	// real lockfile's (or another's sanitized path) is not said twice, since
	// a lockfile is named once in what the walk hands on.
	var unstorable []model.ManifestFailure
	named := map[string]bool{}
	// v0.27.36: a root-stat failure means zero lockfiles with no signal.
	walkErr := eachLockfileOfClone(workDir, func(path, rel string, info os.FileInfo, storable bool) error {
		base := filepath.Base(path)
		spec := lockfileKinds[base]
		if !storable {
			// A path that cannot be stored as it is (a control character,
			// bytes that are not text) names no row: the lockfile is not
			// read. It is a lockfile of the repository all the same, and
			// one that failed (0.29.215, review round 8: it was passed over
			// in silence, and a repository whose only lockfile was such a
			// one was recorded as having none). Its failure is recorded
			// under the sanitized path and carries no rows.
			logger.Warn("lockfile path cannot be stored unchanged — not read", "path", textclean.Sanitize(rel))
			unstorable = append(unstorable, model.ManifestFailure{Path: textclean.Sanitize(rel), Kind: base, Reason: model.ManifestFailedRead, NoCarry: true})
			return nil
		}
		named[rel] = true
		if spec.binaryOnly {
			// bun.lockb: binary — DETECT for the inventory (kind
			// marker, zero entries), never parse.
			res, _ := ParseLockfile(base, nil)
			parsed(parsedLockfile{Path: rel, Result: res})
			return nil
		}
		// Through manifest.ReadManifest (PR #212 review): a UTF-8 BOM or UTF-16 lockfile
		// is decoded like its manifest, not failed whole (SR-17).
		if cloneFileTooLarge(path, info.Size()) {
			// Not read at all: the file alone is larger than the memory
			// this process has left to it (0.29.214). It is one lockfile
			// that failed, and the rest are read.
			logger.Warn("lockfile is larger than the memory left to read it in (collection.manifest_read_memory_mb) — not read; its previous rows are kept",
				"path", rel, "bytes", info.Size())
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedRead, TooLarge: true})
			return nil
		}
		data, readErr := manifest.ReadManifest(path)
		if readErr != nil {
			logger.Warn("failed to read lockfile — its previous rows are kept", "path", rel, "error", readErr)
			// The read bound's refusal is the same failure as the check
			// above (a file that grew, or that the check's rule let by).
			var tooLarge *manifest.ManifestTooLargeError
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedRead, TooLarge: errors.As(readErr, &tooLarge)})
			return nil
		}
		res, parseErr := ParseLockfile(base, data)
		if parseErr != nil {
			logger.Warn("failed to parse lockfile — its previous rows are kept", "path", rel, "error", parseErr)
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedParse})
			return nil
		}
		if item := lockfileItemTooLarge(res); item != "" {
			// One entry too long to hand over (manifestItemLimit): nothing
			// could store it. The lockfile failed, by name.
			logger.Warn("lockfile holds an entry too long to store — its previous rows are kept", "path", rel, "entry", item)
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedParse})
			return nil
		}
		parsed(parsedLockfile{Path: rel, Result: res})
		return nil
	})
	if walkErr != nil {
		logger.Warn("lockfile walk failed — the stored lockfile snapshot is left as it is", "dir", workDir, "error", walkErr)
	}
	for _, f := range unstorable {
		if !named[f.Path] && wirePathOK(f.Path) {
			named[f.Path] = true
			failed = append(failed, f)
		}
	}
	return failed, walkErr
}

// lockfileSet is what one walk of a clone found: the lockfiles that parsed,
// those that exist and could not be read or parsed, and the walk's own
// error when it could not read the clone's root. It is collected once,
// ahead of the libyear phase (parsing only; nothing is stored), and handed
// to the lockfile phase.
type lockfileSet struct {
	// Parsed are the lockfiles that parsed, when they are held here (a walk
	// in this process: a test). The child's are in spool, on disk
	// (lockfile_spool.go). Read them through each.
	Parsed  []parsedLockfile
	spool   *lockfileSpool
	Failed  []model.ManifestFailure
	WalkErr error
	// Started says the lockfile pass began (the reader was reading
	// lockfiles when it stopped, or finished them): with WalkErr set and
	// Started false, it was stopped before it reached them.
	Started bool
	// Present are the paths of the lockfiles the clone holds, as the name
	// pass listed them (0.29.214); PresentKnown says the list was made. It
	// is what the stored rows are held to when the lockfiles themselves
	// went unread: a lockfile that is gone locks nothing.
	Present      map[string]bool
	PresentKnown bool
}

// each hands fn every lockfile that parsed, one at a time.
func (s lockfileSet) each(fn func(parsedLockfile) error) error {
	for _, pl := range s.Parsed {
		if err := fn(pl); err != nil {
			return err
		}
	}
	if s.spool != nil {
		return s.spool.each(fn)
	}
	return nil
}

// lockfileItemTooLarge names an entry or an edge of a parsed lockfile whose
// text is longer than one item may be on the wire ("" when none is).
func lockfileItemTooLarge(res *LockfileResult) string {
	short := func(s string) string {
		if len(s) > 80 {
			return s[:80] + "…"
		}
		return s
	}
	for _, e := range res.Entries {
		if len(e.Name)+len(e.Version)+len(e.Namespace)+len(e.Ecosystem)+len(e.Scope) > manifestItemLimit {
			return short(textclean.Sanitize(e.Name))
		}
	}
	for _, e := range res.Edges {
		if len(e.ParentName)+len(e.ParentVersion)+len(e.ChildName)+len(e.Constraint) > manifestItemLimit {
			return short(textclean.Sanitize(e.ParentName + " > " + e.ChildName))
		}
	}
	return ""
}

// scanLockfiles is Phase 2b of AnalyzeRepo: walk + parse lockfiles,
// filter entries to the repo's DIRECT declared dependencies (the
// repo_deps_libyear rows Phase 2 just refreshed — bounding storage;
// full transitive storage is the deferred Phase C), and
// snapshot-replace both tables in one transaction.
//
// Multi-lockfile repos: a package resolving to DIFFERENT versions in
// different lockfiles is legitimate (two apps in one monorepo) — all
// distinct (package, version) pairs are kept.
func (ac *AnalysisCollector) scanLockfiles(ctx context.Context, repoID int64, workDir string, locks lockfileSet, result *AnalysisResult) error {
	if locks.WalkErr != nil {
		// Nothing is known about the lockfiles: the snapshot is not
		// replaced (SR-5: an unreadable clone is not "no lockfiles").
		var child *errManifestChild
		if errors.As(locks.WalkErr, &child) {
			// The reader was stopped at a limit, or failed, before it had
			// read the lockfiles (0.29.213). Every lockfile row stands as
			// it is and the scan goes on targeting the versions they hold.
			// It is logged, with what to change, and NOT recorded as an
			// inventory attempt: that record means the inventory was not
			// replaced, and here it may just have been (when the
			// declarations were not read either, scanLibyear has recorded
			// the attempt, with this same reason).
			ac.logger.Log(ctx, manifestFailureLevel(locks.WalkErr, slog.LevelError), "the lockfiles were not read — the stored lockfile rows are kept, and are now older than this analysis",
				append([]any{"repo_id", repoID, "result", child.Attempt, "to_do", lockfilesUnreadAdvice(child.Attempt), "error", locks.WalkErr},
					manifestLimits().effective().logLimits()...)...)
			if !child.Stale {
				reason := child.Attempt
				if !locks.Started {
					// The reader was stopped while it read the names or
					// the declarations: the lockfiles were never begun, and
					// the reason is that, not what stopped the other pass
					// (0.29.215: the page said "reading them took more
					// memory than allowed" of lockfiles nothing had read).
					reason = model.LockfileUnreadNotReached
				}
				ac.lockfileReadingEnded(ctx, repoID, lockfilesNotRead(reason, locks))
			}
			ac.dropRowsOfLockfilesThatAreGone(ctx, repoID, locks)
		}
		var root *errManifestRoot
		if errors.As(locks.WalkErr, &root) {
			ac.lockfileReadingEnded(ctx, repoID, LockfileReading{NotCompleted: model.InventoryAttemptRootUnreadable})
		}
		return fmt.Errorf("lockfile walk failed (snapshot NOT replaced): %w", locks.WalkErr)
	}

	deps, err := ac.store.GetRepoDepsForVulnScan(ctx, repoID)
	if err != nil {
		return err
	}
	declared := make(map[string]bool, len(deps))
	for _, d := range deps {
		declared[pkgid.MatchKey(d.PackageManager, d.Name)] = true
	}

	// What is built here and handed to the store whole: the Go closure,
	// bounded by what one repository's expansion may take (goModTaken), and
	// the rows carried for the lockfiles that failed, which are what the
	// previous snapshot stored for them and so at most
	// db.LockfileSnapshotRows for a snapshot written since 0.29.216. The rows
	// of the lockfiles themselves are NOT built here: they are handed to
	// the store one lockfile at a time, inside its one transaction
	// (0.29.215; until then every row of every lockfile was built first,
	// 1.2 times the lockfiles' bytes, with no bound).
	var extra db.LockfileRows

	// v0.27.133 C2: Go transitive closure via the toolchain — same knob,
	// same snapshot transaction. Declared go deps are the direct set.
	//
	// v0.27.150 (round 29): the expansion is best-effort, but the
	// snapshot REPLACE below is not — an incomplete run (missing
	// toolchain, module failure, repo budget exhausted) used to wipe
	// the previously valid Go closure, silently shrinking vuln and
	// SBOM output and FALSE-RESOLVING findings whose packages
	// vanished (they then flip-flopped back on the next healthy
	// scan). On incomplete: discard this run's partial Go rows and
	// carry the PRIOR snapshot's Go closure forward unchanged —
	// whole-and-stale beats partial-and-mixed. A failed prior-rows
	// read propagates (SR-5): failing the phase leaves the prior
	// snapshot intact, which is strictly better than replacing it
	// with a shrunken one.
	if ac.TransitiveLockfiles {
		goPkgs, goEdges, goComplete := ac.scanGoModGraph(ctx, repoID, workDir, declared)
		if goComplete {
			extra.Packages = append(extra.Packages, goPkgs...)
			extra.Edges = append(extra.Edges, goEdges...)
		} else {
			prevPkgs, prevEdges, perr := ac.store.GetRepoGoClosureRows(ctx, repoID)
			if perr != nil {
				return fmt.Errorf("go expansion incomplete and prior-closure read failed (snapshot NOT replaced): %w", perr)
			}
			ac.logger.Warn("go mod graph expansion incomplete — preserving the prior snapshot's Go closure",
				"repo_id", repoID,
				"prior_packages", len(prevPkgs), "prior_edges", len(prevEdges),
				"discarded_partial_packages", len(goPkgs), "discarded_partial_edges", len(goEdges))
			extra.Packages = append(extra.Packages, prevPkgs...)
			extra.Edges = append(extra.Edges, prevEdges...)
		}
	}

	// A lockfile that exists and failed this cycle keeps its previous
	// answer (0.29.176; summary/70, 4.3). The replace below removes every
	// lockfile row of the repository, so the failed paths' rows are read
	// first and re-inserted through the same insert as fresh rows. Only the
	// parsed columns are old: the direct flag comes from THIS cycle's
	// declared set, by the same filter as a fresh row (an old flag would
	// leave a removed declaration's row targeted by nothing, or add a
	// floor-version target beside the locked one). A failed read of the
	// previous rows fails the phase: the snapshot stands (SR-5).
	failed := append([]model.ManifestFailure(nil), locks.Failed...)
	if len(failed) > 0 {
		var paths []string
		for _, f := range failed {
			// A failure recorded under a sanitized path (a lockfile whose
			// own path cannot be stored) names no stored rows: nothing is
			// carried for it.
			if !f.NoCarry {
				paths = append(paths, f.Path)
			}
		}
		prevInv, prevPkgs, prevEdges, perr := ac.store.GetRepoLockfileRowsAtPaths(ctx, repoID, paths)
		if perr != nil {
			return fmt.Errorf("lockfiles failed and their previous rows could not be read (snapshot NOT replaced): %w", perr)
		}
		carried := map[string]int{}
		for _, p := range prevPkgs {
			p.Direct = declared[pkgid.MatchKey(p.Ecosystem, p.PackageName)]
			if !p.Direct && !ac.TransitiveLockfiles {
				continue
			}
			carried[p.LockfilePath]++
			extra.Packages = append(extra.Packages, p)
		}
		extra.Inventory = append(extra.Inventory, prevInv...)
		if ac.TransitiveLockfiles {
			extra.Edges = append(extra.Edges, prevEdges...)
		}
		for i := range failed {
			if !failed[i].NoCarry {
				failed[i].RowsCarried = carried[failed[i].Path]
			}
		}
	}

	// What the snapshot holds, counted as it is handed over. The store may
	// ask for the rows again when it retries its transaction, so the counts
	// are one handing-over's own (lockfileFeedCounts) and the last one's
	// stand.
	var fed lockfileFeedCounts
	feed := func(write func(db.LockfileRows) error) error {
		var now lockfileFeedCounts
		hand := func(rows db.LockfileRows) error {
			now.take(rows)
			return write(rows)
		}
		// One lockfile at a time (0.29.214): the child's are read back from
		// disk, and what is held of each is the rows it becomes, until the
		// store has them.
		if err := locks.each(func(pl parsedLockfile) error {
			now.aliased += pl.Result.Aliased
			now.notRegistry += pl.Result.NotRegistry
			now.notAVersion += pl.Result.NotAVersion
			return hand(rowsOfLockfile(pl, declared, ac.TransitiveLockfiles))
		}); err != nil {
			return fmt.Errorf("the lockfiles read could not be handed to the store (snapshot NOT replaced): %w", err)
		}
		if err := hand(extra); err != nil {
			return err
		}
		fed = now
		return nil
	}
	var tooLarge []string
	for _, f := range failed {
		if f.TooLarge {
			tooLarge = append(tooLarge, f.Path)
		}
	}
	// How the reading ended, said when the rows are in (the count of
	// lockfiles is known then). A lockfile left unread for its size makes
	// the reading one that did NOT complete, for want of memory, although
	// the snapshot is replaced (with that lockfile's previous rows kept in
	// it): that is what the repository's page cautions about. Before
	// 0.29.215 such a lockfile was either larger than the memory left (one
	// failed lockfile, nothing said of the reading) or stopped the reader
	// mid-parse (no lockfile read at all); now it is always the one file,
	// and always said.
	reading := func() model.LockfileReading {
		if len(tooLarge) > 0 {
			return model.LockfileReading{NotCompleted: model.LockfileUnreadTooLarge, Lockfiles: true}
		}
		// The repository has lockfiles when the clone holds one the
		// roster names: one that parsed, or one that is there and failed,
		// whatever way (a lockfile at a path that cannot be stored is one
		// of the failed).
		return model.LockfileReading{Completed: true, Lockfiles: fed.lockfiles > 0 || len(failed) > 0}
	}
	// The record is written in the replace's own transaction when it is the
	// store that keeps it; a recorder given to the collector instead (a
	// test's) is told after the replace.
	inTx := reading
	if ac.LockfileReadings != nil {
		inTx = nil
	}
	if err := ac.store.ReplaceRepoLockfileSnapshotFrom(ctx, repoID, feed, failed, inTx); err != nil {
		ac.lockfileSnapshotNotReplaced(ctx, repoID, err)
		return err
	}
	if inTx == nil {
		ac.lockfileReadingEnded(ctx, repoID, reading())
	}
	if fed.aliased+fed.notRegistry+fed.notAVersion > 0 {
		ac.logger.Info("lockfile entries whose version field is not a version",
			"repo_id", repoID, "npm_aliases_recorded_under_the_real_package", fed.aliased,
			"git_or_url_sources_left_out", fed.notRegistry, "requirement_strings_left_out", fed.notAVersion)
	}
	if len(tooLarge) > 0 {
		ac.logger.Error("lockfiles too large for the memory left to read them in were not read — their previous rows are kept, and are now older than this analysis",
			append([]any{"repo_id", repoID, "lockfiles", len(tooLarge), "first", tooLarge[0], "result", model.LockfileUnreadTooLarge,
				"to_do", lockfilesUnreadAdvice(model.InventoryAttemptReadMemoryLimit)}, manifestLimits().effective().logLimits()...)...)
	}
	direct, transitive := fed.direct, fed.transitive
	if fed.lockfiles > 0 {
		// Round-21 (v0.27.138): with transitive scanning on by default,
		// the packages are direct + transitive + the Go build list —
		// log the SPLIT, not a mislabeled total (the house
		// log-the-effective-value rule).
		ac.logger.Info("lockfile scan complete",
			"repo_id", repoID,
			"lockfiles", fed.lockfiles,
			"direct_resolutions", direct,
			"transitive_resolutions", transitive,
			"edges", fed.edges)
	}
	result.Lockfiles = fed.lockfiles
	result.LockfilePackages = direct + transitive
	return nil
}

// rowsOfLockfile is what one parsed lockfile becomes in the snapshot: its
// inventory row, its package rows and its edges. declared holds the match
// keys of the repository's declared dependencies; transitive is the
// vuln_scan_transitive knob.
//
// Multi-lockfile repos: a package resolving to DIFFERENT versions in
// different lockfiles is legitimate (two apps in one monorepo) — all
// distinct (package, version) pairs are kept.
func rowsOfLockfile(pl parsedLockfile, declared map[string]bool, transitive bool) db.LockfileRows {
	var rows db.LockfileRows
	res := pl.Result
	// v0.27.133 C2: dependency edges — the attribution substrate —
	// are stored ONLY under the same vuln_scan_transitive knob that
	// gates transitive package rows (edges without the transitive
	// closure have nothing to attribute; knob off keeps the exact
	// pre-C2 write set).
	if transitive {
		edgeSeen := map[string]bool{}
		for _, ed := range res.Edges {
			key := ed.ParentName + "@" + ed.ParentVersion + ">" + ed.ChildName
			if edgeSeen[key] {
				continue
			}
			edgeSeen[key] = true
			rows.Edges = append(rows.Edges, &db.RepoLockfileEdge{
				Ecosystem:       res.Ecosystem,
				LockfilePath:    pl.Path,
				ParentName:      ed.ParentName,
				ParentVersion:   ed.ParentVersion,
				ChildName:       ed.ChildName,
				ChildConstraint: ed.Constraint,
			})
		}
	}
	matched := 0
	seen := map[string]bool{}
	directFlagged := 0
	for _, e := range res.Entries {
		if e.Direct {
			directFlagged++
		}
		// By the entry's own ecosystem: a Bioconductor entry is not the
		// resolution of a declared CRAN dependency of its name.
		isDeclared := declared[pkgid.MatchKey(entryEcosystem(res, e), e.Name)]
		// v0.27.21 C1: with vuln_scan_transitive on, the FULL entry set is
		// stored (direct=FALSE rows are the transitive closure the vuln
		// scan targets). Knob off: pre-C1 behavior — declared-matched
		// rows only.
		if !isDeclared && !transitive {
			continue
		}
		key := e.Name + "@" + e.Version
		if seen[key] {
			continue
		}
		seen[key] = true
		if isDeclared {
			matched++
		}
		rows.Packages = append(rows.Packages, &model.RepoLockfilePackage{
			Ecosystem:       entryEcosystem(res, e),
			PackageName:     e.Name,
			ResolvedVersion: e.Version,
			LockfilePath:    pl.Path,
			Namespace:       e.Namespace,
			// Stored 'direct' means "resolution of a repo-level declared
			// dependency" — exactly the pre-C1 row set.
			Direct: isDeclared,
			Scope:  e.Scope,
		})
	}
	// direct_count: what the format itself flags as direct when it
	// distinguishes; else the declared-dep matches (best proxy) — the
	// Phase-C sizing pair alongside entry_count.
	directCount := directFlagged
	if !res.DirectKnown {
		directCount = matched
	}
	rows.Inventory = append(rows.Inventory, &db.RepoLockfileInfo{
		Ecosystem:    res.Ecosystem,
		LockfilePath: pl.Path,
		LockfileKind: res.Kind,
		EntryCount:   len(res.Entries),
		DirectCount:  directCount,
	})
	return rows
}

// lockfilesInUse is what the libyear phase reads to give an inventory row
// its locked version (0.29.177): every lockfile that parsed this cycle, and,
// for one that exists and failed, the rows it keeps from the previous
// snapshot (0.29.176), so a row does not fall back from its locked version
// to the declared one for as long as the lockfile is broken. If those
// previous rows cannot be read the failed lockfiles are simply not offered:
// their dependencies report the declared version this cycle, and the scan's
// targets, which come from the lockfile table, are unaffected.
//
// wanted (0.29.214) are the match keys of the dependencies the inventory
// will ask about; nil is every key. A lockfile's other entries are not
// kept: the map this builds lives through the libyear phase, and a
// repository's lockfiles hold millions of packages its manifests do not
// declare. versionuse.Nearest asks by exactly these keys, so the answers are
// the same.
func (ac *AnalysisCollector) lockfilesInUse(ctx context.Context, repoID int64, locks lockfileSet, wanted map[string]bool) []versionuse.Lockfile {
	byPath := map[string]*versionuse.Lockfile{}
	var order []string
	add := func(path, ecosystem, name, version string) {
		lf := byPath[path]
		if lf == nil {
			lf = &versionuse.Lockfile{Path: path, Ecosystem: ecosystem, Versions: map[string][]string{}}
			byPath[path] = lf
			order = append(order, path)
		}
		if name == "" {
			return
		}
		key := pkgid.MatchKey(ecosystem, name)
		if wanted != nil && !wanted[key] {
			return
		}
		if !slices.Contains(lf.Versions[key], version) {
			lf.Versions[key] = append(lf.Versions[key], version)
		}
	}
	if err := locks.each(func(pl parsedLockfile) error {
		add(pl.Path, pl.Result.Ecosystem, "", "")
		for _, e := range pl.Result.Entries {
			// An entry of another ecosystem (renv.lock's Bioconductor
			// packages) locks no dependency of the lockfile's: the CRAN
			// dependency of that name is another package (SR-6).
			if entryEcosystem(pl.Result, e) != pl.Result.Ecosystem {
				continue
			}
			add(pl.Path, pl.Result.Ecosystem, e.Name, e.Version)
		}
		return nil
	}); err != nil {
		// What the child read could not be read back from this host's
		// disk. No lockfile is offered: the dependencies report the
		// declared version this cycle, and the lockfile phase, which reads
		// the same result, fails and leaves the snapshot as it is.
		ac.logger.Warn("the lockfiles read could not be read back — dependencies report the declared version this cycle", "repo_id", repoID, "error", err)
		return nil
	}
	if locks.WalkErr != nil {
		// The lockfiles were not read at all this cycle (0.29.213: the
		// reader was stopped before it had read them), and every lockfile
		// row stands. The inventory, if it is replaced, goes on reporting
		// the versions those rows lock: without this every row fell back to
		// its declared version for as long as the lockfiles went unread.
		//
		// Only for the lockfiles that are still there (0.29.214): the
		// name pass listed the clone's lockfiles, and a stored row of one
		// that is gone locks nothing. Without the list (the name pass did
		// not finish either) no stored row is offered: which lockfiles
		// exist is not known, and a version locked by a file that was
		// deleted is a wrong version (review round 7).
		gone := 0
		for _, p := range ac.storedLockfileRows(ctx, repoID) {
			if !locks.PresentKnown || !locks.Present[filepath.ToSlash(p.LockfilePath)] {
				gone++
				continue
			}
			add(p.LockfilePath, p.Ecosystem, p.PackageName, p.ResolvedVersion)
		}
		if gone > 0 {
			ac.logger.Info("stored lockfile rows not used for the inventory: their lockfile is not in the clone now, or the clone's lockfiles could not be listed",
				"repo_id", repoID, "rows", gone, "lockfiles_listed", locks.PresentKnown)
		}
	}
	if len(locks.Failed) > 0 {
		paths := make([]string, len(locks.Failed))
		for i, f := range locks.Failed {
			paths[i] = f.Path
		}
		_, prev, _, err := ac.store.GetRepoLockfileRowsAtPaths(ctx, repoID, paths)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				ac.logger.Warn("the previous rows of lockfiles that failed could not be read — their dependencies report the declared version this cycle",
					"repo_id", repoID, "lockfiles", len(paths), "error", err)
			}
		}
		for _, p := range prev {
			if p.Ecosystem == bioconductorEcosystem {
				continue // as above, for a carried row
			}
			add(p.LockfilePath, p.Ecosystem, p.PackageName, p.ResolvedVersion)
		}
	}
	out := make([]versionuse.Lockfile, 0, len(order))
	for _, p := range order {
		out = append(out, *byPath[p])
	}
	return out
}

// storedLockfileRows are the rows the stored snapshot holds for the
// repository's lockfiles: the direct and the transitive ones, of files the
// lockfile roster names (the Go build list's rows are stored beside them and
// are not a lockfile's), less a renv.lock's Bioconductor packages, as for a
// lockfile read this cycle. If they cannot be read none are offered, and the
// dependencies report the declared version this cycle.
func (ac *AnalysisCollector) storedLockfileRows(ctx context.Context, repoID int64) []model.RepoLockfilePackage {
	direct, err := ac.store.GetRepoLockedVersions(ctx, repoID)
	var transitive []model.RepoLockfilePackage
	if err == nil {
		transitive, err = ac.store.GetRepoTransitivePackagesWithPaths(ctx, repoID)
	}
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			ac.logger.Warn("the stored lockfile rows could not be read while the lockfiles went unread — dependencies report the declared version this cycle",
				"repo_id", repoID, "error", err)
		}
		return nil
	}
	var out []model.RepoLockfilePackage
	for _, p := range append(direct, transitive...) {
		if storedRowLocksADependency(p) {
			out = append(out, p)
		}
	}
	return out
}

// storedRowLocksADependency reports whether a stored lockfile row is one a
// lockfile read this cycle would have offered the inventory: a row of a file
// the lockfile roster names, of the lockfile's own ecosystem.
func storedRowLocksADependency(p model.RepoLockfilePackage) bool {
	_, isLockfile := lockfileKinds[path.Base(p.LockfilePath)]
	return isLockfile && p.Ecosystem != bioconductorEcosystem
}

// lockfilesUnreadAdvice is what an operator does about lockfiles that went
// unread, by the reason the reader gave.
func lockfilesUnreadAdvice(attempt string) string {
	switch attempt {
	case model.InventoryAttemptReadMemoryLimit:
		low, high := lockfileReadCostRange()
		return fmt.Sprintf("raise collection.manifest_read_memory_mb: a lockfile is read whole and is priced at %d to %d times its size by its format (the configuration page has each format's)", low, high)
	case model.InventoryAttemptReadCPULimit, model.InventoryAttemptReadTimedOut:
		return "raise collection.manifest_read_cpu_seconds, or look at this repository's build files and lockfiles: one of them takes this long to read"
	}
	return "the reader failed and did not say a limit: see the error, and the reader's own output in it"
}

// lockfileReadCostRange is the least and the most a lockfile of the roster
// is priced at, in times its length (readCostFactor), so that the advice
// above says the table's numbers and not a copy of them.
func lockfileReadCostRange() (low, high int64) {
	for kind := range lockfileKinds {
		f := readCostFactor(kind)
		if low == 0 || f < low {
			low = f
		}
		if f > high {
			high = f
		}
	}
	return low, high
}

// lockfileFeedCounts is what ONE handing-over of a snapshot's rows held: the
// lockfiles, the rows of each kind, and (v0.29.175) the entries whose
// "version" was not a version, over every lockfile of the repository.
type lockfileFeedCounts struct {
	lockfiles, direct, transitive, edges int
	aliased, notRegistry, notAVersion    int
}

func (c *lockfileFeedCounts) take(rows db.LockfileRows) {
	c.lockfiles += len(rows.Inventory)
	c.edges += len(rows.Edges)
	for _, p := range rows.Packages {
		if p.Direct {
			c.direct++
		} else {
			c.transitive++
		}
	}
}

// lockfileSnapshotNotReplaced is told of a snapshot replace that failed.
// One failure is a statement about the lockfiles and not about the database:
// the store keeps at most db.LockfileSnapshotRows rows for a repository and
// refused this snapshot whole (0.29.216). The previous rows stand and are
// older than this analysis, which is what the lockfile caution says, so the
// reading is recorded as one whose lockfiles were too large to take
// (model.LockfileUnreadTooLarge, the reason a lockfile refused for its size
// has; no setting raises this limit, and the error the analysis logs names
// it). The error itself is logged once, by the
// analysis, like every other error of this phase.
func (ac *AnalysisCollector) lockfileSnapshotNotReplaced(ctx context.Context, repoID int64, err error) {
	if !errors.Is(err, db.ErrLockfileSnapshotTooLarge) {
		return
	}
	ac.lockfileReadingEnded(ctx, repoID, LockfileReading{NotCompleted: model.LockfileUnreadTooLarge, Lockfiles: true})
}

// LockfileRowDeleter removes the stored rows of lockfiles at the given
// paths, in one statement. When the analysis's store (*db.PostgresStore) has
// this method it is used (it has: db.DeleteRepoLockfileRowsAtPaths, one
// statement over the three lockfile tables); AnalysisCollector's
// LockfileRows field, when set, is used instead.
type LockfileRowDeleter interface {
	DeleteRepoLockfileRowsAtPaths(ctx context.Context, repoID int64, paths []string) (int64, error)
}

// dropRowsOfLockfilesThatAreGone is for an analysis whose lockfiles went
// unread (0.29.214): the snapshot is not replaced, so the rows of a lockfile
// the repository has since deleted would stand, and go on giving the
// vulnerability scan a version that nothing locks any more (review round 7:
// express at 4.18.2 after its lockfile was removed and its manifest moved to
// ^5.0.0). The name pass listed the lockfiles the clone holds; the stored
// rows of lockfiles that are not among them are removed, which is what the
// replace would have done to them. Nothing else of the snapshot is touched.
// Without the list nothing is removed: which lockfiles exist is not known.
//
// The inventory does not wait for this: lockfilesInUse already offers it no
// row of a lockfile that is gone.
func (ac *AnalysisCollector) dropRowsOfLockfilesThatAreGone(ctx context.Context, repoID int64, locks lockfileSet) {
	if !locks.PresentKnown {
		return
	}
	seen := map[string]bool{}
	var gone []string
	note := func(stored string) {
		at := filepath.ToSlash(stored)
		if !locks.Present[at] && !seen[at] {
			seen[at] = true
			gone = append(gone, stored)
		}
	}
	for _, p := range ac.storedLockfileRows(ctx, repoID) {
		note(p.LockfilePath)
	}
	// And by the snapshot's inventory rows: a lockfile that gave no package
	// row that locks a dependency (a binary one, an empty one, one whose
	// entries are all of another registry) is named only there, and its
	// row stood after the lockfile was deleted (0.29.215, review round 8).
	if lister, ok := any(ac.store).(interface {
		GetRepoLockfilePaths(ctx context.Context, repoID int64) ([]string, error)
	}); ok {
		paths, err := lister.GetRepoLockfilePaths(ctx, repoID)
		if err != nil && !errors.Is(err, context.Canceled) {
			ac.logger.Warn("the stored lockfile inventory could not be read while the lockfiles went unread", "repo_id", repoID, "error", err)
		}
		for _, p := range paths {
			note(p)
		}
	}
	if len(gone) == 0 {
		return
	}
	del := ac.LockfileRows
	if del == nil {
		var ok bool
		if del, ok = any(ac.store).(LockfileRowDeleter); !ok {
			ac.logger.Warn("stored lockfile rows are of lockfiles that are not in the clone now, and stand until the lockfiles are next read: the scan still targets the versions they hold",
				"repo_id", repoID, "lockfiles_gone", len(gone))
			return
		}
	}
	n, err := del.DeleteRepoLockfileRowsAtPaths(ctx, repoID, gone)
	if errors.Is(err, context.Canceled) {
		return // a `stop serve`
	}
	if err != nil {
		ac.logger.Warn("the stored rows of lockfiles that are gone could not be removed", "repo_id", repoID, "lockfiles_gone", len(gone), "error", err)
		return
	}
	ac.logger.Info("removed the stored rows of lockfiles that are not in the clone now", "repo_id", repoID, "lockfiles_gone", len(gone), "rows", n)
}
