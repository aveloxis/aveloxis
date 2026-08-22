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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aveloxis/aveloxis/internal/db"
)

// parsedLockfile pairs a repo-relative path with its parse result.
type parsedLockfile struct {
	Path   string
	Result *LockfileResult
}

// collectLockfiles walks workDir for roster lockfiles and parses each.
// Best-effort throughout: an unreadable or malformed lockfile logs a
// WARN and is skipped — the walk itself never fails. Same traversal
// rules as the manifest walks in analysis.go: symlinks rejected,
// vendor/node_modules/.git skipped.
func collectLockfiles(workDir string, logger *slog.Logger) []parsedLockfile {
	var out []parsedLockfile
	// v0.27.36: a root-stat failure means zero lockfiles with no signal.
	walkErr := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == "node_modules" || base == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(path)
		spec, ok := lockfileKinds[base]
		if !ok {
			return nil
		}
		rel, relErr := filepath.Rel(workDir, path)
		if relErr != nil {
			rel = base
		}
		if spec.binaryOnly {
			// bun.lockb: binary — DETECT for the inventory (kind
			// marker, zero entries), never parse.
			res, _ := ParseLockfile(base, nil)
			out = append(out, parsedLockfile{Path: rel, Result: res})
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			logger.Warn("failed to read lockfile", "path", rel, "error", readErr)
			return nil
		}
		res, parseErr := ParseLockfile(base, data)
		if parseErr != nil {
			logger.Warn("failed to parse lockfile — skipping", "path", rel, "error", parseErr)
			return nil
		}
		out = append(out, parsedLockfile{Path: rel, Result: res})
		return nil
	})
	if walkErr != nil {
		logger.Warn("lockfile walk failed — lockfile inventory may be incomplete", "dir", workDir, "error", walkErr)
	}
	return out
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
func (ac *AnalysisCollector) scanLockfiles(ctx context.Context, repoID int64, workDir string, result *AnalysisResult) error {
	parsed := collectLockfiles(workDir, ac.logger)

	deps, err := ac.store.GetRepoDepsForVulnScan(ctx, repoID)
	if err != nil {
		return err
	}
	declared := make(map[string]bool, len(deps))
	for _, d := range deps {
		declared[lockfileMatchKey(d.PackageManager, d.Name)] = true
	}

	var inventory []*db.RepoLockfileInfo
	var packages []*db.RepoLockfilePackage
	var edges []*db.RepoLockfileEdge
	for _, pl := range parsed {
		res := pl.Result
		// v0.27.133 C2: dependency edges — the attribution substrate —
		// are stored ONLY under the same vuln_scan_transitive knob that
		// gates transitive package rows (edges without the transitive
		// closure have nothing to attribute; knob off keeps the exact
		// pre-C2 write set).
		if ac.TransitiveLockfiles {
			edgeSeen := map[string]bool{}
			for _, ed := range res.Edges {
				key := ed.ParentName + "@" + ed.ParentVersion + ">" + ed.ChildName
				if edgeSeen[key] {
					continue
				}
				edgeSeen[key] = true
				edges = append(edges, &db.RepoLockfileEdge{
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
			if !declared[lockfileMatchKey(res.Ecosystem, e.Name)] {
				// v0.27.21 C1: with vuln_scan_transitive on, the FULL
				// entry set is stored (direct=FALSE rows are the
				// transitive closure the vuln scan targets). Knob off:
				// pre-C1 behavior — declared-matched rows only.
				if !ac.TransitiveLockfiles {
					continue
				}
				key := e.Name + "@" + e.Version
				if seen[key] {
					continue
				}
				seen[key] = true
				packages = append(packages, &db.RepoLockfilePackage{
					Ecosystem:       res.Ecosystem,
					PackageName:     e.Name,
					ResolvedVersion: e.Version,
					LockfilePath:    pl.Path,
					Direct:          false,
					Scope:           e.Scope,
				})
				continue
			}
			key := e.Name + "@" + e.Version
			if seen[key] {
				continue
			}
			seen[key] = true
			matched++
			packages = append(packages, &db.RepoLockfilePackage{
				Ecosystem:       res.Ecosystem,
				PackageName:     e.Name,
				ResolvedVersion: e.Version,
				LockfilePath:    pl.Path,
				// Stored 'direct' means "resolution of a repo-level
				// declared dependency" — exactly the pre-C1 row set.
				Direct: true,
				Scope:  e.Scope,
			})
		}
		// direct_count: what the format itself flags as direct when it
		// distinguishes; else the declared-dep matches (best proxy) —
		// the Phase-C sizing pair alongside entry_count.
		directCount := directFlagged
		if !res.DirectKnown {
			directCount = matched
		}
		inventory = append(inventory, &db.RepoLockfileInfo{
			Ecosystem:    res.Ecosystem,
			LockfilePath: pl.Path,
			LockfileKind: res.Kind,
			EntryCount:   len(res.Entries),
			DirectCount:  directCount,
		})
	}

	// v0.27.133 C2: Go transitive closure via the toolchain — same knob,
	// same snapshot transaction. Declared go deps are the direct set.
	if ac.TransitiveLockfiles {
		packages, edges = ac.scanGoModGraph(ctx, workDir, declared, packages, edges)
	}

	if err := ac.store.ReplaceRepoLockfileSnapshot(ctx, repoID, inventory, packages, edges); err != nil {
		return err
	}
	if len(inventory) > 0 {
		// Round-21 (v0.27.138): with transitive scanning on by default,
		// packages holds direct + transitive + the Go build list —
		// log the SPLIT, not a mislabeled total (the house
		// log-the-effective-value rule).
		direct := 0
		for _, p := range packages {
			if p.Direct {
				direct++
			}
		}
		ac.logger.Info("lockfile scan complete",
			"repo_id", repoID,
			"lockfiles", len(inventory),
			"direct_resolutions", direct,
			"transitive_resolutions", len(packages)-direct,
			"edges", len(edges))
	}
	result.Lockfiles = len(inventory)
	result.LockfilePackages = len(packages)
	return nil
}
