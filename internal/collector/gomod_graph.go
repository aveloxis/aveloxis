// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// gomod_graph.go — v0.27.133 (C2): Go transitive closure via the go
// toolchain, the summary/14 "revisit in C2 with `go mod graph`" item.
//
// Go has no lockfile in the roster ON PURPOSE (go.mod versions are
// exact under MVS — direct deps classify 'locked' by construction, and
// go.sum is a verification SUPERSET that would manufacture false
// exposure). What C1 therefore lacked for Go was the TRANSITIVE
// closure. The honest source is the toolchain itself:
//   - `go list -m all` — the RESOLVED build list (MVS-selected
//     versions), which becomes the transitive package rows;
//   - `go mod graph` — the requirement edges, which become the
//     attribution substrate.
//
// Both run in the analysis phase's full working checkout (the scc
// precedent, analysis.go: LookPath-gated, best-effort WARN-and-skip,
// bounded by a wall-clock timeout). Network use matches the analysis
// phase's existing posture (libyear talks to registries). Gated on the
// same vuln_scan_transitive knob as every other transitive source.

package collector

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// goModGraphTimeout bounds the WHOLE repo's toolchain expansion —
// every module shares one budget. v0.27.148 (round 28): the timeout
// was created per module, so a monorepo with N go.mod files could
// occupy a collection worker for N×5 minutes (kubernetes-class repos
// carry ~30 staging modules), unbounded at the repo level — the
// per-item-budgets-sum-to-nothing class. The 5-minute figure was
// sized from the scc precedent, which is per REPO; the per-module
// placement was the accident. Healthy modules resolve in seconds —
// the shared budget covers dozens; only wedged ones consume it, and
// the WARN-and-skip posture keeps partial results honest.
const goModGraphTimeout = 5 * time.Minute

// scanGoModGraph returns Go transitive package rows + requirement
// edges for every go.mod module under workDir, plus a COMPLETE flag.
// Best-effort: a missing go binary, a broken module, or the shared
// repo budget logs and contributes nothing — but v0.27.150 (round
// 29) that best-effort posture must never SHRINK the stored
// snapshot: complete=false tells the caller to preserve the prior
// snapshot's Go closure instead of replacing it with a partial (or
// empty) one, which silently deflated vuln and SBOM output and
// false-resolved findings. declared holds the repo's declared-dep
// match keys (the direct set — its members are never written as
// transitive rows).
func (ac *AnalysisCollector) scanGoModGraph(ctx context.Context, workDir string, declared map[string]bool) (
	packages []*db.RepoLockfilePackage, edges []*db.RepoLockfileEdge, complete bool) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		ac.logger.Debug("go toolchain not installed — skipping go mod graph transitive expansion")
		return nil, nil, false
	}
	var modDirs []string
	_ = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			base := info.Name()
			if base == "vendor" || base == "node_modules" || base == ".git" || base == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Name() == "go.mod" {
			modDirs = append(modDirs, filepath.Dir(path))
		}
		return nil
	})
	// ONE repo-wide budget; per-module contexts derive from it.
	rctx, cancel := context.WithTimeout(ctx, goModGraphTimeout)
	defer cancel()
	complete = true
	for i, dir := range modDirs {
		if rctx.Err() != nil {
			// No silent caps: say exactly what the budget dropped.
			ac.logger.Warn("go mod graph repo budget exhausted — skipping remaining modules",
				"budget", goModGraphTimeout, "modules_processed", i, "modules_skipped", len(modDirs)-i)
			complete = false
			break
		}
		var modOK bool
		packages, edges, modOK = ac.goModGraphOne(rctx, goBin, workDir, dir, declared, packages, edges)
		if !modOK {
			complete = false
		}
	}
	return packages, edges, complete
}

func (ac *AnalysisCollector) goModGraphOne(ctx context.Context, goBin, workDir, dir string, declared map[string]bool,
	packages []*db.RepoLockfilePackage, edges []*db.RepoLockfileEdge) ([]*db.RepoLockfilePackage, []*db.RepoLockfileEdge, bool) {
	// ctx carries the REPO-WIDE deadline (round 28) — no per-module
	// timeout here, or N modules would multiply the budget.
	rel, _ := filepath.Rel(workDir, filepath.Join(dir, "go.mod"))
	env := append(os.Environ(), "GOFLAGS=-mod=mod")

	run := func(args ...string) (string, bool) {
		cmd := exec.CommandContext(ctx, goBin, args...)
		cmd.Dir = dir
		cmd.Env = env
		out, rerr := cmd.Output()
		if rerr != nil {
			ac.logger.Warn("go toolchain invocation failed — skipping this module's transitive expansion",
				"module_dir", rel, "args", strings.Join(args, " "), "error", rerr)
			return "", false
		}
		return string(out), true
	}

	// The RESOLVED build list: "module version" per line; the first
	// line is the main module (no version) and is skipped.
	listOut, ok := run("list", "-m", "all")
	if !ok {
		return packages, edges, false
	}
	seenPkg := map[string]bool{}
	for _, line := range strings.Split(listOut, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue // main module or blank
		}
		name, version := fields[0], fields[1]
		if declared[lockfileMatchKey("go", name)] || seenPkg[name+"@"+version] {
			continue // declared deps are the direct set, already scanned
		}
		seenPkg[name+"@"+version] = true
		packages = append(packages, &db.RepoLockfilePackage{
			Ecosystem:       "go",
			PackageName:     name,
			ResolvedVersion: version,
			LockfilePath:    rel,
			Direct:          false,
		})
	}

	// The requirement edges: "parent@ver child@ver" per line (the main
	// module appears without @ver — its edges express the DIRECT set,
	// which the declared deps already cover, so they are skipped).
	graphOut, ok := run("mod", "graph")
	if !ok {
		return packages, edges, false
	}
	seenEdge := map[string]bool{}
	for _, line := range strings.Split(graphOut, "\n") {
		parent, child, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found {
			continue
		}
		pName, pVer, pOK := strings.Cut(parent, "@")
		cName, cVer, _ := strings.Cut(child, "@")
		if !pOK || pName == "" || cName == "" {
			continue // main-module edges (no @ver) express the declared set
		}
		key := parent + ">" + cName
		if seenEdge[key] {
			continue
		}
		seenEdge[key] = true
		edges = append(edges, &db.RepoLockfileEdge{
			Ecosystem:       "go",
			LockfilePath:    rel,
			ParentName:      pName,
			ParentVersion:   pVer,
			ChildName:       cName,
			ChildConstraint: cVer,
		})
	}
	return packages, edges, true
}
