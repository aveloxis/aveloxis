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
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
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

// What the toolchain prints is decided by the repository's go.mod files,
// so it is taken as a stream under two bounds (0.29.215, review round 8:
// a go.mod of N requires, each replaced by a local directory whose go.mod
// has the same N, prints N*N edges with no network; 228 KB of go.mod
// became nine million edges and 2.75 GB in the worker).
//
// PROVISIONAL, both. goModGraphLines is the lines taken for one
// repository, over all its modules and both commands; past it the
// expansion is abandoned as INCOMPLETE, so the stored closure is kept.
// Derivation: this module's own graph is 121 lines; a kubernetes-class
// monorepo is some thirty modules of about three thousand lines each,
// about 90,000; half a million is five times that, and at the measured
// 142 bytes held per edge it is about 70 MiB at the bound.
// goModOutputBytes is the output taken from one invocation: a line names
// two modules with versions, about 100 bytes, so 64 MiB is more than the
// line bound allows and exists for output that is not lines at all.
const (
	goModGraphLines  = 500_000
	goModOutputBytes = 64 << 20
)

// errGoModGraphLines: the repository's expansion passed goModGraphLines.
var errGoModGraphLines = errors.New("the Go module expansion printed more lines than are taken for one repository")

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
func (ac *AnalysisCollector) scanGoModGraph(ctx context.Context, repoID int64, workDir string, declared map[string]bool) (
	packages []*model.RepoLockfilePackage, edges []*db.RepoLockfileEdge, complete bool) {
	// v0.27.152 (round 31, suppressed): discover modules BEFORE the
	// toolchain check. Zero go.mod files is a DEFINITIVELY complete
	// (empty) expansion regardless of whether go is installed — with
	// the old order, a toolless host reported incomplete forever, so
	// a repo that REMOVED all its Go modules kept its stale closure
	// preserved on every scan.
	// v0.27.153 (round 32): a DISCOVERY failure is an INCOMPLETE scan
	// (SR-16 — a walk error is not "zero modules"). The old
	// swallow-everything walk let an unreadable root/subtree produce
	// empty-or-partial modDirs with complete=true, and the caller then
	// REPLACED the valid prior closure — the exact false-resolve the
	// preserve contract exists to prevent. Partial discovery is
	// discarded outright: incomplete output is thrown away by the
	// caller anyway, so expanding the modules we happened to find
	// would be wasted toolchain time.
	var modDirs []string
	// walkBounded, not filepath.Walk: this walk runs in the worker, and a
	// directory's listing is as large as the repository makes it.
	walkErr := walkBounded(workDir, cloneWalkEntries, func(path string, entry fs.DirEntry) error {
		if entry.IsDir() {
			base := entry.Name()
			if base == "vendor" || base == "node_modules" || base == ".git" || base == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Name() == "go.mod" {
			modDirs = append(modDirs, filepath.Dir(path))
		}
		return nil
	})
	if walkErr != nil {
		ac.logger.Warn("go.mod discovery walk failed — treating the expansion as INCOMPLETE (prior closure preserved)",
			"repo_id", repoID, "dir", workDir, "walk_error", walkErr, "partial_modules_found", len(modDirs))
		return nil, nil, false
	}
	// The order the toolchain is run in is the order of the paths, as
	// filepath.Walk gave it (a directory's entries now arrive unsorted).
	sort.Strings(modDirs)
	if len(modDirs) == 0 {
		return nil, nil, true
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		ac.logger.Debug("go toolchain not installed — skipping go mod graph transitive expansion (incomplete: modules exist but cannot be expanded)", "repo_id", repoID)
		return nil, nil, false
	}
	// ONE repo-wide budget; per-module contexts derive from it.
	rctx, cancel := context.WithTimeout(ctx, goModGraphTimeout)
	defer cancel()
	complete = true
	budgetWarned := false
	lines := 0
	for i, dir := range modDirs {
		if errors.Is(rctx.Err(), context.Canceled) {
			return packages, edges, false // shutdown: incomplete, the prior closure is preserved
		}
		if rctx.Err() != nil {
			// No silent caps: say exactly what the budget dropped.
			ac.logger.Warn("go mod graph repo budget exhausted — skipping remaining modules",
				"repo_id", repoID, "budget", goModGraphTimeout, "modules_processed", i, "modules_skipped", len(modDirs)-i)
			budgetWarned = true
			complete = false
			break
		}
		var modOK bool
		packages, edges, modOK = ac.goModGraphOne(rctx, repoID, goBin, workDir, dir, declared, packages, edges, &lines)
		if !modOK {
			complete = false
		}
		if lines > goModGraphLines {
			// Said once, with what was dropped: the expansion is
			// incomplete, and what was gathered is not used.
			ac.logger.Warn("go mod graph: the expansion is larger than is taken for one repository — abandoned as INCOMPLETE (prior closure preserved)",
				"repo_id", repoID, "lines_limit", goModGraphLines, "modules_processed", i+1, "modules_skipped", len(modDirs)-i-1)
			return nil, nil, false
		}
	}
	if !budgetWarned && errors.Is(rctx.Err(), context.DeadlineExceeded) {
		// The budget ran out INSIDE the last module — the loop-top
		// check never saw it; say so (pass 36).
		ac.logger.Warn("go mod graph repo budget exhausted during the last module — its expansion was cut short",
			"repo_id", repoID, "budget", goModGraphTimeout, "modules_processed", len(modDirs))
	}
	return packages, edges, complete
}

func (ac *AnalysisCollector) goModGraphOne(ctx context.Context, repoID int64, goBin, workDir, dir string, declared map[string]bool,
	packages []*model.RepoLockfilePackage, edges []*db.RepoLockfileEdge, lines *int) ([]*model.RepoLockfilePackage, []*db.RepoLockfileEdge, bool) {
	// ctx carries the REPO-WIDE deadline (round 28) — no per-module
	// timeout here, or N modules would multiply the budget.
	rel, _ := filepath.Rel(workDir, filepath.Join(dir, "go.mod"))
	// GOWORK=off: each module is expanded on its own, and inside a go.work
	// the toolchain refuses -mod=mod ("may only be set to readonly or
	// vendor when in workspace mode") — 52 of 64 failures on kate
	// (2026-09-23 log review), each an empty Go closure.
	// GOTOOLCHAIN=local: a cloned go.mod's `toolchain go1.X` line must not
	// make this host download and run that toolchain (under `auto` it
	// would, per module per collection — the repository's choice; ASVS
	// review of 0.29.125, V5.3.2); a too-new go line fails into the WARN
	// arm below, as install-tools' own probe already pins it (tools.go).
	env := goModuleEnv(ctx, goBin, ac.logger)

	// run hands each line the toolchain prints to each, and reports
	// whether the invocation ran to its end. The output is never held
	// whole: boundedLines stops the toolchain past goModOutputBytes, and
	// the count shared by the repository's modules stops it past
	// goModGraphLines.
	run := func(each func(line string), args ...string) bool {
		cmd := exec.CommandContext(ctx, goBin, args...)
		cmd.Dir = dir
		cmd.Env = env
		stderr, rerr := boundedLines(cmd, goModOutputBytes, func(line string) error {
			if *lines++; *lines > goModGraphLines {
				return errGoModGraphLines
			}
			each(line)
			return nil
		})
		if rerr != nil && errors.Is(ctx.Err(), context.Canceled) {
			return false // shutdown killed the toolchain: not a module failure (pass 35); a budget DEADLINE still warns below
		}
		if errors.Is(rerr, errGoModGraphLines) {
			return false // the caller says so, once, for the repository
		}
		if rerr != nil {
			// v0.29.58: the toolchain's stderr is in the line; without it
			// 147 warnings in one run read "exit status 1" and nothing
			// else (2026-09-22 log review).
			ac.logger.Warn("go toolchain invocation failed — skipping this module's transitive expansion",
				"repo_id", repoID, "module_dir", rel, "args", strings.Join(args, " "), "error", rerr, "stderr", stderr)
			return false
		}
		return true
	}

	// The RESOLVED build list: "module version" per line; the first
	// line is the main module (no version) and is skipped.
	seenPkg := map[string]bool{}
	if !run(func(line string) {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return // main module or blank
		}
		name, version := fields[0], fields[1]
		if declared[pkgid.MatchKey("go", name)] || seenPkg[name+"@"+version] {
			return // declared deps are the direct set, already scanned
		}
		seenPkg[name+"@"+version] = true
		packages = append(packages, &model.RepoLockfilePackage{
			Ecosystem:       "go",
			PackageName:     name,
			ResolvedVersion: version,
			LockfilePath:    rel,
			Direct:          false,
		})
	}, "list", "-m", "all") {
		return packages, edges, false
	}

	// The requirement edges: "parent@ver child@ver" per line (the main
	// module appears without @ver — its edges express the DIRECT set,
	// which the declared deps already cover, so they are skipped).
	seenEdge := map[string]bool{}
	if !run(func(line string) {
		parent, child, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found {
			return
		}
		pName, pVer, pOK := strings.Cut(parent, "@")
		cName, cVer, _ := strings.Cut(child, "@")
		if !pOK || pName == "" || cName == "" {
			return // main-module edges (no @ver) express the declared set
		}
		key := parent + ">" + cName
		if seenEdge[key] {
			return
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
	}, "mod", "graph") {
		return packages, edges, false
	}
	return packages, edges, true
}

// goPublicProxy is the proxy used when the environment names none.
const goPublicProxy = "https://proxy.golang.org"

// goProxyWithoutDirect is the GOPROXY list with its `direct` entries
// removed (the fallback that reaches any host a go.mod names), keeping the
// operator's proxies and separators; an empty result is the public proxy.
func goProxyWithoutDirect(list string) string {
	if strings.TrimSpace(list) == "" {
		return goPublicProxy
	}
	var out strings.Builder
	sep := ""
	for i := 0; i < len(list); {
		j := strings.IndexAny(list[i:], ",|")
		end := len(list)
		if j >= 0 {
			end = i + j
		}
		entry := strings.TrimSpace(list[i:end])
		if entry != "" && entry != "direct" {
			out.WriteString(sep)
			out.WriteString(entry)
		}
		if end < len(list) {
			if out.Len() > 0 {
				sep = string(list[end])
			}
			i = end + 1
		} else {
			break
		}
	}
	if out.Len() == 0 {
		return goPublicProxy
	}
	return out.String()
}

// goModuleEnv is the environment the go-module closure runs under: the
// one builder (0.29.133; the behaviour is 0.29.132's), pinned by
// TestGoModuleEnvironmentOverridesSavedPrivateSettings. GOPROXY without
// `direct` (L10 round 2 of 0.29.132): a `require` or `replace` naming a
// host the proxy answers 404/410 for fell through to `direct` — a GET and a
// git ls-remote from this host to any address the cloned go.mod names, the
// reachability the outbound-address rule closes elsewhere. The operator's
// own proxy list is kept (a host behind a corporate module proxy keeps
// resolving through it; round 3) with `direct` removed — the EFFECTIVE
// list, read through `go env`, since a value saved with `go env -w` is not
// in this process's environment (Copilot's twelfth review of PR #246).
// GONOPROXY and GOPRIVATE are set to a pattern no module path matches, not
// emptied: the go command falls back to the saved file for an EMPTY
// variable (cfg.Getenv), so an operator's `go env -w GOPRIVATE=corp.internal`
// stayed in force and a stranger's go.mod naming that host reached it
// directly. GONOSUMDB is DERIVED from GOPRIVATE when not set itself
// (cfg.go: EnvOrAndChanged("GONOSUMDB", GOPRIVATE)), so overriding
// GOPRIVATE would silently send the operator's private modules to
// sum.golang.org (L10 round 1 of 0.29.133): its effective value is read in
// the same `go env` call and passed as it was, so the checksum settings are
// unchanged in effect — they govern integrity, not egress (the checksum
// database is never the repository host; round 4). A module no proxy serves
// is the WARN arm in goModGraphOne (it used to succeed through `direct`).
// The probe is GoBinDir's (goEnvValues, the one spelling); a failed probe,
// or one that does not answer as it should, falls back to this process's
// environment, logged (everything that errors is logged) unless the
// context was cancelled.
func goModuleEnv(ctx context.Context, goBin string, logger *slog.Logger) []string {
	proxy, noSumDB := os.Getenv("GOPROXY"), os.Getenv("GONOSUMDB")
	values, err := goEnvValues(ctx, goBin, "GOPROXY", "GONOSUMDB")
	if err == nil {
		proxy, noSumDB = strings.TrimSpace(values[0]), strings.TrimSpace(values[1]) // the effective values: the environment, else the saved file, else the default
	}
	// A stop during the probe is not a failure: the closure's commands below
	// stop the same way.
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("go env probe failed; the go-module closure runs on this process's GOPROXY and GONOSUMDB (a value saved with go env -w is not seen)",
			"error", err)
	}
	if noSumDB == "" {
		noSumDB = goNoModule // nothing was exempt from the checksum database; an empty value would derive from the overridden GOPRIVATE
	}
	return append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local",
		"GOPROXY="+goProxyWithoutDirect(proxy), "GONOPROXY="+goNoModule, "GOPRIVATE="+goNoModule, "GONOSUMDB="+noSumDB)
}

// goNoModule is a module-path pattern nothing matches: a module path's first
// element carries a dot (a host), so the bare word never names one. Any
// non-empty value overrides a saved setting; an empty one does not.
const goNoModule = "none"
