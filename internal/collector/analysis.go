// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — analysis.go implements on-demand repo analysis phases:
// dependency scanning, libyear calculation, and code complexity (scc).
//
// These phases require a full checkout (not a bare clone) to scan file contents.
// A temporary working copy is created from the existing bare clone, analysis
// tools run against it, results are inserted into the database, and the
// working copy is immediately deleted to minimize disk usage.
//
// Design: bare clones (permanent, small) for git log/commits.
//
//	full clones (temporary, on-demand) for file analysis.
package collector

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/spdx"
	"github.com/aveloxis/aveloxis/internal/textclean"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// AnalysisCollector runs file-content analysis on repos.
type AnalysisCollector struct {
	store *db.PostgresStore
	// LockfileReadings, when set, is told how each analysis's reading of
	// the lockfiles ended (lockfile_reading.go).
	LockfileReadings LockfileReadingRecorder
	// LockfileRows, when set, removes the stored rows of lockfiles that
	// are gone (lockfile_scan.go, dropRowsOfLockfilesThatAreGone).
	LockfileRows LockfileRowDeleter
	logger       *slog.Logger
	bareDir      string // base directory for bare clones (e.g., ~/aveloxis-repos)
	tempDir      string // base directory for temporary full clones

	// RetainClone skips automatic cleanup of the temporary full clone after
	// analysis. When true, the clone path is set in AnalysisResult.ClonePath
	// and the caller is responsible for cleanup (os.RemoveAll). This allows
	// scorecard to run against the local clone before it is deleted.
	RetainClone bool

	// TransitiveLockfiles (v0.27.21 C1, collection.vuln_scan_transitive)
	// stores the FULL lockfile entry set in repo_lockfile_packages
	// (direct=FALSE rows = the transitive closure) instead of only the
	// declared-dep resolutions. Read at the point of use in
	// scanLockfiles; false = byte-identical pre-C1 row set.
	TransitiveLockfiles bool

	// DevBuildDeps (v0.27.45, summary/19 P2, collection.dev_build_deps)
	// expands Python dependency collection to the dev/test/build/
	// optional manifest families (requirements-variant files, pyproject
	// build-system/optional/PEP 735/poetry groups, Pipfile
	// dev-packages, setup.py/cfg extras). Default FALSE — the
	// findings-volume driver; false = the pre-v0.27.45 walk row set.
	DevBuildDeps bool

	// GitHubActionsDeps (v0.27.47, summary/19 P4,
	// collection.github_actions_deps) inventories workflow `uses:`
	// references as build-scope deps. Default FALSE.
	GitHubActionsDeps bool

	// GitHubAPI (v0.29.56) is the key-pooled GitHub REST client for the
	// libyear lookups GitHub hosts (Go module licenses, SwiftPM releases).
	// nil when no GitHub keys are loaded: those lookups then fail and are
	// counted in the libyear WARNs, never sent anonymously.
	GitHubAPI githubAPIGetter
}

// NewAnalysisCollector creates an analysis collector.
func NewAnalysisCollector(store *db.PostgresStore, logger *slog.Logger, bareDir string) *AnalysisCollector {
	return &AnalysisCollector{
		store:   store,
		logger:  logger,
		bareDir: bareDir,
		tempDir: filepath.Join(os.TempDir(), "aveloxis-analysis"),
	}
}

// logAnalysisPhaseErrors logs each analysis-phase failure. Before this,
// AnalyzeRepo appended every phase error to result.Errors and logged only
// the COUNT ("errors", len(result.Errors)), and the scheduler never reads
// the slice. So any phase failure that its phase did not log itself never
// reached a log line (round 3, F5). Two examples are scc exiting non-zero
// or being killed after a COMPLETE report, and trailing garbage after the
// report. In the 2026-09-06..15 production log, 35 of 138,397 analyses
// carried errors, about 90 over nine days, so a WARN per error is cheap.
//
// This is the ONLY place scanSCC's errors are logged. Some other phases do
// log before returning (libyear's rotation failure logs at ERROR), so
// those failures appear twice: the phase's own line with its context,
// then this one. That duplicate is accepted, because dropping it would
// mean auditing every phase's internal logging to find which errors are
// silent (round 4, R4-2).
//
// A cancellation is a shutdown, not a phase failure, so it is skipped
// (the context.Canceled classification rule).
func logAnalysisPhaseErrors(logger *slog.Logger, repoID int64, errs []error) {
	for _, phaseErr := range errs {
		if errors.Is(phaseErr, context.Canceled) {
			continue
		}
		logger.Log(context.Background(), manifestFailureLevel(phaseErr, slog.LevelWarn), "analysis phase failed", "repo_id", repoID, "error", phaseErr)
	}
}

// AnalysisResult tracks what was collected.
type AnalysisResult struct {
	Dependencies     int
	LibyearDeps      int
	Lockfiles        int // v0.27.11: committed lockfiles inventoried
	LockfilePackages int // v0.27.11: direct-dep resolutions stored
	LaborFiles       int
	ScancodeFiles    int // files with scancode findings (licenses, copyrights, packages)
	Errors           []error

	// ClonePath is the path to the temporary full clone. Only set when
	// AnalysisCollector.RetainClone is true. The caller must clean it up
	// with os.RemoveAll after any post-analysis work (e.g., scorecard).
	ClonePath string
}

// AnalyzeRepo creates a temporary full checkout from the bare clone,
// runs all analysis phases, inserts results, then deletes the checkout.
func (ac *AnalysisCollector) AnalyzeRepo(ctx context.Context, repoID int64) (*AnalysisResult, error) {
	result := &AnalysisResult{}

	barePath := BareClonePath(ac.bareDir, repoID)
	if !HasBareClone(ac.bareDir, repoID) {
		ac.recordInventoryAttempt(ctx, repoID, model.InventoryAttemptCloneUnavailable)
		return result, fmt.Errorf("no bare clone at %s", barePath)
	}

	// Create temporary full clone from the bare repo (local clone, no network).
	workDir := filepath.Join(ac.tempDir, fmt.Sprintf("repo_%d_%d", repoID, time.Now().UnixNano()))
	if !ac.RetainClone {
		defer func() {
			_ = os.RemoveAll(workDir)
			ac.logger.Info("removed temporary analysis clone", "path", workDir)
		}()
	}

	ac.logger.Info("creating temporary full clone for analysis",
		"repo_id", repoID, "bare", barePath, "workdir", workDir)

	if err := os.MkdirAll(filepath.Dir(workDir), 0o755); err != nil {
		return result, err
	}
	// Bounded: git lists one line per path for some warnings (paths that
	// collide on this filesystem), and the repository chooses its paths.
	stderr := newBoundedCapture()
	cmd := exec.CommandContext(ctx, "git", "clone", barePath, workDir)
	// Skip LFS smudge filters — dependency/license scanners only need text
	// source files. LFS objects with expired quotas cause fatal checkout failures.
	cmd.Env = gitCloneEnv()
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		ac.recordInventoryAttempt(ctx, repoID, model.InventoryAttemptCloneUnavailable)
		return result, fmt.Errorf("local clone failed: %w: %s", execErr(ctx, err), stderr.String())
	}

	// The clone is read once, by one child process, before any phase
	// writes (0.29.213, clone_read.go): the dependency names, the
	// declarations and the lockfiles.
	read := ac.readClone(ctx, repoID, workDir)
	defer read.close()

	// Phase 1: Dependency scanning.
	if err := ac.scanDependencies(ctx, repoID, workDir, read, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("dependencies: %w", err))
	}

	// v0.29.176 (summary/70, 4.3): the lockfiles are READ ahead of the
	// libyear phase, and stored in phase 2b. The order is what gives an
	// inventory row its locked version (0.29.177).
	locks := read.lockfiles()

	// Phase 2: Libyear (dependency age).
	if err := ac.scanLibyear(ctx, repoID, workDir, read, ac.lockfilesInUse(ctx, repoID, locks, read.declaredKeys()), result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("libyear: %w", err))
	}

	// Phase 2b (v0.27.11): lockfile inventory + direct-dep
	// resolutions. Must run AFTER Phase 2 — the storage filter matches
	// lockfile entries against the repo_deps_libyear rows the libyear
	// phase just refreshed. Best-effort like every other phase.
	if err := ac.scanLockfiles(ctx, repoID, workDir, locks, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("lockfiles: %w", err))
	}

	// Phase 3: Code complexity via scc (if installed).
	if err := ac.scanSCC(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("scc: %w", err))
	}

	// Phase 4: ScanCode removed from the analysis pipeline in v0.21.0.
	// Scancode is now run by a dedicated ScancodeWorker pool in
	// internal/collector/scancode_worker.go, claimed via FOR UPDATE
	// SKIP LOCKED against aveloxis_data.repos, paced by
	// collection.scancode_start_interval_s, and run with a
	// configurable cadence of (default) 180 days. The 2026-05-14
	// production incident showed that running scancode inline here
	// with a 2-slot semaphore parked 177 of 180 collection workers
	// for 7+ hours. Do NOT put scancode back on this path — see
	// docs/architecture/scancode.md for the architectural rationale
	// and TestAnalyzeRepoNoLongerInvokesScancode for the regression
	// guard.

	logAnalysisPhaseErrors(ac.logger, repoID, result.Errors)

	// When RetainClone is true, hand the clone path to the caller for
	// post-analysis work (e.g., local scorecard execution).
	if ac.RetainClone {
		result.ClonePath = workDir
	}

	ac.logger.Info("analysis complete",
		"repo_id", repoID,
		"dependencies", result.Dependencies,
		"libyear_deps", result.LibyearDeps,
		"lockfiles", result.Lockfiles,
		"lockfile_packages", result.LockfilePackages,
		"labor_files", result.LaborFiles,
		"retain_clone", ac.RetainClone,
		"errors", len(result.Errors))

	return result, nil
}

// ============================================================
// Dependency scanning
// ============================================================

func (ac *AnalysisCollector) scanDependencies(ctx context.Context, repoID int64, workDir string, read *cloneRead, result *AnalysisResult) error {
	ac.logger.Info("scanning dependencies", "repo_id", repoID)

	// Walk first, write last (0.29.158). repo_dependencies is a snapshot
	// table with no history; it is replaced in one transaction once the
	// whole clone has been walked. Before, the table was cleared and then
	// the clone walked, so a clone that could not be read emptied it.
	//
	// The walk ran in the child process that read the clone (0.29.213,
	// clone_read.go).
	run := read.run
	err := run.passErr(manifestPassNames, read.runErr)
	if errors.Is(err, context.Canceled) {
		// A `stop serve`, not a defect.
		return fmt.Errorf("dependency manifest walk: %w", err)
	}
	if err != nil {
		var root *errManifestRoot
		switch {
		case errors.As(err, &root):
			ac.logger.Error("dependency manifest walk failed at the clone's root — the previous dependency names are kept",
				"repo_id", repoID, "dir", workDir, "error", err)
		default:
			ac.logger.Log(ctx, manifestFailureLevel(err, slog.LevelError), "the dependency names were not read — the previous dependency names are kept",
				append([]any{"repo_id", repoID, "dir", workDir, "error", err}, read.limits.logLimits()...)...)
		}
		return fmt.Errorf("dependency manifest walk: %w", err)
	}
	depCounts := run.names // language -> dep_name -> count

	// One row PostgreSQL refuses would refuse the whole snapshot, so rows
	// are made storable here. A name is never repaired: one the sanitizer
	// would change is left out and counted.
	var rows []db.RepoDependency
	nameRejected := 0
	for lang, deps := range depCounts {
		for depName, count := range deps {
			if textclean.WouldChange(depName) {
				nameRejected++
				continue
			}
			rows = append(rows, db.RepoDependency{Language: textclean.Sanitize(lang), Name: depName, Count: count})
		}
	}
	if err := ac.store.ReplaceRepoDependencies(ctx, repoID, rows); err != nil {
		// A cancelled context is a `stop serve`, not a defect.
		if !errors.Is(err, context.Canceled) {
			ac.logger.Error("failed to replace the dependency names — the previous ones are kept",
				"repo_id", repoID, "rows", len(rows), "error", err)
		}
		return fmt.Errorf("replace dependency snapshot: %w", err)
	}
	result.Dependencies = len(rows)
	if nameRejected > 0 {
		ac.logger.Warn("dependency names that cannot be stored unchanged were left out",
			"repo_id", repoID, "deps", nameRejected)
	}

	return nil
}

// ============================================================
// Libyear scanning (dependency age)
// ============================================================

func (ac *AnalysisCollector) scanLibyear(ctx context.Context, repoID int64, workDir string, read *cloneRead, lockfiles []versionuse.Lockfile, result *AnalysisResult) error {
	ac.logger.Info("scanning libyear", "repo_id", repoID)
	// The registry layer's own conditions (a deep pacing queue) report
	// through this collection's logger, not slog's default.
	ctx = withRegistryLogger(ctx, ac.logger)

	// The order of work is: walk and parse, resolve, replace (summary/70,
	// 4.7). Nothing is written until the whole inventory is in memory, and
	// then it is written in ONE transaction (store.ReplaceRepoLibyear), so
	// the stored inventory is never partial. An analysis that stops before
	// the replace, for any reason, has written nothing and the previous
	// inventory stands whole. Before 0.29.157 the previous rows were rotated
	// away first and each new row inserted after its own registry lookup; a
	// vulnerability scan reading the table part-way closed the findings of
	// every dependency not yet re-inserted.
	var allDeps []model.LibyearDep
	// The analysis half of the coverage record, written with the inventory.
	var cov model.InventoryCoverage
	// v0.29.182: a Maven parent or BOM that is not in the clone was obtained
	// from the configured Maven repositories, and stored, while the clone
	// was read (readClone).
	poms, err := read.poms, read.pomsErr
	if err != nil {
		// Which repository hosts may be used is not known. Nothing is
		// walked; the previous inventory stands.
		if !errors.Is(err, context.Canceled) {
			ac.logger.Error("libyear: the distrusted repository hosts could not be read — the previous dependency inventory is kept",
				"repo_id", repoID, "error", err)
		}
		return fmt.Errorf("libyear: %w", err)
	}
	defer poms.logSummary(repoID)
	// The registry lookups below ask the same list.
	ctx = withDistrustedHosts(ctx, poms.distrusted)
	if configured := mavenRepositoryBases(); len(usableMavenRepositories(ctx)) != len(configured) {
		// The effective value, where it is used: a configured repository
		// is distrusted and is not being asked.
		var hosts []string
		for _, b := range usableMavenRepositories(ctx) {
			hosts = append(hosts, hostOf(b))
		}
		ac.logger.Info("maven repositories in use (a configured one is distrusted)", "repo_id", repoID, "hosts", hosts)
	}

	// The manifests were read in a child process (0.29.213,
	// manifest_child.go, clone_read.go): the walk over the clone, every
	// reader, version hygiene and the settling of Maven's managed entries.
	// The child asked this process for the poms that are not in the clone,
	// and poms answered. Nothing it read could hold this worker past the
	// child's limits.
	limits := read.limits
	run := read.run
	err = run.passErr(manifestPassDeclarations, read.runErr)
	if errors.Is(err, context.Canceled) {
		// A `stop serve`, not a failed attempt: nothing is logged or
		// recorded.
		return fmt.Errorf("libyear manifest walk: %w", err)
	}
	if err != nil {
		var root *errManifestRoot
		var child *errManifestChild
		switch {
		case errors.As(err, &root):
			// The walk failed at the root itself (per-entry errors are
			// skipped inside it), so nothing was visited. Replacing the
			// inventory now would replace it with nothing, and every
			// finding would close as if its dependency were gone. The
			// previous inventory stands (v0.27.36 logged this and went on
			// with zero dependencies).
			ac.logger.Error("libyear manifest walk failed at the clone's root — the previous dependency inventory is kept",
				"repo_id", repoID, "dir", workDir, "error", err)
			ac.recordInventoryAttempt(ctx, repoID, model.InventoryAttemptRootUnreadable)
		case errors.As(err, &child):
			// The reader was stopped at a limit, or failed. Nothing it
			// read is used: a part of an inventory reads as all of it. The
			// previous inventory stands, and the coverage record says
			// which limit, with the limits in force here.
			ac.logger.Log(ctx, manifestFailureLevel(err, slog.LevelError), "libyear: the manifests were not read — the previous dependency inventory is kept",
				"repo_id", repoID, "dir", workDir, "result", child.Attempt, "error", err,
				"manifest_read_cpu_seconds", int(limits.CPU/time.Second), "manifest_read_memory_mb", limits.HeapBytes>>20)
			ac.recordInventoryAttempt(ctx, repoID, child.Attempt)
		default:
			ac.logger.Error("libyear: the manifests were not read — the previous dependency inventory is kept",
				"repo_id", repoID, "dir", workDir, "error", err)
			ac.recordInventoryAttempt(ctx, repoID, model.InventoryAttemptReadFailed)
		}
		return fmt.Errorf("libyear manifest walk: %w", err)
	}
	decl := run.decl
	allDeps = decl.Deps
	cov.ManifestsFound, cov.ManifestsParsed, cov.ManifestsFailed = decl.Found, decl.Parsed, decl.Failed
	cov.MavenTreeHosts, cov.MavenTreesNotWhole = decl.TreeHosts, decl.NotWhole
	if decl.RemoteNotAsked > 0 {
		ac.logger.Warn("libyear: this repository's build asks for more parent and BOM poms than one analysis asks repositories for; the rest were not asked for, and what depends on them is reported as not obtained",
			"repo_id", repoID, "asked_limit", maxRemotePomCoordinates, "not_asked", decl.RemoteNotAsked)
	}

	// v0.29.177 (summary/70, 4.3): the version IN USE. Where the nearest
	// lockfile in a manifest's ancestry resolves a dependency to exactly
	// one version, the row reports that version, with its source; the
	// registry is then asked about the version really installed. AFTER the
	// hygiene above, on purpose: a locked version is the lockfile's field
	// as written. This changes what a row reports and never what the scan
	// targets (versionuse.Nearest).
	lockedRows := 0
	for i := range allDeps {
		if allDeps[i].NonRegistry {
			continue
		}
		version, source := versionuse.Nearest(allDeps[i].ManifestPath, allDeps[i].Manager, allDeps[i].Name, lockfiles)
		if source != "" {
			// A lockfile's answer replaces a source the reader set (a
			// Maven version from dependencyManagement); no answer leaves
			// the reader's in place.
			allDeps[i].VersionSource = source
		}
		if source == versionuse.SourceLocked {
			allDeps[i].Version = version
			lockedRows++
		}
	}
	if lockedRows > 0 {
		ac.logger.Info("libyear: dependencies reported at the version their nearest lockfile resolves",
			"repo_id", repoID, "deps", lockedRows)
	}

	// v0.27.45 (summary/19 P2, Go C1): go.mod deps whose packages are
	// imported ONLY from _test.go files are test-scope. The child that read
	// the clone relabelled them (readManifestDeclarations; 0.29.214: this
	// worker read every .go file itself until then).

	// Resolve each dependency against its package registry. Answers are
	// shared across the process for registryCacheTTL (resolveLibyearCached).
	resolveFailures := map[string]int{}
	resolveSample := map[string]error{}
	licenseFailures := 0
	nonRegistry := 0
	var licenseSample error
	// The whole inventory, held in memory until the one replace below.
	rows := make([]*model.LibyearRow, 0, len(allDeps))
	nameRejected := 0
	nameRejectedSample := ""
	withoutRow := map[string]int{}
	// A declared dependency that gets no row is recorded as a note, with the
	// reason, and counted under the same reason in the coverage record.
	var notes []model.InventoryNote
	noRow := func(dep model.LibyearDep, kind string) {
		withoutRow[kind]++
		notes = append(notes, inventoryNote(dep, kind))
	}
	cov.DependenciesDeclared = len(allDeps) + len(decl.Unused)
	for _, dep := range decl.Unused {
		noRow(dep, model.NoRowManagedUnused)
	}
	swiftPM := func(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
		return resolveSwiftPMLibyear(ctx, ac.GitHubAPI, dep)
	}
	for _, dep := range allDeps {
		var resolve func(context.Context, model.LibyearDep) (*model.LibyearRow, error)
		switch dep.Manager {
		case "npm":
			resolve = resolveNPMLibyear
		case "pypi":
			resolve = resolvePyPILibyear
		case "go":
			resolve = resolveGoLibyear
		case "cargo":
			resolve = resolveCargoLibyear
		case "rubygems":
			resolve = resolveRubyGemsLibyear
		case "maven":
			resolve = resolveMavenLibyear
		case "packagist":
			resolve = resolvePackagistLibyear
		case "hex":
			resolve = resolveHexLibyear
		case "nuget":
			resolve = resolveNuGetLibyear
		case "pub":
			resolve = resolvePubDevLibyear
		case "hackage":
			resolve = resolveHackageLibyear
		case "cran":
			resolve = resolveCRANLibyear
		case "swiftpm":
			resolve = swiftPM
		case "githubactions":
			// v0.27.47: Actions have no registry timeline — store the
			// inventory row directly with libyear NULL (NoLibyear).
			// The upstream-dependencies snapshot filters libyear IS
			// NOT NULL, so actions never inflate the headline.
		default:
			// No resolver for this package manager. No parser emits one the
			// switch above lacks; counted so the coverage record's numbers
			// still add up if one ever does.
			withoutRow[model.NoRowLookupFailed]++
			continue
		}
		if dep.NonRegistry {
			// Sourced from a local path, a workspace, git or a URL: the
			// registry knows nothing about it (looking it up only drew
			// 404s), and it is NOT the registry package of the same name,
			// so it gets no row and no purl here — the dependency scan
			// still inventories it in repo_dependencies. Storing a
			// purl-less row instead would enter it into the vulnerability
			// scan's universe as an unscannable dependency, which blocks
			// the scan-complete stamp (v0.28.5).
			nonRegistry++
			noRow(dep, model.NoRowNonRegistry)
			ac.logger.Debug("libyear: dependency is not from a registry — not looked up",
				"dep", dep.Name, "manager", dep.Manager, "requirement", dep.Requirement)
			continue
		}
		// Before any lookup: can this dependency have a row at all? A
		// coordinate with no purl (no group where the purl type requires a
		// namespace) is recorded and not looked up; it is not garbage. Then
		// the name itself: one that fails its ecosystem's grammar, cannot be
		// stored unchanged or is too long is not a package name, and the
		// registry is not asked about it (0.29.164: the registry used to be
		// the de facto validator, by answering for nothing else).
		if _, hasPurl := manifestPurl(dep); !hasPurl {
			noRow(dep, model.NoRowNoNamespace)
			continue
		}
		if !pkgid.ValidName(dep.Manager, dep.Name) || textclean.WouldChange(dep.Name) {
			nameRejected++
			noRow(dep, model.NoRowNameRejected)
			if nameRejectedSample == "" {
				nameRejectedSample = textclean.Sanitize(dep.Name)
			}
			continue
		}
		var lb *model.LibyearRow
		var err error
		if resolve == nil {
			lb = &model.LibyearRow{
				Name: dep.Name, Requirement: dep.Requirement,
				Type: dep.Type, PackageManager: dep.Manager,
				CurrentVersion: dep.Version,
				Purl:           pkgid.Purl("githubactions", dep.Name, dep.Version),
				NoLibyear:      true,
				ManifestPath:   dep.ManifestPath,
			}
		} else {
			lb, err = resolveLibyearCached(ctx, libyearRowCache, dep, resolve)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}

		if err != nil || lb == nil {
			// 0.29.162: the dependency keeps its row. The inventory says
			// what the repository DEPENDS ON, which the manifest states and
			// the registry only annotates; dropping the row made a registry
			// outage, a typo or a private package invisible to the
			// vulnerability scan and to the SBOM.
			kept, hasPurl := unansweredLibyearRow(dep, err)
			if kept != nil && errors.Is(err, errRepositoryDistrusted) && dep.VersionReason == "" {
				// Its only repositories are distrusted. Said as that, and
				// never as "not on a registry", which would be false.
				dep.VersionReason = model.VersionReasonInheritedRepositoryRefused
			}
			// v0.27.19: the silent `continue` here hid TWO
			// since-inception ecosystem outages (npm CLI missing on
			// hosts; crates.io 403 on curl's default UA). Count per
			// manager and surface an aggregate WARN below — a per-dep
			// WARN would flood on big manifests, but silence is how
			// this stayed invisible for the product's whole life.
			if err != nil {
				resolveFailures[dep.Manager]++
				if resolveSample[dep.Manager] == nil {
					resolveSample[dep.Manager] = err
				}
				ac.logger.Debug("libyear resolution failed", "dep", dep.Name, "manager", dep.Manager,
					"reason", libyearStatus(dep, lb, err), "error", err)
			}
			if !hasPurl || !storableLibyearRow(kept) {
				// Unreachable: both were decided before the lookup. Kept as
				// the same record a dependency with no purl gets, so the
				// numbers add up if the two checks ever drift apart.
				noRow(dep, model.NoRowNoNamespace)
				continue
			}
			stampVersionSource(dep, kept)
			rows = append(rows, kept)
			continue
		}
		if dep.Manager == "go" {
			// The Go proxy has no license; the GitHub repository hosting
			// the module does (through the key pool, cached per repo).
			lic, lerr := githubModuleLicense(ctx, ac.GitHubAPI, githubLicenseCache, dep.Name)
			if errors.Is(lerr, context.Canceled) {
				return lerr
			}
			if lerr != nil {
				licenseFailures++
				if licenseSample == nil {
					licenseSample = lerr
				}
			}
			lb.License = lic
		}
		settleLibyear(dep, lb)
		stampVersionSource(dep, lb)
		if !storableLibyearRow(lb) {
			// A resolver's row whose name differs from the declared one
			// and cannot be stored unchanged (the declared name passed
			// above). No resolver renames, so this is a guard.
			nameRejected++
			noRow(dep, model.NoRowNameRejected)
			continue
		}
		rows = append(rows, lb)
	}

	// Replace: the previous inventory goes to history and rows becomes the
	// inventory, in one transaction, or nothing changes.
	cov.DependenciesWithoutRow = withoutRow
	// v0.29.184: the package repositories this repository's build files
	// declare, written by the same replace.
	for _, d := range decl.Repositories {
		if textclean.WouldChange(d.URL) || textclean.WouldChange(d.ManifestPath) {
			continue // cannot be stored unchanged; the next analysis reads it again
		}
		cov.DeclaredRepositories = append(cov.DeclaredRepositories, model.DeclaredRepository{
			ManifestPath: d.ManifestPath, URL: d.URL, Host: hostOf(d.URL), Kind: d.Kind, NotFetched: d.NotFetched})
	}
	if err := ac.store.ReplaceRepoLibyear(ctx, repoID, rows, notes, cov); err != nil {
		// A cancelled context is a `stop serve`, not a defect.
		if !errors.Is(err, context.Canceled) {
			ac.logger.Error("failed to replace the dependency inventory — the previous inventory is kept",
				"repo_id", repoID, "rows", len(rows), "error", err)
			ac.recordInventoryAttempt(ctx, repoID, model.InventoryAttemptReplaceFailed)
		}
		return fmt.Errorf("replace dependency inventory: %w", err)
	}
	result.LibyearDeps = len(rows)
	if nameRejected > 0 {
		ac.logger.Warn("libyear: declared names that are not package names were given no row and not looked up",
			"repo_id", repoID, "deps", nameRejected, "sample_name_sanitized", nameRejectedSample)
	}

	if nonRegistry > 0 {
		// Every manager whose reader marks such an entry (npm and cargo
		// first; Dart, Elixir, Ruby, Go, Composer, R and NuGet since).
		// Not Python: it drops a VCS/URL/path requirement while
		// parsing (manifest.IsNonRegistryPyRequirement, applied in
		// manifest.ParseRequirementsTxtVersions and manifest.ParsePyRequirement), because such
		// a line names no PyPI package to look up, so it never reaches this
		// counter. Its INVENTORY row survives when the entry point is one
		// the inventory walk parses (requirements.txt lines other than
		// pip's option lines, PEP 621 dependencies, install_requires), with
		// the requirement line itself as the dependency name
		// (docs/architecture/analysis.md).
		ac.logger.Info("libyear: dependencies not from a registry were not looked up",
			"repo_id", repoID, "deps", nonRegistry)
	}
	if licenseFailures > 0 {
		ac.logger.Warn("libyear license lookups failed without an answer — rows stored without a license, retried on the next analysis",
			"repo_id", repoID, "manager", "go", "failed_deps", licenseFailures, "sample_error", licenseSample)
	}
	for manager, n := range resolveFailures {
		ac.logger.Warn("libyear resolution failures for ecosystem",
			"repo_id", repoID, "manager", manager, "failed_deps", n,
			"sample_error", resolveSample[manager])
	}

	return nil
}

// registryHTTPClient bounds every registry request at 30s — the same
// budget the curl-era --max-time carried (v0.27.5's #2 hang class). The
// same figure is the whole-lookup budget for rate-limit waits
// (registryRetryBudget). Per-host PACING waits are separate and are not
// bounded by it (paceRegistryHost).
//
// v0.29.179: under the outbound-address rule (registry_boundary.go): the
// transport's dial pins the checked address, and each redirect hop is
// judged before it is followed.
var registryHTTPClient = &http.Client{Timeout: 30 * time.Second, Transport: registryTransport, CheckRedirect: registryCheckRedirect}

// fetchRegistryJSON fetches a registry URL over net/http (v0.27.30 —
// converted from curl so every resolver's base URL is injectable for
// behavioral tests and live canaries). headers are optional "Key: Value"
// pairs (Hackage needs Accept). Despite the name, the returned bytes are
// whatever the endpoint served — Hackage's upload-time endpoints return
// plain text and use this helper too.
//
// Every registry request goes through doRegistryRequest
// (libyear_registry_client.go): the identifying User-Agent (crates.io
// 403s anonymous defaults, v0.27.19), per-host pacing for registries
// with a published rate limit, and bounded Retry-After handling
// (v0.29.56). A non-2xx answer is a *registryStatusError; 404/410 are
// definitive misses (isDefinitiveRegistryMiss), everything else is a
// failure without an answer.
func fetchRegistryJSON(ctx context.Context, url string, headers ...string) ([]byte, error) {
	resp, err := doRegistryRequest(ctx, http.MethodGet, url, headers...)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// Bounded (0.29.179): one byte past the bound is read, to tell a body
	// at the bound from one beyond it.
	limit := registryBodyLimit(ctx)
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	platform.NoteResponseSize(registryLoggerFrom(ctx), "registry", url, int64(len(b)))
	if err == nil && int64(len(b)) > limit {
		return nil, fmt.Errorf("%w (%d bytes): %s", errRegistryBodyTooLarge, limit, platform.RedactURLUserinfo(url))
	}
	return b, err
}

// npmRegistryBase is the npm registry root — a var so behavioral tests
// can point it at an httptest server.
var npmRegistryBase = "https://registry.npmjs.org"

// Registry base URLs — vars so behavioral tests and live canaries can
// point each resolver at an httptest server (v0.27.30; npm led the
// way in v0.27.19). Before this, 7 of the 12 registries had ZERO test
// coverage of any kind: their URLs were hardcoded through curl and
// structurally unreachable by tests — the audit's G2 gap, the exact
// preconditions of the npm/cargo whole-ecosystem outages.
var (
	pypiRegistryBase      = "https://pypi.org"
	goProxyBase           = "https://proxy.golang.org"
	cratesRegistryBase    = "https://crates.io"
	rubygemsRegistryBase  = "https://rubygems.org"
	mavenRepositoryBase   = "https://repo1.maven.org/maven2"
	packagistRegistryBase = "https://repo.packagist.org"
	hexRegistryBase       = "https://hex.pm"
	nugetRegistryBase     = "https://api.nuget.org"
	pubDevRegistryBase    = "https://pub.dev"
	hackageRegistryBase   = "https://hackage.haskell.org"
	// crandb, the R-hub project's database of CRAN (0.29.204; operator's
	// choice, 2026-11-19): CRAN itself has no JSON interface.
	cranRegistryBase = "https://crandb.r-pkg.org"
)

// resolveNPMLibyear checks the npm registry for the latest version.
//
// v0.27.19: plain HTTP against registry.npmjs.org. The previous
// implementation shelled out to the `npm` CLI — which is not installed
// on collection hosts (aveloxis install-tools ships scc/scorecard/
// scancode only), so EVERY npm dependency failed "executable file not
// found" and was silently dropped, since inception: zero npm rows ever
// reached repo_deps_libyear, and therefore JavaScript dependencies
// were never vulnerability-scanned (the vuln scan reads purls from
// that table). Same defect class as the v0.24.1 deps.dev and v0.27.4
// OSV bugs: no live-API canary. One is added alongside this fix.
//
// Scoped names (@babel/core) must be path-escaped (@babel%2Fcore) —
// the deps.dev URL-encoding lesson.
func resolveNPMLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx, npmRegistryBase+"/"+url.PathEscape(dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		DistTags struct {
			Latest string `json:"latest"`
		} `json:"dist-tags"`
		// time maps each version to its publish date — and, for a package
		// with an unpublish record, "unpublished" to an OBJECT (2026-09-23
		// log review: that entry failed the whole decode). Values are read
		// one by one below; non-string entries are skipped.
		Time    map[string]json.RawMessage `json:"time"`
		License json.RawMessage            `json:"license"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	timeOf := func(version string) string {
		var s string
		if raw, ok := info.Time[version]; ok && json.Unmarshal(raw, &s) == nil {
			return s
		}
		return ""
	}
	info2 := struct{ Version, License string }{Version: info.DistTags.Latest}
	// license is usually a string, but old packages use the object
	// form {"type": "MIT", "url": ...}.
	if len(info.License) > 0 {
		if err := json.Unmarshal(info.License, &info2.License); err != nil {
			var obj struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(info.License, &obj) == nil {
				info2.License = obj.Type
			}
		}
	}

	currentDate := timeOf(dep.Version)
	_, onRegistry := info.Time[dep.Version]
	latestDate := timeOf(info2.Version)
	libyear := calcLibyear(currentDate, latestDate)

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "npm",
		CurrentVersion:     dep.Version,
		LatestVersion:      info2.Version,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            libyear,
		License:            info2.License,
		Purl:               pkgid.Purl("npm", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// resolvePyPILibyear checks PyPI for the latest version.
func resolvePyPILibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(pypiRegistryBase+"/pypi/%s/json", dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		Info struct {
			Version string `json:"version"`
			License string `json:"license"`
			// PEP 639 (live on PyPI since 2024): modern packages set
			// license_expression (an SPDX expression) and leave the
			// legacy license field AND the trove classifiers empty —
			// flask 3.x live-verified 2026-07-21. The v0.27.30 PyPI
			// canary caught this on its FIRST run: without this field
			// every PEP-639 package silently loses license data.
			LicenseExpression string   `json:"license_expression"`
			Classifiers       []string `json:"classifiers"`
		} `json:"info"`
		Releases map[string][]struct {
			UploadTime string `json:"upload_time"`
		} `json:"releases"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}

	currentDate := ""
	_, onRegistry := info.Releases[dep.Version]
	if releases, ok := info.Releases[dep.Version]; ok && len(releases) > 0 {
		currentDate = releases[0].UploadTime
	}
	latestDate := ""
	if releases, ok := info.Releases[info.Info.Version]; ok && len(releases) > 0 {
		latestDate = releases[0].UploadTime
	}
	libyear := calcLibyear(currentDate, latestDate)

	// Many PyPI packages declare license via trove classifiers instead of
	// info.license. Fall back to classifier parsing when the license field
	// is empty or a sentinel value. This was causing 35.7% of PyPI deps
	// to have empty license data.
	// Precedence: PEP 639 license_expression (already SPDX) wins;
	// then the legacy free-text field; then trove classifiers.
	license := info.Info.LicenseExpression
	if license == "" {
		license = info.Info.License
	}
	if license == "" || strings.EqualFold(license, "UNKNOWN") {
		license = parsePyPIClassifierLicense(info.Info.Classifiers)
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "pypi",
		License:            license,
		Purl:               pkgid.Purl("pypi", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
		CurrentVersion:     dep.Version,
		LatestVersion:      info.Info.Version,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            libyear,
	}, nil
}

// (parsePyprojectVersions replaced by manifest.ParsePyprojectVersionsFromContent which
// correctly handles PEP 621 array format, not just Poetry key=value format.)

// goProxyEscape applies the module proxy protocol's case encoding to a
// module path or version: every uppercase letter becomes '!' followed by
// its lowercase form (github.com/Masterminds → github.com/!masterminds).
// The protocol exists so a proxy can serve from a case-insensitive file
// system; proxy.golang.org answers 404 for the unencoded spelling. Same
// rule as golang.org/x/mod/module.EscapePath, which is not a dependency
// here. '!' and non-ASCII are not legal in module paths or versions and
// would make the encoding ambiguous, so they are refused.
func goProxyEscape(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '!' || c >= utf8.RuneSelf:
			return "", fmt.Errorf("go module path or version %q cannot be case-encoded for the module proxy: %w", s, errRegistryCannotBeAsked)
		case 'A' <= c && c <= 'Z':
			b.WriteByte('!')
			b.WriteByte(c + ('a' - 'A'))
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

// resolveGoLibyear checks the Go proxy for module version dates.
func resolveGoLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	// Go proxy API: https://proxy.golang.org/{module}/@v/{version}.info,
	// with the module path and version case-encoded (goProxyEscape).
	module, err := goProxyEscape(dep.Name)
	if err != nil {
		return nil, err
	}
	latestBody, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(goProxyBase+"/%s/@latest", module))
	if err != nil {
		return nil, err
	}
	var latestInfo struct {
		Version string `json:"Version"`
		Time    string `json:"Time"`
	}
	if err := json.Unmarshal(latestBody, &latestInfo); err != nil {
		return nil, err
	}

	currentDate := ""
	if dep.Version != "" {
		// go.mod versions keep their "v" (manifest.ParseGoModVersions); add one only
		// when it is missing. Prefixing unconditionally requested
		// @v/vv1.2.3.info, which never exists.
		version := dep.Version
		if !strings.HasPrefix(version, "v") {
			version = "v" + version
		}
		version, verr := goProxyEscape(version)
		if verr != nil {
			return nil, verr
		}
		// A failure that says nothing (timeout, 5xx, rate limit) FAILS the
		// dependency; only a definitive "no such version" costs the date.
		// Swallowing it stored libyear 0 — which reads as "perfectly
		// fresh" — and since v0.29.56 the answer is cached for a day, so
		// one blip would have fabricated that 0 for every repo in the
		// process. Same rule as resolveMavenLibyear's .pom dates.
		curBody, cerr := fetchRegistryJSON(ctx,
			fmt.Sprintf(goProxyBase+"/%s/@v/%s.info", module, version))
		if cerr != nil && !isDefinitiveRegistryMiss(cerr) {
			return nil, cerr
		}
		if cerr == nil {
			var curInfo struct {
				Time string `json:"Time"`
			}
			if uerr := json.Unmarshal(curBody, &curInfo); uerr != nil {
				return nil, fmt.Errorf("go proxy %s@%s .info: %w", dep.Name, version, uerr)
			}
			currentDate = curInfo.Time
		}
	}

	// The Go proxy carries no license. scanLibyear adds it from the GitHub
	// repository hosting the module (githubModuleLicense, through the key
	// pool) after this answer is cached, so the license lookup is shared
	// per owner/repo instead of repeated per version.
	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "go",
		CurrentVersion:     dep.Version,
		LatestVersion:      strings.TrimPrefix(latestInfo.Version, "v"),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestInfo.Time,
		Libyear:            calcLibyear(currentDate, latestInfo.Time),
		Purl:               pkgid.Purl("golang", dep.Name, dep.Version),
	}, nil
}

// resolveCargoLibyear checks crates.io for Rust crate versions.
func resolveCargoLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(cratesRegistryBase+"/api/v1/crates/%s", dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		Crate struct {
			NewestVersion string `json:"newest_version"`
		} `json:"crate"`
		Versions []struct {
			Num       string `json:"num"`
			CreatedAt string `json:"created_at"`
			License   string `json:"license"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}

	currentDate := ""
	onRegistry := false
	latestDate := ""
	license := ""
	// Normalize version for matching: Cargo.toml may say "1.0" but crates.io
	// lists "1.0.0". Without normalization, the version match fails and
	// the license is lost (was causing 19% of cargo deps to have empty license).
	normalizedVersion := normalizeSemanticVersion(dep.Version)
	for _, v := range info.Versions {
		if v.Num == dep.Version || v.Num == normalizedVersion {
			onRegistry = true
			currentDate = v.CreatedAt
			license = v.License
		}
		if v.Num == info.Crate.NewestVersion {
			latestDate = v.CreatedAt
			// Fallback: if current version wasn't found, use latest version's license.
			if license == "" {
				license = v.License
			}
		}
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "cargo",
		CurrentVersion:     dep.Version,
		License:            license,
		Purl:               pkgid.Purl("cargo", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
		LatestVersion:      info.Crate.NewestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

// joinRegistryLicenseList stores a registry's license ARRAY (RubyGems
// licenses, Composer license, Hex licenses) as one SPDX expression joined
// with OR (worklist 53, decision 2; summary/39). Composer documents the
// array as a choice; RubyGems and Hex leave the relationship unstated, and
// dual-licensing is the common reading (the SPDX export says so in
// licenseComments). Until v0.29.67 the join was " AND ", the operator for
// "comply with every license", which inverted the obligation. Empty entries
// are skipped, and a compound entry is parenthesized so the OR cannot change
// its meaning (spdx.JoinExpressions, the one join).
func joinRegistryLicenseList(list []string) string {
	return spdx.JoinExpressions("OR", list)
}

// resolveRubyGemsLibyear checks rubygems.org for gem versions.
func resolveRubyGemsLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(rubygemsRegistryBase+"/api/v1/versions/%s.json", dep.Name))
	if err != nil {
		return nil, err
	}
	var versions []struct {
		Number    string   `json:"number"`
		CreatedAt string   `json:"created_at"`
		Licenses  []string `json:"licenses"`
	}
	if err := json.Unmarshal(body, &versions); err != nil {
		return nil, err
	}

	latestVersion := ""
	latestDate := ""
	currentDate := ""
	license := ""
	latestLicense := "" // Fallback: license from latest version if specific version lacks one.
	if len(versions) > 0 {
		latestVersion = versions[0].Number
		latestDate = versions[0].CreatedAt
		if len(versions[0].Licenses) > 0 {
			latestLicense = joinRegistryLicenseList(versions[0].Licenses)
		}
	}
	for _, v := range versions {
		if v.Number == dep.Version {
			currentDate = v.CreatedAt
			if len(v.Licenses) > 0 {
				license = joinRegistryLicenseList(v.Licenses)
			}
			break
		}
	}
	// Old gem versions often lack license metadata (e.g., authlogic v2.1.3
	// returns licenses=null). Fall back to the latest version's license since
	// the project license typically hasn't changed. Was causing 86% of
	// RubyGems deps to have empty license data.
	if license == "" {
		license = latestLicense
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		License:            license,
		Purl:               pkgid.Purl("gem", dep.Name, dep.Version),
		PackageManager:     "rubygems",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

// recordInventoryAttempt records, in the coverage record, that this analysis
// did not replace the repository's inventory, and why. It is the one writer
// of that fact. A shutdown is not a failed attempt and records nothing; a
// failure to record is logged and never replaces the analysis's own error.
func (ac *AnalysisCollector) recordInventoryAttempt(ctx context.Context, repoID int64, result string) {
	recordInventoryAttempt(ctx, ac.store, ac.logger, repoID, result)
}

// inventoryAttemptStore is the one store call recordInventoryAttempt makes.
type inventoryAttemptStore interface {
	RecordInventoryAttempt(ctx context.Context, repoID int64, result string) error
}

func recordInventoryAttempt(ctx context.Context, store inventoryAttemptStore, logger *slog.Logger, repoID int64, result string) {
	if ctx.Err() != nil {
		return
	}
	if err := store.RecordInventoryAttempt(ctx, repoID, result); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("failed to record that the dependency inventory was not replaced",
			"repo_id", repoID, "result", result, "error", err)
	}
}

// RecordAnalysisSkippedForNoClone records, in the coverage record, that a
// repository's analysis did not run because it has no clone. It is for the
// scheduler, whose own guard returns before the analysis is entered (it also
// skips the scorecard). The scheduler does not log this as a failure: it has
// already logged why there is no clone. Going through here keeps one writer
// of the attempt columns and one set of rules for it (nothing on shutdown; a
// failure to record is a WARN and no more).
func RecordAnalysisSkippedForNoClone(ctx context.Context, store inventoryAttemptStore, logger *slog.Logger, repoID int64) {
	recordInventoryAttempt(ctx, store, logger, repoID, model.InventoryAttemptCloneUnavailable)
}

// relativeToClone is path relative to the clone's root, with forward slashes:
// how a manifest is named in the coverage record. A path that is somehow not
// under the root is returned as it is.
func relativeToClone(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

// inventoryNote builds the note for a declared dependency that gets no row.
// A note is a record, not identity: its name is stored sanitized and cut to
// the name bound, and its key is computed from that stored name. It carries
// the purl the dependency would have had only when one can be built and the
// sanitizer would not change it, and never for a non-registry dependency,
// which is NOT the registry package of the same name.
func inventoryNote(dep model.LibyearDep, kind string) model.InventoryNote {
	name := textclean.Sanitize(dep.Name)
	if len(name) > pkgid.MaxNameBytes {
		name = strings.ToValidUTF8(name[:pkgid.MaxNameBytes], "")
	}
	n := model.InventoryNote{
		Kind: kind, PackageManager: textclean.Sanitize(dep.Manager), Name: name,
		PackageKey: pkgid.Key(dep.Manager, name), ManifestPath: dep.ManifestPath,
	}
	if kind != model.NoRowNonRegistry && !textclean.WouldChange(dep.Name) && len(dep.Name) <= pkgid.MaxNameBytes {
		if purl, ok := manifestPurl(dep); ok && !textclean.WouldChange(purl) {
			n.Purl = purl
		}
	}
	return n
}

// manifestPurl builds a declared dependency's purl from the manifest alone,
// before any lookup: the purl the dependency's resolver builds when it
// answers, byte for byte (TestManifestPurlIsThePurlTheResolverBuilds), since
// a finding's identity includes it. ok is false when the dependency has no
// purl: its package manager has no purl type, its purl type requires a
// namespace and the coordinate has none (a Maven artifact with no group, a
// Composer platform name), or it is a SwiftPM package whose address is not a
// repository URL.
func manifestPurl(dep model.LibyearDep) (purl string, ok bool) {
	if dep.Name == "" {
		return "", false
	}
	if dep.Manager == "githubactions" {
		return pkgid.Purl("githubactions", dep.Name, dep.Version), true
	}
	typ, known := pkgid.PurlTypeOfManager(dep.Manager)
	if !known {
		return "", false
	}
	name := dep.Name
	switch dep.Manager {
	case "maven":
		name = strings.Replace(name, ":", "/", 1)
	case "swiftpm":
		// The purl's namespace is the repository's host and owner, which
		// the manifest gives as the package's URL.
		ref, parsed := manifest.ParseSwiftPackageURL(dep.Requirement)
		if !parsed {
			return "", false
		}
		name = ref.Namespace + "/" + ref.Repo
	}
	if pkgid.NamespaceRequired(typ) && !pkgid.NameHasNamespace(name) {
		return "", false
	}
	return pkgid.Purl(typ, name, dep.Version), true
}

// unansweredLibyearRow builds the inventory row of a dependency its registry
// did not answer for: what the manifest declares, no libyear, and the reason
// (not on the registry; cannot be asked; no answer this cycle). ok is false
// when the dependency has no purl, in which case it gets no row.
func unansweredLibyearRow(dep model.LibyearDep, err error) (row *model.LibyearRow, ok bool) {
	purl, ok := manifestPurl(dep)
	if !ok {
		return nil, false
	}
	return &model.LibyearRow{
		Name: dep.Name, Requirement: dep.Requirement, Type: dep.Type, PackageManager: dep.Manager,
		CurrentVersion: dep.Version, Purl: purl, NoLibyear: true, ManifestPath: dep.ManifestPath,
		Status: libyearStatus(dep, nil, err),
	}, true
}

// storableLibyearRow makes an in-memory inventory row safe to store, and
// reports whether it may be stored at all (summary/70, 4.7).
//
// The inventory is replaced in one transaction, so one row PostgreSQL refuses
// would refuse the whole inventory, every cycle, for good. Rows are therefore
// made storable here, in Go, before the transaction: there is no savepoint
// per row and no reading of error codes.
//
//   - The name is IDENTITY and is never repaired (SR-6): a name the sanitizer
//     would change gets no row. A cleaned name is a different name, and may be
//     another package's.
//   - A version is not identity, and one the sanitizer would change is not a
//     version: the row is kept with no version and its purl without one, the
//     existing "no version declared" path. Manifest versions are already
//     cleaned by the version normaliser, so in practice this is an Actions
//     ref.
//   - Every other text column is sanitized and kept, whatever its origin: a
//     requirement string from a manifest, a licence or a latest version or a
//     date decoded from a registry's JSON (any of which can hold a NUL).
//
// The length bound on a name and the note that records a dependency given no
// row arrive with the notes table (4.2); until then the collection loop
// counts such a dependency and logs one line per repository.
func storableLibyearRow(row *model.LibyearRow) bool {
	if textclean.WouldChange(row.Name) {
		return false
	}
	if textclean.WouldChange(row.CurrentVersion) {
		row.CurrentVersion = ""
		row.Purl = pkgid.ReplaceVersion(row.Purl, "")
	}
	row.Requirement = textclean.Sanitize(row.Requirement)
	row.Type = textclean.Sanitize(row.Type)
	row.PackageManager = textclean.Sanitize(row.PackageManager)
	row.LatestVersion = textclean.Sanitize(row.LatestVersion)
	row.CurrentReleaseDate = textclean.Sanitize(row.CurrentReleaseDate)
	row.LatestReleaseDate = textclean.Sanitize(row.LatestReleaseDate)
	row.License = textclean.Sanitize(row.License)
	row.Purl = textclean.Sanitize(row.Purl)
	return true
}

// libyearStatus is the ONE function that says why a dependency's row has a
// libyear or lacks one (summary/70, 4.1.8), from what the three sites that
// know a reason hold: the dependency, the resolver's row, the resolver's
// error. It returns one of model's Libyear* constants.
//
// The order is the order of certainty. An ecosystem with no timeline has none
// whatever was looked up. An error is the resolver's own account: the
// registry's definitive "no such package", a coordinate that cannot be asked
// about, or (everything else, a row-less answer with no error included) no
// answer this cycle. Then the row's dates, asked of calcLibyear's OWN parser
// and not of emptiness (v0.29.57): a date the parser cannot read is a missing
// date. With no usable pair of dates, a dependency that declares no version
// has nothing to date; one that declares a version got no date for it.
//
// Whether the registry HAS the declared version is a separate fact
// (LibyearRow.VersionFound) and does not change the reason: four resolvers
// cannot tell a version they lack from a version with no date
// (resolverCanProveVersionMiss, the table libyear_status_test.go holds the
// resolvers to), so no reason may depend on it.
func libyearStatus(dep model.LibyearDep, row *model.LibyearRow, err error) string {
	if dep.Manager == "githubactions" {
		return model.LibyearNoTimeline
	}
	switch {
	case err != nil && isDefinitiveRegistryMiss(err):
		return model.LibyearNotOnRegistry
	case err != nil && errors.Is(err, errRegistryCannotBeAsked):
		return model.LibyearCannotBeAsked
	case err != nil || row == nil:
		return model.LibyearLookupFailed
	}
	_, okCurrent := parseLibyearDate(row.CurrentReleaseDate)
	_, okLatest := parseLibyearDate(row.LatestReleaseDate)
	switch {
	case okCurrent && okLatest:
		return model.LibyearResolved
	case dep.Version == "":
		return model.LibyearNoVersion
	default:
		return model.LibyearNoReleaseDate
	}
}

// settleLibyear gives a resolved row its status and makes the stored libyear
// agree with it: anything but "resolved" is stored as NULL, never as 0.
//
// calcLibyear returns 0 when either release date is missing or unparseable,
// which is indistinguishable from a dependency that really is on the latest
// release (v0.29.57). Measured on chaoss.tv on 2026-09-17: 1,704,902 of the
// 3,196,143 rows carrying a number had a missing date, so 87% of every row
// reading "libyear = 0" meant "unknown" — and avg() counts a zero while it
// skips a NULL, which reported the fleet at 1.335 years against an honest
// 2.861.
//
// Applied once, where rows are collected — a dozen resolvers each building
// their own row is exactly where a rule like this drifts.
func settleLibyear(dep model.LibyearDep, row *model.LibyearRow) {
	if row == nil {
		return
	}
	row.Status = libyearStatus(dep, row, nil)
	if row.NoLibyear && row.Status == model.LibyearResolved {
		// A row that arrives already marked "no libyear" keeps the mark,
		// as it always has. The one producer of such a row is the path for
		// an ecosystem with no timeline, so that is what its status says.
		row.Status = model.LibyearNoTimeline
	}
	if row.Status != model.LibyearResolved {
		row.NoLibyear = true
		// Never carry a number alongside the NULL: it would read as a
		// measurement if anything ever dropped the flag.
		row.Libyear = 0
	}
}

// stampVersionSource records where the row's version came from, and for a
// locked version builds the purl the way the scan does for a lockfile
// version (pkgid.WithVersion over the row's purl), so the purl a row stores
// and the purl the scan targets for that version are the same bytes.
func stampVersionSource(dep model.LibyearDep, row *model.LibyearRow) {
	if row == nil {
		return
	}
	row.VersionSource = dep.VersionSource
	row.VersionReason = dep.VersionReason
	row.VersionHosts = dep.VersionHosts
	if dep.VersionSource == versionuse.SourceLocked && row.Purl != "" {
		row.Purl = pkgid.WithVersion(row.Purl, dep.Version)
	}
}

// versionFound is the answer a resolver that CAN prove a miss gives for a
// declared version: not established when none was declared.
func versionFound(declared string, onRegistry bool) model.VersionFound {
	switch {
	case declared == "":
		return model.VersionNotEstablished
	case onRegistry:
		return model.VersionOnRegistry
	default:
		return model.VersionNotOnRegistry
	}
}

// libyearDateLayouts are the formats parseLibyearDate accepts.
//
// The list is NOT the safeguard — sharing it was tried and was not enough
// (v0.29.57 round 2). "Can this be dated?" and "date it" agree because both
// go through parseLibyearDate itself, which also rejects the zero time; a
// second caller doing time.Parse against this list would re-create the
// fabricated zero. Use the function, not the list.
var libyearDateLayouts = []string{
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	time.RFC3339,
}

// parseLibyearDate is the ONE reader for a release date: the single place
// that decides both "can this be dated?" and "what date is it?".
//
// A shared layout LIST was not enough (v0.29.57 round 2): calcLibyear's real
// predicate is "parses AND is not the zero time", and a second spelling that
// only checked parsing called "0001-01-01T00:00:00Z" a known date while
// calcLibyear still returned 0 — re-creating the fabricated zero this
// release exists to remove. Reachable, not theoretical:
// fetchRegistryLastModified re-formats whatever http.ParseTime accepts, so a
// mirror sending "Last-Modified: Mon, 01 Jan 0001 00:00:00 GMT" produces
// exactly that string on the path v0.29.56 moved Maven onto.
func parseLibyearDate(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range libyearDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, !t.IsZero()
		}
	}
	return time.Time{}, false
}

func calcLibyear(currentDate, latestDate string) float64 {
	current, okCur := parseLibyearDate(currentDate)
	latest, okLat := parseLibyearDate(latestDate)
	if !okCur || !okLat {
		return 0
	}
	days := latest.Sub(current).Hours() / 24
	return days / 365.0
}

// normalizeSemanticVersion pads a version string to 3 parts (major.minor.patch).
// "1.0" becomes "1.0.0", "1" becomes "1.0.0". Versions with pre-release suffixes
// (e.g., "0.1.0-beta") are returned as-is. Fixes crates.io version matching where
// Cargo.toml may say "1.0" but crates.io lists "1.0.0".
func normalizeSemanticVersion(v string) string {
	if v == "" {
		return ""
	}
	// Don't modify versions with pre-release suffixes.
	if strings.Contains(v, "-") || strings.Contains(v, "+") {
		return v
	}
	parts := strings.Split(v, ".")
	for len(parts) < 3 {
		parts = append(parts, "0")
	}
	return strings.Join(parts[:3], ".")
}

// parsePyPIClassifierLicense extracts license from PyPI trove classifiers.
// Many Python packages declare license via classifiers instead of info.license.
// Returns the best SPDX-like identifier, or empty string if no license found.
func parsePyPIClassifierLicense(classifiers []string) string {
	for _, c := range classifiers {
		if !strings.HasPrefix(c, "License :: OSI Approved :: ") {
			continue
		}
		name := strings.TrimPrefix(c, "License :: OSI Approved :: ")
		// Map common classifier names to SPDX identifiers.
		switch {
		case strings.Contains(name, "MIT"):
			return "MIT"
		case strings.Contains(name, "Apache"):
			return "Apache-2.0"
		// v0.28.1: the Lesser arm must run BEFORE the GPLv3/GPLv2
		// arms — "LGPLv3" CONTAINS "GPLv3" as a substring, so the
		// old ordering misclassified every LGPL-v3 classifier as
		// GPL-3.0. The single "Lesser General Public License"
		// needle also covers the legacy trove wording "GNU Library
		// or Lesser General Public License (LGPL)" (the operator's
		// LGPL-shown-as-not-OSI report — "Library OR Lesser" misses
		// a "GNU Lesser…" prefix match).
		case strings.Contains(name, "Lesser General Public License"):
			// v0.28.5 (review-lens finding; rationale updated
			// v0.28.7): keep the VERSION the classifier carries —
			// collapsing "LGPLv3" to bare LGPL would discard the
			// v3-only information into the ambiguous version-
			// unspecified family bucket (v0.28.6) that downstream
			// consumers cannot narrow back. Or-later forms first
			// ("LGPLv3+" also Contains "v3").
			switch {
			case strings.Contains(name, "v3 or later"), strings.Contains(name, "LGPLv3+"):
				return "LGPL-3.0-or-later"
			case strings.Contains(name, "v3"):
				return "LGPL-3.0-only"
			case strings.Contains(name, "v2 or later"), strings.Contains(name, "LGPLv2+"):
				return "LGPL-2.0-or-later"
			case strings.Contains(name, "v2.1"):
				return "LGPL-2.1-only"
			case strings.Contains(name, "v2"):
				return "LGPL-2.0-only"
			default:
				// Version-unspecified ("GNU Library or Lesser
				// General Public License (LGPL)") — stays the bare
				// "LGPL" family bucket (v0.28.6): no specific SPDX
				// id (incl. -or-later, which would assert a
				// choose-later-versions grant) matches what the
				// source actually says.
				return "LGPL"
			}
		case strings.Contains(name, "GPLv3"):
			return "GPL-3.0"
		case strings.Contains(name, "GPLv2"):
			return "GPL-2.0"
		case strings.Contains(name, "GNU General Public License v3"):
			return "GPL-3.0"
		case strings.Contains(name, "GNU General Public License v2"):
			return "GPL-2.0"
		case strings.Contains(name, "BSD"):
			return "BSD"
		case strings.Contains(name, "ISC"):
			return "ISC"
		case strings.Contains(name, "Mozilla Public License 2.0"):
			return "MPL-2.0"
		case strings.Contains(name, "Eclipse"):
			return "EPL"
		case strings.Contains(name, "Artistic"):
			return "Artistic"
		case strings.Contains(name, "Zlib"):
			return "Zlib"
		case strings.Contains(name, "Unlicense"):
			return "Unlicense"
		default:
			// Return the classifier name as-is, trimmed of " License" suffix.
			return strings.TrimSuffix(name, " License")
		}
	}
	return ""
}

// ============================================================
// SCC (code complexity / repo labor)
// ============================================================

func (ac *AnalysisCollector) scanSCC(ctx context.Context, repoID int64, workDir string, result *AnalysisResult) error {
	// Check if scc is installed.
	sccPath, err := exec.LookPath("scc")
	if err != nil {
		ac.logger.Info("scc not installed, skipping repo_labor analysis")
		return nil
	}

	ac.logger.Info("running scc for code complexity", "repo_id", repoID)

	// v0.29.14: scc's stdout is STREAMED, never buffered. The pre-v0.29.14
	// code read the whole report into a bytes.Buffer and then
	// json.Unmarshal'd it, holding the raw JSON, every sccFile struct and
	// every labor row live at the same time — and bytes.Buffer grows by
	// DOUBLING. On 2026-09-15 repo 144636
	// (opendatahub-io/odh-build-metadata, a 73 GB store of generated build
	// metadata) drove that buffer to 1 GiB; the doubling to 2 GiB asked the
	// kernel for one contiguous 2 GiB mapping, kate runs
	// vm.overcommit_memory=2 so the mmap was refused, and the process died
	// with "fatal error: runtime: out of memory" — taking all 70 collection
	// workers and every background subsystem with it.
	//
	// Streaming holds only the labor rows, each individually allocated, so
	// no large contiguous request is ever made. Measured against
	// benchSCCDoc(1_000_000) (a 479 MB report) with a faithful replica of
	// the old path — a bytes.Buffer filled in 32 KiB writes, the way
	// os/exec fills it: 397 MB allocated vs 1,793 MB, a 4.5x reduction,
	// with CPU and allocation COUNT unchanged (the win is bytes, not
	// churn). An earlier note here claimed 9.7x; that came from growing a
	// raw []byte with append, which is not how bytes.Buffer grows.
	//
	// Derived context (same contract the facade's git log reader
	// documents, v0.27.105): when decoding fails while scc may still be
	// writing, scc is killed BEFORE the drain below, so a doomed report is
	// abandoned rather than read to the end.
	sccCtx, cancelSCC := context.WithCancel(ctx)
	defer cancelSCC()

	// The file the scan runs, recorded before it starts: the version read
	// after the scan labels the rows only if this is still the file at
	// sccPath (an upgrade-tools during the scan renames a new binary into
	// place while this scan runs the old one). See sccDataSource.
	scanned, scannedErr := os.Stat(sccPath)

	// scc runs from an empty directory with no SCC_CONFIG_PATH, so no host
	// config changes what it counts (see sccCommand); workDir must then be
	// absolute.
	absWork, err := filepath.Abs(workDir)
	if err != nil {
		return fmt.Errorf("scc: resolve the checkout %s: %w", workDir, err)
	}
	cmd, cleanupCmd, err := sccCommand(sccCtx, sccPath, "-f", "json", "--by-file", absWork)
	if err != nil {
		return fmt.Errorf("scc failed to start: %w", err)
	}
	defer cleanupCmd()

	started := time.Now()

	// startSweptCommand owns the stdout pipe and sweeps scc's process
	// group when the leader exits; see its doc for why cmd.StdoutPipe()
	// wedges this function and what WaitDelay does and does not buy.
	swept, err := startSweptCommand(cmd)
	if err != nil {
		return fmt.Errorf("scc failed to start: %w", execErr(ctx, err))
	}
	defer swept.Close()

	counted := &countingReader{r: swept.Stdout}
	dec := json.NewDecoder(counted)
	now := time.Now()
	var laborRows []*db.RepoLaborRow
	// The rows are held until the snapshot is written, one for a file, so
	// their count is bounded (0.29.216): at the first row past
	// sccLaborRowsLimit scc is stopped and the scan FAILS below, like any
	// report that could not be taken, with nothing written.
	tooMany := false
	decodeErr := streamSCCLabor(dec, absWork, now, func(row *db.RepoLaborRow) {
		if tooMany {
			return
		}
		if len(laborRows) >= sccLaborRowsLimit {
			tooMany = true
			cancelSCC()
			return
		}
		laborRows = append(laborRows, row)
		if sccRowStreamed != nil {
			sccRowStreamed()
		}
	})
	// A decode failure makes the rest of the report worthless: kill scc
	// rather than draining a scan that may still have minutes of work left
	// — but ONLY when scc may still be writing. An EOF-class error means
	// the write end is already closed, so there is nothing to abandon and
	// the drain returns at once. Not killing in that case is what lets a
	// death we did not cause stay visible: the kernel OOM-killer sends the
	// same SIGKILL we do, and killing unconditionally made an OOM-killed
	// scc indistinguishable from our own kill (round 3, F2). weKilled
	// records the one case where a SIGKILL is ours.
	weKilled := false
	if decodeErr != nil && !sccStreamEnded(decodeErr) {
		cancelSCC()
		weKilled = true
	}
	if tooMany {
		// Our own stop, and the cause: whatever the decoder then said of
		// the stream that ended under it is not the reason.
		weKilled = true
		laborRows = nil
		decodeErr = fmt.Errorf("%w: more than %d", errSCCTooManyFiles, sccLaborRowsLimit)
	}

	// Drain BEFORE reaping. streamSCCLabor stops at the top-level "]", so
	// anything scc writes after that stays in the pipe — and once it
	// exceeds the OS pipe buffer (64 KiB) scc blocks in write() while
	// cmd.Wait() blocks on it, wedging this worker FOREVER. Reproduced at
	// exactly that boundary: 60,000 trailing bytes returned, 100,000 hung.
	// The pre-v0.29.14 code could not hit this because cmd.Stdout drained
	// to EOF; the streaming rewrite has to drain explicitly. A ctx cancel
	// still unblocks this: cmd.Cancel kills scc's whole process group, so
	// every holder of the write end dies and the pipe reaches EOF. Draining
	// also makes counted.n the true report size
	// rather than "the bytes the decoder happened to consume".
	// Drain the decoder's LEFTOVER buffer first, then the pipe.
	// json.Decoder reads ahead in chunks, so by the time it consumed the
	// top-level "]" it has typically pulled ~445 more bytes off the pipe
	// into its own buffer. Draining only `counted` therefore missed any
	// trailing garbage that arrived in the SAME write as the report —
	// measured: 1 and 50 trailing bytes were silently ACCEPTED, and 500
	// were reported as "55 bytes". Whether garbage was caught came down to
	// where scc happened to split its writes. dec.Buffered() is that
	// leftover; MultiReader puts it back in front of the pipe.
	//
	// DECLINED (round 2, R2-6): killing scc on the first non-whitespace
	// byte to bound the drain. It was implemented and reverted — the kill
	// makes cmd.Wait() report our own "signal: killed", which then
	// pre-empts the trailing-data error this drain exists to raise, and
	// reports a self-inflicted cause to the operator. The unbounded case
	// is hypothetical (scc 3.7.0 emits nothing after "]") and drains at
	// pipe speed, so gigabytes cost seconds, not a wedge.
	trailing := &nonSpaceCounter{}
	_, _ = io.Copy(trailing, io.MultiReader(dec.Buffered(), counted))

	waitErr := swept.Wait()

	if decodeErr != nil {
		// scc's OWN failure is usually the real cause of a short report,
		// so it is joined in. It is read from ProcessState, NOT from
		// waitErr: after our own kill, waitErr can be an injected ctx
		// error rather than scc's status (round 3, F1).
		//
		// execErr (SR-18, v0.28.18 pass 35) then returns ctx.Err() when a
		// `stop serve` landed inside scc, discarding the cause, so a
		// shutdown classifies as a cancellation whatever happened here.
		//
		// Nothing is logged here. scanSCC's errors are logged once, by
		// logAnalysisPhaseErrors in AnalyzeRepo, which also skips
		// cancellations. v0.29.16 logged a WARN here, and it was the ONLY
		// log line for this failure (separate wait_error and decode_error
		// fields; AnalyzeRepo logged just the error count). The unreleased
		// v0.29.17 added logAnalysisPhaseErrors but kept that WARN, so in
		// that tree every scc abnormal exit appeared twice. v0.29.18
		// removed the WARN (round 4, R4-2) and, with it, the ctx gate the
		// WARN needed to stay quiet on shutdown (round 3, F3). execErr
		// already makes a shutdown classify as a cancellation. The history
		// in this paragraph was itself misstated once (round 5, F2).
		cause := decodeErr
		if own := sccOwnFailure(cmd.ProcessState, weKilled); own != nil {
			cause = errors.Join(decodeErr, own)
		}
		return fmt.Errorf("parsing scc output: %w", execErr(ctx, cause))
	}
	if waitErr != nil {
		return fmt.Errorf("scc failed: %w", execErr(ctx, waitErr))
	}
	if trailing.n > 0 {
		// json.Unmarshal rejected any non-space data after the top-level
		// value; the streaming walk stops at "]" and would silently accept
		// it, so restore the rejection. Trailing WHITESPACE stays legal —
		// a future scc adding a newline is not a corrupt report. scc 3.7.0
		// emits nothing after "]" (verified with xxd).
		return fmt.Errorf("scc output: %d bytes of trailing data after the top-level array", trailing.n)
	}

	// Observation only (SR-7): nothing here skips or truncates a repo (the
	// one cap is sccLaborRowsLimit above, which fails the scan). The WARN exists so the frequency of the repo class that caused
	// the 2026-09-15 OOM stays greppable now that it no longer announces
	// itself by killing the process. See sccOutputLargeBytes for what the
	// threshold is and — importantly — what it is not: scc_output_bytes
	// did not exist before this release, so no repo can be SAID to have
	// crossed it; what the 2026-09-06..15 log shows is that of 135,089
	// repos exactly one failed to complete scc at all.
	if counted.n >= sccOutputLargeBytes {
		ac.logger.Warn("scc produced a very large per-file report — labor rows are held in memory until the snapshot is written; this is the repo class that OOM-killed the scheduler before v0.29.14",
			"repo_id", repoID, "scc_output_bytes", counted.n,
			"labor_files", len(laborRows), "duration", time.Since(started).String())
	} else {
		ac.logger.Info("scc complete", "repo_id", repoID,
			"scc_output_bytes", counted.n, "labor_files", len(laborRows),
			"duration", time.Since(started).String())
	}

	// 0.29.144: every row records the scc that produced it. Probed only
	// now, after a scan that succeeded: a failed scan writes nothing, and a
	// probe that fails falls back to "scc" (version unknown) without
	// failing the scan.
	dataSource := ac.sccDataSource(ctx, sccPath, repoID, scanned, scannedErr)

	// v0.27.7: ONE atomic snapshot replace per analysis run. The store
	// rotates the previous snapshot to repo_labor_history and inserts
	// the fresh rows in the SAME transaction — rotation can neither be
	// skipped nor applied per-chunk, and a mid-insert failure rolls
	// the rotation back too (the previous snapshot stays current).
	// scc failures return above BEFORE this call, so a failed scan
	// never rotates the previous snapshot away. A successful scan with
	// zero source files still replaces (empty snapshot is the current
	// truth). Do NOT revert to per-file inserts — that is the
	// unbounded-growth bug (2.0M rows / 29 GB in production) that
	// v0.27.7 fixed; TestScanSCCUsesAtomicSnapshotReplace pins this.
	if err := ac.store.ReplaceRepoLaborSnapshot(ctx, repoID, dataSource, laborRows); err != nil {
		return fmt.Errorf("replacing repo_labor snapshot: %w", err)
	}
	result.LaborFiles += len(laborRows)

	return nil
}

// errSCCTooManyFiles: scc reported more files than a worker keeps rows for.
var errSCCTooManyFiles = errors.New("scc reported more files than are kept for one repository")

// sccLaborRowsLimit is the most per-file rows scanSCC holds for one
// repository: cloneWalkEntries, the bound a worker's own walk of a clone
// has, since scc reports one row for a file (215 bytes a row measured, so
// about 1 GiB at the limit; PROVISIONAL with that constant). A variable only
// so that a test can reach the limit with a handful of rows
// (TestScanSCCStopsAtMoreFilesThanAreKept pins its value).
var sccLaborRowsLimit = cloneWalkEntries

// sccDataSourceUnknown is repo_labor.data_source for rows whose scc version
// is not known: every row written before 0.29.144, and a scan whose version
// probe failed.
const sccDataSourceUnknown = "scc"

// sccVersionProbeTimeout bounds `scc --version`. Printing a version does no
// work; the bound only keeps a wedged binary from holding the collection
// worker, and is far above a cold start of a Go binary on a loaded host.
const sccVersionProbeTimeout = 30 * time.Second

// sccVersionOutputMaxBytes is the most of `scc --version`'s output the probe
// reads (and a failed probe logs): over five times the longest version line
// scc prints ("scc version 4.2.0 (beta)" is 24 bytes), rounded up to a power
// of two — enough to show what answered instead, never a whole report.
const sccVersionOutputMaxBytes = 128

// sccVersionLine is the first line `scc --version` prints: "scc version
// 3.7.0", "scc version 4.2.0 (beta)". The version is a release number with
// an optional pre-release suffix and an optional parenthesised label, and
// nothing else, so no other output (a fake, a wrapper script, an error
// message) is ever stored as a version.
var sccVersionLine = regexp.MustCompile(`^scc version (v?[0-9]+(?:\.[0-9]+)*(?:-[0-9A-Za-z.]+)?(?: \([0-9A-Za-z. ]+\))?)$`)

// parseSCCVersion reads the version from `scc --version` output.
func parseSCCVersion(out []byte) (string, bool) {
	first, _, _ := strings.Cut(string(out), "\n")
	m := sccVersionLine.FindStringSubmatch(strings.TrimSpace(first))
	if m == nil {
		return "", false
	}
	return m[1], true
}

// probeSCCVersion runs `scc --version` under timeout. It is a swept
// command, like the scan: the process group is swept when the leader exits,
// so nothing the binary spawned outlives the probe (TestScanSCCKillsDetachedStragglers
// covers both runs), and the timeout kills the group of a binary that never
// answers.
func probeSCCVersion(ctx context.Context, sccPath string, timeout time.Duration) (string, error) {
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd, cleanupCmd, err := sccCommand(pctx, sccPath, "--version")
	if err != nil {
		return "", err
	}
	defer cleanupCmd()
	swept, err := startSweptCommand(cmd)
	if err != nil {
		return "", execErr(pctx, err)
	}
	defer swept.Close()
	out, readErr := io.ReadAll(io.LimitReader(swept.Stdout, sccVersionOutputMaxBytes))
	if waitErr := swept.Wait(); waitErr != nil {
		return "", execErr(pctx, waitErr)
	}
	if readErr != nil {
		return "", fmt.Errorf("read `scc --version` output: %w", readErr)
	}
	v, ok := parseSCCVersion(out)
	if !ok {
		first, _, _ := strings.Cut(string(out), "\n")
		return "", fmt.Errorf("unrecognised `scc --version` output %q", first)
	}
	return v, nil
}

// sccCommand builds every scc run (the scan and the version probe; 0.29.144
// review round 3). scc 4.x loads $SCC_CONFIG_PATH and ./.sccconfig from its
// working directory on every run (3.x read neither), and a config can change
// what it counts — excluded directories, languages counted as others,
// complexity off — while the rows still name the pinned version. So scc runs
// with the variable removed and from a fresh empty directory: never serve's
// working directory, and never the checkout, whose own .sccconfig would
// steer its counts. --no-config would do the same on 4.x but is an unknown
// flag to 3.x, which a host runs until its upgrade. Arguments that are paths
// must be absolute. The caller runs cleanup after the command has finished.
func sccCommand(ctx context.Context, sccPath string, args ...string) (*exec.Cmd, func(), error) {
	dir, err := os.MkdirTemp("", "aveloxis-scc-")
	if err != nil {
		return nil, func() {}, fmt.Errorf("create scc's empty working directory: %w", err)
	}
	cmd := exec.CommandContext(ctx, sccPath, args...)
	cmd.Dir = dir
	env := make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "SCC_CONFIG_PATH=") {
			env = append(env, kv)
		}
	}
	cmd.Env = env
	return cmd, func() { _ = os.RemoveAll(dir) }, nil
}

// sccDataSource is repo_labor.data_source for a scan by the scc at sccPath:
// "scc <version>", so a row's counts can be read against the scc that
// produced them (4.x counts lines differently from 3.x). A failed probe is
// logged and falls back to sccDataSourceUnknown — the version is a label on
// the rows, never a reason to discard a successful scan. A shutdown logs
// nothing: the snapshot write that follows reports it.
//
// scanned is the file the scan ran, stat'ed before it started. The probe
// runs the file at sccPath NOW, so its answer is the scan's only if that is
// still the same file (inode, size and modification time — a go install
// renames a new file into place, a copy rewrites it in place). Otherwise,
// or when the scanned file was never identified, the rows say "scc": an
// upgrade during a scan must not label old counts with the new version.
func (ac *AnalysisCollector) sccDataSource(ctx context.Context, sccPath string, repoID int64, scanned os.FileInfo, scannedErr error) string {
	v, err := probeSCCVersion(ctx, sccPath, sccVersionProbeTimeout)
	if err != nil {
		if ctx.Err() != nil {
			return sccDataSourceUnknown
		}
		ac.logger.Warn("could not read the scc version — this scan's repo_labor rows record data_source 'scc' (version unknown)",
			"repo_id", repoID, "error", err)
		return sccDataSourceUnknown
	}
	if scannedErr != nil || scanned == nil {
		ac.logger.Warn("could not identify the scc binary this scan ran — its repo_labor rows record data_source 'scc' (version unknown)",
			"repo_id", repoID, "path", sccPath, "error", scannedErr)
		return sccDataSourceUnknown
	}
	now, err := os.Stat(sccPath)
	if err != nil || !sameSCCBinary(scanned, now) {
		ac.logger.Warn("the scc binary changed during this scan (an upgrade-tools while serve runs?) — its repo_labor rows record data_source 'scc' (version unknown); the next analysis records the new version",
			"repo_id", repoID, "path", sccPath, "probed_version", v, "error", err)
		return sccDataSourceUnknown
	}
	return sccDataSourceUnknown + " " + v
}

// sameSCCBinary reports whether two stats of the scc path are one unchanged
// file.
func sameSCCBinary(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// sccOutputLargeBytes marks a report big enough to be worth noticing. On
// 2026-09-15 repo 144636 drove the old stdout bytes.Buffer to a 1 GiB
// CAPACITY and the doubling to 2 GiB was refused by the kernel.
//
// Two caveats, because the number is easy to over-read: 1 GiB is the
// buffer's capacity at death, not the report size that caused it (a
// bytes.Buffer is between half and fully occupied, so the report was
// somewhere in 512 MiB..1 GiB), and scc never finished, so its true size
// is unknown. What IS known from the 2026-09-06..15 log is that of 135,089
// repos exactly one failed to complete scc at all.
//
// Streaming removed the crash, so a repo past this line no longer breaks
// anything — but its labor rows are still held in memory until the
// snapshot is written, which is the residual risk this release accepted
// deliberately. OBSERVATION only (SR-7): never a skip, truncation or cap.
// Every run logs scc_output_bytes regardless; this only raises the level
// so the rare class is greppable without scanning the whole distribution.
const sccOutputLargeBytes = 1 << 30

// sccRowStreamed is a TEST SEAM, nil in production. scanSCC calls it as
// each labor row is built, which is the only way to observe that rows
// appear WHILE scc is still writing — the property the whole rewrite
// exists for. A source-level pin cannot prove it: banning "cmd.Stdout ="
// and friends inside scanSCC is evaded by a one-line helper that buffers
// the pipe and hands back a bytes.Reader, which a fresh-context review
// demonstrated against an earlier version of this change. The nil default
// is pinned by TestSCCRowStreamedSeamDefaultsToNil.
var sccRowStreamed func()

// countingReader counts bytes read through it. scc's report size is the
// quantity that killed the scheduler, so it is the quantity worth logging
// — the labor row count alone does not capture how much JSON produced it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// nonSpaceCounter counts non-whitespace bytes written to it and discards
// them. Used to drain scc's pipe after the report while still noticing
// trailing garbage, which json.Unmarshal used to reject for us.
type nonSpaceCounter struct{ n int64 }

func (w *nonSpaceCounter) Write(p []byte) (int, error) {
	for _, b := range p {
		// Exactly encoding/json's isSpace, so what counts as "trailing
		// data" here matches what json.Unmarshal rejected before the
		// streaming rewrite. \v, \f, NUL and a BOM are NOT whitespace to
		// either, and are correctly counted.
		switch b {
		case ' ', '\t', '\r', '\n':
		default:
			w.n++
		}
	}
	return len(p), nil
}

// sccOwnFailure returns scc's own failure — a non-zero exit, or death by a
// signal we did not send — or nil when scc succeeded or its only failure
// is the SIGKILL this package sent (weKilled). It reads ProcessState rather
// than Wait's error because after our kill os/exec may hand back an
// injected ctx error instead of scc's status (round 3, F1).
//
// A SIGKILL is only presumed ours when weKilled is set, i.e. only on the
// branch that actually killed. The kernel OOM-killer also sends SIGKILL,
// and on the very repo class this release exists for it is the most
// likely way scc dies; suppressing every SIGKILL hid it (round 3, F2). The
// residual ambiguity — the OOM-killer striking in the instant between a
// non-EOF decode error and our own kill — is accepted: that death is then
// reported as a decode failure, which still fails the scan closed.
func sccOwnFailure(st *os.ProcessState, weKilled bool) error {
	if st == nil || st.Success() {
		return nil
	}
	if weKilled {
		if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
			return nil
		}
	}
	return &exec.ExitError{ProcessState: st}
}

// sccStreamEnded reports whether a decode error means scc's stdout reached
// EOF: the write end is closed, so there is no running report to abandon.
// Every wrap in the decoder uses %w, so errors.Is sees through them.
func sccStreamEnded(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// streamSCCLabor decodes scc's `--by-file` report incrementally from r,
// calling emit once per file.
//
// scc emits [{"Name":..., "Files":[{...}, ...]}, ...]. Walking the outer
// array element-by-element — and each Files array element-by-element
// inside it — means the process holds only what emit retains. Nothing
// large and contiguous is ever allocated, which is what makes this safe
// under vm.overcommit_memory=2. See scanSCC for the incident.
//
// scc 3.7.0 emits "Name" before "Files", so rows stream out as they are
// decoded. Key order is not a correctness dependency: files decoded before
// their language name is known are held for that ONE object and emitted
// when it closes.
// The caller owns the decoder so it can drain dec.Buffered() afterwards —
// the read-ahead past the top-level "]" is part of the report's trailing
// bytes and is invisible from the pipe alone (see scanSCC).
func streamSCCLabor(dec *json.Decoder, workDir string, now time.Time, emit func(*db.RepoLaborRow)) error {
	if err := expectSCCDelim(dec, '['); err != nil {
		return err
	}
	for dec.More() {
		// A null array element is a hard error here, where json.Unmarshal
		// skipped it and carried on (`[null]` gave zero rows and no error;
		// `[null,{…}]` gave the good row). Deliberate: this path fails
		// CLOSED, returning before the snapshot write so the previous
		// snapshot is retained, rather than silently replacing it with a
		// partial one. Unreachable from scc, which marshals a struct.
		if err := streamSCCLanguage(dec, workDir, now, emit); err != nil {
			return err
		}
	}
	return expectSCCDelim(dec, ']')
}

// streamSCCLanguage decodes one language object from dec.
func streamSCCLanguage(dec *json.Decoder, workDir string, now time.Time, emit func(*db.RepoLaborRow)) error {
	if err := expectSCCDelim(dec, '{'); err != nil {
		return err
	}
	var (
		language  string
		named     bool
		seenFiles bool
		pending   []*db.RepoLaborRow // only ever used if Files precede Name
	)
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("reading scc language key: %w", err)
		}
		key, _ := tok.(string)
		// Case-INSENSITIVE, matching encoding/json: Unmarshal prefers an
		// exact key match but also accepts a case-insensitive one, so the
		// pre-v0.29.14 path parsed {"name":…,"files":[…]}. An exact switch
		// here skipped those and returned ZERO rows with NO error — and a
		// zero-row success still replaces the snapshot, rotating the real
		// one into history and installing an empty one. Verified: stream 0
		// rows vs Unmarshal 1 row for "name"/"files" and "NAME"/"FILES"
		// (Copilot review on PR #207). The nested sccFile decode was never
		// affected, because encoding/json does the matching there.
		switch {
		case strings.EqualFold(key, "Name"):
			// Duplicate keys are rejected rather than guessed at.
			// encoding/json is last-wins; this walk cannot be, because
			// rows for an already-decoded Files array have been emitted
			// and buffering them to allow a late overwrite would undo the
			// streaming that this whole rewrite exists for. First-wins
			// would silently stamp the wrong language, and an additive
			// second Files array would silently duplicate rows — in both
			// cases with no error, so a wrong snapshot would replace the
			// real one. scc marshals a Go struct through encoding/json,
			// which cannot emit duplicate keys, so this only fires on a
			// tool that is already misbehaving (round: parity sweep).
			if named {
				return fmt.Errorf("scc output: duplicate %q key in a language object", key)
			}
			if err := dec.Decode(&language); err != nil {
				return fmt.Errorf("decoding scc language name: %w", err)
			}
			named = true
			for _, row := range pending {
				row.Language = language
				emit(row)
			}
			pending = nil
		case strings.EqualFold(key, "Files"):
			if seenFiles {
				return fmt.Errorf("scc output: duplicate %q key in a language object", key)
			}
			seenFiles = true
			err := streamSCCFiles(dec, workDir, now, func(row *db.RepoLaborRow) {
				if named {
					row.Language = language
					emit(row)
					return
				}
				pending = append(pending, row)
			})
			if err != nil {
				return err
			}
		default:
			// Every other scc key is a scalar or a short array, so the
			// RawMessage this materializes is bounded.
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return fmt.Errorf("skipping scc key %q: %w", key, err)
			}
		}
	}
	if err := expectSCCDelim(dec, '}'); err != nil {
		return err
	}
	// "Name" never arrived. Emit anyway rather than silently dropping the
	// files — a short snapshot would rotate the real one into history.
	for _, row := range pending {
		row.Language = language
		emit(row)
	}
	return nil
}

// streamSCCFiles decodes one "Files" array, emitting a row per element.
func streamSCCFiles(dec *json.Decoder, workDir string, now time.Time, emit func(*db.RepoLaborRow)) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("reading scc Files: %w", err)
	}
	if tok == nil {
		return nil // "Files": null — a language with nothing to report
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return fmt.Errorf(`scc output: expected "[" for Files, got %v`, tok)
	}
	// One reused struct: the row built from it copies every field it
	// needs, and strings are immutable, so no per-file sccFile survives.
	var f sccFile
	for dec.More() {
		f = sccFile{} // absent keys must not inherit the previous file
		if err := dec.Decode(&f); err != nil {
			return fmt.Errorf("decoding scc file entry: %w", err)
		}
		emit(sccLaborRow(&f, workDir, now))
	}
	return expectSCCDelim(dec, ']')
}

// sccLaborRow converts one scc file entry into a labor row. FilePath is
// Location relative to workDir; the original Location is kept only when
// filepath.Rel returns an error, i.e. when the two paths cannot be related.
// An absolute Location outside workDir is not an error to Rel and becomes
// a ../ walk. This is unchanged from before the v0.29.14 streaming rewrite;
// TestStreamSCCLaborRelPathHandling is the inventory of each case. (An
// earlier version of this comment said an outside path was preserved; it
// never was — Copilot review on PR #207.)
func sccLaborRow(f *sccFile, workDir string, now time.Time) *db.RepoLaborRow {
	relPath, relErr := filepath.Rel(workDir, f.Location)
	if relErr != nil || relPath == "" {
		relPath = f.Location
	}
	return &db.RepoLaborRow{
		CloneDate:    now,
		AnalysisDate: now,
		FilePath:     relPath,
		FileName:     filepath.Base(f.Location),
		TotalLines:   f.Lines,
		CodeLines:    f.Code,
		CommentLines: f.Comment,
		BlankLines:   f.Blank,
		Complexity:   f.Complexity,
	}
}

func expectSCCDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("scc output: reading %q: %w", want, err)
	}
	got, ok := tok.(json.Delim)
	if !ok || got != want {
		return fmt.Errorf("scc output: expected %q, got %v", want, tok)
	}
	return nil
}

type sccFile struct {
	Location   string `json:"Location"`
	Lines      int    `json:"Lines"`
	Code       int    `json:"Code"`
	Comment    int    `json:"Comment"`
	Blank      int    `json:"Blank"`
	Complexity int    `json:"Complexity"`
}

// ============================================================
// Additional manifest parsers (added in v0.5.4)
// ============================================================

// ============================================================
// Additional libyear version parsers (added in v0.5.4)
// ============================================================

// ============================================================
// Additional libyear resolvers (added in v0.5.4)
// ============================================================

// resolveMavenLibyear resolves a Maven artifact from Maven Central's
// repository (repo1), not the search API.
//
// v0.29.56: search.maven.org timed out or answered 403 for 86 WARN lines
// (5,163 dependencies) in two hours of the 2026-09-17 chaoss.tv log and
// stretched one repo's libyear phase to 40 minutes. Its answers also
// carried no release date for the version in use, so every Maven libyear
// was 0 (0 of 51,510 rows written in the prior 7 days). The repository
// serves maven-metadata.xml (the latest release and the version list)
// and a Last-Modified header on each version's .pom, which is the
// version's publication time — both from its CDN.
func resolveMavenLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	groupID, artifactID, ok := strings.Cut(dep.Name, ":")
	if !ok || groupID == "" || artifactID == "" {
		return nil, fmt.Errorf("invalid maven coordinate: %s: %w", dep.Name, errRegistryCannotBeAsked)
	}
	if strings.Contains(dep.Name, "${") {
		// An unresolved property reference is not a coordinate; sending it
		// only draws a 400/404 (manifest.ParsePomXMLVersions resolves what it can).
		return nil, fmt.Errorf("maven coordinate %s has an unresolved property reference: %w", dep.Name, errRegistryCannotBeAsked)
	}
	// v0.29.180: the configured repositories, in order. The first that has
	// the artifact answers. "Not on a registry" needs EVERY repository to
	// have answered that: a miss on one and a failure on another is a
	// failed lookup, since the artifact may be on the one that did not
	// answer (SR-5).
	configured := mavenRepositoryBases()
	if len(configured) == 0 {
		return nil, fmt.Errorf("no Maven repository is configured (collection.maven_repositories is empty): %w", errRegistryCannotBeAsked)
	}
	// v0.29.186: the effective list, the configured repositories minus the
	// distrusted ones, decided here at the moment of use.
	bases := usableMavenRepositories(ctx)
	if len(bases) == 0 {
		return nil, errRepositoryDistrusted
	}
	var miss, failure error
	for _, base := range bases {
		row, err := resolveMavenLibyearAt(ctx, base, groupID, artifactID, dep)
		switch {
		case err == nil:
			return row, nil
		case errors.Is(err, context.Canceled) || ctx.Err() != nil:
			return nil, err
		case isDefinitiveRegistryMiss(err):
			miss = err
		case failure == nil:
			failure = err
		}
	}
	if failure != nil {
		return nil, failure
	}
	return nil, miss
}

// resolveMavenLibyearAt asks one Maven repository about one artifact.
func resolveMavenLibyearAt(ctx context.Context, base, groupID, artifactID string, dep model.LibyearDep) (*model.LibyearRow, error) {
	artifactBase := base + "/" + strings.ReplaceAll(groupID, ".", "/") + "/" + url.PathEscape(artifactID)
	body, err := fetchRegistryJSON(ctx, artifactBase+"/maven-metadata.xml")
	if err != nil {
		return nil, err
	}
	var meta struct {
		Versioning struct {
			Latest   string   `xml:"latest"`
			Release  string   `xml:"release"`
			Versions []string `xml:"versions>version"`
		} `xml:"versioning"`
	}
	if err := xml.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("maven metadata for %s: %w", dep.Name, err)
	}
	latestVersion := meta.Versioning.Release
	if latestVersion == "" {
		latestVersion = meta.Versioning.Latest
	}
	if latestVersion == "" && len(meta.Versioning.Versions) > 0 {
		latestVersion = meta.Versioning.Versions[len(meta.Versioning.Versions)-1]
	}
	if latestVersion == "" {
		return nil, fmt.Errorf("maven metadata for %s lists no versions: %w", dep.Name, errRegistryNotFound)
	}
	pomDate := func(version string) (string, error) {
		return fetchRegistryLastModified(ctx,
			artifactBase+"/"+url.PathEscape(version)+"/"+url.PathEscape(artifactID)+"-"+url.PathEscape(version)+".pom")
	}
	// A version listed in the metadata whose .pom is absent (retracted file,
	// classifier-only artifact) costs that DATE, not the dependency: the
	// artifact exists, so failing here would store nothing and cache a
	// definitive miss for something Maven Central has.
	latestDate, err := pomDate(latestVersion)
	if err != nil && !isDefinitiveRegistryMiss(err) {
		return nil, err
	}
	currentDate := ""
	if dep.Version != "" && slices.Contains(meta.Versioning.Versions, dep.Version) {
		currentDate, err = pomDate(dep.Version)
		if err != nil && !isDefinitiveRegistryMiss(err) {
			return nil, err
		}
		if err != nil {
			currentDate = ""
		}
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "maven",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               pkgid.Purl("maven", groupID+"/"+artifactID, dep.Version),
	}, nil
}

// resolvePackagistLibyear checks Packagist (PHP) for package versions.
func resolvePackagistLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	// Packagist names are case-insensitive and its answer is keyed in lower
	// case, whatever case the request used (0.29.163: a name declared with
	// capitals read as "not found" on every analysis).
	key := strings.ToLower(dep.Name)
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(packagistRegistryBase+"/p2/%s.json", key))
	if err != nil {
		return nil, err
	}
	var info struct {
		Packages map[string][]struct {
			Version string   `json:"version"`
			Time    string   `json:"time"`
			License []string `json:"license"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	versions := info.Packages[key]
	if len(versions) == 0 {
		// Deliberately NOT typed as "the package does not exist" (0.29.156):
		// this is a 200 whose body held no versions for the name, which a
		// changed response shape or a differently spelled key would produce
		// as well. An unexpected shape is not a "no" (SR-5), so it stays a
		// failure that says nothing and is asked again.
		return nil, fmt.Errorf("not found on Packagist: %s", dep.Name)
	}
	// First entry is the latest version.
	latest := versions[0]
	currentDate := ""
	onRegistry := false
	for _, v := range versions {
		if manifest.CleanVersion(v.Version) == dep.Version {
			onRegistry = true
			currentDate = v.Time
			break
		}
	}
	license := ""
	if len(latest.License) > 0 {
		license = joinRegistryLicenseList(latest.License)
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "packagist",
		CurrentVersion:     dep.Version,
		LatestVersion:      manifest.CleanVersion(latest.Version),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latest.Time,
		Libyear:            calcLibyear(currentDate, latest.Time),
		License:            license,
		Purl:               pkgid.Purl("composer", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// resolveHexLibyear checks hex.pm (Elixir/Erlang) for package versions.
func resolveHexLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(hexRegistryBase+"/api/packages/%s", dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		Releases []struct {
			Version    string `json:"version"`
			InsertedAt string `json:"inserted_at"`
		} `json:"releases"`
		Meta struct {
			Licenses []string `json:"licenses"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	if len(info.Releases) == 0 {
		// Deliberately NOT typed as "the package does not exist" (0.29.156):
		// this is a 200 whose body held no versions for the name, which a
		// changed response shape or a differently spelled key would produce
		// as well. An unexpected shape is not a "no" (SR-5), so it stays a
		// failure that says nothing and is asked again.
		return nil, fmt.Errorf("not found on Hex: %s", dep.Name)
	}
	latest := info.Releases[0]
	currentDate := ""
	onRegistry := false
	for _, r := range info.Releases {
		if r.Version == dep.Version {
			onRegistry = true
			currentDate = r.InsertedAt
			break
		}
	}
	license := ""
	if len(info.Meta.Licenses) > 0 {
		license = joinRegistryLicenseList(info.Meta.Licenses)
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "hex",
		CurrentVersion:     dep.Version,
		LatestVersion:      latest.Version,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latest.InsertedAt,
		Libyear:            calcLibyear(currentDate, latest.InsertedAt),
		License:            license,
		Purl:               pkgid.Purl("hex", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// nugetCatalogEntry is one version in a NuGet registration page.
type nugetCatalogEntry struct {
	Version           string `json:"version"`
	Published         string `json:"published"`
	Listed            *bool  `json:"listed"` // absent = listed
	LicenseExpression string `json:"licenseExpression"`
}

type nugetRegistrationPage struct {
	ID    string `json:"@id"`
	Items []struct {
		CatalogEntry nugetCatalogEntry `json:"catalogEntry"`
	} `json:"items"`
}

// nugetIsStable reports a SemVer release version: no prerelease label
// ("-…") before any build metadata ("+…").
func nugetIsStable(v string) bool {
	core, _, _ := strings.Cut(v, "+")
	return !strings.Contains(core, "-")
}

// resolveNuGetLibyear checks nuget.org for .NET package versions.
//
// 2026-09-23 log review, three defects fixed together (probed live):
// it reads the SemVer-2 registration hive (registration5-semver1 omits
// every SemVer-2-only package, e.g. microsoft.semantickernel.agents.openai
// 404ed there); it fetches each page NuGet does not inline, by its @id
// (large packages such as System.Text.Json inline none, so the latest
// version and the current date came back empty); and "latest" is the
// newest LISTED STABLE version, not the last entry (a prerelease:
// Newtonsoft.Json read 14.0.1-beta2). A package with no stable version
// falls back to its newest listed one. Pages come oldest first.
func resolveNuGetLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(nugetRegistryBase+"/v3/registration5-gz-semver2/%s/index.json",
			strings.ToLower(dep.Name)))
	if err != nil {
		return nil, err
	}
	var index struct {
		Items []nugetRegistrationPage `json:"items"`
	}
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, err
	}

	var entries []nugetCatalogEntry
	for _, page := range index.Items {
		if page.Items == nil {
			// Not inlined: fetch the page itself. A failure is the whole
			// resolution's failure (SR-5) — a partial version list would
			// produce a wrong "latest", not a missing one.
			pbody, perr := fetchRegistryJSON(ctx, page.ID)
			if perr != nil {
				return nil, fmt.Errorf("nuget registration page %s: %w", page.ID, perr)
			}
			if err := json.Unmarshal(pbody, &page); err != nil {
				return nil, fmt.Errorf("nuget registration page %s: %w", page.ID, err)
			}
		}
		for _, item := range page.Items {
			entries = append(entries, item.CatalogEntry)
		}
	}

	latestVersion, latestDate, license := "", "", ""
	currentDate := ""
	onRegistry := false
	var newestListed *nugetCatalogEntry
	for i := range entries {
		e := &entries[i]
		unlisted := e.Listed != nil && !*e.Listed
		// An unlisted version is published as 1900-01-01 (review round 1:
		// a dependency pinned to one read ~125 years stale). Its real date
		// is unknown, so the current date stays empty for it.
		// An unlisted version is on the registry; it only has no usable date.
		if dep.Version != "" && strings.EqualFold(e.Version, dep.Version) {
			onRegistry = true
		}
		if strings.EqualFold(e.Version, dep.Version) && !unlisted && !strings.HasPrefix(e.Published, "1900-") {
			currentDate = e.Published
		}
		if unlisted {
			continue
		}
		newestListed = e
		if nugetIsStable(e.Version) {
			latestVersion, latestDate, license = e.Version, e.Published, e.LicenseExpression
		}
	}
	if latestVersion == "" && newestListed != nil {
		latestVersion, latestDate, license = newestListed.Version, newestListed.Published, newestListed.LicenseExpression
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "nuget",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		License:            license,
		Purl:               pkgid.Purl("nuget", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// ============================================================
// Dart (pub.dev) parsers and resolver (added in v0.5.4)
// ============================================================

// resolvePubDevLibyear checks pub.dev (Dart/Flutter) for package versions.
func resolvePubDevLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(pubDevRegistryBase+"/api/packages/%s", dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		Latest struct {
			Version   string `json:"version"`
			Published string `json:"published"`
		} `json:"latest"`
		Versions []struct {
			Version   string `json:"version"`
			Published string `json:"published"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	latestVersion := info.Latest.Version
	latestDate := info.Latest.Published
	currentDate := ""
	onRegistry := false
	for _, v := range info.Versions {
		if v.Version == dep.Version {
			onRegistry = true
			currentDate = v.Published
			break
		}
	}
	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "pub",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               pkgid.Purl("pub", dep.Name, dep.Version),
		VersionFound:       versionFound(dep.Version, onRegistry),
	}, nil
}

// ============================================================
// Swift (SwiftPM) parsers and resolver (added in v0.5.4)
// ============================================================

// resolveSwiftPMLibyear resolves a SwiftPM dependency from its GitHub
// repository's releases. SwiftPM packages are git repositories, not a
// registry. Requests go through the key-pooled GitHub client (v0.29.56;
// they were anonymous, 60 an hour per IP, and failed with 403/429 in the
// 2026-09-17 log). The version in use is looked up as a release tag, with
// and without a leading "v", so libyear is computed instead of 0.
func resolveSwiftPMLibyear(ctx context.Context, gh githubAPIGetter, dep model.LibyearDep) (*model.LibyearRow, error) {
	// v0.29.59: one shared parse (manifest.ParseSwiftPackageURL). GitHub has no
	// subgroups, so the namespace must be exactly host/owner; userinfo
	// and a port never reach the purl.
	ref, ok := manifest.ParseSwiftPackageURL(dep.Requirement)
	if !ok || ref.Host != "github.com" || ref.Namespace != ref.Host+"/"+ref.Owner {
		// Non-GitHub SwiftPM packages can't be resolved without a registry.
		return nil, fmt.Errorf("SwiftPM resolver only supports GitHub repos: %s: %w", platform.RedactURLUserinfo(dep.Requirement), errRegistryCannotBeAsked)
	}
	if gh == nil {
		return nil, errNoGitHubClient
	}
	owner, repo := ref.Owner, ref.Repo
	releases := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/releases"

	type release struct {
		TagName     string `json:"tag_name"`
		PublishedAt string `json:"published_at"`
	}
	var latest release
	if err := gh.GetJSON(ctx, releases+"/latest", &latest); err != nil {
		return nil, err
	}
	currentDate := ""
	if dep.Version != "" {
		tags := []string{dep.Version}
		if !strings.HasPrefix(dep.Version, "v") {
			tags = append(tags, "v"+dep.Version)
		}
		for _, tag := range tags {
			var cur release
			err := gh.GetJSON(ctx, releases+"/tags/"+url.PathEscape(tag), &cur)
			if err == nil {
				currentDate = cur.PublishedAt
				break
			}
			if !platform.IsDefinitiveAnswer(err) {
				return nil, err
			}
		}
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "swiftpm",
		CurrentVersion:     dep.Version,
		LatestVersion:      strings.TrimPrefix(latest.TagName, "v"),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latest.PublishedAt,
		Libyear:            calcLibyear(currentDate, latest.PublishedAt),
		// v0.29.59 (worklist 46): the purl spec's swift namespace is the
		// repository host and owner (pkg:swift/github.com/Owner/Repo@v);
		// the Package.resolved writer emits the same shape, so the direct
		// and transitive purls of one package are one purl.
		Purl: pkgid.Purl("swift", ref.Namespace+"/"+ref.Repo, dep.Version),
	}, nil
}

// ============================================================
// Haskell (Hackage) parsers and resolver (added in v0.5.4)
// ============================================================

// resolveHackageLibyear checks Hackage for Haskell package versions.
func resolveHackageLibyear(ctx context.Context, dep model.LibyearDep) (*model.LibyearRow, error) {
	// Hackage preferred-versions API returns version list. This is the
	// one registry endpoint that needs an Accept header (it serves HTML
	// otherwise) — passed through fetchRegistryJSON's extraArgs.
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(hackageRegistryBase+"/package/%s/preferred", dep.Name),
		"Accept: application/json")
	if err != nil {
		return nil, err
	}
	var info struct {
		NormalVersion []string `json:"normal-version"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	if len(info.NormalVersion) == 0 {
		// Deliberately NOT typed as "the package does not exist" (0.29.156):
		// this is a 200 whose body held no versions for the name, which a
		// changed response shape or a differently spelled key would produce
		// as well. An unexpected shape is not a "no" (SR-5), so it stays a
		// failure that says nothing and is asked again.
		return nil, fmt.Errorf("no versions found on Hackage: %s", dep.Name)
	}
	latestVersion := info.NormalVersion[0] // First is the latest.

	// Hackage doesn't return dates in this endpoint. Get upload time from
	// package info (plain-text responses, same curl choke point).
	// As in the Go and Maven resolvers: a definitive miss costs the DATE, a
	// failure that says nothing fails the dependency — a swallowed error
	// here stores libyear 0 ("perfectly fresh"), now cached for a day.
	uploadTime := func(version string) (string, error) {
		body, err := fetchRegistryJSON(ctx,
			fmt.Sprintf(hackageRegistryBase+"/package/%s-%s/upload-time", dep.Name, version))
		if err != nil {
			if isDefinitiveRegistryMiss(err) {
				return "", nil
			}
			return "", err
		}
		return strings.TrimSpace(string(body)), nil
	}
	latestDate, err := uploadTime(latestVersion)
	if err != nil {
		return nil, err
	}
	currentDate := ""
	if dep.Version != "" {
		if currentDate, err = uploadTime(dep.Version); err != nil {
			return nil, err
		}
	}

	return &model.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "hackage",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               pkgid.Purl("hackage", dep.Name, dep.Version),
	}, nil
}

// scanScanCode was the v0.20-and-earlier per-job wrapper around
// RunScanCode. Removed in v0.21.0 along with RunScanCode itself
// when scancode moved to the dedicated ScancodeWorker pool. See
// internal/collector/scancode_worker.go and
// docs/architecture/scancode.md.
