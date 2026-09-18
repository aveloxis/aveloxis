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
	"bufio"
	"bytes"
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
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// AnalysisCollector runs file-content analysis on repos.
type AnalysisCollector struct {
	store   *db.PostgresStore
	logger  *slog.Logger
	bareDir string // base directory for bare clones (e.g., ~/aveloxis-repos)
	tempDir string // base directory for temporary full clones

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
		logger.Warn("analysis phase failed", "repo_id", repoID, "error", phaseErr)
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
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", "clone", barePath, workDir)
	// Skip LFS smudge filters — dependency/license scanners only need text
	// source files. LFS objects with expired quotas cause fatal checkout failures.
	cmd.Env = gitCloneEnv()
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return result, fmt.Errorf("local clone failed: %w: %s", execErr(ctx, err), stderr.String())
	}

	// Phase 1: Dependency scanning.
	if err := ac.scanDependencies(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("dependencies: %w", err))
	}

	// Phase 2: Libyear (dependency age).
	if err := ac.scanLibyear(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("libyear: %w", err))
	}

	// Phase 2b (v0.27.11): lockfile inventory + direct-dep
	// resolutions. Must run AFTER Phase 2 — the storage filter matches
	// lockfile entries against the repo_deps_libyear rows the libyear
	// phase just refreshed. Best-effort like every other phase.
	if err := ac.scanLockfiles(ctx, repoID, workDir, result); err != nil {
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

// manifestFiles maps filename patterns to their language/ecosystem.
var manifestFiles = map[string]string{
	"package.json":             "JavaScript",
	"yarn.lock":                "JavaScript",
	"requirements.txt":         "Python",
	"setup.py":                 "Python",
	"setup.cfg":                "Python",
	"pyproject.toml":           "Python",
	"Pipfile":                  "Python",
	"poetry.lock":              "Python",
	"go.mod":                   "Go",
	"go.sum":                   "Go",
	"Cargo.toml":               "Rust",
	"Cargo.lock":               "Rust",
	"Gemfile":                  "Ruby",
	"Gemfile.lock":             "Ruby",
	"pom.xml":                  "Java",
	"build.gradle":             "Java",
	"build.gradle.kts":         "Java",
	"composer.json":            "PHP",
	"mix.exs":                  "Elixir",
	"Package.swift":            "Swift",
	"pubspec.yaml":             "Dart",
	"Makefile":                 "C/C++",
	"CMakeLists.txt":           "C/C++",
	"build.sbt":                "Scala",
	"packages.config":          ".NET",
	"Directory.Packages.props": ".NET",
	"package.yaml":             "Haskell",
	"stack.yaml":               "Haskell",
}

func (ac *AnalysisCollector) scanDependencies(ctx context.Context, repoID int64, workDir string, result *AnalysisResult) error {
	ac.logger.Info("scanning dependencies", "repo_id", repoID)

	// Clear previous dependency data before inserting fresh results.
	// repo_dependencies is a snapshot table (no history rotation needed).
	if err := ac.store.ClearRepoDependencies(ctx, repoID); err != nil {
		// Round-8 class sweep: shutdown is not a scan failure, and
		// continuing the whole dependency scan on a dead ctx just
		// produces one WARN per dep below.
		if errors.Is(err, context.Canceled) {
			return err
		}
		ac.logger.Warn("failed to clear old dependencies", "repo_id", repoID, "error", err)
	}

	depCounts := make(map[string]map[string]int) // language -> dep_name -> count

	err := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable files
		}
		// Reject symlinks to prevent traversal attacks. A malicious repo can
		// symlink requirements.txt -> /etc/passwd and have host file contents
		// stored as dependency names or sent to package registries.
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			// Skip vendor/node_modules/.git directories.
			if base == "vendor" || base == "node_modules" || base == ".git" || base == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}

		filename := filepath.Base(path)
		lang, ok := manifestFiles[filename]
		if !ok {
			// Check extension-based matches (e.g., .csproj).
			ext := filepath.Ext(filename)
			if ext == ".csproj" {
				lang = ".NET"
			} else {
				return nil
			}
		}

		deps, err := parseDependencyFile(path, lang)
		if err != nil {
			return nil // skip unparseable files
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
		return err
	}

	// Insert into repo_dependencies.
	for lang, deps := range depCounts {
		for depName, count := range deps {
			if err := ac.store.InsertRepoDependency(ctx, repoID, depName, count, lang); err != nil {
				// Round-8 class sweep: warn-and-continue over every dep
				// of every language — a shutdown here emitted one WARN
				// per dependency (v0.27.91 flood class).
				if errors.Is(err, context.Canceled) {
					return err
				}
				ac.logger.Warn("failed to insert dependency", "dep", depName, "error", err)
				continue
			}
			result.Dependencies++
		}
	}

	return nil
}

// parseDependencyFile extracts dependency names from a manifest file.
func parseDependencyFile(path, lang string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Detect and transcode UTF-16 encoded files. Some Windows-created
	// requirements.txt files use UTF-16LE (BOM 0xff 0xfe) which produces
	// null-interleaved ASCII that PostgreSQL rejects with "invalid byte sequence".
	data = decodeIfUTF16(data)
	content := string(data)

	switch filepath.Base(path) {
	case "package.json":
		return parsePackageJSON(data)
	case "requirements.txt":
		return parseRequirementsTxt(content), nil
	case "go.mod":
		return parseGoMod(content), nil
	case "Cargo.toml":
		return parseTOMLDeps(content, "[dependencies]"), nil
	case "pyproject.toml":
		return parsePyprojectDeps(content)
	case "setup.py":
		return parseSetupPyDeps(content)
	case "setup.cfg":
		return parseSetupCfgDeps(content)
	case "Pipfile":
		return parsePipfileDeps(content)
	case "Gemfile":
		return parseGemfile(content), nil
	case "pom.xml":
		return parsePomXML(content), nil
	case "build.gradle", "build.gradle.kts":
		return parseBuildGradle(content), nil
	case "composer.json":
		return parseComposerJSON(data)
	case "build.sbt":
		return parseBuildSbt(content), nil
	case "packages.config":
		return parseNuGetPackagesConfig(content), nil
	case "Directory.Packages.props":
		return parseDirectoryPackagesProps(content)
	case "package.yaml":
		return parsePackageYaml(content), nil
	case "mix.exs":
		return parseMixExsDeps(content), nil
	case "pubspec.yaml":
		return parsePubspecDeps(content), nil
	case "Package.swift":
		return parsePackageSwiftDeps(content), nil
	default:
		// Extension-based matching for files like *.csproj.
		if strings.HasSuffix(filepath.Base(path), ".csproj") {
			return parseCsprojDeps(content)
		}
		return nil, nil
	}
}

func parsePackageJSON(data []byte) ([]string, error) {
	var pkg struct {
		Dependencies    map[string]interface{} `json:"dependencies"`
		DevDependencies map[string]interface{} `json:"devDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []string
	for name := range pkg.Dependencies {
		deps = append(deps, name)
	}
	for name := range pkg.DevDependencies {
		deps = append(deps, name)
	}
	return deps, nil
}

func parseRequirementsTxt(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "-") {
			continue
		}
		// Strip inline comments: "flask==2.0 # pinned"
		if idx := strings.Index(line, " #"); idx > 0 {
			line = line[:idx]
		}
		// Strip environment markers: "flask>=2.0; python_version>='3.8'"
		if idx := strings.Index(line, ";"); idx > 0 {
			line = line[:idx]
		}
		line = stripPyExtras(line)
		// Strip version specifiers.
		line, _ = splitPyNameSpec(line)
		if name := strings.TrimSpace(line); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

func parseGoMod(content string) []string {
	var deps []string
	inRequire := false
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "require (" {
			inRequire = true
			continue
		}
		if line == ")" {
			inRequire = false
			continue
		}
		if inRequire && line != "" && !strings.HasPrefix(line, "//") {
			parts := strings.Fields(line)
			if len(parts) >= 1 {
				deps = append(deps, parts[0])
			}
		}
	}
	return deps
}

// parseTOMLDeps lists the dependency names declared in one TOML section
// (Cargo [dependencies], poetry's [tool.poetry.dependencies]) through the
// shared table scanner (manifest_toml.go), whatever their source.
func parseTOMLDeps(content, section string) []string {
	var deps []string
	for _, e := range scanTOMLDepTables(content, map[string]bool{section: true}) {
		if e.Name != "python" {
			deps = append(deps, e.Name)
		}
	}
	return deps
}

func parseGemfile(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "gem ") {
			parts := strings.SplitN(line, "'", 3)
			if len(parts) >= 2 {
				deps = append(deps, parts[1])
			} else {
				parts = strings.SplitN(line, "\"", 3)
				if len(parts) >= 2 {
					deps = append(deps, parts[1])
				}
			}
		}
	}
	return deps
}

func parsePomXML(content string) []string {
	var deps []string
	// Simple extraction of <artifactId> within <dependency> blocks.
	inDep := false
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "<dependency>") {
			inDep = true
		}
		if strings.Contains(line, "</dependency>") {
			inDep = false
		}
		if inDep && strings.Contains(line, "<artifactId>") {
			name := strings.TrimPrefix(line, "<artifactId>")
			name = strings.TrimSuffix(name, "</artifactId>")
			name = strings.TrimSpace(name)
			if name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// parsePyprojectDeps extracts dependency names from pyproject.toml.
// Handles both PEP 621 ([project] dependencies = [...]) and Poetry
// ([tool.poetry.dependencies]) formats.
func parsePyprojectDeps(content string) ([]string, error) {
	var deps []string

	// Try PEP 621 format: [project] section with dependencies = ["pkg>=1.0", ...]
	deps = append(deps, parsePEP621Deps(content)...)

	// Try Poetry format: [tool.poetry.dependencies] with key = value pairs
	poetryDeps := parseTOMLDeps(content, "[tool.poetry.dependencies]")
	deps = append(deps, poetryDeps...)

	return deps, nil
}

// parsePEP621Deps extracts dependency names from PEP 621 format pyproject.toml.
// Parses the dependencies = [...] array under [project].
func parsePEP621Deps(content string) []string {
	var deps []string
	lines := strings.Split(content, "\n")
	inProject := false
	inDepsArray := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Track [project] section.
		if trimmed == "[project]" {
			inProject = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") && trimmed != "[project]" {
			inProject = false
			inDepsArray = false
			continue
		}

		if !inProject {
			continue
		}

		// Detect dependencies = [ start.
		if strings.HasPrefix(trimmed, "dependencies") && strings.Contains(trimmed, "=") {
			if strings.Contains(trimmed, "[") {
				inDepsArray = true
				// Handle inline: dependencies = ["flask==2.0"]
				if strings.Contains(trimmed, "]") {
					// Single-line array.
					deps = append(deps, extractPEP621DepsFromLine(trimmed)...)
					inDepsArray = false
				}
			}
			continue
		}

		if inDepsArray {
			// Check if this line closes the array. The array closer is an unquoted ]
			// at line end, not a ] inside a dep name like "sqlalchemy[asyncio]>=2.0".
			stripped := strings.TrimRight(trimmed, " ,")
			if stripped == "]" || strings.HasSuffix(stripped, "]") && !strings.Contains(stripped, "\"") && !strings.Contains(stripped, "'") {
				// Pure array closer (possibly with trailing comma).
				inDepsArray = false
				continue
			}
			if name := extractPEP621DepName(trimmed); name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// extractPEP621DepName extracts a package name from a PEP 621 dependency string.
// Input: `"flask>=2.0",` or `"tomli>=2.2.1 ; python_version < '3.11'",`
// Output: `flask` or `tomli`
func extractPEP621DepName(line string) string {
	line = strings.TrimSpace(line)
	// TOML literal strings are legal in pyproject.toml, so an item may be
	// single-quoted. Trimming only '"' kept the apostrophe in the name
	// ('tomli) while the libyear path stripped it — the two disagreed about
	// the same file (v0.29.56).
	line = strings.Trim(line, "\"',")
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return ""
	}
	// Strip environment markers: everything after ';'
	if idx := strings.Index(line, ";"); idx > 0 {
		line = strings.TrimSpace(line[:idx])
	}
	line = stripPyExtras(line)
	name, _ := splitPyNameSpec(line)
	return name
}

// extractPEP621DepsFromLine extracts dep names from an inline deps array.
func extractPEP621DepsFromLine(line string) []string {
	var deps []string
	// Find content between [ and ].
	start := strings.Index(line, "[")
	end := strings.LastIndex(line, "]")
	if start < 0 {
		start = 0
	} else {
		start++
	}
	if end < 0 {
		end = len(line)
	}
	if start > end {
		// Malformed line where ']' precedes '[' (e.g. "dependencies]=[")
		// — found by FuzzManifestParsers (v0.27.99): line[start:end]
		// panicked with slice bounds out of range, killing the whole
		// analysis phase of any repo carrying the line.
		return nil
	}
	inner := line[start:end]
	// Only a comma BETWEEN items separates them (v0.29.56). A comma inside
	// the quoted item is data — a bounded range ("requests>=2.31.0,<3.0.0")
	// or an extras list ("celery[redis,auth]>=5.3") — and splitting on it
	// cuts one dependency into fragments, so the real package is never
	// resolved and a fragment is inventoried in its place. (Which fragments
	// depends on how each path strips extras, so this comment does not name
	// them; TestInlineDeclarationsSplitOnStructuralCommasOnly asserts the
	// CORRECT output for both paths.)
	for _, item := range splitTOMLTopLevel(inner) {
		if name := extractPEP621DepName(item); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// parseSetupPyDeps extracts dependency names from setup.py install_requires.
func parseSetupPyDeps(content string) ([]string, error) {
	return extractSetupPyInstallRequires(content), nil
}

// extractSetupPyInstallRequires parses the install_requires=[...] list from setup.py.
func extractSetupPyInstallRequires(content string) []string {
	var deps []string
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Detect install_requires=[ or install_requires = [
		if !inRequires && strings.Contains(trimmed, "install_requires") && strings.Contains(trimmed, "[") {
			inRequires = true
			// Check for deps on the same line as install_requires=[
			if idx := strings.Index(trimmed, "["); idx >= 0 {
				rest := trimmed[idx:]
				if strings.Contains(rest, "]") {
					// Single-line: install_requires=['flask', 'requests']
					deps = append(deps, extractQuotedPyDeps(rest)...)
					inRequires = false
				} else {
					deps = append(deps, extractQuotedPyDeps(rest)...)
				}
			}
			continue
		}

		if inRequires {
			if strings.Contains(trimmed, "]") {
				deps = append(deps, extractQuotedPyDeps(trimmed)...)
				inRequires = false
				continue
			}
			deps = append(deps, extractQuotedPyDeps(trimmed)...)
		}
	}
	return deps
}

// extractQuotedPyDeps extracts package names from Python quoted dependency strings.
// Input: `'flask>=2.0', "requests==2.28.0",`
// Output: ["flask", "requests"]
func extractQuotedPyDeps(line string) []string {
	var deps []string
	// Extract all quoted strings.
	for _, quote := range []byte{'\'', '"'} {
		rest := line
		for {
			start := strings.IndexByte(rest, quote)
			if start < 0 {
				break
			}
			end := strings.IndexByte(rest[start+1:], quote)
			if end < 0 {
				break
			}
			depStr := rest[start+1 : start+1+end]
			rest = rest[start+1+end+1:]
			if name := extractPyDepName(depStr); name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// extractPyDepName extracts the package name from a Python requirement string.
// "flask>=2.0" -> "flask", "numpy" -> "numpy"
func extractPyDepName(req string) string {
	req = strings.TrimSpace(req)
	if req == "" {
		return ""
	}
	// Strip environment markers.
	if idx := strings.Index(req, ";"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	req = stripPyExtras(req)
	name, _ := splitPyNameSpec(req)
	return name
}

// parseSetupPyVersions extracts deps with versions from setup.py install_requires.
func parseSetupPyVersions(content string) []libyearDep {
	var deps []libyearDep
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if !inRequires && strings.Contains(trimmed, "install_requires") && strings.Contains(trimmed, "[") {
			inRequires = true
			if idx := strings.Index(trimmed, "["); idx >= 0 {
				rest := trimmed[idx:]
				if strings.Contains(rest, "]") {
					deps = append(deps, extractQuotedPyVersionDeps(rest)...)
					inRequires = false
				} else {
					deps = append(deps, extractQuotedPyVersionDeps(rest)...)
				}
			}
			continue
		}

		if inRequires {
			if strings.Contains(trimmed, "]") {
				deps = append(deps, extractQuotedPyVersionDeps(trimmed)...)
				inRequires = false
				continue
			}
			deps = append(deps, extractQuotedPyVersionDeps(trimmed)...)
		}
	}
	return deps
}

// extractQuotedPyVersionDeps extracts deps with versions from the quoted
// Python strings of a requirement list. A string that is a function-call
// argument or a keyword value is not a list element: v0.29.56 found
// "read_without_comments('dev-requirements')" in pixie-io/cpplint's
// extras_require sent to PyPI as a package, so a string whose preceding
// token is '(' , '=' or an identifier is skipped. Strings are scanned in
// order, so a quote of one kind inside the other kind is not a delimiter.
func extractQuotedPyVersionDeps(line string) []libyearDep {
	var deps []libyearDep
	for i := 0; i < len(line); i++ {
		quote := line[i]
		if quote != '\'' && quote != '"' {
			continue
		}
		end := strings.IndexByte(line[i+1:], quote)
		if end < 0 {
			break
		}
		depStr := line[i+1 : i+1+end]
		prev := strings.TrimRight(line[:i], " \t")
		i += end + 1
		if prev != "" {
			switch c := prev[len(prev)-1]; {
			case c == '(' || c == '=' || c == '_' || c == '.' ||
				('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9'):
				continue
			}
		}
		if d := parsePyRequirement(depStr); d != nil {
			deps = append(deps, *d)
		}
	}
	return deps
}

// pyVersionOperators are PEP 440's comparison operators, longest first so a
// scan at one position matches "===" before "==" and ">=" before ">".
var pyVersionOperators = []string{"===", "==", "~=", "!=", ">=", "<=", ">", "<"}

// stripPyExtras removes a PEP 508 extras group ("celery[redis,auth]>=5.3"
// -> "celery>=5.3"). An UNCLOSED bracket keeps only the prefix.
//
// One spelling, because there were five (v0.29.56) and they disagreed about
// the unclosed case in three different ways: "celery[redis" answered
// "celery" from pyproject.toml and setup.py but "celery[redis" from
// requirements.txt — and buildPurl turns that into "pkg:pypi/celery[redis",
// which wireValidPurl ACCEPTS, so it reaches OSV as a package that cannot
// exist. A name can never legitimately contain '[': PEP 508 names are
// [A-Za-z0-9._-] (SR-17).
func stripPyExtras(req string) string {
	idx := strings.Index(req, "[")
	if idx <= 0 {
		return req
	}
	if end := strings.Index(req, "]"); end > idx {
		return req[:idx] + req[end+1:]
	}
	return req[:idx]
}

// splitPyNameSpec splits a PEP 508 requirement at its LEFTMOST version
// operator: name is the text before it, spec the whole specifier set from
// that operator on ("" when the requirement carries no operator, and name ""
// when it opens with one and so names nothing).
//
// Position, not preference (v0.29.56). A specifier set is legal in either
// order, and five parsers each scanned their own preference list — in four
// different orders, one of them missing five operators — so the first
// operator in the LIST rather than in the STRING ended the name.
// "numpy<2,>=1.22", which is what apache/airflow ships, matched ">=" first
// and yielded the package name "numpy<2,": no registry can answer for it, so
// the dependency got no libyear row, no purl and no OSV coverage. The
// preference lists were reaching for the VERSION, which is a separate
// question — see pyFloorVersion (SR-17).
func splitPyNameSpec(req string) (name, spec string) {
	for i := 0; i < len(req); i++ {
		for _, op := range pyVersionOperators {
			if strings.HasPrefix(req[i:], op) {
				return strings.TrimSpace(req[:i]), strings.TrimSpace(req[i:])
			}
		}
	}
	return strings.TrimSpace(req), ""
}

// pyPermittedVersionRank ranks the PEP 440 operators that may supply the
// version to record, best first: an exact pin, then a compatible release,
// then an inclusive floor, then an inclusive ceiling.
//
// Membership is the rule, not the ranking (v0.29.56): an operator appears
// here only if its OWN clause permits the version it names. "!=", "<" and
// ">" are all EXCLUSIVE — each rules its version out — so none of them can
// answer, and their absence from this map is what enforces that.
//
// The check is per CLAUSE, not per set. A redundant set such as "<3,<=4"
// still yields 4, because "<=4" permits 4 on its own even though "<3" rules
// it out. (The set itself is satisfiable — 2.0 is in it — so the recorded
// value is wrong, not the requirement.) Catching that needs whole-set
// version comparison, and two independent sweeps of real manifests —
// checking every recorded version against its own specifier set — found NOT
// ONE that its set rejects. So this is left undone deliberately rather than
// overlooked. (No tally, on purpose: the corpora were fetched ad hoc and are
// not in the repo, so any figure here would be one a later reader cannot
// re-derive. This release shipped four such counts, the last of them inside
// the sentence that replaced the third.)
//
// Recording an excluded version writes a release the manifest contradicts
// into repo_dependencies and into the purl, and OSV then answers about a
// version nobody installed (SR-6). For "!=" that invents findings; for "<"
// it HIDES them, which is the worse direction. Measured against OSV on
// 2026-09-17: pkg:pypi/pyyaml@6.0 reports 0 vulnerabilities, while permitted
// releases below it carry up to 6 (5.3.1 → 2, 5.3 → 4, 5.1 → 6; 5.4 and
// 5.4.1 are also permitted and carry 0, so the bound is not uniformly
// safe-looking — it is just not the version in use). An upper bound is
// often pinned because that release is the one that changed something, so
// the excluded value tends to sit outside the vulnerable range.
//
// An empty version yields a versionless purl, which OSV answers with the
// package's whole advisory history — less precise, but not a claim the
// manifest contradicts.
var pyPermittedVersionRank = map[string]int{"===": 4, "==": 4, "~=": 3, ">=": 2, "<=": 1}

// pyFloorVersion picks the version to record for a PEP 508 specifier set.
func pyFloorVersion(spec string) string {
	best, bestRank := "", -1
	for _, clause := range strings.Split(spec, ",") {
		clause = strings.TrimSpace(clause)
		for _, op := range pyVersionOperators {
			if !strings.HasPrefix(clause, op) {
				continue
			}
			if r, ok := pyPermittedVersionRank[op]; ok && r > bestRank {
				best, bestRank = strings.TrimSpace(clause[len(op):]), r
			}
			break
		}
	}
	return best
}

// parsePyRequirement parses a single Python requirement string into a libyearDep.
func parsePyRequirement(req string) *libyearDep {
	req = strings.TrimSpace(req)
	if req == "" {
		return nil
	}
	// Strip an inline comment FIRST, and the array punctuation a list item
	// carries. The non-registry predicate below matches " @ ", URLs and
	// paths, all of which appear in comments: leaving one attached deletes a
	// legitimate pinned dependency, which then has no libyear row and drops
	// out of the OSV scan entirely. requirements*.txt learned this in
	// v0.29.56; these entry points inherited the predicate afterwards and
	// must strip the same way. (The trailing `",` also left versions like
	// `2.31.0"` behind on the PEP 621 path.)
	req = strings.Trim(strings.TrimSpace(req), "\"',")
	if idx := strings.Index(req, " #"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	req = strings.Trim(strings.TrimSpace(req), "\"',")
	// Strip environment markers.
	if idx := strings.Index(req, ";"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	// A VCS or URL reference, a local path, or a PEP 508 direct reference
	// names no PyPI package (v0.29.56). requirements*.txt learned this from
	// the eclipse-velocitas shape; every other Python entry point —
	// pyproject.toml, setup.py, setup.cfg, and the dev/build variants —
	// builds its deps HERE, so the check belongs here too, or the same
	// "velocitas-sdk @ git+https://…" goes to PyPI as a package name from
	// one file format away.
	if isNonRegistryPyRequirement(req) {
		return nil
	}
	cleanReq := stripPyExtras(req)

	name, spec := splitPyNameSpec(cleanReq)
	version := pyFloorVersion(spec)
	if name == "" {
		return nil
	}
	return &libyearDep{Name: name, Version: version, Requirement: req, Type: "runtime", Manager: "pypi"}
}

// pythonTableIsNonRegistry reports a Poetry/Pipfile inline dependency table
// that names a source other than PyPI — a local path, a git repository or a
// URL. Such a dependency is not the PyPI package of the same name (SR-6),
// and looking it up only ever returned 404.
func pythonTableIsNonRegistry(table string) bool {
	for _, kv := range splitTOMLTopLevel(strings.Trim(strings.TrimSpace(table), "{}")) {
		switch k, _, _ := strings.Cut(kv, "="); strings.TrimSpace(k) {
		case "path", "git", "url", "file":
			return true
		}
	}
	return false
}

// pythonTableVersion reads the version key out of such a table ("" when it
// has none — the unpinned pathway).
func pythonTableVersion(table string) string {
	for _, kv := range splitTOMLTopLevel(strings.Trim(strings.TrimSpace(table), "{}")) {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.TrimSpace(k) == "version" {
			return strings.Trim(strings.TrimSpace(v), "\"' ")
		}
	}
	return ""
}

// parsePipfileDeps extracts dependency names from Pipfile [packages] section.
func parsePipfileDeps(content string) ([]string, error) {
	var deps []string
	inPackages := false

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[packages]" {
			inPackages = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inPackages = false
			continue
		}
		if inPackages && strings.Contains(trimmed, "=") {
			name := strings.TrimSpace(strings.SplitN(trimmed, "=", 2)[0])
			if name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps, nil
}

// parsePipfileVersions extracts deps with versions from Pipfile [packages].
func parsePipfileVersions(content string) []libyearDep {
	var deps []libyearDep
	inPackages := false

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[packages]" {
			inPackages = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inPackages = false
			continue
		}
		if !inPackages || !strings.Contains(trimmed, "=") {
			continue
		}

		// Pipfile uses "name = value" where value is quoted.
		// Use the first unquoted = as delimiter.
		eqIdx := strings.Index(trimmed, "=")
		if eqIdx < 0 {
			continue
		}
		name := strings.TrimSpace(trimmed[:eqIdx])
		if name == "" {
			continue
		}
		versionRaw := strings.TrimSpace(trimmed[eqIdx+1:])
		// Pipfile values: "==2.0.2", "~=2.28", "*", {version = "==2.0.0", ...}
		version := ""
		versionRaw = strings.Trim(versionRaw, "\"' ")
		if versionRaw != "*" {
			// Handle table-style: {version = "==2.0.0", extras = [...]}
			if strings.HasPrefix(versionRaw, "{") {
				// path/git/url: not a PyPI package (v0.29.56).
				if pythonTableIsNonRegistry(versionRaw) {
					continue
				}
				// The shared reader, not a second spelling of it (SR-17,
				// v0.29.56): the hand-rolled loop here split inside quoted
				// values and then stopped at the first fragment that merely
				// STARTED with "version", reporting no version at all. The
				// Poetry arm already reads its tables this way.
				version = cleanVersion(pythonTableVersion(versionRaw))
			} else {
				version = cleanVersion(versionRaw)
			}
		}
		deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: "runtime", Manager: "pypi"})
	}
	return deps
}

// parsePyprojectVersionsFromContent extracts deps with versions from pyproject.toml content.
// Handles both PEP 621 and Poetry formats.
func parsePyprojectVersionsFromContent(content string) []libyearDep {
	var deps []libyearDep

	// PEP 621: [project] dependencies = ["flask==2.0.2", ...]
	deps = append(deps, parsePEP621Versions(content)...)

	// Poetry: [tool.poetry.dependencies] key = "^version"
	deps = append(deps, parsePoetryVersions(content)...)

	return deps
}

// parsePEP621Versions extracts deps with versions from PEP 621 format.
func parsePEP621Versions(content string) []libyearDep {
	var deps []libyearDep
	lines := strings.Split(content, "\n")
	inProject := false
	inDepsArray := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if trimmed == "[project]" {
			inProject = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") && trimmed != "[project]" {
			inProject = false
			inDepsArray = false
			continue
		}
		if !inProject {
			continue
		}

		if strings.HasPrefix(trimmed, "dependencies") && strings.Contains(trimmed, "=") {
			if strings.Contains(trimmed, "[") {
				inDepsArray = true
				if strings.Contains(trimmed, "]") {
					deps = append(deps, extractPEP621VersionDeps(trimmed)...)
					inDepsArray = false
				}
			}
			continue
		}

		if inDepsArray {
			// Check for unquoted array closer (not ] inside extras like [asyncio]).
			stripped := strings.TrimRight(trimmed, " ,")
			if stripped == "]" || strings.HasSuffix(stripped, "]") && !strings.Contains(stripped, "\"") && !strings.Contains(stripped, "'") {
				inDepsArray = false
				continue
			}
			depStr := strings.Trim(trimmed, "\",")
			depStr = strings.TrimSpace(depStr)
			if depStr != "" && !strings.HasPrefix(depStr, "#") {
				if d := parsePyRequirement(depStr); d != nil {
					deps = append(deps, *d)
				}
			}
		}
	}
	return deps
}

// extractPEP621VersionDeps extracts deps with versions from inline PEP 621 arrays.
func extractPEP621VersionDeps(line string) []libyearDep {
	var deps []libyearDep
	start := strings.Index(line, "[")
	end := strings.LastIndex(line, "]")
	if start < 0 {
		start = 0
	} else {
		start++
	}
	if end < 0 {
		end = len(line)
	}
	if start > end {
		// Malformed line where ']' precedes '[' (e.g. "dependencies]=[")
		// — found by FuzzManifestParsers (v0.27.99): line[start:end]
		// panicked with slice bounds out of range, killing the whole
		// analysis phase of any repo carrying the line.
		return nil
	}
	inner := line[start:end]
	// Structural commas only — see extractPEP621DepsFromLine (v0.29.56).
	for _, item := range splitTOMLTopLevel(inner) {
		item = strings.Trim(strings.TrimSpace(item), "\"'")
		if item != "" {
			if d := parsePyRequirement(item); d != nil {
				deps = append(deps, *d)
			}
		}
	}
	return deps
}

// parsePoetryVersions extracts deps with versions from Poetry format pyproject.toml.
func parsePoetryVersions(content string) []libyearDep {
	var deps []libyearDep
	inSection := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[tool.poetry.dependencies]" {
			inSection = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inSection = false
			continue
		}
		if inSection && strings.Contains(trimmed, "=") {
			parts := strings.SplitN(trimmed, "=", 2)
			name := strings.TrimSpace(parts[0])
			raw := strings.TrimSpace(parts[1])
			if name == "" || name == "python" {
				continue
			}
			// A path/git/url dependency names no PyPI package (v0.29.56 —
			// the same rule parsePyRequirement applies to the other Python
			// formats); an inline table otherwise carries its version under
			// the version key, not as the whole table body.
			version := ""
			if strings.HasPrefix(raw, "{") {
				if pythonTableIsNonRegistry(raw) {
					continue
				}
				version = cleanVersion(pythonTableVersion(raw))
			} else {
				version = cleanVersion(strings.Trim(raw, "\"'^~>="))
			}
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: "runtime", Manager: "pypi"})
		}
	}
	return deps
}

// parseDirectoryPackagesProps extracts dependency names from .NET Directory.Packages.props.
// Format: <PackageVersion Include="Name" Version="1.0.0" />
func parseDirectoryPackagesProps(content string) ([]string, error) {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageVersion") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps, nil
}

// parseDirectoryPackagesPropsVersions extracts deps with versions from Directory.Packages.props.
func parseDirectoryPackagesPropsVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageVersion") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		version := extractXMLAttr(line, "Version")
		// v0.27.44 (summary/19 P1): PrivateAssets="all" marks a
		// build/analyzer-only package that never flows to consumers
		// or the runtime output → build. Only the attribute form is
		// visible to this line-based parser; the child-element form
		// (<PrivateAssets>all</PrivateAssets>) spans lines and is a
		// documented limitation.
		scope := "runtime"
		if strings.EqualFold(extractXMLAttr(line, "PrivateAssets"), "all") {
			scope = model.ScopeBuild
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "nuget"})
		}
	}
	return deps
}

// extractXMLAttr extracts the value of an XML attribute from a line.
// extractXMLAttr(`<PackageVersion Include="Foo" />`, "Include") returns "Foo".
func extractXMLAttr(line, attr string) string {
	key := attr + "=\""
	idx := strings.Index(line, key)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(key):]
	end := strings.Index(rest, "\"")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// ============================================================
// Libyear scanning (dependency age)
// ============================================================

func (ac *AnalysisCollector) scanLibyear(ctx context.Context, repoID int64, workDir string, result *AnalysisResult) error {
	ac.logger.Info("scanning libyear", "repo_id", repoID)
	// The registry layer's own conditions (a deep pacing queue) report
	// through this collection's logger, not slog's default.
	ctx = withRegistryLogger(ctx, ac.logger)

	// Rotate previous libyear data to history before inserting fresh data.
	// This ensures the main table always has the latest snapshot with current
	// license values. Without rotation, old rows with empty licenses persist
	// because ON CONFLICT DO NOTHING skips existing rows.
	// v0.27.17: a failed rotation ABORTS this pass. The tables have no
	// unique arbiter, so inserting on top of an un-rotated snapshot
	// would duplicate every row (the old blanket ON CONFLICT DO
	// NOTHING never protected against this — it was dead code). The
	// next cycle retries with a fresh rotation.
	if err := ac.store.RotateLibyearToHistory(ctx, repoID); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			ac.logger.Error("failed to rotate libyear to history — skipping libyear insert this cycle",
				"repo_id", repoID, "error", err)
		}
		return fmt.Errorf("rotate libyear to history: %w", err)
	}

	var allDeps []libyearDep

	// v0.27.36: a root-stat failure makes Walk return without visiting
	// anything — silently producing ZERO dependencies for the repo.
	// Per-entry errors are still skipped inside the callback.
	if err := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// Reject symlinks to prevent traversal attacks (see scanDependencies).
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
		switch base {
		case "package.json":
			deps, err := parsePackageJSONVersions(path)
			if err != nil {
				ac.logger.Warn("failed to parse package.json versions", "path", path, "error", err)
			}
			allDeps = append(allDeps, deps...)
		case "requirements.txt":
			deps := parseRequirementsTxtVersions(path)
			allDeps = append(allDeps, deps...)
		case "pyproject.toml":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePyprojectVersionsFromContent(string(data))
				allDeps = append(allDeps, deps...)
				if ac.DevBuildDeps {
					allDeps = append(allDeps, parsePyprojectDevBuildVersions(string(data))...)
				}
			}
		case "setup.py":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseSetupPyVersions(string(data))
				allDeps = append(allDeps, deps...)
				if ac.DevBuildDeps {
					allDeps = append(allDeps, parseSetupPyDevBuildVersions(string(data))...)
				}
			}
		case "Pipfile":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePipfileVersions(string(data))
				allDeps = append(allDeps, deps...)
				if ac.DevBuildDeps {
					allDeps = append(allDeps, parsePipfileDevPackages(string(data))...)
				}
			}
		case "setup.cfg":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseSetupCfgVersions(string(data))
				allDeps = append(allDeps, deps...)
				if ac.DevBuildDeps {
					allDeps = append(allDeps, parseSetupCfgExtrasVersions(string(data))...)
				}
			}
		case "go.mod":
			deps := parseGoModVersions(path)
			allDeps = append(allDeps, deps...)
		case "Cargo.toml":
			deps := parseCargoVersions(path)
			allDeps = append(allDeps, deps...)
		case "Gemfile":
			deps := parseGemfileVersions(path)
			allDeps = append(allDeps, deps...)
		case "pom.xml":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePomXMLVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "build.gradle", "build.gradle.kts":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseBuildGradleVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "composer.json":
			deps, err := parseComposerJSONVersions(path)
			if err != nil {
				ac.logger.Warn("failed to parse composer.json versions", "path", path, "error", err)
			}
			allDeps = append(allDeps, deps...)
		case "mix.exs":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseMixExsVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "packages.config":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseNuGetPackagesConfigVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "Directory.Packages.props":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseDirectoryPackagesPropsVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "build.sbt":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseBuildSbtVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "pubspec.yaml":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePubspecVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "Package.swift":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePackageSwiftVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "package.yaml":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseHaskellPackageYamlVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		default:
			// v0.27.45 (summary/19 P2): requirements-variant files
			// (requirements-dev.txt, test_requirements.txt,
			// requirements/*.txt) with filename-token scope.
			if ac.DevBuildDeps {
				if scope, ok := requirementsFileScope(base, path); ok {
					allDeps = append(allDeps, parseRequirementsTxtVersionsScoped(path, scope)...)
				}
			}
			// v0.27.47 (summary/19 P4): GitHub Actions workflow
			// `uses:` references → build-scope deps.
			if ac.GitHubActionsDeps && isWorkflowPath(path) {
				if data, err := os.ReadFile(path); err == nil {
					allDeps = append(allDeps, parseWorkflowUses(string(data))...)
				}
			}
			// Extension-based matching (e.g., *.csproj).
			if strings.HasSuffix(base, ".csproj") {
				if data, err := os.ReadFile(path); err == nil {
					deps := parseCsprojVersions(string(data))
					allDeps = append(allDeps, deps...)
				}
			}
		}
		return nil
	}); err != nil {
		ac.logger.Warn("libyear manifest walk failed — dependency scan may be incomplete", "dir", workDir, "error", err)
	}

	// v0.27.71: central version-hygiene choke point. Every parser's
	// output passes through here before resolution, storage, and purl
	// construction — a malformed version (line-continuation backslash,
	// property interpolation, inline-table fragment, monorepo
	// protocol) becomes "" (the honest 'unpinned' pathway) instead of
	// silently defeating OSV version matching and registry lookups.
	// See version_normalize.go for the incident history.
	for i := range allDeps {
		allDeps[i].Version = normalizeParsedVersion(allDeps[i].Manager, allDeps[i].Version)
	}

	// v0.27.45 (summary/19 P2, Go C1): relabel go.mod deps whose
	// packages are imported ONLY from _test.go files as test-scope.
	// Ungated — relabel only, adds no rows.
	allDeps = classifyGoModTestOnlyDeps(workDir, allDeps)

	// Resolve each dependency against its package registry. Answers are
	// shared across the process for registryCacheTTL (resolveLibyearCached).
	resolveFailures := map[string]int{}
	resolveSample := map[string]error{}
	licenseFailures := 0
	nonRegistry := 0
	var licenseSample error
	swiftPM := func(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
		return resolveSwiftPMLibyear(ctx, ac.GitHubAPI, dep)
	}
	for _, dep := range allDeps {
		var resolve func(context.Context, libyearDep) (*db.LibyearRow, error)
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
		case "swiftpm":
			resolve = swiftPM
		case "githubactions":
			// v0.27.47: Actions have no registry timeline — store the
			// inventory row directly with libyear NULL (NoLibyear).
			// The upstream-dependencies snapshot filters libyear IS
			// NOT NULL, so actions never inflate the headline.
		default:
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
			ac.logger.Debug("libyear: dependency is not from a registry — not looked up",
				"dep", dep.Name, "manager", dep.Manager, "requirement", dep.Requirement)
			continue
		}
		var lb *db.LibyearRow
		var err error
		if resolve == nil {
			lb = &db.LibyearRow{
				Name: dep.Name, Requirement: dep.Requirement,
				Type: dep.Type, PackageManager: dep.Manager,
				CurrentVersion: dep.Version,
				Purl:           buildPurl("githubactions", dep.Name, dep.Version),
				NoLibyear:      true,
			}
		} else {
			lb, err = resolveLibyearCached(ctx, libyearRowCache, dep, resolve)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}

		if err != nil || lb == nil {
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
				ac.logger.Debug("libyear resolution failed", "dep", dep.Name, "manager", dep.Manager, "error", err)
			}
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
		markUnknownLibyear(lb)
		if err := ac.store.InsertRepoLibyear(ctx, repoID, lb); err != nil {
			// Round-8 class sweep: same flood shape as the dependency
			// loop above.
			if errors.Is(err, context.Canceled) {
				return err
			}
			ac.logger.Warn("failed to insert libyear", "dep", dep.Name, "error", err)
			continue
		}
		result.LibyearDeps++
	}

	if nonRegistry > 0 {
		// npm and cargo only: Python drops a VCS/URL/path requirement while
		// parsing (isNonRegistryPyRequirement, applied in
		// parseRequirementsTxtVersions and parsePyRequirement), because such
		// a line carries no package name to inventory, so it never reaches
		// this counter.
		ac.logger.Info("libyear: npm/cargo dependencies not from a registry were not looked up",
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

type libyearDep struct {
	Name        string
	Version     string
	Requirement string
	Type        string // "runtime", "dev"
	Manager     string // "npm", "pypi"
	// NonRegistry (v0.29.56) marks a dependency the manifest sources from
	// somewhere other than its ecosystem's registry: a local path or
	// workspace package, a git or URL reference. The registry knows nothing
	// about it — looking it up only drew 404s — and it is NOT the registry
	// package of the same name (SR-6), so scanLibyear skips it: no lookup
	// and no repo_deps_libyear row, counted in one INFO line per repo. The
	// dependency scan still inventories it in repo_dependencies.
	//
	// Storing a purl-less row instead was tried and reverted: a blank purl
	// counts as an UNSCANNABLE dependency in the vulnerability scan, which
	// would stop a repo whose deps are all local from being stamped
	// scan-complete (the v0.28.5 contract).
	NonRegistry bool
}

// registryHTTPClient bounds every registry request at 30s — the same
// budget the curl-era --max-time carried (v0.27.5's #2 hang class). The
// same figure is the whole-lookup budget for rate-limit waits
// (registryRetryBudget). Per-host PACING waits are separate and are not
// bounded by it (paceRegistryHost).
var registryHTTPClient = &http.Client{Timeout: 30 * time.Second}

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
	return io.ReadAll(resp.Body)
}

func parsePackageJSONVersions(path string) ([]libyearDep, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pkg struct {
		Dependencies         map[string]string `json:"dependencies"`
		DevDependencies      map[string]string `json:"devDependencies"`
		PeerDependencies     map[string]string `json:"peerDependencies"`
		OptionalDependencies map[string]string `json:"optionalDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []libyearDep
	add := func(m map[string]string, scope string) {
		for name, requirement := range m {
			if d, ok := npmManifestDep(name, requirement, scope); ok {
				deps = append(deps, d)
			}
		}
	}
	add(pkg.Dependencies, "runtime")
	add(pkg.DevDependencies, "dev")
	// v0.27.44 (summary/19 P1): peer/optional carry their OWN scopes —
	// they were conflated with runtime. Peer deps are expectations the
	// CONSUMER satisfies (not vendored by this package); optional deps
	// may be absent at runtime.
	add(pkg.PeerDependencies, model.ScopePeer)
	add(pkg.OptionalDependencies, model.ScopeOptional)
	return deps, nil
}

// npmManifestDep turns one package.json dependency into a registry lookup.
// v0.29.56: an alias ("typescript-3.5": "npm:typescript@~3.5.3") resolves
// the package it names, and a dependency that is not on the registry at
// all — a local package (file:, link:, portal:, workspace:) or a git, URL
// or GitHub-shorthand reference — is skipped: its key was sent to the npm
// registry as a name ("@ZettaScaleLabs/zenoh-ts": "file:…" in
// eclipse-zenoh/zenoh-demos → 404). catalog: stays: it names a registry
// version pinned elsewhere (the unpinned pathway).
func npmManifestDep(name, requirement, scope string) (libyearDep, bool) {
	req := strings.TrimSpace(requirement)
	lower := strings.ToLower(req)
	resolvedName, version := name, req
	if strings.HasPrefix(lower, "npm:") {
		alias := req[len("npm:"):]
		if n, v, ok := splitNPMSpec(alias); ok {
			resolvedName, version = n, v
		} else {
			resolvedName, version = alias, ""
		}
	}
	nonRegistry := false
	for _, prefix := range []string{"file:", "link:", "portal:", "workspace:", "git+", "git:", "git://", "github:", "gitlab:", "bitbucket:", "http://", "https://"} {
		if strings.HasPrefix(lower, prefix) {
			nonRegistry = true
		}
	}
	// GitHub shorthand "owner/repo" or "owner/repo#ref": a '/' outside a
	// version range, with no leading scope '@'.
	if strings.Contains(req, "/") && !strings.HasPrefix(lower, "npm:") && !strings.HasPrefix(req, "@") {
		nonRegistry = true
	}
	if resolvedName == "" {
		return libyearDep{}, false
	}
	return libyearDep{Name: resolvedName, Version: cleanVersion(version), Requirement: requirement,
		Type: scope, Manager: "npm", NonRegistry: nonRegistry}, true
}

func parseRequirementsTxtVersions(path string) []libyearDep {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var deps []libyearDep
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "-") {
			continue
		}
		// v0.27.71 (the zephyr false-CRITICAL incident): pip's
		// hash-pinned format ends the pin line with a backslash
		// continuation ("pyyaml==6.0.3 \"). The --hash continuation
		// lines are already skipped by the "-" prefix filter above,
		// but the backslash survived into the version — and from
		// there into purls, where it defeated OSV's version matching
		// (unparseable version → package-level match → the package's
		// entire advisory history reported as findings).
		line = strings.TrimSpace(strings.TrimSuffix(line, "\\"))
		// Inline comments, PEP 508 environment markers, and same-line
		// pip args are not version content ("attrs==3.1.0  # Apache-2.0",
		// "croniter==0.4.6 ; sys_platform == 'win32'").
		if idx := strings.Index(line, " #"); idx > 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if idx := strings.Index(line, ";"); idx > 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if idx := strings.Index(line, " --"); idx > 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}
		// AFTER the comment/marker strip: a comment can contain " @ " (the
		// direct-reference marker) — "numpy==1.26.0  # updated @ 2026-01-01"
		// was dropped whole, taking numpy out of libyear AND out of the OSV
		// scan.
		if isNonRegistryPyRequirement(line) {
			continue
		}
		name, spec := splitPyNameSpec(line)
		version := cleanVersion(pyFloorVersion(spec))
		// Strip PEP 508 extras from the name (anyio[trio] → anyio) —
		// the local-canary catch, 2026-07-21: extras-suffixed names
		// produced 404ing registry URLs and unmatchable purls, so
		// those deps silently got no libyear and no OSV coverage.
		name = strings.TrimSpace(stripPyExtras(name))
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "pypi"})
		}
	}
	return deps
}

// isNonRegistryPyRequirement reports a requirement line that does not name
// a PyPI package: a VCS or URL reference (git+https://…, https://…/x.tar.gz,
// file:…), a local path (./pkg, ../pkg, /opt/pkg) or a PEP 508 direct
// reference ("name @ url"). v0.29.56: these were sent to PyPI as names
// ("pypi/git+https://github.com/eclipse-velocitas/vehicle-model-python.git@v0.3.0/json").
func isNonRegistryPyRequirement(line string) bool {
	lower := strings.ToLower(line)
	for _, prefix := range []string{"git+", "hg+", "svn+", "bzr+", "http://", "https://", "file:", "./", "../", "/", "~/"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	// PEP 508's direct-reference marker takes OPTIONAL whitespace
	// ("urlspec = '@' URI_reference", with wsp* around it), so
	// "mypkg@https://host/x.whl" is as legal as the spaced form and matching
	// only " @ " sent it to PyPI as a package name (v0.29.56). A bare '@' is
	// safe to key on: neither a PEP 508 name ([A-Za-z0-9._-]) nor a version
	// specifier can contain one, and this runs on the line AFTER its comment
	// and environment marker are stripped.
	return strings.Contains(line, "@")
}

// cleanVersion strips version specifier prefixes (^, ~, >=, ==, etc.)
// and extracts the first version from compound ranges (>=1.0,<2.0).
func cleanVersion(v string) string {
	v = strings.TrimSpace(v)

	// For compound ranges (>=1.0,<2.0 or ^1 || ^2), take only the first segment.
	if idx := strings.IndexAny(v, ",|"); idx > 0 {
		v = v[:idx]
		v = strings.TrimSpace(v)
	}

	// Strip operator prefixes. Order matters: longest first.
	for _, prefix := range []string{"^", "~=", "~", ">=", "<=", "==", "!=", ">", "<", "="} {
		v = strings.TrimPrefix(v, prefix)
	}

	// Trim any whitespace left after operator removal (e.g., "~> 1.2" -> " 1.2").
	v = strings.TrimSpace(v)

	// v0.27.71: space-separated compound ranges keep the floor. pub
	// ("'>=0.11.1 <0.12.0'"), npm hyphen ranges ("1.2.3 - 2.3.4"),
	// and hex ("~> 3.0.0 and < 5.0.0") all separate bounds with
	// spaces, which the comma/pipe split above never saw — the whole
	// range leaked into versions and purls. No real version contains
	// a space, so cutting at the first one is universally safe.
	if idx := strings.IndexByte(v, ' '); idx > 0 {
		v = strings.TrimSpace(v[:idx])
	}
	return v
}

// decodeIfUTF16 detects UTF-16 BOM and converts to UTF-8. Some Windows-created
// manifest files (especially requirements.txt) are saved as UTF-16LE, producing
// null-interleaved ASCII that fails PostgreSQL's UTF-8 validation.
func decodeIfUTF16(data []byte) []byte {
	if len(data) < 2 {
		return data
	}
	// UTF-16LE BOM: 0xff 0xfe
	if data[0] == 0xff && data[1] == 0xfe {
		return utf16LEToUTF8(data[2:]) // skip BOM
	}
	// UTF-16BE BOM: 0xfe 0xff
	if data[0] == 0xfe && data[1] == 0xff {
		return utf16BEToUTF8(data[2:]) // skip BOM
	}
	return data
}

func utf16LEToUTF8(data []byte) []byte {
	if len(data)%2 != 0 {
		data = data[:len(data)-1] // drop trailing byte
	}
	var result []byte
	for i := 0; i+1 < len(data); i += 2 {
		ch := rune(data[i]) | rune(data[i+1])<<8
		if ch < 0x80 {
			result = append(result, byte(ch))
		} else if ch < 0x800 {
			result = append(result, byte(0xC0|(ch>>6)), byte(0x80|(ch&0x3F)))
		} else {
			result = append(result, byte(0xE0|(ch>>12)), byte(0x80|((ch>>6)&0x3F)), byte(0x80|(ch&0x3F)))
		}
	}
	return result
}

func utf16BEToUTF8(data []byte) []byte {
	if len(data)%2 != 0 {
		data = data[:len(data)-1]
	}
	var result []byte
	for i := 0; i+1 < len(data); i += 2 {
		ch := rune(data[i])<<8 | rune(data[i+1])
		if ch < 0x80 {
			result = append(result, byte(ch))
		} else if ch < 0x800 {
			result = append(result, byte(0xC0|(ch>>6)), byte(0x80|(ch&0x3F)))
		} else {
			result = append(result, byte(0xE0|(ch>>12)), byte(0x80|((ch>>6)&0x3F)), byte(0x80|(ch&0x3F)))
		}
	}
	return result
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
func resolveNPMLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx, npmRegistryBase+"/"+url.PathEscape(dep.Name))
	if err != nil {
		return nil, err
	}
	var info struct {
		DistTags struct {
			Latest string `json:"latest"`
		} `json:"dist-tags"`
		Time    map[string]string `json:"time"`
		License json.RawMessage   `json:"license"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
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

	currentDate := info.Time[dep.Version]
	latestDate := info.Time[info2.Version]
	libyear := calcLibyear(currentDate, latestDate)

	return &db.LibyearRow{
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
		Purl:               buildPurl("npm", dep.Name, dep.Version),
	}, nil
}

// resolvePyPILibyear checks PyPI for the latest version.
func resolvePyPILibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "pypi",
		License:            license,
		Purl:               buildPurl("pypi", dep.Name, dep.Version),
		CurrentVersion:     dep.Version,
		LatestVersion:      info.Info.Version,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            libyear,
	}, nil
}

// (parsePyprojectVersions replaced by parsePyprojectVersionsFromContent which
// correctly handles PEP 621 array format, not just Poetry key=value format.)

// parseGoModVersions extracts deps with versions from go.mod.
// Handles both block form "require (" and single-line "require module version".
func parseGoModVersions(path string) []libyearDep {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var deps []libyearDep
	inRequire := false
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		// Handle single-line require: "require github.com/foo/bar v1.2.3"
		if strings.HasPrefix(line, "require ") && !strings.Contains(line, "(") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				// Keep the v prefix — purl spec for golang keeps it, and
				// OSV.dev matches on versions with v. Stripping it broke
				// all Go vulnerability scanning.
				deps = append(deps, libyearDep{Name: parts[1], Version: parts[2], Requirement: line, Type: "runtime", Manager: "go"})
			}
			continue
		}

		if line == "require (" {
			inRequire = true
			continue
		}
		if line == ")" {
			inRequire = false
			continue
		}
		if inRequire && line != "" && !strings.HasPrefix(line, "//") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				// Keep the v prefix for purl and OSV compatibility.
				version := parts[1]
				deps = append(deps, libyearDep{Name: parts[0], Version: version, Requirement: line, Type: "runtime", Manager: "go"})
			}
		}
	}
	return deps
}

// parseCargoVersions extracts crates.io deps from Cargo.toml through the
// shared table scanner (manifest_toml.go).
//
// v0.27.44 (summary/19 P1): [dev-dependencies] = tests/examples/benches,
// [build-dependencies] = build scripts. v0.27.71: a table carries a
// version only under an explicit version key; a workspace-inherited dep
// has none (the unpinned pathway). v0.29.56: git- and
// alternative-registry-sourced deps are not crates.io packages, and a
// path dep counts only with the version it is published as, so none of
// them is looked up on crates.io.
func parseCargoVersions(path string) []libyearDep {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	scopes := map[string]string{
		"[dependencies]":       "runtime",
		"[dev-dependencies]":   model.ScopeDev,
		"[build-dependencies]": model.ScopeBuild,
	}
	sections := map[string]bool{}
	for s := range scopes {
		sections[s] = true
	}
	var deps []libyearDep
	for _, e := range scanTOMLDepTables(string(data), sections) {
		// A git or alternative-registry dep is not the crates.io crate of
		// that name; a path dep is only when it declares the version it
		// publishes as.
		nonRegistry := e.Git || e.Registry || (e.Path && e.Version == "")
		deps = append(deps, libyearDep{Name: e.Name, Version: cleanVersion(e.Version), Requirement: e.Raw,
			Type: scopes[e.Section], Manager: "cargo", NonRegistry: nonRegistry})
	}
	return deps
}

// parseGemfileVersions extracts deps with versions from Gemfile.
func parseGemfileVersions(path string) []libyearDep {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var deps []libyearDep
	// v0.27.44 (summary/19 P1): Bundler group blocks + inline group:
	// options were conflated as runtime. :test → test; :development
	// (and any other non-default group) → dev. Nested groups keep the
	// innermost classification; "end" pops the stack.
	var groupStack []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "group ") {
			groupStack = append(groupStack, gemGroupScope(line))
			continue
		}
		if line == "end" && len(groupStack) > 0 {
			groupStack = groupStack[:len(groupStack)-1]
			continue
		}
		if !strings.HasPrefix(line, "gem ") {
			continue
		}
		// v0.29.56: a trailing comment was read into the name or version
		// ("gem 'logger'   # stdlib in Ruby <= 3.x" → name
		// "logger'   # stdlib in Ruby <= 3.x", meshery/meshery.io).
		line = strings.TrimSpace(stripRubyComment(line))
		// gem 'name', '~> 1.0'
		parts := strings.Split(line, ",")
		name := ""
		version := ""
		if len(parts) >= 1 {
			name = strings.Trim(strings.TrimPrefix(strings.TrimSpace(parts[0]), "gem "), "\"' ")
		}
		if len(parts) >= 2 {
			// v0.27.71: only a QUOTED second argument is a version
			// requirement ("~> 1.0", ">= 1.1"). Unquoted keyword
			// options (require: false, path: "..", group: :x,
			// platforms: [..]) were captured as versions —
			// "require: false" was the #1 rubygems garbage version
			// in production (1,212 rows).
			if arg := strings.TrimSpace(parts[1]); strings.HasPrefix(arg, `"`) || strings.HasPrefix(arg, "'") {
				version = cleanVersion(strings.Trim(arg, "\"' "))
			}
		}
		scope := "runtime"
		if len(groupStack) > 0 {
			scope = groupStack[len(groupStack)-1]
		}
		if inlineGroup := gemInlineGroupScope(line); inlineGroup != "" {
			scope = inlineGroup
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "rubygems"})
		}
	}
	return deps
}

// stripRubyComment removes a '#' comment that is outside a quoted string
// (a '#' inside quotes, including "#{…}" interpolation, is kept).
func stripRubyComment(line string) string {
	var quote byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == '\\' {
				i++
			} else if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '#':
			return line[:i]
		}
	}
	return line
}

// gemGroupScope classifies a Bundler `group :x[, :y] do` line: any
// mention of :test → test; any other named group (development, ci,
// lint, ...) → dev. A group line is by definition non-default.
func gemGroupScope(line string) string {
	if strings.Contains(line, ":test") {
		return model.ScopeTest
	}
	return model.ScopeDev
}

// gemInlineGroupScope classifies `gem 'x', group: :test` /
// `groups: [:development, :test]` inline options; "" = no group option.
func gemInlineGroupScope(line string) string {
	if !strings.Contains(line, "group") {
		return ""
	}
	idx := strings.Index(line, "group")
	tail := line[idx:]
	if !strings.Contains(tail, ":") {
		return ""
	}
	if strings.Contains(tail, ":test") {
		return model.ScopeTest
	}
	if strings.Contains(tail, ":development") || strings.Contains(tail, ":dev") {
		return model.ScopeDev
	}
	return ""
}

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
			return "", fmt.Errorf("go module path or version %q cannot be case-encoded for the module proxy", s)
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
func resolveGoLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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
		// go.mod versions keep their "v" (parseGoModVersions); add one only
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
	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "go",
		CurrentVersion:     dep.Version,
		LatestVersion:      strings.TrimPrefix(latestInfo.Version, "v"),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestInfo.Time,
		Libyear:            calcLibyear(currentDate, latestInfo.Time),
		Purl:               buildPurl("golang", dep.Name, dep.Version),
	}, nil
}

// resolveCargoLibyear checks crates.io for Rust crate versions.
func resolveCargoLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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
	latestDate := ""
	license := ""
	// Normalize version for matching: Cargo.toml may say "1.0" but crates.io
	// lists "1.0.0". Without normalization, the version match fails and
	// the license is lost (was causing 19% of cargo deps to have empty license).
	normalizedVersion := normalizeSemanticVersion(dep.Version)
	for _, v := range info.Versions {
		if v.Num == dep.Version || v.Num == normalizedVersion {
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "cargo",
		CurrentVersion:     dep.Version,
		License:            license,
		Purl:               buildPurl("cargo", dep.Name, dep.Version),
		LatestVersion:      info.Crate.NewestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

// resolveRubyGemsLibyear checks rubygems.org for gem versions.
func resolveRubyGemsLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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
			latestLicense = strings.Join(versions[0].Licenses, " AND ")
		}
	}
	for _, v := range versions {
		if v.Number == dep.Version {
			currentDate = v.CreatedAt
			if len(v.Licenses) > 0 {
				license = strings.Join(v.Licenses, " AND ")
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		License:            license,
		Purl:               buildPurl("gem", dep.Name, dep.Version),
		PackageManager:     "rubygems",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

// markUnknownLibyear flags a row whose libyear could not be WORKED OUT, so
// it is stored as NULL rather than 0.
//
// calcLibyear returns 0 when either release date is missing or unparseable,
// which is indistinguishable from a dependency that really is on the latest
// release (v0.29.57). Measured on chaoss.tv on 2026-09-17: 1,704,902 of the
// 3,196,143 rows carrying a number had a missing date, so 87% of every row
// reading "libyear = 0" meant "unknown" — and avg() counts a zero while it
// skips a NULL, which reported the fleet at 1.335 years against an honest
// 2.861.
//
// NULL is the answer that already exists for "no timeline" (the v0.27.47
// GitHub Actions path) and every consumer already handles it. Two classes
// arrive here: a dependency with no pinned version, which can never be
// computed, and a pinned one whose registry would not give a date, which
// heals when it does.
//
// Applied once, where rows are collected — a dozen resolvers each building
// their own row is exactly where a rule like this drifts.
func markUnknownLibyear(row *db.LibyearRow) {
	if row == nil {
		return
	}
	// Asked of calcLibyear's OWN parser, not of emptiness (v0.29.57): the
	// two spellings would otherwise drift, and a date the parser cannot
	// read — the case the doc comment claims to cover — would still store a
	// fabricated 0 with the flag clear. No registry currently emits an
	// unparseable date, so this is latent; SR-17 says one spelling anyway.
	if _, okCur := parseLibyearDate(row.CurrentReleaseDate); !okCur {
		row.NoLibyear = true
	}
	if _, okLat := parseLibyearDate(row.LatestReleaseDate); !okLat {
		row.NoLibyear = true
	}
	if row.NoLibyear {
		// Never carry a number alongside the NULL: it would read as a
		// measurement if anything ever dropped the flag.
		row.Libyear = 0
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

	started := time.Now()
	cmd := exec.CommandContext(sccCtx, sccPath, "-f", "json", "--by-file", workDir)

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
	decodeErr := streamSCCLabor(dec, workDir, now, func(row *db.RepoLaborRow) {
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

	// Observation only (SR-7): nothing here skips, truncates or caps a
	// repo. The WARN exists so the frequency of the repo class that caused
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
	if err := ac.store.ReplaceRepoLaborSnapshot(ctx, repoID, laborRows); err != nil {
		return fmt.Errorf("replacing repo_labor snapshot: %w", err)
	}
	result.LaborFiles += len(laborRows)

	return nil
}

type sccLanguage struct {
	Name  string    `json:"Name"`
	Files []sccFile `json:"Files"`
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

// parseBuildGradle extracts dependency names from build.gradle / build.gradle.kts.
func parseBuildGradle(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		// Matches: implementation 'group:artifact:version' or implementation("group:artifact:version")
		for _, prefix := range []string{"implementation", "api", "compileOnly", "runtimeOnly", "testImplementation", "testRuntimeOnly", "testCompileOnly"} {
			if strings.HasPrefix(line, prefix) {
				// Extract the string between quotes.
				for _, q := range []string{"'", "\""} {
					start := strings.Index(line, q)
					if start < 0 {
						continue
					}
					end := strings.Index(line[start+1:], q)
					if end < 0 {
						continue
					}
					coord := line[start+1 : start+1+end]
					parts := strings.Split(coord, ":")
					if len(parts) >= 2 {
						deps = append(deps, parts[0]+":"+parts[1])
					}
					break
				}
			}
		}
	}
	return deps
}

// parseBuildGradleVersions extracts deps with versions from build.gradle / build.gradle.kts.
// Handles both Groovy (single-quoted) and Kotlin DSL (parenthesized double-quoted) syntax.
// Format: implementation 'group:artifact:version' or implementation("group:artifact:version")
func parseBuildGradleVersions(content string) []libyearDep {
	var deps []libyearDep
	prefixes := []string{"implementation", "api", "compileOnly", "runtimeOnly", "testImplementation", "testRuntimeOnly"}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "//") {
			continue
		}
		// v0.27.71: project(":core:util") references are the repo's
		// OWN modules, not external deps. The quoted ":"-separated
		// module path split like a maven coordinate and emitted fake
		// deps named ":core" with MODULE NAMES as versions (583+
		// production rows on apereo/cas alone).
		if strings.Contains(line, "project(") {
			continue
		}
		for _, prefix := range prefixes {
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			for _, q := range []string{"'", "\""} {
				start := strings.Index(line, q)
				if start < 0 {
					continue
				}
				end := strings.Index(line[start+1:], q)
				if end < 0 {
					continue
				}
				coord := line[start+1 : start+1+end]
				parts := strings.Split(coord, ":")
				if len(parts) >= 2 {
					name := parts[0] + ":" + parts[1]
					version := ""
					if len(parts) >= 3 {
						version = parts[2]
					}
					depType := "runtime"
					if strings.HasPrefix(prefix, "test") {
						depType = "dev"
					}
					deps = append(deps, libyearDep{
						Name:        name,
						Version:     version,
						Requirement: coord,
						Type:        depType,
						Manager:     "maven",
					})
				}
				break
			}
		}
	}
	return deps
}

// parseSetupCfgDeps extracts dependency names from setup.cfg [options] install_requires.
func parseSetupCfgDeps(content string) ([]string, error) {
	var deps []string
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Detect install_requires = (with possible inline deps)
		if strings.HasPrefix(trimmed, "install_requires") && strings.Contains(trimmed, "=") {
			inRequires = true
			// Check for inline deps after =
			afterEq := strings.SplitN(trimmed, "=", 2)
			if len(afterEq) == 2 {
				inline := strings.TrimSpace(afterEq[1])
				if inline != "" {
					if name := extractPyDepName(inline); name != "" {
						deps = append(deps, name)
					}
				}
			}
			continue
		}

		// In setup.cfg, continuation lines are indented.
		if inRequires {
			if trimmed == "" || (!strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t")) {
				// End of install_requires section (non-indented non-empty line).
				if trimmed != "" {
					inRequires = false
				}
				continue
			}
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if name := extractPyDepName(trimmed); name != "" {
				deps = append(deps, name)
			}
		}
	}
	return deps, nil
}

// parseSetupCfgVersions extracts deps with versions from setup.cfg.
func parseSetupCfgVersions(content string) []libyearDep {
	var deps []libyearDep
	lines := strings.Split(content, "\n")
	inRequires := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "install_requires") && strings.Contains(trimmed, "=") {
			inRequires = true
			afterEq := strings.SplitN(trimmed, "=", 2)
			if len(afterEq) == 2 {
				inline := strings.TrimSpace(afterEq[1])
				if inline != "" {
					if d := parsePyRequirement(inline); d != nil {
						deps = append(deps, *d)
					}
				}
			}
			continue
		}
		if inRequires {
			if trimmed == "" || (!strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t")) {
				if trimmed != "" {
					inRequires = false
				}
				continue
			}
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if d := parsePyRequirement(trimmed); d != nil {
				deps = append(deps, *d)
			}
		}
	}
	return deps
}

// parseCsprojDeps extracts dependency names from .csproj PackageReference elements.
// Format: <PackageReference Include="Name" Version="1.0.0" />
func parseCsprojDeps(content string) ([]string, error) {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageReference") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps, nil
}

// parseCsprojVersions extracts deps with versions from .csproj files.
func parseCsprojVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PackageReference") || !strings.Contains(line, "Include=") {
			continue
		}
		name := extractXMLAttr(line, "Include")
		version := extractXMLAttr(line, "Version")
		// v0.27.44 (summary/19 P1): PrivateAssets="all" marks a
		// build/analyzer-only package that never flows to consumers
		// or the runtime output → build. Only the attribute form is
		// visible to this line-based parser; the child-element form
		// (<PrivateAssets>all</PrivateAssets>) spans lines and is a
		// documented limitation.
		scope := "runtime"
		if strings.EqualFold(extractXMLAttr(line, "PrivateAssets"), "all") {
			scope = model.ScopeBuild
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "nuget"})
		}
	}
	return deps
}

// manifestExtensions lists file extensions that are detected in addition to the
// exact filename matches in manifestFiles. These are checked during filepath.Walk.
var manifestExtensions = []string{".csproj"}

// parseComposerJSON extracts dependency names from composer.json (PHP).
func parseComposerJSON(data []byte) ([]string, error) {
	var pkg struct {
		Require    map[string]string `json:"require"`
		RequireDev map[string]string `json:"require-dev"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []string
	for name := range pkg.Require {
		if name != "php" && !strings.HasPrefix(name, "ext-") {
			deps = append(deps, name)
		}
	}
	for name := range pkg.RequireDev {
		deps = append(deps, name)
	}
	return deps, nil
}

// parseBuildSbt extracts dependency names from Scala build.sbt.
func parseBuildSbt(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "libraryDependencies") || !strings.Contains(line, "%") {
			continue
		}
		// libraryDependencies += "org" %% "name" % "version"
		parts := strings.Split(line, "\"")
		if len(parts) >= 4 {
			org := parts[1]
			name := parts[3]
			deps = append(deps, org+":"+name)
		}
	}
	return deps
}

// parseNuGetPackagesConfig extracts package names from NuGet packages.config (XML).
func parseNuGetPackagesConfig(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "<package ") {
			continue
		}
		// Extract id attribute.
		if idx := strings.Index(line, `id="`); idx >= 0 {
			rest := line[idx+4:]
			if end := strings.Index(rest, `"`); end >= 0 {
				deps = append(deps, rest[:end])
			}
		}
	}
	return deps
}

// parsePackageYaml extracts dependency names from Haskell package.yaml (hpack).
func parsePackageYaml(content string) []string {
	var deps []string
	inDeps := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" {
			inDeps = true
			continue
		}
		if inDeps && strings.HasPrefix(trimmed, "- ") {
			dep := strings.TrimPrefix(trimmed, "- ")
			// Strip version constraints: "base >= 4.7 && < 5" -> "base"
			if idx := strings.IndexAny(dep, " ><=!"); idx > 0 {
				dep = dep[:idx]
			}
			if dep != "" {
				deps = append(deps, dep)
			}
		} else if inDeps && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && trimmed != "" {
			inDeps = false
		}
	}
	return deps
}

// parseMixExsDeps extracts dependency names from Elixir mix.exs.
func parseMixExsDeps(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{:") {
			continue
		}
		// {:phoenix, "~> 1.7.0"}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) < 1 {
			continue
		}
		name := strings.TrimPrefix(parts[0], "{:")
		name = strings.TrimSpace(name)
		if name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// ============================================================
// Additional libyear version parsers (added in v0.5.4)
// ============================================================

// parsePomXMLVersions extracts groupId:artifactId with version from pom.xml.
// Handles both multi-line and single-line XML formats.
func parsePomXMLVersions(content string) []libyearDep {
	var deps []libyearDep
	props := pomProperties(content)
	// Process dependency blocks — works even when everything is on one line
	// by scanning for <dependency>...</dependency> substrings.
	rest := content
	for {
		start := strings.Index(rest, "<dependency>")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], "</dependency>")
		if end < 0 {
			break
		}
		block := rest[start : start+end+len("</dependency>")]
		rest = rest[start+end+len("</dependency>"):]

		// v0.29.56: ${…} references resolve from the pom's <properties> and
		// project coordinates (knative/func "${quarkus.platform.group-id}",
		// dogtagpki/pki "${project.groupId}"). A coordinate that stays
		// unresolved is not sent to Maven Central; an unresolved version
		// is left for normalizeParsedVersion's unpinned pathway.
		groupID := resolvePomProperties(extractXMLValue(block, "groupId"), props)
		artifactID := resolvePomProperties(extractXMLValue(block, "artifactId"), props)
		version := resolvePomProperties(extractXMLValue(block, "version"), props)
		if strings.Contains(groupID, "${") || strings.Contains(artifactID, "${") {
			continue
		}
		// v0.27.44 (summary/19 P1): Maven's own scope element.
		// test → test; provided = compile-time-only, supplied by the
		// runtime container → build. compile/runtime/absent → runtime.
		depScope := "runtime"
		switch extractXMLValue(block, "scope") {
		case "test":
			depScope = model.ScopeTest
		case "provided":
			depScope = model.ScopeBuild
		}
		if groupID != "" && artifactID != "" {
			name := groupID + ":" + artifactID
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: name + ":" + version, Type: depScope, Manager: "maven"})
		}
	}
	return deps
}

// pomPropertyRe matches one <name>value</name> element; the closing name
// is compared in code (RE2 has no backreferences).
var pomPropertyRe = regexp.MustCompile(`<([A-Za-z0-9_.\-]+)>([^<]*)</([A-Za-z0-9_.\-]+)>`)

// pomProperties collects a pom's own property values: every
// <properties> element, plus project.groupId/artifactId/version (the
// project's own coordinates, falling back to <parent>'s groupId and
// version, as Maven inherits them) and their parent.* forms.
func pomProperties(content string) map[string]string {
	props := map[string]string{}
	rest := content
	for {
		start := strings.Index(rest, "<properties>")
		if start < 0 {
			break
		}
		end := strings.Index(rest[start:], "</properties>")
		if end < 0 {
			break
		}
		for _, m := range pomPropertyRe.FindAllStringSubmatch(rest[start:start+end], -1) {
			if m[1] == m[3] {
				if _, seen := props[m[1]]; !seen {
					props[m[1]] = strings.TrimSpace(m[2])
				}
			}
		}
		rest = rest[start+end:]
	}
	parent := ""
	top := content
	if ps := strings.Index(content, "<parent>"); ps >= 0 {
		if pe := strings.Index(content[ps:], "</parent>"); pe >= 0 {
			parent = content[ps : ps+pe]
			top = content[:ps] + content[ps+pe:]
		}
	}
	// The project's own coordinates are the ones outside every nested block.
	for _, block := range []string{"dependencyManagement", "dependencies", "build", "profiles", "reporting", "properties", "modules"} {
		for {
			bs := strings.Index(top, "<"+block+">")
			if bs < 0 {
				break
			}
			be := strings.Index(top[bs:], "</"+block+">")
			if be < 0 {
				break
			}
			top = top[:bs] + top[bs+be+len("</"+block+">"):]
		}
	}
	for _, key := range []string{"groupId", "artifactId", "version"} {
		if v := extractXMLValue(parent, key); v != "" {
			props["parent."+key] = v
			props["project.parent."+key] = v
		}
		v := extractXMLValue(top, key)
		if v == "" && key != "artifactId" {
			v = extractXMLValue(parent, key)
		}
		if v != "" {
			props["project."+key] = v
			props["pom."+key] = v
		}
	}
	return props
}

// resolvePomProperties substitutes ${name} references from props. A value
// may reference another property, so substitution repeats — at most once
// per property, which bounds a reference cycle.
func resolvePomProperties(v string, props map[string]string) string {
	for range len(props) + 1 {
		start := strings.Index(v, "${")
		if start < 0 {
			return v
		}
		end := strings.Index(v[start:], "}")
		if end < 0 {
			return v
		}
		val, ok := props[v[start+2:start+end]]
		if !ok {
			return v
		}
		v = v[:start] + val + v[start+end+1:]
	}
	return v
}

func extractXMLValue(line, tag string) string {
	start := strings.Index(line, "<"+tag+">")
	end := strings.Index(line, "</"+tag+">")
	if start < 0 || end < 0 {
		return ""
	}
	return strings.TrimSpace(line[start+len(tag)+2 : end])
}

// parseComposerJSONVersions extracts deps with versions from composer.json.
func parseComposerJSONVersions(path string) ([]libyearDep, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pkg struct {
		Require    map[string]string `json:"require"`
		RequireDev map[string]string `json:"require-dev"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	var deps []libyearDep
	for name, version := range pkg.Require {
		if name == "php" || strings.HasPrefix(name, "ext-") {
			continue
		}
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "runtime", Manager: "packagist"})
	}
	for name, version := range pkg.RequireDev {
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "dev", Manager: "packagist"})
	}
	return deps, nil
}

// parseMixExsVersions extracts deps with versions from Elixir mix.exs.
func parseMixExsVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{:") {
			continue
		}
		// {:phoenix, "~> 1.7.0"}
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimPrefix(strings.TrimSpace(parts[0]), "{:")
		// v0.27.71: only a QUOTED second element is a version
		// requirement ("~> 1.7.0"). Keyword options (git:, github:,
		// path:) mark source deps with no registry version — the
		// pre-fix code captured the option text as the version.
		version := ""
		if p1 := strings.TrimSpace(parts[1]); strings.HasPrefix(p1, `"`) {
			version = cleanVersion(strings.Trim(p1, "\"'}] "))
		}
		// v0.27.44 (summary/19 P1): Mix's only: option restricts a dep
		// to specific envs. only: :test → test; anything mentioning
		// :dev → dev (only: [:dev, :test] is a dev-tooling dep that
		// also runs in test). No only: → runtime.
		scope := "runtime"
		if onlyIdx := strings.Index(line, "only:"); onlyIdx >= 0 {
			onlyPart := line[onlyIdx:]
			switch {
			case strings.Contains(onlyPart, ":dev"):
				scope = model.ScopeDev
			case strings.Contains(onlyPart, ":test"):
				scope = model.ScopeTest
			}
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "hex"})
		}
	}
	return deps
}

// parseNuGetPackagesConfigVersions extracts packages with versions from packages.config.
func parseNuGetPackagesConfigVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "<package ") && !strings.HasPrefix(line, "<Package ") {
			continue
		}
		name := ""
		version := ""
		// Case-insensitive attribute matching: NuGet packages.config in older
		// .NET projects frequently uses Id="..." and Version="..." (capital letters).
		lower := strings.ToLower(line)
		if idx := strings.Index(lower, `id="`); idx >= 0 {
			rest := line[idx+4:]
			if end := strings.Index(rest, `"`); end >= 0 {
				name = rest[:end]
			}
		}
		if idx := strings.Index(lower, `version="`); idx >= 0 {
			rest := line[idx+9:]
			if end := strings.Index(rest, `"`); end >= 0 {
				version = rest[:end]
			}
		}
		// v0.27.44 (summary/19 P1): developmentDependency="true" is the
		// legacy packages.config marker for build-only packages.
		scope := "runtime"
		if strings.Contains(lower, `developmentdependency="true"`) {
			scope = model.ScopeBuild
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: scope, Manager: "nuget"})
		}
	}
	return deps
}

// parseBuildSbtVersions extracts deps with versions from build.sbt.
func parseBuildSbtVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "libraryDependencies") || !strings.Contains(line, "%") {
			continue
		}
		parts := strings.Split(line, "\"")
		if len(parts) >= 6 {
			org := parts[1]
			name := parts[3]
			version := parts[5]
			fullName := org + ":" + name
			// v0.27.44 (summary/19 P1): sbt configuration after the
			// version — `% Test` (or the quoted "test" form) → test;
			// `% Provided` mirrors Maven provided → build;
			// `% Optional` → optional. No configuration → runtime.
			scope := "runtime"
			switch {
			case strings.Contains(line, "% Test") || strings.Contains(line, `% "test"`):
				scope = model.ScopeTest
			case strings.Contains(line, "% Provided") || strings.Contains(line, `% "provided"`):
				scope = model.ScopeBuild
			case strings.Contains(line, "% Optional") || strings.Contains(line, `% "optional"`):
				scope = model.ScopeOptional
			}
			deps = append(deps, libyearDep{Name: fullName, Version: version, Requirement: line, Type: scope, Manager: "maven"})
		}
	}
	return deps
}

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
func resolveMavenLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	groupID, artifactID, ok := strings.Cut(dep.Name, ":")
	if !ok || groupID == "" || artifactID == "" {
		return nil, fmt.Errorf("invalid maven coordinate: %s", dep.Name)
	}
	if strings.Contains(dep.Name, "${") {
		// An unresolved property reference is not a coordinate; sending it
		// only draws a 400/404 (parsePomXMLVersions resolves what it can).
		return nil, fmt.Errorf("maven coordinate %s has an unresolved property reference", dep.Name)
	}
	artifactBase := mavenRepositoryBase + "/" + strings.ReplaceAll(groupID, ".", "/") + "/" + url.PathEscape(artifactID)
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "maven",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               buildPurl("maven", groupID+"/"+artifactID, dep.Version),
	}, nil
}

// resolvePackagistLibyear checks Packagist (PHP) for package versions.
func resolvePackagistLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(packagistRegistryBase+"/p2/%s.json", dep.Name))
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
	versions := info.Packages[dep.Name]
	if len(versions) == 0 {
		return nil, fmt.Errorf("not found on Packagist: %s", dep.Name)
	}
	// First entry is the latest version.
	latest := versions[0]
	currentDate := ""
	for _, v := range versions {
		if cleanVersion(v.Version) == dep.Version {
			currentDate = v.Time
			break
		}
	}
	license := ""
	if len(latest.License) > 0 {
		license = strings.Join(latest.License, " AND ")
	}

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "packagist",
		CurrentVersion:     dep.Version,
		LatestVersion:      cleanVersion(latest.Version),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latest.Time,
		Libyear:            calcLibyear(currentDate, latest.Time),
		License:            license,
		Purl:               buildPurl("composer", dep.Name, dep.Version),
	}, nil
}

// resolveHexLibyear checks hex.pm (Elixir/Erlang) for package versions.
func resolveHexLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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
		return nil, fmt.Errorf("not found on Hex: %s", dep.Name)
	}
	latest := info.Releases[0]
	currentDate := ""
	for _, r := range info.Releases {
		if r.Version == dep.Version {
			currentDate = r.InsertedAt
			break
		}
	}
	license := ""
	if len(info.Meta.Licenses) > 0 {
		license = strings.Join(info.Meta.Licenses, " AND ")
	}

	return &db.LibyearRow{
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
		Purl:               buildPurl("hex", dep.Name, dep.Version),
	}, nil
}

// resolveNuGetLibyear checks nuget.org for .NET package versions.
func resolveNuGetLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// NuGet registration API.
	body, err := fetchRegistryJSON(ctx,
		fmt.Sprintf(nugetRegistryBase+"/v3/registration5-semver1/%s/index.json",
			strings.ToLower(dep.Name)))
	if err != nil {
		return nil, err
	}
	var info struct {
		Items []struct {
			Upper string `json:"upper"`
			Items []struct {
				CatalogEntry struct {
					Version           string `json:"version"`
					Published         string `json:"published"`
					LicenseExpression string `json:"licenseExpression"`
				} `json:"catalogEntry"`
			} `json:"items"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}

	latestVersion := ""
	latestDate := ""
	currentDate := ""
	license := ""
	// Walk all pages — latest is the last item in the last page.
	for _, page := range info.Items {
		for _, item := range page.Items {
			entry := item.CatalogEntry
			latestVersion = entry.Version
			latestDate = entry.Published
			license = entry.LicenseExpression
			if strings.EqualFold(entry.Version, dep.Version) {
				currentDate = entry.Published
			}
		}
	}

	return &db.LibyearRow{
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
		Purl:               buildPurl("nuget", dep.Name, dep.Version),
	}, nil
}

// ============================================================
// Dart (pub.dev) parsers and resolver (added in v0.5.4)
// ============================================================

// parsePubspecDeps extracts dependency names from Dart pubspec.yaml.
func parsePubspecDeps(content string) []string {
	var deps []string
	inDeps := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" || trimmed == "dev_dependencies:" {
			inDeps = true
			continue
		}
		// Section ends at a top-level key (no leading whitespace).
		if inDeps && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inDeps = false
		}
		if !inDeps {
			continue
		}
		// Skip sdk dependencies like "flutter: sdk: flutter".
		if strings.Contains(trimmed, "sdk:") {
			continue
		}
		// Line like "  http: ^0.13.6" — name is the key before the colon.
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "#") {
			name := strings.TrimSpace(strings.SplitN(trimmed, ":", 2)[0])
			if name != "" && name != "flutter" && name != "flutter_test" {
				deps = append(deps, name)
			}
		}
	}
	return deps
}

// parsePubspecVersions extracts deps with versions from Dart pubspec.yaml.
func parsePubspecVersions(content string) []libyearDep {
	var deps []libyearDep
	inDeps := false
	depType := "runtime"
	// v0.27.71: block-style deps nest their source under the name:
	//
	//	my_lib:
	//	  path: ../
	//
	// The pre-fix parser emitted the SUB-KEYS as deps — a package
	// named "path" at version "../" (90 production rows), fake "git"/
	// "url"/"ref" packages. Track the indent of the first dep line in
	// each section; anything deeper is a sub-key, not a dep. (Name
	// filtering would be wrong: "path" is a real pub.dev package that
	// legitimately appears at dep level.)
	depIndent := -1
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" {
			inDeps = true
			depType = "runtime"
			depIndent = -1
			continue
		}
		if trimmed == "dev_dependencies:" {
			inDeps = true
			depType = "dev"
			depIndent = -1
			continue
		}
		if inDeps && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inDeps = false
		}
		if !inDeps || strings.Contains(trimmed, "sdk:") {
			continue
		}
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "#") {
			indent := len(line) - len(strings.TrimLeft(line, " \t"))
			if depIndent == -1 {
				depIndent = indent
			}
			if indent > depIndent {
				continue // sub-key of a block-style dep
			}
			parts := strings.SplitN(trimmed, ":", 2)
			name := strings.TrimSpace(parts[0])
			if name == "" || name == "flutter" || name == "flutter_test" {
				continue
			}
			versionStr := strings.TrimSpace(parts[1])
			version := cleanVersion(strings.Trim(versionStr, "\"' "))
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: depType, Manager: "pub"})
		}
	}
	return deps
}

// resolvePubDevLibyear checks pub.dev (Dart/Flutter) for package versions.
func resolvePubDevLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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
	for _, v := range info.Versions {
		if v.Version == dep.Version {
			currentDate = v.Published
			break
		}
	}
	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "pub",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               buildPurl("pub", dep.Name, dep.Version),
	}, nil
}

// ============================================================
// Swift (SwiftPM) parsers and resolver (added in v0.5.4)
// ============================================================

// parsePackageSwiftDeps extracts dependency names from Package.swift.
func parsePackageSwiftDeps(content string) []string {
	var deps []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, ".package(url:") {
			continue
		}
		// Extract repo name from URL: "https://github.com/Alamofire/Alamofire.git"
		if name := extractSwiftPackageName(line); name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// parsePackageSwiftVersions extracts deps with versions from Package.swift.
func parsePackageSwiftVersions(content string) []libyearDep {
	var deps []libyearDep
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, ".package(url:") {
			continue
		}
		name := extractSwiftPackageName(line)
		if name == "" {
			continue
		}
		// Extract version: from: "5.6.0", exact: "5.0.1", .upToNextMajor(from: "10.40.0")
		version := extractSwiftVersion(line)
		repoURL := extractSwiftRepoURL(line)
		deps = append(deps, libyearDep{
			Name:        name,
			Version:     version,
			Requirement: repoURL,
			Type:        "runtime",
			Manager:     "swiftpm",
		})
	}
	return deps
}

// extractSwiftPackageName pulls the repo name from a .package(url:...) line.
func extractSwiftPackageName(line string) string {
	// Find URL between quotes after "url:"
	urlStart := strings.Index(line, `url:`)
	if urlStart < 0 {
		return ""
	}
	rest := line[urlStart+4:]
	// Find the quoted URL.
	q1 := strings.IndexByte(rest, '"')
	if q1 < 0 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '"')
	if q2 < 0 {
		return ""
	}
	url := rest[q1+1 : q1+1+q2]
	// Extract repo name: last path component, strip .git suffix.
	parts := strings.Split(strings.TrimSuffix(url, ".git"), "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// extractSwiftRepoURL pulls the git URL from a .package(url:...) line.
func extractSwiftRepoURL(line string) string {
	urlStart := strings.Index(line, `url:`)
	if urlStart < 0 {
		return ""
	}
	rest := line[urlStart+4:]
	q1 := strings.IndexByte(rest, '"')
	if q1 < 0 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '"')
	if q2 < 0 {
		return ""
	}
	return rest[q1+1 : q1+1+q2]
}

// extractSwiftVersion pulls the version from patterns like:
//
//	from: "5.6.0", exact: "5.0.1", .upToNextMajor(from: "10.40.0")
func extractSwiftVersion(line string) string {
	// Try patterns in order: from: "x", exact: "x", .upToNextMajor(from: "x")
	for _, prefix := range []string{`from: "`, `exact: "`, `.upToNextMajor(from: "`, `.upToNextMinor(from: "`} {
		idx := strings.Index(line, prefix)
		if idx < 0 {
			continue
		}
		rest := line[idx+len(prefix):]
		end := strings.IndexByte(rest, '"')
		if end > 0 {
			return rest[:end]
		}
	}
	return ""
}

// resolveSwiftPMLibyear resolves a SwiftPM dependency from its GitHub
// repository's releases. SwiftPM packages are git repositories, not a
// registry. Requests go through the key-pooled GitHub client (v0.29.56;
// they were anonymous, 60 an hour per IP, and failed with 403/429 in the
// 2026-09-17 log). The version in use is looked up as a release tag, with
// and without a leading "v", so libyear is computed instead of 0.
func resolveSwiftPMLibyear(ctx context.Context, gh githubAPIGetter, dep libyearDep) (*db.LibyearRow, error) {
	// The requirement field contains the git URL. Extract owner/repo.
	repoURL := dep.Requirement
	repoURL = strings.TrimSuffix(repoURL, ".git")
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(repoURL, "https://"), "http://"), "/")
	if len(parts) < 3 || !strings.Contains(parts[0], "github.com") {
		// Non-GitHub SwiftPM packages can't be resolved without a registry.
		return nil, fmt.Errorf("SwiftPM resolver only supports GitHub repos: %s", repoURL)
	}
	if gh == nil {
		return nil, errNoGitHubClient
	}
	owner, repo := parts[1], parts[2]
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "swiftpm",
		CurrentVersion:     dep.Version,
		LatestVersion:      strings.TrimPrefix(latest.TagName, "v"),
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latest.PublishedAt,
		Libyear:            calcLibyear(currentDate, latest.PublishedAt),
		Purl:               buildPurl("swift", owner+"/"+repo, dep.Version),
	}, nil
}

// ============================================================
// Haskell (Hackage) parsers and resolver (added in v0.5.4)
// ============================================================

// parseHaskellPackageYamlVersions extracts deps with versions from package.yaml.
// Version constraints like ">= 2.0" are parsed to extract the lower bound.
func parseHaskellPackageYamlVersions(content string) []libyearDep {
	var deps []libyearDep
	inDeps := false
	// v0.27.44 (summary/19 P1): a dependencies: block nested under a
	// top-level tests: or benchmarks: section is test-only. The
	// section tracker watches unindented `key:` lines; library:,
	// executables:, and the top level stay runtime.
	sectionScope := "runtime"
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && strings.HasSuffix(trimmed, ":") {
			switch strings.TrimSuffix(trimmed, ":") {
			case "tests", "benchmarks":
				sectionScope = model.ScopeTest
			default:
				sectionScope = "runtime"
			}
		}
		if trimmed == "dependencies:" {
			inDeps = true
			continue
		}
		if inDeps && strings.HasPrefix(trimmed, "- ") {
			dep := strings.TrimPrefix(trimmed, "- ")
			name := dep
			version := ""
			// Parse "base >= 4.7 && < 5" -> name="base", version="4.7"
			if idx := strings.IndexAny(dep, " ><=!"); idx > 0 {
				name = dep[:idx]
				// Extract the first version number from the constraint.
				rest := dep[idx:]
				version = extractFirstVersion(rest)
			}
			if name != "" {
				deps = append(deps, libyearDep{Name: name, Version: version, Requirement: dep, Type: sectionScope, Manager: "hackage"})
			}
		} else if inDeps && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && trimmed != "" {
			inDeps = false
		}
	}
	return deps
}

// extractFirstVersion pulls the first semver-like number from a constraint string.
// e.g., ">= 2.0 && < 3" -> "2.0", " ^4.7" -> "4.7"
func extractFirstVersion(s string) string {
	var start, end int
	inVersion := false
	for i, c := range s {
		if (c >= '0' && c <= '9') || c == '.' {
			if !inVersion {
				start = i
				inVersion = true
			}
			end = i + 1
		} else if inVersion {
			break
		}
	}
	if inVersion {
		return strings.TrimRight(s[start:end], ".")
	}
	return ""
}

// resolveHackageLibyear checks Hackage for Haskell package versions.
func resolveHackageLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
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

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "hackage",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
		Purl:               buildPurl("hackage", dep.Name, dep.Version),
	}, nil
}

// ============================================================
// Scanning via git ls-files (for bare repos without full clone)
// ============================================================

// ListRepoFiles returns file paths from a bare clone using git ls-tree.
func ListRepoFiles(ctx context.Context, barePath string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", barePath, "ls-tree", "-r", "--name-only", "HEAD")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, execErr(ctx, err)
	}
	var files []string
	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		files = append(files, scanner.Text())
	}
	return files, scanner.Err()
}

// scanScanCode was the v0.20-and-earlier per-job wrapper around
// RunScanCode. Removed in v0.21.0 along with RunScanCode itself
// when scancode moved to the dedicated ScancodeWorker pool. See
// internal/collector/scancode_worker.go and
// docs/architecture/scancode.md.
