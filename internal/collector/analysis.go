// Package collector — analysis.go implements on-demand repo analysis phases:
// dependency scanning, libyear calculation, and code complexity (scc).
//
// These phases require a full checkout (not a bare clone) to scan file contents.
// A temporary working copy is created from the existing bare clone, analysis
// tools run against it, results are inserted into the database, and the
// working copy is immediately deleted to minimize disk usage.
//
// Design: bare clones (permanent, small) for git log/commits.
//         full clones (temporary, on-demand) for file analysis.
package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
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

// AnalysisResult tracks what was collected.
type AnalysisResult struct {
	Dependencies  int
	LibyearDeps   int
	LaborFiles    int
	ScancodeFiles int // files with scancode findings (licenses, copyrights, packages)
	Errors        []error

	// ClonePath is the path to the temporary full clone. Only set when
	// AnalysisCollector.RetainClone is true. The caller must clean it up
	// with os.RemoveAll after any post-analysis work (e.g., scorecard).
	ClonePath string
}

// AnalyzeRepo creates a temporary full checkout from the bare clone,
// runs all analysis phases, inserts results, then deletes the checkout.
func (ac *AnalysisCollector) AnalyzeRepo(ctx context.Context, repoID int64) (*AnalysisResult, error) {
	result := &AnalysisResult{}

	barePath := filepath.Join(ac.bareDir, fmt.Sprintf("repo_%d", repoID))
	if _, err := os.Stat(filepath.Join(barePath, "HEAD")); err != nil {
		return result, fmt.Errorf("no bare clone at %s", barePath)
	}

	// Create temporary full clone from the bare repo (local clone, no network).
	workDir := filepath.Join(ac.tempDir, fmt.Sprintf("repo_%d_%d", repoID, time.Now().UnixNano()))
	if !ac.RetainClone {
		defer func() {
			os.RemoveAll(workDir)
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
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return result, fmt.Errorf("local clone failed: %w: %s", err, stderr.String())
	}

	// Phase 1: Dependency scanning.
	if err := ac.scanDependencies(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("dependencies: %w", err))
	}

	// Phase 2: Libyear (dependency age).
	if err := ac.scanLibyear(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("libyear: %w", err))
	}

	// Phase 3: Code complexity via scc (if installed).
	if err := ac.scanSCC(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("scc: %w", err))
	}

	// Phase 4: ScanCode — license, copyright, and package detection (if installed).
	// Runs every 30 days per repo. Skipped if scancode is not installed or if
	// the last scan was recent.
	if err := ac.scanScanCode(ctx, repoID, workDir, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("scancode: %w", err))
	}

	// When RetainClone is true, hand the clone path to the caller for
	// post-analysis work (e.g., local scorecard execution).
	if ac.RetainClone {
		result.ClonePath = workDir
	}

	ac.logger.Info("analysis complete",
		"repo_id", repoID,
		"dependencies", result.Dependencies,
		"libyear_deps", result.LibyearDeps,
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
	"package.json":    "JavaScript",
	"yarn.lock":       "JavaScript",
	"requirements.txt": "Python",
	"setup.py":        "Python",
	"setup.cfg":       "Python",
	"pyproject.toml":  "Python",
	"Pipfile":         "Python",
	"poetry.lock":     "Python",
	"go.mod":          "Go",
	"go.sum":          "Go",
	"Cargo.toml":      "Rust",
	"Cargo.lock":      "Rust",
	"Gemfile":         "Ruby",
	"Gemfile.lock":    "Ruby",
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
		// Strip extras: "requests[security]>=2.0" -> "requests>=2.0"
		if idx := strings.Index(line, "["); idx > 0 {
			rest := line[idx:]
			if end := strings.Index(rest, "]"); end > 0 {
				line = line[:idx] + rest[end+1:]
			}
		}
		// Strip version specifiers. Include "<" for bare less-than constraints.
		for _, sep := range []string{"===", "==", ">=", "<=", "!=", "~=", ">", "<"} {
			if idx := strings.Index(line, sep); idx > 0 {
				line = line[:idx]
				break
			}
		}
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

func parseTOMLDeps(content, section string) []string {
	var deps []string
	inSection := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == section {
			inSection = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inSection = false
			continue
		}
		if inSection && strings.Contains(trimmed, "=") {
			name := strings.TrimSpace(strings.SplitN(trimmed, "=", 2)[0])
			if name != "" && name != "python" {
				deps = append(deps, name)
			}
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
	line = strings.Trim(line, "\",")
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return ""
	}
	// Strip environment markers: everything after ';'
	if idx := strings.Index(line, ";"); idx > 0 {
		line = strings.TrimSpace(line[:idx])
	}
	// Strip extras: name[extra]>=1.0
	if idx := strings.Index(line, "["); idx > 0 {
		line = line[:idx] + line[strings.Index(line, "]")+1:]
	}
	// Extract name before version specifiers.
	for _, sep := range []string{">=", "<=", "!=", "~=", "==", ">", "<"} {
		if idx := strings.Index(line, sep); idx > 0 {
			return strings.TrimSpace(line[:idx])
		}
	}
	return strings.TrimSpace(line)
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
	inner := line[start:end]
	for _, item := range strings.Split(inner, ",") {
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
	// Strip extras: name[extra]
	if idx := strings.Index(req, "["); idx > 0 {
		req = req[:idx]
	}
	// Strip version specifiers.
	for _, sep := range []string{">=", "<=", "!=", "~=", "==", ">", "<"} {
		if idx := strings.Index(req, sep); idx > 0 {
			return strings.TrimSpace(req[:idx])
		}
	}
	return strings.TrimSpace(req)
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

// extractQuotedPyVersionDeps extracts deps with versions from quoted Python strings.
func extractQuotedPyVersionDeps(line string) []libyearDep {
	var deps []libyearDep
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
			if d := parsePyRequirement(depStr); d != nil {
				deps = append(deps, *d)
			}
		}
	}
	return deps
}

// parsePyRequirement parses a single Python requirement string into a libyearDep.
func parsePyRequirement(req string) *libyearDep {
	req = strings.TrimSpace(req)
	if req == "" {
		return nil
	}
	// Strip environment markers.
	if idx := strings.Index(req, ";"); idx > 0 {
		req = strings.TrimSpace(req[:idx])
	}
	// Strip extras.
	cleanReq := req
	if idx := strings.Index(cleanReq, "["); idx > 0 {
		endBracket := strings.Index(cleanReq, "]")
		if endBracket > idx {
			cleanReq = cleanReq[:idx] + cleanReq[endBracket+1:]
		}
	}

	name := cleanReq
	version := ""
	for _, sep := range []string{"==", ">=", "<=", "~=", "!=", ">", "<"} {
		if idx := strings.Index(cleanReq, sep); idx > 0 {
			name = strings.TrimSpace(cleanReq[:idx])
			version = strings.TrimSpace(cleanReq[idx+len(sep):])
			// Strip trailing version bounds: ">=1.10.0,<1.13.0" -> "1.10.0"
			if commaIdx := strings.Index(version, ","); commaIdx > 0 {
				version = version[:commaIdx]
			}
			break
		}
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	return &libyearDep{Name: name, Version: version, Requirement: req, Type: "runtime", Manager: "pypi"}
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
				for _, kv := range strings.Split(versionRaw, ",") {
					kv = strings.TrimSpace(kv)
					if strings.HasPrefix(kv, "{") {
						kv = strings.TrimPrefix(kv, "{")
					}
					kv = strings.TrimSuffix(kv, "}")
					kv = strings.TrimSpace(kv)
					if strings.HasPrefix(kv, "version") {
						kvParts := strings.SplitN(kv, "=", 2)
						if len(kvParts) == 2 {
							version = cleanVersion(strings.Trim(strings.TrimSpace(kvParts[1]), "\"' "))
						}
						break
					}
				}
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
	inner := line[start:end]
	for _, item := range strings.Split(inner, ",") {
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
			version := cleanVersion(strings.Trim(strings.TrimSpace(parts[1]), "\"'^~>="))
			if name != "" && name != "python" {
				deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: "runtime", Manager: "pypi"})
			}
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
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "nuget"})
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

	// Rotate previous libyear data to history before inserting fresh data.
	// This ensures the main table always has the latest snapshot with current
	// license values. Without rotation, old rows with empty licenses persist
	// because ON CONFLICT DO NOTHING skips existing rows.
	if err := ac.store.RotateLibyearToHistory(ctx, repoID); err != nil {
		ac.logger.Warn("failed to rotate libyear to history", "repo_id", repoID, "error", err)
	}

	var allDeps []libyearDep

	filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
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
			}
		case "setup.py":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseSetupPyVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "Pipfile":
			if data, err := os.ReadFile(path); err == nil {
				deps := parsePipfileVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		case "setup.cfg":
			if data, err := os.ReadFile(path); err == nil {
				deps := parseSetupCfgVersions(string(data))
				allDeps = append(allDeps, deps...)
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
			// Extension-based matching (e.g., *.csproj).
			if strings.HasSuffix(base, ".csproj") {
				if data, err := os.ReadFile(path); err == nil {
					deps := parseCsprojVersions(string(data))
					allDeps = append(allDeps, deps...)
				}
			}
		}
		return nil
	})

	// Resolve each dependency against its package registry.
	for _, dep := range allDeps {
		var lb *db.LibyearRow
		var err error

		switch dep.Manager {
		case "npm":
			lb, err = resolveNPMLibyear(ctx, dep)
		case "pypi":
			lb, err = resolvePyPILibyear(ctx, dep)
		case "go":
			lb, err = resolveGoLibyear(ctx, dep)
		case "cargo":
			lb, err = resolveCargoLibyear(ctx, dep)
		case "rubygems":
			lb, err = resolveRubyGemsLibyear(ctx, dep)
		case "maven":
			lb, err = resolveMavenLibyear(ctx, dep)
		case "packagist":
			lb, err = resolvePackagistLibyear(ctx, dep)
		case "hex":
			lb, err = resolveHexLibyear(ctx, dep)
		case "nuget":
			lb, err = resolveNuGetLibyear(ctx, dep)
		case "pub":
			lb, err = resolvePubDevLibyear(ctx, dep)
		case "hackage":
			lb, err = resolveHackageLibyear(ctx, dep)
		case "swiftpm":
			lb, err = resolveSwiftPMLibyear(ctx, dep)
		default:
			continue
		}

		if err != nil || lb == nil {
			continue
		}
		if err := ac.store.InsertRepoLibyear(ctx, repoID, lb); err != nil {
			ac.logger.Warn("failed to insert libyear", "dep", dep.Name, "error", err)
			continue
		}
		result.LibyearDeps++
	}

	return nil
}

type libyearDep struct {
	Name       string
	Version    string
	Requirement string
	Type       string // "runtime", "dev"
	Manager    string // "npm", "pypi"
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
	for name, version := range pkg.Dependencies {
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "runtime", Manager: "npm"})
	}
	for name, version := range pkg.DevDependencies {
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "dev", Manager: "npm"})
	}
	for name, version := range pkg.PeerDependencies {
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "runtime", Manager: "npm"})
	}
	for name, version := range pkg.OptionalDependencies {
		deps = append(deps, libyearDep{Name: name, Version: cleanVersion(version), Requirement: version, Type: "runtime", Manager: "npm"})
	}
	return deps, nil
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
		name := line
		version := ""
		for _, sep := range []string{"==", ">=", "~="} {
			if idx := strings.Index(line, sep); idx > 0 {
				name = strings.TrimSpace(line[:idx])
				version = strings.TrimSpace(line[idx+len(sep):])
				break
			}
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "pypi"})
		}
	}
	return deps
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
	return strings.TrimSpace(v)
}

// resolveNPMLibyear checks the npm registry for the latest version.
func resolveNPMLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// Reject dep names starting with "-" to prevent argument injection.
	// A malicious package.json key like "--registry=http://evil" would
	// become an npm flag without this check.
	if strings.HasPrefix(dep.Name, "-") {
		return nil, fmt.Errorf("rejecting npm dep name starting with dash: %q", dep.Name)
	}
	// "--" separates npm flags from the package spec, preventing any
	// remaining argument injection vectors.
	cmd := exec.CommandContext(ctx, "npm", "view", "--", dep.Name, "version", "time", "license", "--json")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Version string            `json:"version"`
		Time    map[string]string `json:"time"`
		License string            `json:"license"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
		return nil, err
	}

	currentDate := info.Time[dep.Version]
	latestDate := info.Time[info.Version]
	libyear := calcLibyear(currentDate, latestDate)

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "npm",
		CurrentVersion:     dep.Version,
		LatestVersion:      info.Version,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            libyear,
		License:            info.License,
		Purl:               fmt.Sprintf("pkg:npm/%s@%s", dep.Name, dep.Version),
	}, nil
}

// resolvePyPILibyear checks PyPI for the latest version.
func resolvePyPILibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://pypi.org/pypi/%s/json", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Info struct {
			Version     string   `json:"version"`
			License     string   `json:"license"`
			Classifiers []string `json:"classifiers"`
		} `json:"info"`
		Releases map[string][]struct {
			UploadTime string `json:"upload_time"`
		} `json:"releases"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
	license := info.Info.License
	if license == "" || strings.EqualFold(license, "UNKNOWN") {
		license = parsePyPIClassifierLicense(info.Info.Classifiers)
	}

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "pypi",
		License:            license,
		Purl:               fmt.Sprintf("pkg:pypi/%s@%s", dep.Name, dep.Version),
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

// parseCargoVersions extracts deps from Cargo.toml.
func parseCargoVersions(path string) []libyearDep {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	content := string(data)
	var deps []libyearDep
	inDeps := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "[dependencies]" || trimmed == "[dev-dependencies]" || trimmed == "[build-dependencies]" {
			inDeps = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inDeps = false
			continue
		}
		if inDeps && strings.Contains(trimmed, "=") {
			parts := strings.SplitN(trimmed, "=", 2)
			name := strings.TrimSpace(parts[0])
			version := strings.Trim(strings.TrimSpace(parts[1]), "\"' {}")
			// Handle table-style: serde = { version = "1.0", features = ["derive"] }
			if strings.Contains(version, "version") {
				for _, kv := range strings.Split(version, ",") {
					kv = strings.TrimSpace(kv)
					if strings.HasPrefix(kv, "version") {
						version = strings.Trim(strings.TrimPrefix(kv, "version"), " =\"")
						break
					}
				}
			}
			version = cleanVersion(version)
			if name != "" {
				deps = append(deps, libyearDep{Name: name, Version: version, Requirement: trimmed, Type: "runtime", Manager: "cargo"})
			}
		}
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
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "gem ") {
			continue
		}
		// gem 'name', '~> 1.0'
		parts := strings.Split(line, ",")
		name := ""
		version := ""
		if len(parts) >= 1 {
			name = strings.Trim(strings.TrimPrefix(strings.TrimSpace(parts[0]), "gem "), "\"' ")
		}
		if len(parts) >= 2 {
			version = cleanVersion(strings.Trim(strings.TrimSpace(parts[1]), "\"' "))
		}
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "rubygems"})
		}
	}
	return deps
}

// resolveGoLibyear checks the Go proxy for module version dates.
func resolveGoLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// Go proxy API: https://proxy.golang.org/{module}/@v/{version}.info
	latestCmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://proxy.golang.org/%s/@latest", dep.Name))
	var latestOut bytes.Buffer
	latestCmd.Stdout = &latestOut
	if err := latestCmd.Run(); err != nil {
		return nil, err
	}
	var latestInfo struct {
		Version string `json:"Version"`
		Time    string `json:"Time"`
	}
	if err := json.Unmarshal(latestOut.Bytes(), &latestInfo); err != nil {
		return nil, err
	}

	currentDate := ""
	if dep.Version != "" {
		curCmd := exec.CommandContext(ctx, "curl", "-sf",
			fmt.Sprintf("https://proxy.golang.org/%s/@v/v%s.info", dep.Name, dep.Version))
		var curOut bytes.Buffer
		curCmd.Stdout = &curOut
		if err := curCmd.Run(); err == nil {
			var curInfo struct{ Time string `json:"Time"` }
			json.Unmarshal(curOut.Bytes(), &curInfo)
			currentDate = curInfo.Time
		}
	}

	// Go proxy doesn't return license data. Fall back to the GitHub API
	// license endpoint for modules hosted on github.com. This covers the
	// vast majority of Go modules (~15K deps were missing licenses before this).
	license := fetchGoModuleLicense(ctx, dep.Name)

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
		License:            license,
		Purl:               fmt.Sprintf("pkg:golang/%s@%s", dep.Name, dep.Version),
	}, nil
}

// resolveCargoLibyear checks crates.io for Rust crate versions.
func resolveCargoLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://crates.io/api/v1/crates/%s", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
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
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
		Purl:               fmt.Sprintf("pkg:cargo/%s@%s", dep.Name, dep.Version),
		LatestVersion:      info.Crate.NewestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

// resolveRubyGemsLibyear checks rubygems.org for gem versions.
func resolveRubyGemsLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://rubygems.org/api/v1/versions/%s.json", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var versions []struct {
		Number    string   `json:"number"`
		CreatedAt string   `json:"created_at"`
		Licenses  []string `json:"licenses"`
	}
	if err := json.Unmarshal(out.Bytes(), &versions); err != nil {
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
		Purl:               fmt.Sprintf("pkg:gem/%s@%s", dep.Name, dep.Version),
		PackageManager:     "rubygems",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		CurrentReleaseDate: currentDate,
		LatestReleaseDate:  latestDate,
		Libyear:            calcLibyear(currentDate, latestDate),
	}, nil
}

func calcLibyear(currentDate, latestDate string) float64 {
	layouts := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	var current, latest time.Time
	for _, layout := range layouts {
		if t, err := time.Parse(layout, currentDate); err == nil {
			current = t
			break
		}
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, latestDate); err == nil {
			latest = t
			break
		}
	}
	if current.IsZero() || latest.IsZero() {
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
		case strings.Contains(name, "GPLv3"):
			return "GPL-3.0"
		case strings.Contains(name, "GPLv2"):
			return "GPL-2.0"
		case strings.Contains(name, "GNU General Public License v3"):
			return "GPL-3.0"
		case strings.Contains(name, "GNU General Public License v2"):
			return "GPL-2.0"
		case strings.Contains(name, "GNU Lesser General Public License"):
			return "LGPL"
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

// fetchGoModuleLicense tries to fetch the license for a Go module from the
// GitHub API. Most Go modules are hosted on GitHub, so we can use the
// /repos/{owner}/{repo}/license endpoint which returns the SPDX ID directly.
// Returns empty string if the module isn't on GitHub or the API call fails.
func fetchGoModuleLicense(ctx context.Context, modulePath string) string {
	// Extract GitHub owner/repo from module path.
	// e.g., "github.com/go-openapi/swag" → "go-openapi/swag"
	if !strings.HasPrefix(modulePath, "github.com/") {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(modulePath, "github.com/"), "/", 3)
	if len(parts) < 2 {
		return ""
	}
	owner, repo := parts[0], parts[1]

	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://api.github.com/repos/%s/%s/license", owner, repo))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return ""
	}
	var info struct {
		License struct {
			SpdxID string `json:"spdx_id"`
		} `json:"license"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
		return ""
	}
	if info.License.SpdxID == "" || info.License.SpdxID == "NOASSERTION" {
		return ""
	}
	return info.License.SpdxID
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

	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, sccPath, "-f", "json", "--by-file", workDir)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scc failed: %w", err)
	}

	var languages []sccLanguage
	if err := json.Unmarshal(out.Bytes(), &languages); err != nil {
		return fmt.Errorf("parsing scc output: %w", err)
	}

	now := time.Now()
	for _, lang := range languages {
		for _, file := range lang.Files {
			relPath, relErr := filepath.Rel(workDir, file.Location)
			if relErr != nil || relPath == "" {
				relPath = file.Location
			}
			err := ac.store.InsertRepoLabor(ctx, repoID, &db.RepoLaborRow{
				CloneDate:    now,
				AnalysisDate: now,
				Language:     lang.Name,
				FilePath:     relPath,
				FileName:     filepath.Base(file.Location),
				TotalLines:   file.Lines,
				CodeLines:    file.Code,
				CommentLines: file.Comment,
				BlankLines:   file.Blank,
				Complexity:   file.Complexity,
			})
			if err != nil {
				continue
			}
			result.LaborFiles++
		}
	}

	return nil
}

type sccLanguage struct {
	Name  string    `json:"Name"`
	Files []sccFile `json:"Files"`
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
						Name:       name,
						Version:    version,
						Requirement: coord,
						Type:       depType,
						Manager:    "maven",
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
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "nuget"})
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

		groupID := extractXMLValue(block, "groupId")
		artifactID := extractXMLValue(block, "artifactId")
		version := extractXMLValue(block, "version")
		if groupID != "" && artifactID != "" {
			name := groupID + ":" + artifactID
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: name + ":" + version, Type: "runtime", Manager: "maven"})
		}
	}
	return deps
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
		// Extract version from second part, stripping quotes and braces.
		versionPart := strings.Trim(strings.TrimSpace(parts[1]), "\"'}] ")
		version := cleanVersion(versionPart)
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "hex"})
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
		if name != "" {
			deps = append(deps, libyearDep{Name: name, Version: version, Requirement: line, Type: "runtime", Manager: "nuget"})
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
			deps = append(deps, libyearDep{Name: fullName, Version: version, Requirement: line, Type: "runtime", Manager: "maven"})
		}
	}
	return deps
}

// ============================================================
// Additional libyear resolvers (added in v0.5.4)
// ============================================================

// resolveMavenLibyear checks Maven Central for artifact versions.
func resolveMavenLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// Maven Central search API: search by groupId:artifactId.
	parts := strings.SplitN(dep.Name, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid maven coordinate: %s", dep.Name)
	}
	groupID, artifactID := parts[0], parts[1]
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://search.maven.org/solrsearch/select?q=g:%%22%s%%22+AND+a:%%22%s%%22&rows=1&wt=json", groupID, artifactID))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Response struct {
			Docs []struct {
				LatestVersion string `json:"latestVersion"`
				Timestamp     int64  `json:"timestamp"`
			} `json:"docs"`
		} `json:"response"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
		return nil, err
	}
	if len(info.Response.Docs) == 0 {
		return nil, fmt.Errorf("not found on Maven Central: %s", dep.Name)
	}
	latestVersion := info.Response.Docs[0].LatestVersion
	latestDate := time.UnixMilli(info.Response.Docs[0].Timestamp).Format(time.RFC3339)

	return &db.LibyearRow{
		Name:              dep.Name,
		Requirement:       dep.Requirement,
		Type:              dep.Type,
		PackageManager:    "maven",
		CurrentVersion:    dep.Version,
		LatestVersion:     latestVersion,
		LatestReleaseDate: latestDate,
		Libyear:           0, // No current release date from Maven search API.
		Purl:              fmt.Sprintf("pkg:maven/%s/%s@%s", groupID, artifactID, dep.Version),
	}, nil
}

// resolvePackagistLibyear checks Packagist (PHP) for package versions.
func resolvePackagistLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://repo.packagist.org/p2/%s.json", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Packages map[string][]struct {
			Version string `json:"version"`
			Time    string `json:"time"`
			License []string `json:"license"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
		Purl:               fmt.Sprintf("pkg:composer/%s@%s", dep.Name, dep.Version),
	}, nil
}

// resolveHexLibyear checks hex.pm (Elixir/Erlang) for package versions.
func resolveHexLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://hex.pm/api/packages/%s", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Releases []struct {
			Version   string `json:"version"`
			InsertedAt string `json:"inserted_at"`
		} `json:"releases"`
		Meta struct {
			Licenses []string `json:"licenses"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
		Purl:               fmt.Sprintf("pkg:hex/%s@%s", dep.Name, dep.Version),
	}, nil
}

// resolveNuGetLibyear checks nuget.org for .NET package versions.
func resolveNuGetLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// NuGet registration API.
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://api.nuget.org/v3/registration5-semver1/%s/index.json",
			strings.ToLower(dep.Name)))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Items []struct {
			Upper string `json:"upper"`
			Items []struct {
				CatalogEntry struct {
					Version   string `json:"version"`
					Published string `json:"published"`
					LicenseExpression string `json:"licenseExpression"`
				} `json:"catalogEntry"`
			} `json:"items"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
		Purl:               fmt.Sprintf("pkg:nuget/%s@%s", dep.Name, dep.Version),
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
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "dependencies:" {
			inDeps = true
			depType = "runtime"
			continue
		}
		if trimmed == "dev_dependencies:" {
			inDeps = true
			depType = "dev"
			continue
		}
		if inDeps && len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			inDeps = false
		}
		if !inDeps || strings.Contains(trimmed, "sdk:") {
			continue
		}
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "#") {
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
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://pub.dev/api/packages/%s", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		Latest struct {
			Version string `json:"version"`
			Published string `json:"published"`
		} `json:"latest"`
		Versions []struct {
			Version   string `json:"version"`
			Published string `json:"published"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
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
		Purl:               fmt.Sprintf("pkg:pub/%s@%s", dep.Name, dep.Version),
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

// resolveSwiftPMLibyear resolves a SwiftPM dependency by checking GitHub tags.
// SwiftPM packages are just git repos (usually GitHub), not a central registry.
// We use the GitHub API to find the latest release/tag.
func resolveSwiftPMLibyear(ctx context.Context, dep libyearDep) (*db.LibyearRow, error) {
	// The requirement field contains the git URL. Extract owner/repo.
	repoURL := dep.Requirement
	repoURL = strings.TrimSuffix(repoURL, ".git")
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(repoURL, "https://"), "http://"), "/")
	if len(parts) < 3 || !strings.Contains(parts[0], "github.com") {
		// Non-GitHub SwiftPM packages can't be resolved without a registry.
		return nil, fmt.Errorf("SwiftPM resolver only supports GitHub repos: %s", repoURL)
	}
	owner, repo := parts[1], parts[2]

	// Get latest release from GitHub API.
	cmd := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", owner, repo))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var release struct {
		TagName     string `json:"tag_name"`
		PublishedAt string `json:"published_at"`
	}
	if err := json.Unmarshal(out.Bytes(), &release); err != nil {
		return nil, err
	}
	latestVersion := strings.TrimPrefix(release.TagName, "v")

	return &db.LibyearRow{
		Name:               dep.Name,
		Requirement:        dep.Requirement,
		Type:               dep.Type,
		PackageManager:     "swiftpm",
		CurrentVersion:     dep.Version,
		LatestVersion:      latestVersion,
		LatestReleaseDate:  release.PublishedAt,
		Libyear:            0, // No current release date without per-tag API call.
		Purl:               fmt.Sprintf("pkg:swift/%s/%s@%s", owner, repo, dep.Version),
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
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
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
				deps = append(deps, libyearDep{Name: name, Version: version, Requirement: dep, Type: "runtime", Manager: "hackage"})
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
	// Hackage preferred-versions API returns version list.
	cmd := exec.CommandContext(ctx, "curl", "-sf", "-H", "Accept: application/json",
		fmt.Sprintf("https://hackage.haskell.org/package/%s/preferred", dep.Name))
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var info struct {
		NormalVersion []string `json:"normal-version"`
	}
	if err := json.Unmarshal(out.Bytes(), &info); err != nil {
		return nil, err
	}
	if len(info.NormalVersion) == 0 {
		return nil, fmt.Errorf("no versions found on Hackage: %s", dep.Name)
	}
	latestVersion := info.NormalVersion[0] // First is the latest.

	// Hackage doesn't return dates in this endpoint. Get upload time from package info.
	latestDate := ""
	cmd2 := exec.CommandContext(ctx, "curl", "-sf",
		fmt.Sprintf("https://hackage.haskell.org/package/%s-%s/upload-time", dep.Name, latestVersion))
	var out2 bytes.Buffer
	cmd2.Stdout = &out2
	if err := cmd2.Run(); err == nil {
		latestDate = strings.TrimSpace(out2.String())
	}

	currentDate := ""
	if dep.Version != "" {
		cmd3 := exec.CommandContext(ctx, "curl", "-sf",
			fmt.Sprintf("https://hackage.haskell.org/package/%s-%s/upload-time", dep.Name, dep.Version))
		var out3 bytes.Buffer
		cmd3.Stdout = &out3
		if err := cmd3.Run(); err == nil {
			currentDate = strings.TrimSpace(out3.String())
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
		Purl:               fmt.Sprintf("pkg:hackage/%s@%s", dep.Name, dep.Version),
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
		return nil, err
	}
	var files []string
	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		files = append(files, scanner.Text())
	}
	return files, scanner.Err()
}

// scanScanCode runs ScanCode Toolkit for per-file license and copyright detection.
// Delegates to RunScanCode which handles the 30-day skip check, CLI invocation,
// JSON parsing, and database storage.
func (ac *AnalysisCollector) scanScanCode(ctx context.Context, repoID int64, workDir string, result *AnalysisResult) error {
	scResult, err := RunScanCode(ctx, ac.store, repoID, workDir, ac.logger)
	if err != nil {
		return err
	}
	if scResult != nil {
		result.ScancodeFiles = scResult.FilesWithFindings
	}
	return nil
}
