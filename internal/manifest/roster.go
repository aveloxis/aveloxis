// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The roster is the one list of the manifest kinds Aveloxis reads, and the
// parser contract every kind meets (summary/70, 4.1.5). Before it, two walks
// in internal/collector each carried their own dispatch: the dependency-name
// walk a filename table plus a switch, the libyear walk a second switch of
// twenty-odd arms. A kind added to one and not the other was a silent gap.
// Both walks now ask the roster, so there is one place to add a format and
// one place to see what is read.
//
// The roster is declarative data and parsers only. It does no network I/O.
// Which directories a walk skips, and what is done with what a file declares,
// stay with the walk.

// Options are the configuration knobs that widen what is read.
type Options struct {
	// DevBuildDeps adds development and build declarations: optional
	// dependency groups, build requirements, Pipfile dev packages, setup.cfg
	// extras, and requirements-variant files (collection.dev_build_deps).
	DevBuildDeps bool
	// GitHubActionsDeps adds the actions a workflow file uses
	// (collection.github_actions_deps).
	GitHubActionsDeps bool
}

// Kind is one manifest kind: a file the roster recognises, by its exact
// canonical name or by a predicate on its path.
type Kind struct {
	// Name is the file's canonical base name ("package.json"), or for a kind
	// matched by a predicate a description of it ("*.csproj").
	Name string
	// Language is the label the dependency-name inventory
	// (repo_dependencies) files this kind's names under. Empty: the kind is
	// not part of that inventory.
	Language string
	// Ecosystem is the package manager its declarations belong to, as the
	// libyear inventory spells it. Empty: the kind declares no versioned
	// dependency (a lockfile or a build file the name inventory only counts).
	Ecosystem string

	// match recognises a kind that has no fixed name. nil: Name is exact.
	match func(base, path string) bool
	// gate, when set, must hold for the kind's declarations to be read.
	gate func(Options) bool
	// names reads the dependency NAMES the file declares. nil: none are read.
	names func(data []byte) ([]string, error)
	// declared reads the versioned declarations. nil: none are read.
	declared func(w *Walk, path, base string, o Options) ([]model.LibyearDep, error)
}

// ReadError is a manifest whose declarations could not be read, or could not
// be parsed. What was read before a parse failure is still returned with it.
type ReadError struct {
	Kind string // the kind's Name
	Path string
	// Reason is model.ManifestFailedRead when the file could not be read at
	// all, model.ManifestFailedParse when it was read and its content could
	// not be parsed.
	Reason string
	Err    error
}

func (e *ReadError) Error() string {
	return fmt.Sprintf("reading %s %s: %v", e.Kind, e.Path, e.Err)
}

func (e *ReadError) Unwrap() error { return e.Err }

// Walk is the state one pass over one clone shares between files: every
// Cargo.toml of a workspace inherits from the same root, which is read once
// and never above the clone.
//
// A Walk is for ONE goroutine: its trees are caches with no lock. Two walks
// share nothing and may run at once.
//
// Nothing in this package bounds the time or memory a walk takes for a
// hostile clone. The collector runs the pass in a child process whose CPU
// time and heap the operating system's accounting limits (0.29.213;
// internal/collector/manifest_child.go); a caller that reads untrusted
// clones in its own process has no such bound.
type Walk struct {
	cargo  *cargoWorkspaceIndex
	pom    *pomTree
	gradle *gradleTree
}

// NewWalk starts a pass over the clone rooted at root. A nil logger is
// allowed.
func NewWalk(root string, logger *slog.Logger) *Walk {
	w := &Walk{cargo: newCargoWorkspaceIndex(root, logger), pom: newPomTree(root, logger), gradle: newGradleTree(root, logger)}
	w.gradle.pom = w.pom
	return w
}

// SetPomSource installs where a Maven parent or BOM that is not in the clone
// is obtained from (0.29.181). Without one, nothing outside the clone is
// asked.
func (w *Walk) SetPomSource(src PomSource) { w.pom.remote = src }

// SetRemotePomLimit sets how many parent and BOM coordinates this walk may
// ask its PomSource for (0.29.213). Past it a pom that is wanted is not
// asked for, and what depends on it is reported as not obtained. The walk
// keeps one answer per coordinate asked, so this is also the bound on what
// it keeps of them. 0, the default, is no limit: for a caller whose source
// answers from memory.
func (w *Walk) SetRemotePomLimit(n int) { w.pom.remoteLimit = n }

// RemotePomsNotAsked is how many questions were not put to the PomSource
// because the walk's limit had been reached.
func (w *Walk) RemotePomsNotAsked() int { return w.pom.notAsked }

// DeclaredRepositories lists the repositories the Maven poms read so far
// declare (0.29.184), each classified.
func (w *Walk) DeclaredRepositories() []DeclaredRepository {
	out := append(w.pom.declaredRepositories(), w.gradle.declaredRepositories()...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ManifestPath < out[j].ManifestPath })
	return out
}

// fromContent adapts a reader of a manifest's text. A file that cannot be
// read declares nothing here, as before the roster existed (the walk that
// replaces the inventory whole, 4.7, is where an unreadable manifest becomes
// a recorded failure). extra, when not nil, is read as well under
// DevBuildDeps.
func fromContent(parse, extra func(string) []model.LibyearDep) func(*Walk, string, string, Options) ([]model.LibyearDep, error) {
	return func(_ *Walk, path, _ string, o Options) ([]model.LibyearDep, error) {
		data, err := ReadManifest(path)
		if err != nil {
			return nil, nil
		}
		deps := parse(string(data))
		if extra != nil && o.DevBuildDeps {
			deps = append(deps, extra(string(data))...)
		}
		return deps, nil
	}
}

// fromPath adapts a reader that opens the file itself and cannot fail.
func fromPath(parse func(string) []model.LibyearDep) func(*Walk, string, string, Options) ([]model.LibyearDep, error) {
	return func(_ *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
		return parse(path), nil
	}
}

// fromPathErr adapts a reader that opens the file itself and reports failure.
func fromPathErr(parse func(string) ([]model.LibyearDep, error)) func(*Walk, string, string, Options) ([]model.LibyearDep, error) {
	return func(_ *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
		return parse(path)
	}
}

// namesOf adapts a name reader over the manifest's text that cannot fail.
func namesOf(parse func(string) []string) func([]byte) ([]string, error) {
	return func(data []byte) ([]string, error) { return parse(string(data)), nil }
}

// namesOfErr adapts a name reader over the manifest's text that reports
// failure.
func namesOfErr(parse func(string) ([]string, error)) func([]byte) ([]string, error) {
	return func(data []byte) ([]string, error) { return parse(string(data)) }
}

// roster is the list. Order matters only among the predicate kinds, which
// are tried, all of them, in this order, for a file no exact name claims.
var roster = []Kind{
	{Name: "package.json", Language: "JavaScript", Ecosystem: "npm",
		names: parsePackageJSON, declared: fromPathErr(parsePackageJSONVersions)},
	{Name: "yarn.lock", Language: "JavaScript"},

	{Name: "requirements.txt", Language: "Python", Ecosystem: "pypi",
		names: namesOf(ParseRequirementsTxt), declared: fromPath(ParseRequirementsTxtVersions)},
	{Name: "setup.py", Language: "Python", Ecosystem: "pypi",
		names: namesOfErr(parseSetupPyDeps), declared: fromContent(parseSetupPyVersions, parseSetupPyDevBuildVersions)},
	{Name: "setup.cfg", Language: "Python", Ecosystem: "pypi",
		names: namesOfErr(parseSetupCfgDeps), declared: fromContent(parseSetupCfgVersions, parseSetupCfgExtrasVersions)},
	{Name: "pyproject.toml", Language: "Python", Ecosystem: "pypi",
		names: namesOfErr(parsePyprojectDeps), declared: fromContent(ParsePyprojectVersionsFromContent, parsePyprojectDevBuildVersions)},
	{Name: "Pipfile", Language: "Python", Ecosystem: "pypi",
		names: namesOfErr(parsePipfileDeps), declared: fromContent(parsePipfileVersions, parsePipfileDevPackages)},
	{Name: "poetry.lock", Language: "Python"},

	{Name: "go.mod", Language: "Go", Ecosystem: "go",
		names: namesOf(parseGoMod), declared: fromPath(ParseGoModVersions)},
	{Name: "go.sum", Language: "Go"},

	{Name: "Cargo.toml", Language: "Rust", Ecosystem: "cargo",
		names: namesOf(func(content string) []string { return parseTOMLDeps(content, "[dependencies]") }),
		declared: func(w *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
			return parseCargoVersionsIn(w.cargo, path), nil
		}},
	{Name: "Cargo.lock", Language: "Rust"},

	{Name: "Gemfile", Language: "Ruby", Ecosystem: "rubygems",
		names: namesOf(parseGemfile), declared: fromPath(ParseGemfileVersions)},
	{Name: "Gemfile.lock", Language: "Ruby"},

	{Name: "pom.xml", Language: "Java", Ecosystem: "maven",
		names: namesOf(parsePomXML),
		// v0.29.178: the pom's own declarations, then what the tree knows
		// (a parent's or a BOM's dependencyManagement, inherited
		// properties).
		declared: func(w *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
			// The tree does not read a pom over the bound; neither does
			// this, which would otherwise read it whole with nothing from
			// its tree. It is a manifest that could not be read.
			if buildFileTooLarge(path) {
				return nil, errBuildFileTooLarge
			}
			content, err := ReadManifest(path)
			if err != nil {
				return nil, nil // Read has already recorded an unreadable file
			}
			return w.pom.settle(path, ParsePomXMLVersions(string(content))), nil
		}},
	{Name: "build.gradle", Language: "Java", Ecosystem: "maven",
		names: namesOf(parseBuildGradle), declared: gradleDeclared},
	{Name: "build.gradle.kts", Language: "Java", Ecosystem: "maven",
		names: namesOf(parseBuildGradle), declared: gradleDeclared},

	{Name: "composer.json", Language: "PHP", Ecosystem: "packagist",
		names: parseComposerJSON,
		declared: func(w *Walk, path, _ string, _ Options) ([]model.LibyearDep, error) {
			return parseComposerJSONVersionsIn(w.pom.root, path)
		}},

	{Name: "mix.exs", Language: "Elixir", Ecosystem: "hex",
		names: namesOf(parseMixExsDeps), declared: fromContent(parseMixExsVersions, nil)},

	{Name: "Package.swift", Language: "Swift", Ecosystem: "swiftpm",
		names: namesOf(parsePackageSwiftDeps), declared: fromContent(parsePackageSwiftVersions, nil)},

	{Name: "pubspec.yaml", Language: "Dart", Ecosystem: "pub",
		names: namesOf(parsePubspecDeps), declared: fromContent(parsePubspecVersions, nil)},

	{Name: "Makefile", Language: "C/C++"},
	{Name: "CMakeLists.txt", Language: "C/C++"},

	{Name: "build.sbt", Language: "Scala", Ecosystem: "maven",
		names: namesOf(parseBuildSbt), declared: fromContent(parseBuildSbtVersions, nil)},

	{Name: "packages.config", Language: ".NET", Ecosystem: "nuget",
		names: namesOf(parseNuGetPackagesConfig), declared: fromContent(parseNuGetPackagesConfigVersions, nil)},
	{Name: "Directory.Packages.props", Language: ".NET", Ecosystem: "nuget",
		names: namesOfErr(parseDirectoryPackagesProps), declared: fromContent(parseDirectoryPackagesPropsVersions, nil)},

	{Name: "package.yaml", Language: "Haskell", Ecosystem: "hackage",
		names: namesOf(parsePackageYaml), declared: fromContent(parseHaskellPackageYamlVersions, nil)},
	{Name: "stack.yaml", Language: "Haskell"},

	// An R package's DESCRIPTION (0.29.204). No Language: R is not part of
	// the dependency-name inventory (repo_dependencies), whose counts this
	// program does not change (summary/70, 4.1), so there is no name reader.
	{Name: "DESCRIPTION", Ecosystem: "cran", declared: fromContent(parseRDescriptionVersions, nil)},

	// Kinds with no fixed name.

	// A requirements-variant file (requirements-dev.txt, test_requirements.txt,
	// or another .txt directly inside a requirements/ directory; never
	// requirements.txt itself), its scope read from its name.
	{Name: "requirements variant", Ecosystem: "pypi",
		match: func(base, path string) bool { _, ok := requirementsFileScope(base, path); return ok },
		gate:  func(o Options) bool { return o.DevBuildDeps },
		declared: func(_ *Walk, path, base string, _ Options) ([]model.LibyearDep, error) {
			scope, _ := requirementsFileScope(base, path)
			return parseRequirementsTxtVersionsScoped(path, scope), nil
		}},
	// A GitHub Actions workflow: its `uses:` references.
	{Name: "GitHub Actions workflow", Ecosystem: "githubactions",
		match:    func(_, path string) bool { return isWorkflowPath(path) },
		gate:     func(o Options) bool { return o.GitHubActionsDeps },
		declared: fromContent(parseWorkflowUses, nil)},
	// A .NET project file, by extension.
	{Name: "*.csproj", Language: ".NET", Ecosystem: "nuget",
		match: func(base, _ string) bool { return strings.HasSuffix(base, ".csproj") },
		names: namesOfErr(parseCsprojDeps), declared: fromContent(parseCsprojVersions, nil)},
}

// byName indexes the kinds with a fixed name; byPredicate keeps the others in
// roster order.
var byName, byPredicate = func() (map[string]*Kind, []*Kind) {
	m := map[string]*Kind{}
	var p []*Kind
	for i := range roster {
		k := &roster[i]
		if k.match != nil {
			p = append(p, k)
			continue
		}
		if m[k.Name] != nil {
			panic("manifest: roster names " + k.Name + " twice")
		}
		m[k.Name] = k
	}
	return m, p
}()

// Roster returns a copy of the list, for documentation and tests that count
// what is supported.
func Roster() []Kind {
	return append([]Kind(nil), roster...)
}

// kindsFor returns the kinds that claim a file: the one with its exact
// canonical name, or else every predicate kind that matches.
func kindsFor(path string) (base string, kinds []*Kind) {
	base = canonicalManifestName(filepath.Base(path))
	if k := byName[base]; k != nil {
		return base, []*Kind{k}
	}
	for _, k := range byPredicate {
		if k.match(base, path) {
			kinds = append(kinds, k)
		}
	}
	return base, kinds
}

// Declarations is what one file declares, and whether it could be read.
type Declarations struct {
	// Claimed is true when at least one manifest kind whose gate the options
	// satisfy reads declarations from this file: it is a manifest. A file no
	// kind claims is not one and declares nothing.
	Claimed bool
	// Kind is the name of the first kind that claimed it.
	Kind string
	// Deps are the declarations read. On a parse failure they are what was
	// read before it.
	Deps []model.LibyearDep
	// Failure is why the manifest contributed nothing or less than it holds;
	// nil when it was read and parsed.
	Failure *ReadError
}

// Read returns what one file declares, read by every kind that claims it and
// whose gate the options satisfy.
//
// A manifest that cannot be READ is a failure for every kind (summary/70,
// 4.7): before 0.29.159 most kinds returned nothing for such a file,
// silently, and a manifest that declares nothing is indistinguishable from a
// project that dropped all its dependencies. Only two kinds can fail to
// PARSE (package.json and composer.json, both JSON documents); every other
// reader is line-based and takes what it recognises.
func (w *Walk) Read(path string, o Options) Declarations {
	base, kinds := kindsFor(path)
	var out Declarations
	for _, k := range kinds {
		if k.declared == nil || (k.gate != nil && !k.gate(o)) {
			continue
		}
		if !out.Claimed {
			out.Claimed, out.Kind = true, k.Name
			// One read decides readability for every kind, the readers that
			// open the file themselves and say nothing included.
			if _, err := ReadManifest(path); err != nil {
				out.Failure = &ReadError{Kind: k.Name, Path: path, Reason: model.ManifestFailedRead, Err: err}
				return out
			}
		}
		got, err := k.declared(w, path, base, o)
		out.Deps = append(out.Deps, notPackages(got)...)
		if err != nil && out.Failure == nil {
			out.Failure = &ReadError{Kind: k.Name, Path: path, Reason: model.ManifestFailedParse, Err: err}
		}
	}
	return out
}

// DeclaredKind says whether Read would claim the file at path under these
// options, and as which kind, from the PATH alone (0.29.214): nothing is
// opened, so it answers for a file that is too large to read, or that is not
// there. A caller that will not read a file can still record it as a
// manifest that was not read.
func DeclaredKind(path string, o Options) (kind string, claimed bool) {
	_, kinds := kindsFor(path)
	for _, k := range kinds {
		if k.declared == nil || (k.gate != nil && !k.gate(o)) {
			continue
		}
		return k.Name, true
	}
	return "", false
}

// Declared is Read for a caller that wants only the declarations and the
// failure, as an error: a *ReadError when not nil.
func (w *Walk) Declared(path string, o Options) ([]model.LibyearDep, error) {
	d := w.Read(path, o)
	if d.Failure != nil {
		return d.Deps, d.Failure
	}
	return d.Deps, nil
}

// Names returns the dependency names one file declares and the language label
// they are filed under. matched is false for a file the name inventory does
// not count. A matched file with no name reader (a lockfile, a Makefile)
// returns no names: it is recognised, and nothing is read from it.
func Names(path string) (language string, names []string, matched bool, err error) {
	_, kinds := kindsFor(path)
	for _, k := range kinds {
		if k.Language == "" {
			continue
		}
		if k.names == nil {
			return k.Language, nil, true, nil
		}
		data, err := ReadManifest(path)
		if err != nil {
			return k.Language, nil, true, err
		}
		names, err := k.names(data)
		return k.Language, names, true, err
	}
	return "", nil, false, nil
}

// notPackages settles two things a reader can hand back that are not
// packages (v0.29.174; the production log showed both sent to a registry).
// A template expression is dropped: it declares nothing. A project file's
// path in a NuGet reference is kept as a dependency no registry holds.
// It is here, after every reader, so that no reader needs its own copy.
func notPackages(deps []model.LibyearDep) []model.LibyearDep {
	out := deps[:0:0]
	for _, d := range deps {
		if isTemplateExpression(d.Name) {
			continue
		}
		if d.Manager == "nuget" && isProjectPath(d.Name) {
			d.NonRegistry = true
		}
		out = append(out, d)
	}
	return out
}

// isTemplateExpression reports whether a name holds a template placeholder
// or statement: the paired delimiters of Jinja-style templates (cookiecutter,
// copier), which is what the log showed ("{% if cookiecutter.use_ruff").
// Single braces and a lone '%' are not delimiters.
func isTemplateExpression(name string) bool {
	for _, delim := range []string{"{{", "}}", "{%", "%}"} {
		if strings.Contains(name, delim) {
			return true
		}
	}
	return false
}

// isProjectPath reports whether a NuGet reference names a project file, not
// a package: it has a path separator, or ends in a project extension. A
// package id has neither (dots and dashes only).
func isProjectPath(name string) bool {
	if strings.ContainsAny(name, `/\`) {
		return true
	}
	lower := strings.ToLower(name)
	for _, ext := range []string{".csproj", ".fsproj", ".vbproj"} {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}
