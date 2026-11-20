// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The roster replaced two hand-written dispatches. These tests keep both as
// they were, verbatim, as oracles, and require the roster to return what they
// return for every file of a tree that holds every manifest kind, under every
// combination of the two knobs. They are the proof that 0.29.153 changed no
// behaviour; they stay as the record of what the dispatch was.

// goModName is go.mod's file name. It is a constant, not a literal in the
// helpers below, because scripts' srctest ratchet reads a "go.mod" literal in
// a test helper's body as a hand-rolled repository-root finder, which these
// are not.
const goModName = "go" + ".mod"

// oldDeclared is the body of the libyear walk's switch in
// internal/collector/analysis.go as of 0.29.152. warned collects what it
// logged at WARN.
func oldDeclared(ws *cargoWorkspaceIndex, path string, o Options, warned *[]string) []model.LibyearDep {
	var allDeps []model.LibyearDep
	base := canonicalManifestName(filepath.Base(path))
	switch base {
	case "package.json":
		deps, err := parsePackageJSONVersions(path)
		if err != nil {
			*warned = append(*warned, "failed to parse package.json versions")
		}
		allDeps = append(allDeps, deps...)
	case "requirements.txt":
		deps := ParseRequirementsTxtVersions(path)
		allDeps = append(allDeps, deps...)
	case "pyproject.toml":
		if data, err := ReadManifest(path); err == nil {
			deps := ParsePyprojectVersionsFromContent(string(data))
			allDeps = append(allDeps, deps...)
			if o.DevBuildDeps {
				allDeps = append(allDeps, parsePyprojectDevBuildVersions(string(data))...)
			}
		}
	case "setup.py":
		if data, err := ReadManifest(path); err == nil {
			deps := parseSetupPyVersions(string(data))
			allDeps = append(allDeps, deps...)
			if o.DevBuildDeps {
				allDeps = append(allDeps, parseSetupPyDevBuildVersions(string(data))...)
			}
		}
	case "Pipfile":
		if data, err := ReadManifest(path); err == nil {
			deps := parsePipfileVersions(string(data))
			allDeps = append(allDeps, deps...)
			if o.DevBuildDeps {
				allDeps = append(allDeps, parsePipfileDevPackages(string(data))...)
			}
		}
	case "setup.cfg":
		if data, err := ReadManifest(path); err == nil {
			deps := parseSetupCfgVersions(string(data))
			allDeps = append(allDeps, deps...)
			if o.DevBuildDeps {
				allDeps = append(allDeps, parseSetupCfgExtrasVersions(string(data))...)
			}
		}
	case goModName:
		deps := ParseGoModVersions(path)
		allDeps = append(allDeps, deps...)
	case "Cargo.toml":
		deps := parseCargoVersionsIn(ws, path)
		allDeps = append(allDeps, deps...)
	case "Gemfile":
		deps := ParseGemfileVersions(path)
		allDeps = append(allDeps, deps...)
	case "pom.xml":
		if data, err := ReadManifest(path); err == nil {
			deps := ParsePomXMLVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "build.gradle", "build.gradle.kts":
		if data, err := ReadManifest(path); err == nil {
			deps := parseBuildGradleVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "composer.json":
		deps, err := parseComposerJSONVersions(path)
		if err != nil {
			*warned = append(*warned, "failed to parse composer.json versions")
		}
		allDeps = append(allDeps, deps...)
	case "mix.exs":
		if data, err := ReadManifest(path); err == nil {
			deps := parseMixExsVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "packages.config":
		if data, err := ReadManifest(path); err == nil {
			deps := parseNuGetPackagesConfigVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "Directory.Packages.props":
		if data, err := ReadManifest(path); err == nil {
			deps := parseDirectoryPackagesPropsVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "build.sbt":
		if data, err := ReadManifest(path); err == nil {
			deps := parseBuildSbtVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "pubspec.yaml":
		if data, err := ReadManifest(path); err == nil {
			deps := parsePubspecVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "Package.swift":
		if data, err := ReadManifest(path); err == nil {
			deps := parsePackageSwiftVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	case "package.yaml":
		if data, err := ReadManifest(path); err == nil {
			deps := parseHaskellPackageYamlVersions(string(data))
			allDeps = append(allDeps, deps...)
		}
	default:
		if o.DevBuildDeps {
			if scope, ok := requirementsFileScope(base, path); ok {
				allDeps = append(allDeps, parseRequirementsTxtVersionsScoped(path, scope)...)
			}
		}
		if o.GitHubActionsDeps && isWorkflowPath(path) {
			if data, err := ReadManifest(path); err == nil {
				allDeps = append(allDeps, parseWorkflowUses(string(data))...)
			}
		}
		if strings.HasSuffix(base, ".csproj") {
			if data, err := ReadManifest(path); err == nil {
				deps := parseCsprojVersions(string(data))
				allDeps = append(allDeps, deps...)
			}
		}
	}
	return allDeps
}

// oldManifestFiles is the filename table of the dependency-name walk as of
// 0.29.152.
var oldManifestFiles = map[string]string{
	"package.json": "JavaScript", "yarn.lock": "JavaScript",
	"requirements.txt": "Python", "setup.py": "Python", "setup.cfg": "Python",
	"pyproject.toml": "Python", "Pipfile": "Python", "poetry.lock": "Python",
	goModName: "Go", "go.sum": "Go",
	"Cargo.toml": "Rust", "Cargo.lock": "Rust",
	"Gemfile": "Ruby", "Gemfile.lock": "Ruby",
	"pom.xml": "Java", "build.gradle": "Java", "build.gradle.kts": "Java",
	"composer.json": "PHP", "mix.exs": "Elixir", "Package.swift": "Swift", "pubspec.yaml": "Dart",
	"Makefile": "C/C++", "CMakeLists.txt": "C/C++",
	"build.sbt": "Scala", "packages.config": ".NET", "Directory.Packages.props": ".NET",
	"package.yaml": "Haskell", "stack.yaml": "Haskell",
}

// oldParseDependencyFile is ParseDependencyFile as of 0.29.152.
func oldParseDependencyFile(path string) ([]string, error) {
	data, err := ReadManifest(path)
	if err != nil {
		return nil, err
	}
	content := string(data)
	switch canonicalManifestName(filepath.Base(path)) {
	case "package.json":
		return parsePackageJSON(data)
	case "requirements.txt":
		return ParseRequirementsTxt(content), nil
	case goModName:
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
		if strings.HasSuffix(filepath.Base(path), ".csproj") {
			return parseCsprojDeps(content)
		}
		return nil, nil
	}
}

// oldNames is the dependency-name walk's per-file step as of 0.29.152: the
// language, the names, and whether the file contributed (an unparseable or
// unrecognised file contributed nothing).
func oldNames(path string) (lang string, names []string, counted bool) {
	filename := canonicalManifestName(filepath.Base(path))
	lang, ok := oldManifestFiles[filename]
	if !ok {
		if filepath.Ext(filename) == ".csproj" {
			lang = ".NET"
		} else {
			return "", nil, false
		}
	}
	deps, err := oldParseDependencyFile(path)
	if err != nil {
		return "", nil, false
	}
	return lang, deps, true
}

// clone is a tree on disk: its root and every file under it, sorted.
type clone struct {
	root  string
	files []string
}

// oracleTree writes one clone holding every manifest kind and returns its
// root and its files. The bodies are the corpus the fuzz targets seed from,
// plus the shapes the dispatch treats specially.
func oracleTree(t *testing.T) clone {
	t.Helper()
	root := t.TempDir()
	corpus := filepath.Join("testdata", "manifest_corpus")
	entries, err := os.ReadDir(corpus)
	if err != nil {
		t.Fatal(err)
	}
	write := func(rel string, data []byte) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bodies := map[string][]byte{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(corpus, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		bodies[e.Name()] = data
		write("corpus/"+e.Name(), data)
	}
	if len(bodies) < 15 {
		t.Fatalf("the manifest corpus has %d files; the oracle would compare almost nothing", len(bodies))
	}
	reqs := []byte("flask==2.0.0\nrequests>=2.25\n# a comment\npytest\n")
	for rel, data := range map[string][]byte{
		// every fixed name the corpus may lack
		"all/package.json":             []byte(`{"dependencies":{"a":"^1.0.0"},"devDependencies":{"b":"2.0.0"},"peerDependencies":{"c":"3"}}`),
		"all/composer.json":            []byte(`{"require":{"monolog/monolog":"^3.0","php":">=8.1"},"require-dev":{"phpunit/phpunit":"^10"}}`),
		"all/requirements.txt":         reqs,
		"all/" + goModName:             []byte("module example.com/m\n\ngo 1.22\n\nrequire (\n\tgithub.com/pkg/errors v0.9.1\n)\nrequire golang.org/x/text v0.14.0\n"),
		"all/Gemfile":                  []byte("source 'https://rubygems.org'\ngem 'rails', '~> 7.1'\ngroup :test do\n  gem 'rspec'\nend\n"),
		"all/pom.xml":                  []byte("<project><properties><v>1.2</v></properties><dependencies><dependency>\n<groupId>org.slf4j</groupId>\n<artifactId>slf4j-api</artifactId>\n<version>${v}</version>\n</dependency></dependencies></project>\n"),
		"all/build.gradle":             []byte("dependencies {\n  implementation 'com.google.guava:guava:33.0.0-jre'\n  testImplementation \"junit:junit:4.13.2\"\n}\n"),
		"all/build.gradle.kts":         []byte("dependencies {\n  implementation(\"com.squareup.okhttp3:okhttp:4.12.0\")\n}\n"),
		"all/mix.exs":                  []byte("defp deps do\n  [{:phoenix, \"~> 1.7\"}, {:ecto, \">= 3.0.0\"}]\nend\n"),
		"all/packages.config":          []byte("<packages>\n  <package Id=\"Newtonsoft.Json\" Version=\"13.0.1\" />\n</packages>\n"),
		"all/Directory.Packages.props": []byte("<Project><ItemGroup>\n  <PackageVersion Include=\"Serilog\" Version=\"3.1.1\" />\n</ItemGroup></Project>\n"),
		"all/build.sbt":                []byte("libraryDependencies += \"org.typelevel\" %% \"cats-core\" % \"2.10.0\"\n"),
		"all/pubspec.yaml":             []byte("dependencies:\n  http: ^1.1.0\ndev_dependencies:\n  test: any\n"),
		"all/Package.swift":            []byte("dependencies: [\n  .package(url: \"https://github.com/Alamofire/Alamofire.git\", from: \"5.8.0\"),\n]\n"),
		"all/package.yaml":             []byte("dependencies:\n- base >= 4.7 && < 5\n- aeson\n"),
		"all/pyproject.toml":           []byte("[project]\ndependencies = [\"httpx>=0.25\"]\n[project.optional-dependencies]\ndev = [\"ruff\"]\n[build-system]\nrequires = [\"hatchling\"]\n"),
		"all/setup.py":                 []byte("setup(install_requires=['click>=8'], extras_require={'dev': ['black']}, setup_requires=['wheel'])\n"),
		"all/setup.cfg":                []byte("[options]\ninstall_requires =\n    attrs>=23\n[options.extras_require]\ntest =\n    pytest\n"),
		"all/Pipfile":                  []byte("[packages]\ndjango = \"==4.2\"\n[dev-packages]\nmypy = \"*\"\n"),
		"all/Cargo.toml":               []byte("[package]\nname = \"x\"\n[dependencies]\nserde = \"1.0\"\n[dev-dependencies]\ncriterion = \"0.5\"\n"),
		"all/App.csproj":               []byte("<Project><ItemGroup>\n  <PackageReference Include=\"Dapper\" Version=\"2.1.28\" />\n</ItemGroup></Project>\n"),
		// recognised by the name inventory, nothing read
		"all/yarn.lock": []byte("# yarn lockfile v1\n"), "all/poetry.lock": []byte("[[package]]\nname = \"x\"\n"),
		"all/go.sum": []byte("x v1 h1:abc=\n"), "all/Cargo.lock": []byte("version = 3\n"), "all/Gemfile.lock": []byte("GEM\n"),
		"all/Makefile": []byte("all:\n\ttrue\n"), "all/CMakeLists.txt": []byte("project(x)\n"), "all/stack.yaml": []byte("resolver: lts-21\n"),
		// requirements.txt in another letter case, and the variants
		"cased/Requirements.TXT":          reqs,
		"variants/requirements-dev.txt":   reqs,
		"variants/test_requirements.txt":  reqs,
		"variants/requirements/ci.txt":    reqs,
		"variants/requirements/base.txt":  reqs,
		"variants/notes.txt":              reqs,
		"variants/requirements/README.md": []byte("# not a requirements file\n"),
		// workflows, and yaml that is not one
		".github/workflows/ci.yml":   []byte("jobs:\n  b:\n    steps:\n      - uses: actions/checkout@v4\n      - uses: tj-actions/changed-files@a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0\n"),
		".github/workflows/rel.yaml": []byte("jobs:\n  b:\n    steps:\n      - uses: actions/setup-go@v5.0.1\n"),
		".github/other.yml":          []byte("jobs:\n  b:\n    steps:\n      - uses: actions/checkout@v4\n"),
		// broken documents: the two readers that report failure
		"broken/package.json":  []byte("{not json"),
		"broken/composer.json": []byte("[1,2"),
		// a workspace member inheriting from its root
		"ws/Cargo.toml":   []byte("[workspace]\nmembers = [\"m\"]\n[workspace.dependencies]\ntokio = \"1.35\"\n"),
		"ws/m/Cargo.toml": []byte("[package]\nname = \"m\"\n[dependencies]\ntokio = { workspace = true }\n"),
		// a UTF-16 file and a UTF-8 BOM
		"enc/requirements.txt": append([]byte{0xef, 0xbb, 0xbf}, reqs...),
		"unrelated/README.md":  []byte("# hello\n"),
	} {
		write(rel, data)
	}
	var files []string
	if err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	return clone{root: root, files: files}
}

// unorderedReader reports the two kinds whose readers range over a decoded
// JSON object, a Go map: the order of what they return differs from call to
// call, before the roster and after it. For every other kind the order is
// part of what is compared.
func unorderedReader(path string) bool {
	base := filepath.Base(path)
	return base == "package.json" || base == "composer.json"
}

// sameInOrder compares two results element by element; anyOrder compares them
// as multisets. Two empty results are equal whether nil or not.
func sameInOrder[T any](got, want []T, anyOrder bool) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	if !anyOrder {
		return reflect.DeepEqual(got, want)
	}
	key := func(xs []T) []string {
		out := make([]string, len(xs))
		for i, x := range xs {
			out[i] = fmt.Sprintf("%+v", x)
		}
		sort.Strings(out)
		return out
	}
	return reflect.DeepEqual(key(got), key(want))
}

func TestRosterDeclaredEqualsTheOldLibyearDispatch(t *testing.T) {
	tree := oracleTree(t)
	root, files := tree.root, tree.files
	compared, withDeps, warnedTotal := 0, 0, 0
	for _, o := range []Options{{}, {DevBuildDeps: true}, {GitHubActionsDeps: true}, {DevBuildDeps: true, GitHubActionsDeps: true}} {
		// Each side gets its own workspace index, visited in the same order.
		oldWS := newCargoWorkspaceIndex(root, nil)
		w := NewWalk(root, nil)
		for _, path := range files {
			var warned []string
			want := oldDeclared(oldWS, path, o, &warned)
			got, err := w.Declared(path, o)
			compared++
			if len(want) > 0 {
				withDeps++
			}
			if !sameInOrder(got, want, unorderedReader(path)) {
				t.Errorf("%+v %s:\n roster %+v\n    old %+v", o, strings.TrimPrefix(path, root), got, want)
			}
			// The old walk warned exactly where the roster returns a ReadError,
			// and with the kind's name in the message.
			var re *ReadError
			if (len(warned) > 0) != errors.As(err, &re) {
				t.Errorf("%+v %s: old walk warned %v, roster error %v", o, path, warned, err)
			} else if re != nil {
				warnedTotal++
				if msg := "failed to parse " + re.Kind + " versions"; msg != warned[0] {
					t.Errorf("the walk's WARN would read %q, it read %q", msg, warned[0])
				}
			}
		}
	}
	// Guard the denominator: the tree must exercise the dispatch.
	if compared != 4*len(files) || withDeps < 4*30 || warnedTotal != 4*2 {
		t.Errorf("compared %d files (%d in the tree x 4), %d with declarations, %d read failures; the tree no longer exercises the dispatch", compared, len(files), withDeps, warnedTotal)
	}
}

func TestRosterNamesEqualsTheOldNameDispatch(t *testing.T) {
	files := oracleTree(t).files
	counted, withNames := 0, 0
	for _, path := range files {
		wantLang, wantNames, wantCounted := oldNames(path)
		lang, names, matched, err := Names(path)
		gotCounted := matched && err == nil
		if gotCounted != wantCounted {
			t.Errorf("%s: roster counted=%v (matched %v, err %v), old walk counted=%v", path, gotCounted, matched, err, wantCounted)
			continue
		}
		if !wantCounted {
			continue
		}
		counted++
		if len(wantNames) > 0 {
			withNames++
		}
		if lang != wantLang || !sameInOrder(names, wantNames, unorderedReader(path)) {
			t.Errorf("%s: roster (%q, %v), old walk (%q, %v)", path, lang, names, wantLang, wantNames)
		}
	}
	if counted < 40 || withNames < 25 {
		t.Errorf("%d files counted, %d with names; the tree no longer exercises the name dispatch", counted, withNames)
	}
}

// TestRosterCoversTheOldFilenameTable: every name the old table knew is a
// roster kind with the same language, and the roster names nothing else with
// a language except the one predicate kind the old walk matched by extension.
func TestRosterCoversTheOldFilenameTable(t *testing.T) {
	inRoster := map[string]string{}
	for _, k := range Roster() {
		if k.Language != "" {
			inRoster[k.Name] = k.Language
		}
	}
	for name, lang := range oldManifestFiles {
		if inRoster[name] != lang {
			t.Errorf("roster files %q under %q, the old table under %q", name, inRoster[name], lang)
		}
		delete(inRoster, name)
	}
	if len(inRoster) != 1 || inRoster["*.csproj"] != ".NET" {
		t.Errorf("roster kinds with a language beyond the old table: %v; want only *.csproj", inRoster)
	}
}
