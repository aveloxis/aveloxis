// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// manifest_parser_names_test.go — v0.29.56. Manifest parsers that read
// text which is not a package name, found in the libyear WARN samples of
// the 2026-09-17 chaoss.tv log. Every input below is a trimmed copy of the
// real manifest in the named repository (fetched the same day):
//   - Cargo.toml: a comment line with '=' (eclipse-zenoh/zenoh-flat-jni:
//     "# zenoh-flat's `unstable` feature (= `zenoh/unstable`)"), the
//     closing line of a multi-line inline table (eclipse-zenoh/zenoh-python:
//     "], default-features = false }"), and dotted keys
//     (kata-containers: "oci-spec.workspace = true", 10 lines and 1,702
//     deps in two hours).
//   - Gemfile: a trailing comment kept in the gem name
//     (meshery/meshery.io: "gem 'logger'   # stdlib in Ruby <= 3.x").
//   - setup.py: a function-call argument read as a requirement
//     (pixie-io/cpplint: "read_without_comments('dev-requirements')").
//   - requirements*.txt: VCS and URL references sent to PyPI as names
//     (eclipse-velocitas: "git+https://github.com/…/vehicle-model-python.git@v0.3.0").
//   - pom.xml: property references sent to Maven Central unresolved
//     (knative/func "${quarkus.platform.group-id}", dogtagpki/pki
//     "${project.groupId}").
//   - package.json: npm aliases and local packages resolved as registry
//     names (chapeaux/web-component-analyzer "typescript-3.5":
//     "npm:typescript@~3.5.3"; eclipse-zenoh/zenoh-demos
//     "@ZettaScaleLabs/zenoh-ts": "file:…").

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func writeManifest(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func depNames(deps []libyearDep) []string {
	var out []string
	for _, d := range deps {
		out = append(out, d.Name+"@"+d.Version)
	}
	sort.Strings(out)
	return out
}

func assertDeps(t *testing.T, label string, got []libyearDep, want []string) {
	t.Helper()
	sort.Strings(want)
	g := depNames(got)
	if strings.Join(g, ",") != strings.Join(want, ",") {
		t.Errorf("%s:\n got  %v\n want %v", label, g, want)
	}
}

// Real shape: eclipse-zenoh/zenoh-flat-jni + zenoh-python + kata-containers.
const cargoRealShapes = `[package]
name = "zenoh_flat_jni"
version = "1.10.1"

[dependencies]
# ` + "`unstable`" + ` must be on to match the ` + "`unstable`" + ` zenoh features enabled below:
# zenoh-flat's ` + "`unstable`" + ` feature (= ` + "`zenoh/unstable`" + `) gates the QoS surface
zenoh-flat = { version = "1.10.1", git = "https://github.com/eclipse-zenoh/zenoh-flat.git", branch = "main", features = ["unstable"] }
prebindgen-jni-runtime = "0.5"   # the leaf crate
zenoh = { version = "1.10.1", git = "https://github.com/eclipse-zenoh/zenoh.git", branch = "main", features = [
  "internal",
  "unstable",
], default-features = false }
jni = "0.21.1"
tracing = { version = "0.1", features = ["log"] }
oci-spec.workspace = true
serde_json.workspace = true
local-crate = { path = "../local-crate" }
published-member = { path = "../member", version = "2.1.0" }
serde.version = "1.0.200"
serde.features = ["derive"]
private = { version = "1.0", registry = "internal" }

[build-dependencies]
syn = "2"

[lib]
crate-type = ["staticlib", "cdylib"]
`

func TestParseCargoVersionsRealShapes(t *testing.T) {
	deps := parseCargoVersions(writeManifest(t, "Cargo.toml", cargoRealShapes))
	// Every declared dependency is inventoried — including the comment
	// line's text and the inline table's closing line, which are NOT
	// dependencies and must be absent.
	assertDeps(t, "Cargo.toml libyear deps", deps, []string{
		"prebindgen-jni-runtime@0.5",
		"jni@0.21.1",
		"tracing@0.1",
		"oci-spec@",
		"serde_json@",
		"local-crate@",
		"published-member@2.1.0",
		"serde@1.0.200",
		"private@1.0",
		"zenoh@1.10.1",
		"zenoh-flat@1.10.1",
		"syn@2",
	})
	// git- and alternative-registry-sourced deps are not the crates.io
	// crate of that name, and a path dep is only the one it publishes as:
	// inventoried, never looked up.
	nonRegistry := map[string]bool{}
	for _, d := range deps {
		if d.NonRegistry {
			nonRegistry[d.Name] = true
		}
		if d.Name == "syn" && d.Type != "build" {
			t.Errorf("syn scope = %q, want build", d.Type)
		}
	}
	for _, name := range []string{"zenoh", "zenoh-flat", "local-crate", "private"} {
		if !nonRegistry[name] {
			t.Errorf("%s must be marked non-registry", name)
		}
	}
	for _, name := range []string{"jni", "published-member", "serde", "oci-spec"} {
		if nonRegistry[name] {
			t.Errorf("%s is a crates.io dependency and must be looked up", name)
		}
	}
}

func TestParseTOMLDepsRealShapes(t *testing.T) {
	names := parseTOMLDeps(cargoRealShapes, "[dependencies]")
	sort.Strings(names)
	want := []string{"jni", "local-crate", "oci-spec", "prebindgen-jni-runtime", "private", "published-member", "serde", "serde_json", "tracing", "zenoh", "zenoh-flat"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("parseTOMLDeps inventory:\n got  %v\n want %v", names, want)
	}
}

func TestParseGemfileVersionsStripsTrailingComments(t *testing.T) {
	// Real shape: meshery/meshery.io Gemfile.
	path := writeManifest(t, "Gemfile", `source "https://rubygems.org"
gem "jekyll", "~> 4.3"            # the site generator
gem 'logger'                    # stdlib in Ruby <= 3.x, explicit in Ruby >= 4.0
gem "tzinfo-data", platforms: [:mingw, :x64_mingw] # windows
gem "hash#tag", "1.0"
`)
	assertDeps(t, "Gemfile", parseGemfileVersions(path), []string{"jekyll@4.3", "logger@", "tzinfo-data@", "hash#tag@1.0"})
	if names := parseGemfile(mustRead(t, path)); strings.Join(names, ",") != "jekyll,logger,tzinfo-data,hash#tag" {
		t.Errorf("parseGemfile inventory = %v", names)
	}
}

func TestSetupPyIgnoresFunctionCallArguments(t *testing.T) {
	// Real shape: pixie-io/cpplint setup.py.
	content := `test_required = read_without_comments('test-requirements')

setup(name='cpplint',
      install_requires=['requests>=2.0', "six"],
      tests_require=test_required,
      extras_require={
          'test': test_required,
          'dev': read_without_comments('dev-requirements') + test_required,
          'docs': ['sphinx>=4.0'],
      },
      long_description=open('README.rst').read())
`
	assertDeps(t, "setup.py install_requires", parseSetupPyVersions(content), []string{"requests@2.0", "six@"})
	assertDeps(t, "setup.py extras", parseSetupPyDevBuildVersions(content), []string{"sphinx@4.0"})
}

func TestRequirementsSkipsVCSURLAndPathReferences(t *testing.T) {
	// Real shape: eclipse-velocitas/vehicle-app-python-sdk requirements-links.txt.
	path := writeManifest(t, "requirements.txt", `# Needed by some old examples that rely on a pre-generated signal model
git+https://github.com/eclipse-velocitas/vehicle-model-python.git@v0.3.0
hg+https://hg.example.org/project#egg=project
https://files.example.org/pkg-1.0.tar.gz
file:///opt/wheels/local.whl
./vendored/package
../sibling
mypkg @ git+https://github.com/org/mypkg.git@v1.2
flask==2.0.0
`)
	assertDeps(t, "requirements.txt", parseRequirementsTxtVersions(path), []string{"flask@2.0.0"})

	// The check runs on the STRIPPED line: a comment or an environment
	// marker may contain " @ " (the PEP 508 direct-reference marker) or a
	// URL, and dropping the whole line would take a real dependency out of
	// libyear AND out of the OSV scan.
	path = writeManifest(t, "requirements.txt", `numpy==1.26.0  # updated @ 2026-01-01 by https://example.org/bot
requests==2.31.0 ; python_version >= "3.8"  # see https://example.org/notes
urllib3==2.2.0  # pinned ./vendored copy dropped
`)
	assertDeps(t, "requirements.txt with @ and URLs in comments", parseRequirementsTxtVersions(path),
		[]string{"numpy@1.26.0", "requests@2.31.0", "urllib3@2.2.0"})
}

func TestPomXMLResolvesPropertyReferences(t *testing.T) {
	// Real shape: knative/func templates/quarkus/http/pom.xml.
	knative := `<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.acme</groupId>
  <artifactId>function</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <properties>
    <quarkus.platform.artifact-id>quarkus-bom</quarkus.platform.artifact-id>
    <quarkus.platform.group-id>io.quarkus.platform</quarkus.platform.group-id>
    <quarkus.platform.version>3.39.1</quarkus.platform.version>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>${quarkus.platform.group-id}</groupId>
        <artifactId>${quarkus.platform.artifact-id}</artifactId>
        <version>${quarkus.platform.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.quarkus</groupId>
      <artifactId>quarkus-arc</artifactId>
    </dependency>
    <dependency>
      <groupId>${undefined.group}</groupId>
      <artifactId>ghost</artifactId>
    </dependency>
  </dependencies>
</project>`
	assertDeps(t, "knative pom", parsePomXMLVersions(knative), []string{"io.quarkus.platform:quarkus-bom@3.39.1", "io.quarkus:quarkus-arc@"})

	// Real shape: dogtagpki/pki base/acme/pom.xml — project.groupId comes
	// from <parent> when the project does not declare its own.
	dogtag := `<project>
    <parent>
        <groupId>org.dogtagpki.pki</groupId>
        <artifactId>pki-base-parent</artifactId>
        <version>11.10.1-SNAPSHOT</version>
    </parent>
    <artifactId>pki-acme</artifactId>
    <dependencies>
        <dependency>
            <groupId>${project.groupId}</groupId>
            <artifactId>pki-server</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>`
	assertDeps(t, "dogtag pom", parsePomXMLVersions(dogtag), []string{"org.dogtagpki.pki:pki-server@11.10.1-SNAPSHOT"})

	// A reference cycle must terminate and stay unresolved (skipped).
	cycle := `<project><properties><a>${b}</a><b>${a}</b></properties><dependencies>
<dependency><groupId>${a}</groupId><artifactId>x</artifactId></dependency></dependencies></project>`
	assertDeps(t, "cyclic pom", parsePomXMLVersions(cycle), nil)
}

func TestPackageJSONAliasesAndLocalPackages(t *testing.T) {
	// Real shapes: chapeaux/web-component-analyzer and eclipse-zenoh/zenoh-demos.
	path := writeManifest(t, "package.json", `{
  "dependencies": {
    "@ZettaScaleLabs/zenoh-ts": "file:eclipse-zenohs/zenoh-ts/zenoh-ts/",
    "shared": "workspace:*",
    "linked": "link:../linked",
    "gitdep": "git+https://github.com/org/gitdep.git#v1.0.0",
    "shorthand": "github:org/shorthand",
    "tarball": "https://example.org/tarball-1.0.0.tgz",
    "lodash": "^4.17.21",
    "catalogued": "catalog:"
  },
  "devDependencies": {
    "typescript-3.5": "npm:typescript@~3.5.3",
    "@scoped/alias": "npm:@real/pkg@1.2.3",
    "bare-alias": "npm:left-pad"
  }
}`)
	deps, err := parsePackageJSONVersions(path)
	if err != nil {
		t.Fatal(err)
	}
	// Versions are raw here; normalizeParsedVersion runs later in scanLibyear.
	var got []libyearDep
	for _, d := range deps {
		d.Version = normalizeParsedVersion(d.Manager, d.Version)
		got = append(got, d)
	}
	assertDeps(t, "package.json", got, []string{
		"lodash@4.17.21",
		"catalogued@",
		"typescript@3.5.3",
		"@real/pkg@1.2.3",
		"left-pad@",
		// Inventoried, not looked up on the registry.
		"@ZettaScaleLabs/zenoh-ts@",
		"shared@",
		"linked@",
		"gitdep@",
		"shorthand@",
		"tarball@",
	})
	nonRegistry := map[string]bool{}
	for _, d := range got {
		if d.NonRegistry {
			nonRegistry[d.Name] = true
		}
	}
	for _, name := range []string{"@ZettaScaleLabs/zenoh-ts", "shared", "linked", "gitdep", "shorthand", "tarball"} {
		if !nonRegistry[name] {
			t.Errorf("%s must be marked non-registry", name)
		}
	}
	// An alias and a catalog reference name real registry packages.
	for _, name := range []string{"typescript", "@real/pkg", "left-pad", "lodash", "catalogued"} {
		if nonRegistry[name] {
			t.Errorf("%s must be looked up on the registry", name)
		}
	}
}

// TestNonRegistryDepsAreNeverLookedUpOrStored pins what the NonRegistry
// flag is for. They get no registry call — that was the bug — and no row:
// a purl-less row would enter the vulnerability scan's universe as an
// unscannable dependency, which blocks that repo's scan-complete stamp
// (v0.28.5). The dependency scan still inventories them.
func TestNonRegistryDepsAreNeverLookedUpOrStored(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/collector/analysis.go"),
		"func (ac *AnalysisCollector) scanLibyear(")
	skip := strings.Index(body, "if dep.NonRegistry {")
	resolveCall := strings.Index(body, "resolveLibyearCached(")
	insert := strings.Index(body, "InsertRepoLibyear(")
	if skip < 0 || resolveCall < 0 || insert < 0 {
		t.Fatal("cannot find the non-registry branch, the resolve call or the insert")
	}
	if !(skip < resolveCall && skip < insert) {
		t.Error("the non-registry branch must come before both the registry lookup and the insert")
	}
	if !strings.Contains(body[skip:resolveCall], "continue") {
		t.Error("a non-registry dependency must be skipped, not resolved or stored")
	}
}

// TestPythonManifestsSkipVCSURLAndPathReferencesEverywhere: the fix for
// requirements*.txt (the eclipse-velocitas shape, v0.29.56) applied at ONE
// of Python's parsing entry points. pyproject.toml, setup.py and setup.cfg
// all build their deps through parsePyRequirement, which had no such check,
// so "velocitas-sdk @ git+https://…" and "./vendor/local-pkg" were sent to
// PyPI as package names from those files — the same defect, one file format
// away (a missed sibling).
func TestPythonManifestsSkipVCSURLAndPathReferencesEverywhere(t *testing.T) {
	pyproject := `[project]
name = "app"
dependencies = [
  "flask==2.0.0",
  "velocitas-sdk @ git+https://github.com/eclipse-velocitas/vehicle-model-python.git@v0.3.0",
  "velocitas-compact@git+https://github.com/eclipse-velocitas/vehicle-model-python.git@v0.3.0",
  "local-thing @ file:///opt/local-thing",
  "https://files.example.org/pkg-1.0.tar.gz",
]
`
	assertDeps(t, "pyproject.toml", parsePyprojectVersionsFromContent(pyproject), []string{"flask@2.0.0"})

	setupPy := `setup(name='app',
      install_requires=[
          'requests>=2.0',
          'velocitas-sdk @ git+https://github.com/x/y.git@v1',
          'velocitas-compact@git+https://github.com/x/y.git@v1',
          './vendor/local-pkg',
      ])
`
	assertDeps(t, "setup.py", parseSetupPyVersions(setupPy), []string{"requests@2.0"})

	setupCfg := `[options]
install_requires =
    flask==2.0.0
    velocitas-sdk @ git+https://github.com/x/y.git@v1
    ../sibling-pkg
`
	assertDeps(t, "setup.cfg", parseSetupCfgVersions(setupCfg), []string{"flask@2.0.0"})

	// The predicate must run on the line WITHOUT its comment. A comment
	// containing " @ " (or a URL, or a path) would otherwise delete a
	// legitimate pinned dependency — it would get no libyear row and, with
	// it, leave the OSV scan entirely. This is the round-6 finding for
	// requirements*.txt, which round 9 re-introduced at the entry points it
	// routed through parsePyRequirement.
	commented := `[project]
dependencies = [
  "requests>=2.31.0",  # pinned @ 2.31 after CVE-2023-32681
  "flask==2.0.0",  # see https://example.org/notes
]
`
	assertDeps(t, "pyproject with comments", parsePyprojectVersionsFromContent(commented),
		[]string{"requests@2.31.0", "flask@2.0.0"})

	cfgCommented := `[options]
install_requires =
    flask==2.0.0  # bumped @ 2026-01-01
    requests>=2.31.0  # https://example.org/why
`
	assertDeps(t, "setup.cfg with comments", parseSetupCfgVersions(cfgCommented),
		[]string{"flask@2.0.0", "requests@2.31.0"})
}

// TestPoetryAndPipfileSkipNonRegistrySources: Poetry and Pipfile build their
// dependencies straight from TOML key/value instead of parsePyRequirement,
// so the v0.29.56 rule has to reach them too — otherwise
// `mypkg = { path = "../mypkg" }` is looked up on PyPI under the name of
// whatever package shares it (the SR-6 shape).
func TestPoetryAndPipfileSkipNonRegistrySources(t *testing.T) {
	poetry := `[tool.poetry.dependencies]
python = "^3.11"
flask = "^2.0.0"
mypkg = { path = "../mypkg" }
forked = { git = "https://github.com/org/forked.git", branch = "main" }
tarball = { url = "https://example.org/pkg-1.0.tar.gz" }
pinned = { version = "^3.1.0", optional = true }
`
	assertDeps(t, "poetry", parsePoetryVersions(poetry), []string{"flask@2.0.0", "pinned@3.1.0"})

	pipfile := `[packages]
requests = "==2.31.0"
local = {path = "../local"}
forked = {git = "https://github.com/org/forked.git", ref = "main"}
pinned = {version = "==2.0.0", extras = ["socks"]}
`
	assertDeps(t, "pipfile", parsePipfileVersions(pipfile), []string{"requests@2.31.0", "pinned@2.0.0"})

	// Poetry's GROUP tables build their deps the same way and need the same
	// rule (they are collected when collection.dev_build_deps is on).
	groups := `[tool.poetry.group.dev.dependencies]
pytest = "^7.0.0"
mypkg = { path = "../mypkg" }
forked = { git = "https://github.com/org/forked.git" }

[tool.poetry.dev-dependencies]
legacy-local = { path = "../legacy" }
black = "^24.1.0"
`
	assertDeps(t, "poetry groups", parsePyprojectDevBuildVersions(groups),
		[]string{"pytest@7.0.0", "black@24.1.0"})
}

// TestInlineDeclarationsSplitOnStructuralCommasOnly — v0.29.56 round 12.
// A comma inside a quoted string is DATA; only a comma between array items
// or table keys is a separator. Three sites split on every comma instead:
// the PEP 621 inline array on both the name path
// (extractPEP621DepsFromLine) and the libyear path
// (extractPEP621VersionDeps), and the Pipfile inline table. A bounded
// range written on one line — dependencies = ["requests>=2.31.0,<3.0.0"],
// the ordinary way to cap a major version — therefore invented a second
// dependency named "<3.0.0" and sent it to PyPI as a package name. TOML
// already has one quote- and nesting-aware splitter (splitTOMLTopLevel)
// and one table-version reader (pythonTableVersion); a second inline
// spelling of either is the defect (SR-17), which is why the Poetry arm
// fixed in round 10 and the Pipfile arm beside it disagreed.
func TestInlineDeclarationsSplitOnStructuralCommasOnly(t *testing.T) {
	// A single item carrying a bounded range: one dependency, not two.
	const bounded = `dependencies = ["requests>=2.31.0,<3.0.0"]`
	assertDeps(t, "inline array, bounded range", extractPEP621VersionDeps(bounded), []string{"requests@2.31.0"})
	if got := extractPEP621DepsFromLine(bounded); len(got) != 1 || got[0] != "requests" {
		t.Errorf("inline array names, bounded range: got %v, want [requests]", got)
	}

	// Structural commas still separate: the fix must not merge items.
	const twoItems = `dependencies = ["requests>=2.31.0,<3.0.0", "flask>=2.0", "tomli>=1.1.0; python_version < '3.11'"]`
	assertDeps(t, "inline array, three items", extractPEP621VersionDeps(twoItems),
		[]string{"requests@2.31.0", "flask@2.0", "tomli@1.1.0"})
	if got := extractPEP621DepsFromLine(twoItems); len(got) != 3 {
		t.Errorf("inline array names, three items: got %v, want 3 names", got)
	}

	// Extras carry brackets and a comma inside the quoted item. Split on
	// every comma, both paths fragment the dependency and neither resolves
	// it; exactly which fragments each produced depended on how it stripped
	// extras, which the round-13 and round-14 fixes have since changed
	// again. What is asserted here is the CORRECT output — one dependency,
	// on both paths.
	const extras = `dependencies = ["celery[redis,auth]>=5.3"]`
	assertDeps(t, "inline array, extras", extractPEP621VersionDeps(extras), []string{"celery@5.3"})
	if got := extractPEP621DepsFromLine(extras); len(got) != 1 || got[0] != "celery" {
		t.Errorf("inline array names, extras: got %v, want [celery]", got)
	}

	// The Pipfile inline table reads its version with a second, hand-rolled
	// spelling of pythonTableVersion instead of calling it. The two disagree
	// whenever an EARLIER key's quoted value carries a comma: the hand-rolled
	// one splits inside the string, then stops at the first fragment that
	// merely STARTS with "version" and reports no version at all. The input
	// below is contrived — a realistic key order passes by luck — but the
	// divergence is the defect (SR-17), and luck is not a contract.
	pipfile := "[packages]\n" +
		`requests = {markers = "extra == 'a', version needed", version = "==2.31.0"}` + "\n" +
		`flask = {version = "==2.0.2", extras = ["async", "dotenv"]}` + "\n"
	assertDeps(t, "pipfile inline tables", parsePipfileVersions(pipfile),
		[]string{"requests@2.31.0", "flask@2.0.2"})
}

// TestPythonSpecifierSetOrderDoesNotEatTheName — v0.29.56 round 13.
// PEP 508 lets a specifier set be written in EITHER order, and
// "numpy<2,>=1.22" is as legal as "numpy>=1.22,<2". Five Python parsers
// found the end of the package name by scanning a PREFERENCE list of
// operators — in four different orders, one of them missing five operators
// — instead of taking the LEFTMOST operator in the string. So the reverse
// order matched ">=" first and everything before it, "numpy<2,", became the
// package name: a name no registry can answer for, hence no libyear row, no
// purl and no OSV coverage for a real dependency.
//
// apache/airflow's own pyproject.toml carries the shape
// ("apache-airflow-task-sdk<1.5.0,>=1.4.0"), so this is not an edge case;
// the multi-line array and requirements.txt have always been wrong, and
// round 12's splitter fix exposed it on the single-line array by no longer
// cutting the string before the second operator.
//
// Name and version are two questions: the name ends at the leftmost
// operator, and the version is the floor of the set (an exact pin first,
// then ~=, then >=), which is what the preference lists were reaching for.
func TestPythonSpecifierSetOrderDoesNotEatTheName(t *testing.T) {
	// Both orders name the same package and report the same floor.
	for _, req := range []string{"numpy>=1.22,<2", "numpy<2,>=1.22"} {
		if got := extractPyDepName(req); got != "numpy" {
			t.Errorf("extractPyDepName(%q) = %q, want numpy", req, got)
		}
		if got := extractPEP621DepName(req); got != "numpy" {
			t.Errorf("extractPEP621DepName(%q) = %q, want numpy", req, got)
		}
		d := parsePyRequirement(req)
		if d == nil || d.Name != "numpy" || d.Version != "1.22" {
			t.Errorf("parsePyRequirement(%q) = %+v, want numpy@1.22", req, d)
		}
		assertDeps(t, "inline array "+req,
			extractPEP621VersionDeps(`dependencies = ["`+req+`"]`), []string{"numpy@1.22"})
		if got := extractPEP621DepsFromLine(`dependencies = ["` + req + `"]`); len(got) != 1 || got[0] != "numpy" {
			t.Errorf("inline array names %q: got %v, want [numpy]", req, got)
		}
		// The multi-line array is the common form and was never protected.
		assertDeps(t, "multi-line array "+req,
			parsePEP621Versions("[project]\ndependencies = [\n  \""+req+"\",\n]\n"), []string{"numpy@1.22"})
		if got := parseRequirementsTxt(req + "\n"); len(got) != 1 || got[0] != "numpy" {
			t.Errorf("parseRequirementsTxt(%q): got %v, want [numpy]", req, got)
		}
		assertDeps(t, "requirements.txt "+req, requirementsTxtVersionsFromContent(t, req+"\n"), []string{"numpy@1.22"})
	}

	// The real shape, from apache/airflow's pyproject.toml.
	const airflow = "apache-airflow-task-sdk<1.5.0,>=1.4.0"
	assertDeps(t, "airflow", parsePEP621Versions("[project]\ndependencies = [\n  \""+airflow+"\",\n]\n"),
		[]string{"apache-airflow-task-sdk@1.4.0"})

	// Each rank boundary needs a MULTI-clause case: with only single-clause
	// sets, any rank still beats the empty seed, so the precedence the doc
	// comment promises would be unpinned (round 14).
	for _, tc := range []struct{ req, want string }{
		{"pkg==1.2.3", "pkg@1.2.3"},
		{"pkg===1.2.3+local", "pkg@1.2.3+local"},
		{"pkg~=1.4.2", "pkg@1.4.2"},
		{"pkg!=1.3,~=1.2", "pkg@1.2"},
		{"celery[redis,auth]>=5.3", "celery@5.3"},
		// An exact pin outranks a lower bound, in either order.
		{"pkg==1.4,>=1.0", "pkg@1.4"},
		{"pkg>=1.0,==1.4", "pkg@1.4"},
		{"pkg===1.0,>=0.9", "pkg@1.0"},
		// A compatible release outranks a lower bound, which outranks an
		// inclusive ceiling.
		{"pkg~=1.4,>=1.0", "pkg@1.4"},
		{"pkg>=1.0,<=4", "pkg@1.0"},
		// An INCLUSIVE ceiling names a version its own clause permits, so it
		// is reported when nothing better is stated. ("<=4,<5" permits 4;
		// "<3,<=4" would not, and pyFloorVersion does not catch that —
		// the rule is per clause, documented at pyPermittedVersionRank.)
		{"pkg<=3", "pkg@3"},
		{"pkg<=4,<5", "pkg@4"},
		// An EXCLUSIVE bound names a version the set FORBIDS, exactly as
		// "!=" does — "<X" and ">X" both rule X out. Recording it is the
		// same fabrication, and for "<" it fails in the more dangerous
		// direction: measured against OSV on 2026-09-17, pkg:pypi/pyyaml@6.0
		// reports 0 vulnerabilities while permitted releases below it carry
		// up to 6 (5.3.1 → 2, 5.3 → 4, 5.1 → 6). Both shapes are ordinary in
		// real manifests — transformers ships "GitPython<3.1.19",
		// matplotlib "pandas!=0.25.0".
		{"pyyaml<6.0", "pyyaml@"},
		{"GitPython<3.1.19", "GitPython@"},
		{"pkg>1.0", "pkg@"},
		{"pkg<4,>3.1.0", "pkg@"},
		// A space before the operator is ordinary PEP 508 style (fastapi,
		// ray and ansible all ship it) and must not survive in the NAME: a
		// trailing space escapes into the purl as %20, which wireValidPurl
		// accepts and OSV can never match.
		{"anyio[trio] >=3.2.1,<5.0.0", "anyio@3.2.1"},
		{"mkdocstrings[python] >=1.0.3", "mkdocstrings@1.0.3"},
		// An EXCLUDED version is not the version in use. Recording it put
		// the one release the manifest rules out into the purl and sent it
		// to OSV, which then answers about a version nobody installed
		// (matplotlib, transformers and pandas all ship bare "!=" sets).
		// With no floor to report the version stays empty, which yields a
		// versionless purl — OSV answers that with the package's whole
		// advisory history, which is honest about what the manifest said.
		{"pandas!=0.25.0", "pandas@"},
		{"grpcio != 1.56.0", "grpcio@"},
		{"nbconvert[execute]!=6.0.0,!=6.0.1,!=7.3.0", "nbconvert@"},
		{"pandas>=1.0,!=1.1", "pandas@1.0"},
	} {
		d := parsePyRequirement(tc.req)
		got := "<nil>"
		if d != nil {
			got = d.Name + "@" + d.Version
		}
		if got != tc.want {
			t.Errorf("parsePyRequirement(%q) = %s, want %s", tc.req, got, tc.want)
		}
	}

	// A spaced operator must not leave a trailing space on the name in the
	// parsers that return it directly, nor in the inventory.
	if got := extractPEP621DepName("anyio[trio] >=3.2.1,<5.0.0"); got != "anyio" {
		t.Errorf("extractPEP621DepName spaced = %q, want anyio", got)
	}
	if got := extractPyDepName("anyio[trio] >=3.2.1"); got != "anyio" {
		t.Errorf("extractPyDepName spaced = %q, want anyio", got)
	}
	if got := parseRequirementsTxt("resolvelib >= 0.8.0, < 2.0.0\n"); len(got) != 1 || got[0] != "resolvelib" {
		t.Errorf("parseRequirementsTxt spaced = %v, want [resolvelib]", got)
	}

	// An unclosed extras bracket is malformed TOML, but both name
	// extractors must still agree about it — asserted by VALUE, because
	// "not the duplicated string" left the else arm free to return
	// anything (round 14).
	if got := extractPEP621DepName("celery[redis"); got != "celery" {
		t.Errorf("extractPEP621DepName(\"celery[redis\") = %q, want celery", got)
	}
	if d := parsePyRequirement("celery[redis"); d == nil || d.Name != "celery" {
		t.Errorf("parsePyRequirement(\"celery[redis\") = %+v, want name celery — the two name extractors must not disagree about the same line", d)
	}
	// ALL five extras strips must agree, not three of them: an unclosed
	// bracket left "celery[redis" in requirements.txt, which buildPurl turns
	// into "pkg:pypi/celery[redis" — a purl wireValidPurl ACCEPTS, so it
	// reaches OSV as a package that cannot exist.
	if got := extractPyDepName("celery[redis"); got != "celery" {
		t.Errorf("extractPyDepName(\"celery[redis\") = %q, want celery", got)
	}
	if got := parseRequirementsTxt("celery[redis\n"); len(got) != 1 || got[0] != "celery" {
		t.Errorf("parseRequirementsTxt(\"celery[redis\") = %v, want [celery]", got)
	}
	if got := requirementsTxtVersionsFromContent(t, "celery[redis\n"); len(got) != 1 || got[0].Name != "celery" {
		t.Errorf("parseRequirementsTxtVersions(\"celery[redis\") = %v, want name celery", depNames(got))
	}

	// F5: TOML literal strings are legal in pyproject.toml, and the name
	// path kept the apostrophe while the libyear path stripped it — the two
	// disagreed on the same file.
	if got := extractPEP621DepsFromLine(`dependencies = ['tomli>=1.1.0', 'flask']`); len(got) != 2 ||
		got[0] != "tomli" || got[1] != "flask" {
		t.Errorf("single-quoted array: got %v, want [tomli flask]", got)
	}
}

// requirementsTxtVersionsFromContent runs the requirements.txt version
// parser, which reads from disk, over literal content.
func requirementsTxtVersionsFromContent(t *testing.T, content string) []libyearDep {
	t.Helper()
	return parseRequirementsTxtVersions(writeManifest(t, "requirements.txt", content))
}

// TestDirectReferenceDetectedWithoutSpaces — v0.29.56 round 17. PEP 508's
// direct-reference marker takes optional whitespace ("urlspec = '@'
// URI_reference", with wsp* around it), so "mypkg[extra]@https://host/x.whl"
// is as legal as the spaced form. The predicate matched only " @ ", so the
// unspaced form went to PyPI as a package name — the very failure this
// release exists to fix, one space away from the shape it fixed.
func TestDirectReferenceDetectedWithoutSpaces(t *testing.T) {
	// The last three carry NO "://", so they separate this rule from the
	// nearest competing predicate — a URL-scheme test, which is what a
	// later simplification would reach for and which every "https://"
	// fixture alone would let pass. All three are legal PEP 508 direct
	// references (checked against packaging.requirements.Requirement:
	// name=mypkg, url=file:x.whl / ../local/pkg / local-dir/pkg-1.0.tar.gz).
	for _, req := range []string{
		"mypkg @ https://host/x.whl",
		"mypkg@https://host/x.whl",
		"mypkg[extra]@https://host/x.whl",
		"velocitas-sdk@git+https://github.com/org/repo.git@v1.2",
		"mypkg@file:x.whl",
		"mypkg @ ../local/pkg",
		"mypkg@local-dir/pkg-1.0.tar.gz",
	} {
		if !isNonRegistryPyRequirement(req) {
			t.Errorf("isNonRegistryPyRequirement(%q) = false, want true", req)
		}
		if d := parsePyRequirement(req); d != nil {
			t.Errorf("parsePyRequirement(%q) = %+v, want nil — a direct reference names no PyPI package", req, d)
		}
	}
	// An '@' is only a direct-reference marker when it separates a NAME
	// from a reference. These are ordinary registry requirements and must
	// still be looked up.
	for _, req := range []string{"flask==2.0.0", "requests>=2.31.0,<3.0.0", "celery[redis]>=5.3"} {
		if isNonRegistryPyRequirement(req) {
			t.Errorf("isNonRegistryPyRequirement(%q) = true, want false", req)
		}
	}
}
