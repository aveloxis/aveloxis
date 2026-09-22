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

	"github.com/aveloxis/aveloxis/internal/model"
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

// assertNames is assertDeps for the readers that return names only: an
// EXACT set, so an extra fabricated name fails as loudly as a missing one.
func assertNames(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if strings.Join(g, ",") != strings.Join(w, ",") {
		t.Errorf("%s:\n got  %v\n want %v", label, g, w)
	}
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

// Every reader of the `name = constraint` grammar resolves a quoted key to
// the same package name (v0.29.57, Copilot on PR #210): a name kept with its
// quotes matches nothing on PyPI, and two readers of one file disagreeing is
// the shape manifest_toml.go exists to prevent.
func TestQuotedTOMLDependencyKeysNameTheSamePackage(t *testing.T) {
	poetry := "[tool.poetry.dependencies]\npython = \"^3.11\"\n\"zope.interface\" = \"^6.0\"\nruamel.yaml = \"^0.18\"\n"
	assertDeps(t, "poetry versions", parsePoetryVersions(poetry), []string{"zope.interface@6.0", "ruamel.yaml@0.18"})

	pipfile := "[packages]\n\"zope.interface\" = \"==6.0\"\nruamel.yaml = \"==0.18\"\n"
	assertDeps(t, "pipfile versions", parsePipfileVersions(pipfile), []string{"zope.interface@6.0", "ruamel.yaml@0.18"})

	names, err := parsePipfileDeps(pipfile)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "parsePipfileDeps", names, []string{"zope.interface", "ruamel.yaml"})
}

// A bracket inside a quoted item is part of the VALUE — `"celery[redis]>=5.0"`
// names one package with an extra — and a bracket in a comment is not syntax
// at all. Both readers of `[project].dependencies` decided where the array
// began and ended with quote-blind `strings.Contains`, so:
//
//   - an extra on the OPENING line ended the array immediately and every
//     later dependency was dropped from the inventory, from libyear and from
//     the OSV scan; and
//   - the last item sharing a line with the closer (`"celery>=5.0"]`) left
//     the array open, so `requires-python`, `name` and `version` were
//     collected as dependencies and the item's own version kept the `"]`.
//
// Measured on the fixtures below before the fix (v0.29.57). The dev/build
// reader was given the quote-aware rule first; these are its runtime
// siblings, and they read the same file.
func TestProjectDependencyArrayBoundsIgnoreQuotedBrackets(t *testing.T) {
	t.Run("an extra on the opening line does not end the array", func(t *testing.T) {
		content := "[project]\ndependencies = [\"celery[redis]>=5.0\",\n  \"flask==2.0\",\n  \"requests>=2.31\",\n]\n"
		assertDeps(t, "pep621 versions", parsePEP621Versions(content),
			[]string{"celery@5.0", "flask@2.0", "requests@2.31"})
		names, err := parsePyprojectDeps(content)
		if err != nil {
			t.Fatal(err)
		}
		assertNames(t, "pep621 inventory", names, []string{"celery", "flask", "requests"})
	})

	t.Run("the closer may share a line with the last item", func(t *testing.T) {
		content := "[project]\ndependencies = [\n  \"flask==2.0\",\n  \"celery>=5.0\"]\nrequires-python = \">=3.9\"\nname = \"demo\"\nversion = \"1.2.3\"\n"
		assertDeps(t, "pep621 versions", parsePEP621Versions(content),
			[]string{"flask@2.0", "celery@5.0"})
		names, err := parsePyprojectDeps(content)
		if err != nil {
			t.Fatal(err)
		}
		assertNames(t, "pep621 inventory", names, []string{"flask", "celery"})
	})

	t.Run("a commented-out item is not a dependency", func(t *testing.T) {
		content := "[project]\ndependencies = [\n  # \"coverage>=7\" dropped for now\n  \"flask==2.0\",  # the web bit\n]\n"
		assertDeps(t, "pep621 versions", parsePEP621Versions(content), []string{"flask@2.0"})
		names, err := parsePyprojectDeps(content)
		if err != nil {
			t.Fatal(err)
		}
		assertNames(t, "pep621 inventory", names, []string{"flask"})
	})
}

// The same comment rule at the other three readers of the `name = constraint`
// grammar. Production evidence (chaoss.tv extract, 2026-09-17): 234 rows of
// repo_deps_libyear carry a version ending in a stray quote, one of them the
// entire trailing comment — the version reaches the purl and the OSV lookup,
// and a commented-out line was inventoried as a package literally named
// "# requests".
func TestTrailingAndWholeLineCommentsAreNotDependencies(t *testing.T) {
	poetry := "[tool.poetry.dependencies]\nblack = \"^24.0\"  # pinned\n# requests = \"^2.0\"\n"
	assertDeps(t, "poetry versions", parsePoetryVersions(poetry), []string{"black@24.0"})

	pipfile := "[packages]\nrequests = \"==2.0\"  # http\n# flask = \"==1.0\"\n"
	assertDeps(t, "pipfile versions", parsePipfileVersions(pipfile), []string{"requests@2.0"})

	names, err := parsePipfileDeps(pipfile)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "pipfile inventory", names, []string{"requests"})
}

// setup.py declares its requirements as a Python list, and the list obeys the
// same two rules as a TOML array: a bracket inside a quoted requirement is
// part of the value, and a `#` comment is not code. Both setup.py readers
// decided the list had ended at the first `]` they saw anywhere on a line, so
// an extra in the FIRST requirement truncated the list — every later
// dependency missing from the inventory, from libyear and from the OSV scan —
// and neither stripped comments, so a requirement someone had commented out
// was collected, looked up and scanned (v0.29.57).
func TestSetupPyRequirementListBoundsAndComments(t *testing.T) {
	multi := `setup(
    name="demo",
    install_requires=[
        "celery[redis]>=5.0",
        "flask==2.0",
        "requests>=2.31",
    ],
)
`
	names, err := parseSetupPyDeps(multi)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "setup.py inventory", names, []string{"celery", "flask", "requests"})
	assertDeps(t, "setup.py versions", parseSetupPyVersions(multi),
		[]string{"celery@5.0", "flask@2.0", "requests@2.31"})

	commented := `setup(install_requires=[
        # 'old-pkg>=1',
        'flask==2.0',
    ],
    tests_require=[
        # 'old-test-pkg>=2',
        'pytest>=7',
    ],
)
`
	names2, err := parseSetupPyDeps(commented)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "setup.py inventory", names2, []string{"flask"})
	assertDeps(t, "setup.py versions", parseSetupPyVersions(commented), []string{"flask@2.0"})
	for _, d := range parseSetupPyDevBuildVersions(commented) {
		if d.Name == "old-test-pkg" {
			t.Error("a commented-out requirement in tests_require was collected as a test dependency")
		}
	}
}

// A Poetry inline table may span lines. Its continuation lines are the
// table's KEYS — `version`, `extras`, `git` — and reading them as
// declarations of their own invented packages literally named after the key:
// 56 libyear rows and 21 dependency rows on the production database carried
// one. They reach the purl and the OSV lookup like any other name.
func TestMultilineInlineTableDoesNotNameItsKeys(t *testing.T) {
	check := func(t *testing.T, label string, got []libyearDep, after string) {
		t.Helper()
		found := false
		for _, d := range got {
			switch d.Name {
			case "version", "extras", "git", "path", "optional", "markers":
				t.Errorf("%s: the inline table's %q key was collected as a package (at version %q)", label, d.Name, d.Version)
			case after:
				found = true
			}
		}
		// The dependency AFTER the table must still be found: a table that
		// never closes would swallow the rest of the section.
		if !found {
			t.Errorf("%s: %s, declared after the multi-line table, was not collected — the table never closed", label, after)
		}
	}

	poetry := "[tool.poetry.dependencies]\nblack = {\n    version = \"^24.0\",\n    extras = [\"d\"]\n}\nflask = \"^2.0\"\n"
	check(t, "parsePoetryVersions", parsePoetryVersions(poetry), "flask")

	group := "[tool.poetry.group.dev.dependencies]\nblack = {\n    version = \"^24.0\",\n    extras = [\"d\"]\n}\nruff = \"^0.4\"\n"
	check(t, "parsePyprojectDevBuildVersions", parsePyprojectDevBuildVersions(group), "ruff")

	pipfile := "[packages]\nblack = {\n    version = \"==24.0\"\n}\nflask = \"==2.0\"\n"
	check(t, "parsePipfileVersions", parsePipfileVersions(pipfile), "flask")

	names, err := parsePipfileDeps(pipfile)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "parsePipfileDeps", names, []string{"black", "flask"})
}

// A line that CONTINUES a multi-line inline table can begin with `[` — an
// array value wrapped onto its own line — and the Pipfile readers checked for
// a section header before they checked whether a table was open, so that line
// was taken as a new section: the table never closed, and every remaining
// declaration in the FILE was skipped. Worse than what it replaced, because
// the old code recovered at the next section header. Every reader that tracks
// an open table checks that first now (v0.29.57): the four line readers here
// and in analysis_devbuild.go, plus scanTOMLDepTables, which carries the same
// state for the NAME inventory and the Cargo tables. The [dev-packages]
// reader has no such check to order — it delegates rather than splitting.
func TestPipfileInlineTableDoesNotSwallowTheRestOfTheFile(t *testing.T) {
	content := `[packages]
black = {version = "==24.0", extras =
    ["d"]}
flask = "*"

[dev-packages]
pytest = "*"

[packages]
requests = "*"
`
	names, err := parsePipfileDeps(content)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "pipfile inventory", names, []string{"black", "flask", "requests"})

	var got []string
	for _, d := range parsePipfileVersions(content) {
		got = append(got, d.Name)
	}
	assertNames(t, "pipfile versions", got, []string{"black", "flask", "requests"})
}

// The Gemfile name inventory split the RAW line on `'` and only fell back to
// `"` when the line held no apostrophe at all — so an apostrophe in a trailing
// comment became the split point, and a double-quoted gem (what Rails' own
// generated Gemfile writes) came back as the comment's text while the real gem
// was lost. Its sibling version reader already stripped the comment, so the
// two readers of one Gemfile disagreed.
func TestGemfileNamesIgnoreApostrophesInComments(t *testing.T) {
	for _, tc := range []struct{ content, want string }{
		{"gem \"rails\", \"~> 7.0\" # don't upgrade yet\n", "rails"},
		{"gem \"rails\" # we shouldn't upgrade\n", "rails"},
		{"gem 'rails' # don't upgrade\n", "rails"},
		{"gem \"rails\"\n", "rails"},
		// A double-quoted NAME with a single-quoted CONSTRAINT: the split
		// preferred `'`, so the constraint became the package. 387 rows of
		// the production database are a Ruby "package" named `~> 7.0` or so.
		{"gem \"rails\", '~> 7.0'\n", "rails"},
		{"gem 'rails', \"~> 7.0\"\n", "rails"},
	} {
		assertNames(t, "gemfile "+tc.content, parseGemfile(tc.content), []string{tc.want})
	}
	// A commented-out gem is not a dependency at all.
	assertNames(t, "commented-out gem", parseGemfile("# gem \"old\"\ngem \"rails\"\n"), []string{"rails"})
}

// Scala and Swift comment with `//`, and neither reader stripped it, so a
// declaration someone had commented out was collected — with a valid-looking
// version, so it survived normalisation and reached the purl, the registry
// lookup and the OSV scan. A `//` inside a string (every Swift package URL)
// is not a comment.
func TestSlashCommentsAreNotDeclarations(t *testing.T) {
	sbt := "libraryDependencies += \"org.typelevel\" %% \"cats-core\" % \"2.10\"\n// libraryDependencies += \"old\" %% \"pkg\" % \"1.0\"\n"
	assertNames(t, "build.sbt inventory", parseBuildSbt(sbt), []string{"org.typelevel:cats-core"})
	assertDeps(t, "build.sbt versions", parseBuildSbtVersions(sbt), []string{"org.typelevel:cats-core@2.10"})

	sw := ".package(url: \"https://github.com/apple/swift-nio.git\", from: \"2.0.0\"),\n// .package(url: \"https://github.com/old/pkg.git\", from: \"1.0.0\"),\n"
	assertNames(t, "Package.swift inventory", parsePackageSwiftDeps(sw), []string{"swift-nio"})
	assertDeps(t, "Package.swift versions", parsePackageSwiftVersions(sw), []string{"swift-nio@2.0.0"})
}

// setup.py's extras dict is `{'test': [...], 'dev': [...]}`. The reader cut
// through the FIRST colon only, so on a single-line dict every later KEY was
// handed to the requirement extractor and became a package of its own.
func TestSetupPyExtrasKeysAreNotPackages(t *testing.T) {
	got := parseSetupPyDevBuildVersions("setup(extras_require={'test': ['pytest>=7'], 'dev': ['ruff>=0.4']})\n")
	for _, d := range got {
		if d.Name == "dev" || d.Name == "test" {
			t.Errorf("the extras key %q was collected as a package", d.Name)
		}
	}
	assertDeps(t, "setup.py extras", got, []string{"pytest@7", "ruff@0.4"})
}

// Poetry's MULTIPLE CONSTRAINTS form is a dependency whose value is an ARRAY
// of tables (`foo = [{version = "<=1.9", …}, {version = "^2.0", …}]`). The
// line readers take the opening bracket as the value and emit `foo` at
// version `[` — a fragment, not a version. That is not fixed in the readers
// on purpose: the central version-hygiene choke point (v0.27.71,
// normalizeParsedVersion, applied to EVERY parser's output in scanLibyear
// before resolution, storage and purl construction) is the layer that owns
// version validity, and a fourth spelling of the rule in the readers would be
// the duplication SR-17 exists to prevent. This pins that the gate really
// does reject what these readers can emit — production carries zero rows with
// such a version, which is the same claim measured from the other end.
func TestVersionGateRejectsTheFragmentsTheReadersCanEmit(t *testing.T) {
	for _, frag := range []string{"[", "[{version", "{version = \"*\"", "{"} {
		if got := normalizeParsedVersion("pypi", frag); got != "" {
			t.Errorf("normalizeParsedVersion(pypi, %q) = %q, want \"\" — a value the manifest readers can emit from a multi-line or multi-constraint declaration is not a version, and storing it would put it in the purl OSV is asked about", frag, got)
		}
	}
	// A real version still passes, or the gate would be rejecting everything.
	if got := normalizeParsedVersion("pypi", "24.0"); got != "24.0" {
		t.Errorf("normalizeParsedVersion(pypi, \"24.0\") = %q, want it unchanged", got)
	}
}

// [dev-packages] is read by isolating the section and handing it to the
// [packages] reader, and that isolation was a SECOND section splitter with
// the ordering this release fixed everywhere else: a continuation line
// beginning with `[` ended the section, so the rest of [dev-packages] was
// dropped before the delegate's own table tracking could see it.
func TestPipfileDevPackagesSurviveAWrappedTable(t *testing.T) {
	content := `[packages]
flask = "*"

[dev-packages]
black = {version = "==24.0", extras =
    ["d"]}
pytest = "*"
mypy = "*"
`
	var got []string
	for _, d := range parsePipfileDevPackages(content) {
		if d.Type != model.ScopeDev {
			t.Errorf("%s came back as %q, want dev", d.Name, d.Type)
		}
		got = append(got, d.Name)
	}
	assertNames(t, "pipfile dev-packages", got, []string{"black", "pytest", "mypy"})
	// The runtime section must not leak into the dev list.
	for _, n := range got {
		if n == "flask" {
			t.Error("flask is declared in [packages] and must not be reported as a dev dependency")
		}
	}
}

// An inline table that never closes is malformed TOML, and tracking its depth
// means the tracker would otherwise consume the rest of the FILE — losing
// declarations the pre-v0.29.57 code still collected. A section header is the
// boundary no inline table can cross, so it ends the table; a quoted array
// item that merely looks like one (`["d"]`) does not.
func TestUnclosedInlineTableEndsAtTheNextSection(t *testing.T) {
	malformed := `[packages]
black = {version = "==24.0"

[dev-packages]
pytest = "*"

[packages]
requests = "*"
`
	names, err := parsePipfileDeps(malformed)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "pipfile inventory after an unclosed table", names, []string{"black", "requests"})

	// The section AFTER the unclosed table has to be one this reader
	// collects, or the assertion holds with or without the escape and
	// proves only that the depth tracking exists.
	poetry := "[tool.poetry.dependencies]\nblack = {version = \"^24.0\"\n\n[tool.poetry.group.dev.dependencies]\nruff = \"^0.4\"\n\n[tool.poetry.dependencies]\nflask = \"^2.0\"\n"
	var got []string
	for _, d := range parsePoetryVersions(poetry) {
		got = append(got, d.Name)
	}
	assertNames(t, "poetry after an unclosed table", got, []string{"black", "flask"})

	// The guard cases: a wrapped ARRAY VALUE is not a section header, so the
	// table it belongs to must stay open (this is the previous round's
	// regression in the other direction). Two shapes, because they fail
	// tomlSectionHeader for DIFFERENT reasons and only one of them exercises
	// the charset guard: `["d"]}` is rejected by the terminator test, while
	// `["d"]` on a line of its own reaches the charset loop and is rejected
	// because a table name cannot contain a quote.
	for _, wrapped := range []string{
		"[packages]\nblack = {version = \"==24.0\", extras =\n    [\"d\"]}\nflask = \"*\"\n",
		"[packages]\nblack = {version = \"==24.0\", extras =\n    [\"d\"]\n}\nflask = \"*\"\n",
	} {
		names2, err := parsePipfileDeps(wrapped)
		if err != nil {
			t.Fatal(err)
		}
		assertNames(t, "pipfile inventory with a wrapped array value", names2, []string{"black", "flask"})
	}
}

// TOML allows a comment after a TABLE HEADER, and every reader compared the
// header byte-for-byte, so `[tool.poetry.dependencies]  # runtime deps` did
// not match and the whole section was skipped. The name inventory and the
// libyear reader disagreed about the same file (the inventory reads Poetry
// through scanTOMLDepTables, which strips the comment), and for the other
// sections both sides dropped everything (v0.29.57).
func TestSectionHeadersMayCarryAComment(t *testing.T) {
	poetry := "[tool.poetry.dependencies]  # runtime deps\nblack = \"^24.0\"\n"
	var got []string
	for _, d := range parsePoetryVersions(poetry) {
		got = append(got, d.Name)
	}
	assertNames(t, "poetry versions", got, []string{"black"})

	pep621 := "[project]  # the project\ndependencies = [\"flask==2.0\"]\n"
	assertDeps(t, "pep621 versions", parsePEP621Versions(pep621), []string{"flask@2.0"})
	names, err := parsePyprojectDeps(pep621)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "pep621 inventory", names, []string{"flask"})

	pipfile := "[packages]   # runtime\nflask = \"==2.0\"\n"
	names2, err := parsePipfileDeps(pipfile)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "pipfile inventory", names2, []string{"flask"})

	groups := "[dependency-groups]  # PEP 735\ntest = [\"pytest>=7\"]\n"
	var got2 []string
	for _, d := range parsePyprojectDevBuildVersions(groups) {
		got2 = append(got2, d.Name)
	}
	assertNames(t, "dependency groups", got2, []string{"pytest"})
}

// scanTOMLDepTables tracks inline-table depth too — it is the reader the
// dependency NAME inventory uses for Poetry and for every Cargo table — so it
// needs the same escape as the four line readers: a table that never closes
// ends at the next section header instead of swallowing the file.
func TestScannerRecoversFromAnUnclosedTable(t *testing.T) {
	cargo := "[dependencies]\nbroken = {version = \"1.0\"\n\n[dev-dependencies]\ntokio = \"1\"\n"
	assertNames(t, "cargo dev-dependencies after an unclosed table",
		parseTOMLDeps(cargo, "[dev-dependencies]"), []string{"tokio"})

	poetry := "[tool.poetry.dependencies]\nbroken = {version = \"^1.0\"\n\n[tool.poetry.group.dev.dependencies]\nruff = \"^0.4\"\n"
	names, err := parsePyprojectDeps(poetry)
	if err != nil {
		t.Fatal(err)
	}
	// broken is declared, ruff is in a different section this reader does not
	// collect; what matters is that the walk did not run off the end.
	assertNames(t, "poetry inventory after an unclosed table", names, []string{"broken"})
}

// The quote scanners in this package must agree about what a string is, or
// one of them sees a bracket the others do not. A backslash escapes the next
// byte inside a double-quoted TOML string, so `"a \" { b"` contains no
// structure at all — but the bracket counters did not honour escapes, so a
// balanced line counted an unclosed brace and the depth tracking then
// swallowed the rest of the file. (Of the scanners that existed before this
// release, only the Ruby comment stripper got it right; unifying them adopted
// its rule.)
func TestQuoteScannersAgreeAboutEscapes(t *testing.T) {
	// Each scanner needs a fixture carrying the byte IT looks for, or the
	// assertion passes under any rule: a line with no `[` tells you nothing
	// about listBrackets, and a line with no `#` nothing about the stripper.
	line := `foo = { version = "1.0", note = "a \" { b" }`
	if got := bracketDelta(line); got != 0 {
		t.Errorf("bracketDelta(%s) = %d, want 0 — the brace is inside an escaped-quote string", line, got)
	}
	const withBracket = `x = "a \" [ b"`
	if opens, closes := listBrackets(withBracket); opens || closes {
		t.Errorf("listBrackets(%s) = (%v, %v), want (false, false) — the bracket is inside the string", withBracket, opens, closes)
	}
	const withHash = `note = "a \" # b"`
	if got := stripHashComment(withHash); got != withHash {
		t.Errorf("stripHashComment(%s) = %q, want it unchanged — the # is inside the string", withHash, got)
	}
	cargo := "[dependencies]\n" + line + "\nserde = \"1.0\"\n"
	assertNames(t, "cargo after an escaped-quote line",
		parseTOMLDeps(cargo, "[dependencies]"), []string{"foo", "serde"})
}

// XML manifests comment with `<!-- … -->`, which spans lines, and the eight
// readers of pom.xml, *.csproj, packages.config and Directory.Packages.props
// read straight through it — so a dependency commented out during debugging
// was inventoried, resolved against its registry and scanned for
// vulnerabilities, at a version the repository does not use. This is the same
// rule the `#` and `//` grammars got in v0.29.57 ("a declaration someone
// commented out is not one"), applied to the grammar where the comment is a
// BLOCK: the strip runs over the whole document, before any reader sees a
// line, because the opening and closing markers need not share one.
func TestXMLCommentsAreNotDeclarations(t *testing.T) {
	pom := `<project><dependencies>
<dependency>
<groupId>g</groupId>
<artifactId>live</artifactId>
<version>1.0</version>
</dependency>
<!--
<dependency>
<groupId>g</groupId>
<artifactId>dead</artifactId>
<version>9.9</version>
</dependency>
-->
</dependencies></project>
`
	assertNames(t, "pom.xml inventory", parsePomXML(pom), []string{"live"})
	assertDeps(t, "pom.xml versions", parsePomXMLVersions(pom), []string{"g:live@1.0"})

	csproj := "<Project>\n<PackageReference Include=\"Live\" Version=\"1.0\" />\n<!-- <PackageReference Include=\"Dead\" Version=\"9.9\" /> -->\n</Project>\n"
	names, err := parseCsprojDeps(csproj)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "csproj inventory", names, []string{"Live"})
	assertDeps(t, "csproj versions", parseCsprojVersions(csproj), []string{"Live@1.0"})

	cfg := "<packages>\n<package id=\"Live\" version=\"1.0\" />\n<!--\n<package id=\"Dead\" version=\"9.9\" />\n-->\n</packages>\n"
	assertNames(t, "packages.config inventory", parseNuGetPackagesConfig(cfg), []string{"Live"})
	assertDeps(t, "packages.config versions", parseNuGetPackagesConfigVersions(cfg), []string{"Live@1.0"})

	props := "<Project>\n<PackageVersion Include=\"Live\" Version=\"1.0\" />\n<!-- <PackageVersion Include=\"Dead\" Version=\"9.9\" /> -->\n</Project>\n"
	names2, err := parseDirectoryPackagesProps(props)
	if err != nil {
		t.Fatal(err)
	}
	assertNames(t, "Directory.Packages.props inventory", names2, []string{"Live"})
	assertDeps(t, "Directory.Packages.props versions", parseDirectoryPackagesPropsVersions(props), []string{"Live@1.0"})
}

// extractXMLValue took the index of the opening tag and of the closing tag
// and sliced between them without checking their ORDER, so a document whose
// closing tag precedes its opening one — `</groupId><groupId>` — panicked on
// a slice with bounds out of range. A panic in a manifest parser kills the
// analysis phase for the whole repository, which is why FuzzManifestParsers
// exists; this one survived because `parsePomXMLVersions` was never in it.
// Adding the reader found it within seconds, and the fuzzer wrote its
// reproducer to testdata/fuzz/FuzzManifestParsers/817d3f397ff2bafe — a NEW
// corpus entry, not an old one that had been sitting there (v0.29.57).
func TestXMLValueSurvivesTagsInTheWrongOrder(t *testing.T) {
	for _, content := range []string{
		"</groupId><groupId>",
		"</version><version>1.0",
		"<project><properties></a><a></properties></project>",
	} {
		// The contract is the fuzz contract: never panic. A wrong answer is
		// a different matter; there is no right answer for malformed XML.
		_ = parsePomXMLVersions(content)
		_ = parsePomXML(content)
	}
	if got := extractXMLValue("</groupId><groupId>", "groupId"); got != "" {
		t.Errorf("extractXMLValue on a closing-before-opening document = %q, want \"\"", got)
	}
	if got := extractXMLValue("<version>1.0</version>", "version"); got != "1.0" {
		t.Errorf("extractXMLValue = %q, want 1.0 — the ordinary case must still work", got)
	}
}

// strings.ToLower can CHANGE A STRING'S LENGTH — an invalid UTF-8 byte
// becomes a 3-byte replacement rune, and the Kelvin sign becomes one byte —
// so an index found in the lowered copy is not an index into the original.
// packages.config is matched case-insensitively (older .NET projects write
// Id= and Version=) and then sliced the ORIGINAL at that index, which
// panicked on `<package \xb1version="` and took the whole analysis phase of
// that repository with it (v0.29.57, found by the fuzz target within seconds
// of the reader being registered in it).
func TestCaseInsensitiveAttributesSurviveInvalidUTF8(t *testing.T) {
	for _, content := range []string{
		"<package \xb1version=\"",
		"<package \xb1Id=\"x\" Version=\"1.0\" />",
		"<package Kid=\"x\" version=\"1.0\" />",
	} {
		// The contract is the fuzz contract: never panic on a document a
		// repository can carry.
		_ = parseNuGetPackagesConfigVersions(content)
		_ = parseNuGetPackagesConfig(content)
	}
	// The case-insensitivity it exists for must still work.
	ok := "<packages>\n<package Id=\"Live\" Version=\"1.0\" />\n</packages>\n"
	assertDeps(t, "packages.config mixed case", parseNuGetPackagesConfigVersions(ok), []string{"Live@1.0"})
}
