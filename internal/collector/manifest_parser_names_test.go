// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// manifest_parser_names_test.go — v0.29.56. Manifest parsers that read
// text which is not a package name, found in the libyear WARN samples of
// the 2026-09-17 production log. Every input below is a trimmed copy of the
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

func depNames(deps []model.LibyearDep) []string {
	var out []string
	for _, d := range deps {
		out = append(out, d.Name+"@"+d.Version)
	}
	sort.Strings(out)
	return out
}

func assertDeps(t *testing.T, label string, got []model.LibyearDep, want []string) {
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
	// oci-spec is `oci-spec.workspace = true`: this lone manifest has no
	// workspace root to say where it comes from, so it is not guessed at
	// (v0.29.71; TestCargoWorkspaceInheritedDepsTakeTheRootsSource covers
	// the root that answers).
	for _, name := range []string{"zenoh", "zenoh-flat", "local-crate", "private", "oci-spec"} {
		if !nonRegistry[name] {
			t.Errorf("%s must be marked non-registry", name)
		}
	}
	for _, name := range []string{"jni", "published-member", "serde"} {
		if nonRegistry[name] {
			t.Errorf("%s is a crates.io dependency and must be looked up", name)
		}
	}
}

// TestNonRegistryDepsAreNeverLookedUpOrStored pins what the NonRegistry
// flag is for. They get no registry call — that was the bug — and no row:
// a purl-less row would enter the vulnerability scan's universe as an
// unscannable dependency, which blocks that repo's scan-complete stamp
// (v0.28.5). The dependency scan still inventories them.
func TestNonRegistryDepsAreNeverLookedUpOrStored(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/analysis.go"),
		"func (ac *AnalysisCollector) scanLibyear("))
	skip := strings.Index(body, "if dep.NonRegistry {")
	resolveCall := strings.Index(body, "resolveLibyearCached(")
	insert := strings.Index(body, "rows = append(rows, lb)")
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
