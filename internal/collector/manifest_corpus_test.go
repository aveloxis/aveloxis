// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.72 — the manifest-corpus regression net (the routine-testing
// hardening from the zephyr malformed-version incident).
//
// testdata/manifest_corpus/ holds one small fixture per manifest kind,
// each carrying BOTH legitimate declarations and the garbage corners
// production taught us about (hash-pinned continuations, cargo inline
// tables, gradle project() refs, ${}/$() interpolation, workspace:/
// catalog: protocols, pubspec block sub-keys, keyword options).
//
// Two nets, run on every `go test ./...` (no DB, no network):
//
//  1. TestManifestCorpusGolden — parses every fixture through the SAME
//     parser + normalizeParsedVersion pipeline the analysis walk runs,
//     and compares the full output against golden.json. ANY behavior
//     change in ANY parser — intended or not — shows up as a diff.
//     Intended changes regenerate the golden:
//
//        AVELOXIS_UPDATE_GOLDEN=1 go test ./internal/collector/ -run TestManifestCorpusGolden
//
//     then REVIEW THE GOLDEN DIFF before committing — the golden diff
//     IS the regression review.
//
//  2. TestManifestCorpusVersionHygiene — the invariant that survives
//     careless golden regeneration: every version the pipeline emits
//     is either "" (unpinned) or passes versionGateOK. A parser
//     change that leaks garbage fails HERE even if someone blindly
//     regenerated the golden.
//
// Extending coverage = drop a fixture file + add one dispatch line +
// regenerate the golden.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const corpusDir = "testdata/manifest_corpus"

// corpusDep is the golden-file shape: the fields of libyearDep that
// constitute parser output contract (Requirement included — it is the
// raw-manifest display truth the GUI renders).
type corpusDep struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Requirement string `json:"requirement"`
	Type        string `json:"type"`
	Manager     string `json:"manager"`
}

// parseCorpusFile dispatches a fixture to the same parser the analysis
// walk uses for that filename, then applies the central normalizer
// exactly as the walk does.
func parseCorpusFile(t *testing.T, name string) []libyearDep {
	t.Helper()
	path := filepath.Join(corpusDir, name)
	content := func() string {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(data)
	}
	var deps []libyearDep
	switch name {
	case "requirements.txt":
		deps = parseRequirementsTxtVersions(path)
	case "package.json":
		var err error
		deps, err = parsePackageJSONVersions(path)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
	case "Cargo.toml":
		deps = parseCargoVersions(path)
	case "Gemfile":
		deps = parseGemfileVersions(path)
	case "go.mod":
		deps = parseGoModVersions(path)
	case "composer.json":
		var err error
		deps, err = parseComposerJSONVersions(path)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
	case "pom.xml":
		deps = parsePomXMLVersions(content())
	case "build.gradle":
		deps = parseBuildGradleVersions(content())
	case "mix.exs":
		deps = parseMixExsVersions(content())
	case "pubspec.yaml":
		deps = parsePubspecVersions(content())
	case "packages.config":
		deps = parseNuGetPackagesConfigVersions(content())
	case "example.csproj":
		deps = parseCsprojVersions(content())
	case "Pipfile":
		deps = parsePipfileVersions(content())
	case "pyproject.toml":
		deps = parsePyprojectVersionsFromContent(content())
	case "build.sbt":
		deps = parseBuildSbtVersions(content())
	case "Package.swift":
		deps = parsePackageSwiftVersions(content())
	case "package.yaml":
		deps = parseHaskellPackageYamlVersions(content())
	case "workflow.yml":
		deps = parseWorkflowUses(content())
	default:
		t.Fatalf("no parser dispatch for corpus fixture %q — add it to parseCorpusFile", name)
	}
	// The walk's central hygiene layer (v0.27.71).
	for i := range deps {
		deps[i].Version = normalizeParsedVersion(deps[i].Manager, deps[i].Version)
	}
	return deps
}

func corpusFixtures(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(corpusDir)
	if err != nil {
		t.Fatalf("corpus dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || e.Name() == "golden.json" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if len(names) < 15 {
		t.Fatalf("corpus has only %d fixtures — the regression net is supposed to cover every ecosystem (self-check against silent fixture loss)", len(names))
	}
	return names
}

func TestManifestCorpusGolden(t *testing.T) {
	got := map[string][]corpusDep{}
	for _, name := range corpusFixtures(t) {
		deps := parseCorpusFile(t, name)
		out := make([]corpusDep, 0, len(deps))
		for _, d := range deps {
			out = append(out, corpusDep(d))
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Name != out[j].Name {
				return out[i].Name < out[j].Name
			}
			return out[i].Requirement < out[j].Requirement
		})
		got[name] = out
	}

	goldenPath := filepath.Join(corpusDir, "golden.json")
	if os.Getenv("AVELOXIS_UPDATE_GOLDEN") == "1" {
		blob, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(blob, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("golden regenerated at %s — review the diff before committing", goldenPath)
		return
	}

	blob, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("golden missing (%v) — generate with AVELOXIS_UPDATE_GOLDEN=1", err)
	}
	var want map[string][]corpusDep
	if err := json.Unmarshal(blob, &want); err != nil {
		t.Fatalf("golden unreadable: %v", err)
	}

	for name, wantDeps := range want {
		gotDeps, ok := got[name]
		if !ok {
			t.Errorf("%s: fixture in golden but not parsed — dispatch or fixture removed?", name)
			continue
		}
		if len(gotDeps) != len(wantDeps) {
			t.Errorf("%s: %d deps, golden has %d\n got: %+v\nwant: %+v", name, len(gotDeps), len(wantDeps), gotDeps, wantDeps)
			continue
		}
		for i := range wantDeps {
			if gotDeps[i] != wantDeps[i] {
				t.Errorf("%s[%d]:\n got: %+v\nwant: %+v\n(intended change? regenerate with AVELOXIS_UPDATE_GOLDEN=1 and REVIEW the diff)", name, i, gotDeps[i], wantDeps[i])
			}
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s: new fixture has no golden entry — regenerate with AVELOXIS_UPDATE_GOLDEN=1", name)
		}
	}
}

// The golden-independent invariant: nothing the parser+normalizer
// pipeline emits may carry a garbage version. This is what fails when
// someone regenerates the golden without looking.
func TestManifestCorpusVersionHygiene(t *testing.T) {
	for _, name := range corpusFixtures(t) {
		for _, d := range parseCorpusFile(t, name) {
			if d.Manager == "githubactions" {
				continue // refs are refs (v0.27.47), exempt by design
			}
			if d.Version != "" && !versionGateOK(d.Version) {
				t.Errorf("%s: dep %s emitted garbage version %q — the parser+normalizer pipeline must only produce \"\" or gate-valid versions", name, d.Name, d.Version)
			}
			if d.Name == "" {
				t.Errorf("%s: dep with empty name (version %q)", name, d.Version)
			}
		}
	}
}
