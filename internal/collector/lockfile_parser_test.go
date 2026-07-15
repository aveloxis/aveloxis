// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.11 — lockfile parser tests. Fixture-driven (committed realistic
// lockfiles in testdata/lockfiles/) plus malformed/empty/truncated
// cases. Parsers are best-effort: a broken lockfile returns an error
// that the analysis walk logs and skips — it must never panic and
// never fail the analysis phase.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readLockFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "lockfiles", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// entryMap flattens entries to "name@version" → Direct for assertions.
func entryMap(res *LockfileResult) map[string]bool {
	out := map[string]bool{}
	for _, e := range res.Entries {
		out[e.Name+"@"+e.Version] = e.Direct
	}
	return out
}

func parseFixture(t *testing.T, kind, fixture string) *LockfileResult {
	t.Helper()
	res, err := ParseLockfile(kind, readLockFixture(t, fixture))
	if err != nil {
		t.Fatalf("ParseLockfile(%s, %s): %v", kind, fixture, err)
	}
	return res
}

func TestParsePackageLockV1(t *testing.T) {
	res := parseFixture(t, "package-lock.json", "package-lock.v1.json")
	if res.Ecosystem != "npm" || res.DirectKnown {
		t.Errorf("v1 lockfile: ecosystem=npm, direct NOT distinguished; got %+v", res)
	}
	m := entryMap(res)
	for _, want := range []string{"express@4.19.2", "body-parser@1.20.2", "jest@29.7.0", "nested-dep@2.0.1"} {
		if _, ok := m[want]; !ok {
			t.Errorf("v1 must include %s (nested deps walked); got %v", want, m)
		}
	}
}

func TestParsePackageLockV3(t *testing.T) {
	res := parseFixture(t, "package-lock.json", "package-lock.v3.json")
	if !res.DirectKnown {
		t.Fatal("v2/v3 lockfiles distinguish direct deps via the root packages[\"\"] entry")
	}
	m := entryMap(res)
	// Names come from the path AFTER the last node_modules/ (scoped +
	// nested paths included); the root "" entry and link:true entries
	// are not packages.
	for name, wantDirect := range map[string]bool{
		"express@4.19.2":     true,
		"@scope/pkg@1.2.5":   true,
		"jest@29.7.0":        true, // devDependency of the root — still direct
		"body-parser@1.20.2": false,
		"cookie@0.6.0":       false, // nested node_modules path
	} {
		direct, ok := m[name]
		if !ok {
			t.Errorf("missing entry %s in %v", name, m)
			continue
		}
		if direct != wantDirect {
			t.Errorf("%s: direct=%v, want %v", name, direct, wantDirect)
		}
	}
	if _, ok := m["linked-thing@"]; ok {
		t.Error("link:true workspace entries are not resolved packages")
	}
	if _, ok := m["fixture-app@1.0.0"]; ok {
		t.Error("the root \"\" entry is the project itself, not a dependency")
	}
}

func TestParseYarnLockV1(t *testing.T) {
	res := parseFixture(t, "yarn.lock", "yarn.lock")
	if res.Ecosystem != "npm" || res.DirectKnown {
		t.Errorf("yarn v1 does not distinguish direct; got %+v", res)
	}
	m := entryMap(res)
	for _, want := range []string{"@babel/core@7.23.9", "express@4.19.2", "lodash@4.17.21"} {
		if _, ok := m[want]; !ok {
			t.Errorf("yarn.lock must include %s (scoped names + multi-key blocks); got %v", want, m)
		}
	}
	if len(res.Entries) != 3 {
		t.Errorf("multi-range keys resolve to ONE entry per block, got %d: %v", len(res.Entries), m)
	}
}

func TestParsePnpmLock(t *testing.T) {
	res := parseFixture(t, "pnpm-lock.yaml", "pnpm-lock.yaml")
	if !res.DirectKnown {
		t.Fatal("pnpm importers section distinguishes direct deps")
	}
	m := entryMap(res)
	for name, wantDirect := range map[string]bool{
		"express@4.19.2":     true,
		"@scope/pkg@1.2.5":   true,
		"typescript@5.4.2":   true,
		"body-parser@1.20.2": false,
	} {
		direct, ok := m[name]
		if !ok {
			t.Errorf("missing entry %s in %v", name, m)
			continue
		}
		if direct != wantDirect {
			t.Errorf("%s: direct=%v, want %v", name, direct, wantDirect)
		}
	}
}

func TestParseBunLockJSONC(t *testing.T) {
	res := parseFixture(t, "bun.lock", "bun.lock")
	if res.Ecosystem != "npm" || !res.DirectKnown {
		t.Errorf("bun.lock (text JSONC) carries workspace direct deps; got %+v", res)
	}
	m := entryMap(res)
	for name, wantDirect := range map[string]bool{
		"express@4.19.2":     true,
		"typescript@5.4.2":   true,
		"body-parser@1.20.2": false,
		"@scope/pkg@1.2.5":   false,
	} {
		direct, ok := m[name]
		if !ok {
			t.Errorf("missing entry %s (JSONC comments + trailing commas must parse) in %v", name, m)
			continue
		}
		if direct != wantDirect {
			t.Errorf("%s: direct=%v, want %v", name, direct, wantDirect)
		}
	}
}

// bun.lockb is BINARY: detected for the inventory (kind marker, zero
// entries), never parsed.
func TestBunLockbDetectedNeverParsed(t *testing.T) {
	res, err := ParseLockfile("bun.lockb", readLockFixture(t, "bun.lockb"))
	if err != nil {
		t.Fatalf("bun.lockb detection must not error: %v", err)
	}
	if res.Kind != "bun.lockb" || res.Ecosystem != "npm" {
		t.Errorf("bun.lockb inventory marker wrong: %+v", res)
	}
	if len(res.Entries) != 0 {
		t.Errorf("bun.lockb is binary — zero entries, got %d", len(res.Entries))
	}
}

func TestParsePoetryLock(t *testing.T) {
	res := parseFixture(t, "poetry.lock", "poetry.lock")
	if res.Ecosystem != "pypi" || res.DirectKnown {
		t.Errorf("poetry.lock: pypi, direct not distinguished; got %+v", res)
	}
	m := entryMap(res)
	for _, want := range []string{"flask@2.3.3", "werkzeug@3.0.1", "pytest@7.4.4"} {
		if _, ok := m[want]; !ok {
			t.Errorf("poetry.lock must include %s; got %v", want, m)
		}
	}
}

func TestParsePipfileLock(t *testing.T) {
	res := parseFixture(t, "Pipfile.lock", "Pipfile.lock")
	m := entryMap(res)
	// "==2.3.3" version pins are stripped to the bare version.
	for _, want := range []string{"flask@2.3.3", "werkzeug@3.0.1", "pytest@7.4.4"} {
		if _, ok := m[want]; !ok {
			t.Errorf("Pipfile.lock must include %s (== stripped, default+develop sections); got %v", want, m)
		}
	}
}

func TestParseUvLock(t *testing.T) {
	res := parseFixture(t, "uv.lock", "uv.lock")
	m := entryMap(res)
	for _, want := range []string{"flask@2.3.3", "werkzeug@3.0.1"} {
		if _, ok := m[want]; !ok {
			t.Errorf("uv.lock must include %s; got %v", want, m)
		}
	}
}

func TestParsePdmLock(t *testing.T) {
	res := parseFixture(t, "pdm.lock", "pdm.lock")
	m := entryMap(res)
	for _, want := range []string{"flask@2.3.3", "werkzeug@3.0.1"} {
		if _, ok := m[want]; !ok {
			t.Errorf("pdm.lock must include %s; got %v", want, m)
		}
	}
}

func TestParseCargoLock(t *testing.T) {
	res := parseFixture(t, "Cargo.lock", "Cargo.lock")
	if res.Ecosystem != "cargo" {
		t.Errorf("Cargo.lock ecosystem: %s", res.Ecosystem)
	}
	m := entryMap(res)
	for _, want := range []string{"serde@1.0.188", "serde_derive@1.0.188", "tokio@1.35.1"} {
		if _, ok := m[want]; !ok {
			t.Errorf("Cargo.lock must include %s; got %v", want, m)
		}
	}
}

func TestParseGemfileLock(t *testing.T) {
	res := parseFixture(t, "Gemfile.lock", "Gemfile.lock")
	if res.Ecosystem != "rubygems" || !res.DirectKnown {
		t.Errorf("Gemfile.lock has a DEPENDENCIES section — direct known; got %+v", res)
	}
	m := entryMap(res)
	for name, wantDirect := range map[string]bool{
		"rails@7.1.2":                  true,
		"nokogiri@1.15.5-x86_64-linux": true,
		"actioncable@7.1.2":            false,
		"concurrent-ruby@1.2.2":        false,
		"racc@1.7.3":                   false,
	} {
		direct, ok := m[name]
		if !ok {
			t.Errorf("missing entry %s in %v", name, m)
			continue
		}
		if direct != wantDirect {
			t.Errorf("%s: direct=%v, want %v", name, direct, wantDirect)
		}
	}
	// Sub-dependency requirement lines ("actionpack (= 7.1.2)" at
	// 6-space indent) are constraints, not resolutions.
	if _, ok := m["actionpack@= 7.1.2"]; ok {
		t.Error("6-space-indented constraint lines must not become entries")
	}
}

func TestParseComposerLock(t *testing.T) {
	res := parseFixture(t, "composer.lock", "composer.lock")
	if res.Ecosystem != "packagist" {
		t.Errorf("composer.lock ecosystem: %s", res.Ecosystem)
	}
	m := entryMap(res)
	for _, want := range []string{"monolog/monolog@2.9.2", "guzzlehttp/guzzle@7.8.1", "phpunit/phpunit@10.5.5"} {
		if _, ok := m[want]; !ok {
			t.Errorf("composer.lock must include %s (leading v stripped, dev included); got %v", want, m)
		}
	}
}

func TestParseMixLock(t *testing.T) {
	res := parseFixture(t, "mix.lock", "mix.lock")
	if res.Ecosystem != "hex" {
		t.Errorf("mix.lock ecosystem: %s", res.Ecosystem)
	}
	m := entryMap(res)
	for _, want := range []string{"phoenix@1.7.10", "plug@1.15.2"} {
		if _, ok := m[want]; !ok {
			t.Errorf("mix.lock must include %s; got %v", want, m)
		}
	}
	if _, ok := m["git_dep@abc123"]; ok {
		t.Error(":git entries have no registry version — skip them")
	}
}

func TestParsePubspecLock(t *testing.T) {
	res := parseFixture(t, "pubspec.lock", "pubspec.lock")
	if res.Ecosystem != "pub" || !res.DirectKnown {
		t.Errorf("pubspec.lock marks direct/transitive; got %+v", res)
	}
	m := entryMap(res)
	for name, wantDirect := range map[string]bool{
		"http@1.1.2":  true,
		"test@1.24.9": true, // "direct dev" is still direct
		"meta@1.11.0": false,
	} {
		direct, ok := m[name]
		if !ok {
			t.Errorf("missing entry %s in %v", name, m)
			continue
		}
		if direct != wantDirect {
			t.Errorf("%s: direct=%v, want %v", name, direct, wantDirect)
		}
	}
}

func TestParsePackageResolved(t *testing.T) {
	for fixture, want := range map[string]string{
		"Package.resolved.v2": "alamofire@5.8.1",
		"Package.resolved.v1": "Alamofire@5.6.4",
	} {
		res, err := ParseLockfile("Package.resolved", readLockFixture(t, fixture))
		if err != nil {
			t.Fatalf("%s: %v", fixture, err)
		}
		if res.Ecosystem != "swiftpm" {
			t.Errorf("%s ecosystem: %s", fixture, res.Ecosystem)
		}
		if _, ok := entryMap(res)[want]; !ok {
			t.Errorf("%s must include %s; got %v", fixture, want, entryMap(res))
		}
	}
}

func TestParseDotnetPackagesLock(t *testing.T) {
	res := parseFixture(t, "packages.lock.json", "packages.lock.json")
	if res.Ecosystem != "nuget" || !res.DirectKnown {
		t.Errorf("packages.lock.json marks Direct/Transitive; got %+v", res)
	}
	m := entryMap(res)
	if direct, ok := m["Newtonsoft.Json@13.0.3"]; !ok || !direct {
		t.Errorf("Newtonsoft.Json must be a single Direct entry (deduped across frameworks); got %v", m)
	}
	if direct, ok := m["System.Text.Encodings.Web@8.0.0"]; !ok || direct {
		t.Errorf("transitive entry wrong: %v", m)
	}
	if len(res.Entries) != 2 {
		t.Errorf("entries must dedupe across target frameworks, got %d", len(res.Entries))
	}
}

func TestParseGradleLockfile(t *testing.T) {
	res := parseFixture(t, "gradle.lockfile", "gradle.lockfile")
	if res.Ecosystem != "maven" {
		t.Errorf("gradle.lockfile ecosystem: %s", res.Ecosystem)
	}
	m := entryMap(res)
	for _, want := range []string{
		"org.springframework:spring-core@5.3.31",
		"com.google.guava:guava@32.1.3-jre",
	} {
		if _, ok := m[want]; !ok {
			t.Errorf("gradle.lockfile must include %s (group:artifact names match the pom/gradle manifest parsers); got %v", want, m)
		}
	}
	for k := range m {
		if strings.HasPrefix(k, "empty@") {
			t.Error("the empty= configurations line is not a package")
		}
	}
}

func TestParseStackYamlLock(t *testing.T) {
	res := parseFixture(t, "stack.yaml.lock", "stack.yaml.lock")
	if res.Ecosystem != "hackage" {
		t.Errorf("stack.yaml.lock ecosystem: %s", res.Ecosystem)
	}
	m := entryMap(res)
	for _, want := range []string{"aeson@2.1.2.1", "http-client@0.7.16"} {
		if _, ok := m[want]; !ok {
			t.Errorf("stack.yaml.lock must include %s (name-version split at the last dash before digits); got %v", want, m)
		}
	}
}

// ---------- malformed / empty / truncated ----------

func TestParserErrorsAreErrorsNotPanics(t *testing.T) {
	cases := []struct{ kind, fixture string }{
		{"package-lock.json", "truncated.package-lock.json"},
		{"pnpm-lock.yaml", "malformed.pnpm-lock.yaml"},
	}
	for _, c := range cases {
		if _, err := ParseLockfile(c.kind, readLockFixture(t, c.fixture)); err == nil {
			t.Errorf("%s on %s must return an error (walk logs WARN and skips)", c.kind, c.fixture)
		}
	}
}

func TestParserEmptyInputs(t *testing.T) {
	// An empty poetry.lock parses to zero entries (no error needed —
	// there is nothing malformed about an empty TOML document).
	res, err := ParseLockfile("poetry.lock", readLockFixture(t, "empty.poetry.lock"))
	if err != nil {
		t.Fatalf("empty poetry.lock: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("empty lockfile → zero entries, got %d", len(res.Entries))
	}
	// Every text parser must survive empty + garbage input without
	// panicking, whatever it returns.
	for kind := range lockfileKinds {
		_, _ = ParseLockfile(kind, nil)
		_, _ = ParseLockfile(kind, []byte("\x00\x01garbage{{{"))
	}
}

// Unknown filenames are not lockfiles.
func TestParseLockfileRejectsUnknownKinds(t *testing.T) {
	if _, err := ParseLockfile("definitely-not-a-lockfile.txt", []byte("{}")); err == nil {
		t.Error("unknown kinds must be rejected")
	}
}

// ---------- negative tripwires (operator rulings) ----------

// requirements.txt is NEVER a lockfile — even fully hash-pinned
// ("it usually includes all the same ambiguities and is often their
// source"). Its == pins classify 'exact' per-finding, but it must
// never appear in the lockfile roster or contribute to certainty.
func TestRequirementsTxtIsNeverALockfile(t *testing.T) {
	if _, ok := lockfileKinds["requirements.txt"]; ok {
		t.Fatal("requirements.txt must NOT be in the lockfile roster (operator ruling 2026-07-15)")
	}
	if _, err := ParseLockfile("requirements.txt", []byte("flask==2.3.3")); err == nil {
		t.Fatal("ParseLockfile must reject requirements.txt")
	}
}

// go.sum is not a lockfile either: go.mod versions are exact under
// MVS, so Go deps classify 'locked' by construction without parsing
// anything (see TestGoDepsAreLockedByConstruction).
func TestGoSumIsNotInTheLockfileRoster(t *testing.T) {
	for _, name := range []string{"go.sum", "go.mod"} {
		if _, ok := lockfileKinds[name]; ok {
			t.Errorf("%s must not be in the lockfile roster — Go is locked by construction", name)
		}
	}
}
