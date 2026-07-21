// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.30 — REAL-BINARY tool canaries (audit G1/G8/G9, the "next
// silent-dark incident generator"). The three analysis tools install
// unpinned and auto-upgrade monthly; every parse test before this fed
// fixtures authored from our own structs, so a tool renaming its JSON
// keys (ScanCode has done exactly that: license_expressions →
// detected_license_expression*) parses to ZERO findings with no error
// — the npm/cargo failure mode with an automatic recurring trigger.
//
// These canaries run the INSTALLED binaries on tiny fixtures and
// unmarshal through the REAL parse structs. The version allowlists
// turn "silent shape drift after an auto-upgrade" into "loud canary
// failure naming the unvalidated version": when a new version appears,
// a human eyeballs the parsed output (the assertions here) and adds
// the version to the list — that eyeball IS the validation.
//
// Gated on AVELOXIS_TEST_TOOLS=1 (needs the binaries installed) —
// wired into network-canary.yml's weekly tools job.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Validated tool versions. EXTEND (don't replace) after eyeballing a
// green run on the new version; the git history of these lists is the
// validation record.
var (
	scancodeValidatedVersions = map[string]bool{
		"32.5.0": true, // 2026-07-21: fleet version, canary authored against it
	}
	sccValidatedMajors = map[string]bool{
		"3": true, // scc 3.x JSON shape (Files/Lines/Code/Complexity)
	}
)

func skipUnlessTools(t *testing.T, binary string) {
	t.Helper()
	if os.Getenv("AVELOXIS_TEST_TOOLS") != "1" {
		t.Skip("tool canary: set AVELOXIS_TEST_TOOLS=1 to run")
	}
	if _, err := exec.LookPath(binary); err != nil {
		t.Skipf("%s not installed", binary)
	}
}

const mitHeader = `// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Canary Fixture Authors
package fixture
`

func TestLiveScancodeOutputShape(t *testing.T) {
	skipUnlessTools(t, "scancode")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(mitHeader), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.json")
	cmd := exec.Command("scancode", "-clpi", "--only-findings", "--json", out, "--quiet", dir)
	if b, err := cmd.CombinedOutput(); err != nil {
		// v0.23.4: scancode exits 1 on per-file errors with valid JSON;
		// only fail when there is no parseable output at all.
		if _, statErr := os.Stat(out); statErr != nil {
			t.Fatalf("scancode run failed with no output: %v\n%s", err, b)
		}
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	// THE REAL PARSE STRUCT — the same one ingestScancodeOutput uses.
	var raw scancodeOutput
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("installed scancode's JSON no longer parses into our struct: %v", err)
	}
	if len(raw.Headers) == 0 || raw.Headers[0].ToolVersion == "" {
		t.Fatal("headers[0].tool_version empty — the header shape drifted")
	}
	ver := raw.Headers[0].ToolVersion
	if !scancodeValidatedVersions[ver] {
		t.Errorf("scancode %s is NOT in the validated allowlist — the monthly auto-upgrade delivered an unvalidated version. Eyeball this canary's other assertions on a green run, then add %q to scancodeValidatedVersions (the git history of the list is the validation record).", ver, ver)
	}
	var sawMIT bool
	for _, f := range raw.Files {
		if strings.Contains(strings.ToLower(f.DetectedLicenseExpressionSPDX), "mit") {
			sawMIT = true
		}
	}
	if !sawMIT {
		t.Error("an MIT-headed file produced no MIT detection — the per-file license keys drifted (the zero-rows-without-error mode)")
	}
}

func TestLiveSCCOutputShape(t *testing.T) {
	skipUnlessTools(t, "scc")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(mitHeader+"\nfunc F() int { return 1 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := exec.Command("scc", "-f", "json", "--by-file", dir).Output()
	if err != nil {
		t.Fatalf("scc run failed: %v", err)
	}
	var langs []sccLanguage
	if err := json.Unmarshal(b, &langs); err != nil {
		t.Fatalf("installed scc's JSON no longer parses into our struct: %v", err)
	}
	var sawGo bool
	for _, l := range langs {
		if l.Name == "Go" && len(l.Files) > 0 && l.Files[0].Lines > 0 {
			sawGo = true
		}
	}
	if !sawGo {
		t.Error("scc parsed to zero Go lines for a real Go file — shape drift")
	}
	if v, err := exec.Command("scc", "--version").Output(); err == nil {
		fields := strings.Fields(string(v))
		if len(fields) >= 3 {
			major, _, _ := strings.Cut(strings.TrimPrefix(fields[2], "v"), ".")
			if !sccValidatedMajors[major] {
				t.Errorf("scc major version %s not in the validated set — eyeball a green run and extend sccValidatedMajors", major)
			}
		}
	}
}

func TestLiveScorecardOutputShape(t *testing.T) {
	skipUnlessTools(t, "scorecard")
	skipUnlessTools(t, "git")
	// Scorecard --local needs a real git repo; a minimal one suffices.
	dir := t.TempDir()
	run := func(args ...string) {
		c := exec.Command(args[0], args[1:]...)
		c.Dir = dir
		c.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=c", "GIT_AUTHOR_EMAIL=c@x", "GIT_COMMITTER_NAME=c", "GIT_COMMITTER_EMAIL=c@x")
		if b, err := c.CombinedOutput(); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, b)
		}
	}
	run("git", "init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "LICENSE"), []byte("MIT License\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("git", "add", ".")
	run("git", "commit", "-q", "-m", "fixture")

	cmd := exec.Command("scorecard", "--local", dir, "--format", "json")
	b, _ := cmd.Output() // scorecard exits 1 on failed checks; JSON still valid
	var raw scorecardOutput
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("installed scorecard's JSON no longer parses into our struct: %v\n%s", err, b)
	}
	if len(raw.Checks) == 0 {
		t.Fatal("scorecard parsed to zero checks — the checks[] shape drifted (zero-rows-without-error mode)")
	}
	var named bool
	for _, c := range raw.Checks {
		if c.Name != "" {
			named = true
		}
	}
	if !named {
		t.Error("every parsed check has an empty name — the check-name key drifted")
	}
}
