// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"gopkg.in/yaml.v3"
)

// TestCIWorkflowsExist verifies all required GitHub Actions workflow files are present.
func TestCIWorkflowsExist(t *testing.T) {
	// Find the repo root by walking up from the test directory.
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}

	required := map[string]string{
		"test.yml":            "Go tests on every push",
		"container-build.yml": "Docker/Podman build test on PRs",
		"docker-publish.yml":  "Docker image publish on main push",
		"codeql.yml":          "CodeQL security analysis on PRs",
		"lint.yml":            "Linting checks on PRs",
		"docs.yml":            "Sphinx docs build with warnings-as-errors on PRs",
	}

	for filename, purpose := range required {
		path := filepath.Join(root, ".github", "workflows", filename)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("missing CI workflow %s (%s)", filename, purpose)
		}
	}
}

// TestDockerfileExists verifies the Dockerfile is present for container builds.
func TestDockerfileExists(t *testing.T) {
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}
	path := filepath.Join(root, "Dockerfile")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("missing Dockerfile in repo root")
	}
}

// TestCIBadgesInREADME verifies the README has CI status badges.
func TestCIBadgesInREADME(t *testing.T) {
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}
	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("could not read README.md: %v", err)
	}
	readme := string(data)

	badges := []string{"test.yml", "lint.yml", "codeql.yml", "container-build.yml", "docker-publish.yml"}
	for _, badge := range badges {
		if !strings.Contains(readme, badge) {
			t.Errorf("README.md missing badge for %s", badge)
		}
	}
}

// TestGolangciLintRunsWithoutItsCache — v0.29.65 (PR #212): from
// 2026-09-22 10:34 every golangci-lint run on a PR failed with SA5011
// "possible nil pointer dereference" on test sites guarded by t.Fatal,
// while the same job's plain `staticcheck ./...` step (no cache) passed,
// the same tree linted clean locally (also as linux/amd64 with Go 1.26.0),
// and a docs-only Dependabot PR failed too. The flagged files differed
// from run to run with Go, linter and cache key identical: the restored
// analysis cache had lost the fact that t.Fatal does not return. The step
// takes seconds, so it runs uncached.
func TestGolangciLintRunsWithoutItsCache(t *testing.T) {
	src := srctest.Read(t, ".github/workflows/lint.yml")
	step := src[strings.Index(src, "uses: golangci/golangci-lint-action@"):]
	if end := strings.Index(step, "\n      - "); end > 0 {
		step = step[:end]
	}
	if !strings.Contains(step, "skip-cache: true") {
		t.Error("the golangci-lint step must set skip-cache: true (a stale restored cache produced false SA5011 failures)")
	}
}

// TestOfficialSBOMValidatorsAreScheduled — v0.29.67 (worklist 53): the
// official-validator check skips unless AVELOXIS_TEST_SBOM_TOOLS is set, and
// "an unscheduled canary provides no protection" (network-canary.yml, the
// v0.27.29 audit). The workflow is PARSED (review round 11: substring and
// banned-word checks passed on "needs: tools", "|| true" and a dropped line
// continuation, each of which switches the check off). Asserted:
//   - the workflow keeps its schedule;
//   - the sbom-validators job exists, with no needs / if / continue-on-error
//     (round 9: as steps after the tool canaries it was skipped whenever
//     they failed; "needs: tools" would bring that back);
//   - one step installs both validators;
//   - the test step gets the variable through step-level env (no shell line
//     can drop it), runs exactly the validator test, whose name is taken from
//     the function so a rename cannot leave "-run <old name>" running no
//     tests (round 12), and has no shell, working-directory, if or
//     continue-on-error that could neutralise it; the variable's name is the
//     test's own constant (round 13), and no workflow-level defaults exist.
func TestOfficialSBOMValidatorsAreScheduled(t *testing.T) {
	var wf struct {
		Defaults any `yaml:"defaults"`
		On       struct {
			Schedule []map[string]string `yaml:"schedule"`
		} `yaml:"on"`
		Jobs map[string]struct {
			Needs           any `yaml:"needs"`
			If              any `yaml:"if"`
			ContinueOnError any `yaml:"continue-on-error"`
			Defaults        any `yaml:"defaults"`
			Steps           []struct {
				Name             string            `yaml:"name"`
				If               any               `yaml:"if"`
				ContinueOnError  any               `yaml:"continue-on-error"`
				Shell            any               `yaml:"shell"`
				WorkingDirectory any               `yaml:"working-directory"`
				Env              map[string]string `yaml:"env"`
				Run              string            `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".github/workflows/network-canary.yml")), &wf); err != nil {
		t.Fatalf("network-canary.yml does not parse: %v", err)
	}
	if len(wf.On.Schedule) == 0 {
		t.Error("network-canary.yml must keep its schedule trigger")
	}
	// Workflow-level defaults apply run.shell / working-directory to every
	// job's steps, the job-level rule one level up (review round 13).
	if wf.Defaults != nil {
		t.Errorf("network-canary.yml must have no workflow-level defaults (%v)", wf.Defaults)
	}
	job, ok := wf.Jobs["sbom-validators"]
	if !ok {
		t.Fatal("network-canary.yml must run the SBOM validators in their own job, sbom-validators")
	}
	if job.Needs != nil || job.If != nil || job.ContinueOnError != nil || job.Defaults != nil {
		t.Errorf("the sbom-validators job must have no needs, if, continue-on-error or defaults (needs=%v if=%v continue-on-error=%v defaults=%v)", job.Needs, job.If, job.ContinueOnError, job.Defaults)
	}
	// The scheduled test's name comes from the function itself (review
	// round 12): a rename breaks this build instead of leaving the weekly
	// job running "go test -run <old name>", which exits 0 with no tests.
	fn := runtime.FuncForPC(reflect.ValueOf(TestSBOMsPassTheOfficialValidators).Pointer()).Name()
	testName := fn[strings.LastIndex(fn, ".")+1:]
	wantRun := "go test ./internal/collector/ -run " + testName + " -count=1 -v"
	installed, ran := false, false
	for _, st := range job.Steps {
		if st.If != nil || st.ContinueOnError != nil || st.Shell != nil || st.WorkingDirectory != nil {
			t.Errorf("step %q must have no if, continue-on-error, shell or working-directory", st.Name)
		}
		if strings.Contains(st.Run, "install -q spdx-tools 'cyclonedx-python-lib[json-validation]'") {
			installed = true
		}
		if strings.Contains(st.Run, "-run "+testName) {
			ran = true
			if got := st.Env[sbomToolsEnv]; got != "${{ runner.temp }}/sbomtools/bin/python" {
				t.Errorf("the validator step must set %s in its env (got %q)", sbomToolsEnv, got)
			}
			if strings.TrimSpace(st.Run) != wantRun {
				t.Errorf("the validator step must run only the test, so no shell can swallow its failure: %q", st.Run)
			}
		}
	}
	if !installed || !ran {
		t.Errorf("sbom-validators must install both validators (%v) and run the test (%v)", installed, ran)
	}
}
