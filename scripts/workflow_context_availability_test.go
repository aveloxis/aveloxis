// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A workflow file that names a context where GitHub does not provide it is
// refused when it is pushed — the run is recorded as a failure with no jobs,
// and a scheduled workflow never fires. nightly.yml (0.29.107) put
// `${{ runner.temp }}` in a job's `env:` block, where only the github,
// needs, strategy, matrix, vars, secrets and inputs contexts exist; every
// push since then reported a failed Nightly run, and the schedule never ran
// (0.29.125). The job-level `env:` is the one place in these files where the
// runner, env, steps and job contexts cannot be used; a step's `env:`, `run:`,
// `with:` and `if:` may use them.
func TestWorkflowJobEnvUsesOnlyAvailableContexts(t *testing.T) {
	root := srctest.Root(t)
	files, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	srctest.MinCount(t, "workflow files", len(files), 3)
	jobEnv := regexp.MustCompile(`^    env:\s*$`)                          // jobs.<id>.env: two levels under jobs
	unavailable := regexp.MustCompile(`\$\{\{\s*(runner|env|steps|job)\.`) // contexts a job's env cannot see
	examined := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(b), "\n")
		for i, line := range lines {
			if !jobEnv.MatchString(line) {
				continue
			}
			examined++
			for j := i + 1; j < len(lines) && (strings.HasPrefix(lines[j], "      ") || strings.TrimSpace(lines[j]) == ""); j++ {
				if m := unavailable.FindStringSubmatch(lines[j]); m != nil {
					rel, _ := filepath.Rel(root, f)
					t.Errorf("%s:%d: a job's env uses the %s context, which GitHub does not provide there (the file is refused at push and never runs): set the value in a step, e.g. `echo \"NAME=$RUNNER_TEMP/...\" >> \"$GITHUB_ENV\"`", rel, j+1, m[1])
				}
			}
		}
	}
	srctest.MinCount(t, "job-level env blocks examined", examined, 1)
}
