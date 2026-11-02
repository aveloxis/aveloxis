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
// `with:` and `if:` may use them. Every reference inside every expression of
// a value counts (Copilot's fifth review of PR #246, 0.29.128: the first
// version read the first token after `${{` alone, so a forbidden context
// wrapped in `format(…)` or behind `||` passed).
func TestWorkflowJobEnvUsesOnlyAvailableContexts(t *testing.T) {
	root := srctest.Root(t)
	files, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	srctest.MinCount(t, "workflow files", len(files), 3)
	examined := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		blocks, violations := jobEnvContextViolations(strings.Split(string(b), "\n"))
		examined += blocks
		for _, v := range violations {
			rel, _ := filepath.Rel(root, f)
			t.Errorf("%s:%d: a job's env uses the %s context, which GitHub does not provide there (the file is refused at push and never runs): set the value in a step, e.g. `echo \"NAME=$RUNNER_TEMP/...\" >> \"$GITHUB_ENV\"`", rel, v.line, v.context)
		}
	}
	srctest.MinCount(t, "job-level env blocks examined", examined, 1)
}

// jobEnvViolation is one context a job-level env value names that GitHub
// does not provide there, at its 1-based line.
type jobEnvViolation struct {
	line    int
	context string
}

var jobEnvBlock = regexp.MustCompile(`^    env:\s*$`) // jobs.<id>.env: two levels under jobs; a step's env is deeper

// jobEnvContextViolations walks a workflow file's lines: every job-level
// `env:` block (counted) and, in each, every value naming a context the job
// level does not have. A step's own `env:` is indented deeper and is not read.
func jobEnvContextViolations(lines []string) (blocks int, out []jobEnvViolation) {
	for i, line := range lines {
		if !jobEnvBlock.MatchString(line) {
			continue
		}
		blocks++
		for j := i + 1; j < len(lines) && (strings.HasPrefix(lines[j], "      ") || strings.TrimSpace(lines[j]) == ""); j++ {
			for _, name := range unavailableJobEnvContexts(lines[j]) {
				out = append(out, jobEnvViolation{line: j + 1, context: name})
			}
		}
	}
	return blocks, out
}

var (
	workflowExpression   = regexp.MustCompile(`\$\{\{(.*?)\}\}`)                      // each `${{ … }}` of a line
	expressionLiteral    = regexp.MustCompile(`'(?:[^']|'')*'`)                       // a string literal; '' is an escaped quote
	unavailableJobEnvRef = regexp.MustCompile(`(?:^|[^\w.])(runner|env|steps|job)\.`) // a context reference, not a property spelled like one (needs.job.…)
)

// unavailableJobEnvContexts is every context a job-level env value names that
// GitHub does not provide there, each once in order of first appearance:
// every reference inside every expression of the value, string literals
// removed first (a context's name inside '…' is text).
func unavailableJobEnvContexts(value string) []string {
	var out []string
	seen := map[string]bool{}
	for _, expr := range workflowExpression.FindAllStringSubmatch(value, -1) {
		code := expressionLiteral.ReplaceAllString(expr[1], "''")
		for _, m := range unavailableJobEnvRef.FindAllStringSubmatch(code, -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				out = append(out, m[1])
			}
		}
	}
	return out
}

// Copilot's fifth review of PR #246 (0.29.128): the first tripwire read only
// the first token after `${{`, so `${{ format('{0}/ci.json', runner.temp) }}`
// or `${{ vars.CFG || runner.temp }}` — both refused by GitHub at a job's
// env — passed it. The scan now reads every context reference inside each
// expression, with string literals removed first (a context's name inside
// '…' is text, not a reference).
func TestJobEnvContextScanReadsWholeExpressions(t *testing.T) {
	cases := []struct {
		value string
		want  []string
	}{
		{"${{ runner.temp }}/ci.json", []string{"runner"}},
		{"${{ format('{0}/ci.json', runner.temp) }}", []string{"runner"}},
		{"${{ vars.CFG || runner.temp }}", []string{"runner"}},
		{"${{ vars.A }}-${{ steps.setup.outputs.dir }}", []string{"steps"}},
		{"${{ job.status }}", []string{"job"}},
		{"${{ env.HOME }}/${{ runner.os }}", []string{"env", "runner"}},
		{"${{ format('runner.temp is {0}', vars.X) }}", nil},            // the name inside a string literal is text
		{"${{ format('it''s {0}', runner.temp) }}", []string{"runner"}}, // an escaped quote does not end the literal early
		{"${{ github.workspace }}/x", nil},
		{"${{ needs.job.outputs.dir }}", nil}, // a property spelled like a context is not the context
		{"plain text, runner.temp outside an expression", nil},
		{"postgres://u:p@localhost:5432/db?sslmode=disable", nil},
	}
	for _, c := range cases {
		got := unavailableJobEnvContexts(c.value)
		if len(got) != len(c.want) {
			t.Errorf("%q: contexts %v, want %v", c.value, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%q: contexts %v, want %v", c.value, got, c.want)
				break
			}
		}
	}
}

// The walk over a file, on a synthetic workflow: a job-level env value with
// the forbidden context inside format(…) is a violation at its line; the
// same context in a STEP's env (deeper) is allowed; a job env of allowed
// contexts is examined and clean.
func TestJobEnvWalkFlagsOnlyJobLevelValues(t *testing.T) {
	lines := strings.Split(`name: x
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      TZ: America/Chicago
      CFG: ${{ format('{0}/ci.json', runner.temp) }}
      DSN: ${{ secrets.DSN }}
    steps:
      - run: echo
        env:
          TMP: ${{ runner.temp }}
  other:
    env:
      A: ${{ vars.A }}
    steps:
      - run: echo
`, "\n")
	blocks, got := jobEnvContextViolations(lines)
	if blocks != 2 {
		t.Errorf("job-level env blocks examined = %d, want 2 (a step's env is not one)", blocks)
	}
	if len(got) != 1 || got[0] != (jobEnvViolation{line: 8, context: "runner"}) {
		t.Errorf("violations = %+v, want the one job-level format(runner.temp) at line 8", got)
	}
}
