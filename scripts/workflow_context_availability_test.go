// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A workflow file that names a context where GitHub does not provide it is
// refused when it is pushed — the run is recorded as a failure with no jobs,
// and a scheduled workflow never fires. nightly.yml (0.29.107) put
// `${{ runner.temp }}` in a job's `env:` block, where only the github,
// needs, strategy, matrix, vars, secrets and inputs contexts exist; every
// push since then reported a failed Nightly run, and the schedule never ran
// (0.29.125). GitHub's context-availability table (docs.github.com,
// "Contexts reference", read 2026-11-02): a job's `env` has github, needs,
// strategy, matrix, vars, secrets and inputs — not runner, env, steps, job
// or jobs; a container's or a service's `env.<id>` has job, runner and env
// as well — not steps or jobs; a step's `env:`, `run:` and `with:` have
// every context but jobs (a reusable workflow's outputs alone), and a
// step's `if:` also lacks secrets. Every reference inside every expression
// of a value counts
// (Copilot's sixth review of PR #246, 0.29.128: the first version read the
// first token after `${{` alone, so a forbidden context wrapped in
// `format(…)` or behind `||` passed; L10 round 1 then found the name
// matched only with a `.` after it — `toJSON(runner)`, `runner['temp']` —
// and the blocks found by indentation, which a flow mapping, a comment or
// another indent style defeated, so the file is decoded; round 2 found the
// job's forbidden set applied to the container and service env too, where
// GitHub allows three of the five).
func TestWorkflowJobEnvUsesOnlyAvailableContexts(t *testing.T) {
	root := srctest.Root(t)
	var files []string
	for _, ext := range []string{"*.yml", "*.yaml"} { // GitHub reads both extensions
		matches, err := filepath.Glob(filepath.Join(root, ".github", "workflows", ext))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	srctest.MinCount(t, "workflow files", len(files), 3)
	examined := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, f)
		blocks, violations, err := jobEnvContextViolations(b)
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		examined += blocks
		for _, v := range violations {
			switch {
			case v.context == mergeKey:
				t.Errorf("%s:%d: job %s's %s is a YAML merge key, which GitHub's parser does not support (the file is refused at push and never runs): write the keys out, or anchor the whole mapping (`env: *common`)", rel, v.line, v.job, v.key)
			case strings.HasPrefix(v.key, "env"):
				t.Errorf("%s:%d: job %s's %s uses the %s context, which GitHub does not provide there (the file is refused at push and never runs): set the value in a step, e.g. `echo \"NAME=$RUNNER_TEMP/...\" >> \"$GITHUB_ENV\"`", rel, v.line, v.job, v.key, v.context)
			default: // a container's or a service's env is set when the container starts, before any step: no step can supply it
				t.Errorf("%s:%d: job %s's %s uses the %s context, which GitHub does not provide there (the file is refused at push and never runs): a container's or service's env cannot read steps or jobs — pass the value through a job output or the step's own env", rel, v.line, v.job, v.key, v.context)
			}
		}
	}
	srctest.MinCount(t, "job-level env blocks examined", examined, 1)
}

// jobEnvViolation is one context a job-level env value names that GitHub
// does not provide there — the job, the value's key (`env.NAME`,
// `container.env.NAME`, `services.<id>.env.NAME`), its 1-based line and the
// context — or a `<<` merge key in such a mapping (context = mergeKey):
// GitHub's parser accepts anchors and aliases but has no merge keys, so the
// file is refused as surely as for a withheld context.
type jobEnvViolation struct {
	job     string
	key     string
	line    int
	context string
}

// jobEnvContextViolations decodes a workflow file and reads every job-level
// env mapping — `jobs.<id>.env` against the job's forbidden set,
// `jobs.<id>.container.env` and `jobs.<id>.services.<id>.env` against
// theirs — by position in the document, so indentation style, flow mappings
// (`env: { A: 1 }`), comments, multi-line scalars, anchors and aliases
// (`env: *common`) do not matter. It returns the number of such mappings
// examined and every value naming a context its place does not have, plus
// every `<<` merge key (GitHub's parser has none). A step's env is not one
// of them. Decoded to yaml.Node rather than the typed structs
// ci_workflows_test.go uses because a finding needs the value's line (L18:
// a different shape with a reason). Only the first YAML document is read
// and, of a duplicated structural key (`env:` twice in a job), the first
// value — a duplicated key inside an env mapping has both values read
// (GitHub refuses a workflow with any of these); a file that does not parse
// is an error, never an empty examination.
func jobEnvContextViolations(src []byte) (blocks int, out []jobEnvViolation, err error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return 0, nil, err
	}
	if len(doc.Content) == 0 {
		return 0, nil, nil
	}
	jobs := mappingValue(doc.Content[0], "jobs")
	if jobs == nil || jobs.Kind != yaml.MappingNode {
		return 0, nil, nil
	}
	pairs := func(jobID, key string, env *yaml.Node, unavailable *regexp.Regexp) {
		for i := 0; i+1 < len(env.Content); i += 2 {
			k, v := env.Content[i], deref(env.Content[i+1])
			if k.Value == mergeKey { // not expanded: GitHub has no merge keys, so this key refuses the file (and a self-referential `&e … <<: *e` would recurse)
				out = append(out, jobEnvViolation{job: jobID, key: key + "." + mergeKey, line: k.Line, context: mergeKey})
				continue
			}
			for _, name := range unavailableContexts(v.Value, unavailable) {
				out = append(out, jobEnvViolation{job: jobID, key: key + "." + k.Value, line: v.Line, context: name})
			}
		}
	}
	scan := func(jobID, key string, env *yaml.Node, unavailable *regexp.Regexp) {
		env = deref(env)
		switch {
		case env == nil, env.Kind == yaml.ScalarNode && env.Tag == "!!null":
			return // absent, or `env:` with nothing under it: nothing examined
		case env.Kind == yaml.MappingNode:
			blocks++
			pairs(jobID, key, env, unavailable)
		case env.Kind == yaml.ScalarNode: // a whole-map expression, `env: ${{ fromJSON(…) }}`
			blocks++
			for _, name := range unavailableContexts(env.Value, unavailable) {
				out = append(out, jobEnvViolation{job: jobID, key: key, line: env.Line, context: name})
			}
		}
	}
	for i := 0; i+1 < len(jobs.Content); i += 2 {
		jobID, job := jobs.Content[i].Value, deref(jobs.Content[i+1])
		scan(jobID, "env", mappingValue(job, "env"), jobEnvUnavailable)
		scan(jobID, "container.env", mappingValue(mappingValue(job, "container"), "env"), containerEnvUnavailable)
		if services := mappingValue(job, "services"); services != nil && services.Kind == yaml.MappingNode {
			for j := 0; j+1 < len(services.Content); j += 2 {
				scan(jobID, "services."+services.Content[j].Value+".env", mappingValue(services.Content[j+1], "env"), containerEnvUnavailable)
			}
		}
	}
	return blocks, out, nil
}

// mergeKey is YAML's `<<`, which GitHub Actions' parser does not support
// (anchors and aliases since 2025, merge keys not: community discussion
// 185877, the runner's own statement).
const mergeKey = "<<"

// deref follows an alias (`*name`) to the node it names; any other node is
// itself. GitHub accepts anchors and aliases in a workflow file. yaml.v3
// never anchors an alias, so the loop runs at most once.
func deref(n *yaml.Node) *yaml.Node {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	return n
}

// mappingValue is the value of key in a mapping node (an alias followed),
// nil when n is not a mapping or has no such key.
func mappingValue(n *yaml.Node, key string) *yaml.Node {
	n = deref(n)
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return deref(n.Content[i+1])
		}
	}
	return nil
}

var (
	// Each `${{ … }}` of a value, across the lines of a multi-line scalar.
	// GitHub's reader also ends an expression at the first `}}`, so a `}}`
	// inside a string literal is not a case to handle here.
	workflowExpression = regexp.MustCompile(`(?s)\$\{\{(.*?)\}\}`)
	expressionLiteral  = regexp.MustCompile(`'(?:[^']|'')*'`) // a string literal; '' is an escaped quote (GitHub expressions have no double-quoted strings)
	// A context NAME as a word — `runner.temp`, `runner['temp']`,
	// `toJSON(runner)` — case-insensitively (GitHub resolves contexts
	// ignoring case), not a property spelled like one (`needs.job.…`,
	// `github.job`): the `[^\w.]` guard. One set per place, from GitHub's
	// context-availability table.
	jobEnvUnavailable       = regexp.MustCompile(`(?i)(?:^|[^\w.])(runner|env|steps|job|jobs)\b`) // jobs.<id>.env
	containerEnvUnavailable = regexp.MustCompile(`(?i)(?:^|[^\w.])(steps|jobs)\b`)                // jobs.<id>.container.env, services.<id>.env
)

// unavailableJobEnvContexts is unavailableContexts for a job's own env.
func unavailableJobEnvContexts(value string) []string {
	return unavailableContexts(value, jobEnvUnavailable)
}

// unavailableContexts is every context a value names that GitHub does not
// provide at its place (unavailable), lower-cased, each once in order of
// first appearance: every reference inside every expression of the value,
// string literals removed first (a context's name inside '…' is text).
func unavailableContexts(value string, unavailable *regexp.Regexp) []string {
	var out []string
	seen := map[string]bool{}
	for _, expr := range workflowExpression.FindAllStringSubmatch(value, -1) {
		code := expressionLiteral.ReplaceAllString(expr[1], "''")
		for _, m := range unavailable.FindAllStringSubmatch(code, -1) {
			name := strings.ToLower(m[1])
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// The expression scanner, one value at a time (Copilot's sixth review of
// PR #246 and L10 round 1 of 0.29.128).
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
		{"${{ runner.temp }}/${{ runner.os }}", []string{"runner"}}, // each context once
		{"${{ toJSON(runner) }}", []string{"runner"}},               // the whole context as an argument
		{"${{ runner['temp'] }}", []string{"runner"}},               // bracket property access
		{"${{ steps['setup'].outputs.dir }}", []string{"steps"}},
		{"${{ RUNNER.temp }} ${{ Job.status }}", []string{"runner", "job"}}, // contexts resolve ignoring case
		{"${{ jobs.build.outputs.x }}", []string{"jobs"}},
		{"${{ format('{0}/ci.json',\n        runner.temp) }}", []string{"runner"}}, // a folded multi-line scalar
		{"${{ format('runner.temp is {0}', vars.X) }}", nil},                       // the name inside a string literal is text
		{"${{ format('it''s {0}', runner.temp) }}", []string{"runner"}},            // an escaped quote does not end the literal early
		{"${{ github.workspace }}/x", nil},
		{"${{ needs.job.outputs.dir }}", nil}, // a property spelled like a context is not the context
		{"${{ github.job }}", nil},
		{"${{ vars.runner_env }}", nil}, // part of a longer name is not the context
		{"plain text, runner.temp outside an expression", nil},
		{"postgres://u:p@localhost:5432/db?sslmode=disable", nil},
	}
	for _, c := range cases {
		got := unavailableJobEnvContexts(c.value)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("%q: contexts %v, want %v", c.value, got, c.want)
		}
	}
	// A container's or a service's env: job, runner and env are available
	// there (GitHub's table), steps and jobs are not.
	for value, want := range map[string]string{
		"${{ toJSON(job) }}/${{ runner.temp }}/${{ env.HOME }}": "",
		"${{ steps.x.outputs.y }}":                              "steps",
		"${{ jobs.a.outputs.b }} ${{ runner.os }}":              "jobs",
	} {
		if got := strings.Join(unavailableContexts(value, containerEnvUnavailable), ","); got != want {
			t.Errorf("container env %q: contexts %q, want %q", value, got, want)
		}
	}
}

// The decoded walk, on synthetic workflows: a job's env value with the
// forbidden context inside format(…) is a violation at its line; a
// container's or a service's env may name job, runner and env (GitHub's
// table) but not steps or jobs; the job's contexts in a STEP's env are
// allowed; a flow-mapping env, a comment inside the block, a trailing
// comment, a multi-line scalar, an expression-valued env, an empty env and a
// 4-space indentation style are all read as GitHub reads them; an aliased
// env is followed, and a `<<` merge key is reported (GitHub's parser has
// none) — a self-referential one included, without recursing.
func TestJobEnvWalkFlagsOnlyJobLevelValues(t *testing.T) {
	const twoSpace = `name: x
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    env:   # the job's env
      TZ: America/Chicago
      # a comment inside the block
      CFG: ${{ format('{0}/ci.json', runner.temp) }}

      DSN: ${{ secrets.DSN }}
      TWO: ${{ format('{0}',
        steps.x.outputs.y) }}
    container:
      image: img
      env:
        A: ${{ toJSON(job) }}
        S: ${{ steps.x.outputs.y }}
    services:
      db:
        image: postgres
        env:
          B: ${{ env.HOME }}/${{ runner.temp }}
          J: ${{ jobs.a.outputs.b }}
    steps:
      - run: echo
        env:
          TMP: ${{ runner.temp }}
  other:
    env: { A: "${{ vars.A }}", B: "${{ runner['os'] }}" }
    steps:
      - run: echo
  clean:
    env:
      A: ${{ vars.A }}
    steps:
      - run: echo
  whole:
    env: ${{ toJSON(runner) }}
    steps:
      - run: echo
  empty:
    env:
    steps:
      - run: echo
`
	blocks, got, err := jobEnvContextViolations([]byte(twoSpace))
	if err != nil {
		t.Fatal(err)
	}
	if blocks != 6 {
		t.Errorf("job-level env mappings examined = %d, want 6 (build's env, container and service; other's; clean's; whole's expression-valued env — a step's env is not one, nor an empty env)", blocks)
	}
	want := []jobEnvViolation{
		{job: "build", key: "env.CFG", line: 9, context: "runner"},
		{job: "build", key: "env.TWO", line: 12, context: "steps"},
		{job: "build", key: "container.env.S", line: 18, context: "steps"},  // not A: job is available to a container's env
		{job: "build", key: "services.db.env.J", line: 24, context: "jobs"}, // not B: env and runner are available to a service's env
		{job: "other", key: "env.B", line: 30, context: "runner"},
		{job: "whole", key: "env", line: 39, context: "runner"},
	}
	if len(got) != len(want) {
		t.Fatalf("violations = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("violation %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// The same job in a 4-space style: found by position, not by indentation.
	const fourSpace = "name: x\non: push\njobs:\n    build:\n        runs-on: ubuntu-latest\n        env:\n            CFG: ${{ runner.temp }}\n        steps:\n            - run: echo\n"
	blocks, got, err = jobEnvContextViolations([]byte(fourSpace))
	if err != nil {
		t.Fatal(err)
	}
	if blocks != 1 || len(got) != 1 || got[0] != (jobEnvViolation{job: "build", key: "env.CFG", line: 7, context: "runner"}) {
		t.Errorf("4-space style: blocks %d, violations %+v; want 1 and the env.CFG runner at line 7", blocks, got)
	}
	if _, _, err := jobEnvContextViolations([]byte("jobs: [\n")); err == nil {
		t.Error("a file that does not parse must be an error, not an empty examination")
	}
	// Anchors and aliases: an env that IS an alias is read as the mapping it
	// names; a `<<` merge key is reported, not expanded (GitHub has none), and
	// a self-referential one (`&e … <<: *e`) must not recurse.
	const aliased = `name: x
on: push
x-common: &common
  CFG: ${{ runner.temp }}
jobs:
  a:
    env: *common
    steps:
      - run: echo
  b:
    env:
      <<: *common
      OWN: ${{ steps.s.outputs.o }}
    steps:
      - run: echo
  c:
    env: &e
      <<: *e
      A: ${{ runner.temp }}
    steps:
      - run: echo
`
	blocks, got, err = jobEnvContextViolations([]byte(aliased))
	if err != nil {
		t.Fatal(err)
	}
	wantAliased := []jobEnvViolation{
		{job: "a", key: "env.CFG", line: 4, context: "runner"},
		{job: "b", key: "env.<<", line: 12, context: mergeKey},
		{job: "b", key: "env.OWN", line: 13, context: "steps"},
		{job: "c", key: "env.<<", line: 18, context: mergeKey},
		{job: "c", key: "env.A", line: 19, context: "runner"},
	}
	if blocks != 3 || strings.Join(fmtViolations(got), ";") != strings.Join(fmtViolations(wantAliased), ";") {
		t.Errorf("aliases: blocks %d, violations %+v; want 3 and %+v", blocks, got, wantAliased)
	}
}

func fmtViolations(vs []jobEnvViolation) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.job+" "+v.key+" "+v.context+" "+strconv.Itoa(v.line))
	}
	return out
}
