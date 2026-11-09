// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.22.8 introduces `aveloxis data-test` — an operator-driven
// shadow-database verification harness for schema changes. Compares
// the local working-tree binary against a GitHub-tagged release
// binary by collecting the same repo into two scratch DBs and
// diffing row counts table-by-table.
//
// Tests in this file pin the command's source-level contract.
// Behavioral tests for the row-count diff function live in
// internal/db/rowcount_diff_test.go (also v0.22.8).

func TestDataTestCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "dataTestCmd(") {
		t.Error("main.go must call dataTestCmd() and add it to root via root.AddCommand — " +
			"without registration the subcommand can't be invoked from the CLI")
	}
}

func TestDataTestCmdHasRequiredFlags(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatalf("data_test_cmd.go does not exist — v0.22.8 command source file required: %v", err)
	}
	code := string(src)

	// Required flags
	for _, flag := range []string{"released-tag", "repo"} {
		if !strings.Contains(code, `"`+flag+`"`) {
			t.Errorf("data-test command must define --%s flag", flag)
		}
	}

	// Optional flags
	for _, flag := range []string{"keep-dbs", "work-dir"} {
		if !strings.Contains(code, `"`+flag+`"`) {
			t.Errorf("data-test command must define --%s flag (optional)", flag)
		}
	}

	// Must use cobra.Command
	if !strings.Contains(code, "cobra.Command") {
		t.Error("data-test command must be a cobra.Command (matches the rest of the CLI)")
	}
}

func TestDataTestCmdInvokesAllPhases(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Each phase is a separate helper function, called in order.
	// The names pin the operational sequence so a future refactor
	// can't accidentally skip a phase.
	phases := []string{
		"resolveReleasedBinary",
		"resolveLocalBinary",
		"provisionScratchDB",
		"generateScratchConfig",
		"runMigrate",
		"copyAPIKeys",
		"dtRunAddRepo",
		"dtRunCollect",
		"runRowCountDiff",
		"writeReport",
		"cleanupScratchDBs",
	}
	for _, p := range phases {
		if !strings.Contains(code, p) {
			t.Errorf("data-test command must invoke phase helper %s — without it, the "+
				"corresponding phase of the harness doesn't run", p)
		}
	}
}

func TestDataTestUsesGitWorktreeNotFreshClone(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The released-binary resolver uses `git worktree add` (fast,
	// reuses local clone's objects) rather than a fresh `git clone`
	// from the remote (slow, redundant network fetch). Empirically
	// chosen for v0.22.8.
	if !strings.Contains(code, "worktree") {
		t.Error("resolveReleasedBinary must use `git worktree add` to materialize the " +
			"tagged release source — fresh `git clone` would re-fetch the entire repo " +
			"from origin, slow and redundant when the local working copy already has " +
			"the tag")
	}
}

func TestDataTestStreamsSubprocessOutput(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Collection takes ~30 minutes per side. Operator needs live
	// progress, not a 30-minute silence. The subprocess wrappers
	// must wire stdout/stderr through.
	if !strings.Contains(code, "Stdout") || !strings.Contains(code, "Stderr") {
		t.Error("runCollect (and runMigrate) must wire subprocess Stdout/Stderr to the " +
			"parent so the operator sees live progress during the ~30-min collection " +
			"window")
	}
}

func TestDataTestDropsScratchDBsByDefault(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Per operator decision on 2026-05-17: drop scratch DBs by
	// default; --keep-dbs to retain. The cleanup function must
	// honor the flag.
	if !strings.Contains(code, "keepDBs") && !strings.Contains(code, "KeepDBs") {
		t.Error("data-test must thread a keepDBs boolean from the --keep-dbs flag " +
			"through to cleanupScratchDBs so the operator can opt to retain DBs for " +
			"ad-hoc SQL inspection after a FAIL")
	}
}

// TestCopyAPIKeysUsesWorkerOauthNotAPIKeys is a recurring-drift
// tripwire. On 2026-05-17 the operator hit a runtime error from an
// earlier draft of copyAPIKeys that queried `aveloxis_ops.api_keys`
// — a table that does not exist (the real one is worker_oauth).
// The operator noted "we've had this issue before": assuming a
// generic-sounding name for the keys table is a known recurring
// failure mode in this codebase.
//
// This test fails the build if `aveloxis_ops.api_keys` ever
// re-appears in data_test_cmd.go AND fails if worker_oauth is
// missing. Both halves required: a future refactor that uses an
// even-newer table name would still pass the first check but fail
// the second.
// TestDataTestForcesFullCollection pins the v0.22.10 fix: the
// harness must invoke `<binary> collect URL --full`, not just
// `<binary> collect URL`. Without --full the orchestrator uses
// the default incremental since-filter, which produces hundreds
// of spurious FK violations (issue_events / pull_request_events
// referencing parents that weren't refetched in the since window).
// Those violations cause silent row-loss that's symmetric across
// both DBs — hiding any real regression behind noise.
//
// Confirmed empirically on 2026-05-17: a partial-collection run
// reported 0 rows in issue_events / pull_request_events on BOTH
// sides, which masked the question of whether v0.22.7's
// DEFERRABLE INITIALLY DEFERRED changed behavior at all.
//
// If a future refactor drops --full, this test fires immediately.
func TestDataTestForcesFullCollection(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, `"--full"`) {
		t.Error("dtRunCollect must pass --full to the collect subprocess so the " +
			"harness exercises full-history collection (since=zero). Without it the " +
			"FK-violation noise from incremental partial collections hides any real " +
			"regression. See the 2026-05-17 diagnostic for the empirical evidence.")
	}
	if !strings.Contains(code, "dtRunCollect") {
		t.Error("dtRunCollect must still exist as the named phase helper")
	}
}

func TestCopyAPIKeysUsesWorkerOauthNotAPIKeys(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "aveloxis_ops.worker_oauth") {
		t.Error("copyAPIKeys must query aveloxis_ops.worker_oauth — that's the " +
			"real API-key table. See internal/db/keys.go for the canonical " +
			"INSERT/SELECT shape.")
	}
	if strings.Contains(code, "aveloxis_ops.api_keys") {
		t.Error("regression: aveloxis_ops.api_keys does NOT exist in the schema. " +
			"The real table is aveloxis_ops.worker_oauth. This exact assumption " +
			"drift broke the v0.22.8 release on 2026-05-17; do not let it recur.")
	}
}

func TestScratchDBNamesAreConventional(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Operator-mandated DB names so the harness output is
	// predictable from invocation to invocation: aveloxis_released
	// and aveloxis_new.
	if !strings.Contains(code, "aveloxis_released") {
		t.Error("data-test must use the conventional name `aveloxis_released` for the " +
			"released-version scratch DB so output is predictable across runs")
	}
	if !strings.Contains(code, "aveloxis_new") {
		t.Error("data-test must use the conventional name `aveloxis_new` for the " +
			"local-version scratch DB so output is predictable across runs")
	}
}

// v0.27.129 — the 2026-08-21 main-vs-branch run spent ~50 minutes
// collecting both sides and then crashed in the column-fill diff on the
// first deliberately-dropped table it met (contributors_old, removed in
// v0.27.115). Two harness affordances came out of it: shape drift is
// REPORTED (not fatal), and --diff-only regenerates the report against
// kept scratch DBs without another hour of collection.
func TestDataTestHasDiffOnlyMode(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, `"diff-only"`) {
		t.Error("data-test must register --diff-only")
	}
	if !strings.Contains(s, "if diffOnly {") {
		t.Error("--diff-only must gate the build/provision/collect phases (diff + report only)")
	}
	for _, needle := range []string{"TablesOnlyInReleased", "TablesOnlyInNew", "AddedColumns", "REMOVED"} {
		if !strings.Contains(s, needle) {
			t.Errorf("writeReport must render the v0.27.129 schema-shape drift (%q)", needle)
		}
	}
}

// v0.29.95: the scratch configs copied the operator's aveloxis.json and
// changed only the database name, so both sides cloned into the
// operator's own repo_clone_dir (~/aveloxis-repos/repo_<id>). The id is
// the row id in the SCRATCH database, so the first repository of the
// run (backstage, 2026-10-14) overwrote repo_1 of the operator's local
// deployment (apache/tomcat), and the second side re-cloned over the
// first. Every location field of the copied config that names shared
// disk must be rewritten per side, as the database name already was.
//
// Behavioral: call the one owner and read what it wrote.
func TestWriteScratchConfigsGivesEachSideItsOwnCloneDir(t *testing.T) {
	workDir := t.TempDir()
	primary := &config.Config{}
	primary.Database.DBName = "aveloxis"
	primary.Collection.RepoCloneDir = "/operators/real/clones"

	paths, err := writeScratchConfigs(primary, workDir)
	if err != nil {
		t.Fatalf("writeScratchConfigs: %v", err)
	}
	if primary.Collection.RepoCloneDir != "/operators/real/clones" || primary.Database.DBName != "aveloxis" {
		t.Error("writeScratchConfigs mutated the operator's loaded config")
	}
	if dataTestSides[dataTestReleased].name != "released" || dataTestSides[dataTestNew].name != "new" {
		t.Fatalf("dataTestSides = %v: the released side must be index dataTestReleased and the new side dataTestNew (the binaries are picked by those indexes)", dataTestSides)
	}

	var dirs []string
	for i, side := range dataTestSides {
		if want := filepath.Join(workDir, "config-"+side.name+".json"); paths[i] != want {
			t.Errorf("%s config path = %q, want %q", side.name, paths[i], want)
		}
		raw, err := os.ReadFile(paths[i])
		if err != nil {
			t.Fatal(err)
		}
		var cfg config.Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			t.Fatalf("config-%s.json is not a config: %v", side.name, err)
		}
		if cfg.Database.DBName != side.dbname {
			t.Errorf("config-%s.json dbname = %q, want %q", side.name, cfg.Database.DBName, side.dbname)
		}
		dir := cfg.Collection.RepoCloneDir
		if dir == primary.Collection.RepoCloneDir {
			t.Errorf("config-%s.json keeps the operator's repo_clone_dir %q: the scratch collect would clone over the operator's repo_<id> folders", side.name, dir)
		}
		if !filepath.IsAbs(dir) {
			t.Errorf("config-%s.json repo_clone_dir = %q, want an absolute path (the collect subprocess must not depend on the harness's cwd)", side.name, dir)
		}
		if rel, err := filepath.Rel(workDir, dir); err != nil || strings.HasPrefix(rel, "..") {
			t.Errorf("config-%s.json repo_clone_dir = %q, want a folder under the work dir %q", side.name, dir, workDir)
		}
		dirs = append(dirs, dir)
	}
	if len(dirs) == 2 && dirs[0] == dirs[1] {
		t.Errorf("both sides share repo_clone_dir %q: the new side would re-clone over the released side's bare clones", dirs[0])
	}
	if dataTestSides[0].dbname == dataTestSides[1].dbname {
		t.Errorf("both sides collect into %q", dataTestSides[0].dbname)
	}
}

// The work dir is resolved once, absolute and created, whatever the
// operator typed (round 1 F4: a relative --work-dir reached the
// released build's -o with its cwd in the worktree; round 2 F2: the
// token pin on filepath.Abs did not require the result to be used).
func TestResolveDataTestWorkDirIsAbsoluteAndCreated(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	// The "" arm creates a folder under os.TempDir(); point that at a
	// per-test folder so a concurrent run (another test process, a real
	// data-test started in the same second) never shares or loses it.
	t.Setenv("TMPDIR", t.TempDir())

	got, err := resolveDataTestWorkDir("work-rel")
	if err != nil {
		t.Fatalf("resolveDataTestWorkDir(relative): %v", err)
	}
	if !filepath.IsAbs(got) || filepath.Base(got) != "work-rel" {
		t.Errorf("resolveDataTestWorkDir(\"work-rel\") = %q, want an absolute path ending in work-rel", got)
	}
	if st, err := os.Stat(filepath.Join(base, "work-rel")); err != nil || !st.IsDir() {
		t.Errorf("resolveDataTestWorkDir must create the folder under the cwd: %v", err)
	}
	if st, err := os.Stat(got); err != nil || !st.IsDir() {
		t.Errorf("the returned path %q is not a folder: %v", got, err)
	}

	def, err := resolveDataTestWorkDir("")
	if err != nil {
		t.Fatalf("resolveDataTestWorkDir(\"\"): %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(def) })
	if !filepath.IsAbs(def) || !strings.HasPrefix(filepath.Base(def), "aveloxis-data-test-") {
		t.Errorf("resolveDataTestWorkDir(\"\") = %q, want an absolute fresh aveloxis-data-test-<ts> folder", def)
	}
	if st, err := os.Stat(def); err != nil || !st.IsDir() {
		t.Errorf("the default work dir %q was not created: %v", def, err)
	}
}

// The command must take its work dir from resolveDataTestWorkDir and
// its configs from writeScratchConfigs, and generateScratchConfig must
// have exactly one call site in the package, inside writeScratchConfigs.
// Read comment-stripped: a comment cannot satisfy it; a second helper
// that rewrites the configs (round 2 F1's mutant) or a command that
// bypasses the owner (round 1 F1's) adds a call site and fails it.
func TestDataTestCmdWritesConfigsThroughTheOwner(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/data_test_cmd.go"))
	body := srctest.FuncBody(t, src, "func dataTestCmd(")
	if !strings.Contains(body, "workDir, err = resolveDataTestWorkDir(workDir)") {
		t.Error("data-test must take its work dir from `workDir, err = resolveDataTestWorkDir(workDir)` (absolute, created, once)")
	}
	for _, banned := range []string{"filepath.Abs(", "os.MkdirAll(workDir"} {
		if strings.Contains(body, banned) {
			t.Errorf("data-test must not call %s itself: resolveDataTestWorkDir owns the work dir", banned)
		}
	}
	if !strings.Contains(body, "writeScratchConfigs(primaryCfg, workDir)") {
		t.Error("data-test must write both scratch configs with writeScratchConfigs(primaryCfg, workDir)")
	}
	for _, pair := range []string{"dataTestReleased: releasedBin", "dataTestNew: localBin"} {
		if !strings.Contains(body, pair) {
			t.Errorf("data-test must pair the binaries with the sides as %q: swapped, the released binary collects the new side and every regression reads as FLAG and every addition as FAIL", pair)
		}
	}
	owner := srctest.FuncBody(t, src, "func writeScratchConfigs(")
	if !strings.Contains(owner, "generateScratchConfig(") || !strings.Contains(owner, "dataTestCloneDir(workDir, side.name)") {
		t.Error("writeScratchConfigs must call generateScratchConfig with dataTestCloneDir(workDir, side.name) for each side")
	}

	// One USE of the identifier in the whole package (go/ast over every
	// declaration, so neither a comment nor an error string counts, and
	// a function value `gen := generateScratchConfig` — round 3 F1's
	// mutant — counts as a use), and it is a direct call in the owner.
	var uses []string
	for name, file := range srctest.PackageFiles(t, "cmd/aveloxis", 40) {
		uses = append(uses, usesOf(t, name, file, "generateScratchConfig")...)
	}
	if len(uses) != 1 || uses[0] != "cmd/aveloxis/data_test_cmd.go:writeScratchConfigs" {
		t.Errorf("generateScratchConfig is used at %v, want exactly [cmd/aveloxis/data_test_cmd.go:writeScratchConfigs] (a direct call): another caller, or a function value, can write a config without the side's clone dir", uses)
	}
}

// usesOf returns "<file>:<enclosing decl>" for every identifier use of
// the named package-level function in one Go source file, its own
// declaration excluded; a use that is not the callee of a call
// expression is suffixed "(ref)".
func usesOf(t *testing.T, name, src, fn string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var out []string
	for _, d := range f.Decls {
		where := name + ":"
		var skip *ast.Ident
		switch decl := d.(type) {
		case *ast.FuncDecl:
			where += decl.Name.Name
			if decl.Name.Name == fn {
				skip = decl.Name
			}
		case *ast.GenDecl:
			where += "(package-level " + decl.Tok.String() + ")"
		}
		callees := map[*ast.Ident]bool{}
		ast.Inspect(d, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok {
					callees[id] = true
				}
			}
			return true
		})
		ast.Inspect(d, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok || id.Name != fn || id == skip {
				return true
			}
			if callees[id] {
				out = append(out, where)
			} else {
				out = append(out, where+"(ref)")
			}
			return true
		})
	}
	return out
}

// dataTestSides is the one spelling of the scratch database names
// (round 2 F3): a rename there must not leave a site provisioning,
// connecting to or dropping a database under the old name.
func TestDataTestSidesAreTheOneSpelling(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/data_test_cmd.go"))
	table := src[strings.Index(src, "var dataTestSides"):]
	table = table[:strings.Index(table, "\n}\n")]
	for _, side := range dataTestSides {
		quoted := `"` + side.dbname + `"`
		if !strings.Contains(table, quoted) {
			t.Fatalf("dataTestSides must spell %s as a quoted literal", quoted)
		}
		// The BARE name: a raw-string SQL spelling, a longer help text
		// or a Long paragraph naming the database is a second spelling
		// too (round 3 F3) — operator-facing text reads the table.
		if n := strings.Count(src, side.dbname); n != 1 {
			t.Errorf("%s appears %d times in comment-stripped data_test_cmd.go, want once (the table); every other site, including help and Long text, reads dataTestSides", side.dbname, n)
		}
	}
	for _, derivation := range []string{`"aveloxis_" +`, `"aveloxis_"+`, `aveloxis_%`} {
		if strings.Contains(src, derivation) {
			t.Errorf("a scratch database name is derived with %s; read dataTestSides[i].dbname instead", derivation)
		}
	}
}

// The snapshot flag rules decide before anything runs (Copilot's review of
// cce17a6f): the --diff-only branch returned before checkSnapshotFlags, so
// `--from-db … --diff-only --expect …` ran the collection run's row-count
// comparison and could report PASSED without the release's rules or the
// catalog checks. Both combinations below are refused, with the rule's own
// message, and leave the work dir without a report.
func TestDataTestRefusesSnapshotFlagCombinationsBeforeAnyRun(t *testing.T) {
	for name, args := range map[string][]string{
		"from-db with diff-only":         {"--from-db", "aveloxis", "--diff-only"},
		"diff-only with a snapshot flag": {"--diff-only", "--expect", "testdata/expect-0.29.141.json"},
	} {
		t.Run(name, func(t *testing.T) {
			work := t.TempDir()
			// A kept collection run's record, so the diff-only path's own
			// rules let it through: the refusal must not lean on them.
			if err := os.WriteFile(filepath.Join(work, "repos.txt"), []byte("https://github.com/sunpy/sunpy\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := filepath.Join(t.TempDir(), "absent.json")
			cmd := dataTestCmd(&cfg)
			cmd.SetArgs(append([]string{"--released-tag", "origin/main", "--work-dir", work}, args...))
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			err := cmd.Execute()
			if err == nil || !(strings.Contains(err.Error(), "--diff-only re-diffs") || strings.Contains(err.Error(), "applies to the snapshot mode only")) {
				t.Fatalf("want the snapshot flag rule's refusal, got %v", err)
			}
			if _, statErr := os.Stat(filepath.Join(work, "report.md")); statErr == nil {
				t.Error("a refused run wrote a report")
			}
		})
	}
}
