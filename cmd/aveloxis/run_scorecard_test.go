// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — `aveloxis run-scorecard` (the bulk remote-primary
// scorecard catch-up pass). Behavioral tests for the two refusal
// guards (HOME-scoped pidfiles) + source-contract pins on wiring,
// flags, the shared invoke/persist path, and the v0.21.5 no-Migrate
// contract.

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pidfile"
)

func TestRunScorecardCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "runScorecardCmd(&cfgPath)") {
		t.Error("main.go must register runScorecardCmd on the root command")
	}
}

// isolateHome points pidfile.Dir() at a scratch dir so the guard tests
// can't see (or disturb) a real ~/.aveloxis.
func isolateHome(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	return filepath.Join(tmp, ".aveloxis")
}

func TestRunScorecardRefusesWhileServeRunning(t *testing.T) {
	isolateHome(t)

	// No serve pidfile → no refusal.
	if err := refuseIfServeRunning(); err != nil {
		t.Fatalf("no pidfile should not refuse: %v", err)
	}

	// A LIVE serve (simulated with our own pid — definitely alive) →
	// refusal with an actionable message.
	if err := pidfile.Write(pidfile.Path("serve"), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	err := refuseIfServeRunning()
	if err == nil {
		t.Fatal("run-scorecard must refuse while aveloxis serve is running on this host")
	}
	if !strings.Contains(err.Error(), "aveloxis stop serve") {
		t.Errorf("refusal message should tell the operator the recovery command, got: %v", err)
	}

	// Stale pidfile (serve exited without cleanup) → no refusal.
	pidfile.Remove(pidfile.Path("serve"))
	if err := refuseIfServeRunning(); err != nil {
		t.Fatalf("removed pidfile should not refuse: %v", err)
	}
}

func TestRunScorecardPidfileGuardPreventsOverlap(t *testing.T) {
	isolateHome(t)

	release, err := acquireRunScorecardPidfile()
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// A second bulk pass while the first is alive (our own pid) must
	// refuse.
	if _, err := acquireRunScorecardPidfile(); err == nil {
		t.Fatal("second acquire while the first run-scorecard is alive must refuse")
	}

	// After release, a new pass can start.
	release()
	release2, err := acquireRunScorecardPidfile()
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	release2()
}

func TestRunScorecardFlags(t *testing.T) {
	cfgPath := "aveloxis.json"
	cmd := runScorecardCmd(&cfgPath)

	if cmd.Use != "run-scorecard" {
		t.Errorf("Use = %q, want run-scorecard", cmd.Use)
	}
	for _, flag := range []string{"workers", "older-than", "limit"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("run-scorecard must declare --%s", flag)
		}
	}
}

func TestRunScorecardCapsWorkersAtNumCPU(t *testing.T) {
	src, err := os.ReadFile("run_scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "runtime.NumCPU()") {
		t.Error("run-scorecard's --workers default/cap must derive from runtime.NumCPU()")
	}
	if !strings.Contains(code, "workers > runtime.NumCPU()") {
		t.Error("run-scorecard must hard-cap --workers at runtime.NumCPU() — each " +
			"invocation is a full scorecard subprocess tree; oversubscription just " +
			"pushes every attempt toward its wall-clock timeout")
	}
}

// TestRunScorecardDoesNotMigrate pins the v0.21.5 contract: schema
// currency is serve/migrate's job; one-shot CLIs must not run
// store.Migrate (comments stripped so the explanatory citation doesn't
// false-match — the v0.21.5 lesson re-learned in v0.27.4's
// heal-vulnerabilities test).
func TestRunScorecardDoesNotMigrate(t *testing.T) {
	src, err := os.ReadFile("run_scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, line := range strings.Split(string(src), "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	if strings.Contains(sb.String(), ".Migrate(") {
		t.Error("run-scorecard must NOT call store.Migrate — v0.21.5: only serve and " +
			"the migrate subcommand run migrations")
	}
}

// TestRunScorecardUsesSharedScorecardPath pins the no-duplicated-logic
// contract: the bulk pass composes the SAME collector.RunScorecard
// invoke/persist code as the per-cycle phase (options struct, pooled
// multi-token, remote-primary) instead of shelling out to scorecard
// itself.
func TestRunScorecardUsesSharedScorecardPath(t *testing.T) {
	src, err := os.ReadFile("run_scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, needle := range []string{
		"collector.RunScorecard(",
		"collector.ScorecardTokens(",
		"RemotePrimary:   true",
		"ScorecardTimeout()",
		"ScorecardTokenCountOrDefault()",
		"ListScorecardBacklog(",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("run_scorecard.go must contain %q — the bulk pass shares the "+
				"phase's invoke/persist code and config knobs, never a parallel implementation", needle)
		}
	}
	if strings.Contains(code, `exec.CommandContext`) {
		t.Error("run_scorecard.go must not exec subprocesses itself — scorecard " +
			"invocation lives in collector.RunScorecard")
	}
}

// Borrowing history: Copilot review on PR #203 moved the loan from once
// for the whole pass into each worker goroutine (a shared loan
// under-counted `lent` and concentrated every worker on the same keys).
// v0.29.10 moves it into each REPO, as defense in depth: this process's
// pool sends no forge traffic, so its keys never rest today, but a loan
// held for a whole pass would freeze the usable set chosen at worker start
// if that ever changed. Pinned on the parsed AST: the ScorecardTokens call
// and the collector.RunScorecard call it feeds sit in ONE function literal
// inside the `for r := range jobs` loop body, with `defer releaseTokens()`
// in that literal — so the loan outlives the subprocess and is released
// when the repo finishes, on every exit path.
func TestRunScorecardBorrowsTokensPerRepo(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "run_scorecard.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var loops []*ast.RangeStmt
	ast.Inspect(file, func(n ast.Node) bool {
		if rs, ok := n.(*ast.RangeStmt); ok {
			if id, ok := rs.X.(*ast.Ident); ok && id.Name == "jobs" {
				loops = append(loops, rs)
			}
		}
		return true
	})
	if len(loops) != 1 {
		t.Fatalf("want exactly one `for ... range jobs` worker loop, found %d", len(loops))
	}
	isCall := func(n ast.Node, pkg, name string) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != name {
			return false
		}
		x, ok := sel.X.(*ast.Ident)
		return ok && x.Name == pkg
	}
	countIn := func(root ast.Node, pkg, name string) int {
		n := 0
		ast.Inspect(root, func(x ast.Node) bool {
			if isCall(x, pkg, name) {
				n++
			}
			return true
		})
		return n
	}
	if total, inLoop := countIn(file, "collector", "ScorecardTokens"), countIn(loops[0].Body, "collector", "ScorecardTokens"); total != 1 || inLoop != 1 {
		t.Fatalf("collector.ScorecardTokens: %d call(s) in the file, %d inside the jobs loop — want exactly one, inside the loop (one loan per repo)", total, inLoop)
	}
	// The loan and its deferred release live in one function literal inside
	// the loop body: a defer directly in the loop body would hold every loan
	// until the worker goroutine exits.
	found := false
	ast.Inspect(loops[0].Body, func(n ast.Node) bool {
		lit, ok := n.(*ast.FuncLit)
		if !ok {
			return true
		}
		if countIn(lit.Body, "collector", "ScorecardTokens") != 1 {
			return true
		}
		// The subprocess must run INSIDE the literal: a literal that only
		// borrows and hands the token out would release the loan before
		// scorecard starts (review of this pin: that mutation passed).
		if countIn(lit.Body, "collector", "RunScorecard") != 1 {
			t.Error("collector.RunScorecard must be called inside the same function literal that borrows the tokens, or the deferred release frees the loan before the subprocess runs")
			return false
		}
		for _, stmt := range lit.Body.List {
			if d, ok := stmt.(*ast.DeferStmt); ok {
				if id, ok := d.Call.Fun.(*ast.Ident); ok && id.Name == "releaseTokens" {
					found = true
				}
			}
		}
		return false
	})
	if !found {
		t.Error("the per-repo loan must be released by `defer releaseTokens()` inside a function literal in the jobs loop body")
	}
	for _, stmt := range loops[0].Body.List {
		if d, ok := stmt.(*ast.DeferStmt); ok {
			t.Errorf("a defer directly in the jobs loop body (line %d) runs only when the worker exits", fset.Position(d.Pos()).Line)
		}
	}
}
