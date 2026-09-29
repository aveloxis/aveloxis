// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestGoModGraphFailureLinesNameTheRepo — worklist item 80 (Stage 1,
// observation only): the toolchain failure lines carried the module
// directory but not the repository, so "which repos have an empty Go
// closure" needed a join against the incomplete-expansion line by time.
// Every expansion line names repo_id.
func TestGoModGraphFailureLinesNameTheRepo(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("this is not a go.mod\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	_, _, complete := ac.scanGoModGraph(context.Background(), 4242, dir, map[string]bool{})
	if complete {
		t.Fatal("a broken go.mod must make the expansion incomplete")
	}
	if !strings.Contains(logs.String(), "go toolchain invocation failed") || !strings.Contains(logs.String(), "repo_id=4242") {
		t.Errorf("the toolchain failure line must name repo_id:\n%s", logs.String())
	}
}

// TestGoModGraphBudgetLineNamesTheRepo drives the budget-exhausted arm:
// a parent context already past its deadline makes the derived budget
// report DeadlineExceeded (not a shutdown) at the loop top.
func TestGoModGraphBudgetLineNamesTheRepo(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/m\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	var logs strings.Builder
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	if _, _, complete := ac.scanGoModGraph(ctx, 4343, dir, map[string]bool{}); complete {
		t.Fatal("an exhausted budget must make the expansion incomplete")
	}
	if !strings.Contains(logs.String(), "go mod graph repo budget exhausted — skipping remaining modules") || !strings.Contains(logs.String(), "repo_id=4343") {
		t.Errorf("the budget line must name repo_id:\n%s", logs.String())
	}
}
