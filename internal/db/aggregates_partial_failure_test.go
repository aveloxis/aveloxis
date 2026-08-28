// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 4 on PR #191: RefreshAllRepoAggregates logged and dropped
// every per-repo / per-group failure and returned nil, so
// `refresh-views --aggregates` exited 0 over stale dm_ rows. The pass
// keeps going but returns the accumulated failures (bounded), and the
// operator command therefore exits nonzero.
func TestRefreshAllRepoAggregatesReturnsPartialFailures(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, readSourceFile(t, "aggregates.go"), "func (s *PostgresStore) RefreshAllRepoAggregates("))
	for _, needle := range []string{
		`failed = append(failed, fmt.Errorf("repo %d: %w", repoID, err))`,
		`failed = append(failed, fmt.Errorf("group of repo %d: %w", repoID, err))`,
		"if len(failed) > 0 {",
		"return boundedJoin(",
		`"failed_repos", failedRepos, "failed_groups", failedGroups`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("RefreshAllRepoAggregates must accumulate and return per-repo failures: missing %q", needle)
		}
	}
	if strings.Contains(body, `logger.Info("aggregate refresh failed"`) || strings.Contains(body, `logger.Info("group aggregate refresh failed"`) {
		t.Error("a refresh failure is a WARN, not an INFO")
	}
	// Pass 27: both loops bail on a canceled ctx (one exit, not one WARN
	// per remaining repo), and the group-query error keeps the repo
	// failures accumulated before it.
	if strings.Count(body, "if ctx.Err() != nil {") < 2 {
		t.Error("both per-repo loops must check ctx.Err() before each iteration")
	}
	if !strings.Contains(body, `append(failed, fmt.Errorf("querying repo groups: %w", err))`) {
		t.Error("a repo-groups query failure must return the already-accumulated repo failures with it")
	}
	// One call deeper: a repo_group lookup ERROR must surface, not read
	// as "no group" (pass 26 — a canceled pass reported failed_groups=0).
	group := srctest.StripGoComments(srctest.FuncBody(t, readSourceFile(t, "aggregates.go"), "func (s *PostgresStore) RefreshRepoGroupAggregates("))
	if !strings.Contains(group, `return fmt.Errorf("repo_group for repo %d: %w", repoID, err)`) || strings.Contains(group, "err != nil || rgID == nil") {
		t.Error("RefreshRepoGroupAggregates must return a repo_group lookup error (SR-5), never fold it into the no-group nil")
	}
	// The matview half of the same command has the same contract.
	views := srctest.StripGoComments(srctest.FuncBody(t, readSourceFile(t, "matviews.go"), "func RefreshMaterializedViews("))
	for _, needle := range []string{`failed = append(failed, fmt.Errorf("view %s: %w", name, err))`, "return boundedJoin("} {
		if !strings.Contains(views, needle) {
			t.Errorf("RefreshMaterializedViews must accumulate and return per-view failures: missing %q", needle)
		}
	}
	// The CLI returns the pass's error verbatim (nonzero exit), and the
	// weekly caller still distinguishes the lock skip from a failure.
	if !strings.Contains(readSourceFile(t, "../../cmd/aveloxis/main.go"), "return errors.Join(viewErr, aggErr)") {
		t.Error("refresh-views --aggregates must return the aggregate pass's error joined with the view half's (nonzero exit on partial failure, and the aggregate pass runs even when a view failed)")
	}
}

func TestBoundedJoin(t *testing.T) {
	if boundedJoin("x", nil, 3) != nil {
		t.Fatal("no errors → nil")
	}
	sentinel := errors.New("boom")
	var errs []error
	for i := 0; i < 25; i++ {
		errs = append(errs, fmt.Errorf("repo %d: %w", i, sentinel))
	}
	err := boundedJoin("dm_ refresh: 25 failed", errs, 10)
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is must reach the joined errors, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "(first 10 of 25)") || strings.Contains(msg, "repo 10:") || !strings.Contains(msg, "repo 9:") {
		t.Errorf("bounded join must keep exactly the first 10 and state the full count: %s", msg)
	}
	if got := boundedJoin("all", errs[:3], 10).Error(); !strings.Contains(got, "(first 3 of 3)") {
		t.Errorf("fewer errors than the cap keeps them all: %s", got)
	}
}
