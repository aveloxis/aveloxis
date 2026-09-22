// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/testdb"
)

// VerifyFailures is the residue gate every test package's database passes
// before it is dropped (v0.29.57, internal/testdb): only FAIL findings fail it.
func TestVerifyFailures(t *testing.T) {
	if err := VerifyFailures([]VerifyResult{
		{Check: "stranded repos", Severity: "WARN", Detail: "1 non-archived repo"},
		{Check: "cached counts vs actual", Severity: "OK", Detail: "0 sampled"},
	}); err != nil {
		t.Errorf("WARN and OK findings must pass, got %v", err)
	}
	if err := VerifyFailures(nil); err != nil {
		t.Errorf("no findings must pass, got %v", err)
	}
	err := VerifyFailures([]VerifyResult{
		{Check: "cached counts vs actual", Severity: "FAIL", Detail: "1 of 1 sampled repos disagree"},
		{Check: "stranded repos", Severity: "WARN", Detail: "ignored"},
		{Check: "default repo_group singleton", Severity: "FAIL", Detail: "2 'Default' groups"},
	})
	if err == nil {
		t.Fatal("FAIL findings must fail the gate")
	}
	for _, want := range []string{"2 FAIL", "cached counts vs actual: 1 of 1 sampled repos disagree", "default repo_group singleton: 2 'Default' groups"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must carry %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "ignored") {
		t.Errorf("WARN findings must not be reported as failures: %v", err)
	}
}

// The verify hook this package's TestMain runs must fail on residue the
// battery rates FAIL (L10 round 11: its inline copy was pinned by nothing — a
// `_ = VerifyFailures(…)` mutant passed a package whose test leaked such a
// row). Seeded: a collected repo whose queue row claims issues it does not
// have; removed again, so the real post-run check stays clean.
func TestVerifyThisPackageFailsOnAFAILFinding(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	dsn := os.Getenv(testdb.EnvVar)
	if err := verifyThisPackage(ctx, dsn); err != nil {
		t.Fatalf("this package's database must verify clean before the seed, got %v", err)
	}
	url := fmt.Sprintf("https://github.com/_avverifyhook/r%d", time.Now().UnixNano())
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, url)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, url)
	})
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: url, Owner: "_avverifyhook", Name: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 100); err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.collection_queue SET last_collected = NOW(), last_issues = 5, last_prs = 0, status = 'queued' WHERE repo_id = $1`, repoID)
	if err := verifyThisPackage(ctx, dsn); err == nil || !strings.Contains(err.Error(), "cached counts vs actual") {
		t.Fatalf("the verify hook must fail on the cached-count FAIL, got %v", err)
	}
}
