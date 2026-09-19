// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package prepare

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, Deployment, Verify)) }

// Verify must fail on a FAIL finding — the gate that catches a test leaving
// invariant-violating rows in its package's database. Seeded: a collected
// repo whose queue row claims issues it does not have ("cached counts vs
// actual"). The L10 pass found nothing drove this path: residue gives the
// FAIL probes nothing to examine, so a Verify that never failed passed
// every package.
func TestVerifyFailsOnAFAILFinding(t *testing.T) {
	dsn := os.Getenv(testdb.EnvVar)
	if dsn == "" {
		t.Skip(testdb.EnvVar + " not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	if err := Verify(ctx, dsn); err != nil {
		t.Fatalf("a freshly prepared database must verify clean, got %v", err)
	}

	url := fmt.Sprintf("https://github.com/_avprepverify/r%d", time.Now().UnixNano())
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, url)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, url)
	})
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: url, Owner: "_avprepverify", Name: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueRepo(ctx, repoID, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = NOW(), last_issues = 5, last_prs = 0, status = 'queued' WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}

	err = Verify(ctx, dsn)
	if err == nil || !strings.Contains(err.Error(), "cached counts vs actual") {
		t.Fatalf("Verify must fail on the cached-count FAIL, got %v", err)
	}
}

// Copilot on PR #210: the preparation built every materialized view in
// every package's database. No DB-tier test reads one, and the testing
// convention is to skip them unless a test is about views;
// TestRunMigrationsOnFreshDB checks that each builds on an empty database.
func TestDeploymentLeavesTheViewsOut(t *testing.T) {
	dsn := os.Getenv(testdb.EnvVar)
	if dsn == "" {
		t.Skip(testdb.EnvVar + " not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	var views int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data'`).Scan(&views); err != nil {
		t.Fatal(err)
	}
	if views != 0 {
		t.Errorf("the prepared database has %d materialized views; the preparation should skip them", views)
	}
}
