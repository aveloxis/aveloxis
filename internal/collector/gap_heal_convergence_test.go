// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// gap_heal_convergence_test.go — v0.27.146: SR-19's flagship. The
// heal-collection-gaps contract is "rerun until 0 candidates"; round
// 23 proved the contract was a promise (healed repos never left the
// candidate set). This test DRIVES the loop to done through the real
// machinery: candidate selected → GapFiller lists, diffs, fetches the
// missing issues from an httptest GitHub, stages, processes → queue
// counts refreshed → the candidate set is EMPTY.

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// Enforces SR-19 (scripts/standing_rules.go).
func TestGapHealConvergesToZeroCandidates(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const owner, repoName = "_avgapheal", "converge"
	repoID := int64(944_146_001)

	// Seed: a collected GitHub repo storing 1 of 3 issues — a
	// count-gap candidate (meta issues 3 > stored 1; no PR gap).
	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := store.Pool().Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, $3, $4, 1) ON CONFLICT (repo_id) DO NOTHING`,
		repoID, "https://github.com/"+owner+"/"+repoName, owner, repoName)
	mustExec(`INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_collected, last_issues, last_prs)
		VALUES ($1, 100, 'queued', NOW(), NOW() - INTERVAL '1 day', 1, 0)
		ON CONFLICT (repo_id) DO UPDATE SET status='queued', last_collected = NOW() - INTERVAL '1 day',
			last_issues = 1, last_prs = 0`, repoID)
	mustExec(`INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date)
		VALUES ($1, 3, 0, NOW())`, repoID)
	mustExec(`INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, issue_state, created_at)
		VALUES ($1, 944146101, 1, 'stored issue', 'closed', NOW() - INTERVAL '30 days')
		ON CONFLICT (repo_id, platform_issue_id) DO NOTHING`, repoID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		type cq struct {
			sql  string
			args []any
		}
		for _, q := range []cq{
			{`DELETE FROM aveloxis_ops.staging WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_data.issue_message_ref WHERE issue_id IN (SELECT issue_id FROM aveloxis_data.issues WHERE repo_id = $1)`, []any{repoID}},
			{`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_data.messages WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_ops.collection_status WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = '_avgapheal-alice')`, nil},
			// contributor_login_history is a cntrb_id child with ON DELETE
			// RESTRICT (v0.23.0) and every resolved login writes one — the
			// contributors DELETE below fails with 23001 until it is gone,
			// which stranded the fixture's contributor on every run
			// (children before parents; pass 44).
			{`DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = '_avgapheal-alice')`, nil},
			{`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = '_avgapheal-alice'`, nil},
			{`DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, []any{repoID}},
			{`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, []any{repoID}},
		} {
			if _, derr := store.Pool().Exec(cctx, q.sql, q.args...); derr != nil {
				t.Logf("cleanup %q: %v", q.sql, derr)
			}
		}
	})

	// Precondition: the repo IS a candidate.
	inCandidates := func() *db.GapHealCandidate {
		t.Helper()
		cands, cerr := store.GetGapHealCandidates(ctx, repoID-1, 50, false)
		if cerr != nil {
			t.Fatal(cerr)
		}
		for i := range cands {
			if cands[i].RepoID == repoID {
				return &cands[i]
			}
		}
		return nil
	}
	before := inCandidates()
	if before == nil {
		t.Fatal("seeded gap-bearing repo must appear in the candidate set")
	}
	if before.Gap != 2 {
		t.Fatalf("seeded count gap = %d, want 2", before.Gap)
	}

	// The httptest forge: the listing knows issues 1-3; per-item
	// fetches serve each; comments are empty.
	issue := func(n int) map[string]any {
		return map[string]any{
			"id": 944146100 + n, "number": n,
			"title": fmt.Sprintf("issue %d", n), "state": "closed",
			"user":       map[string]any{"login": "_avgapheal-alice", "id": 944146142},
			"created_at": "2026-01-02T03:04:05Z",
			"updated_at": "2026-01-02T03:04:05Z",
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/"+owner+"/"+repoName+"/issues", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{issue(3), issue(2), issue(1)})
	})
	for _, n := range []int{1, 2, 3} {
		mux.HandleFunc(fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repoName, n), func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(issue(n))
		})
		mux.HandleFunc(fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, repoName, n), func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode([]map[string]any{})
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	client := github.New(srv.URL, keys, logger)

	// The healer's per-repo sequence, exactly as heal-collection-gaps
	// runs it: threshold-0 assess-and-fill, then the counter refresh.
	gf := NewGapFillerWithMode(store, client, logger, "rest")
	filled, ferr := gf.AssessAndFillGapsWithThreshold(ctx, repoID, owner, repoName, 3, 0, 0)
	if ferr != nil {
		t.Fatalf("gap fill: %v", ferr)
	}
	if filled < 2 {
		t.Fatalf("expected the 2 missing issues filled, got %d", filled)
	}
	if rerr := store.RefreshQueueGatheredCounts(ctx, repoID); rerr != nil {
		t.Fatal(rerr)
	}

	// CONVERGENCE: the data is whole and the candidate set is DRY.
	var stored int
	if err := store.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 3 {
		t.Fatalf("stored issues = %d, want 3", stored)
	}
	if after := inCandidates(); after != nil {
		t.Fatalf("healed repo must LEAVE the candidate set (rerun-until-0 is the contract), still present with gap=%d", after.Gap)
	}
}
