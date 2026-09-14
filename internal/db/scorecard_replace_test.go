// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review on PR #203 (operator-marked): persistScorecard's mode
// check, rotation and inserts were separate statements, so a local
// writer could read "not remote", a concurrent remote writer could
// rotate+insert, and the local writer then rotated that complete set
// away — the partial-never-replaces-complete invariant (D9) violated by
// interleaving. The two writers are real: serve's per-cycle phase and
// the `run-scorecard` bulk pass are separate processes on the same
// repo. ReplaceScorecard fuses check + rotate + insert into ONE
// transaction under a per-repo advisory lock (SR-18: the owning layer
// enforces; feedback_fused_operation_must_carry_every_property).

package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestReplaceScorecardShape(t *testing.T) {
	src := readSourceFile(t, "analysis_store.go")
	i := strings.Index(src, "func (s *PostgresStore) ReplaceScorecard(")
	if i < 0 {
		t.Fatal("ReplaceScorecard missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	for _, needle := range []string{
		"pg_advisory_xact_lock(",
		"s.pool.Begin(ctx)",
		"rotateRepoRowsToHistory(ctx, tx,",
		"tx.Commit(ctx)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("ReplaceScorecard must contain %q", needle)
		}
	}
	if strings.Count(body, "s.pool.Begin(ctx)") != 1 {
		t.Error("ReplaceScorecard must open exactly one transaction")
	}
}

func seedScorecardRepo(t *testing.T, store *PostgresStore, ctx context.Context, slug string) int64 {
	t.Helper()
	var repoID int64
	err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, 'https://github.com/'||$1||'/it-' || floor(random()*1e9)::text, $1, 'it')
		RETURNING repo_id`, slug).Scan(&repoID)
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id = $1`, repoID)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, repoID)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return repoID
}

func currentScorecardSet(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) (modes map[string]int, n int) {
	t.Helper()
	rows, err := store.pool.Query(ctx,
		`SELECT COALESCE(scorecard_mode,'') FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	modes = map[string]int{}
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			t.Fatal(err)
		}
		modes[m]++
		n++
	}
	return modes, n
}

// D9 policy as the collector states it, reproduced here so the store
// test drives the real callback shape.
func d9Allow(mode string) func(stored string, found bool) bool {
	return func(stored string, found bool) bool {
		return mode == "remote" || !(found && stored == "remote")
	}
}

func TestReplaceScorecardAtomicAndSerialized(t *testing.T) {
	store, ctx := scorecardModeConnect(t)
	t.Cleanup(store.pool.Close)
	repoID := seedScorecardRepo(t, store, ctx, "_avscreplace")

	remote := []ScorecardRow{{Name: ScorecardOverallName, Score: "7.0"}, {Name: "Code-Review", Score: "8"}, {Name: "Maintained", Score: "10"}}
	local := []ScorecardRow{{Name: ScorecardOverallName, Score: "3.0"}, {Name: "Binary-Artifacts", Score: "10"}}

	// Empty store: local writes.
	written, err := store.ReplaceScorecard(ctx, repoID, "local", local, d9Allow("local"))
	if err != nil || !written {
		t.Fatalf("local over empty: written=%v err=%v", written, err)
	}
	// Remote replaces local.
	if written, err = store.ReplaceScorecard(ctx, repoID, "remote", remote, d9Allow("remote")); err != nil || !written {
		t.Fatalf("remote over local: written=%v err=%v", written, err)
	}
	modes, n := currentScorecardSet(t, store, ctx, repoID)
	if n != 3 || modes["remote"] != 3 {
		t.Fatalf("after remote: modes=%v n=%d, want 3 remote rows", modes, n)
	}
	// Local over remote is refused: NOTHING written, nothing rotated.
	var hist int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id = $1`, repoID).Scan(&hist); err != nil {
		t.Fatal(err)
	}
	if written, err = store.ReplaceScorecard(ctx, repoID, "local", local, d9Allow("local")); err != nil || written {
		t.Fatalf("local over remote: written=%v err=%v, want refused", written, err)
	}
	modes, n = currentScorecardSet(t, store, ctx, repoID)
	if n != 3 || modes["remote"] != 3 {
		t.Fatalf("a refused replacement must leave the current set intact: modes=%v n=%d", modes, n)
	}
	var hist2 int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id = $1`, repoID).Scan(&hist2); err != nil {
		t.Fatal(err)
	}
	if hist2 != hist {
		t.Fatalf("a refused replacement must not rotate: history %d → %d", hist, hist2)
	}

	// The interleaving the review named, driven for real: many local
	// and remote writers racing on one repo. Under the lock every
	// writer's check sees the committed state of the previous one, so
	// a local writer can never rotate a remote set away. Final state:
	// exactly one current set, and it is remote.
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := store.ReplaceScorecard(ctx, repoID, "local", local, d9Allow("local")); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := store.ReplaceScorecard(ctx, repoID, "remote", remote, d9Allow("remote")); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent ReplaceScorecard: %v", err)
	}
	modes, n = currentScorecardSet(t, store, ctx, repoID)
	if n != 3 || modes["remote"] != 3 {
		t.Fatalf("after 32 racing writers the current set must be the single remote set: modes=%v n=%d", modes, n)
	}

	// A canceled context inside the transaction writes nothing (SR-5:
	// an unknowable state is not "no prior set").
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.ReplaceScorecard(dead, repoID, "remote", remote, d9Allow("remote")); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: err=%v, want context.Canceled", err)
	}
	modes, n = currentScorecardSet(t, store, ctx, repoID)
	if n != 3 || modes["remote"] != 3 {
		t.Fatalf("a failed transaction must leave the set intact: modes=%v n=%d", modes, n)
	}
}

// TestScorecardWritersCannotSkipRotation — the repo_labor precedent
// (v0.27.7, TestRepoLaborWritersCannotSkipRotation) applied to
// scorecard after the PR #203 fusion: exactly ONE literal INSERT into
// repo_deps_scorecard across non-test Go sources, inside
// ReplaceScorecard, and the three removed unfused methods never come
// back — a bare writer is the bypass the interface pin alone cannot
// see (it bans names on the collector's interface text, not on the
// store).
func TestScorecardWritersCannotSkipRotation(t *testing.T) {
	insertRe := regexp.MustCompile(`INSERT INTO aveloxis_data\.repo_deps_scorecard\b`)
	var hits []string
	for _, root := range []string{".", "../collector", "../api", "../scheduler", "../web", "../monitor", "../../cmd"} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			src := stripLineComments(string(data))
			if insertRe.MatchString(src) {
				hits = append(hits, path)
			}
			for _, banned := range []string{
				"func (s *PostgresStore) InsertScorecardResult(",
				"func (s *PostgresStore) RotateScorecardToHistory(",
				"func (s *PostgresStore) CurrentScorecardMode(",
				"InsertScorecardResult(ctx", "RotateScorecardToHistory(ctx", "CurrentScorecardMode(ctx",
			} {
				if strings.Contains(src, banned) {
					t.Errorf("%s revives a removed unfused scorecard writer/reader (%q) — use ReplaceScorecard, which fuses check + rotation + insert under the per-repo lock so interleaving is impossible", path, banned)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	if len(hits) != 1 || !strings.HasSuffix(hits[0], "analysis_store.go") {
		t.Fatalf("expected exactly ONE literal `INSERT INTO aveloxis_data.repo_deps_scorecard` across non-test Go sources (inside analysis_store.go's ReplaceScorecard); found %d: %v", len(hits), hits)
	}
	body := extractFunctionBody(t, "analysis_store.go", "ReplaceScorecard")
	if !insertRe.MatchString(body) {
		t.Error("the single repo_deps_scorecard INSERT must live inside ReplaceScorecard — anywhere else and the check/rotation can be skipped")
	}
}
