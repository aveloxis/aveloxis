// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Manual scale-validation harness for the whitespace walker (v0.27.105).
// Gated on AVELOXIS_WS_SCALE_REPO pointing at a real local git checkout;
// self-skips otherwise. 2026-08-19 baseline on chaoss/augur (12,618
// commits / 41,039 file rows, M-series laptop): numstat pass 10.7s,
// whitespace walk 12.9s, 12,805 rows (31%) gained whitespace data —
// the walk costs about the same as the numstat pass per repo. Also the
// operator's canary harness before a fleet rewalk-whitespace run.
package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

func TestWhitespaceScaleValidation(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	repoDir := os.Getenv("AVELOXIS_WS_SCALE_REPO")
	if dsn == "" || repoDir == "" {
		t.Skip("set AVELOXIS_TEST_DB and AVELOXIS_WS_SCALE_REPO")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGenericGit,
		GitURL:   "https://scale.invalid/avwsscale/repo",
		Owner:    "avwsscale", Name: "repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commit_parents WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commits WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.repos WHERE repo_id=$1`, repoID)
	})

	fc := NewFacadeCollector(store, logger, t.TempDir())
	res := &FacadeResult{}
	t0 := time.Now()
	if err := fc.parseGitLog(ctx, repoID, repoDir, res); err != nil {
		t.Fatalf("parseGitLog: %v", err)
	}
	numstatDur := time.Since(t0)

	t1 := time.Now()
	updated, head, err := fc.runWhitespaceWalk(ctx, repoID, repoDir, "")
	if err != nil {
		t.Fatalf("runWhitespaceWalk: %v", err)
	}
	walkDur := time.Since(t1)

	var totalRows, wsRows, adjRows int64
	if err := store.Pool().QueryRow(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE COALESCE(cmt_whitespace,0) > 0),
		       COUNT(*) FILTER (WHERE COALESCE(cmt_whitespace,0) > 0 OR COALESCE(cmt_added,0)+COALESCE(cmt_removed,0) > 0)
		FROM aveloxis_data.commits WHERE repo_id=$1`, repoID).Scan(&totalRows, &wsRows, &adjRows); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("SCALE: commits=%d file_rows=%d numstat=%s walk=%s rows_updated=%d ws_rows=%d head=%s\n",
		res.Commits, totalRows, numstatDur.Round(time.Millisecond), walkDur.Round(time.Millisecond),
		updated, wsRows, head[:8])
	if updated == 0 || wsRows == 0 {
		t.Fatal("scale walk produced no whitespace data on a real repo — suspicious")
	}
}
