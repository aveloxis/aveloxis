// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// D7 (0.29.120, summary/63): the leftover-staging drain processes staged
// rows outside any job, and its synchronous fallback neither parks nor
// releases the queue row: drainOneRepo stamps the repository, so the API's
// cached answers follow what it wrote.
func TestDrainOneRepoStampsTheRepository(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'r', '_avdrainstamp', 1) RETURNING repo_id`, "https://github.com/_avdrainstamp/r"+time.Now().Format("150405.000000")).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	s := &Scheduler{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.drainOneRepo(ctx, repoID)
	var at *time.Time
	if err := store.Pool().QueryRow(ctx, `SELECT data_changed_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&at); err != nil {
		t.Fatal(err)
	}
	if at == nil {
		t.Error("drainOneRepo must stamp the repository's data_changed_at after processing its staging")
	}
}
