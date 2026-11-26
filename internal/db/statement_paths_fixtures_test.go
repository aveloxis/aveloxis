// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Fixtures of the tests that send, and check, the store's statements no
// other DB-tier test reached (0.29.214; operator decision 2026-11-20: the
// baseline of unexecuted statement sites is closed, not kept). Each of
// those tests seeds what its statement reads, calls the production
// function, and asserts what the statement did.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var pathsSeq atomic.Int64

// pathsTag is a name no other row of the package's database has.
func pathsTag(what string) string {
	return fmt.Sprintf("zzpaths-%s-%d-%d", what, time.Now().UnixNano()%1_000_000_000, pathsSeq.Add(1))
}

// pathsRepo inserts a repository and returns its id, owner and name. The
// rows named by children (tables with a repo_id column) are deleted before
// it at cleanup, in the order given.
func pathsRepo(t *testing.T, store *PostgresStore, what string, children ...string) (id int64, owner, name string) {
	t.Helper()
	ctx := context.Background()
	owner, name = pathsTag(what), "r"
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, $3, 1) RETURNING repo_id`, "https://github.com/"+owner+"/"+name, owner, name).Scan(&id); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	t.Cleanup(func() {
		c := context.Background()
		for _, table := range children {
			cleanupExecRetry(c, store, `DELETE FROM `+table+` WHERE repo_id = $1`, id)
		}
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	return id, owner, name
}

// pathsPR inserts a pull request of the repository and returns its id.
func pathsPR(t *testing.T, store *PostgresStore, repoID int64, number int, state string) int64 {
	t.Helper()
	var id int64
	if err := store.pool.QueryRow(context.Background(), `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, pr_state)
		VALUES ($1, $2, $3, $4) RETURNING pull_request_id`, repoID, repoID*1000+int64(number), number, state).Scan(&id); err != nil {
		t.Fatalf("seed pull request: %v", err)
	}
	return id
}

// pathsIssue inserts an issue of the repository and returns its id.
func pathsIssue(t *testing.T, store *PostgresStore, repoID int64, number int) int64 {
	t.Helper()
	var id int64
	if err := store.pool.QueryRow(context.Background(), `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number)
		VALUES ($1, $2, $3) RETURNING issue_id`, repoID, repoID*1000+int64(number), number).Scan(&id); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	return id
}

// pathsInt is one number read from the database.
func pathsInt(t *testing.T, store *PostgresStore, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := store.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// pathsText is one text read from the database ("" for NULL).
func pathsText(t *testing.T, store *PostgresStore, sql string, args ...any) string {
	t.Helper()
	var s *string
	if err := store.pool.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if s == nil {
		return ""
	}
	return *s
}
