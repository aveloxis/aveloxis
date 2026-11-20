// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"sort"
	"strings"
	"testing"
)

// The name inventory (repo_dependencies) is replaced whole, in one
// transaction (0.29.158, operator 2026-11-19). Before, the table was cleared
// in one statement and each row inserted in its own, so a failure part-way
// left it half-filled, and an unreadable clone emptied it.

func repoDependencyRows(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) string {
	t.Helper()
	rows, err := store.pool.Query(ctx, `
		SELECT dep_language || '/' || dep_name || 'x' || dep_count
		FROM aveloxis_data.repo_dependencies WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func TestReplaceRepoDependenciesReplacesTheSnapshot(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedRepoForDeps(t, store, ctx, "_avdeprep", "replace")

	if err := store.ReplaceRepoDependencies(ctx, repoID, []RepoDependency{
		{Language: "Python", Name: "flask", Count: 2}, {Language: "JavaScript", Name: "express", Count: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if got := repoDependencyRows(t, store, ctx, repoID); got != "JavaScript/expressx1 Python/flaskx2" {
		t.Fatalf("after the first replace: %q", got)
	}
	if err := store.ReplaceRepoDependencies(ctx, repoID, []RepoDependency{{Language: "Go", Name: "github.com/pkg/errors", Count: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := repoDependencyRows(t, store, ctx, repoID); got != "Go/github.com/pkg/errorsx1" {
		t.Errorf("after the second replace: %q; want only the new snapshot", got)
	}
	// Nothing declared is a whole snapshot too.
	if err := store.ReplaceRepoDependencies(ctx, repoID, nil); err != nil {
		t.Fatal(err)
	}
	if got := repoDependencyRows(t, store, ctx, repoID); got != "" {
		t.Errorf("after an empty replace: %q; want no rows", got)
	}
}

// All or nothing: one row PostgreSQL refuses leaves the previous snapshot
// untouched, whichever batch it is in.
func TestReplaceRepoDependenciesIsAllOrNothing(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedRepoForDeps(t, store, ctx, "_avdeprep", "atomic")
	if err := store.ReplaceRepoDependencies(ctx, repoID, []RepoDependency{{Language: "Python", Name: "flask", Count: 1}}); err != nil {
		t.Fatal(err)
	}
	rows := make([]RepoDependency, 0, repoDependenciesReplaceBatchSize+2)
	for i := range repoDependenciesReplaceBatchSize + 1 {
		rows = append(rows, RepoDependency{Language: "JavaScript", Name: "pkg-" + itoa(i), Count: 1})
	}
	rows = append(rows, RepoDependency{Language: "JavaScript", Name: "bad\x00name", Count: 1})
	if err := store.ReplaceRepoDependencies(ctx, repoID, rows); err == nil {
		t.Fatal("a row PostgreSQL refuses must fail the replace")
	}
	if got := repoDependencyRows(t, store, ctx, repoID); got != "Python/flaskx1" {
		t.Errorf("after a failed replace: %q; want the previous snapshot untouched", got)
	}
}
