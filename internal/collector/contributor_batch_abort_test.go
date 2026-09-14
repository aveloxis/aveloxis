// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL driver for the contributor-batch abort contract (F6 of the
// 2026-09-11 chaoss.tv log analysis). Processor.processBatch's
// contributor arm logged a Warn and returned nil when
// UpsertContributorBatch failed, and PostgresStore.ProcessStaged then
// proceeded to markStagedProcessed — so the staged contributor payloads
// were marked processed and never re-driven. Silent, permanent loss of
// a whole batch's contributors on any transient batch-level failure.
//
// THE DRIVER, and why it is a real failure rather than a seam: a
// batch-level error from UpsertContributorBatch is reachable only from
// the savepoint machinery, the commit, or the connection —
// upsertOneContributor swallows every per-contributor failure through
// captureErr, so no payload can fail a single row loudly. But v0.22.7
// made EVERY FK in the schema DEFERRABLE INITIALLY DEFERRED, which
// moved the check from the statement to the COMMIT — past the
// savepoints. So a staged identity naming a platform_id that does not
// exist in aveloxis_data.platforms inserts cleanly, releases its
// savepoint, and takes the COMMIT down with SQLSTATE 23503. That is a
// genuine production-reachable shape (a client emitting an unknown
// platform), it exercises the real store against a real Postgres, and
// it needs no injected seam.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// unknownPlatformID is absent from aveloxis_data.platforms, so the
// deferred FK on contributor_identities.platform_id fails at COMMIT.
const unknownPlatformID = 99

func TestContributorBatchFailureLeavesStagedRowsForReplay(t *testing.T) {
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
	if err := db.RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Independent raw connection: staged-row state is asserted without
	// trusting the store under test.
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	const slug = "_avc5abort"
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_ops.staging WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%'`,
			// Children before parents (pass 44): contributor_login_history
			// is an ON DELETE RESTRICT cntrb_id child, so the contributors
			// DELETE fails 23001 without it and strands the fixture.
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '` + slug + `%'`,
		} {
			cleanupExec(ctx, raw, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "/Repo",
		Owner:    slug, Name: "Repo",
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}

	// Stage two contributors. The second carries an identity on a
	// platform that does not exist, which fails the batch at COMMIT.
	sw := db.NewStagingWriter(store, repoID, int16(model.PlatformGitHub), logger)
	good := model.Contributor{
		Login: slug + "_good",
		Identities: []model.ContributorIdentity{{
			Platform: model.PlatformGitHub, UserID: 910001, Login: slug + "_good",
		}},
	}
	poison := model.Contributor{
		Login: slug + "_poison",
		Identities: []model.ContributorIdentity{{
			Platform: model.Platform(unknownPlatformID), UserID: 910002, Login: slug + "_poison",
		}},
	}
	for _, c := range []model.Contributor{good, poison} {
		if err := sw.Stage(ctx, EntityContributor, c); err != nil {
			t.Fatalf("Stage: %v", err)
		}
	}
	if err := sw.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	stagedTotal := countStaged(ctx, t, raw, repoID, false)
	if stagedTotal != 2 {
		t.Fatalf("fixture: expected 2 unprocessed staged contributor rows, got %d", stagedTotal)
	}

	proc := NewProcessor(store, logger)

	// ---- Arm 1: the failing pass must NOT consume the staged rows. ----
	err = proc.ProcessRepo(ctx, repoID, int16(model.PlatformGitHub))
	if err == nil {
		t.Fatal("ProcessRepo returned nil after a failed contributor batch — " +
			"the failure was swallowed, so ProcessStaged went on to mark the " +
			"staged rows processed and the contributors are lost permanently (F6)")
	}

	if got := countStaged(ctx, t, raw, repoID, false); got != 2 {
		t.Fatalf("after a failed contributor batch: %d of 2 staged rows remain unprocessed — "+
			"rows marked processed despite the batch failing means the payloads can never be replayed", got)
	}
	if got := countStaged(ctx, t, raw, repoID, true); got != 0 {
		t.Fatalf("after a failed contributor batch: %d staged rows were marked processed; want 0", got)
	}

	// ---- Arm 2: convergence. Once the cause clears, the replay drains. ----
	//
	// This arm is what makes the abort safe rather than head-blocking: the
	// batch-level failures that can reach here are transient by
	// construction, so the next drain succeeds and the rows leave the
	// queue. Patching the staged payload's platform is the in-fixture
	// stand-in for "the transient cause went away" — it touches only this
	// test's own rows.
	if _, err := raw.Exec(ctx, `
		UPDATE aveloxis_ops.staging
		SET payload = jsonb_set(payload, '{Identities,0,Platform}', '1')
		WHERE repo_id = $1 AND entity_type = $2 AND NOT processed`,
		repoID, EntityContributor); err != nil {
		t.Fatalf("patch staged payload: %v", err)
	}

	if err := proc.ProcessRepo(ctx, repoID, int16(model.PlatformGitHub)); err != nil {
		t.Fatalf("replay pass after the cause cleared: %v", err)
	}
	if got := countStaged(ctx, t, raw, repoID, false); got != 0 {
		t.Fatalf("replay left %d staged rows unprocessed — the abort head-blocks instead of converging", got)
	}

	// Both contributors landed on the replay, so nothing was lost.
	var contribs int
	if err := raw.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_login LIKE $1`,
		slug+"%").Scan(&contribs); err != nil {
		t.Fatal(err)
	}
	if contribs != 2 {
		t.Fatalf("after replay: %d of 2 contributors persisted", contribs)
	}
}

func countStaged(ctx context.Context, t *testing.T, raw *pgxpool.Pool, repoID int64, processed bool) int {
	t.Helper()
	var n int
	if err := raw.QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_ops.staging
		WHERE repo_id = $1 AND entity_type = $2 AND processed = $3`,
		repoID, EntityContributor, processed).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func cleanupExec(ctx context.Context, raw *pgxpool.Pool, sql string) {
	_, _ = raw.Exec(ctx, sql)
}
