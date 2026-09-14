// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL driver for the batch rename pre-probe (F5 of the
// 2026-09-11 chaoss.tv log analysis).
//
// WHAT THE MEASUREMENT SHOWED. Over 2026-09-07..09-11 the production
// serve raised 884 contributors_pkey unique violations and logged
// exactly 884 "contributor rename recovered in batch upsert" lines,
// with zero batch failures — so the v0.22.13 recovery is CORRECT and
// nothing is being lost. The problem is that it is permanent: because
// recovery deliberately leaves cntrb_login at the first-observed value
// (R2), ON CONFLICT (cntrb_login) can never match a renamed
// contributor again, so the same INSERT is guaranteed to fail on
// contributors_pkey on EVERY subsequent cycle, forever, for every
// renamed contributor. 7,387 such Postgres ERROR lines across the two
// logs, burying real errors.
//
// WHY THIS SHAPE AND NOT A PER-CONTRIBUTOR PROBE. The obvious fix —
// look the row up by cntrb_id before inserting — adds a round trip to
// the busiest write path in the system to avoid ~884 recoverable
// failures out of millions of upserts. That is a pessimization, not a
// fix. One BULK probe per batch is the bounded form: a single indexed
// `cntrb_id = ANY(...)` lookup per ~500-contributor batch, whose cost
// does not scale with the number of contributors and which eliminates
// every doomed INSERT it finds.
//
// The 23505 recovery path is KEPT as the backstop: the probe's snapshot
// can go stale against a concurrent worker, and when it does the
// behaviour is exactly today's.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestContributorBatchRenameSkipsDoomedInsert(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const (
		oldLogin = "_avpp_oldname"
		newLogin = "_avpp_newname"
		userID   = int64(930077)
	)
	detID := PlatformUUID(1, userID).String()

	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '_avpp_%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '_avpp_%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avpp_%'`,
		} {
			cleanupExecRetry(ctx, store, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	// The person, already known under the FIRST-observed login.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, gh_user_id)
		VALUES ($1::uuid, $2, $2, $3)`, detID, oldLogin, userID)

	logBuf.Reset()

	// The same person, observed again after a rename on GitHub: stable
	// gh_user_id (so the same deterministic cntrb_id), new login.
	err = store.UpsertContributorBatch(ctx, []model.Contributor{{
		Login: newLogin,
		Identities: []model.ContributorIdentity{{
			Platform: model.PlatformGitHub, UserID: userID, Login: newLogin,
		}},
	}})
	if err != nil {
		t.Fatalf("UpsertContributorBatch: %v", err)
	}

	logs := logBuf.String()

	// ---- The discriminator: the rename is handled, but WITHOUT the
	// doomed INSERT that raises contributors_pkey. ----
	if !strings.Contains(logs, "rename") {
		t.Fatalf("the rename was not handled at all.\nLog was:\n%s", logs)
	}
	if !strings.Contains(logs, "known rename — insert skipped") {
		t.Errorf("the batch still routes a KNOWN rename through the failed-INSERT recovery path.\n"+
			"The row's existence under this deterministic cntrb_id is knowable from one bulk probe "+
			"per batch, and every such INSERT is guaranteed to violate contributors_pkey — 884 of them "+
			"in a five-day production window, recurring every cycle forever because R2 keeps "+
			"cntrb_login at the old value so ON CONFLICT (cntrb_login) can never match.\nLog was:\n%s", logs)
	}
	if strings.Contains(logs, "contributors_pkey collision") {
		t.Errorf("a contributors_pkey collision was still raised for a rename the batch could have "+
			"seen coming.\nLog was:\n%s", logs)
	}

	// ---- End state is unchanged from the recovery path's. ----
	var gotCntrbLogin, gotGhLogin string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_login, COALESCE(gh_login,'')
		FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, detID,
	).Scan(&gotCntrbLogin, &gotGhLogin); err != nil {
		t.Fatal(err)
	}
	if gotCntrbLogin != oldLogin {
		t.Errorf("cntrb_login = %q, want %q (R2: first-observed login never changes)", gotCntrbLogin, oldLogin)
	}
	if gotGhLogin != newLogin {
		t.Errorf("gh_login = %q, want %q (the rename must still reach the display-name mirror)", gotGhLogin, newLogin)
	}

	// Exactly one row for this person — the pre-probe must not mint a
	// second contributor.
	var rows int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avpp_%'`,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("got %d contributor rows for one person, want 1", rows)
	}

	// The identity row must still land — the pre-probe path skips the
	// contributor INSERT, not the rest of the per-contributor work.
	var idents int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, detID,
	).Scan(&idents); err != nil {
		t.Fatal(err)
	}
	if idents != 1 {
		t.Errorf("got %d identity rows, want 1 — skipping the doomed INSERT must not skip the "+
			"identity upsert or the gh_*/gl_* backfill that follows it", idents)
	}
}

// TestContributorBatchFirstSightStillInserts pins that the pre-probe
// only short-circuits KNOWN rows. A genuinely new contributor must
// still take the ordinary INSERT path.
func TestContributorBatchFirstSightStillInserts(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const login = "_avpp_brandnew"
	const userID = int64(930078)

	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '_avpp_%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '_avpp_%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avpp_%'`,
		} {
			cleanupExecRetry(ctx, store, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	if err := store.UpsertContributorBatch(ctx, []model.Contributor{{
		Login: login,
		Identities: []model.ContributorIdentity{{
			Platform: model.PlatformGitHub, UserID: userID, Login: login,
		}},
	}}); err != nil {
		t.Fatalf("UpsertContributorBatch: %v", err)
	}

	var gotID, gotLogin string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_id::text, cntrb_login FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login,
	).Scan(&gotID, &gotLogin); err != nil {
		t.Fatalf("the new contributor was not inserted: %v", err)
	}
	if want := PlatformUUID(1, userID).String(); gotID != want {
		t.Errorf("cntrb_id = %q, want the deterministic %q", gotID, want)
	}

	// Re-running must be idempotent and must not now look like a rename.
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{{
		Login: login,
		Identities: []model.ContributorIdentity{{
			Platform: model.PlatformGitHub, UserID: userID, Login: login,
		}},
	}}); err != nil {
		t.Fatalf("re-running the same batch: %v", err)
	}
	var rows int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("got %d rows after a repeat upsert, want 1", rows)
	}
}

// TestRenamePreProbeRunsInsideTheSavepoint pins the ORDERING that makes
// the pre-probe safe, because getting it wrong is silent.
//
// THE BUG THIS CAUGHT (found reviewing the F5 change itself): the first
// draft ran the relabel UPDATE above the per-contributor SAVEPOINT. An
// unprotected failed UPDATE aborts the transaction, so the SAVEPOINT
// that follows also fails, upsertOneContributor returns non-nil, and the
// WHOLE BATCH dies — exactly the "count=420 batches drop ~419 innocent
// contributors" failure v0.22.13 introduced savepoints to end. The
// draft's own comment claimed it would "fall through to the ordinary
// path", which an aborted transaction cannot do.
//
// Pinned structurally rather than behaviorally on purpose: making that
// UPDATE fail requires a constraint violation on columns that carry no
// unique index and no FK, so the failure is near-unreachable. A
// structurally missing guard is a defect whether or not anyone has
// produced the input that needs it (the v0.29.4 probingSessionSQL
// lesson) — and the cost of the guard is one statement.
func TestRenamePreProbeRunsInsideTheSavepoint(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/postgres.go"),
		"func (s *PostgresStore) upsertOneContributor(")

	spIdx := strings.Index(body, `tx.Exec(ctx, "SAVEPOINT "+cntrbSP)`)
	if spIdx < 0 {
		t.Fatal("could not find the per-contributor SAVEPOINT in upsertOneContributor")
	}
	probeIdx := strings.Index(body, "renameRecoveryUpdateSQL")
	if probeIdx < 0 {
		t.Fatal("could not find the pre-probe relabel in upsertOneContributor")
	}
	if probeIdx < spIdx {
		t.Error("the known-rename relabel UPDATE runs BEFORE the per-contributor SAVEPOINT. " +
			"An unprotected failure there aborts the transaction, so the SAVEPOINT below fails, " +
			"upsertOneContributor returns non-nil and the whole batch dies — the v0.22.13 " +
			"failure mode. Move the relabel inside the savepoint.")
	}

	// And its failure arm must actually restore a usable transaction,
	// or "fall through to the ordinary path" is still a false claim.
	tail := body[probeIdx:]
	end := len(tail)
	if k := strings.Index(tail, "upsertContributorIdentities"); k > 0 {
		end = k
	}
	if !strings.Contains(tail[:end], "ROLLBACK TO SAVEPOINT "+"\"+cntrbSP") {
		t.Error("the pre-probe's failure arm must ROLLBACK TO SAVEPOINT before falling through " +
			"to the INSERT. Without it the transaction is aborted and the fall-through cannot run.")
	}
}
