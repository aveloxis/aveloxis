// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL driver for the R2 invariant on the commit-resolver path
// (F4 of the 2026-09-11 chaoss.tv log analysis).
//
// THE CONTRADICTION: docs/architecture/contributor-resolution.md's
// invariant table says cntrb_login changes NEVER — "durable audit trail,
// the login as first observed" — and three of the four rename paths obey
// it (the v0.22.13 batch recovery has a hard NEGATIVE pin forbidding the
// write; v0.22.12's RenameContributorGhLogin documents leaving it alone;
// the login-exists-under-a-different-id branch in this very function
// touches only gh_login). Only UpsertContributorFull's
// row-exists-by-deterministic-ID branch wrote `cntrb_login = $2`.
//
// MEASURED COST, not theorised: every one of the 555 idx_contributors_login
// unique violations in the 2026-09-06..09-11 production Postgres log came
// from that single statement — 555 of 555, with the ON CONFLICT arm in the
// same function contributing zero. They were recovered (a 23505 fallback
// re-ran the UPDATE without cntrb_login) but logged at Debug, so they were
// invisible in production while filling the Postgres log with ERROR lines.
//
// WHY THE ASSERTIONS ARE ON LOGS, not just end state: the recovery
// produced the SAME end state as not attempting the write, so end-state
// assertions alone cannot tell the two apart. The discriminators are (a)
// the rename is now reported as an ordinary observation, and (b) the
// collision-recovery path is never reached.
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
)

func TestUpsertContributorFullPreservesCntrbLoginOnRename(t *testing.T) {
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
		oldLogin  = "_avr2_oldname"
		newLogin  = "_avr2_newname"
		ghUserID  = int64(920042)
		probeMail = "_avr2@example.org"
	)
	// The deterministic UUID for (GitHub, ghUserID) — the key the
	// commit resolver arrives with, and the reason this branch believes
	// it is looking at the same person.
	detID := PlatformUUID(1, ghUserID).String()

	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '_avr2_%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '_avr2_%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avr2_%'`,
		} {
			cleanupExecRetry(ctx, store, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	// Row A: the person, keyed by the deterministic UUID, carrying the
	// login as FIRST observed.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, gh_user_id)
		VALUES ($1::uuid, $2, $2, $3)`, detID, oldLogin, ghUserID)

	// Row B: some other row already holds the NEW login — the shape that
	// made the write collide (a lazy-resolver row stamped with the
	// post-rename login under its own random UUID).
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login)
		VALUES (gen_random_uuid(), $1)`, newLogin)

	logBuf.Reset()

	created, actualID, err := store.UpsertContributorFull(ctx, detID, newLogin, ghUserID, probeMail)
	if err != nil {
		t.Fatalf("UpsertContributorFull: %v", err)
	}
	if created {
		t.Error("created = true for a row that already existed by cntrb_id")
	}
	if actualID != detID {
		t.Errorf("actualID = %q, want the deterministic id %q", actualID, detID)
	}

	// ---- R2: the audit trail is intact. ----
	var gotCntrbLogin, gotGhLogin string
	var gotUserID *int64
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_login, COALESCE(gh_login,''), gh_user_id
		FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, detID,
	).Scan(&gotCntrbLogin, &gotGhLogin, &gotUserID); err != nil {
		t.Fatal(err)
	}
	if gotCntrbLogin != oldLogin {
		t.Errorf("cntrb_login = %q, want %q — R2 says the login as FIRST observed never changes "+
			"(docs/architecture/contributor-resolution.md)", gotCntrbLogin, oldLogin)
	}
	// gh_login is the "current display name" mirror — that IS where a
	// rename belongs, and it must still be updated.
	if gotGhLogin != newLogin {
		t.Errorf("gh_login = %q, want %q — the current-display-name mirror must still follow the rename",
			gotGhLogin, newLogin)
	}
	if gotUserID == nil || *gotUserID != ghUserID {
		t.Errorf("gh_user_id = %v, want %d", gotUserID, ghUserID)
	}

	logs := logBuf.String()

	// ---- Discriminator 1: the rename is reported as an observation. ----
	if !strings.Contains(logs, "contributor rename observed by commit resolver") {
		t.Errorf("no rename observation logged — a rename the commit resolver sees should be "+
			"visible to operators as an ordinary event, not as 555 Postgres ERROR lines "+
			"recovered at Debug. Log was:\n%s", logs)
	}

	// ---- Discriminator 2: the collision path is never reached. ----
	if strings.Contains(logs, "login update skipped") {
		t.Errorf("the 23505 collision-recovery path was reached — the UPDATE still attempts to "+
			"write cntrb_login, so every rename costs a failed statement plus a recovery. "+
			"Log was:\n%s", logs)
	}

	// Row B is untouched: this path must never reassign another row's login.
	var bLogin string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_login FROM aveloxis_data.contributors WHERE cntrb_login = $1`, newLogin,
	).Scan(&bLogin); err != nil {
		t.Fatalf("row B lost its login: %v", err)
	}
}

// TestUpsertContributorFullStillFillsLoginOnFirstSight pins the other
// side of the contract: R2 protects an EXISTING login from changing, it
// does not stop a first observation from being recorded. A fresh
// contributor must still get its cntrb_login.
func TestUpsertContributorFullStillFillsLoginOnFirstSight(t *testing.T) {
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

	const login = "_avr2_freshuser"
	const ghUserID = int64(920043)
	detID := PlatformUUID(1, ghUserID).String()

	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '_avr2_%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '_avr2_%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avr2_%'`,
		} {
			cleanupExecRetry(ctx, store, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	created, actualID, err := store.UpsertContributorFull(ctx, detID, login, ghUserID, "_avr2f@example.org")
	if err != nil {
		t.Fatalf("UpsertContributorFull: %v", err)
	}
	if !created {
		t.Error("created = false for a genuinely new contributor")
	}
	if actualID != detID {
		t.Errorf("actualID = %q, want %q", actualID, detID)
	}

	var gotLogin string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_login FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, detID,
	).Scan(&gotLogin); err != nil {
		t.Fatal(err)
	}
	if gotLogin != login {
		t.Errorf("cntrb_login = %q, want %q — a first observation must still be recorded", gotLogin, login)
	}
}
