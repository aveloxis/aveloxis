// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5"
)

// Integration tests for the v0.23.5 utf8ScrubTracer. Exercise every
// path that takes external-data string args, against a live Postgres
// configured via AVELOXIS_TEST_DB.
//
// Recipe (local laptop):
//
//   AVELOXIS_TEST_DB="postgres://aveloxis:CHANGEME@localhost:5432/aveloxis_cascade_test?sslmode=prefer" \
//       go test ./internal/db/ -run TestUTF8Tracer -v
//
// All tests in this file SKIP when AVELOXIS_TEST_DB is unset so
// `go test ./...` stays runnable without a database.

func openTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping UTF8 tracer integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		store.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestUTF8TracerScrubsPoolExec(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	// Use a temp table to avoid colliding with production-schema
	// rows; keeps the test isolated.
	_, err := store.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS aveloxis_test_utf8_pool (
			id SERIAL PRIMARY KEY,
			val TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DROP TABLE IF EXISTS aveloxis_test_utf8_pool`)
	})
	_, _ = store.pool.Exec(ctx, `TRUNCATE aveloxis_test_utf8_pool`)

	// 0xb3 is the byte that triggered the 2026-05-21 production
	// SQLSTATE 22021 storm on Linux kernel commits. Pre-v0.23.5
	// this INSERT errored out. Post-v0.23.5 the tracer scrubs.
	bad := "kernel\xb3author"
	if _, err := store.pool.Exec(ctx,
		`INSERT INTO aveloxis_test_utf8_pool (val) VALUES ($1)`, bad); err != nil {
		t.Fatalf("INSERT with invalid-UTF8 arg must succeed under "+
			"the v0.23.5 tracer; got: %v", err)
	}

	var stored string
	if err := store.pool.QueryRow(ctx,
		`SELECT val FROM aveloxis_test_utf8_pool LIMIT 1`).Scan(&stored); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !utf8.ValidString(stored) {
		t.Errorf("persisted value must be valid UTF-8; got %q (bytes %v)",
			stored, []byte(stored))
	}
	// The scrubbed value should retain "kernel" and "author"
	// substrings with a U+FFFD between (or similar substitution).
	if !bytes.Contains([]byte(stored), []byte("kernel")) ||
		!bytes.Contains([]byte(stored), []byte("author")) {
		t.Errorf("scrub must preserve surrounding valid bytes; got %q", stored)
	}
}

func TestUTF8TracerScrubsTxExec(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	_, err := store.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS aveloxis_test_utf8_tx (
			id SERIAL PRIMARY KEY,
			val TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DROP TABLE IF EXISTS aveloxis_test_utf8_tx`)
	})
	_, _ = store.pool.Exec(ctx, `TRUNCATE aveloxis_test_utf8_tx`)

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	bad := "tx\xe1\x7a\x71row"
	if _, err := tx.Exec(ctx,
		`INSERT INTO aveloxis_test_utf8_tx (val) VALUES ($1)`, bad); err != nil {
		t.Fatalf("tx.Exec with invalid-UTF8 must succeed under v0.23.5: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var stored string
	if err := store.pool.QueryRow(ctx,
		`SELECT val FROM aveloxis_test_utf8_tx LIMIT 1`).Scan(&stored); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !utf8.ValidString(stored) {
		t.Errorf("tx-written value must be valid UTF-8; got %q", stored)
	}
}

func TestUTF8TracerScrubsSendBatch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	_, err := store.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS aveloxis_test_utf8_batch (
			id SERIAL PRIMARY KEY,
			val TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DROP TABLE IF EXISTS aveloxis_test_utf8_batch`)
	})
	_, _ = store.pool.Exec(ctx, `TRUNCATE aveloxis_test_utf8_batch`)

	// Three queued queries: clean, dirty, clean. Pre-v0.23.5 the
	// dirty one would have aborted the entire batch.
	batch := &pgx.Batch{}
	batch.Queue(`INSERT INTO aveloxis_test_utf8_batch (val) VALUES ($1)`, "clean_a")
	batch.Queue(`INSERT INTO aveloxis_test_utf8_batch (val) VALUES ($1)`, "dirty\xf6\x6c\xe4row")
	batch.Queue(`INSERT INTO aveloxis_test_utf8_batch (val) VALUES ($1)`, "clean_c")
	br := store.pool.SendBatch(ctx, batch)
	for i := 0; i < 3; i++ {
		if _, err := br.Exec(); err != nil {
			t.Errorf("batch exec %d failed: %v", i, err)
		}
	}
	if err := br.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	rows, err := store.pool.Query(ctx,
		`SELECT val FROM aveloxis_test_utf8_batch ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !utf8.ValidString(v) {
			t.Errorf("batch-written value must be valid UTF-8; got %q", v)
		}
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 batched rows, got %d (%v)", len(got), got)
	}
}

// TestUTF8TracerProtectsUpsertCommit is the live-bug regression
// test. The byte 0xb3 (or 0xe1 0x7a 0x71) in cmt_author_name, etc.
// is exactly the 2026-05-21 kernel-repo case. Pre-v0.23.5 this
// returned `ERROR: invalid byte sequence for encoding "UTF8"
// (SQLSTATE 22021)` and the commit was discarded. Post-v0.23.5 the
// insert succeeds and the persisted strings are valid UTF-8.
func TestUTF8TracerProtectsUpsertCommit(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	// Seed a repo row so the FK in commits is satisfiable. Use a
	// negative ID to avoid colliding with operator-imported data.
	repoID := int64(-191919)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos
			(repo_id, platform_id, repo_git, repo_owner, repo_name, repo_archived)
		VALUES ($1, 1, 'https://example.invalid/utf8-fixture', 'utf8-fixture',
				'utf8-fixture', FALSE)
		ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	_, _ = store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)

	now := time.Now().UTC()
	commit := &model.Commit{
		RepoID:               repoID,
		Hash:                 "deadbeefcafebabe0000000000000000000000aa",
		AuthorName:           "L\xe1szlo K\xf6v\xe1cs", // ISO-8859-1
		AuthorEmail:          "laszlo@example.invalid",
		AuthorRawEmail:       "laszlo@example.invalid",
		AuthorDate:           now.Format(time.RFC3339),
		CommitterName:        "Bj\xf6rn Andersen", // ISO-8859-1
		CommitterEmail:       "bjorn@example.invalid",
		CommitterRawEmail:    "bjorn@example.invalid",
		CommitterDate:        now.Format(time.RFC3339),
		Filename:             "arch/i386/kernel/cpu\xb3longhaul.c",
		LinesAdded:           3,
		LinesRemoved:         1,
		AuthorPlatformLogin:  "",
		AuthorAffiliation:    "",
		CommitterAffiliation: "",
		CommitterTimestamp:   &now,
		AuthorTimestamp:      &now,
		Origin: model.DataOrigin{
			ToolSource: "aveloxis-facade",
			DataSource: "git",
		},
	}

	if err := store.UpsertCommit(ctx, commit); err != nil {
		t.Fatalf("UpsertCommit with kernel-style invalid-UTF8 fields "+
			"must succeed under v0.23.5; got: %v", err)
	}

	var authorName, committerName, filename string
	if err := store.pool.QueryRow(ctx, `
		SELECT cmt_author_name, cmt_committer_name, cmt_filename
		FROM aveloxis_data.commits
		WHERE repo_id = $1 AND cmt_commit_hash = $2
		LIMIT 1`, repoID, commit.Hash).Scan(&authorName, &committerName, &filename); err != nil {
		t.Fatalf("read back: %v", err)
	}
	for label, v := range map[string]string{
		"author_name":    authorName,
		"committer_name": committerName,
		"filename":       filename,
	} {
		if !utf8.ValidString(v) {
			t.Errorf("persisted %s must be valid UTF-8; got %q (bytes %v)",
				label, v, []byte(v))
		}
	}
}

// TestUTF8TracerProtectsUpsertContributorBatch covers the v0.19.2
// case (PNG-signature byte 0x89 in cntrb_company) at the batch
// path. v0.19.2 added explicit safeUTF8 calls in
// PopulateAffiliations; v0.23.5 makes the same protection happen
// via the tracer at the broader contributor-upsert layer as
// belt-and-suspenders.
func TestUTF8TracerProtectsUpsertContributorBatch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	login := "utf8-fixture-user"
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
		_, _ = store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	// Pre-cleanup in case of a prior failed run.
	_, _ = store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
	_, _ = store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)

	c := model.Contributor{
		Login:    login,
		Email:    "fixture@example.invalid",
		FullName: "Pat\xf6 Smith", // ISO-8859-1
		Company:  "Acme\x89Corp",  // PNG signature byte mid-string
		Location: "Z\xfcrich",
		Identities: []model.ContributorIdentity{{
			Platform: model.PlatformGitHub,
			UserID:   99999991,
			Login:    login,
		}},
	}
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{c}); err != nil {
		t.Fatalf("UpsertContributorBatch with non-UTF8 profile fields "+
			"must succeed under v0.23.5; got: %v", err)
	}

	var fullName, company, location string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_full_name, cntrb_company, cntrb_location
		FROM aveloxis_data.contributors
		WHERE cntrb_login = $1
		LIMIT 1`, login).Scan(&fullName, &company, &location); err != nil {
		t.Fatalf("read back: %v", err)
	}
	for label, v := range map[string]string{
		"full_name": fullName,
		"company":   company,
		"location":  location,
	} {
		if !utf8.ValidString(v) {
			t.Errorf("persisted %s must be valid UTF-8; got %q", label, v)
		}
	}
}

// TestUTF8TracerLeavesJSONBValid covers the staging-JSONB path:
// staging.Stage writes JSON-encoded payloads as JSONB. Invalid
// UTF-8 inside a JSON string would still be rejected by Postgres
// JSONB. The tracer scrubs the string parameter (which pgx will
// pass as a string to the JSONB column).
func TestUTF8TracerLeavesJSONBValid(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	_, err := store.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS aveloxis_test_utf8_jsonb (
			id SERIAL PRIMARY KEY,
			doc JSONB NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(),
			`DROP TABLE IF EXISTS aveloxis_test_utf8_jsonb`)
	})
	_, _ = store.pool.Exec(ctx, `TRUNCATE aveloxis_test_utf8_jsonb`)

	// JSON-shaped string with a dirty byte inside one of the
	// fields. Pre-v0.23.5 the JSONB column would have rejected the
	// whole INSERT with SQLSTATE 22P05.
	dirty := "{\"name\":\"k\xf6vacs\",\"role\":\"committer\"}"
	if _, err := store.pool.Exec(ctx,
		`INSERT INTO aveloxis_test_utf8_jsonb (doc) VALUES ($1::jsonb)`,
		dirty); err != nil {
		t.Fatalf("JSONB insert with non-UTF8 must succeed under v0.23.5: %v", err)
	}
	var got string
	if err := store.pool.QueryRow(ctx,
		`SELECT doc::text FROM aveloxis_test_utf8_jsonb LIMIT 1`).Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !utf8.ValidString(got) {
		t.Errorf("persisted JSONB must be valid UTF-8; got %q", got)
	}
}
