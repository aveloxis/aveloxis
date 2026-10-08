// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsStalePreparedStatement_MatchesSQLSTATE26000 — the classifier
// must recognize a pgconn.PgError with Code "26000" (prepared
// statement does not exist). This is the v0.18.11 production failure
// mode: pgx's per-connection prepared-statement cache falls out of
// sync with the server backend when a TCP connection is silently
// swapped under load, and the next SendBatch hits SQLSTATE 26000.
func TestIsStalePreparedStatement_MatchesSQLSTATE26000(t *testing.T) {
	err := &pgconn.PgError{
		Code:    "26000",
		Message: `prepared statement "stmtcache_c9d081ae61e306706c11345fabcd5c6b0cbe6f7ae724995e" does not exist`,
	}
	if !isStalePreparedStatement(err) {
		t.Error("isStalePreparedStatement must return true for SQLSTATE 26000 " +
			"(the stale-cache retry signal)")
	}
}

// TestIsStalePreparedStatement_MatchesWrappedError — real errors
// come through a fmt.Errorf("flushing staging batch (%d rows): %w",
// ...) wrapper. The classifier must errors.As unwrap to find the
// underlying PgError, otherwise production 26000s appear wrapped
// and the retry never fires.
func TestIsStalePreparedStatement_MatchesWrappedError(t *testing.T) {
	inner := &pgconn.PgError{Code: "26000", Message: "prepared statement X does not exist"}
	wrapped := fmt.Errorf("flushing staging batch (500 rows): %w", inner)
	if !isStalePreparedStatement(wrapped) {
		t.Error("isStalePreparedStatement must unwrap via errors.As so callers can " +
			"wrap the error with context (batch size, file path, etc.) without " +
			"breaking the retry classifier")
	}
}

// TestIsStalePreparedStatement_RejectsOtherPgErrors — other SQL
// errors must NOT trigger a retry. Retrying a 22P02 (invalid JSON),
// 22P05 (bad escape), 23505 (unique violation), or 42P01 (undefined
// table) would waste time and possibly mask real bugs.
func TestIsStalePreparedStatement_RejectsOtherPgErrors(t *testing.T) {
	cases := []string{
		"22P02", // invalid_text_representation
		"22P05", // untranslatable_character
		"23505", // unique_violation
		"42P01", // undefined_table
		"40P01", // deadlock_detected
		"",      // empty code — shouldn't match
	}
	for _, code := range cases {
		err := &pgconn.PgError{Code: code, Message: "test"}
		if isStalePreparedStatement(err) {
			t.Errorf("isStalePreparedStatement must return false for SQLSTATE %q; "+
				"only 26000 triggers the prepared-statement retry", code)
		}
	}
}

// TestIsStalePreparedStatement_RejectsNonPgErrors — non-pg errors
// (context cancellation, plain io errors, nil) must not match.
func TestIsStalePreparedStatement_RejectsNonPgErrors(t *testing.T) {
	cases := []error{
		nil,
		errors.New("random string error"),
		fmt.Errorf("wrapped: %w", errors.New("inner")),
	}
	for _, err := range cases {
		if isStalePreparedStatement(err) {
			t.Errorf("isStalePreparedStatement must return false for non-pg error %v", err)
		}
	}
}

// TestSendBatchWithRetry_ExistsOnStore — source-contract test:
// PostgresStore must expose sendBatchWithRetry so the hot flush
// path can route around stale prepared-statement cache entries.
// Without this method, StagingWriter.Flush is stuck with
// pool.SendBatch which has no retry on SQLSTATE 26000.
//
// The method lives in prepared_stmt_retry.go alongside the
// classifier — kept out of postgres.go so the retry policy is easy
// to find and easy to change without touching the pool setup.
func TestSendBatchWithRetry_ExistsOnStore(t *testing.T) {
	data, err := os.ReadFile("prepared_stmt_retry.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) sendBatchWithRetry(") {
		t.Error("prepared_stmt_retry.go must define sendBatchWithRetry on " +
			"PostgresStore to wrap pool.SendBatch with a retry-once-on-" +
			"SQLSTATE-26000 policy")
	}
	if !strings.Contains(src, "isStalePreparedStatement") {
		t.Error("sendBatchWithRetry must use isStalePreparedStatement to " +
			"classify the retry signal")
	}
}

// TestStagingFlushUsesRetryWrapper — source-contract test: the hot
// flush path in staging.go must go through sendBatchWithRetry, not
// raw pool.SendBatch. Otherwise the 26000 stays fatal and the whole
// batch is lost.
func TestStagingFlushUsesRetryWrapper(t *testing.T) {
	data, err := os.ReadFile("staging.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "sendBatchWithRetry") {
		t.Error("staging.go must call sendBatchWithRetry (not raw pool.SendBatch) " +
			"in the Flush path so a stale prepared-statement on one connection " +
			"doesn't fail the whole 500-row batch")
	}
	// Also: the raw pool.SendBatch line should be gone from Flush —
	// leaving both would silently defeat the wrapper.
	flushIdx := strings.Index(src, "func (w *StagingWriter) Flush(")
	if flushIdx < 0 {
		t.Fatal("cannot find StagingWriter.Flush in staging.go")
	}
	flushBody := src[flushIdx:]
	end := strings.Index(flushBody, "\n}\n")
	if end > 0 {
		flushBody = flushBody[:end]
	}
	if strings.Contains(flushBody, "w.store.pool.SendBatch(") {
		t.Error("StagingWriter.Flush still calls w.store.pool.SendBatch directly — " +
			"remove it so only sendBatchWithRetry runs the batch")
	}
}

// TestSendBatchWithRetryRecoversFromAStaleStatement — review 9 on PR #226:
// the retry must send a FRESH batch. pgx (v5, QueryExecModeCacheStatement)
// writes each queued query's statement description into the batch on the
// first send and skips the prepare on a resend, so retrying the same batch
// sent the same stale statement name again and failed a second time; pgx
// does clear its cache on the failure, so a fresh copy re-prepares. The
// stale entry is made real here: the statement is prepared, then the server
// forgets it (DEALLOCATE ALL) while pgx's cache still lists it.
func TestSendBatchWithRetryRecoversFromAStaleStatement(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1 // one connection: the stale entry and the retry meet on it
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &PostgresStore{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	const q = `SELECT $1::int + 0 AS avstale`
	prime := &pgx.Batch{}
	prime.Queue(q, 1)
	if err := pool.SendBatch(ctx, prime).Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DEALLOCATE ALL", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	b := &pgx.Batch{}
	b.Queue(q, 2)
	if err := s.sendBatchWithRetry(ctx, b); err != nil {
		t.Errorf("the retry must recover from a stale prepared statement, got %v", err)
	}
}
