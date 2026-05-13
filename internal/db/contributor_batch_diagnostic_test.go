package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.11 (Fix G): 70 "failed to upsert contributor batch
// commit unexpectedly resulted in rollback" warnings appeared
// in the May 9–12 production log. The error message is final
// (it's what tx.Commit returned) but the root cause is hidden
// — the actual SQL error inside the transaction was swallowed
// by `s.logger.Debug` (line 1261) and the per-identity Exec
// calls used `_, _ =` to discard their errors. Without seeing
// the underlying SQLSTATE we can't tell whether the failures
// are unique-constraint races (23505), deadlocks (40P01),
// constraint violations, or something else.
//
// Fix G's scope is purely diagnostic: capture and log the
// underlying pgx errors with their SQLSTATE so the next run
// surfaces enough data to plan a real fix. No behavioral
// change to the upsert logic itself; that's deferred until we
// know the root cause.

func TestUpsertContributorBatch_CapturesPgErrorCode(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the UpsertContributorBatch function body.
	idx := strings.Index(src, "func (s *PostgresStore) UpsertContributorBatch(")
	if idx < 0 {
		t.Fatal("cannot find UpsertContributorBatch")
	}
	// Find end via the next func declaration.
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of UpsertContributorBatch")
	}
	body := tail[:1+endRel]

	// Three diagnostic-logging signals must appear inside the
	// function body. They can be expressed in different ways
	// but each captures a previously-swallowed error class.

	// 1. The QueryRow error (contributor INSERT) must include
	//    the pg error code, not just the generic error.
	hasPgErr := strings.Contains(body, "pgconn.PgError") ||
		strings.Contains(body, "pg.Code") ||
		strings.Contains(body, "errors.As") && strings.Contains(body, "SQLSTATE")
	if !hasPgErr {
		t.Error("UpsertContributorBatch must capture and log the pgx error code (errors.As(err, &pgconn.PgError{}) or equivalent) on QueryRow failures — pre-v0.20.11 the log only showed the generic error message, leaving the actual SQLSTATE invisible. We need the code to plan a real fix for the 70 rollback events.")
	}

	// 2. The identity-table Exec at the bottom must capture and
	//    log its error rather than discarding with `_, _ =`.
	if strings.Contains(body, `_, _ = tx.Exec(ctx, `+"`"+`
					INSERT INTO aveloxis_data.contributor_identities`) {
		t.Error("the contributor_identities INSERT inside UpsertContributorBatch must capture its error — pre-v0.20.11 `_, _ = tx.Exec(...)` discarded the error, which is the most likely source of the silent tx-poison that produces 'commit unexpectedly resulted in rollback' on the final Commit")
	}

	// 3. The Commit error path must distinguish a normal rollback
	//    from a real-failure rollback so operators can grep.
	hasCommitDiag := strings.Contains(body, "tx.Commit") &&
		(strings.Contains(body, "commit transaction") ||
			strings.Contains(body, "commit failed") ||
			strings.Contains(body, "rollback") ||
			strings.Contains(body, "first_failed"))
	if !hasCommitDiag {
		t.Error("UpsertContributorBatch must annotate tx.Commit failures with context (which contributor caused the tx-abort) — pre-v0.20.11 the bare 'commit unexpectedly resulted in rollback' from pgx is what operators saw, with no way to find the offending row")
	}
}

// TestPgconnImported is a regression pin: the import for
// pgconn must remain present after the diagnostic logging is
// added. A future refactor that drops the import would break
// the SQLSTATE introspection silently because errors.As
// returns false for a missing target type and the test above
// would still pass via the alternate matchers.
func TestPgconnImported(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"github.com/jackc/pgx/v5/pgconn"`) {
		t.Error("postgres.go must import github.com/jackc/pgx/v5/pgconn so UpsertContributorBatch can introspect pgError.Code on the previously-swallowed SQL errors. Without this import, the diagnostic logging would silently fall back to the alternate matcher path.")
	}
}
