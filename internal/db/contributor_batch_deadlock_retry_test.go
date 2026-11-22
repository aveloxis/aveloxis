// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// faultTx stands in for the batch transaction: SAVEPOINT/ROLLBACK
// TO/RELEASE succeed, the contributor INSERT (QueryRow) and the identity
// INSERT (the Exec whose SQL inserts contributor_identities) fail with
// the configured SQLSTATEs. Only the methods upsertOneContributor calls
// are implemented; any other call panics on the nil embedded Tx.
type faultTx struct {
	pgx.Tx
	insertCode       string // SQLSTATE for the contributors INSERT, "" = success
	insertConstraint string // the INSERT failure's constraint (contributors_pkey reaches the rename path)
	identCode        string // SQLSTATE for the identity INSERT, "" = success
	renameCode       string // SQLSTATE for the rename-recovery UPDATE, "" = success
	rollbacks        int
}

func (f *faultTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT") {
		f.rollbacks++
	}
	if f.renameCode != "" && strings.Contains(sql, "SET gh_login") {
		return pgconn.CommandTag{}, &pgconn.PgError{Code: f.renameCode, Message: "injected"}
	}
	if f.identCode != "" && strings.Contains(sql, "contributor_identities") {
		return pgconn.CommandTag{}, &pgconn.PgError{Code: f.identCode, Message: "injected"}
	}
	return pgconn.CommandTag{}, nil
}

type faultRow struct{ err error }

func (r faultRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if p, ok := dest[0].(*string); ok {
		*p = "00000000-0000-0000-0000-000000000001"
	}
	return nil
}

func (f *faultTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	if f.insertCode != "" {
		return faultRow{err: &pgconn.PgError{Code: f.insertCode, ConstraintName: f.insertConstraint, Message: "injected"}}
	}
	return faultRow{}
}

// TestContributorDeadlockAbortsTheBatchForRetry — 2026-09-23 log review:
// a deadlock (40P01) inside one contributor's savepoint was caught,
// logged as "contributor batch sub-statement failed", rolled back to the
// savepoint and SKIPPED, so the batch's withRetry never saw it and that
// contributor was silently dropped for the cycle. A transient
// whole-transaction failure (40P01, 40001) must escape the savepoint so
// the batch rolls back and withRetry retries it; any other failure keeps
// the per-contributor skip.
func TestContributorDeadlockAbortsTheBatchForRetry(t *testing.T) {
	s := &PostgresStore{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx := context.Background()
	contrib := &model.Contributor{Login: "alice"}
	idents := []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: 42, Login: "alice"}}
	noCapture := func(string, string, error) {}

	for _, tc := range []struct {
		name      string
		tx        faultTx
		renamedTo map[string]string
		wantCode  string // "" = the contributor is skipped, the batch continues
	}{
		{"contributor insert deadlock", faultTx{insertCode: "40P01"}, nil, "40P01"},
		{"contributor insert serialization failure", faultTx{insertCode: "40001"}, nil, "40001"},
		{"identity insert deadlock", faultTx{identCode: "40P01"}, nil, "40P01"},
		// The two rename sites (review round 1): the known-rename pre-probe
		// UPDATE and the 23505 contributors_pkey recovery UPDATE.
		{"rename pre-probe update deadlock", faultTx{renameCode: "40P01"}, map[string]string{PlatformUUID(int(model.PlatformGitHub), 42).String(): "alice-old"}, "40P01"},
		{"rename recovery update deadlock", faultTx{insertCode: "23505", insertConstraint: "contributors_pkey", renameCode: "40P01"}, nil, "40P01"},
		{"contributor insert other failure is skipped", faultTx{insertCode: "23502"}, nil, ""},
		{"identity insert other failure is skipped", faultTx{identCode: "23502"}, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.tx
			sp := 0
			err := s.upsertOneContributor(ctx, &tx, "alice", contrib, idents, tc.renamedTo, noCapture, &sp)
			var pgErr *pgconn.PgError
			if tc.wantCode != "" {
				if !errors.As(err, &pgErr) || pgErr.Code != tc.wantCode {
					t.Fatalf("want the %s to escape so withRetry retries the batch, got %v", tc.wantCode, err)
				}
			} else if err != nil {
				t.Fatalf("a non-transient failure must skip this contributor and keep the batch, got %v", err)
			}
			if tx.rollbacks == 0 {
				t.Error("the savepoint must be rolled back on either path")
			}
		})
	}
}

// TestBreadthMarkRetriesADeadlock — the other half of the pair: the
// breadth stamp is one UPDATE over up to 500 contributors; as the deadlock
// victim it must go through withRetry, not return and leave the chunk
// unstamped (those contributors would be fetched again next cycle).
func TestBreadthMarkRetriesADeadlock(t *testing.T) {
	src, err := os.ReadFile("breadth_store.go")
	if err != nil {
		t.Fatal(err)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(src), "func (s *PostgresStore) MarkBreadthAttemptedBatch("))
	retry := strings.Index(body, "s.withRetry(ctx, func(ctx context.Context) error {")
	update := strings.Index(body, "SET cntrb_last_breadth_at = NOW()")
	if retry < 0 || update < retry {
		t.Error("MarkBreadthAttemptedBatch must run its UPDATE inside s.withRetry")
	}
	if strings.Count(body, "s.pool.Exec(") != 1 {
		t.Error("exactly one Exec, the one inside withRetry")
	}
	if !isRetryableTxError(&pgconn.PgError{Code: "40P01"}) || !isRetryableTxError(&pgconn.PgError{Code: "40001"}) || isRetryableTxError(&pgconn.PgError{Code: "23505"}) {
		t.Error("isRetryableTxError must be exactly 40P01 and 40001")
	}
}
