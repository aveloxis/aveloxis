// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGetGroupDetailReturnsQueryErrors (PR #218 review C17, SR-5): after the
// ownership check succeeds, GetGroupDetail read a failed repo count as 0 and
// dropped loadGroupRepos's and loadGroupOrgs's errors, so a query failure
// rendered an owned group as empty, with nothing logged. Each of the three
// reads is made to fail on its own — a second session holds an ACCESS
// EXCLUSIVE lock on the one table only that read touches, and the store under
// test runs with a short lock_timeout — and GetGroupDetail must return an
// error, never a detail.
func TestGetGroupDetailReturnsQueryErrors(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	// lock_timeout is a server setting pgx passes through as a runtime
	// parameter: every read of a locked table fails with 55P03 after it.
	short, err := NewPostgresStore(ctx, dsn+sep+"lock_timeout=300", quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(short.Close)

	const login = "_avc17-group-detail"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "c17 probe")
	if err != nil {
		t.Fatal(err)
	}
	// The baseline: nothing locked, the owned group reads.
	if _, _, err := short.GetGroupDetail(ctx, uid, gid, 1, 10, ""); err != nil {
		t.Fatalf("baseline GetGroupDetail: %v", err)
	}

	for _, tc := range []struct{ read, table string }{
		// The unfiltered count reads only user_repos.
		{"the repo count", "aveloxis_ops.user_repos"},
		// Unfiltered, only the page query joins repos.
		{"loadGroupRepos", "aveloxis_data.repos"},
		{"loadGroupOrgs", "aveloxis_ops.user_org_requests"},
	} {
		t.Run(tc.read, func(t *testing.T) {
			tx, err := store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, "LOCK TABLE "+tc.table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			detail, total, err := short.GetGroupDetail(ctx, uid, gid, 1, 10, "")
			if err == nil {
				t.Fatalf("GetGroupDetail with %s failing = (%+v, %d, nil); want the read's error", tc.read, detail, total)
			}
			if !strings.Contains(err.Error(), "lock") {
				t.Errorf("GetGroupDetail error = %v; want the lock timeout from %s", err, tc.read)
			}
		})
	}
}

// TestGetGroupDetailErrorBranchesReturn (PR #218 review C17) covers what the
// runtime test above cannot isolate: the repo count and the page query read
// the same table, so no lock fails the count alone, and a count whose error
// were dropped again would still surface through the page query's failure
// there. Structurally, then: in GetGroupDetail every `if err != nil` branch
// returns a non-nil error, and no call result is assigned to the blank
// identifier (the `detail.Repos, _ =` shape).
func TestGetGroupDetailErrorBranchesReturn(t *testing.T) {
	src := srctest.Read(t, "internal/db/web_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) GetGroupDetail(")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "web_store.go", "package db\n"+body, 0)
	if err != nil {
		t.Fatal(err)
	}
	examined := 0
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.IfStmt:
			cond, ok := n.Cond.(*ast.BinaryExpr)
			if !ok || cond.Op != token.NEQ {
				return true
			}
			if id, ok := cond.X.(*ast.Ident); !ok || id.Name != "err" {
				return true
			}
			if id, ok := cond.Y.(*ast.Ident); !ok || id.Name != "nil" {
				return true
			}
			examined++
			var ret *ast.ReturnStmt
			if len(n.Body.List) > 0 {
				ret, _ = n.Body.List[0].(*ast.ReturnStmt)
			}
			if ret == nil || len(ret.Results) == 0 {
				t.Errorf("GetGroupDetail line %d: an `err != nil` branch does not return the error", fset.Position(n.Pos()).Line)
				return true
			}
			if id, ok := ret.Results[len(ret.Results)-1].(*ast.Ident); ok && id.Name == "nil" {
				t.Errorf("GetGroupDetail line %d: an `err != nil` branch returns a nil error", fset.Position(n.Pos()).Line)
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == "_" {
					t.Errorf("GetGroupDetail line %d: a result is discarded to _", fset.Position(n.Pos()).Line)
				}
			}
		}
		return true
	})
	// The ownership check, the count, the page and the orgs: four branches.
	srctest.MinCount(t, "`err != nil` branches examined in GetGroupDetail", examined, 4)
}
