// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// What a statement site is: a call of a library function that takes SQL,
// and (0.29.214) a call of a method of one of the package's own interfaces
// that is such a function in name and signature. A call of a helper of the
// package is not one, nor a batch result's Exec, which takes no text.
func TestRunnerCallsCountsLibraryCallsAndCallsThroughThePackagesInterfaces(t *testing.T) {
	src := `
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
type asker interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
type other interface {
	Exec(sql string) error // the name, and not the library's signature
}
type mine struct{}

func (mine) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func direct(ctx context.Context, p *pgxpool.Pool, tx pgx.Tx) {
	p.Exec(ctx, "SELECT 1")
	tx.QueryRow(ctx,
		"SELECT 2")
}
func through(ctx context.Context, e execer, a asker) {
	e.Exec(ctx, "SELECT 3")
	a.QueryRow(ctx, "SELECT 4")
}
func notOnes(ctx context.Context, o other, m mine, p *pgxpool.Pool) {
	o.Exec("SELECT 5")
	m.Exec(ctx, "SELECT 6")
	direct(ctx, p, nil)
}
func batch(ctx context.Context, p *pgxpool.Pool) {
	b := &pgx.Batch{}
	b.Queue("SELECT 7")
	br := p.SendBatch(ctx, b)
	br.Exec()
	br.Close()
}
`
	calls, err := RunnerCalls(typedRoot, typedFiles(src))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range calls {
		q := ""
		if c.Queues {
			q = " (queues)"
		}
		if c.EndLine < c.Line {
			t.Errorf("%s: lines %d..%d", c.Callee, c.Line, c.EndLine)
		}
		got = append(got, c.Decl+" "+c.Callee+q)
	}
	want := []string{"direct p.Exec", "direct tx.QueryRow", "through e.Exec", "through a.QueryRow", "batch b.Queue (queues)"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("statement sites = %q\nwant %q", got, want)
	}
	// A call written over two lines covers both: a stack frame may name
	// either.
	for _, c := range calls {
		if c.Callee == "tx.QueryRow" && c.EndLine != c.Line+1 {
			t.Errorf("tx.QueryRow is written over two lines and has lines %d..%d", c.Line, c.EndLine)
		}
	}
}
