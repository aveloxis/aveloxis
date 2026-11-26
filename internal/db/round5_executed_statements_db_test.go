// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// Review round 5 (S5-2): three statements were rewritten in 0.29.211 so
// that their text could be read, and no test RAN any of them: an upgrade
// proof by hand was the only evidence that the database accepts them. Each
// is executed here.

// The deferrable step's ALTER, on a foreign key whose table and constraint
// names need SQL's quoting (a space, a capital, a double quote).
func TestTheDeferrableStepAltersAForeignKeyWithAnOddName(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const parent, child = `aveloxis_scan."zz r5 Parent"`, `aveloxis_scan."zz r5 ""Child"""`
	drop := func() {
		store.pool.Exec(ctx, `DROP TABLE IF EXISTS `+child)
		store.pool.Exec(ctx, `DROP TABLE IF EXISTS `+parent)
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		`CREATE TABLE ` + parent + ` (id BIGINT PRIMARY KEY)`,
		`CREATE TABLE ` + child + ` (id BIGINT, CONSTRAINT "fk Odd ""name""" FOREIGN KEY (id) REFERENCES ` + parent + ` (id))`,
	} {
		if _, err := store.pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	deferrable := func() (d bool) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT condeferrable FROM pg_constraint WHERE conname = 'fk Odd "name"'`).Scan(&d); err != nil {
			t.Fatal(err)
		}
		return d
	}
	if deferrable() {
		t.Fatal("the fixture's foreign key is already deferrable: the step would have nothing to alter")
	}
	var errs []error
	ensureAllFKsDeferrable(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) > 0 {
		t.Fatalf("the step reported errors: %v", errs)
	}
	if !deferrable() {
		t.Error("the foreign key is not deferrable after the step")
	}
}

// tableRowCounts counts every base table of the schemas it is given,
// including one whose name needs quoting.
func TestTableRowCountsCountsEveryTableIncludingAnOddName(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const odd = `aveloxis_scan."zz r5 ""Count"" Table"`
	drop := func() { store.pool.Exec(ctx, `DROP TABLE IF EXISTS `+odd) }
	drop()
	t.Cleanup(drop)
	if _, err := store.pool.Exec(ctx, `CREATE TABLE `+odd+` AS SELECT g AS n FROM generate_series(1, 3) g`); err != nil {
		t.Fatal(err)
	}
	counts, err := tableRowCounts(ctx, store.pool, []string{"aveloxis_scan", "aveloxis_ops"})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := counts[`aveloxis_scan.zz r5 "Count" Table`]; !ok || got != 3 {
		t.Errorf("the odd-named table counted %d (present %v), want 3; keys are schema.table", got, ok)
	}
	if _, ok := counts["aveloxis_ops.collection_queue"]; !ok {
		t.Error("aveloxis_ops.collection_queue was not counted")
	}
	var want int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema IN ('aveloxis_scan', 'aveloxis_ops')`).Scan(&want); err != nil {
		t.Fatal(err)
	}
	// Another test may create or drop a scratch table meanwhile; the count
	// is of the same order, never a fraction of it.
	if len(counts) < want-5 || len(counts) > want+5 {
		t.Errorf("%d tables counted, the catalog has %d", len(counts), want)
	}
}

// The closed_by dry run's count is a statement the database accepts, and
// it writes nothing.
func TestTheClosedByDryRunCountsAndWritesNothing(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	filled := func() (n int64) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.issues WHERE closed_by_id IS NOT NULL`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := filled()
	n, err := store.BackfillClosedByFromEvents(ctx, 1000, 0, true)
	if err != nil {
		t.Fatalf("the dry run failed: %v", err)
	}
	if n < 0 {
		t.Errorf("the dry run counted %d", n)
	}
	if after := filled(); after != before {
		t.Errorf("the dry run wrote: %d issues had closed_by_id, now %d", before, after)
	}
}
