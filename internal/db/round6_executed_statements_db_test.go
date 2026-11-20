// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// Review round 6 (S6-1, S6-2): 0.29.212 moved these statements into builder
// functions, and the write-policy check runs each builder and judges its
// text. That shows the text obeys the policy; it does not show PostgreSQL
// accepts it. Each statement is SENT here. (The 8Knot refresh is sent by
// TestRunMigrationsOnFreshDB, where the views exist.)

// The dm_ aggregate rebuilds: every delete and insert of both lists.
func TestTheAggregateRebuildStatementsRun(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if len(repoAggQueries) == 0 || len(groupAggQueries) == 0 {
		t.Fatal("an aggregate list is empty: nothing would be sent")
	}
	// A repository and a group with no rows: every statement still runs.
	if err := store.RefreshRepoAggregates(ctx, -6001); err != nil {
		t.Errorf("RefreshRepoAggregates: %v", err)
	}
	if err := store.RefreshGroupAggregates(ctx, -6001); err != nil {
		t.Errorf("RefreshGroupAggregates: %v", err)
	}
}

// The assignment backfill's dry-run count and the closed-by sweep's list,
// with and without a limit.
func TestTheBackfillCountAndTheSweepListRun(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if n, err := store.BackfillAssignmentIdentities(ctx, 1000, 0, true); err != nil || n < 0 {
		t.Errorf("BackfillAssignmentIdentities dry run = %d, %v", n, err)
	}
	for _, limit := range []int64{0, 3} {
		got, err := store.IssuesNeedingClosedBySweep(ctx, limit)
		if err != nil {
			t.Errorf("IssuesNeedingClosedBySweep(%d): %v", limit, err)
		}
		if limit > 0 && int64(len(got)) > limit {
			t.Errorf("IssuesNeedingClosedBySweep(%d) returned %d rows", limit, len(got))
		}
	}
}

// The three foreign-key steps of the migrate act only on a key that is not
// yet in the wanted state, which no fresh database has. Here one key of
// each list is put back as a plain one (no update rule, not deferrable),
// and each step must bring it to the full behaviour: its ADD and its
// VALIDATE statements are sent.
func TestTheForeignKeyStepsRepairAPlainKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	run := func(name string, step func(context.Context, *PostgresStore, *slog.Logger, *[]error)) {
		t.Helper()
		var errs []error
		step(ctx, store, logger, &errs)
		if len(errs) > 0 {
			t.Fatalf("%s: %v", name, errs)
		}
	}
	// Whatever happens below, the three steps leave every key as the
	// migrate leaves it.
	t.Cleanup(func() {
		var errs []error
		ensureOnUpdateCascadeOnCntrbIDFKs(ctx, store, logger, &errs)
		ensureCntrbIDFKsFullBehavior(ctx, store, logger, &errs)
		ensureExtraFKConstraints(ctx, store, logger, &errs)
		if len(errs) > 0 {
			t.Errorf("restoring the foreign keys: %v", errs)
		}
	})
	plain := func(table, column, parent, parentCol, constraint string) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE aveloxis_data.%s DROP CONSTRAINT IF EXISTS %s, ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES aveloxis_data.%s(%s)`,
			table, constraint, constraint, column, parent, parentCol)); err != nil {
			t.Fatalf("making %s a plain key: %v", constraint, err)
		}
	}
	state := func(constraint string) (full, validated bool, updateRule string) {
		t.Helper()
		full, exists, err := constraintStateMatches(ctx, store, constraint, "CASCADE", "RESTRICT", true)
		if err != nil || !exists {
			t.Fatalf("%s: exists %v, %v", constraint, exists, err)
		}
		if err := store.pool.QueryRow(ctx, `SELECT con.convalidated, rc.update_rule FROM pg_constraint con
			JOIN information_schema.referential_constraints rc ON rc.constraint_name = con.conname AND rc.constraint_schema = 'aveloxis_data'
			WHERE con.conname = $1`, constraint).Scan(&validated, &updateRule); err != nil {
			t.Fatal(err)
		}
		return full, validated, updateRule
	}

	c := cntrbIDChildFKs[len(cntrbIDChildFKs)-1]
	plain(c.table, c.column, "contributors", "cntrb_id", c.constraint)
	if full, _, rule := state(c.constraint); full || rule == "CASCADE" {
		t.Fatalf("the fixture key %s is not plain (update rule %s)", c.constraint, rule)
	}
	run("ensureOnUpdateCascadeOnCntrbIDFKs", ensureOnUpdateCascadeOnCntrbIDFKs)
	if full, validated, rule := state(c.constraint); rule != "CASCADE" || !validated || full {
		t.Errorf("after the v0.22.1 step %s has update rule %s, validated %v, full behaviour %v; want CASCADE, validated, not yet full", c.constraint, rule, validated, full)
	}
	run("ensureCntrbIDFKsFullBehavior", ensureCntrbIDFKsFullBehavior)
	if full, validated, _ := state(c.constraint); !full || !validated {
		t.Errorf("after the v0.22.7 step %s: full behaviour %v, validated %v", c.constraint, full, validated)
	}

	e := fkExtraConstraints[len(fkExtraConstraints)-1]
	plain(e.table, e.column, e.parent, e.parentCol, e.constraintName)
	if full, _, _ := state(e.constraintName); full {
		t.Fatalf("the fixture key %s is not plain", e.constraintName)
	}
	run("ensureExtraFKConstraints", ensureExtraFKConstraints)
	if full, validated, _ := state(e.constraintName); !full || !validated {
		t.Errorf("after the step %s: full behaviour %v, validated %v", e.constraintName, full, validated)
	}
}
