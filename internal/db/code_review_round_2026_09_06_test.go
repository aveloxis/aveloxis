// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestAliasRepairTreatsNullDeletedAsActive (code-review round 2026-09-06,
// finding 1 — empirically reproduced): cntrb_deleted IS NULL means ACTIVE
// everywhere in this repo (the pre-v0.20.2 cohort; the convention is
// COALESCE(cntrb_deleted, 0) = 0). The first round-29 spelling COALESCEd
// the whole subquery so a NULL-deleted owner read as DEAD and their alias
// was deterministically stolen and restamped on every
// EnsureContributorAlias touch.
func TestAliasRepairTreatsNullDeletedAsActive(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	tag := time.Now().UnixNano()
	email := fmt.Sprintf("avcr1+%d@example.org", tag)

	// A LEGACY-shaped owner: cntrb_deleted NULL (not 0).
	var legacyID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, cntrb_deleted, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', NULL, 'test', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, &legacyID)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases
		(cntrb_id, canonical_email, alias_email, cntrb_active, tool_source, data_source, data_collection_date)
		VALUES ($1::uuid, $2, $2, 1, 'test', 'Mailing List', NOW())`, legacyID, email)

	var rivalID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', 'test', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, &rivalID)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text IN ($1, $2)`, legacyID, rivalID)
	})

	if err := store.EnsureContributorAlias(ctx, rivalID, email, "test", "Mailing List"); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err := store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != legacyID {
		t.Fatalf("a NULL-deleted owner is ACTIVE — their alias must never be stolen: want %s, got %s", legacyID, owner)
	}
}

// TestAliasUpsertIsTwoStatementsNotDoUpdateSubquery (finding 2 —
// empirically reproduced snapshot race): a DO UPDATE's correlated WHERE
// subquery evaluates under the STATEMENT snapshot while the conflict
// arbitration re-reads the newest alias row — a rival that blocked on a
// concurrent creator's commit could not see the just-committed owner,
// read "missing = dead", and STOLE the alias from an ACTIVE owner. The
// repair must be a SEPARATE statement (fresh snapshot after the block).
func TestAliasUpsertIsTwoStatementsNotDoUpdateSubquery(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/commit_resolver_store.go"))
	if !strings.Contains(src, "contributorAliasInsertSQL") || !strings.Contains(src, "contributorAliasRepairSQL") {
		t.Fatal("the alias upsert must be the two-statement insert+repair pair (code-review round 2026-09-06)")
	}
	insert := srctest.BacktickLiterals(src)
	for _, lit := range insert {
		if strings.Contains(lit, "ON CONFLICT (alias_email) DO UPDATE") {
			t.Error("the alias insert must be ON CONFLICT DO NOTHING — a DO UPDATE WHERE subquery evaluates under the statement snapshot and steals from just-committed active owners (reproduced live)")
		}
	}
	// The repair guard must use the repo's NULL-means-active convention.
	if !strings.Contains(src, "COALESCE(dead.cntrb_deleted, 0) <> 0") {
		t.Error("the repair's dead-owner guard must spell COALESCE(dead.cntrb_deleted, 0) <> 0 — NULL means ACTIVE (finding 1)")
	}
	// canonical_email must never downgrade to '' (finding 9).
	if !strings.Contains(src, "NULLIF((SELECT cntrb_canonical") {
		t.Error("canonical_email must go through NULLIF so an empty stored canonical falls back to the alias email (finding 9)")
	}
}

// TestCreateEmailOnlyBeltResolvesRivalWinner (finding 4 + 10): an UNLOCKED
// alias writer can commit this alias_email toward a different ACTIVE
// contributor while the locked creator holds the per-email lock. The belt
// re-read must then DROP the creator's own insert and adopt the alias's
// actual owner — and its errors must surface, never be discarded.
func TestCreateEmailOnlyBeltResolvesRivalWinner(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/mailinglist_sender_resolve_store.go"),
		"func (s *PostgresStore) CreateEmailOnlyContributor("))
	if strings.Contains(body, "_ = tx.QueryRow") {
		t.Error("the belt re-read is load-bearing — its error must not be discarded (finding 10)")
	}
	for _, needle := range []string{
		"owner != id",
		"DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid",
		"errors.Is(berr, pgx.ErrNoRows)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("CreateEmailOnlyContributor's belt must contain %q — drop the duplicate insert and adopt the alias's actual owner when a rival won (finding 4)", needle)
		}
	}
}

// TestActiveAliasOwnerPredicateIsShared (finding 14, SR-17): the
// "alias owned by an ACTIVE contributor" predicate must have ONE spelling
// consumed by the candidates anti-join, the create-path probe, and the
// belt re-read.
func TestActiveAliasOwnerPredicateIsShared(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/mailinglist_sender_resolve_store.go"))
	uses := strings.Count(src, "fmt.Sprintf(activeAliasOwnerCoreSQL,")
	if uses < 3 {
		t.Errorf("activeAliasOwnerCoreSQL must be consumed by all three sites (anti-join, probe, belt); found %d uses", uses)
	}
	// No residual inline spelling of the join.
	if strings.Count(src, "JOIN aveloxis_data.contributors c2 ON c2.cntrb_id = a.cntrb_id") > 1 {
		t.Error("the active-owner join may be spelled only once (inside activeAliasOwnerCoreSQL) — inline copies drift (SR-17)")
	}
}

// TestHistoryFailureCooldownRetiresFailingContributor (finding 3): a
// contributor whose history fetch persistently fails must leave the claim
// head for HistoryFailureCooldown — the claim is ORDER BY
// gh_history_backfilled_at NULLS FIRST with no other failure signal, so a
// stuck account would otherwise be re-claimed every tick, burning a
// subdivision chain per claim until the failing cohort pins the batch.
func TestHistoryFailureCooldownRetiresFailingContributor(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	tag := time.Now().UnixNano()
	login := fmt.Sprintf("_avcr3-%d", tag)

	var id string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', $1, 'test', 'test', NOW())
		RETURNING cntrb_id::text`, &id, login)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, id)
	})

	claimed := func() bool {
		out, err := store.GetContributorsForHistoryBackfill(ctx, 1000000, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range out {
			if c.ID == id {
				return true
			}
		}
		return false
	}

	if !claimed() {
		t.Fatal("precondition: a never-attempted contributor must be claimable")
	}

	// A fresh failure stamp retires it.
	if err := store.MarkHistoryFetchFailed(ctx, id); err != nil {
		t.Fatal(err)
	}
	if claimed() {
		t.Fatal("a contributor stamped failed within HistoryFailureCooldown must NOT be re-claimed")
	}

	// A stale stamp (past the cooldown) re-admits it.
	mustExecRetry(ctx, t, store,
		`UPDATE aveloxis_data.contributors SET gh_history_failed_at = NOW() - INTERVAL '25 hours' WHERE cntrb_id::text = $1`, id)
	if !claimed() {
		t.Fatal("a failure stamp older than the cooldown must re-admit the contributor")
	}

	// A successful store CLEARS the stamp.
	if err := store.MarkHistoryFetchFailed(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkHistoryBackfilled(ctx, id); err != nil {
		t.Fatal(err)
	}
	var failedAt *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT gh_history_failed_at FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, id).Scan(&failedAt); err != nil {
		t.Fatal(err)
	}
	if failedAt != nil {
		t.Fatal("a success stamp must CLEAR gh_history_failed_at")
	}
}
