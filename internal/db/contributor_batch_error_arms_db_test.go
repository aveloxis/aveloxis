// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The contributor batch's error arms, against a real database (0.29.214).
//
// Each contributor of a batch, and each of its identities, is written
// under a savepoint so that one failed row does not abort the batch's
// transaction. Until now the arms that run AFTER a failure (roll back to
// the savepoint, record what failed, release, go on) were run only against
// a stand-in transaction that sent nothing, so PostgreSQL had never been
// asked whether the bracket holds.
//
// A failure is made here without touching production code: a second
// session holds a row lock on the one row a statement needs, and the
// batch's transaction has a short lock_timeout, so that statement fails
// with 55P03 (lock_not_available: not a deadlock, so the batch does not
// retry it). Everything else in the transaction must still work.

// batchTx is a transaction for the contributor batch's functions, with a
// lock_timeout, rolled back at the end of the test.
func batchTx(t *testing.T, store *PostgresStore) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '250ms'`); err != nil {
		t.Fatal(err)
	}
	return tx
}

// holdRow locks the rows a statement selects, in a session of its own,
// until the test ends.
func holdRow(t *testing.T, store *PostgresStore, selectForUpdate string, args ...any) {
	t.Helper()
	ctx := context.Background()
	held, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Rollback(context.Background()) })
	tag, err := held.Exec(ctx, selectForUpdate, args...)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("%q locked %d rows, want the one row the test is about", selectForUpdate, tag.RowsAffected())
	}
}

// captured records what the batch reports as failed.
type captured struct {
	kinds []string
	errs  []error
}

func (c *captured) add(kind, _ string, e error) {
	c.kinds = append(c.kinds, kind)
	c.errs = append(c.errs, e)
}

func (c *captured) allLockTimeouts(t *testing.T) {
	t.Helper()
	for i, e := range c.errs {
		var pgErr *pgconn.PgError
		if !errors.As(e, &pgErr) || pgErr.Code != "55P03" {
			t.Errorf("the failure recorded as %s is %v; this test makes a lock timeout (55P03)", c.kinds[i], e)
		}
	}
}

// txAlive: the transaction can still run a statement (it was not left
// aborted).
func txAlive(t *testing.T, tx pgx.Tx) {
	t.Helper()
	var one int
	if err := tx.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("the batch's transaction is unusable after a contained failure: %v", err)
	}
}

// savepointsGone: none of the batch's savepoints up to n is still defined
// in the transaction (each was released, also on the failure arms: a
// savepoint left behind is held, with its locks, to the end of the batch).
// Rolling back to a savepoint that does not exist is an error, which this
// takes inside a savepoint of its own so that the transaction lives.
func savepointsGone(t *testing.T, tx pgx.Tx, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("%s_%d", prefix, i)
		if _, err := tx.Exec(ctx, `SAVEPOINT zz_probe`); err != nil {
			t.Fatal(err)
		}
		_, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT `+name)
		var pgErr *pgconn.PgError
		gone := errors.As(err, &pgErr) && pgErr.Code == "3B001"
		if _, rbErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT zz_probe`); rbErr != nil {
			t.Fatal(rbErr)
		}
		if _, relErr := tx.Exec(ctx, `RELEASE SAVEPOINT zz_probe`); relErr != nil {
			t.Fatal(relErr)
		}
		if !gone {
			t.Errorf("the savepoint %s is still defined after its contributor or identity was dealt with (rolling back to it gave %v): it was not released", name, err)
		}
	}
}

// pathsUserID is a platform user id no other row has (above 2^32, where
// no forge is yet, and where the migration tooling's 32-bit casts do not
// look).
func pathsUserID() int64 { return time.Now().UnixNano()%900_000_000 + 5_000_000_000 + pathsSeq.Add(1) }

// A contributor whose deterministic id is already stored under an older
// login, and whom the batch's pre-probe did not report: the INSERT fails on
// the primary key, the savepoint is rolled back, and the stored row is
// relabelled (its first login kept).
func TestARenamedContributorIsRecoveredAfterThePrimaryKeyCollision(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := pathsUserID()
	tag := pathsTag("rename")
	idents := []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: uid, Login: tag + "-new", NodeID: "N"}}
	id := desiredCntrbIDFor(idents)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_company, cntrb_deleted) VALUES ($1::uuid, $2, $2, 'kept co', 0)`, id, tag+"-old")
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var got captured
	sp := 0
	contrib := &model.Contributor{Login: tag + "-new", Email: "new@example.org", Company: "new co", Location: "here"}
	if err := store.upsertOneContributor(ctx, tx, contrib.Login, contrib, idents, nil, got.add, &sp); err != nil {
		t.Fatal(err)
	}
	if len(got.kinds) != 0 {
		t.Errorf("a recovered rename was reported as failed: %v %v", got.kinds, got.errs)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the batch's transaction did not commit after the recovery: %v", err)
	}
	if row := pathsText(t, store, `SELECT cntrb_login || '|' || gh_login || '|' || cntrb_company || '|' || cntrb_location || '|' || cntrb_email FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id); row != tag+"-old|"+tag+"-new|kept co|here|new@example.org" {
		t.Errorf("the stored contributor after the recovery is %q; want its first login kept, the new one as gh_login, its company kept and its empty fields filled", row)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, tag+"-new"); n != 0 {
		t.Errorf("%d rows stored under the new login, want none: the person has one row", n)
	}
	if src := pathsText(t, store, `SELECT source FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid AND login = $2`, id, tag+"-new"); src != LoginSourceRenameRecovery {
		t.Errorf("the new login's history row has source %q, want %q", src, LoginSourceRenameRecovery)
	}
}

// The same collision while another session holds the stored row: the
// pre-probe's relabel fails, then the INSERT collides, then the recovery
// relabel fails too. Each failure is contained by the savepoint and
// reported, the contributor is skipped, and the transaction lives.
func TestAContributorWhoseStoredRowIsLockedIsSkippedAndTheBatchLives(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := pathsUserID()
	tag := pathsTag("locked")
	idents := []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: uid, Login: tag + "-new"}}
	id := desiredCntrbIDFor(idents)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_deleted) VALUES ($1::uuid, $2, $2, 0)`, id, tag+"-old")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	holdRow(t, store, `SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid FOR UPDATE`, id)
	tx := batchTx(t, store)
	var got captured
	sp := 0
	contrib := &model.Contributor{Login: tag + "-new"}
	if err := store.upsertOneContributor(ctx, tx, contrib.Login, contrib, idents, map[string]string{id: tag + "-old"}, got.add, &sp); err != nil {
		t.Fatalf("a contributor whose row is locked ended the batch: %v", err)
	}
	if want := []string{"contributors_rename_preprobe_update", "contributors_rename_update"}; !reflect.DeepEqual(got.kinds, want) {
		t.Errorf("reported as failed: %v, want %v", got.kinds, want)
	}
	got.allLockTimeouts(t)
	txAlive(t, tx)
	if sp != 1 {
		t.Errorf("%d savepoints were taken, want the contributor's one: its identities are not written once it is skipped", sp)
	}
	savepointsGone(t, tx, "cntrb_sp", 1)
	// The next contributor of the same batch is written.
	other := &model.Contributor{Login: tag + "-other"}
	if err := store.upsertOneContributor(ctx, tx, other.Login, other, nil, nil, got.add, &sp); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, tag+"-other").Scan(&n); err != nil || n != 1 {
		t.Errorf("the next contributor of the batch: %d rows (%v), want it written", n, err)
	}
	if gh := pathsText(t, store, `SELECT gh_login FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id); gh != tag+"-old" {
		t.Errorf("the locked row's gh_login is %q: it was changed", gh)
	}
}

// A failure that is no rename (here: the stored row of the same LOGIN is
// locked, so the upsert's own update cannot take it): reported as an insert
// failure, the contributor skipped, the transaction alive.
func TestAContributorWhoseInsertFailsIsSkippedAndTheBatchLives(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("insfail")
	stored := seedR6Contributor(t, store, tag, "", nil)
	holdRow(t, store, `SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid FOR UPDATE`, stored)
	tx := batchTx(t, store)
	var got captured
	sp := 0
	contrib := &model.Contributor{Login: tag, Company: "would be filled"}
	if err := store.upsertOneContributor(ctx, tx, tag, contrib, nil, nil, got.add, &sp); err != nil {
		t.Fatalf("a contributor whose insert failed ended the batch: %v", err)
	}
	if want := []string{"contributors_insert"}; !reflect.DeepEqual(got.kinds, want) {
		t.Errorf("reported as failed: %v, want %v", got.kinds, want)
	}
	got.allLockTimeouts(t)
	txAlive(t, tx)
	savepointsGone(t, tx, "cntrb_sp", 1)
	if co := pathsText(t, store, `SELECT COALESCE(cntrb_company, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, stored); co != "" {
		t.Errorf("the locked row's company is %q: it was changed", co)
	}
}

// The identity loop: an identity whose own row, whose login-history row, or
// whose contributor row cannot be taken is reported, its savepoint rolled
// back and released, and the NEXT identity of the same contributor is
// written.
func TestAnIdentityThatFailsIsSkippedAndTheNextOneIsWritten(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	for name, c := range map[string]struct {
		// lock is what the second session holds: the first identity's row,
		// its history row, or the contributor's row.
		lock string
		want string
		// nextWritten: whether the second identity gets its row (it does
		// not when the contributor's own row is what is locked: the
		// mirror onto that row fails for it too).
		nextWritten bool
	}{
		"the identity's row is locked": {"identity", "contributor_identities_insert", true},
		"its history row is locked":    {"history", "contributor_login_history_insert", true},
		"the contributor is locked":    {"contributor", "identity_denorm_backfill", false},
	} {
		t.Run(name, func(t *testing.T) {
			tag := pathsTag("ident")
			cntrb := seedR6Contributor(t, store, tag, "", nil)
			first, second := pathsUserID(), pathsUserID()
			idents := []model.ContributorIdentity{
				{Platform: model.PlatformGitHub, UserID: first, Login: tag + "-a"},
				{Platform: model.PlatformGitHub, UserID: second, Login: tag + "-b"},
			}
			switch c.lock {
			case "identity":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 1, $2, 'stored')`, cntrb, first)
				holdRow(t, store, `SELECT 1 FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1 FOR UPDATE`, first)
			case "history":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_login_history (cntrb_id, platform_id, login, source) VALUES ($1::uuid, 1, $2, 'observation')`, cntrb, tag+"-a")
				holdRow(t, store, `SELECT 1 FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid AND login = $2 FOR UPDATE`, cntrb, tag+"-a")
			case "contributor":
				holdRow(t, store, `SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid FOR NO KEY UPDATE`, cntrb)
			}
			tx := batchTx(t, store)
			var got captured
			sp := 0
			if err := store.upsertContributorIdentities(ctx, tx, cntrb, tag, &model.Contributor{Login: tag}, idents, false, got.add, &sp); err != nil {
				t.Fatalf("an identity that failed ended the batch: %v", err)
			}
			want := []string{c.want}
			if !c.nextWritten {
				want = append(want, c.want)
			}
			if !reflect.DeepEqual(got.kinds, want) {
				t.Errorf("reported as failed: %v, want %v", got.kinds, want)
			}
			got.allLockTimeouts(t)
			txAlive(t, tx)
			if sp != 2 {
				t.Errorf("%d savepoints were taken, want one for each of the two identities", sp)
			}
			var next int
			if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1 AND login = $2`, second, tag+"-b").Scan(&next); err != nil {
				t.Fatal(err)
			}
			if (next == 1) != c.nextWritten {
				t.Errorf("the second identity has %d rows in the batch's transaction; written = %v is wanted", next, c.nextWritten)
			}
			// What the failed identity's savepoint rolled back is gone
			// from the transaction: its identity row is as it was stored.
			var firstLogin *string
			if err := tx.QueryRow(ctx, `SELECT MAX(login) FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, first).Scan(&firstLogin); err != nil {
				t.Fatal(err)
			}
			switch {
			case c.lock == "identity" && (firstLogin == nil || *firstLogin != "stored"):
				t.Errorf("the locked identity row reads %v in the batch's transaction, want it as stored", firstLogin)
			case c.lock != "identity" && firstLogin != nil:
				t.Errorf("the failed identity left a row (%q) in the batch's transaction: its savepoint was not rolled back", *firstLogin)
			}
			savepointsGone(t, tx, "ident_sp", 2)
		})
	}
}

// Two writers of one address, the second unlocked (the commit resolver):
// CreateEmailOnlyContributor has inserted its contributor and is about to
// record the alias when the other writer's alias for the same address, to
// ANOTHER active contributor, commits. The function must adopt that owner
// and drop the row it inserted, or the address has two active contributors
// and one of them owns no alias.
//
// The order is made certain without a seam: the other writer's alias row
// is inserted in a transaction the test holds open, so the function's own
// alias insert waits on it (the address is unique); the test commits once
// it sees the function waiting.
func TestAnEmailOnlyContributorYieldsToTheAliasAnotherWriterCommitted(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("race")
	email := tag + "@example.org"
	owner := seedR6Contributor(t, store, tag+"-owner", "", nil)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email = $1`, email)
	})
	rival, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := false
	t.Cleanup(func() {
		if !committed {
			_ = rival.Rollback(context.Background())
		}
	})
	if _, err := rival.Exec(ctx, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email) VALUES ($1::uuid, $2, $2)`, owner, email); err != nil {
		t.Fatal(err)
	}

	type result struct {
		id  string
		err error
	}
	done := make(chan result, 1)
	go func() {
		id, err := store.CreateEmailOnlyContributor(ctx, email)
		done <- result{id, err}
	}()
	// Wait until the function is waiting on the rival's uncommitted alias.
	deadline := time.Now().Add(20 * time.Second)
	for {
		var waiting int
		if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%contributors_aliases%' AND pid <> pg_backend_pid()`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case r := <-done:
			t.Fatalf("the function returned (%q, %v) before it met the rival's alias: the order this test needs was not established", r.id, r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the function did not reach its alias insert within 20 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := rival.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	committed = true
	var got result
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the function did not return within 20 s of the rival's commit")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.id != owner {
		t.Errorf("the function returned %s; want the contributor the committed alias points at, %s", got.id, owner)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.contributors WHERE cntrb_email = $1`, email); n != 0 {
		t.Errorf("%d email-only contributors were left for the address, want none: the function drops the row it inserted", n)
	}
	if got := pathsText(t, store, `SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email); got != owner {
		t.Errorf("the alias points at %s after the race, want it left with %s", got, owner)
	}
}

// An address a SOFT-DELETED contributor has (a merge's loser) does not
// answer for the address: the function makes, or finds, an active one
// (round 8: no deleted row was seeded, and a lookup that took any row
// passed). A dead owner is an id nothing resolves to.
func TestAnEmailOnlyContributorIsNotADeletedRow(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	tag := pathsTag("dead")
	email := tag + "@example.org"
	dead := seedR6Contributor(t, store, tag+"-dead", email, nil)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_deleted = 1 WHERE cntrb_id = $1::uuid`, dead)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email = $1 AND cntrb_login = ''`, email)
	})
	id, err := store.CreateEmailOnlyContributor(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" || id == dead {
		t.Fatalf("the function returned %q for an address only a deleted contributor (%s) has; want a new, active one", id, dead)
	}
	if r6Deleted(t, store, id) != 0 {
		t.Errorf("the contributor returned is soft-deleted")
	}
	if got := pathsText(t, store, `SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email); got != id {
		t.Errorf("the address's alias points at %s, want the contributor returned, %s", got, id)
	}
	// A second call finds that one.
	if again, err := store.CreateEmailOnlyContributor(ctx, email); err != nil || again != id {
		t.Errorf("a second call returned %q (%v), want the same contributor", again, err)
	}
}
