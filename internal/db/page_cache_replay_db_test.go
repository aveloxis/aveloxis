// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// D2 (0.29.122, summary/63 §8): the API's re-warm list survives a restart.
// Save applies a difference in one transaction — upserts (a repeated key
// takes the new cost) and deletes — and Load reads the set back.
func TestPageCacheReplayRoundTrip(t *testing.T) {
	ctx, store := caseConnect(t)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.page_cache_replay WHERE uri LIKE '/_avreplay/%'`)
	})
	a := PageCacheReplayKey{RepoID: 1, URI: "/_avreplay/a"}
	b := PageCacheReplayKey{RepoID: 2, URI: "/_avreplay/b?x=1"}
	list, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(list.Release)
	// Every load and save carries a deadline, as the API's do (M1).
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := list.SavePageCacheReplay(ctx, nil, nil); err != nil {
		t.Fatalf("an empty difference is a no-op, got %v", err)
	}
	if err := list.SavePageCacheReplay(ctx, []PageCacheReplayRow{{Key: a, CostMS: 5}, {Key: b, CostMS: 7}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := list.SavePageCacheReplay(ctx, []PageCacheReplayRow{{Key: b, CostMS: 70}}, []PageCacheReplayKey{a}); err != nil {
		t.Fatal(err)
	}
	rows, err := list.LoadPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []PageCacheReplayRow
	for _, r := range rows {
		if len(r.Key.URI) > 11 && r.Key.URI[:11] == "/_avreplay/" {
			mine = append(mine, r)
		}
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].Key.URI < mine[j].Key.URI })
	if len(mine) != 1 || mine[0].Key != b || mine[0].CostMS != 70 {
		t.Errorf("loaded %+v, want only %+v at cost 70", mine, b)
	}
}

// The saved re-warm list has one owner per database (whole-branch review of
// 0.29.119–0.29.123, A1, operator decision 2026-10-29): each API process
// saves the difference from what IT last saved, so a second writer deleted
// rows the first still held. The owner holds an advisory lock for its life;
// a second claim is refused with the typed sentinel; and the owner's loads
// and saves run on the lock's own session, so a session that died (and with
// it the lock, which another process may then take) cannot save.
func TestPageCacheReplayHasOneOwner(t *testing.T) {
	ctx, store := caseConnect(t)
	first, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := store.ClaimPageCacheReplay(ctx); !errors.Is(err, ErrPageCacheReplayHeld) {
		if second != nil {
			second.Release()
		}
		first.Release()
		t.Fatalf("a second claim while the list is owned: got %v, want ErrPageCacheReplayHeld", err)
	}
	// The owner's session ends (a database restart, a dropped connection):
	// its lock is gone, so its save must fail rather than write unowned.
	if !first.Held() {
		t.Fatal("a claimed list's lock is held")
	}
	pid := first.lock.conn.Conn().PgConn().PID()
	if _, err := store.pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	bctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := first.SavePageCacheReplay(bctx, []PageCacheReplayRow{{Key: PageCacheReplayKey{RepoID: 1, URI: "/_avreplay/dead"}}}, nil); err == nil {
		t.Error("a save after the owner's session ended succeeded: it must run on the lock's own session")
	}
	if first.Held() {
		t.Error("Held must answer false once the lock's session ended (the api gives the list up and claims it again)")
	}
	first.Release()
	next, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatalf("the list must be claimable once its owner's session ended, got %v", err)
	}
	next.Release()
	again, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatalf("a released list must be claimable, got %v", err)
	}
	again.Release()
}

// Copilot's review of PR #246 (0.29.124), M1: the saved list's reads and
// writes run on the lock's connection, detached from the caller's cancel, so
// a blocked statement held later passes and the stop — the store refuses a
// context with no deadline (SR-18: the layer that writes enforces the
// bound), and the API gives every call one.
func TestPageCacheReplayRefusesAnUnboundedContext(t *testing.T) {
	ctx, store := caseConnect(t)
	list, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(list.Release)
	row := PageCacheReplayRow{Key: PageCacheReplayKey{RepoID: 1, URI: "/_avreplay/bound"}}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.page_cache_replay WHERE uri LIKE '/_avreplay/%'`)
	})
	if err := list.SavePageCacheReplay(context.Background(), []PageCacheReplayRow{row}, nil); !errors.Is(err, ErrPageCacheReplayUnbounded) {
		t.Errorf("a save with no deadline: %v, want ErrPageCacheReplayUnbounded", err)
	}
	if _, err := list.LoadPageCacheReplay(context.Background()); !errors.Is(err, ErrPageCacheReplayUnbounded) {
		t.Errorf("a load with no deadline: %v, want ErrPageCacheReplayUnbounded", err)
	}
	bctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := list.SavePageCacheReplay(bctx, []PageCacheReplayRow{row}, nil); err != nil {
		t.Errorf("a bounded save: %v", err)
	}
	if _, err := list.LoadPageCacheReplay(bctx); err != nil {
		t.Errorf("a bounded load: %v", err)
	}
}

// Copilot's review of PR #246, M2: the list's primary key holds the request
// URI, and a btree entry has a size limit — one over-long row (a crafted
// license filter) failed the whole pass's save. MaxPageCacheReplayURIBytes
// is derived from that limit; a URI of that many incompressible bytes saves,
// and one of the limit itself does not.
func TestPageCacheReplayURIBoundFitsTheIndex(t *testing.T) {
	ctx, store := caseConnect(t)
	list, err := store.ClaimPageCacheReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(list.Release)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.page_cache_replay WHERE repo_id = 424242`)
	})
	bctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	incompressible := func(n int) string {
		raw := make([]byte, n)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)[:n]
	}
	fits := PageCacheReplayRow{Key: PageCacheReplayKey{RepoID: 424242, URI: incompressible(MaxPageCacheReplayURIBytes)}}
	if err := list.SavePageCacheReplay(bctx, []PageCacheReplayRow{fits}, nil); err != nil {
		t.Errorf("a URI of MaxPageCacheReplayURIBytes (%d) must save: %v", MaxPageCacheReplayURIBytes, err)
	}
	over := PageCacheReplayRow{Key: PageCacheReplayKey{RepoID: 424242, URI: incompressible(btreeMaxIndexTupleBytes)}}
	var pgErr *pgconn.PgError
	if err := list.SavePageCacheReplay(bctx, []PageCacheReplayRow{over}, nil); !errors.As(err, &pgErr) || pgErr.Code != "54000" {
		t.Errorf("a URI of the btree limit itself (%d bytes) must exceed the index entry (SQLSTATE 54000), got %v", btreeMaxIndexTupleBytes, err)
	}
}
