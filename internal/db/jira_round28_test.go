// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCreateEmailOnlyContributorConcurrentIsIdempotent (Copilot round 28,
// PR #193): two scheduler processes creating the same email-only contributor
// concurrently must NOT leave duplicate active contributors — the per-email
// advisory lock serializes them, so exactly one row exists and every caller
// returns the same id (the one the alias points at). Run under -race.
func TestCreateEmailOnlyContributorConcurrentIsIdempotent(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	email := fmt.Sprintf("avr28+%d@example.org", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email = $1`, email)
	})

	const N = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make([]string, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], errs[i] = store.CreateEmailOnlyContributor(ctx, email)
		}(i)
	}
	close(start) // release all goroutines together
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d: %v", i, e)
		}
	}
	for i := 1; i < N; i++ {
		if ids[i] != ids[0] {
			t.Errorf("goroutine %d returned %s, want %s — all concurrent callers must agree", i, ids[i], ids[0])
		}
	}
	var cnt int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_email = $1 AND COALESCE(cntrb_deleted, 0) = 0`, email).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Errorf("exactly one contributor per email, got %d — the concurrent-create race is not serialized", cnt)
	}
}
