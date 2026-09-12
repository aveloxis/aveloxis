// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// 2026-09-12 chaoss.tv analysis: the pool governed WHICH key a caller used
// but never WHETHER it could spend. One background sweep demanded 113% of
// the fleet's GraphQL budget; keys depleted; the per-key 500-point
// background cliff shrank the eligible set; the shared round-robin cursor
// funnelled up to 96% of 192 concurrent requests onto one survivor; that
// key tripped GitHub's per-key secondary limit; and because a 403 never
// marked the key, every other goroutine kept being handed it — 179
// rejections in one second, 46,612 in five days, 54 keys behaving like 3.
//
// These tests pin the admission contract that closes all of it: a lease
// (acquire/release) with a global and a per-key in-flight ceiling, a
// least-loaded selection that spreads instead of funnelling, a per-key
// secondary-limit cooldown so a throttled key rests while the others
// serve, and a POOL-level foreground reservation in place of the per-key
// cliff. Run under -race.

package platform

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// heldAcquire acquires and reports the key while holding the slot until
// the returned release runs — the shape every ceiling test needs.
func heldAcquire(t *testing.T, kp *KeyPool, ctx context.Context, res Resource) (*APIKey, func()) {
	t.Helper()
	k, release, err := kp.Acquire(ctx, res)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	return k, release
}

// checkout acquires and immediately releases — the pre-2026-09-12
// hand-out shape, for sequential tests that only care WHICH key the pool
// picks (rotation, refill, exhaustion, quarantine). With nothing in
// flight, selection reduces to most-remaining then round-robin, so those
// contracts read exactly as they did against GetKey/GetGraphQLKey.
func checkout(kp *KeyPool, ctx context.Context, res Resource) (*APIKey, error) {
	k, release, err := kp.Acquire(ctx, res)
	if err != nil {
		return nil, err
	}
	release()
	return k, nil
}

// TestAcquireNeverExceedsGlobalInflight: 100 goroutines against a global
// ceiling of 5 — the observed peak may never exceed 5, and every acquire
// eventually succeeds once earlier holders release.
func TestAcquireNeverExceedsGlobalInflight(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c", "d"}, rlTestLogger())
	kp.SetAdmission(5, 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var cur, peak int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := kp.Acquire(ctx, ResourceGraphQL)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			n := atomic.AddInt64(&cur, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt64(&cur, -1)
			release()
		}()
	}
	wg.Wait()
	if peak > 5 {
		t.Fatalf("peak in-flight = %d, want <= 5 — the global ceiling is the one thing GitHub's ~100-concurrent secondary limit needs from us", peak)
	}
	if peak == 0 {
		t.Fatal("nothing ever ran — the test is vacuous")
	}
}

// TestAcquireNeverExceedsPerKeyInflight: two keys, per-key ceiling 2, no
// global ceiling. No key may ever carry more than 2 in flight — this is the
// structural guarantee that concentration can never reach a per-token
// secondary limit however the pool is otherwise depleted.
func TestAcquireNeverExceedsPerKeyInflight(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, rlTestLogger())
	kp.SetAdmission(0, 2, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	peak := map[string]*int64{"a": new(int64), "b": new(int64)}
	cur := map[string]*int64{"a": new(int64), "b": new(int64)}
	var wg sync.WaitGroup
	for range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			k, release, err := kp.Acquire(ctx, ResourceGraphQL)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			n := atomic.AddInt64(cur[k.Token], 1)
			for {
				p := atomic.LoadInt64(peak[k.Token])
				if n <= p || atomic.CompareAndSwapInt64(peak[k.Token], p, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt64(cur[k.Token], -1)
			release()
		}()
	}
	wg.Wait()
	for tok, p := range peak {
		if *p > 2 {
			t.Errorf("key %q peak in-flight = %d, want <= 2", tok, *p)
		}
		if *p == 0 {
			t.Errorf("key %q never served — least-loaded selection should have spread across both", tok)
		}
	}
}

// TestReleaseFreesSlotAndIsIdempotent: a release returns the slot exactly
// once; calling it twice must not underflow the counters (a double defer
// is the realistic mistake, and an underflow would let the ceiling admit
// one extra caller forever).
func TestReleaseFreesSlotAndIsIdempotent(t *testing.T) {
	kp := NewKeyPool([]string{"a"}, rlTestLogger())
	kp.SetAdmission(1, 0, 0)
	ctx := context.Background()

	_, release := heldAcquire(t, kp, ctx, ResourceCore)
	release()
	release() // second call must be a no-op

	kp.mu.Lock()
	inflight, keyInflight := kp.inflight, kp.keys[0].inflight
	kp.mu.Unlock()
	if inflight != 0 || keyInflight != 0 {
		t.Fatalf("after double release: pool inflight=%d key inflight=%d, want 0/0 — a second release must not underflow", inflight, keyInflight)
	}
	// And the slot really is free: a second acquire on a ceiling of 1 succeeds.
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, release2 := heldAcquire(t, kp, ctx2, ResourceCore)
	release2()
}

// TestAcquireCtxCancelUnblocksSlotWaiter: a caller blocked on a full
// ceiling must return ctx.Err() promptly when its context is canceled —
// not sit on the condition variable until some unrelated release.
func TestAcquireCtxCancelUnblocksSlotWaiter(t *testing.T) {
	kp := NewKeyPool([]string{"a"}, rlTestLogger())
	kp.SetAdmission(1, 0, 0)

	_, holder := heldAcquire(t, kp, context.Background(), ResourceCore)
	defer holder()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := kp.Acquire(ctx, ResourceCore)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("waiter took %v to observe cancellation", time.Since(start))
	}
}

// TestAcquireSpreadsLeastInflight is the red test for Bug B (the shared
// round-robin cursor). Three of many keys are eligible; the cursor is
// scattered by a foreground call that landed elsewhere; then N concurrent
// acquires are HELD OPEN. Under first-eligible-after-cursor the survivor
// with the longest ineligible run before it takes ~everything; under
// least-inflight each of the three carries ~N/3.
func TestAcquireSpreadsLeastInflight(t *testing.T) {
	tokens := make([]string, 30)
	for i := range tokens {
		tokens[i] = "k" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	kp := NewKeyPool(tokens, rlTestLogger())
	kp.SetAdmission(0, 0, 0)
	// Only indices 0, 1, 2 have graphql budget; everything else is dead
	// for the next hour. Survivors sit at the front so the ineligible run
	// preceding index 0 wraps the whole ring — the worst-case skew shape.
	for i, k := range kp.keys {
		if i > 2 {
			k.GraphQLRemaining = 0
			k.GraphQLResetAt = time.Now().Add(time.Hour)
		}
	}
	kp.mu.Lock()
	kp.rrIndexGQL = 17 // scattered by unrelated traffic
	kp.mu.Unlock()

	const N = 30
	counts := map[string]int{}
	var mu sync.Mutex
	releases := make([]func(), 0, N)
	for range N {
		k, release := heldAcquire(t, kp, context.Background(), ResourceGraphQL)
		mu.Lock()
		counts[k.Token]++
		releases = append(releases, release)
		mu.Unlock()
	}
	for _, r := range releases {
		r()
	}
	for _, tok := range tokens[:3] {
		if c := counts[tok]; c < N/3-2 || c > N/3+2 {
			t.Errorf("survivor %q carried %d of %d held acquires (all: %v) — selection must be least-loaded, not first-eligible-after-cursor", tok, c, N, counts)
		}
	}
}

// TestMarkSecondaryLimitedRestsOnlyThatKey: the Bug C fix. After a key is
// marked for a Retry-After, every subsequent acquire lands on the OTHER
// key; the marked key's budget and auth state are untouched (the three
// causes stay distinguishable); and it returns exactly when the cooldown
// elapses.
func TestMarkSecondaryLimitedRestsOnlyThatKey(t *testing.T) {
	kp := NewKeyPool([]string{"throttled", "healthy"}, rlTestLogger())
	kp.SetAdmission(0, 0, 0)
	th := kp.keys[0]
	th.GraphQLRemaining = 4200

	kp.MarkSecondaryLimited(th, 120*time.Millisecond)

	for i := range 20 {
		k, release := heldAcquire(t, kp, context.Background(), ResourceGraphQL)
		release()
		if k.Token != "healthy" {
			t.Fatalf("acquire %d landed on the secondary-limited key — every other goroutine must NOT keep being handed a throttled key (the 179-in-one-second herd)", i)
		}
	}
	kp.mu.Lock()
	rem, q := th.GraphQLRemaining, th.quarantineUntil
	kp.mu.Unlock()
	if rem != 4200 {
		t.Errorf("GraphQLRemaining changed to %d — a secondary limit is not a primary exhaustion", rem)
	}
	if !q.IsZero() {
		t.Errorf("quarantineUntil set — a secondary limit is not a 401")
	}

	time.Sleep(150 * time.Millisecond)
	// Hold "healthy" so the only eligible key is the recovered one.
	_, hold := heldAcquire(t, kp, context.Background(), ResourceGraphQL)
	kp.SetAdmission(0, 1, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	k, release := heldAcquire(t, kp, ctx, ResourceGraphQL)
	release()
	hold()
	if k.Token != "throttled" {
		t.Fatalf("after the cooldown elapsed the key must be eligible again, got %q", k.Token)
	}
}

// TestForegroundReserveBlocksBackgroundNotForeground: the POOL-level
// reservation that replaces the per-key 500-point cliff. With the pool's
// total graphql budget at or below the reserve, a background caller is
// refused (fast-fail) while a foreground caller is still admitted.
func TestForegroundReserveBlocksBackgroundNotForeground(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25) // reserve 25% of 2 x 5000 = 2500 points
	kp.keys[0].GraphQLRemaining = 1200
	kp.keys[1].GraphQLRemaining = 1300 // total 2500 == reserve line

	bg := WithGraphQLFastFail(WithGraphQLBackgroundBudget(context.Background()))
	if _, _, err := kp.Acquire(bg, ResourceGraphQL); !errors.Is(err, ErrGraphQLBudgetExhausted) {
		t.Fatalf("background at the reserve line: err = %v, want ErrGraphQLBudgetExhausted", err)
	}
	_, release := heldAcquire(t, kp, WithGraphQLFastFail(context.Background()), ResourceGraphQL)
	release()

	// Above the line by exactly 3 points: background admits exactly 3
	// (the round-8 checkout reservation still binds), then refuses.
	kp.keys[1].GraphQLRemaining = 1303
	admitted := 0
	for range 10 {
		_, release, err := kp.Acquire(bg, ResourceGraphQL)
		if err != nil {
			break
		}
		release()
		admitted++
	}
	if admitted != 3 {
		t.Fatalf("background admitted %d above the reserve line, want exactly 3", admitted)
	}
}

// TestFastFailDoesNotFireOnSlotContention: fast-fail means "my budget is
// spent — my subdivision is the retry". A momentarily full in-flight gate
// is not that; turning it into ErrGraphQLBudgetExhausted would make
// subdividing callers halve healthy batches under ordinary contention. A
// fast-fail acquire blocked only by the ceiling must WAIT and succeed.
func TestFastFailDoesNotFireOnSlotContention(t *testing.T) {
	kp := NewKeyPool([]string{"a"}, rlTestLogger())
	kp.SetAdmission(1, 0, 0)

	_, holder := heldAcquire(t, kp, context.Background(), ResourceGraphQL)
	got := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, release, err := kp.Acquire(WithGraphQLFastFail(ctx), ResourceGraphQL)
		if err == nil {
			release()
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-got:
		t.Fatalf("fast-fail acquire returned %v while only the ceiling was full — it must wait, not fail", err)
	default:
	}
	holder()
	if err := <-got; err != nil {
		t.Fatalf("after the holder released: %v", err)
	}
}

// TestLendTokensSpreadsAndReleases: scorecard cannot hold a Go lease for
// a 15-minute subprocess, so it BORROWS tokens through an accounted path
// instead of the old AllTokens bypass. Lends prefer the least-lent keys,
// the count is visible to the pool, and release returns them.
func TestLendTokensSpreadsAndReleases(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c", "d"}, rlTestLogger())
	kp.InvalidateKey(kp.keys[3])

	t1, r1 := kp.LendTokens(2)
	t2, r2 := kp.LendTokens(2)
	if len(t1) != 2 || len(t2) != 2 {
		t.Fatalf("lends returned %d and %d tokens, want 2 and 2", len(t1), len(t2))
	}
	for _, tok := range append(t1, t2...) {
		if tok == "d" {
			t.Fatal("an invalidated key was lent")
		}
	}
	kp.mu.Lock()
	lent := map[string]int{}
	for _, k := range kp.keys {
		lent[k.Token] = k.lent
	}
	kp.mu.Unlock()
	// 4 lends over 3 valid keys, least-lent first: no key carries 3.
	for tok, n := range lent {
		if n > 2 {
			t.Errorf("key %q lent %d times — lending must spread", tok, n)
		}
	}
	r1()
	r2()
	r2() // idempotent
	kp.mu.Lock()
	defer kp.mu.Unlock()
	for _, k := range kp.keys {
		if k.lent != 0 {
			t.Errorf("key %q lent=%d after release, want 0", k.Token, k.lent)
		}
	}
}
