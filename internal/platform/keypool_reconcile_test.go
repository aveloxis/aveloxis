// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

// v0.30.0 Phase C (live key reload): Reconcile changes a running pool in
// place — every holder of the *KeyPool (clients, the breadth worker, the
// commit resolver, scorecard's lender) sees the change without rewiring. A
// removed key leaves selection at once and drains: its in-flight leases and
// lends finish on the *APIKey pointer, and it is pruned when both reach zero.

func okResponse() *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}
}

func poolTokens(kp *KeyPool) []string {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := make([]string, 0, len(kp.keys))
	for _, k := range kp.keys {
		out = append(out, k.Token)
	}
	return out
}

func drainingTokens(kp *KeyPool) []string {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := make([]string, 0, len(kp.draining))
	for _, k := range kp.draining {
		out = append(out, k.Token)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestKeyPoolReconcileAddsRemovesAndRestores(t *testing.T) {
	kp := NewKeyPool([]string{"tok-a", "tok-b"}, testLogger())

	// Add c, keep b, drop a; duplicates in the desired set count once.
	res := kp.Reconcile([]string{"tok-b", "tok-c", "tok-c"})
	if res != (ReconcileResult{Added: 1, Removed: 1}) {
		t.Fatalf("Reconcile = %+v, want Added 1, Removed 1", res)
	}
	if got := poolTokens(kp); !sameStrings(got, []string{"tok-b", "tok-c"}) {
		t.Fatalf("active keys = %v, want [tok-b tok-c]", got)
	}
	// Nothing held a lease on a, so it is pruned at once rather than parked.
	if got := drainingTokens(kp); len(got) != 0 {
		t.Fatalf("draining = %v, want none (no lease was held)", got)
	}

	// A key removed while leased drains; re-adding it restores the SAME
	// key with its state (budget, strikes) rather than a fresh one.
	key, release, err := kp.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	key.Remaining = 1234
	held := key.Token
	var keep []string
	for _, tok := range poolTokens(kp) {
		if tok != held {
			keep = append(keep, tok)
		}
	}
	if res := kp.Reconcile(keep); res.Removed != 1 {
		t.Fatalf("Reconcile removing the leased key = %+v, want Removed 1", res)
	}
	if got := drainingTokens(kp); !sameStrings(got, []string{held}) {
		t.Fatalf("draining = %v, want [%s] while its lease is held", got, held)
	}
	if res := kp.Reconcile(append(keep, held)); res.Restored != 1 || res.Added != 0 {
		t.Fatalf("re-adding a draining key = %+v, want Restored 1, Added 0", res)
	}
	kp.mu.Lock()
	var restored *APIKey
	for _, k := range kp.keys {
		if k.Token == held {
			restored = k
		}
	}
	kp.mu.Unlock()
	if restored != key || restored.Remaining != 1234 {
		t.Fatalf("restored key is not the original with its state (same=%v remaining=%d)", restored == key, restored.Remaining)
	}
	release()
}

func TestKeyPoolRemovedKeyIsNeverSelectedAndPrunedAfterRelease(t *testing.T) {
	kp := NewKeyPool([]string{"tok-a", "tok-b"}, testLogger())
	key, release, err := kp.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	other := "tok-a"
	if key.Token == "tok-a" {
		other = "tok-b"
	}
	kp.Reconcile([]string{other})

	for i := 0; i < 20; i++ {
		k, err := checkout(kp, context.Background(), ResourceCore)
		if err != nil {
			t.Fatal(err)
		}
		if k.Token != other {
			t.Fatalf("checkout %d returned removed key %s", i, k.Token)
		}
	}
	if toks, rel := kp.LendTokens(0); len(toks) != 1 || toks[0] != other {
		t.Fatalf("LendTokens = %v, want only %s", toks, other)
	} else {
		rel()
	}
	if n := kp.AliveCount(); n != 1 {
		t.Fatalf("AliveCount = %d, want 1 (a draining key is not alive)", n)
	}

	// The lease on the removed key still releases cleanly, and the release
	// prunes it from the draining list.
	kp.UpdateFromResponse(key, okResponse())
	release()
	if got := drainingTokens(kp); len(got) != 0 {
		t.Fatalf("draining = %v after the last lease released, want none", got)
	}
	kp.mu.Lock()
	inflight := kp.inflight
	kp.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("pool inflight = %d after release, want 0", inflight)
	}
}

func TestKeyPoolRemovedLentKeyPrunedAfterLendRelease(t *testing.T) {
	kp := NewKeyPool([]string{"tok-a"}, testLogger())
	toks, release := kp.LendTokens(0)
	if len(toks) != 1 {
		t.Fatalf("LendTokens = %v", toks)
	}
	kp.Reconcile(nil)
	if got := drainingTokens(kp); !sameStrings(got, []string{"tok-a"}) {
		t.Fatalf("draining = %v, want [tok-a] while lent", got)
	}
	release()
	if got := drainingTokens(kp); len(got) != 0 {
		t.Fatalf("draining = %v after the lend released, want none", got)
	}
}

func TestKeyPoolReconcileToEmptyFailsFastWithErrNoKeys(t *testing.T) {
	kp := NewKeyPool([]string{"tok-a"}, testLogger())
	kp.Reconcile(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := kp.Acquire(ctx, ResourceCore)
	if !errors.Is(err, ErrNoKeys) {
		t.Fatalf("Acquire on a reconciled-empty pool = %v, want ErrNoKeys", err)
	}
	if errors.Is(err, ErrAllKeysInvalidated) {
		t.Fatal("an emptied pool must not read as every key invalidated")
	}
	if !kp.IsEmpty() || kp.Len() != 0 {
		t.Fatalf("IsEmpty = %v, Len = %d after removing every key", kp.IsEmpty(), kp.Len())
	}
}

// A caller parked waiting for budget wakes as soon as a key is added.
func TestKeyPoolReconcileWakesWaiter(t *testing.T) {
	kp := NewKeyPool([]string{"tok-dry"}, testLogger())
	kp.mu.Lock()
	kp.keys[0].Remaining = 0
	kp.keys[0].ResetAt = time.Now().Add(time.Hour)
	kp.mu.Unlock()

	got := make(chan *APIKey, 1)
	go func() {
		k, err := checkout(kp, context.Background(), ResourceCore)
		if err != nil {
			t.Error(err)
		}
		got <- k
	}()
	// Let the waiter park, then add a fresh key.
	time.Sleep(100 * time.Millisecond)
	if len(got) > 0 {
		t.Fatal("waiter returned before a usable key existed")
	}
	kp.Reconcile([]string{"tok-dry", "tok-fresh"})
	select {
	case k := <-got:
		if k.Token != "tok-fresh" {
			t.Fatalf("woken waiter got %s, want tok-fresh", k.Token)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter was not woken by the added key")
	}
}

func TestKeyPoolReconcileKeepsRoundRobinInRange(t *testing.T) {
	kp := NewKeyPool([]string{"a1", "a2", "a3", "a4", "a5"}, testLogger())
	for i := 0; i < 9; i++ {
		if _, err := checkout(kp, context.Background(), ResourceCore); err != nil {
			t.Fatal(err)
		}
		if _, err := checkout(kp, context.Background(), ResourceGraphQL); err != nil {
			t.Fatal(err)
		}
	}
	kp.Reconcile([]string{"a2"})
	kp.mu.Lock()
	rr, rrg, n := kp.rrIndex, kp.rrIndexGQL, len(kp.keys)
	kp.mu.Unlock()
	if rr < 0 || rr >= n || rrg < 0 || rrg >= n {
		t.Fatalf("round-robin cursors (%d, %d) out of range for %d keys", rr, rrg, n)
	}
	if _, err := checkout(kp, context.Background(), ResourceGraphQL); err != nil {
		t.Fatal(err)
	}
}

// Snapshot reports draining keys (so the operator sees a removed key still
// finishing) and never the token.
func TestKeyPoolSnapshotIncludesDrainingAndKeyID(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_aaaaaaaaaaaaaaaaaaaa", "ghp_bbbbbbbbbbbbbbbbbbbb"}, testLogger())
	key, release, err := kp.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var keep string
	for _, tok := range poolTokens(kp) {
		if tok != key.Token {
			keep = tok
		}
	}
	kp.Reconcile([]string{keep})
	snap, _ := kp.Snapshot()
	states := map[string]KeyState{}
	for _, s := range snap {
		states[s.KeyID] = s.State
	}
	if states[KeyID(keep)] != KeyActive || states[KeyID(key.Token)] != KeyDraining || len(snap) != 2 {
		t.Fatalf("snapshot states = %v, want %s active and %s draining", states, KeyID(keep), KeyID(key.Token))
	}
}

// Concurrent Acquire/release, lends, snapshots and reconciles: run under
// -race; the pool must end consistent (no negative counters, nothing
// draining once every lease is back).
func TestKeyPoolReconcileConcurrent(t *testing.T) {
	sets := [][]string{{"k1", "k2", "k3"}, {"k2", "k4"}, {"k1", "k4", "k5"}, {"k5"}}
	kp := NewKeyPool(sets[0], testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res := ResourceCore
				if g%2 == 1 {
					res = ResourceGraphQL
				}
				k, release, err := kp.Acquire(ctx, res)
				if err != nil {
					if errors.Is(err, ErrNoKeys) || ctx.Err() != nil {
						continue
					}
					t.Error(err)
					return
				}
				kp.UpdateFromResponse(k, okResponse())
				release()
				if g == 0 {
					_, rel := kp.LendTokens(1)
					kp.Snapshot()
					rel()
				}
			}
		}(g)
	}
	for i := 0; i < 200; i++ {
		kp.Reconcile(sets[i%len(sets)])
	}
	close(stop)
	wg.Wait()
	kp.Reconcile(sets[len(sets)-1])
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.inflight != 0 || len(kp.draining) != 0 {
		t.Fatalf("after quiesce: inflight=%d draining=%d, want 0 and 0", kp.inflight, len(kp.draining))
	}
	for _, k := range kp.keys {
		if k.inflight != 0 || k.lent != 0 {
			t.Fatalf("key %s inflight=%d lent=%d, want 0", k.Token, k.inflight, k.lent)
		}
	}
}

func TestKeyIDAndMaskToken(t *testing.T) {
	a, b := KeyID("glpat-abcdefghijklmnopqrst"), KeyID("glpat-abcdefghijklmnopqrsu")
	if len(a) != 16 || a == b {
		t.Fatalf("KeyID = %q / %q, want 16 hex chars that differ per token", a, b)
	}
	if KeyID("glpat-abcdefghijklmnopqrst") != a {
		t.Fatal("KeyID must be deterministic")
	}
	for tok, want := range map[string]string{
		"":                          "(hidden)",
		"short":                     "(hidden)",
		"elevenchars":               "(hidden)",
		"twelve-chars":              "twel...hars",
		"ghp_1234567890abcdefghijk": "ghp_...hijk",
	} {
		if got := MaskToken(tok); got != want {
			t.Errorf("MaskToken(%q) = %q, want %q", tok, got, want)
		}
	}
}
