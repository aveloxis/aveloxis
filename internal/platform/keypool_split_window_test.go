// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// exhausted403 is GitHub's primary-exhaustion shape: 403, no Retry-After,
// X-RateLimit-Remaining: 0 and the reset of the window that refused it.
func exhausted403(reset int64) *http.Response {
	h := http.Header{}
	h.Set("X-RateLimit-Resource", "core")
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
	return &http.Response{StatusCode: http.StatusForbidden, Header: h}
}

// TestZeroRemaining403WithEarlierResetStillBenchesTheKey replays the
// chaoss.tv 2026-09-17 sequence at the pool level. One token, labelled
// `core` on every response, was seen with two different windows: a
// later-reset response with a high balance (the breadth worker's
// /users/{login}/events traffic carried remaining ~4,305 and a reset about
// an hour out) and then 403s with Remaining: 0 and an EARLIER reset
// (ghp_YbNl, reset 11:10:32). windowedBudgetUpdate filed the 403 as an
// "older window" and dropped it, so the pool kept the key at ~4,305 — the
// most-remaining key, which selection hands out first. 4,388 exhausted
// 403s on that one key followed in seven minutes, and every 5-minute pool
// summary still read core_remaining_min=4305.
//
// Whatever produced the second reset, a 403 carrying Remaining: 0 is
// GitHub refusing THIS key right now; the pool must not hand the key out
// again before that reset passes.
func TestZeroRemaining403WithEarlierResetStillBenchesTheKey(t *testing.T) {
	kp := NewKeyPool([]string{"poisoned", "healthy"}, testLogger())
	poisoned, healthy := kp.keys[0], kp.keys[1]
	now := time.Now()
	later := now.Add(55 * time.Minute).Unix()
	earlier := now.Add(5 * time.Minute).Unix()

	kp.UpdateFromResponse(poisoned, windowResp("core", "4310", later))
	kp.UpdateFromResponse(healthy, windowResp("core", "4305", later))
	kp.UpdateFromResponse(poisoned, exhausted403(earlier))

	for i := range 10 {
		key, release, err := kp.Acquire(context.Background(), ResourceCore)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		release()
		if key == poisoned {
			t.Fatalf("acquire %d returned the key GitHub just refused with Remaining: 0 (tracked Remaining=%d ResetAt=%v); the zero was discarded as an older window",
				i, poisoned.Remaining, poisoned.ResetAt)
		}
	}
}

// TestGetRotatesAwayFromZeroRemaining403 is the same incident end to end
// through the REST client: every retry of one URL landed on the refused
// key ~70 ms apart (the 403 arm returns respRetry with no wait, trusting
// the pool to route elsewhere), so each URL spent its whole 10-attempt
// budget on one key and failed while 53 others were available.
func TestGetRotatesAwayFromZeroRemaining403(t *testing.T) {
	now := time.Now()
	later := strconv.FormatInt(now.Add(55*time.Minute).Unix(), 10)
	earlier := strconv.FormatInt(now.Add(5*time.Minute).Unix(), 10)

	var mu sync.Mutex
	hits := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		hits[auth]++
		mu.Unlock()
		w.Header().Set("X-RateLimit-Resource", "core")
		if auth == "token poisoned" {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", earlier)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded for user ID 1."}`))
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "4300")
		w.Header().Set("X-RateLimit-Reset", later)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"poisoned", "healthy"}, testLogger())
	// The poisoned key's tracked window came from a later-reset response.
	keys.UpdateFromResponse(keys.keys[0], windowResp("core", "4310", now.Add(55*time.Minute).Unix()))
	keys.UpdateFromResponse(keys.keys[1], windowResp("core", "4305", now.Add(55*time.Minute).Unix()))

	client := NewHTTPClient(server.URL, keys, testLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/users/HanMuYa")
	if err != nil {
		t.Fatalf("Get failed with a healthy key available: %v (hits=%v)", err, hits)
	}
	resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if hits["token poisoned"] > 1 {
		t.Fatalf("the refused key was retried %d times; one 403 with Remaining: 0 must bench it (hits=%v)", hits["token poisoned"], hits)
	}
}

// refusingServer answers every token containing "dead" with a primary
// refusal (status, Remaining: 0, reset +5m, no Retry-After) and every other
// token with a 200. It counts requests per Authorization value.
func refusingServer(t *testing.T, status int, resource string, okBody string) (*httptest.Server, func() map[string]int) {
	t.Helper()
	reset := strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10)
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		hits[auth]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Resource", resource)
		w.Header().Set("X-RateLimit-Reset", reset)
		if strings.Contains(auth, "dead") {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(status)
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "4000")
		_, _ = w.Write([]byte(okBody))
	}))
	t.Cleanup(srv.Close)
	return srv, func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]int{}
		for k, v := range hits {
			out[k] = v
		}
		return out
	}
}

// deadThenLive is 11 keys that will be refused and one live key LAST, so
// the live key is only reachable past the maxRetries=10 budget, and only
// if rotations hand their attempts back.
func deadThenLive() []string {
	return []string{
		"dead00", "dead01", "dead02", "dead03", "dead04", "dead05",
		"dead06", "dead07", "dead08", "dead09", "dead10", "live-key",
	}
}

// TestRESTRefusalRotationsDoNotSpendRetryBudget: with more refused keys
// than retries, Get must still reach the live key ("stop and go get
// another key"), and each refused key must be asked exactly once — it is
// benched for everyone after its first refusal. Both refusal spellings
// GitHub documents (403 and 429) rotate.
func TestRESTRefusalRotationsDoNotSpendRetryBudget(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv, hits := refusingServer(t, status, "core", `{}`)
			keys := NewKeyPool(deadThenLive(), testLogger())
			// Fresh keys tie on budget; give the live key the least so
			// selection reaches it last.
			keys.UpdateFromResponse(keys.keys[11], windowResp("core", "4000", time.Now().Add(55*time.Minute).Unix()))
			c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitHub)
			resp, err := c.Get(context.Background(), "/users/someone")
			if err != nil {
				t.Fatalf("Get must rotate through every refused key to the live one: %v (hits=%v)", err, hits())
			}
			resp.Body.Close()
			for tok, n := range hits() {
				if n != 1 {
					t.Errorf("%s asked %d times, want 1 — a refused key must be benched after its first refusal", tok, n)
				}
			}
			if got := hits()["token live-key"]; got != 1 {
				t.Fatalf("live key asked %d times, want 1 (hits=%v)", got, hits())
			}
		})
	}
}

// TestPrimaryRefusal429IsNotASecondaryRest: a 429 with Remaining: 0 and no
// Retry-After is primary exhaustion, benched until its reset — not the
// 60-second secondary rest parseRetryAfter would default to, which would
// hand the refused key back a minute later.
func TestPrimaryRefusal429IsNotASecondaryRest(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, testLogger())
	key := kp.keys[0]
	resp := exhausted403(time.Now().Add(40 * time.Minute).Unix())
	resp.StatusCode = http.StatusTooManyRequests
	kp.UpdateFromResponse(key, resp)
	if !key.secondaryUntil.IsZero() {
		t.Errorf("secondaryUntil = %v, want zero — a primary refusal is not a secondary limit", key.secondaryUntil)
	}
	if time.Until(key.refusedUntil) < 39*time.Minute {
		t.Errorf("refusedUntil = %v, want the refusal's own reset (~40m out)", key.refusedUntil)
	}
}

// TestRefusalIsNotLiftedByLaterWindowHeaders: after the refusal, a success
// carrying a LATER reset and a high balance (the other series) is accepted
// by the window guard — and must still not return the key before the
// refusal's reset.
func TestRefusalIsNotLiftedByLaterWindowHeaders(t *testing.T) {
	kp := NewKeyPool([]string{"refused", "other"}, testLogger())
	refused := kp.keys[0]
	kp.UpdateFromResponse(refused, exhausted403(time.Now().Add(5*time.Minute).Unix()))
	kp.UpdateFromResponse(refused, windowResp("core", "4999", time.Now().Add(55*time.Minute).Unix()))
	kp.UpdateFromResponse(kp.keys[1], windowResp("core", "100", time.Now().Add(55*time.Minute).Unix()))
	key, release, err := kp.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if key == refused {
		t.Fatalf("a later-window header lifted the refusal (Remaining=%d)", refused.Remaining)
	}
}

// TestRefusalWithoutUsableResetBenchesForTheProbeWindow: an absent,
// unparseable or already-past reset gives no end instant, so the refusal
// lasts the probe window — neither zero (the hot loop again) nor forever.
func TestRefusalWithoutUsableResetBenchesForTheProbeWindow(t *testing.T) {
	for name, reset := range map[string]string{
		"absent":      "",
		"unparseable": "soon",
		"past":        strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10),
	} {
		t.Run(name, func(t *testing.T) {
			kp := NewKeyPool([]string{"k"}, testLogger())
			key := kp.keys[0]
			h := http.Header{}
			h.Set("X-RateLimit-Remaining", "0")
			if reset != "" {
				h.Set("X-RateLimit-Reset", reset)
			}
			before := time.Now()
			kp.UpdateFromResponse(key, &http.Response{StatusCode: http.StatusForbidden, Header: h})
			lo, hi := before.Add(graphQLDepletedProbe), time.Now().Add(graphQLDepletedProbe)
			if key.refusedUntil.Before(lo) || key.refusedUntil.After(hi) {
				t.Fatalf("refusedUntil = %v, want the probe window [%v, %v]", key.refusedUntil, lo, hi)
			}
		})
	}
}

// TestAllKeysRefusedAcquireWaitsForTheEarliestRefusal: when every key is
// refused, Acquire must not hand one out (the hot loop) and must wake at
// the EARLIEST refusal end, not at a later tracked window.
func TestAllKeysRefusedAcquireWaitsForTheEarliestRefusal(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, testLogger())
	now := time.Now()
	early, late := now.Add(3*time.Minute), now.Add(7*time.Minute)
	// Both keys carry a high balance in a LATER tracked window (the
	// incident shape), so only the refusals — not a zeroed budget — can
	// supply the wake.
	for _, k := range kp.keys {
		kp.UpdateFromResponse(k, windowResp("core", "4999", now.Add(55*time.Minute).Unix()))
	}
	kp.UpdateFromResponse(kp.keys[0], exhausted403(late.Unix()))
	kp.UpdateFromResponse(kp.keys[1], exhausted403(early.Unix()))

	kp.mu.Lock()
	key, verdict, wake, _ := kp.selectLocked(time.Now(), ResourceCore, false)
	kp.mu.Unlock()
	if key != nil || verdict != verdictBudgetBlocked {
		t.Fatalf("verdict = %v key = %v, want budget-blocked with no key", verdict, key)
	}
	if wake.Unix() != early.Unix() {
		t.Fatalf("wake = %v, want the earliest refusal end %v", wake, early)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if k, release, err := kp.Acquire(ctx, ResourceCore); err == nil {
		release()
		t.Fatalf("Acquire handed out refused key %q", k.Token)
	}

	// Once the refusal ends the key rejoins without any header.
	kp.mu.Lock()
	kp.keys[1].refusedUntil = time.Now().Add(-time.Second)
	kp.mu.Unlock()
	k, release, err := kp.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if k != kp.keys[1] {
		t.Fatalf("Acquire = %q, want the key whose refusal ended", k.Token)
	}
}

// TestUntrackedResourceRefusalIsNotBenched: a search refusal benches the
// key for SEARCH only (TestSearchRefusalRotatesToAnotherKey); it must not
// bench the key's core or graphql budget, which would stall REST and
// GraphQL collection on a 60-second search limit.
func TestUntrackedResourceRefusalIsNotBenched(t *testing.T) {
	srv, _ := refusingServer(t, http.StatusForbidden, "search", `{}`)
	keys := NewKeyPool([]string{"dead-only"}, testLogger())
	c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitHub)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = c.Get(ctx, "/search/users?q=x")
	snap, _ := keys.Snapshot()
	if !snap[0].CoreRefusedUntil.IsZero() || !snap[0].GraphQLRefusedUntil.IsZero() {
		t.Fatalf("a search refusal benched a tracked bucket: %+v", snap[0])
	}
}

// TestRefusedKeyBudgetIsNotCountedInTheReserve: the background reserve
// compares the usable total against its line; a refused key's tracked
// balance is not budget and must not hold the total above the line.
func TestRefusedKeyBudgetIsNotCountedInTheReserve(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, testLogger())
	reset := time.Now().Add(30 * time.Minute).Unix()
	kp.UpdateFromResponse(kp.keys[0], windowResp("graphql", "5000", reset))
	kp.UpdateFromResponse(kp.keys[1], windowResp("graphql", "5000", reset))
	// The refusal's reset is EARLIER than the tracked window, so the window
	// guard keeps the refused key's 5000 — the balance under test.
	h := exhausted403(time.Now().Add(5 * time.Minute).Unix())
	h.Header.Set("X-RateLimit-Resource", "graphql")
	kp.UpdateFromResponse(kp.keys[0], h)
	kp.mu.Lock()
	_, spendable, total, _ := kp.reserveStateLocked(time.Now(), ResourceGraphQL)
	kp.mu.Unlock()
	if spendable != 1 || total != 5000 {
		t.Fatalf("spendable=%d total=%d, want 1 and 5000 — the refused key's balance was counted", spendable, total)
	}
}

// TestGraphQLRefusalRotationsDoNotSpendTransportBudget: the 403 +
// Remaining: 0 arm of the GraphQL client rotates like the in-body arm
// (round 24), so 11 refused keys cannot exhaust the budget before the
// live key is tried.
func TestGraphQLRefusalRotationsDoNotSpendTransportBudget(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	srv, hits := refusingServer(t, http.StatusForbidden, "graphql", `{"data":{"hello":"world"}}`)
	keys := NewKeyPool(deadThenLive(), testLogger())
	keys.UpdateFromResponse(keys.keys[11], windowResp("graphql", "4000", time.Now().Add(55*time.Minute).Unix()))
	c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitHub)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL must rotate past 11 refused keys: %v (hits=%v)", err, hits())
	}
	for tok, n := range hits() {
		if n != 1 {
			t.Errorf("%s asked %d times, want 1", tok, n)
		}
	}
}

// TestSnapshotReportsRefusalsPerResponse: the summary's refusal state is
// visible, and the lifetime count is one per refused response (two
// concurrent leases refused with the same reset are two refused requests;
// the GraphQL belt's re-mark of one response is not — see
// TestGraphQLRefusalBenchesUntilTheResponseReset).
func TestSnapshotReportsRefusalsPerResponse(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, testLogger())
	reset := time.Now().Add(10 * time.Minute).Unix()
	kp.UpdateFromResponse(kp.keys[0], exhausted403(reset))
	kp.UpdateFromResponse(kp.keys[0], exhausted403(reset))
	snap, _ := kp.Snapshot()
	if snap[0].Refusals != 2 || snap[0].CoreRefusedUntil.Unix() != reset {
		t.Fatalf("snapshot = %+v, want Refusals=2 and CoreRefusedUntil=%d", snap[0], reset)
	}
}

// TestLendTokensWithholdsRefusedKeys: scorecard subprocesses are collectors
// too (SR-20). A key GitHub refused would be handed to scorecard, which
// waits out a refused token inside its subprocess slot. LendTokens already
// withholds keys Acquire refuses (quarantine, secondary rest); a standing
// refusal on either bucket is the same kind of refusal.
func TestLendTokensWithholdsRefusedKeys(t *testing.T) {
	kp := NewKeyPool([]string{"core-refused", "graphql-refused", "healthy"}, testLogger())
	reset := time.Now().Add(10 * time.Minute).Unix()
	kp.UpdateFromResponse(kp.keys[0], exhausted403(reset))
	gql := exhausted403(reset)
	gql.Header.Set("X-RateLimit-Resource", "graphql")
	kp.UpdateFromResponse(kp.keys[1], gql)
	tokens, release := kp.LendTokens(0)
	defer release()
	if len(tokens) != 1 || tokens[0] != "healthy" {
		t.Fatalf("lent %v, want only the unrefused key", tokens)
	}
}
