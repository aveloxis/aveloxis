// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

func windowResp(resource, remaining string, reset int64) *http.Response {
	h := http.Header{}
	if resource != "" {
		h.Set("X-RateLimit-Resource", resource)
	}
	h.Set("X-RateLimit-Remaining", remaining)
	h.Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
	return &http.Response{StatusCode: 200, Header: h}
}

// TestBudgetUpdatesAreWindowGuarded (Copilot round 10 on PR #193):
// concurrent requests complete out of order, and the pre-fix blind
// absolute assignment let an OLDER response with a higher Remaining
// arrive after a newer one and RAISE the tracked budget — re-admitting
// an exhausted key to the graphql checkout and spending straight through the
// background reserve. Within one reset window the true balance only
// decreases; only a NEWER window may raise it.
func TestBudgetUpdatesAreWindowGuarded(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]
	win := time.Now().Add(30 * time.Minute).Unix()

	// First observation: accepted (both buckets).
	kp.UpdateFromResponse(key, windowResp("graphql", "350", win))
	if key.GraphQLRemaining != 350 {
		t.Fatalf("first observation: GraphQLRemaining = %d, want 350", key.GraphQLRemaining)
	}

	// Same window, HIGHER remaining = a stale out-of-order response.
	// The pre-fix code took the 400 and re-inflated the budget.
	kp.UpdateFromResponse(key, windowResp("graphql", "400", win))
	if key.GraphQLRemaining != 350 {
		t.Fatalf("same-window increase accepted: GraphQLRemaining = %d, want 350 (stale response must be ignored)", key.GraphQLRemaining)
	}

	// Same window, lower remaining: monotonic down, accepted.
	kp.UpdateFromResponse(key, windowResp("graphql", "300", win))
	if key.GraphQLRemaining != 300 {
		t.Fatalf("same-window decrease: GraphQLRemaining = %d, want 300", key.GraphQLRemaining)
	}

	// OLDER window entirely: ignored, both values.
	kp.UpdateFromResponse(key, windowResp("graphql", "5000", win-3600))
	if key.GraphQLRemaining != 300 || key.GraphQLResetAt.Unix() != win {
		t.Fatalf("older window applied: remaining=%d reset=%d, want 300/%d", key.GraphQLRemaining, key.GraphQLResetAt.Unix(), win)
	}

	// NEWER window: the refill is real — both values accepted.
	kp.UpdateFromResponse(key, windowResp("graphql", "5000", win+3600))
	if key.GraphQLRemaining != 5000 || key.GraphQLResetAt.Unix() != win+3600 {
		t.Fatalf("newer window refused: remaining=%d reset=%d, want 5000/%d", key.GraphQLRemaining, key.GraphQLResetAt.Unix(), win+3600)
	}

	// The core bucket rides the same helper (SR-17): same-window
	// increase must be ignored there too.
	kp.UpdateFromResponse(key, windowResp("", "100", win))
	kp.UpdateFromResponse(key, windowResp("", "200", win))
	if key.Remaining != 100 {
		t.Fatalf("core same-window increase accepted: Remaining = %d, want 100", key.Remaining)
	}
}

// A response with no Reset header cannot identify a window, so it may
// only LOWER a tracked balance — including on a key with no tracked
// window yet (a fresh key, or one refillLocked just refilled and
// zeroed). Through v0.29.8 that zero-window case accepted every value, so
// concurrent header-less responses arriving as 50 then a stale 60 raised
// the balance again: the out-of-order bug this guard exists to prevent
// (Copilot review 5189042842 on PR #203). Nothing needs a header-less
// raise: keys start full, and refill comes from refillLocked once a
// known or probe-stamped window passes. Header-poor paths still track —
// every decrease lands — and the first response that DOES carry a reset
// establishes the window and is accepted whole.
func TestBudgetUpdateWithoutResetHeaderOnlyDecreases(t *testing.T) {
	noReset := func(remaining string) *http.Response {
		h := http.Header{}
		h.Set("X-RateLimit-Remaining", remaining)
		return &http.Response{StatusCode: 200, Header: h}
	}
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]

	// Fresh key, no window: a decrease lands.
	kp.UpdateFromResponse(key, noReset("50"))
	if key.Remaining != 50 {
		t.Fatalf("header-less decrease on a fresh key: Remaining = %d, want 50", key.Remaining)
	}
	// Still no window: a stale higher value must not raise it.
	kp.UpdateFromResponse(key, noReset("60"))
	if key.Remaining != 50 {
		t.Fatalf("header-less increase with no tracked window accepted: Remaining = %d, want 50", key.Remaining)
	}
	if !key.ResetAt.IsZero() {
		t.Fatalf("a header-less response must not invent a window: ResetAt = %v", key.ResetAt)
	}

	// The first response WITH a reset identifies the window and is taken
	// whole, even when it is higher than the header-less tracked value.
	win := time.Now().Add(10 * time.Minute).Unix()
	kp.UpdateFromResponse(key, windowResp("", "4000", win))
	if key.Remaining != 4000 || key.ResetAt.Unix() != win {
		t.Fatalf("first reset-bearing response: remaining=%d reset=%d, want 4000/%d", key.Remaining, key.ResetAt.Unix(), win)
	}
	// With a KNOWN window and no reset header, only decreases land.
	kp.UpdateFromResponse(key, noReset("3990"))
	kp.UpdateFromResponse(key, noReset("3995"))
	if key.Remaining != 3990 {
		t.Fatalf("header-less same-window increase accepted: Remaining = %d, want 3990", key.Remaining)
	}

	// After refillLocked refills the key and zeroes its window, the same
	// out-of-order pair must not re-inflate it either.
	kp.mu.Lock()
	key.ResetAt = time.Now().Add(-time.Second)
	kp.refillLocked(time.Now(), ResourceCore)
	kp.mu.Unlock()
	if key.Remaining != 5000 || !key.ResetAt.IsZero() {
		t.Fatalf("refill precondition: remaining=%d reset=%v, want 5000/zero", key.Remaining, key.ResetAt)
	}
	kp.UpdateFromResponse(key, noReset("4900"))
	kp.UpdateFromResponse(key, noReset("4950"))
	if key.Remaining != 4900 {
		t.Fatalf("header-less increase after a refill accepted: Remaining = %d, want 4900", key.Remaining)
	}

	// The graphql bucket rides the same helper (SR-17).
	gq := func(remaining string) *http.Response {
		r := noReset(remaining)
		r.Header.Set("X-RateLimit-Resource", "graphql")
		return r
	}
	kp.UpdateFromResponse(key, gq("300"))
	kp.UpdateFromResponse(key, gq("400"))
	if key.GraphQLRemaining != 300 {
		t.Fatalf("graphql header-less increase with no tracked window accepted: GraphQLRemaining = %d, want 300", key.GraphQLRemaining)
	}
}
