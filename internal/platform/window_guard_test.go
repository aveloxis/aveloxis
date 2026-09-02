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
// an exhausted key to GetGraphQLKey and spending straight through the
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

// A response with no Reset header against a key that has never seen
// one stays fully accepted (the pre-round-10 behavior for trackers
// that omit the header) — the guard must not brick header-poor paths.
func TestBudgetUpdateWithoutResetHeaderStillTracksFirstWindow(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]
	h := http.Header{}
	h.Set("X-RateLimit-Remaining", "77")
	kp.UpdateFromResponse(key, &http.Response{StatusCode: 200, Header: h})
	if key.Remaining != 77 {
		t.Fatalf("Remaining = %d, want 77 (zero tracked window accepts everything)", key.Remaining)
	}
	// With a KNOWN window and no reset header, only decreases land.
	win := time.Now().Add(10 * time.Minute).Unix()
	kp.UpdateFromResponse(key, windowResp("", "50", win))
	h2 := http.Header{}
	h2.Set("X-RateLimit-Remaining", "60")
	kp.UpdateFromResponse(key, &http.Response{StatusCode: 200, Header: h2})
	if key.Remaining != 50 {
		t.Fatalf("header-less same-window increase accepted: Remaining = %d, want 50", key.Remaining)
	}
}
