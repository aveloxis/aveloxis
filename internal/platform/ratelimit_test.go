package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewKeyPool(t *testing.T) {
	tokens := []string{"tok1", "tok2", "tok3"}
	kp := NewKeyPool(tokens, testLogger())

	if len(kp.keys) != 3 {
		t.Fatalf("NewKeyPool created %d keys, want 3", len(kp.keys))
	}
	for i, k := range kp.keys {
		if k.Token != tokens[i] {
			t.Errorf("key[%d].Token = %q, want %q", i, k.Token, tokens[i])
		}
		if k.Remaining != 5000 {
			t.Errorf("key[%d].Remaining = %d, want 5000", i, k.Remaining)
		}
		if k.Invalid {
			t.Errorf("key[%d].Invalid = true, want false", i)
		}
	}
}

func TestGetKey_ReturnsAvailableKey(t *testing.T) {
	kp := NewKeyPool([]string{"mytoken"}, testLogger())
	ctx := context.Background()

	key, err := kp.GetKey(ctx)
	if err != nil {
		t.Fatalf("GetKey() error: %v", err)
	}
	if key.Token != "mytoken" {
		t.Errorf("GetKey() returned token %q, want %q", key.Token, "mytoken")
	}
}

func TestGetKey_BlocksWhenExhausted(t *testing.T) {
	kp := NewKeyPool([]string{"tok1"}, testLogger())
	// Set remaining below threshold and reset in the future.
	kp.keys[0].Remaining = 0
	kp.keys[0].ResetAt = time.Now().Add(10 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("GetKey() should have returned error due to context timeout")
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", ctx.Err())
	}
}

func TestInvalidateKey(t *testing.T) {
	kp := NewKeyPool([]string{"badtoken1"}, testLogger())

	key := kp.keys[0]
	kp.InvalidateKey(key)

	if !key.Invalid {
		t.Error("InvalidateKey() did not mark key as invalid")
	}

	// GetKey should fail since the only key is invalid.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("GetKey() should fail when all keys are invalid")
	}
}

func TestGetKey_RoundRobin(t *testing.T) {
	// With 3 keys, GetKey should rotate through them evenly.
	kp := NewKeyPool([]string{"a", "b", "c"}, testLogger())
	ctx := context.Background()

	seen := map[string]int{}
	for range 9 {
		key, err := kp.GetKey(ctx)
		if err != nil {
			t.Fatalf("GetKey() error: %v", err)
		}
		seen[key.Token]++
	}

	// Each key should be returned 3 times.
	for _, tok := range []string{"a", "b", "c"} {
		if seen[tok] != 3 {
			t.Errorf("key %q returned %d times, want 3", tok, seen[tok])
		}
	}
}

func TestGetKey_SkipsExhaustedKeys(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c"}, testLogger())
	ctx := context.Background()

	// Exhaust key "a" (below buffer).
	kp.keys[0].Remaining = 5

	// Should only get "b" and "c".
	seen := map[string]int{}
	for range 6 {
		key, err := kp.GetKey(ctx)
		if err != nil {
			t.Fatalf("GetKey() error: %v", err)
		}
		seen[key.Token]++
	}

	if seen["a"] != 0 {
		t.Errorf("exhausted key 'a' was returned %d times", seen["a"])
	}
	if seen["b"] != 3 {
		t.Errorf("key 'b' returned %d times, want 3", seen["b"])
	}
	if seen["c"] != 3 {
		t.Errorf("key 'c' returned %d times, want 3", seen["c"])
	}
}

func TestGetKey_RefillsAfterReset(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	ctx := context.Background()

	// Exhaust the key but set reset to the past.
	kp.keys[0].Remaining = 0
	kp.keys[0].ResetAt = time.Now().Add(-1 * time.Second)

	key, err := kp.GetKey(ctx)
	if err != nil {
		t.Fatalf("GetKey() error: %v", err)
	}
	if key.Token != "tok" {
		t.Errorf("expected 'tok', got %q", key.Token)
	}
	if key.Remaining != 5000 {
		t.Errorf("Remaining = %d, want 5000 (refilled)", key.Remaining)
	}
}

func TestDefaultBuffer(t *testing.T) {
	if DefaultBuffer < 10 || DefaultBuffer > 20 {
		t.Errorf("DefaultBuffer = %d, want between 10-20", DefaultBuffer)
	}
}

func TestNewKeyPoolWithBuffer(t *testing.T) {
	kp := NewKeyPoolWithBuffer([]string{"tok"}, 5, testLogger())
	if kp.buffer != 5 {
		t.Errorf("buffer = %d, want 5", kp.buffer)
	}
}

func TestAliveCount(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c"}, testLogger())
	if kp.AliveCount() != 3 {
		t.Errorf("AliveCount = %d, want 3", kp.AliveCount())
	}
	kp.InvalidateKey(kp.keys[1])
	if kp.AliveCount() != 2 {
		t.Errorf("AliveCount = %d, want 2 after invalidation", kp.AliveCount())
	}
}

func TestTotalRemaining(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, testLogger())
	kp.keys[0].Remaining = 4000
	kp.keys[1].Remaining = 3000
	if kp.TotalRemaining() != 7000 {
		t.Errorf("TotalRemaining = %d, want 7000", kp.TotalRemaining())
	}
}

func makeResponse(headers map[string]string) *http.Response {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{Header: h}
}

func TestUpdateFromResponse_GitHubHeaders(t *testing.T) {
	kp := NewKeyPool([]string{"ghtoken"}, testLogger())
	key := kp.keys[0]

	resetEpoch := time.Now().Add(30 * time.Minute).Unix()
	resp := makeResponse(map[string]string{
		"X-RateLimit-Remaining": "42",
		"X-RateLimit-Reset":     strconv.FormatInt(resetEpoch, 10),
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 42 {
		t.Errorf("Remaining = %d, want 42", key.Remaining)
	}
	if key.ResetAt.Unix() != resetEpoch {
		t.Errorf("ResetAt = %v, want epoch %d", key.ResetAt, resetEpoch)
	}
}

func TestUpdateFromResponse_SearchResourceIgnored(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]
	key.Remaining = 4500 // Simulate some core usage.

	// A search API response with only 8 remaining should NOT overwrite core state.
	resp := makeResponse(map[string]string{
		"X-RateLimit-Remaining": "8",
		"X-RateLimit-Reset":     strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10),
		"X-RateLimit-Resource":  "search",
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 4500 {
		t.Errorf("search response changed core Remaining to %d, want 4500 (unchanged)", key.Remaining)
	}
}

func TestUpdateFromResponse_GraphQLResourceIgnored(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]
	key.Remaining = 3000

	resp := makeResponse(map[string]string{
		"X-RateLimit-Remaining": "200",
		"X-RateLimit-Reset":     strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10),
		"X-RateLimit-Resource":  "graphql",
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 3000 {
		t.Errorf("graphql response changed core Remaining to %d, want 3000 (unchanged)", key.Remaining)
	}
}

func TestUpdateFromResponse_CoreResourceUpdates(t *testing.T) {
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]

	resetEpoch := time.Now().Add(30 * time.Minute).Unix()
	resp := makeResponse(map[string]string{
		"X-RateLimit-Remaining": "99",
		"X-RateLimit-Reset":     strconv.FormatInt(resetEpoch, 10),
		"X-RateLimit-Resource":  "core",
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 99 {
		t.Errorf("Remaining = %d, want 99", key.Remaining)
	}
}

func TestUpdateFromResponse_NoResourceHeaderUpdates(t *testing.T) {
	// When X-RateLimit-Resource is absent (GitLab, or older GitHub responses),
	// we should still update — treat as "core".
	kp := NewKeyPool([]string{"tok"}, testLogger())
	key := kp.keys[0]

	resp := makeResponse(map[string]string{
		"X-RateLimit-Remaining": "200",
		"X-RateLimit-Reset":     strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10),
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 200 {
		t.Errorf("Remaining = %d, want 200", key.Remaining)
	}
}

func TestUpdateFromResponse_GitLabHeaders(t *testing.T) {
	kp := NewKeyPool([]string{"gltoken"}, testLogger())
	key := kp.keys[0]

	resetEpoch := time.Now().Add(15 * time.Minute).Unix()
	resp := makeResponse(map[string]string{
		"RateLimit-Remaining": "100",
		"RateLimit-Reset":     strconv.FormatInt(resetEpoch, 10),
	})

	kp.UpdateFromResponse(key, resp)

	if key.Remaining != 100 {
		t.Errorf("Remaining = %d, want 100", key.Remaining)
	}
	if key.ResetAt.Unix() != resetEpoch {
		t.Errorf("ResetAt = %v, want epoch %d", key.ResetAt, resetEpoch)
	}
}
