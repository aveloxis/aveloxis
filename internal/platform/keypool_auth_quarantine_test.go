// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

// quietLogger discards log output so the noisy WARN/ERROR lines these tests
// deliberately trigger don't clutter `go test` output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRecordAuthFailure_SingleFailureDoesNotDisable is the core regression test
// for the 2026-06-17 outage: a single 401 must NOT take a key out of rotation.
func TestRecordAuthFailure_SingleFailureDoesNotDisable(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_singlekey"}, quietLogger())
	key := kp.keys[0]

	if quarantined := kp.RecordAuthFailure(key); quarantined {
		t.Fatal("a single 401 quarantined the key; it must be treated as transient")
	}

	// The key must still be handed out by GetKey.
	got, err := kp.GetKey(context.Background())
	if err != nil {
		t.Fatalf("GetKey returned error after one 401: %v", err)
	}
	if got != key {
		t.Fatal("GetKey did not return the still-valid key after one transient 401")
	}
	if key.Invalid {
		t.Fatal("key was permanently invalidated by a single 401")
	}
}

// TestRecordAuthFailure_QuarantinesAfterThreshold verifies the key is quarantined
// only after maxAuthStrikes consecutive failures, and recovers automatically.
func TestRecordAuthFailure_QuarantinesAfterThreshold(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_thresholdkey"}, quietLogger())
	key := kp.keys[0]

	for i := 1; i < maxAuthStrikes; i++ {
		if kp.RecordAuthFailure(key) {
			t.Fatalf("quarantined after %d strikes; threshold is %d", i, maxAuthStrikes)
		}
	}
	if !kp.RecordAuthFailure(key) {
		t.Fatalf("not quarantined after %d consecutive strikes", maxAuthStrikes)
	}

	if key.Invalid {
		t.Fatal("quarantine must not set the permanent Invalid flag")
	}
	if key.quarantineUntil.IsZero() || !key.quarantineUntil.After(time.Now()) {
		t.Fatal("quarantineUntil not set to a future time")
	}

	// Simulate cooldown elapsing: the key becomes usable again with no restart.
	key.quarantineUntil = time.Now().Add(-time.Second)
	got, err := kp.GetKey(context.Background())
	if err != nil {
		t.Fatalf("GetKey errored after quarantine cooldown elapsed: %v", err)
	}
	if got != key {
		t.Fatal("key did not auto-recover after its quarantine cooldown elapsed")
	}
}

// TestRecordAuthSuccess_ResetsStrikes verifies an intervening success prevents a
// slow drip of transient 401s from ever reaching the quarantine threshold —
// exactly the pattern (good keys, occasional 401s over 15 hours) seen on June 17.
func TestRecordAuthSuccess_ResetsStrikes(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_resetkey"}, quietLogger())
	key := kp.keys[0]

	for cycle := 0; cycle < 100; cycle++ {
		// Fail almost to the threshold...
		for i := 1; i < maxAuthStrikes; i++ {
			if kp.RecordAuthFailure(key) {
				t.Fatalf("cycle %d: quarantined before a success reset the strikes", cycle)
			}
		}
		// ...then a success resets the counter.
		kp.RecordAuthSuccess(key)
	}
	if key.quarantineCount != 0 {
		t.Fatalf("key was quarantined %d times despite interleaved successes", key.quarantineCount)
	}
}

// TestUpdateFromResponse_ResetsStrikesOn2xx pins that a 2xx response clears the
// strike counter (the production path RecordAuthSuccess depends on).
func TestUpdateFromResponse_ResetsStrikesOn2xx(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_2xxkey"}, quietLogger())
	key := kp.keys[0]
	key.authStrikes = maxAuthStrikes - 1

	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}
	kp.UpdateFromResponse(key, resp)

	if key.authStrikes != 0 {
		t.Fatalf("authStrikes = %d after a 200 response, want 0", key.authStrikes)
	}
}

// TestGetKey_WaitsForQuarantineInsteadOfFatal verifies that when every key is
// quarantined (not permanently invalid), GetKey waits for the soonest recovery
// rather than returning ErrAllKeysInvalidated — so the scheduler does not crash.
func TestGetKey_WaitsForQuarantineInsteadOfFatal(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_a", "ghp_b"}, quietLogger())
	// Quarantine every key well into the future.
	for _, k := range kp.keys {
		k.quarantineUntil = time.Now().Add(time.Hour)
		k.quarantineCount = 1
	}

	// GetKey should block (waiting for the cooldown), NOT return immediately
	// with ErrAllKeysInvalidated. A short-deadline context proves it blocks.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := kp.GetKey(ctx)
	if err == nil {
		t.Fatal("expected GetKey to block until ctx deadline, got a key")
	}
	if err == ErrAllKeysInvalidated {
		t.Fatal("GetKey returned ErrAllKeysInvalidated for quarantined keys; it must wait instead")
	}
	if ctx.Err() == nil {
		t.Fatalf("expected context deadline error, got: %v", err)
	}
}

// TestRecordAuthFailure_ExponentialCooldownIsBounded verifies repeated
// quarantines grow the cooldown but never exceed authQuarantineMax.
func TestRecordAuthFailure_ExponentialCooldownIsBounded(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_revoked"}, quietLogger())
	key := kp.keys[0]

	var lastCooldown time.Duration
	for round := 0; round < 12; round++ {
		for i := 0; i < maxAuthStrikes; i++ {
			kp.RecordAuthFailure(key)
		}
		cooldown := time.Until(key.quarantineUntil)
		if cooldown > authQuarantineMax+time.Second {
			t.Fatalf("round %d cooldown %v exceeds cap %v", round, cooldown, authQuarantineMax)
		}
		lastCooldown = cooldown
		// reset for the next round's strike accumulation
		key.authStrikes = 0
	}
	// After many rounds the cooldown should have saturated near the cap.
	if lastCooldown < authQuarantineMax-time.Minute {
		t.Fatalf("cooldown %v did not saturate near cap %v after many quarantines", lastCooldown, authQuarantineMax)
	}
	if key.Invalid {
		t.Fatal("a repeatedly-failing key must still never be permanently invalidated by the quarantine path")
	}
}
