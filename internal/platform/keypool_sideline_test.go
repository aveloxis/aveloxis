// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

// respond200 feeds one successful response through UpdateFromResponse.
func respond200(kp *KeyPool, key *APIKey) {
	kp.UpdateFromResponse(key, &http.Response{StatusCode: http.StatusOK, Header: http.Header{}})
}

// quarantineOnce drives key through one whole quarantine: maxAuthStrikes
// consecutive 401s.
func quarantineOnce(t *testing.T, kp *KeyPool, key *APIKey) {
	t.Helper()
	for i := 1; i <= maxAuthStrikes; i++ {
		if got, want := respond401(kp, key), i == maxAuthStrikes; got != want {
			t.Fatalf("401 number %d: quarantined = %v, want %v", i, got, want)
		}
	}
}

func isSidelined(kp *KeyPool, key *APIKey) bool {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return key.Invalid
}

// A key that is quarantined authSidelineQuarantines times in a row, with no
// successful request of its own in between, while another key of the pool
// succeeded during each of those stretches, is a dead token: it is
// sidelined for the life of the process (0.29.146, operator decision
// 2026-11-13). One quarantine fewer is not enough.
func TestDeadKeyIsSidelinedWhileOthersWork(t *testing.T) {
	var buf bytes.Buffer
	kp := NewKeyPool([]string{"ghp_dead", "ghp_good"}, slog.New(slog.NewTextHandler(&buf, nil)))
	kp.SetKeyRefs(map[string]string{"ghp_dead": "aveloxis_ops.worker_oauth oauth_id=17 (name \"old\")"})
	dead, good := kp.keys[0], kp.keys[1]

	for q := 1; q <= authSidelineQuarantines; q++ {
		if isSidelined(kp, dead) {
			t.Fatalf("sidelined after %d quarantines, want %d", q-1, authSidelineQuarantines)
		}
		time.Sleep(time.Millisecond) // the other key's success falls strictly inside the stretch
		respond200(kp, good)
		quarantineOnce(t, kp, dead)
	}
	if !isSidelined(kp, dead) {
		t.Fatalf("not sidelined after %d corroborated quarantines in a row", authSidelineQuarantines)
	}
	if isSidelined(kp, good) {
		t.Fatal("the working key was sidelined")
	}
	// The pool never hands it out again, whatever the clock says.
	kp.mu.Lock()
	dead.quarantineUntil = time.Time{}
	kp.mu.Unlock()
	for i := 0; i < 5; i++ {
		got, release, err := kp.Acquire(context.Background(), ResourceCore)
		if err != nil {
			t.Fatal(err)
		}
		if got == dead {
			t.Fatal("Acquire handed out a sidelined key")
		}
		release()
	}
	// The log names the database row, which is what the operator deletes.
	out := buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "API key sidelined until restart") {
		t.Errorf("the sideline is not logged at ERROR: %s", out)
	}
	if !strings.Contains(out, "oauth_id=17") {
		t.Errorf("the sideline's log line does not carry the key's database reference: %s", out)
	}
	// Every 401 line of that key carries it too, not only the last one.
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.Contains(line, "API key") && strings.Contains(line, TokenHash("ghp_dead")) && !strings.Contains(line, "oauth_id=17") {
			t.Errorf("a line about the key lacks its database reference: %s", line)
		}
	}
}

// GitHub's auth backend has rejected valid tokens for hours (2026-06-17:
// good keys bled out one by one). When no other key succeeds, a key's
// quarantines are evidence of an incident, not of a dead token: nothing is
// sidelined, however long it lasts.
func TestNoKeyIsSidelinedWhenNoOtherKeyWorks(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_a", "ghp_b"}, quietLogger())
	a, b := kp.keys[0], kp.keys[1]
	respond200(kp, a) // both were fine before the incident
	respond200(kp, b)
	time.Sleep(time.Millisecond)
	for q := 0; q < 3*authSidelineQuarantines; q++ {
		quarantineOnce(t, kp, a)
		quarantineOnce(t, kp, b)
	}
	if isSidelined(kp, a) || isSidelined(kp, b) {
		t.Fatal("a pool-wide run of 401s sidelined a key: a GitHub auth incident would end collection until a restart")
	}
}

// A pool's only key is never sidelined: there is no other key to say the
// forge is working.
func TestTheOnlyKeyIsNeverSidelined(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_only"}, quietLogger())
	key := kp.keys[0]
	for q := 0; q < 3*authSidelineQuarantines; q++ {
		quarantineOnce(t, kp, key)
	}
	if isSidelined(kp, key) {
		t.Fatal("the pool's only key was sidelined")
	}
	if _, _, err := kp.Acquire(canceledSoon(t), ResourceCore); errors.Is(err, ErrAllKeysInvalidated) {
		t.Fatal("a sidelined last key made the pool report every key invalidated")
	}
}

// One success of its own clears the count: a key that recovers starts over.
func TestASuccessClearsTheSidelineCount(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_flaky", "ghp_good"}, quietLogger())
	flaky, good := kp.keys[0], kp.keys[1]
	for round := 0; round < 3; round++ {
		for q := 1; q < authSidelineQuarantines; q++ {
			time.Sleep(time.Millisecond)
			respond200(kp, good)
			quarantineOnce(t, kp, flaky)
		}
		respond200(kp, flaky) // it works after all
	}
	if isSidelined(kp, flaky) {
		t.Fatalf("a key that succeeded between runs of %d quarantines was sidelined", authSidelineQuarantines-1)
	}
}

// A quarantine during which no other key succeeded does not count, and does
// not undo the ones that did: the evidence is per stretch.
func TestAnUncorroboratedQuarantineDoesNotCount(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_dead", "ghp_good"}, quietLogger())
	dead, good := kp.keys[0], kp.keys[1]
	for q := 1; q < authSidelineQuarantines; q++ {
		time.Sleep(time.Millisecond)
		respond200(kp, good)
		quarantineOnce(t, kp, dead)
	}
	time.Sleep(time.Millisecond)
	quarantineOnce(t, kp, dead) // nothing from the other key in this stretch
	if isSidelined(kp, dead) {
		t.Fatal("a quarantine with no other key's success in its stretch counted toward the sideline")
	}
	time.Sleep(time.Millisecond)
	respond200(kp, good)
	quarantineOnce(t, kp, dead)
	if !isSidelined(kp, dead) {
		t.Fatal("the corroborated quarantines before the uncorroborated one were forgotten")
	}
}

// A sidelined key's own success cannot corroborate another key's failures,
// and a key cannot corroborate itself.
func TestOnlyALiveOtherKeyCorroborates(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_a", "ghp_b"}, quietLogger())
	a := kp.keys[0]
	for q := 0; q < 2*authSidelineQuarantines; q++ {
		quarantineOnce(t, kp, a)
		kp.mu.Lock()
		a.lastSuccess = time.Now().Add(time.Hour) // its own "success" must not count
		a.authStrikes = 0
		kp.mu.Unlock()
	}
	if isSidelined(kp, a) {
		t.Fatal("a key's own success corroborated its own failures")
	}
}

// The threshold is the count at which the log has always said the token is
// probably revoked: one number, not two.
func TestSidelineThresholdIsTheEscalationCount(t *testing.T) {
	if authSidelineQuarantines != authQuarantineEscalate {
		t.Errorf("authSidelineQuarantines = %d, authQuarantineEscalate = %d: the sideline happens at the count the ERROR log starts at", authSidelineQuarantines, authQuarantineEscalate)
	}
}

func TestKeyRef(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_one", "ghp_two"}, quietLogger())
	kp.SetKeyRefs(map[string]string{"ghp_one": "aveloxis.json github.api_keys[0]", "ghp_absent": "x"})
	if got := kp.keys[0].Ref(); got != "aveloxis.json github.api_keys[0]" {
		t.Errorf("Ref = %q", got)
	}
	if got := kp.keys[1].Ref(); got != keyRefUnknown {
		t.Errorf("a key without a reference answers %q, want %q", got, keyRefUnknown)
	}
	snap, _ := kp.Snapshot()
	if snap[0].Ref != "aveloxis.json github.api_keys[0]" || snap[1].Ref != keyRefUnknown {
		t.Errorf("the snapshot's references are %q and %q", snap[0].Ref, snap[1].Ref)
	}
}

func canceledSoon(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}
