// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"
)

// sidelineRig is a key pool whose 401 bookkeeping runs on a clock the test
// moves: every response takes one second, and a rest is served by moving
// the clock past it. No sleeps, no dependence on the host's clock.
type sidelineRig struct {
	t   *testing.T
	kp  *KeyPool
	now time.Time
}

func newSidelineRig(t *testing.T, logger *slog.Logger, tokens ...string) *sidelineRig {
	t.Helper()
	r := &sidelineRig{t: t, kp: NewKeyPool(tokens, logger), now: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)}
	r.kp.authClock = func() time.Time { return r.now }
	return r
}

func (r *sidelineRig) key(i int) *APIKey { return r.kp.keys[i] }

func (r *sidelineRig) respond(key *APIKey, status int) {
	r.now = r.now.Add(time.Second)
	r.kp.UpdateFromResponse(key, &http.Response{StatusCode: status, Header: http.Header{}})
}

func (r *sidelineRig) ok(keys ...*APIKey) {
	for _, k := range keys {
		r.respond(k, http.StatusOK)
	}
}

// quarantine drives key to one quarantine: maxAuthStrikes 401s in a row.
func (r *sidelineRig) quarantine(key *APIKey) {
	r.t.Helper()
	before := r.count(key)
	for i := 0; i < maxAuthStrikes; i++ {
		r.respond(key, http.StatusUnauthorized)
	}
	if r.count(key) != before+1 {
		r.t.Fatalf("%d 401s in a row took the quarantine count from %d to %d, want one quarantine", maxAuthStrikes, before, r.count(key))
	}
}

// rest serves key's current rest: the clock moves just past its end.
func (r *sidelineRig) rest(key *APIKey) {
	r.kp.mu.Lock()
	until := key.quarantineUntil
	r.kp.mu.Unlock()
	if until.After(r.now) {
		r.now = until.Add(time.Second)
	}
}

func (r *sidelineRig) count(key *APIKey) int {
	r.kp.mu.Lock()
	defer r.kp.mu.Unlock()
	return key.quarantineCount
}

func (r *sidelineRig) failedRests(key *APIKey) int {
	r.kp.mu.Lock()
	defer r.kp.mu.Unlock()
	return key.failedRests
}

func (r *sidelineRig) sidelined(key *APIKey) bool {
	r.kp.mu.Lock()
	defer r.kp.mu.Unlock()
	return key.Invalid
}

func (r *sidelineRig) alive() int {
	n := 0
	for _, k := range r.kp.keys {
		if !r.sidelined(k) {
			n++
		}
	}
	return n
}

// A key that comes back from authSidelineFailedRests rests in a row still
// refused, with no success of its own, while the pool's other keys work
// cleanly, is a dead token: it is sidelined for the life of the process
// (0.29.146, operator decision 2026-11-13). One failed rest fewer is not
// enough, and the rests it sat out add up to at least 31 minutes.
func TestDeadKeyIsSidelinedWhileOthersWork(t *testing.T) {
	var buf bytes.Buffer
	r := newSidelineRig(t, slog.New(slog.NewTextHandler(&buf, nil)), "ghp_dead", "ghp_good1", "ghp_good2")
	r.kp.SetKeyRefs(map[string]string{"ghp_dead": `aveloxis_ops.worker_oauth oauth_id=17 name="old"`})
	dead, g1, g2 := r.key(0), r.key(1), r.key(2)
	start := r.now

	r.ok(g1, g2)
	r.quarantine(dead) // the first quarantine of the streak: nothing was retried yet
	for rest := 1; rest <= authSidelineFailedRests; rest++ {
		if r.sidelined(dead) {
			t.Fatalf("sidelined after %d failed rests, want %d", rest-1, authSidelineFailedRests)
		}
		r.ok(g1, g2)
		r.rest(dead)
		r.quarantine(dead)
	}
	if !r.sidelined(dead) {
		t.Fatalf("not sidelined after %d failed rests in a row with the other keys working", authSidelineFailedRests)
	}
	if r.sidelined(g1) || r.sidelined(g2) {
		t.Fatal("a working key was sidelined")
	}
	// 1+2+4+8+16 minutes of rests, from the constants.
	var rests time.Duration
	for i, d := 0, authQuarantineBase; i < authSidelineFailedRests; i, d = i+1, min(2*d, authQuarantineMax) {
		rests += d
	}
	if rests != 31*time.Minute {
		t.Fatalf("the test's own arithmetic: %v, want 31m (the docs say so)", rests)
	}
	if elapsed := r.now.Sub(start); elapsed < rests {
		t.Errorf("sidelined %v after the first 401, before the %v of rests it must sit out", elapsed, rests)
	}
	// The pool never hands it out again, whatever the clock says.
	r.kp.mu.Lock()
	dead.quarantineUntil = time.Time{}
	r.kp.mu.Unlock()
	for i := 0; i < 6; i++ {
		got, release, err := r.kp.Acquire(context.Background(), ResourceCore)
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
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if !strings.Contains(line, TokenHash("ghp_dead")) {
			continue
		}
		lines++
		if !strings.Contains(line, "oauth_id=17") {
			t.Errorf("a line about the key lacks its database reference: %s", line)
		}
	}
	if lines < authSidelineFailedRests {
		t.Errorf("only %d log lines name the dead key", lines)
	}
}

// When no other key works, a key's failures are evidence of an incident at
// the forge, not of a dead token: nothing is sidelined, however long it
// lasts, and no rest counts.
func TestNoKeyIsSidelinedWhenNoOtherKeyWorks(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_a", "ghp_b")
	a, b := r.key(0), r.key(1)
	r.ok(a, b) // both were fine before the incident
	r.now = r.now.Add(time.Hour)
	for q := 0; q < 4*authSidelineFailedRests; q++ {
		r.rest(a)
		r.quarantine(a)
		r.rest(b)
		r.quarantine(b)
	}
	if r.sidelined(a) || r.sidelined(b) {
		t.Fatal("a pool-wide run of 401s sidelined a key: an auth incident at the forge would end collection until a restart")
	}
	// The successes from before the incident are not evidence about it.
	if n := r.failedRests(a) + r.failedRests(b); n != 0 {
		t.Errorf("%d rests counted toward a sideline during a total outage, want 0", n)
	}
}

// Answers to requests that were in flight when the key was quarantined are
// not new failures: without a rest served there is no second quarantine,
// so a burst cannot sideline a key (or even lengthen its rest).
func TestInFlight401sDuringARestCountForNothing(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_a", "ghp_b", "ghp_c")
	a, b, c := r.key(0), r.key(1), r.key(2)
	r.ok(a, b, c)
	r.quarantine(a)
	// Five times the 401s a quarantine takes, all inside the one-minute
	// rest (three seconds of the rig's clock per round).
	for i := 0; i < 5*maxAuthStrikes; i++ {
		r.ok(b, c)
		r.respond(a, http.StatusUnauthorized)
	}
	r.kp.mu.Lock()
	inRest := r.now.Before(a.quarantineUntil)
	r.kp.mu.Unlock()
	if !inRest {
		t.Fatal("premise: the burst outlasted the key's rest on the rig's clock")
	}
	if got := r.count(a); got != 1 {
		t.Errorf("quarantine count %d after a burst of in-flight 401s, want 1", got)
	}
	if r.sidelined(a) || r.failedRests(a) != 0 {
		t.Fatalf("a burst of in-flight 401s counted toward a sideline (failed rests %d, sidelined %v)", r.failedRests(a), r.sidelined(a))
	}
	// And the key recovers like any other.
	r.rest(a)
	r.ok(a)
	if got, _, err := r.kp.Acquire(context.Background(), ResourceCore); err != nil || got == nil {
		t.Fatalf("Acquire after the rest: %v", err)
	}
}

// A pool's only key is never sidelined: there is no other key to say the
// forge is working.
func TestTheOnlyKeyIsNeverSidelined(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_only")
	key := r.key(0)
	for q := 0; q < 4*authSidelineFailedRests; q++ {
		r.rest(key)
		r.quarantine(key)
	}
	if r.sidelined(key) {
		t.Fatal("the pool's only key was sidelined")
	}
}

// Keys die one after another; the last one stays, so the pool never
// reports every key invalidated (the crash loop of 2026-06-17).
func TestThePoolNeverEmpties(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_a", "ghp_b", "ghp_c")
	kill := func(dead *APIKey) {
		t.Helper()
		r.rest(dead)
		r.quarantine(dead)
		for i := 0; i < 2*authSidelineFailedRests; i++ {
			for _, k := range r.kp.keys {
				if k != dead && !r.sidelined(k) {
					r.rest(k)
					r.ok(k)
				}
			}
			r.rest(dead)
			if r.sidelined(dead) {
				return
			}
			r.quarantine(dead)
		}
	}
	kill(r.key(0))
	kill(r.key(1))
	kill(r.key(2))
	if !r.sidelined(r.key(0)) || !r.sidelined(r.key(1)) {
		t.Fatalf("premise: the first two dead keys were not sidelined (alive %d)", r.alive())
	}
	if r.sidelined(r.key(2)) {
		t.Fatal("the pool's last key was sidelined: the pool is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := r.kp.Acquire(ctx, ResourceCore); errors.Is(err, ErrAllKeysInvalidated) {
		t.Fatal("the pool reports every key invalidated")
	}
}

// One success of its own clears the count: a key that recovers starts over.
func TestASuccessClearsTheSidelineCount(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_flaky", "ghp_good")
	flaky, good := r.key(0), r.key(1)
	for round := 0; round < 3; round++ {
		r.quarantine(flaky)
		for rest := 1; rest < authSidelineFailedRests; rest++ {
			r.ok(good)
			r.rest(flaky)
			r.quarantine(flaky)
		}
		if got := r.failedRests(flaky); got != authSidelineFailedRests-1 {
			t.Fatalf("premise: %d failed rests counted, want %d", got, authSidelineFailedRests-1)
		}
		r.rest(flaky)
		r.ok(flaky) // it works after all
	}
	if r.sidelined(flaky) {
		t.Fatalf("a key that succeeded between runs of %d failed rests was sidelined", authSidelineFailedRests-1)
	}
}

// A witness must have worked CLEANLY through the stretch, and most of the
// other keys must be such witnesses. In a partial incident the other keys
// succeed now and then between their own 401s: that is not a forge whose
// authentication is sound.
func TestOnlyCleanWitnessesInTheMajorityCount(t *testing.T) {
	type stretch struct {
		name string
		// what the three other keys do during the rest
		during func(r *sidelineRig, b, c, d *APIKey)
		counts bool
	}
	for _, tc := range []stretch{
		{"all three work cleanly", func(r *sidelineRig, b, c, d *APIKey) { r.ok(b, c, d) }, true},
		{"two of three work cleanly", func(r *sidelineRig, b, c, d *APIKey) { r.ok(b, c) }, true},
		{"one of three works cleanly: not most", func(r *sidelineRig, b, c, d *APIKey) { r.ok(b) }, false},
		{"none makes a request", func(r *sidelineRig, b, c, d *APIKey) {}, false},
		{"all succeed but two were also refused", func(r *sidelineRig, b, c, d *APIKey) {
			r.respond(b, http.StatusUnauthorized)
			r.respond(c, http.StatusUnauthorized)
			r.ok(b, c, d)
		}, false},
		{"refused after succeeding is not clean either", func(r *sidelineRig, b, c, d *APIKey) {
			r.ok(b, c, d)
			r.respond(b, http.StatusUnauthorized)
			r.respond(c, http.StatusUnauthorized)
		}, false},
		{"a 403 or a 5xx is neither a success nor a refusal of the token", func(r *sidelineRig, b, c, d *APIKey) {
			r.respond(b, http.StatusForbidden)
			r.respond(c, http.StatusBadGateway)
			r.respond(d, http.StatusNotFound)
		}, false},
	} {
		r := newSidelineRig(t, quietLogger(), "ghp_a", "ghp_b", "ghp_c", "ghp_d")
		a, b, c, d := r.key(0), r.key(1), r.key(2), r.key(3)
		r.ok(a, b, c, d)
		r.quarantine(a)
		tc.during(r, b, c, d)
		r.rest(a)
		r.quarantine(a)
		want := 0
		if tc.counts {
			want = 1
		}
		if got := r.failedRests(a); got != want {
			t.Errorf("%s: %d failed rests counted, want %d", tc.name, got, want)
		}
	}
}

// A stretch that does not count takes nothing away: the evidence is per
// rest, and the next corroborated one completes the count.
func TestAnUncorroboratedRestDoesNotCountOrReset(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_dead", "ghp_good")
	dead, good := r.key(0), r.key(1)
	r.quarantine(dead)
	for rest := 1; rest < authSidelineFailedRests; rest++ {
		r.ok(good)
		r.rest(dead)
		r.quarantine(dead)
	}
	r.rest(dead)
	r.quarantine(dead) // the other key made no request in this stretch
	if r.sidelined(dead) {
		t.Fatal("a rest with no clean witness counted toward the sideline")
	}
	r.ok(good)
	r.rest(dead)
	r.quarantine(dead)
	if !r.sidelined(dead) {
		t.Fatal("the corroborated rests before the uncorroborated one were forgotten")
	}
}

// The incident this rule must survive: every key valid, the forge refusing
// each request with probability p for fifteen hours (2026-06-17, "bled out
// one at a time over 15 hours"). No valid key may be sidelined at any p.
// The first version of the rule (any one other success in the stretch)
// left 3 of 20 keys alive at p = 0.9 (review round 1).
func TestAPartialAuthIncidentSidelinesNoValidKey(t *testing.T) {
	const keys, step, incident = 20, 5 * time.Second, 15 * time.Hour
	tokens := make([]string, keys)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("ghp_valid%02d", i)
	}
	for _, p := range []float64{0.05, 0.3, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1} {
		for seed := uint64(1); seed <= 3; seed++ {
			r := newSidelineRig(t, quietLogger(), tokens...)
			rng := rand.New(rand.NewPCG(seed, 0x6a09e667f3bcc908))
			end := r.now.Add(incident)
			requests, quarantines := 0, 0
			for r.now.Before(end) {
				tick := r.now
				for _, k := range r.kp.keys {
					r.kp.mu.Lock()
					resting, gone := tick.Before(k.quarantineUntil), k.Invalid
					r.kp.mu.Unlock()
					if resting || gone {
						continue // the pool would not hand it out
					}
					status := http.StatusOK
					if rng.Float64() < p {
						status = http.StatusUnauthorized
					}
					r.now = tick // every key answers within the same tick
					r.kp.UpdateFromResponse(k, &http.Response{StatusCode: status, Header: http.Header{}})
					requests++
				}
				r.now = tick.Add(step)
			}
			for _, k := range r.kp.keys {
				quarantines += r.count(k)
			}
			if alive := r.alive(); alive != keys {
				t.Errorf("p=%.2f seed %d: %d of %d valid keys left after %v (%d requests, %d quarantines)", p, seed, alive, keys, incident, requests, quarantines)
			}
			// Guard the denominator: at a high p the keys really were
			// quarantined over and over, so the rule was exercised.
			if p >= 0.9 && quarantines < keys*authSidelineFailedRests {
				t.Errorf("p=%.2f seed %d: only %d quarantines in the simulation; it did not exercise the rule", p, seed, quarantines)
			}
		}
	}
}

// The same simulation with one dead key among working ones at p = 0: the
// rule still finds it. (Without this the test above passes for a rule that
// never sidelines.)
func TestTheSimulatedPoolStillSidelinesItsDeadKey(t *testing.T) {
	r := newSidelineRig(t, quietLogger(), "ghp_dead", "ghp_b", "ghp_c", "ghp_d")
	dead := r.key(0)
	end := r.now.Add(2 * time.Hour)
	for r.now.Before(end) && !r.sidelined(dead) {
		tick := r.now
		for _, k := range r.kp.keys {
			r.kp.mu.Lock()
			resting := tick.Before(k.quarantineUntil)
			r.kp.mu.Unlock()
			if resting || r.sidelined(k) {
				continue
			}
			status := http.StatusOK
			if k == dead {
				status = http.StatusUnauthorized
			}
			r.now = tick
			r.kp.UpdateFromResponse(k, &http.Response{StatusCode: status, Header: http.Header{}})
		}
		r.now = tick.Add(5 * time.Second)
	}
	if !r.sidelined(dead) {
		t.Fatal("a dead key among three working ones was not sidelined within two hours")
	}
	if r.alive() != 3 {
		t.Errorf("%d keys alive, want the 3 working ones", r.alive())
	}
}

// The threshold is the count at which the log has always said the token is
// probably revoked: one number, not two.
func TestSidelineThresholdIsTheEscalationCount(t *testing.T) {
	if authSidelineFailedRests != authQuarantineEscalate {
		t.Errorf("authSidelineFailedRests = %d, authQuarantineEscalate = %d: the pool acts at the count the ERROR log starts at", authSidelineFailedRests, authQuarantineEscalate)
	}
}

func TestKeyRef(t *testing.T) {
	kp := NewKeyPool([]string{"ghp_one", "ghp_two"}, quietLogger())
	kp.SetKeyRefs(map[string]string{"ghp_one": "config file github.api_keys[0]", "ghp_absent": "x"})
	if got := kp.keys[0].Ref(); got != "config file github.api_keys[0]" {
		t.Errorf("Ref = %q", got)
	}
	if got := kp.keys[1].Ref(); got != keyRefUnknown {
		t.Errorf("a key without a reference answers %q, want %q", got, keyRefUnknown)
	}
	snap, _ := kp.Snapshot()
	if snap[0].Ref != "config file github.api_keys[0]" || snap[1].Ref != keyRefUnknown {
		t.Errorf("the snapshot's references are %q and %q", snap[0].Ref, snap[1].Ref)
	}
}
