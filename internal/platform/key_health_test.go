// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"testing"
	"time"
)

// v0.30.0 Phase C: the key report classifies each key once, in the process
// that holds it (its clock, its buffer): invalid > quarantined > resting >
// exhausted > ok. A spent budget whose window has already reset — or whose
// reset is unknown — refills on the next Acquire, so it is not "exhausted".
func TestKeySnapshotHealth(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	later, earlier := now.Add(10*time.Minute), now.Add(-time.Minute)
	ok := KeySnapshot{CoreSpendable: true, GraphQLSpendable: true}
	cases := []struct {
		name    string
		s       KeySnapshot
		graphQL bool
		want    KeyHealth
	}{
		{"healthy", ok, true, HealthOK},
		{"invalid beats everything", KeySnapshot{Invalid: true, QuarantineUntil: later, SecondaryUntil: later}, true, HealthInvalid},
		{"quarantined beats resting", KeySnapshot{QuarantineUntil: later, SecondaryUntil: later, CoreSpendable: true, GraphQLSpendable: true}, true, HealthQuarantined},
		{"resting", KeySnapshot{SecondaryUntil: later, CoreSpendable: true, GraphQLSpendable: true}, true, HealthResting},
		{"quarantine elapsed", KeySnapshot{QuarantineUntil: earlier, CoreSpendable: true, GraphQLSpendable: true}, true, HealthOK},
		{"core spent until a future reset", KeySnapshot{CoreResetAt: later, GraphQLSpendable: true}, true, HealthExhausted},
		{"core spent, reset passed", KeySnapshot{CoreResetAt: earlier, GraphQLSpendable: true}, true, HealthOK},
		{"core spent, reset unknown", KeySnapshot{GraphQLSpendable: true}, true, HealthOK},
		{"graphql spent on GitHub", KeySnapshot{CoreSpendable: true, GraphQLResetAt: later}, true, HealthExhausted},
		{"graphql ignored on GitLab", KeySnapshot{CoreSpendable: true, GraphQLResetAt: later}, false, HealthOK},
	}
	for _, tc := range cases {
		if got := tc.s.Health(now, tc.graphQL); got != tc.want {
			t.Errorf("%s: Health = %s, want %s", tc.name, got, tc.want)
		}
	}

	// The snapshot's spendable flags come from the pool's own buffer.
	kp := NewKeyPoolWithBuffer([]string{"tok"}, 50, testLogger())
	kp.mu.Lock()
	kp.keys[0].Remaining = 50
	kp.keys[0].ResetAt = later
	kp.mu.Unlock()
	snap, _ := kp.Snapshot()
	if snap[0].CoreSpendable || !snap[0].GraphQLSpendable || snap[0].Health(now, true) != HealthExhausted {
		t.Fatalf("snapshot at the buffer = %+v, want core not spendable (remaining == buffer) and exhausted", snap[0])
	}
}
