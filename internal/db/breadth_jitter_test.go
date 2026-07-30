// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// breadth_jitter_test.go — TDD suite for the v0.27.55 breadth-cooldown
// jitter.
//
// Motivating diagnosis (2026-07-29, aveloxis_large): breadth throughput
// was violently bimodal — 300-560K contributors marked on some days,
// 411-7K on others — because a hard cooldown cliff makes each day's
// eligible supply an exact echo of the marks one cooldown-period
// earlier. Cohorts bunched by historical events (the v0.27.8 backlog
// chew, pre-v0.27.8 famine days) re-bunch forever. Jittering each row's
// effective cooldown by ±BreadthCooldownJitterFrac diffuses the bunches:
// every period adds an independent uniform offset, so cohort spread
// grows period over period and the feast/famine sawtooth decays toward
// the uniform pool/cooldown daily rate.
//
// Soundness invariants pinned here:
//   1. factor range symmetric around 1.0 → the operator's configured
//      cadence is preserved in expectation;
//   2. rows older than (1+frac)×cooldown are ALWAYS claimable (jitter
//      can delay a row, never strand it);
//   3. rows fresher than (1−frac)×cooldown are NEVER claimable (jitter
//      shortens the cooldown boundedly, never defeats it);
//   4. the boundary zone is genuinely probabilistic;
//   5. never-attempted rows (NULL stamp) keep absolute priority and the
//      ORDER BY / LIMIT contract is unchanged.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestBreadthCooldownJitterFracValue pins the constant and its safety
// bounds. 0.10 is derived, not a guess: ±10% of the production 14-day
// cooldown is ±1.4 days — wide enough that adjacent feast cohorts leak
// into the 1-2-day famine valleys within a single period, narrow enough
// that every row's actual cadence stays within 10% of the operator's
// configured knob. The bounds check guards the class: at frac >= 0.5
// the lower edge reaches (1-0.5)x → a half-cooldown re-scan, and
// beyond it the multiplier can go non-positive, making rows eligible
// immediately after marking.
func TestBreadthCooldownJitterFracValue(t *testing.T) {
	if BreadthCooldownJitterFrac != 0.10 {
		t.Errorf("BreadthCooldownJitterFrac = %v, want 0.10 — if this was tuned deliberately, update this pin and the derivation comment together", BreadthCooldownJitterFrac)
	}
	if BreadthCooldownJitterFrac <= 0 || BreadthCooldownJitterFrac >= 0.5 {
		t.Fatalf("BreadthCooldownJitterFrac = %v is outside (0, 0.5): at 0 the jitter is a no-op and the feast/famine echo returns; at >= 0.5 the effective cooldown can reach zero or below", BreadthCooldownJitterFrac)
	}
}

// TestBreadthClaimQueryHasSymmetricJitter pins the SQL shape: the
// cooldown branch must multiply the interval by
// (1 - $3 + random() * 2 * $3) — uniform over [1-frac, 1+frac], mean
// exactly 1.0 — and the constant must be what's passed. The NULL
// branch, ordering, and LIMIT must survive untouched (core claim logic
// unchanged).
func TestBreadthClaimQueryHasSymmetricJitter(t *testing.T) {
	src := readSourceFile(t, "breadth_store.go")
	idx := strings.Index(src, "func (s *PostgresStore) GetContributorsForBreadth(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsForBreadth")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}

	flat := strings.Join(strings.Fields(body), " ")
	if !strings.Contains(flat, "$2::interval * (1.0 - $3::float8 + random() * 2.0 * $3::float8)") {
		t.Error("claim query must jitter the cooldown as $2::interval * (1.0 - $3::float8 + random() * 2.0 * $3::float8) — uniform over [1-frac, 1+frac] with mean exactly 1.0, so the configured cadence is preserved in expectation; an asymmetric range would silently shift the operator's cooldown")
	}
	if !strings.Contains(flat, "BreadthCooldownJitterFrac") {
		t.Error("the jitter parameter must be the shared BreadthCooldownJitterFrac constant, not an inline literal")
	}
	// Unchanged core contract.
	for _, needle := range []string{
		"cntrb_last_breadth_at IS NULL",
		"ORDER BY cntrb_last_breadth_at ASC NULLS FIRST",
		"LIMIT $1",
	} {
		if !strings.Contains(flat, needle) {
			t.Errorf("claim query lost %q — the jitter must not alter the NULL-priority / ordering / limit contract", needle)
		}
	}
}

// seedJitterContributor inserts a synthetic contributor whose
// cntrb_last_breadth_at is NOW() - age, computed DB-side so Go/DB clock
// skew cannot blur the boundary. age == 0 means never attempted (NULL).
func seedJitterContributor(t *testing.T, store *PostgresStore, login string, age time.Duration) string {
	t.Helper()
	ctx := t.Context()
	// Re-runs: clear any leftover row with this login first.
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	stamp := "NULL"
	if age > 0 {
		stamp = fmt.Sprintf("NOW() - '%s'::interval", age.String())
	}
	var id string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, gh_login, cntrb_last_breadth_at, data_collection_date)
		VALUES (gen_random_uuid(), $1, $1, `+stamp+`, NOW())
		RETURNING cntrb_id::text`, login).Scan(&id); err != nil {
		t.Fatalf("seed %s: %v", login, err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	return id
}

// claimIDs runs GetContributorsForBreadth with a limit exceeding the
// whole scratch pool (so LIMIT truncation can never mask presence or
// absence) and returns the claimed IDs as a set.
func claimIDs(t *testing.T, store *PostgresStore, cooldown time.Duration) map[string]bool {
	t.Helper()
	ctx := t.Context()
	var poolN int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL AND gh_login != '' AND COALESCE(cntrb_deleted, 0) = 0`).Scan(&poolN); err != nil {
		t.Fatal(err)
	}
	out, err := store.GetContributorsForBreadth(ctx, poolN+10, cooldown)
	if err != nil {
		t.Fatalf("GetContributorsForBreadth: %v", err)
	}
	ids := make(map[string]bool, len(out))
	for _, c := range out {
		ids[c.ID] = true
	}
	return ids
}

// TestBreadthJitterEligibilityBounds seeds three contributors around a
// 240h cooldown — well past the upper jitter edge (2×cooldown), well
// inside the lower edge (0.5×cooldown), and never-attempted — and
// asserts over repeated claims that jitter delays but never strands,
// shortens but never defeats, and NULL priority survives.
func TestBreadthJitterEligibilityBounds(t *testing.T) {
	store, _ := v0251Connect(t)
	defer store.Close()
	cooldown := 240 * time.Hour

	pastID := seedJitterContributor(t, store, "_avjitter_well_past", 2*cooldown)
	freshID := seedJitterContributor(t, store, "_avjitter_well_inside", cooldown/2)
	nullID := seedJitterContributor(t, store, "_avjitter_never", 0)

	for i := 0; i < 10; i++ {
		ids := claimIDs(t, store, cooldown)
		if !ids[pastID] {
			t.Fatalf("call %d: contributor aged 2x cooldown missing from claim — jitter's upper bound is (1+%v)x cooldown, so anything older must ALWAYS be eligible; a row jitter can strand never converges", i, BreadthCooldownJitterFrac)
		}
		if ids[freshID] {
			t.Fatalf("call %d: contributor aged 0.5x cooldown was claimed — jitter's lower bound is (1-%v)x cooldown, so anything fresher must NEVER be eligible; otherwise jitter defeats the cooldown instead of smoothing it", i, BreadthCooldownJitterFrac)
		}
		if !ids[nullID] {
			t.Fatalf("call %d: never-attempted contributor missing — NULLS FIRST priority must survive the jitter", i)
		}
	}
}

// TestBreadthJitterBoundaryIsProbabilistic seeds a contributor at
// exactly the cooldown age — the midpoint of the jitter window, where
// each claim should include it with probability ~0.5 — and asserts
// both outcomes occur across 40 claims. Without jitter this row is
// deterministically claimed every time (its age creeps past the hard
// cliff), so the "absent at least once" half is the assertion that
// fails on the pre-jitter query. P(all 40 calls agree) ≈ 2×2⁻⁴⁰ —
// not a flake source.
func TestBreadthJitterBoundaryIsProbabilistic(t *testing.T) {
	store, _ := v0251Connect(t)
	defer store.Close()
	cooldown := 240 * time.Hour
	borderID := seedJitterContributor(t, store, "_avjitter_boundary", cooldown)

	seen, missed := 0, 0
	for i := 0; i < 40; i++ {
		if claimIDs(t, store, cooldown)[borderID] {
			seen++
		} else {
			missed++
		}
	}
	if seen == 0 {
		t.Errorf("boundary-aged contributor never claimed in 40 calls — jitter window looks shifted or the row is stranded (want ~50/50 at the midpoint)")
	}
	if missed == 0 {
		t.Errorf("boundary-aged contributor claimed in all 40 calls — the cooldown boundary is still a deterministic cliff, so the feast/famine echo this jitter exists to break is still intact")
	}
}
