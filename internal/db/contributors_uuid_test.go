package db

import (
	"os"
	"strings"
	"testing"
)

// TestContributorResolverUsesDeterministicUUIDs — source-contract test.
// ContributorResolver.Resolve must mint new cntrb_ids via PlatformUUID
// (deterministic by platform+userID) instead of uuid.New() (random).
// Without this, two independent collections of the same repo produce
// different cntrb_id values for the same contributor, breaking every
// downstream table with a cntrb_id FK and making shadow-diff
// equivalence tests impossible to interpret cleanly.
//
// Regression history: the v0.18.1 phase 1 equivalence test surfaced
// UUID drift across 5 tables (issues, issue_events, messages,
// pull_requests, pull_request_reviews) because Resolve was using
// uuid.New(). Bundled into phase 1 as a verification-tooling hygiene
// fix.
func TestContributorResolverUsesDeterministicUUIDs(t *testing.T) {
	src, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (r *ContributorResolver) Resolve(")
	if idx < 0 {
		t.Fatal("cannot find Resolve in contributors.go")
	}
	body := code[idx:]
	if next := strings.Index(body[1:], "\nfunc "); next > 0 {
		body = body[:next+1]
	}

	// The function must consult PlatformUUID when userID > 0.
	if !strings.Contains(body, "PlatformUUID(") {
		t.Error("ContributorResolver.Resolve must call PlatformUUID(platformID, userID) " +
			"so cntrb_id is deterministic across independent collections. Using " +
			"uuid.New() unconditionally breaks shadow-diff equivalence tests and " +
			"any other re-collection verification.")
	}

	// The userID>0 guard is load-bearing: without it, email-only
	// contributors (userID=0) would all collide onto the same UUID
	// via PlatformUUID(1, 0). Pin the guard.
	if !strings.Contains(body, "userID > 0") && !strings.Contains(body, "userID>0") {
		t.Error("ContributorResolver.Resolve must guard the PlatformUUID call with " +
			"userID > 0 — email-only contributors (userID=0) must still get " +
			"distinct UUIDs via uuid.New(), otherwise every anonymous commit " +
			"author gets the same cntrb_id.")
	}
}

// TestPlatformUUIDDeterministicPerUserID — runtime check. Confirms the
// underlying helper is deterministic at the per-user level, which is
// the invariant Resolve's fix relies on.
func TestPlatformUUIDDeterministicPerUserID(t *testing.T) {
	// Same platform + same userID → same UUID.
	a := PlatformUUID(1, 379847)
	b := PlatformUUID(1, 379847)
	if a != b {
		t.Errorf("PlatformUUID(1, 379847) produced different UUIDs on repeated calls: %s vs %s", a, b)
	}

	// Different platforms → different UUIDs (GitHub user 1 != GitLab user 1).
	gh := PlatformUUID(1, 1)
	gl := PlatformUUID(2, 1)
	if gh == gl {
		t.Errorf("PlatformUUID(1,1) == PlatformUUID(2,1) = %s — platform byte must differentiate", gh)
	}

	// Different user IDs same platform → different UUIDs.
	u1 := PlatformUUID(1, 1)
	u2 := PlatformUUID(1, 2)
	if u1 == u2 {
		t.Errorf("PlatformUUID(1,1) == PlatformUUID(1,2) = %s — userID bytes must differentiate", u1)
	}
}
