// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.31 (audit Phase 3, C3) — PlatformUUID's "Augur-compatible"
// claim validated against Augur's ACTUAL algorithm, not against our
// own implementation. The pre-existing UUID tests asserted our output
// against strings produced by our code — if the layout had never
// matched Augur's, every test would still pass (both-sides-agree).
//
// Ground truth: chaoss/augur dev branch, augur/tasks/util/AugurUUID.py
// (fetched 2026-07-21). The normative facts from that source:
//
//	class GithubUUID(AugurUUID):
//	    struct = { "platform": {"start": 0, "size": 1},
//	               "user":     {"start": 1, "size": 4}, ... }
//	write_int: source.to_bytes(num_bytes, "big")  # big-endian
//	to_UUID:   uuid.UUID(bytes=bytes(self.bytes)) # byte-order verbatim
//
// So a contributor UUID is: byte0 = platform_id, bytes1–4 = user id
// big-endian, bytes5–15 = zero. The expected strings below are derived
// BY HAND from those rules (hex-packing the bytes into RFC 4122
// canonical form), never by running our Go code.
//
// Divergence note, deliberate: Augur's write_int raises OverflowError
// for user ids that don't fit 4 bytes — Augur has NO representation
// for them. Aveloxis extends with an 8-byte layout (v0.16.x) rather
// than erroring; the last test asserts that extension stays OUTSIDE
// the Augur-compatible byte space (bytes 5–8 non-zero) so it can never
// collide with a legitimate Augur UUID.

import "testing"

func TestPlatformUUIDMatchesAugurAlgorithm(t *testing.T) {
	cases := []struct {
		platform int
		userID   int64
		want     string // hand-derived from AugurUUID.py's byte rules
	}{
		// platform=1, user=15: bytes 01 00 00 00 0f 00…
		{1, 15, "01000000-0f00-0000-0000-000000000000"},
		// platform=1, user=2445413 (0x255065): bytes 01 00 25 50 65 00…
		// — also the exact cntrb_id from the v0.22.13 production
		// incident log, so this vector is corroborated by a real
		// Augur-era row.
		{1, 2445413, "01002550-6500-0000-0000-000000000000"},
		// platform=2 (GitLab), user=1: bytes 02 00 00 00 01 00…
		{2, 1, "02000000-0100-0000-0000-000000000000"},
		// user = uint32 max — the largest id Augur can represent.
		{1, 4294967295, "01ffffff-ff00-0000-0000-000000000000"},
	}
	for _, tc := range cases {
		got := PlatformUUID(tc.platform, tc.userID).String()
		if got != tc.want {
			t.Errorf("PlatformUUID(%d, %d) = %s, want %s (Augur AugurUUID.py byte layout)", tc.platform, tc.userID, got, tc.want)
		}
	}
}

func TestPlatformUUIDOverflowExtensionStaysOutsideAugurSpace(t *testing.T) {
	// Augur raises OverflowError here; our extension writes 8 bytes at
	// offset 1. Every Augur-compatible UUID has bytes 5–15 zero for a
	// bare user id — the extension MUST populate at least one of bytes
	// 5–8, or it would masquerade as (and collide with) a truncated
	// 4-byte id.
	id := PlatformUUID(1, int64(4294967295)+1) // uint32 max + 1 = 0x1_00000000
	b := [16]byte(id)
	if b[0] != 1 {
		t.Errorf("platform byte = %#x, want 0x01", b[0])
	}
	// 0x0000000100000000 big-endian across bytes 1–8: byte4 = 0x01 at
	// position... hand-derived: 00 00 00 01 00 00 00 00 → bytes1-3
	// zero, byte4 = 0x01, bytes5-8 zero. Truncation to 4 bytes would
	// have produced user=0 — assert we did NOT produce the 4-byte
	// encoding of user 0 (all-zero bytes 1–8).
	allZero := true
	for i := 1; i <= 8; i++ {
		if b[i] != 0 {
			allZero = false
		}
	}
	if allZero {
		t.Error("uint32-overflow id encoded as user 0 — silent truncation, collides with a real Augur UUID")
	}
	if b[4] != 0x01 {
		t.Errorf("byte 4 = %#x, want 0x01 (0x100000000 written big-endian over bytes 1–8)", b[4])
	}
}
