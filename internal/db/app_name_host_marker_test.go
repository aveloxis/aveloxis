// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The tag composer's contract, driven rather than described. The
// separator identity (split the tag, get the prefix back) is the one
// property appNamePrefixSQL depends on; the ceiling is the one that
// makes a marker mean a host rather than a truncation class.
func TestAppNameForHostRoundTripsThroughTheSeparator(t *testing.T) {
	for _, prefix := range []string{"aveloxis-serve", "aveloxis-web", "aveloxis-api", "aveloxis-scancode-worker"} {
		tag := AppNameForHost(prefix)
		if got := strings.SplitN(tag, AppNameHostSep, 2)[0]; got != prefix {
			t.Errorf("AppNameForHost(%q) = %q; its prefix half %q must be the prefix the readers match on", prefix, tag, got)
		}
		if len(tag) > maxAppNameBytes {
			t.Errorf("AppNameForHost(%q) = %q is %d bytes, over PostgreSQL's %d ceiling — the server would truncate and two hosts sharing a long prefix would read as one",
				prefix, tag, len(tag), maxAppNameBytes)
		}
		if strings.Count(tag, AppNameHostSep) > 1 {
			t.Errorf("AppNameForHost(%q) = %q carries more than one %q — split_part would take the wrong half", prefix, tag, AppNameHostSep)
		}
	}
}

// The sanitizer is what guarantees the separator can never appear
// inside a marker, which is what the whole reader stack rests on. Every
// degradation is toward "" (un-suffixed, address rule, today's
// behavior), never toward a marker two hosts could share.
func TestSanitizeHostTagCannotProduceASeparator(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"kate", "kate"},
		{"runner-01.internal.example.com", "runner-01.internal.example.com"},
		{"HOST_02", "HOST_02"},
		{"", ""},
		{"   ", ""},
		{"user@host", "user-host"}, // the byte that must never survive
		{"@@@", ""},                // all-rewritten degrades to un-suffixed, not to "---"
		{"---", ""},                // ditto for a hostname that is already dashes
		{"a b\tc", "a-b-c"},
		{"host\nname", "host-name"}, // a newline would break the DSN too
		{"naïve-host", "na-ve-host"},
	} {
		if got := hostid.SanitizeHostTag(tc.in); got != tc.want {
			t.Errorf("SanitizeHostTag(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(hostid.SanitizeHostTag(tc.in), AppNameHostSep) {
			t.Errorf("SanitizeHostTag(%q) leaked the %q separator — split_part would move the component boundary", tc.in, AppNameHostSep)
		}
	}
}

// THE round-12 test: Copilot #1's finding, reproduced on ONE machine.
//
// A backend tagged for a DIFFERENT host over the SAME client address is
// exactly what a transaction pooler, a database proxy or shared NAT
// produces — every client collapses onto one address, so the
// client-address rule cannot tell the primary's backends from the
// runner's. Before round 12 this backend landed in ThisHost and
// printBackendVerdict offered a pg_terminate_backend recipe for it:
// the 2026-09-09 incident, through a topology the address rule cannot
// see. The host marker in application_name survives the collapse
// because the server stores what the client sent and never rewrites it.
//
// The three companion arms are the additive contract: a matching marker
// still reads as this host, and a backend with NO marker (a
// pre-round-12 binary, or a host that cannot name itself) falls back to
// the address rule in BOTH directions — so a mixed-version fleet keeps
// working and the incident's ACTUAL topology (a runner on a different
// LAN address) stays covered throughout the upgrade window.
func TestHostMarkerBeatsCollapsedClientAddress(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	prefix := fmt.Sprintf("aveloxis-avtest-r12-%d", time.Now().UnixNano())

	// One pool per marker shape, all over the SAME loopback address the
	// probing session uses — the pooler collapse, without a pooler.
	tagged := func(tag string) int {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = tag
		cfg.MaxConns = 1
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		var pid int
		if err := p.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		return pid
	}

	foreignPID := tagged(prefix + AppNameHostSep + "some-other-host")
	oursPID := tagged(AppNameForHost(prefix))
	unmarkedPID := tagged(prefix) // a pre-round-12 binary's tag

	has := func(pids []int, pid int) bool {
		for _, p := range pids {
			if p == pid {
				return true
			}
		}
		return false
	}

	got, err := store.backendsByAppNameFrom(ctx, prefix, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// All three matched the prefix — that identity is what keeps the
	// upgrade window safe, so assert it before the verdict.
	if n := len(got.ThisHost) + got.OtherHosts + got.Hidden; n < 3 {
		t.Fatalf("all three tag shapes must match the prefix %q (suffixed, differently-suffixed, un-suffixed), saw %d: %+v", prefix, n, got)
	}
	if has(got.ThisHost, foreignPID) {
		t.Errorf("a backend marked for another host must NEVER be ThisHost's, even over an identical client address "+
			"(pid %d; this is the pooler/NAT collapse Copilot #1 named — pre-round-12 it got a pg_terminate_backend recipe): %+v",
			foreignPID, got)
	}
	if got.OtherHosts < 1 {
		t.Errorf("the foreign-marked backend must be counted under OtherHosts, got %+v", got)
	}
	if !has(got.ThisHost, oursPID) {
		t.Errorf("a backend marked with THIS host's marker must be ThisHost's (pid %d): %+v", oursPID, got)
	}
	if !has(got.ThisHost, unmarkedPID) {
		t.Errorf("an UN-marked backend on this address must still be ThisHost's — the upgrade-window fallback, "+
			"without which a round-12 stop would stop reporting a pre-round-12 serve's orphans (pid %d): %+v", unmarkedPID, got)
	}

	// The incident's actual topology, during the upgrade window: an
	// un-marked backend seen from a different address must still be
	// another host's. The marker rule must not swallow the address rule.
	lan := "192.168.99.99"
	far, err := store.backendsByAppNameFrom(ctx, prefix, &lan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if has(far.ThisHost, unmarkedPID) {
		t.Errorf("seen from %s, an un-marked backend must fall back to the address rule and read as another host's (pid %d): %+v",
			lan, unmarkedPID, far)
	}

	// A probing session that cannot name itself degrades to the address
	// rule too — for EVERY row, including the foreign-marked one. That
	// is today's behavior, which is the honest degradation: an
	// un-nameable host must not be handed a verdict it cannot support.
	blank := ""
	deg, err := store.backendsByAppNameFrom(ctx, prefix, nil, &blank)
	if err != nil {
		t.Fatal(err)
	}
	if !has(deg.ThisHost, foreignPID) {
		t.Errorf("with no marker of its own the probing session must fall back to the address rule for every row "+
			"(pre-round-12 behavior, not a stricter verdict it cannot justify): %+v", deg)
	}
}
