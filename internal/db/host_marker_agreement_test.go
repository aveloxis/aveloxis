// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// longHostname is over MaxHostMarkerBytes and is not a hypothetical:
// AWS EC2's default private DNS name is this shape
// (ip-10-0-1-23.us-west-2.compute.internal, 39 bytes), as is the
// example AppNameForHost's own doc comment reaches for
// (scancode-runner-01.internal.example.com, 39 bytes), as are ordinary
// Kubernetes pod names. The budget is 63 - len("aveloxis-scancode-worker")
// - 1 = 38, so all three digest.
const longHostname = "scancode-runner-01.internal.example.com"

// THE round-14 test (Copilot round 4, finding 1): the TAGGING half and
// the READING half must compute the SAME marker for the SAME host.
//
// Round 12 gave AppNameForHost a digest branch for hostnames that would
// not fit PostgreSQL's 63-byte application_name ceiling — correct in
// itself, since a truncated marker makes two hosts read as one (the
// 2026-09-09 incident's shape). But no reader ever computed that
// digest: backendsByAppNameFrom, checkBlockersFrom and
// otherServeAddressesFrom each passed the RAW hostid.HostTag() as
// me.host_tag. On any host whose name exceeds the budget the two halves
// therefore disagreed, both markers were non-NULL and unequal, and
// sameHostAsProbeSQL's veto fired against THIS HOST'S OWN backends:
// `aveloxis stop` would see an empty ThisHost, return instantly without
// waiting out the drain (the round-8 finding-2 failure, reintroduced
// through a new path), and print the "still running somewhere else"
// note for its own pool.
//
// The marker is therefore computed ONCE, prefix-independently, and
// composed into the tag. Prefix-independence is not a nicety:
// checkBlockersFrom matches `application_name LIKE 'aveloxis-%'` across
// EVERY component prefix while passing a single marker parameter, so a
// per-prefix budget would make that reader wrong for every prefix but
// one.
func TestHostMarkerAgreesWithTheTagForEveryComponentPrefix(t *testing.T) {
	prefixes := []string{"aveloxis-serve", "aveloxis-web", "aveloxis-api", "aveloxis-scancode-worker"}

	for _, host := range []string{"kate", longHostname, strings.Repeat("x", 200)} {
		want := hostMarkerFor(host)
		if want == "" {
			t.Fatalf("hostMarkerFor(%q) = %q: a nameable host must always produce a marker", host, want)
		}
		if len(want) > MaxHostMarkerBytes {
			t.Errorf("hostMarkerFor(%q) = %q is %d bytes, over the %d budget — it would not fit beside every component prefix",
				host, want, len(want), MaxHostMarkerBytes)
		}
		for _, prefix := range prefixes {
			tag := appNameForHostWith(prefix, host)
			if len(tag) > maxAppNameBytes {
				t.Errorf("appNameForHostWith(%q, %q) = %q is %d bytes, over PostgreSQL's %d ceiling",
					prefix, host, tag, len(tag), maxAppNameBytes)
			}
			parts := strings.SplitN(tag, AppNameHostSep, 2)
			if parts[0] != prefix {
				t.Errorf("appNameForHostWith(%q, %q) = %q: prefix half is %q", prefix, host, tag, parts[0])
			}
			if len(parts) != 2 {
				t.Errorf("appNameForHostWith(%q, %q) = %q carries NO marker while hostMarkerFor returns %q — "+
					"the reader would pass a marker no tag can match", prefix, host, tag, want)
				continue
			}
			if parts[1] != want {
				t.Errorf("appNameForHostWith(%q, %q) marks %q but hostMarkerFor(%q) = %q — the tagging and reading "+
					"halves disagree, so sameHostAsProbeSQL's veto fires against this host's own backends",
					prefix, host, parts[1], host, want)
			}
		}
	}
}

// Prefix-independence stated as its own property, because
// checkBlockersFrom depends on it directly: one marker parameter,
// every component prefix.
// The seam and the production path must agree at the REAL values too,
// or appNameForHostWith could drift into a second implementation that
// proves nothing about what the binary tags with.
func TestProductionTagCarriesProductionMarker(t *testing.T) {
	marker := HostMarker()
	for _, prefix := range []string{"aveloxis-serve", "aveloxis-scancode-worker"} {
		tag := AppNameForHost(prefix)
		parts := strings.SplitN(tag, AppNameHostSep, 2)
		switch {
		case marker == "" && len(parts) != 1:
			t.Errorf("HostMarker() is empty but AppNameForHost(%q) = %q carries a marker", prefix, tag)
		case marker != "" && (len(parts) != 2 || parts[1] != marker):
			t.Errorf("AppNameForHost(%q) = %q but HostMarker() = %q — the binary tags with a marker its own readers "+
				"do not compute", prefix, tag, marker)
		}
		if tag != appNameForHostWith(prefix, hostTagForTest()) {
			t.Errorf("appNameForHostWith is a second implementation: it gives %q where AppNameForHost gives %q",
				appNameForHostWith(prefix, hostTagForTest()), tag)
		}
	}
}

func hostTagForTest() string { return hostid.HostTag() }

func TestHostMarkerIsPrefixIndependent(t *testing.T) {
	marker := func(tag string) string {
		parts := strings.SplitN(tag, AppNameHostSep, 2)
		if len(parts) != 2 {
			return ""
		}
		return parts[1]
	}
	for _, host := range []string{"kate", longHostname} {
		short := marker(appNameForHostWith("aveloxis-web", host))
		long := marker(appNameForHostWith("aveloxis-scancode-worker", host))
		if short != long {
			t.Errorf("host %q marks as %q under aveloxis-web but %q under aveloxis-scancode-worker — "+
				"checkBlockersFrom matches LIKE 'aveloxis-%%' across every prefix with ONE marker parameter, "+
				"so a per-prefix marker is wrong for every prefix but one", host, short, long)
		}
	}
}

// The degradations are unchanged from round 12 and stay monotonic: a
// host that cannot name itself tags un-suffixed and reads by the
// address rule.
func TestHostMarkerDegradesToUnsuffixed(t *testing.T) {
	if got := hostMarkerFor(""); got != "" {
		t.Errorf("hostMarkerFor(\"\") = %q, want \"\": a host that cannot name itself must degrade to the address rule", got)
	}
	if got := appNameForHostWith("aveloxis-serve", ""); got != "aveloxis-serve" {
		t.Errorf("appNameForHostWith with no hostname = %q, want the bare prefix", got)
	}
	// A prefix so long that even the digest marker will not fit is not
	// reachable from resolveComponents, but the arithmetic is checked
	// rather than assumed — and the degradation is the bare prefix, so
	// the row's marker reads NULL and the veto abstains.
	huge := strings.Repeat("p", maxAppNameBytes)
	if got := appNameForHostWith(huge, longHostname); got != huge {
		t.Errorf("appNameForHostWith(<%d-byte prefix>) = %q, want the bare prefix", len(huge), got)
	}
}

// hostid.HostTag() has exactly ONE consumer in this package: the marker
// composer. Every reader passes HostMarker(). This is the ownership map
// that keeps the two halves from drifting again — round 12 shipped the
// digest branch and left three readers on the raw hostname, and no test
// could see it because each half was correct in isolation.
func TestHostTagHasOneConsumerInThisPackage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var sites []string
	var scanned int
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		scanned++
		// Comments are stripped by construction: we walk the AST, so a
		// doc comment mentioning hostid.HostTag() is not a call.
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "HostTag" {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "hostid" {
				return true
			}
			sites = append(sites, name)
			return true
		})
	}
	// Anti-decorative guard: count files EXAMINED, not violations found
	// (v0.28.18's rule — an empty result is the goal state, so guarding
	// on findings would make a completed burn-down pass forever).
	if scanned < 50 {
		t.Fatalf("the internal/db scan broke: only %d non-test sources parsed", scanned)
	}
	if len(sites) != 1 {
		t.Errorf("hostid.HostTag() is called from %d sites in internal/db (%v); it must have exactly ONE — "+
			"the marker composer in app_name.go. Every reader passes HostMarker() instead, or the tagging and "+
			"reading halves drift apart exactly as they did in round 12.", len(sites), sites)
	}
	for _, s := range sites {
		if !strings.HasSuffix(s, "app_name.go") {
			t.Errorf("hostid.HostTag() is called from %s; the ONE consumer must be app_name.go's marker composer", s)
		}
	}
}

// THE behavioral arm, driven through the real reader. A backend tagged
// by a host whose name exceeds the budget carries the DIGEST marker;
// the probing session must compute the same one. Arm 2 reproduces the
// pre-fix shape: the raw hostname is not the marker, so a reader that
// passes it (round 12's three readers) vetoes this host's own backend
// into OtherHosts.
func TestLongHostnameTagAndReaderAgreeThroughTheRealQuery(t *testing.T) {
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

	prefix := fmt.Sprintf("aveloxis-avtest-r14-%d", time.Now().UnixNano())

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appNameForHostWith(prefix, longHostname)
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var pid int
	if err := pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}

	has := func(pids []int, want int) bool {
		for _, p := range pids {
			if p == want {
				return true
			}
		}
		return false
	}

	// Arm 1: the probing session computes the marker the tagger wrote.
	marker := hostMarkerFor(longHostname)
	got, err := store.backendsByAppNameFrom(ctx, prefix, nil, &marker)
	if err != nil {
		t.Fatal(err)
	}
	if !has(got.ThisHost, pid) {
		t.Errorf("a backend tagged by a host whose name exceeds the %d-byte budget must still be ThisHost's when the "+
			"probing session computes the same marker (pid %d, tag %q, marker %q): an empty ThisHost makes "+
			"`aveloxis stop` return before the drain finishes and print the \"running somewhere else\" note for its "+
			"own pool: %+v", MaxHostMarkerBytes, pid, appNameForHostWith(prefix, longHostname), marker, got)
	}

	// Arm 2: the pre-fix shape. Round 12's readers passed the RAW
	// hostname, which is not what the digest branch wrote, so the veto
	// fired against this host's own backend.
	raw := longHostname
	pre, err := store.backendsByAppNameFrom(ctx, prefix, nil, &raw)
	if err != nil {
		t.Fatal(err)
	}
	if has(pre.ThisHost, pid) {
		t.Errorf("passing the RAW hostname %q as the probing session's marker must NOT match a digest-marked backend "+
			"(pid %d) — if it does, the marker rule has stopped separating anything: %+v", raw, pid, pre)
	}
}
