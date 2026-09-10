// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The 2026-09-09 scancode-runner incident: `aveloxis stop serve` on a
// second host polled pg_stat_activity by application_name alone, found
// the PRIMARY's 64 serve backends, and printed 64 pg_terminate_backend
// recipes aimed at production. "Belongs to this host" is decided by
// the backend's client address against the probing session's own —
// spelled ONCE (SR-17) and driven here through the real predicate.
func TestSameClientHostPredicate(t *testing.T) {
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

	// NULL = unix socket, which only a process ON the database host can
	// use; the loopback forms are one bucket because a dual-stack
	// `localhost` resolves to ::1 for one process and 127.0.0.1 for the
	// next (observed on this very machine: psql lands on ::1).
	cases := []struct {
		a, b string // "" = NULL
		same bool
	}{
		{"127.0.0.1", "127.0.0.1", true},
		{"127.0.0.1", "::1", true},
		{"::1", "127.0.0.1", true},
		{"::1", "::1", true},
		{"", "127.0.0.1", true},
		{"", "", true},
		{"::ffff:127.0.0.1", "127.0.0.1", true},
		{"127.0.0.2", "::1", true},
		{"192.168.1.5", "192.168.1.5", true},
		// The incident shape: the primary's pool on the DB host's own
		// loopback vs a stop command dialing in from the LAN.
		{"127.0.0.1", "192.168.1.9", false},
		{"", "192.168.1.9", false},
		{"192.168.1.5", "192.168.1.6", false},
		{"::1", "fe80::1", false},
		{"10.0.0.7", "", false},
	}
	q := "SELECT " + sameClientHostSQL("$1::inet", "$2::inet")
	for _, c := range cases {
		var a, b *string
		if c.a != "" {
			a = &c.a
		}
		if c.b != "" {
			b = &c.b
		}
		var got bool
		if err := store.pool.QueryRow(ctx, q, a, b).Scan(&got); err != nil {
			t.Fatalf("(%q,%q): %v", c.a, c.b, err)
		}
		if got != c.same {
			t.Errorf("sameClientHost(%q, %q) = %v, want %v", c.a, c.b, got, c.same)
		}
	}
}

// Behavioral: a second pool tagged with a nonce application_name from
// THIS process is reported under ThisHost; nothing lands in OtherHosts
// (no other machine holds that tag); an unknown tag yields nothing. The
// other-host arm cannot be produced from one machine — the predicate
// test above drives it with the incident's exact addresses.
func TestBackendsByAppNameScopesToThisHost(t *testing.T) {
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

	tag := fmt.Sprintf("aveloxis-avtest-%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = tag
	other, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	var otherPID int
	if err := other.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&otherPID); err != nil {
		t.Fatal(err)
	}

	got, err := store.BackendsByAppName(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, pid := range got.ThisHost {
		if pid == otherPID {
			found = true
		}
	}
	if !found {
		t.Errorf("the tagged backend (pid %d) from this process must be reported under ThisHost, got %v", otherPID, got.ThisHost)
	}
	if got.OtherHosts != 0 {
		t.Errorf("no other host holds tag %q, OtherHosts = %d", tag, got.OtherHosts)
	}
	if got.Hidden != 0 {
		t.Errorf("a superuser sees every address, Hidden = %d", got.Hidden)
	}

	none, err := store.BackendsByAppName(ctx, tag+"-absent")
	if err != nil {
		t.Fatal(err)
	}
	if len(none.ThisHost) != 0 || none.OtherHosts != 0 {
		t.Errorf("an unused tag must report nothing, got %+v", none)
	}

	// The other-host arm through the real query: pretend the probing
	// session dialed in from a LAN address (the runner's shape) — the
	// same tagged backend must now land in OtherHosts, never ThisHost.
	lan := "192.168.99.99"
	far, err := store.backendsByAppNameFrom(ctx, tag, &lan)
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range far.ThisHost {
		if pid == otherPID {
			t.Errorf("seen from %s, the local backend %d must not be ThisHost's", lan, otherPID)
		}
	}
	if far.OtherHosts < 1 {
		t.Errorf("seen from %s, the local backend must count under OtherHosts, got %+v", lan, far)
	}
}

// Source contract: the probe keeps the cluster-wide datname filter
// (the sixth-pass L11 sweep), keys "this host" on the ONE shared
// predicate against the probing session's own address, surfaces scan
// errors (SR-5: a failed row read is not "no backend"), and the
// host-blind PidsByAppName is GONE from the tree (remove, never
// deprecate — a second caller would reintroduce the incident).
func TestBackendsByAppNameSourceContract(t *testing.T) {
	src := srctest.Read(t, "internal/db/backends_by_app.go")
	// The production entry point takes the session's REAL address (a nil
	// override); only the test drives the seam.
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) BackendsByAppName(")), "backendsByAppNameFrom(ctx, appName, nil)") {
		t.Error("BackendsByAppName must probe from the session's own address (nil override)")
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) backendsByAppNameFrom("))
	for _, needle := range []string{"datname = current_database()", "application_name = $1"} {
		if !strings.Contains(body, needle) {
			t.Errorf("backendsByAppNameFrom must carry %q", needle)
		}
	}
	// Round 7: the verdict is the ONE composed predicate (visible AND
	// same host), the probing session the ONE subquery, and a hidden row
	// is routed by its own column — never by a NULL address.
	for _, needle := range []string{`backendOnThisHostSQL("a")`, `clientAddrVisibleSQL("a")`, `probingSessionSQL("$2")`} {
		if n := strings.Count(body, needle); n != 1 {
			t.Errorf("backendsByAppNameFrom must carry %s exactly once, found %d", needle, n)
		}
	}
	for _, banned := range []string{"sameClientHostSQL(", "pg_backend_pid()", "IS NULL"} {
		if strings.Contains(body, banned) {
			t.Errorf("backendsByAppNameFrom must not spell %q itself — the shared helpers own it (SR-17)", banned)
		}
	}
	// SR-5 pinned on the statement that follows the Scan, not on a token:
	// the plausible refactor `err != nil { continue }` (the shape
	// checkBlockers once had) would silently drop a lingering local
	// orphan from ThisHost — a false all-clear.
	if !srctest.ContainsNormalized(body, "if err := rows.Scan(&pid, &visible, &thisHost); err != nil { return out, fmt.Errorf(") {
		t.Error("a Scan error must be RETURNED from backendsByAppNameFrom (SR-5: a failed row read is not an absent backend)")
	}
	if strings.Contains(body, "continue") {
		t.Error("no keep-going arm in backendsByAppNameFrom — every row is either counted or an error")
	}
	for _, f := range srctest.PackageFiles(t, "internal/db", 30) {
		if strings.Contains(srctest.StripGoComments(f), "PidsByAppName(") {
			t.Fatal("PidsByAppName (host-blind) must not exist anywhere in internal/db")
		}
	}
	if strings.Contains(srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go")), "PidsByAppName(") {
		t.Fatal("cmd/aveloxis/main.go must not call the removed PidsByAppName")
	}
}

// Round 7 (SR-17, SR-18): "belongs to this host" is ONE composed
// predicate — the address is VISIBLE to this role AND equal to (or local
// with) the probing session's — so no reader can compare addresses
// without the visibility gate. Derived over every non-test function in
// the package: the address comparator is composed only by
// backendOnThisHostSQL, the probing session's row is read only by
// probingSessionSQL, and every reader that renders a this-host verdict
// carries the visibility column and the ONE session subquery beside it.
func TestThisHostVerdictHasOneSpelling(t *testing.T) {
	owners := map[string][]string{
		"sameClientHostSQL(": {"func sameClientHostSQL(", "func backendOnThisHostSQL("},
		"pg_backend_pid()":   {"func probingSessionSQL("},
	}
	verdicts := 0
	for name, fsrc := range srctest.PackageFiles(t, "internal/db", 30) {
		for _, sig := range pgStatActivityReaderSigs(fsrc) {
			body := srctest.StripGoComments(srctest.FuncBody(t, fsrc, sig))
			for needle, allowed := range owners {
				if strings.Contains(body, needle) && !slices.Contains(allowed, sig) {
					t.Errorf("%s %s spells %q — only %v may (SR-17)", name, sig, needle, allowed)
				}
			}
			if strings.Contains(body, "backendOnThisHostSQL(") && sig != "func backendOnThisHostSQL(" {
				verdicts++
				if strings.Count(body, "clientAddrVisibleSQL(") != 1 || strings.Count(body, "probingSessionSQL(") != 1 {
					t.Errorf("%s %s renders a this-host verdict without the visibility column and the ONE probing-session subquery beside it", name, sig)
				}
			}
		}
	}
	srctest.MinCount(t, "this-host verdict readers in internal/db", verdicts, 3)
}

// Round-8 finding 5: the three-arm split (this host / another client
// address / address not visible) is one contract, and three operator
// pages describe it — commands.md for `stop`, troubleshooting.md for
// the orphan runbook, dedicated-scancode-host.md for the second host.
// commands.md had drifted: it documented two arms and omitted the role
// privileges the verdict depends on, so an operator reading only that
// page would take a no-verdict note for a bug. A page that describes
// the verdict must describe all of it — a partial description of a
// safety mechanism is the shape that put 64 terminate recipes in front
// of an operator in the first place.
func TestOperatorDocsDescribeAllThreeVerdictArms(t *testing.T) {
	for _, page := range []string{
		"docs/guide/commands.md",
		"docs/guide/troubleshooting.md",
		"docs/guide/dedicated-scancode-host.md",
	} {
		doc := srctest.Read(t, page)
		for _, needle := range []struct{ text, why string }{
			{"pg_read_all_stats", "the privilege that makes another role's client_addr visible"},
			{"WITH INHERIT TRUE", "PG16+ fixes a grant's inherit option at GRANT time, so a plain re-grant does not rescue a NOINHERIT member"},
			{"pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')", "the query that answers whether the role HOLDS the privileges (membership is not the test)"},
			{"not visible", "the third arm: neither this host's nor another's"},
		} {
			// Normalized: these pages wrap prose, so a literal match
			// would pin the line breaks rather than the sentence.
			if !srctest.ContainsNormalized(doc, needle.text) {
				t.Errorf("%s: missing %q — %s", page, needle.text, needle.why)
			}
		}
	}
}
