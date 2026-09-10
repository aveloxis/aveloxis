// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-11 finding 6. The hidden-backend tests create least-privilege
// database roles so the round-7/8 visibility predicates can be driven
// against a REAL restricted viewer. Two properties of those roles made
// the fixture a liability rather than a test cost:
//
//  1. A ROLE IS A CLUSTER OBJECT. It survives DROP DATABASE and the
//     recreation of the scratch database, so a leaked role outlives
//     every other kind of test residue this repo has had to drain.
//     Cleanup was t.Cleanup only — skipped on SIGKILL or a package
//     timeout — and both DROP statements discarded their errors, so
//     nothing in the test tier could detect the leak. Each run's nonce
//     suffix meant each leak was a NEW role, not a re-used one.
//
//  2. The password was a LITERAL IN THE REPOSITORY, so every leaked
//     role was login-capable with a publicly known credential.
//
// Three coupled fixes: a per-run random password (nothing to publish),
// a SWEEP ON ENTRY that drains prior runs' roles (the established
// residue pattern — v0.27.75, the dedup fixture), and DROP errors that
// are CHECKED so a leak fails the run that caused it.

// scratchRolePrefix is the ONE prefix (SR-17) every test role carries:
// the sweep's LIKE pattern and the tripwire below both key on it, so a
// role created outside the prefix would be invisible to both.
const scratchRolePrefix = "avtest_"

// randomScratchPassword returns a per-run credential. Never a constant:
// a leaked role must not also carry a password anyone can read out of
// the repository (round-11 finding 6).
func randomScratchPassword(t testing.TB) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a scratch-role password: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// scratchRoleName builds a per-run role name under the shared prefix.
func scratchRoleName(kind string) string {
	return fmt.Sprintf("%s%s_%d", scratchRolePrefix, kind, time.Now().UnixNano())
}

// scratchRoleAge reports how long ago a scratch role's nonce was minted.
// scratchRoleName puts UnixNano after the last `_`, so the age is
// recoverable from the name alone — no catalog column carries a role's
// creation time. A name whose suffix is not a nonce reports !ok: the
// sweep must KEEP such a role rather than assume it is ancient, because
// assuming is what drops a live run's role.
func scratchRoleAge(name string, now time.Time) (time.Duration, bool) {
	i := strings.LastIndex(name, "_")
	if i < 0 || i == len(name)-1 {
		return 0, false
	}
	nanos, err := strconv.ParseInt(name[i+1:], 10, 64)
	if err != nil || nanos <= 0 {
		return 0, false
	}
	return now.Sub(time.Unix(0, nanos)), true
}

// staleRoleCutoff is the age past which a scratch role PROVABLY belongs
// to a finished run, derived from the package's own `-timeout` rather
// than picked: whatever created a role older than the test timeout has
// already been killed by that timeout, or finished and run its cleanup.
// A role younger than it may still be held by a live run.
//
// `go test` passes -test.timeout=10m by default. `-timeout 0` disables
// it, and with no upper bound on a run's life there is no sound cutoff
// at all — the sweep then drops NOTHING and says so, which is the safe
// direction (residue is drained by the next bounded run; a wrong drop
// fails an innocent one).
func staleRoleCutoff() (time.Duration, bool) {
	f := flag.Lookup("test.timeout")
	if f == nil {
		return 0, false
	}
	g, ok := f.Value.(flag.Getter)
	if !ok {
		return 0, false
	}
	d, ok := g.Get().(time.Duration)
	if !ok || d <= 0 {
		return 0, false
	}
	return d, true
}

// sweepStaleTestRoles drops the scratch roles left behind by earlier
// runs. Roles are CLUSTER objects, so t.Cleanup alone cannot bound the
// residue: a SIGKILL or a package timeout strands a login-capable role
// on the cluster forever.
//
// AGE-GATED, and that gate is load-bearing (L10 finding 1 on the
// round-11 fix). The first version dropped every `avtest_%` role in the
// cluster, on the premise that a role a concurrent run still holds open
// "legitimately refuses to drop". PostgreSQL does not hold that premise:
// verified live, `DROP OWNED BY` and `DROP ROLE IF EXISTS` BOTH succeed
// on a role with an open session, and the role is gone. Roles being
// cluster-wide, that reaches across scratch DATABASES too — run B's
// sweep would drop run A's viewer mid-test, and A's own checked cleanup
// would then fail "role does not exist", failing the innocent run for
// the leak B caused.
func sweepStaleTestRoles(ctx context.Context, t testing.TB, admin *PostgresStore) {
	t.Helper()
	cutoff, bounded := staleRoleCutoff()
	if !bounded {
		t.Log("scratch-role sweep: no package -timeout, so no age proves a role's run has ended — dropping nothing this run")
		return
	}
	// The prefix ends in `_`, which LIKE treats as a single-character
	// wildcard — escape it so the sweep matches only real scratch roles.
	pattern := strings.ReplaceAll(scratchRolePrefix, "_", `\_`) + "%"
	rows, err := admin.pool.Query(ctx, `SELECT rolname FROM pg_roles WHERE rolname LIKE $1`, pattern)
	if err != nil {
		t.Logf("scratch-role sweep: cannot list roles (%v) — prior runs' roles may persist", err)
		return
	}
	var stale []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Logf("scratch-role sweep: %v", err)
			return
		}
		stale = append(stale, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Logf("scratch-role sweep: %v", err)
		return
	}
	now := time.Now()
	for _, name := range stale {
		age, parsed := scratchRoleAge(name, now)
		if !parsed {
			t.Logf("scratch-role sweep: %s has no parseable nonce — keeping it (an unparseable name cannot be proved stale, and dropping a live run's role is what this gate exists to prevent)", name)
			continue
		}
		if age < cutoff {
			t.Logf("scratch-role sweep: %s is %v old (< the %v -timeout) — it may belong to a CONCURRENT run, keeping it", name, age.Round(time.Second), cutoff)
			continue
		}
		// Best-effort past the age gate: a role that owns objects in
		// ANOTHER database refuses DROP OWNED, and failing the sweep for
		// that would make an unrelated run flaky.
		if err := dropScratchRole(ctx, admin, name); err != nil {
			t.Logf("scratch-role sweep: %s not dropped (%v) — it owns objects in another database", name, err)
			continue
		}
		t.Logf("scratch-role sweep: dropped %s, %v old, left by an earlier run", name, age.Round(time.Second))
	}
}

// dropScratchRole revokes what a scratch role owns and drops it. DROP
// OWNED only reaches the CURRENT database, which is all a scratch role
// is ever granted in.
func dropScratchRole(ctx context.Context, admin *PostgresStore, role string) error {
	if !strings.HasPrefix(role, scratchRolePrefix) {
		return fmt.Errorf("refusing to drop %q: not a scratch role (prefix %q)", role, scratchRolePrefix)
	}
	if _, err := admin.pool.Exec(ctx, `DROP OWNED BY `+role); err != nil {
		return fmt.Errorf("DROP OWNED BY %s: %w", role, err)
	}
	if _, err := admin.pool.Exec(ctx, `DROP ROLE IF EXISTS `+role); err != nil {
		return fmt.Errorf("DROP ROLE %s: %w", role, err)
	}
	return nil
}

// cleanupScratchRole is the CHECKED cleanup every creation site
// registers. A failed drop leaves a login-capable cluster object behind,
// so it fails the run that caused it rather than passing in silence.
func cleanupScratchRole(t *testing.T, admin *PostgresStore, role string) {
	t.Helper()
	cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ccancel()
	if err := dropScratchRole(cctx, admin, role); err != nil {
		t.Errorf("scratch role %s was not dropped: %v — a role is a CLUSTER object and survives "+
			"DROP DATABASE, so a leak here outlives every other kind of test residue (round-11 finding 6)", role, err)
	}
}

// The tripwire. Both halves of the class die here: a literal password
// anywhere in a CREATE ROLE, and a creation site that does not route its
// cleanup through the checked helper.
func TestScratchRolesLeaveNoCredentialResidue(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "internal", "db")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	files := map[string]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			t.Fatalf("reading %s: %v", e.Name(), rerr)
		}
		files[e.Name()] = string(b)
	}
	// Anti-decorative guard: a walk that resolves nothing would pass
	// every check below while the residue class it pins is wide open.
	srctest.MinCount(t, "internal/db test files scanned", len(files), 30)

	var sawCreateRole bool
	// Needles are assembled from parts throughout so this file — which
	// necessarily NAMES every banned shape — cannot match its own checks.
	// (?s) + non-greedy so a CREATE ROLE whose PASSWORD sits on a later
	// LINE is still seen — `.` does not match a newline by default, so
	// the first form was blind to every multi-line creation (L10 minor).
	createRe := regexp.MustCompile("(?s)CREATE " + "ROLE .*?PASSWORD '[^']*'")
	for rel, raw := range files {
		src := srctest.StripGoComments(raw)

		// (a) No password constant may reach a CREATE ROLE.
		for _, m := range createRe.FindAllString(src, -1) {
			if !strings.Contains(m, "PASSWORD '%s'") {
				t.Errorf("%s: CREATE ROLE must take its password as a parameter, not inline: %q", rel, m)
			}
		}
		if strings.Contains(src, "const "+`pw = "`) {
			t.Errorf("%s: a scratch-role password must be generated per run (randomScratchPassword), never a\n"+
				"literal in the repository — a role is a CLUSTER object, so a leaked one is a login-capable\n"+
				"account whose credential anyone can read here (round-11 finding 6)", rel)
		}

		// (b) Every file that creates roles must sweep prior runs' roles
		// and drop its own through the CHECKED helper.
		if !createRe.MatchString(src) {
			continue
		}
		sawCreateRole = true
		for _, need := range []string{"sweepStaleTestRoles(", "cleanupScratchRole(", "randomScratchPassword("} {
			if !strings.Contains(src, need) {
				t.Errorf("%s creates database roles but does not call %s — t.Cleanup alone cannot bound\n"+
					"cluster-object residue (a SIGKILL or package timeout skips it), and an unchecked DROP\n"+
					"cannot report the leak it just caused (round-11 finding 6)", rel, need)
			}
		}
		// PER SITE, not per file (L10 minor — the counting-pins lesson
		// this repo has now learned four times): a fourth CREATE ROLE
		// added to a file that already carries one compliant site passed
		// the needles above with no cleanup of its own. Every creation
		// must have its own registration.
		if creates, cleanups := len(createRe.FindAllString(src, -1)), strings.Count(src, "cleanupScratchRole("); cleanups < creates {
			t.Errorf("%s has %d password-bearing CREATE ROLE statements but only %d cleanupScratchRole registrations —\n"+
				"a creation site without its own CHECKED cleanup leaks a login-capable CLUSTER object that survives\n"+
				"DROP DATABASE, and the per-file needles above cannot see it (round-11 finding 6, L10 minor)", rel, creates, cleanups)
		}
		// The unchecked form must be gone. The needle is assembled so
		// this file cannot match its own check.
		uncheckedDrop := "_, _ = admin.pool.Exec(cctx, " + "`" + "DROP " + "ROLE"
		if strings.Contains(src, uncheckedDrop) {
			t.Errorf("%s: a discarded DROP ROLE error is how a leaked cluster role stays invisible", rel)
		}
	}
	if !sawCreateRole {
		t.Fatal("no CREATE ROLE site found in internal/db's tests — this tripwire pins the residue\n" +
			"contract of the least-privilege-viewer fixtures (round-7/8); if they were removed, remove\n" +
			"this test with them rather than leaving it passing over nothing.")
	}
}

// L10 finding 1 (the round-11 fix's own defect). The sweep dropped EVERY
// `avtest_%` role in the cluster on entry, justified by a premise
// PostgreSQL does not hold: both comments claimed a role a concurrent
// run still holds open "legitimately refuses to drop." It does not.
// Verified against a live PostgreSQL — with a session logged in as the
// role, `DROP OWNED BY` and `DROP ROLE IF EXISTS` BOTH succeed and the
// role is gone.
//
// Roles are cluster-wide, so the blast radius is not two `go test`
// invocations against one database: it is any two scratch databases on
// one cluster (this machine carries `aveloxis_cascade_test` and
// `aveloxis_ci_repro` side by side). Run A is mid-test with its viewer
// connected; run B's sweep drops A's role out from under it; A's own
// CHECKED cleanup then fails "role does not exist" and t.Errorf's — so
// the INNOCENT run fails, accused of the leak run B caused.
//
// Age is the only sound discriminator, and it is derivable rather than
// picked: a role whose nonce is younger than the package's own
// `-timeout` may still belong to a live run, and one older than it
// cannot (whatever created it has already been killed by that timeout,
// or finished and cleaned up).
func TestScratchRoleAgeParsesTheNonce(t *testing.T) {
	fresh := scratchRoleName("viewer")
	now := time.Now()
	age, ok := scratchRoleAge(fresh, now)
	if !ok {
		t.Fatalf("scratchRoleAge(%q) must parse a name scratchRoleName just built", fresh)
	}
	if age < 0 || age > time.Minute {
		t.Errorf("a just-minted role must read as ~0 old, got %v", age)
	}

	old := fmt.Sprintf("%sviewer_%d", scratchRolePrefix, now.Add(-3*time.Hour).UnixNano())
	age, ok = scratchRoleAge(old, now)
	if !ok || age < 2*time.Hour {
		t.Errorf("scratchRoleAge(%q) = (%v, %v), want a ~3h age", old, age, ok)
	}

	// A name whose suffix is not a nonce must NOT be assigned an age —
	// treating it as ancient would drop it, which is the very failure
	// this finding is about.
	for _, bad := range []string{scratchRolePrefix + "viewer", scratchRolePrefix + "viewer_notanonce", scratchRolePrefix + "viewer_", "avtest_"} {
		if _, ok := scratchRoleAge(bad, now); ok {
			t.Errorf("scratchRoleAge(%q) must report unparseable, not an age", bad)
		}
	}
}

// The behavioral half: a role named as a CONCURRENT run would name it
// survives the sweep; a backdated one is dropped. This is the test that
// fails against the pre-L10 unconditional sweep.
func TestStaleRoleSweepSparesAConcurrentRunsRole(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	admin, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	cutoff, ok := staleRoleCutoff()
	if !ok {
		t.Skip("no package -timeout, so the sweep has no sound cutoff and drops nothing")
	}

	// A role a concurrent run would have just created.
	live := scratchRoleName("concurrentprobe")
	// A role a run killed long ago left behind: the same shape, backdated
	// past the cutoff.
	stale := fmt.Sprintf("%sstaleprobe_%d", scratchRolePrefix, time.Now().Add(-2*cutoff).UnixNano())
	for _, r := range []string{live, stale} {
		if _, err := admin.pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", r)); err != nil {
			t.Skipf("cannot create scratch roles as this user (%v)", err)
		}
		role := r
		t.Cleanup(func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer ccancel()
			_ = dropScratchRole(cctx, admin, role)
		})
	}

	sweepStaleTestRoles(ctx, t, admin)

	exists := func(role string) bool {
		var n int
		if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname = $1`, role).Scan(&n); err != nil {
			t.Fatalf("counting %s: %v", role, err)
		}
		return n > 0
	}
	if !exists(live) {
		t.Errorf("the sweep dropped %s — a role young enough to belong to a CONCURRENT run. "+
			"PostgreSQL drops a role out from under a live session without complaint, so the "+
			"innocent run then fails its own checked cleanup with \"role does not exist\"", live)
	}
	if exists(stale) {
		t.Errorf("the sweep left %s — a role older than the package -timeout provably belongs to a "+
			"finished run, and draining those is the whole point of the sweep", stale)
	}
}
