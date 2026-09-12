// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The pure decision behind the serve-startup gate. Only a SERVE start
// (fast path armed) that missed the stamp reaches it; `aveloxis
// migrate` never does. Another aveloxis-serve connected + a stamp that
// is not this binary's (or unreadable — a probe error is not "no other
// serve's schema is fine") refuses; no other serve proceeds to the
// full run exactly as before.
func TestStartupMigrateRefusalDecision(t *testing.T) {
	probeErr := errors.New("boom")
	cases := []struct {
		name       string
		stamp      string
		stampErr   error
		otherServe bool
		refuse     bool
	}{
		{"mismatch, another serve", "0.29.2", nil, true, true},
		{"unstamped, another serve", "", nil, true, true},
		{"probe error, another serve", "", probeErr, true, true},
		{"mismatch, alone", "0.29.2", nil, false, false},
		{"probe error, alone", "", probeErr, false, false},
	}
	for _, c := range cases {
		sight := OtherServe{Connected: c.otherServe}
		if c.otherServe {
			sight.From = []string{"10.0.0.5 (other address)"}
		}
		err := startupMigrateRefusal(c.stamp, c.stampErr, sight)
		if c.refuse != (err != nil) {
			t.Errorf("%s: refusal = %v, want refuse=%v", c.name, err, c.refuse)
			continue
		}
		if !c.refuse {
			continue
		}
		if !errors.Is(err, ErrOtherServeConnected) {
			t.Errorf("%s: must wrap ErrOtherServeConnected, got %v", c.name, err)
		}
		msg := err.Error()
		// Round-5 finding 5: the operator needs the other serve's address
		// to tell the primary from a lingering backend of a serve just
		// stopped on this very host.
		for _, needle := range []string{"aveloxis start scancode-worker", "aveloxis stop all", "aveloxis migrate --skip-views", ToolVersion, "10.0.0.5"} {
			if !strings.Contains(msg, needle) {
				t.Errorf("%s: the refusal must tell the operator %q, got %q", c.name, needle, msg)
			}
		}
		if c.stampErr != nil && !strings.Contains(msg, c.stampErr.Error()) {
			t.Errorf("%s: a probe error must be visible in the refusal, got %q", c.name, msg)
		}
		if c.stamp != "" && !strings.Contains(msg, c.stamp) {
			t.Errorf("%s: the stamp must be visible in the refusal, got %q", c.name, msg)
		}
	}
}

// The incident, end to end: a serve on a second host whose binary does
// not match the stamp started a FULL migration beside the primary's
// live serve (base DDL deadlocked 3x against 120 workers). With a fake
// aveloxis-serve session connected and the stamp moved off the binary,
// a fast-path (serve) RunMigrations must refuse with the typed error
// and run NO step — a dropped migration-owned index stays absent.
func TestServeStartupMigrateRefusesBesideAnotherServe(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	// The store under test is tagged exactly as runServe tags its pool,
	// so the probe must exclude OUR backends by server PID (the tag
	// alone would count us as "another serve") and count only the fake.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	store, err := NewPostgresStore(ctx, dsn+sep+"application_name="+ServeApplicationName, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)
	if v := store.GetSchemaVersion(ctx); v != ToolVersion {
		t.Fatalf("precondition: stamp %q != ToolVersion %q", v, ToolVersion)
	}

	// Hold the migrate advisory lock for the whole window (the v0.27.149
	// fast-path test's pattern, try-lock POLLING per v0.27.20): a
	// concurrent full migration from another package would otherwise
	// re-stamp ToolVersion mid-test and turn the refusal into a fast-path
	// skip. The refusal under test sits BEFORE the lock, so no
	// self-deadlock.
	lockConn, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lockHeld := true
	unlock := func() {
		if !lockHeld {
			return
		}
		lockHeld = false
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, MigrateAdvisoryLockID)
		lockConn.Release()
	}
	defer unlock()
	for {
		var got bool
		if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, MigrateAdvisoryLockID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}

	// The "other serve": a second pool from this process tagged the way
	// runServe tags its pool. Its backends are NOT in store's own-PID set,
	// so the probe counts it exactly as it would count the primary.
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = ServeApplicationName
	otherServe, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherServe.Close)
	if err := otherServe.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	// Round-4 finding 3: with the stamp CURRENT the fast path still
	// proceeds beside another serve — a same-version second serve is not
	// refused (whether it should be is the operator's call; the docs say
	// so) — but it must SAY so, because the dedicated-host docs pin the
	// worker to the primary's version and `start serve` there is the
	// incident with the version drift removed.
	// Round-6 finding 1: the fake's OWN address (as the server renders
	// it) must appear — `from=` alone was satisfied by an empty list.
	var fakeAddr string
	if err := otherServe.QueryRow(ctx, `SELECT COALESCE(host(client_addr), 'local socket') FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&fakeAddr); err != nil {
		t.Fatal(err)
	}
	// 2026-09-11 (F3): the residual is CLOSED by default. A second host
	// ran a full aveloxis stack — serve, api, web and 13 migrate runs —
	// against production for ten days and deadlocked the live serve's
	// staging inserts against its schema DDL, and the only thing standing
	// between that and the fleet was a WARN in a log nobody was reading.
	// So a same-version second serve now REFUSES unless the operator says
	// otherwise, and the refusal has to name the override or it is just an
	// obstacle.
	var refusedLog bytes.Buffer
	store.SetMigrateFastPath(true)
	refuseErr := RunMigrations(ctx, store, slog.New(slog.NewTextHandler(&refusedLog, nil)))
	if refuseErr == nil {
		t.Fatal("a current stamp beside another serve must REFUSE by default — a second full " +
			"scheduler competes for the same queue and API keys, and the 2026-08-30..09-09 " +
			"incident shows a WARN does not stop it")
	}
	if !errors.Is(refuseErr, ErrSecondServeRefused) {
		t.Errorf("the refusal must wrap ErrSecondServeRefused so callers can tell it from a "+
			"migration failure, got %v", refuseErr)
	}
	// Distinct from the stamp-mismatch refusal: that one exists because a
	// FULL migration beside a live serve caused the base-DDL deadlock.
	// Conflating them would let --allow-second-serve also wave through a
	// concurrent full migration, which is the incident itself.
	if errors.Is(refuseErr, ErrOtherServeConnected) {
		t.Error("the second-serve refusal must NOT wrap ErrOtherServeConnected — that sentinel is " +
			"the stamp-mismatch refusal (a full migration beside a live serve), and the override " +
			"for this one must never widen to that one")
	}
	// A refusal BLOCKS, so it owes BOTH ways forward. Advice() has just
	// said a "(this host)" entry may be a draining backend rather than a
	// live serve — and for a serve that was KILLED rather than stopped,
	// whose backends linger on TCP keepalive for tens of minutes,
	// --allow-second-serve is the WRONG remedy: it starts a second
	// scheduler beside a corpse instead of clearing the corpse. A
	// blocking message that offers only the override walks the operator
	// into that.
	for _, want := range []string{"--allow-second-serve", "orphan"} {
		if !strings.Contains(refuseErr.Error(), want) {
			t.Errorf("the refusal must name %q. It BLOCKS, so an operator hitting it on a routine "+
				"restart needs every way forward — the override for a deliberate second serve, and "+
				"terminating the orphan when the entry is a killed serve's leftover backend: %v", want, refuseErr)
		}
	}

	// With the override, the pre-2026-09-11 behaviour is reachable
	// verbatim — including every message the rounds 4-11 findings put
	// into it.
	var sameVersionLog bytes.Buffer
	store.SetAllowSecondServe(true)
	t.Cleanup(func() { store.SetAllowSecondServe(false) })
	if err := RunMigrations(ctx, store, slog.New(slog.NewTextHandler(&sameVersionLog, nil))); err != nil {
		t.Fatalf("with --allow-second-serve a current stamp beside another serve must fast-path, got %v", err)
	}
	// Round-11 finding 1: the fake runs on THIS host, so every entry the
	// listing renders is tagged "(this host)" and Advice() WITHDRAWS the
	// "normally the primary, so this host is running the wrong command"
	// reading — nothing in the list identifies a primary, and the
	// reverse-chair case (a runner started serve first, the primary came
	// back, and the PRIMARY is the entry tagged "(other address)") is
	// exactly what that inference gets wrong. What the WARN must still
	// carry: the sighting, the address as the SERVER renders it
	// (round-6 finding 1 — `from=` alone was satisfied by an empty
	// list), and the way to back out of a serve that is starting anyway
	// (round-10 finding 2).
	for _, want := range []string{
		"another aveloxis-serve is connected",
		"nothing here identifies the primary",
		"aveloxis stop serve",
		fakeAddr + " (this host)",
	} {
		if !strings.Contains(sameVersionLog.String(), want) {
			t.Errorf("the fast path beside another serve must warn, name the way out, and report %q with the code's own verdict:\n%s", want, sameVersionLog.String())
		}
	}
	// The counter-pin, live: with no "(other address)" entry the verdict
	// and its command form must be ABSENT. This e2e is the only place
	// the withdrawal is proven against a real listing rather than a
	// hand-built OtherServe value.
	if strings.Contains(sameVersionLog.String(), "aveloxis start scancode-worker") {
		t.Errorf("with every entry tagged (this host) the wrong-command verdict must be withdrawn, not rendered:\n%s", sameVersionLog.String())
	}
	// Round-7 finding 3: the "(other address)" verdict — the datum every
	// message tells the operator to act on — was never executed (one
	// host cannot produce it; a constant "(this host)" kept the suite
	// green). Driven through the seam: seen from a LAN address, the
	// fake's own address is another address's.
	lan := "192.168.99.99"
	far, err := store.otherServeAddressesFrom(ctx, &lan)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(far, fakeAddr+" (other address)") || slices.Contains(far, fakeAddr+" (this host)") {
		t.Errorf("seen from %s the fake serve must be tagged (other address), got %v", lan, far)
	}

	// Move the stamp off this binary; restore whatever happens.
	const bogus = "0.0.0-avotherserve"
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.schema_meta SET schema_version = $1 WHERE id = TRUE`, bogus)
	restore := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `UPDATE aveloxis_ops.schema_meta SET schema_version = $1 WHERE id = TRUE AND schema_version = $2`, ToolVersion, bogus)
	}
	t.Cleanup(restore)

	const probeIdx = "idx_repos_added_at"
	mustExecRetry(ctx, t, store, `DROP INDEX IF EXISTS aveloxis_data.`+probeIdx)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer ccancel()
		heal, herr := NewPostgresStore(cctx, dsn, quiet)
		if herr != nil {
			return
		}
		defer heal.Close()
		heal.SetMatviewSkip(true)
		_ = RunMigrations(cctx, heal, quiet)
	})
	indexExists := func() bool {
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname='aveloxis_data' AND indexname=$1`, probeIdx).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	if indexExists() {
		t.Fatal("precondition: probe index should be dropped")
	}

	// A mutant that proceeds past the gate polls the migrate lock THIS
	// test holds: bound the call so it fails here instead of hanging to
	// the package timeout. The refusal is a stamp read plus one
	// pg_stat_activity probe (milliseconds; the fast-path sibling test
	// caps a stamp read at 5 s) — 30 s is headroom under a loaded
	// combined run, not a budget the refusal ever approaches.
	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	store.SetMigrateFastPath(true)
	err = RunMigrations(rctx, store, quiet)
	if !errors.Is(err, ErrOtherServeConnected) {
		t.Fatalf("a serve startup beside another serve with a stale stamp must refuse with ErrOtherServeConnected, got %v", err)
	}
	if !strings.Contains(err.Error(), fakeAddr+" (this host)") {
		t.Errorf("the refusal must name the other serve's address with the code's verdict, got %q", err.Error())
	}
	if indexExists() {
		t.Error("the refusal must run NO migration step — the probe index was recreated")
	}
	if v := store.GetSchemaVersion(ctx); v != bogus {
		t.Errorf("the refusal must not stamp: stamp = %q", v)
	}

	// Round-2 finding 4: `aveloxis migrate` (fast path OFF) keeps its
	// documented beside-a-live-serve behavior — with the fake serve STILL
	// connected and the stamp still off, the full run proceeds. This is
	// the behavioral half of the "gate inside the fast-path block" pin:
	// hoisting the gate past the block's closing brace fails here.
	unlock()
	store.SetMigrateFastPath(false)
	if err := RunMigrations(ctx, store, quiet); err != nil {
		t.Fatalf("`aveloxis migrate` beside a live serve must not be refused by the serve-startup gate: %v", err)
	}
	if !indexExists() || store.GetSchemaVersion(ctx) != ToolVersion {
		t.Fatal("the fast-path-off run must heal the index and re-stamp")
	}

	// Alone: the same fast-path (serve) call proceeds once the other
	// serve is gone — the gate keys on the other serve, not the mismatch.
	// Backend teardown is asynchronous (the sibling list-dedup e2e waits
	// the same way): wait until the probe itself reads clean.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.schema_meta SET schema_version = $1 WHERE id = TRUE`, bogus)
	mustExecRetry(ctx, t, store, `DROP INDEX IF EXISTS aveloxis_data.`+probeIdx)
	otherServe.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		other, err := store.otherServeConnected(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !other {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fake serve's backends did not tear down within 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	store.SetMigrateFastPath(true)
	if err := RunMigrations(ctx, store, quiet); err != nil {
		t.Fatalf("alone, the stale-stamp serve start must run the full migration: %v", err)
	}
	if !indexExists() {
		t.Error("the full run must heal the dropped index")
	}
	if v := store.GetSchemaVersion(ctx); v != ToolVersion {
		t.Errorf("the full run must re-stamp ToolVersion, got %q", v)
	}
}

// Wiring: the gate lives INSIDE the serve-only fast-path block of
// RunMigrations, after the stamp read and before "running schema
// migrations" — so `aveloxis migrate` (fast path off) never consults
// it and keeps its documented beside-a-live-serve behavior.
func TestServeStartupMigrateGateIsInsideFastPathBlock(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func RunMigrations("))
	fastPath := strings.Index(body, "if pg.migrateFastPath {")
	gate := strings.Index(body, "startupMigrateRefusal(")
	running := strings.Index(body, `"running schema migrations"`)
	if fastPath < 0 || gate < 0 || running < 0 {
		t.Fatalf("anchors: fastPath=%d gate=%d running=%d", fastPath, gate, running)
	}
	if !(fastPath < gate && gate < running) {
		t.Errorf("the gate must sit inside the fast-path block before the full run starts: fastPath=%d gate=%d running=%d", fastPath, gate, running)
	}
	if n := strings.Count(body, "startupMigrateRefusal("); n != 1 {
		t.Errorf("exactly one gate call in RunMigrations, found %d", n)
	}
	if n := strings.Count(body, "otherServeSighting("); n != 1 {
		t.Errorf("exactly one other-serve sighting in RunMigrations, found %d", n)
	}
	sighting := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) otherServeSighting("))
	if strings.Count(sighting, "otherServeConnected(") != 1 || strings.Count(sighting, "otherServeAddresses(") != 1 {
		t.Error("otherServeSighting is the ONE confirm-then-list composition (SR-17): the probe and the listing each exactly once inside it")
	}
	if strings.Contains(body, "otherServeAddresses(") || strings.Contains(body, "otherServeConnected(") {
		t.Error("RunMigrations must not compose the probe and the listing itself")
	}
	// The stamp is read ONCE with its error arm kept (SR-5): the fast
	// path and the gate share the probe result.
	if strings.Contains(body, "GetSchemaVersion(") {
		t.Error("RunMigrations must read the stamp through schemaVersionProbe (error arm kept), not GetSchemaVersion")
	}
	probe := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) otherServeConnected("))
	if !strings.Contains(probe, "serveBackendsBeyondOwnPool(") || !strings.Contains(probe, "s.ownBackendPIDs") {
		t.Error("otherServeConnected must reuse serveBackendsBeyondOwnPool with the pool's own-PID set (SR-17 — one probe)")
	}
}

// Round-2 finding 1 (reproduced by the reviewer at 1-3% per fresh pool):
// a serve's OWN pool constructs connections in the background (MinConns
// at creation, refills later); such a backend is visible in
// pg_stat_activity from its startup packet but joins backendPIDs only
// when AfterConnect fires after the handshake — so one probe can count
// our own in-flight connection as "another serve" and refuse a
// single-host restart. A positive is therefore CONFIRMED by re-probing:
// an own connection registers within a handshake, a real serve persists.
func TestConfirmOtherServeAbsorbsOwnInFlightConnection(t *testing.T) {
	run := func(seq []bool, err error) (bool, int, error) {
		i, sleeps := 0, 0
		probe := func() (bool, error) {
			if err != nil && i == len(seq) {
				return false, err
			}
			v := seq[i]
			i++
			return v, nil
		}
		other, perr := confirmOtherServe(probe, 4, func() { sleeps++ })
		return other, sleeps, perr
	}
	if other, sleeps, err := run([]bool{true, false}, nil); other || err != nil || sleeps != 1 {
		t.Errorf("a positive that clears on re-probe is our own connection: other=%v err=%v sleeps=%d", other, err, sleeps)
	}
	if other, sleeps, err := run([]bool{true, true, true, true}, nil); !other || err != nil || sleeps != 3 {
		t.Errorf("a positive on every attempt is a real serve: other=%v err=%v sleeps=%d", other, err, sleeps)
	}
	if other, sleeps, err := run([]bool{false}, nil); other || err != nil || sleeps != 0 {
		t.Errorf("a negative needs no confirmation: other=%v err=%v sleeps=%d", other, err, sleeps)
	}
	boom := errors.New("boom")
	if _, _, err := run([]bool{true}, boom); !errors.Is(err, boom) {
		t.Errorf("a probe error during confirmation must surface (SR-5), got %v", err)
	}
	if otherServeConfirmAttempts < 2 {
		t.Errorf("confirmation needs at least one re-probe, attempts = %d", otherServeConfirmAttempts)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) otherServeConnected("))
	if n := strings.Count(body, "confirmOtherServe("); n != 1 {
		t.Errorf("otherServeConnected must confirm through confirmOtherServe exactly once, found %d", n)
	}
	for _, needle := range []string{"otherServeConfirmAttempts", "otherServeConfirmInterval"} {
		if !strings.Contains(body, needle) {
			t.Errorf("otherServeConnected must use the named bound %s", needle)
		}
	}
}

// Round-3 finding 1: the confirmation's premise is that the own-PID set
// is re-snapshotted on EVERY probe (a method value, never a hoisted
// slice) — our own in-flight backend joins backendPIDs after probe 1 and
// must be excluded by probe 2. Driven for real: a serve-tagged backend
// this store does not own reads as another serve on probe 1; after that
// probe the test registers it as ours; the confirmation must clear.
func TestOtherServeConfirmationReSnapshotsOwnPIDs(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = ServeApplicationName
	cfg.MaxConns = 1
	fake, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)
	var fakePID uint32
	if err := fake.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&fakePID); err != nil {
		t.Fatal(err)
	}

	// Sanity: unregistered, the fake persists through the whole
	// confirmation and refuses (this is a real other serve's shape).
	if other, err := store.otherServeConnected(ctx); err != nil || !other {
		t.Fatalf("precondition: a persistent foreign serve backend must confirm: other=%v err=%v", other, err)
	}

	// Register it as OURS right after the first probe — exactly what
	// AfterConnect does for a MinConns connection mid-confirmation.
	probes := 0
	otherServeProbeHook = func(attempt int) {
		probes = attempt
		if attempt == 1 {
			store.trackBackend(fakePID, true)
		}
	}
	t.Cleanup(func() {
		otherServeProbeHook = nil
		store.trackBackend(fakePID, false)
	})
	other, err := store.otherServeConnected(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if other {
		t.Fatalf("a backend that joins the own-PID set during confirmation must clear it (probes=%d) — the set must be re-snapshotted per probe", probes)
	}
	if probes != 2 {
		t.Errorf("the clear must land on the very next probe, got %d", probes)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) otherServeConnected("))
	if !srctest.ContainsNormalized(body, "serveBackendsBeyondOwnPool(ctx, conn, s.ownBackendPIDs)") {
		t.Error("the probe must pass s.ownBackendPIDs as a METHOD VALUE (re-read every probe), never a hoisted snapshot")
	}
}

// Round-5 finding 8 (L14): a shutdown that lands inside the startup
// probe is an interruption, never a refusal or a probe failure —
// nothing is logged at ERROR and the returned error is the cancellation.
func TestServeStartupCanceledIsNotARefusal(t *testing.T) {
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
	store.SetMigrateFastPath(true)
	dead, kill := context.WithCancel(ctx)
	kill()
	var buf bytes.Buffer
	err = RunMigrations(dead, store, slog.New(slog.NewTextHandler(&buf, nil)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled startup must surface the cancellation, got %v", err)
	}
	if strings.Contains(buf.String(), "level=ERROR") || strings.Contains(buf.String(), "refus") {
		t.Errorf("a shutdown is not a refusal:\n%s", buf.String())
	}
	// Round 7 (nit): the interruption carries the context's OWN cause —
	// a deadline is not reported as a cancellation.
	expired, expire := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer expire()
	buf.Reset()
	err = RunMigrations(expired, store, slog.New(slog.NewTextHandler(&buf, nil)))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("an expired startup must surface DeadlineExceeded, got %v", err)
	}
	if strings.Contains(buf.String(), "level=ERROR") || strings.Contains(buf.String(), "refus") {
		t.Errorf("a deadline is not a refusal:\n%s", buf.String())
	}
}

// Round-6 findings 3 and 4: ONE rendering of "where the other serve
// connects from" (SR-17) — the addresses with the code's own this-host
// verdict, or an honest "unrecorded" when the listing failed, never an
// empty "[]".
func TestOtherServeDescribeIsHonest(t *testing.T) {
	got := OtherServe{Connected: true, From: []string{"127.0.0.1 (this host)", "192.168.1.9 (other address)"}}.Describe()
	if !strings.Contains(got, "127.0.0.1 (this host)") || !strings.Contains(got, "192.168.1.9 (other address)") {
		t.Errorf("addresses must be rendered with their verdicts: %q", got)
	}
	got = OtherServe{Connected: true, ListErr: errors.New("boom")}.Describe()
	if !strings.Contains(got, "unrecorded address") || !strings.Contains(got, "boom") || strings.Contains(got, "[]") {
		t.Errorf("a failed listing must be said, never rendered as []: %q", got)
	}
	got = OtherServe{Connected: true}.Describe()
	if !strings.Contains(got, "unrecorded address") || strings.Contains(got, "[]") {
		t.Errorf("no address and no error still reads honestly: %q", got)
	}
	// Round 7 (nit): Describe renders WHERE, the callers say "from" — the
	// fast-path attr used to read from="from ::1 (this host)".
	for _, o := range []OtherServe{{Connected: true, From: []string{"::1 (this host)"}}, {Connected: true, ListErr: errors.New("boom")}, {Connected: true}} {
		if strings.HasPrefix(o.Describe(), "from ") {
			t.Errorf("Describe must not carry the preposition, got %q", o.Describe())
		}
	}
	src := srctest.Read(t, "internal/db/migrate.go")
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, src, "func startupMigrateRefusal(")), "(from %s)") {
		t.Error("the refusal must say `from` before Describe()")
	}
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, src, "func RunMigrations(")), `"from", sight.Describe()`) {
		t.Error("the fast-path WARN keys the sighting as from=")
	}
}

// Round-6 finding 7 (L17a): the stamp comparator treats a malformed
// version as "not at least", which the deploy gate reads as REFUSE —
// a non-numeric ToolVersion (an rc suffix) would refuse every
// existing-fleet start as an unrun deploy step.
func TestToolVersionIsComparable(t *testing.T) {
	if !SchemaVersionAtLeast(ToolVersion, ToolVersion) {
		t.Fatalf("ToolVersion %q must be dotted-numeric — the deploy gate compares it with SchemaVersionAtLeast", ToolVersion)
	}
}

// Round-6 finding 2 (L14): a cancellation landing inside the ADDRESS
// listing (one round-trip after a confirmed positive) is an
// interruption too — not a "could not list" warning followed by a
// refusal or a fast path on a dead context. The hook cancels the
// context right after the last confirmation probe, so the listing is
// the first statement to see it.
func TestServeStartupCanceledInsideListingIsNotARefusal(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = ServeApplicationName
	cfg.MaxConns = 1
	fake, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)
	if err := fake.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	dead, kill := context.WithCancel(ctx)
	defer kill()
	otherServeProbeHook = func(attempt int) {
		if attempt == otherServeConfirmAttempts {
			kill()
		}
	}
	t.Cleanup(func() { otherServeProbeHook = nil })
	store.SetMigrateFastPath(true)
	var buf bytes.Buffer
	err = RunMigrations(dead, store, slog.New(slog.NewTextHandler(&buf, nil)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancellation inside the listing must surface as the cancellation, got %v", err)
	}
	if strings.Contains(buf.String(), "level=ERROR") || strings.Contains(buf.String(), "refus") || strings.Contains(buf.String(), "could not list") {
		t.Errorf("a shutdown is neither a refusal nor a listing failure:\n%s", buf.String())
	}
}

// Round-7 finding 2 (L13/L10; mutation-proved by the reviewer): the
// exported gate entry point had no pin — rewriting OtherServeConnected
// to compose probe + listing itself and fold a listing failure into an
// error (the round-6 finding-3 defect) left every test green. The
// composition has ONE owner, derived over the package: only
// otherServeSighting may call the probe and the listing, only the
// listing wrapper may call the address seam, and the exported entry is
// the wrapper's return.
func TestOtherServeCompositionHasOneOwner(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	if !srctest.ContainsNormalized(srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) OtherServeConnected(")), "{ return s.otherServeSighting(ctx) }") {
		t.Error("OtherServeConnected must be exactly `return s.otherServeSighting(ctx)`")
	}
	if !srctest.ContainsNormalized(srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) otherServeAddresses(")), "{ return s.otherServeAddressesFrom(ctx, nil) }") {
		t.Error("otherServeAddresses must list from the session's REAL address (nil override); only tests drive the seam")
	}
	owners := map[string][]string{
		"otherServeConnected(":     {"func (s *PostgresStore) otherServeConnected(", "func (s *PostgresStore) otherServeSighting("},
		"otherServeAddresses(":     {"func (s *PostgresStore) otherServeAddresses(", "func (s *PostgresStore) otherServeSighting("},
		"otherServeAddressesFrom(": {"func (s *PostgresStore) otherServeAddressesFrom(", "func (s *PostgresStore) otherServeAddresses("},
	}
	for name, fsrc := range srctest.PackageFiles(t, "internal/db", 30) {
		for _, sig := range pgStatActivityReaderSigs(fsrc) {
			body := srctest.StripGoComments(srctest.FuncBody(t, fsrc, sig))
			for call, allowed := range owners {
				if strings.Contains(body, call) && !slices.Contains(allowed, sig) {
					t.Errorf("%s %s calls %s — only %v may (the ONE composition, SR-17)", name, sig, call, allowed)
				}
			}
		}
	}
	// Round-7 finding 4 (L1/L9): a listing that fails mid-iteration must
	// not hand back the partial list beside the error — Describe prefers
	// the list, so the refusal, the WARN and the gate note would render a
	// truncated address list with no "(listing failed)".
	listing := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) otherServeAddressesFrom("))
	if !srctest.ContainsNormalized(listing, "if err := rows.Err(); err != nil { return nil, err }") || strings.Contains(listing, "return out, rows.Err()") {
		t.Error("otherServeAddressesFrom must return NO list on rows.Err() — a partial listing is not a listing")
	}
	if strings.Count(listing, `backendOnThisHostSQL("a")`) != 1 || strings.Count(listing, `clientAddrVisibleSQL("a")`) != 1 || strings.Count(listing, `probingSessionSQL("$3", "$4")`) != 1 {
		t.Error("otherServeAddressesFrom must render its verdict through the shared visibility + this-host predicates and the ONE probing-session subquery")
	}
	// Round-8 finding 5: the own-PID set must be snapshotted AFTER the
	// connection is acquired (a backend is tagged in pg_stat_activity
	// from its startup packet but joins backendPIDs only after
	// AfterConnect). Acquiring first is sufficient for this connection:
	// pgxpool runs AfterConnect as part of establishing one, so a
	// connection Acquire has returned is already registered. A
	// pg_backend_pid() belt is deliberately NOT added — it would be a
	// second spelling of the probing session (SR-17,
	// TestThisHostVerdictHasOneSpelling).
	acquireAt := strings.Index(listing, "s.pool.Acquire(ctx)")
	snapshotAt := strings.Index(listing, "s.ownBackendPIDs()")
	if acquireAt < 0 || snapshotAt < 0 {
		t.Fatalf("otherServeAddressesFrom must acquire its own connection and snapshot the own-PID set (offsets %d/%d)", acquireAt, snapshotAt)
	}
	if acquireAt > snapshotAt {
		t.Error("otherServeAddressesFrom must ACQUIRE before it snapshots the own-PID set — a set read first can miss the connection this query runs on")
	}
	if strings.Contains(listing, "s.pool.Query(") {
		t.Error("otherServeAddressesFrom must query the connection it acquired, not the pool (the acquire is what orders the snapshot)")
	}
}

// v0.29.4 round 9 (SR-17), revised round 10: the operator verdict that
// reads an address tag — (other address) is normally the primary, (this
// host) is a serve here, running or draining — is ONE spelling,
// OtherServe.Advice.
//
// It had already drifted by round 8: the fast-path WARN and the startup
// refusal said `aveloxis scancode-worker` while the deploy-gate note
// said `aveloxis start scancode-worker`. Both are real commands, so
// neither was wrong and nothing caught it — the pins match SUBSTRINGS
// and each pinned what its own site happened to say.
//
// WHAT THIS PIN ENFORCES, stated honestly (round-10 finding 9). The
// round-9 godoc claimed "a FOURTH inline spelling fails the build". A
// substring ban cannot deliver that: a PARAPHRASE of the verdict ships
// green, mutation-proved by the round-10 reviewer. What IS enforced is
// narrower and worth stating exactly:
//
//  1. An exact copy of the verdict's own words outside the renderer
//     fails — the drift shape that actually happened in round 8.
//  2. Each of the THREE verdict sites composes the renderer, checked
//     per site BY NAME. Round 9 counted `.Advice()` occurrences over
//     the tree instead, which any three occurrences anywhere satisfy:
//     a legitimate fourth call site landing while one of the three
//     dropped left the entire unit tier green with the fast-path WARN's
//     verdict gone (round-10 finding 4, mutation-proved).
//  3. The verdict's rendered VALUE carries the one command form, in
//     both of Advice's branches.
func TestOtherServeAdviceIsTheOneVerdictSpelling(t *testing.T) {
	root := srctest.Root(t)

	// Every non-test Go source, read once.
	sources := map[string]string{}
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			sources[path] = string(b)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", top, err)
		}
	}
	// Anti-decorative guard: a walk that resolves nothing would let
	// every check below pass while the contract it pins is gone.
	srctest.MinCount(t, "non-test Go sources scanned", len(sources), 200)

	// Find the renderer by SHAPE, not by path (round-10 findings 5+6).
	// The round-9 pin hard-coded internal/db/migrate.go twice, so
	// relocating the renderer to its own file — a legitimate refactor
	// that changes no behavior — failed first inside srctest.FuncBody
	// and then again with a message accusing the renderer's new home of
	// spelling the verdict inline.
	declRe := regexp.MustCompile(`func \(\w* ?OtherServe\) Advice\(\)`)
	var rendererPath, rendererDecl string
	for path, body := range sources {
		m := declRe.FindString(body)
		if m == "" {
			continue
		}
		if rendererPath != "" {
			t.Fatalf("two OtherServe.Advice declarations (%s and %s) — SR-17 wants exactly one", rendererPath, path)
		}
		rendererPath, rendererDecl = path, m
	}
	if rendererPath == "" {
		t.Fatal("no OtherServe.Advice declaration found — the shared verdict renderer is the whole contract (SR-17)")
	}
	adviceBody := srctest.StripGoComments(srctest.FuncBody(t, sources[rendererPath], rendererDecl))

	// Assert on the RENDERED value, not the source literal (round-10
	// finding 8): the round-9 pin required the literal
	// "aveloxis start scancode-worker" inside the renderer's body, so
	// deriving the command form from a const — a refactor that changes
	// not one byte of output — failed the build.
	tagged := OtherServe{Connected: true, From: []string{"10.0.0.5 (other address)"}}.Advice()
	tagless := OtherServe{Connected: true, ListErr: errors.New("listing failed")}.Advice()
	for _, c := range []struct{ name, got string }{{"tagged", tagged}, {"tagless", tagless}} {
		if !strings.Contains(c.got, "aveloxis start scancode-worker") {
			t.Errorf("the %s verdict must name `aveloxis start scancode-worker` — the mistake it corrects is a MANAGED start (`aveloxis start serve`), so the correction belongs in the same idiom, and it is the form docs/guide/commands.md already uses for this verdict; got %q", c.name, c.got)
		}
		if strings.Contains(strings.ReplaceAll(c.got, "aveloxis start scancode-worker", ""), "aveloxis scancode-worker") {
			t.Errorf("the %s verdict must not ALSO offer the bare `aveloxis scancode-worker` — one verdict, one command form (that split IS the round-8 drift)", c.name)
		}
	}

	// Round-10 finding 3 (L9): the receiver is load-bearing. With no
	// addresses listed, Describe() renders "an unrecorded address" —
	// there are no tags on screen, so a tag legend is advice about
	// output the operator cannot see.
	for _, tag := range []string{"(other address)", "(this host)"} {
		if !strings.Contains(tagged, tag) {
			t.Errorf("the tagged verdict must explain the %s tag Describe() just printed; got %q", tag, tagged)
		}
		if strings.Contains(tagless, tag) {
			t.Errorf("the tagless verdict must not explain the %s tag — Describe() printed no tags for this sighting; got %q", tag, tagless)
		}
	}

	// The three verdict sites, BY NAME. See point 2 of the godoc for
	// why counting occurrences is not equivalent.
	for _, site := range []struct{ file, sig, what string }{
		{"internal/db/migrate.go", "func RunMigrations(", "the serve fast-path WARN"},
		{"internal/db/migrate.go", "func startupMigrateRefusal(", "the startup-migration refusal"},
		{"cmd/aveloxis/deploy_checklist.go", "func checkDeployReadiness(", "the `aveloxis start serve` deploy-gate note"},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, site.file), site.sig))
		if !strings.Contains(body, ".Advice()") {
			t.Errorf("%s (%s in %s) must compose the shared renderer — a verdict site that stops calling Advice() is exactly how the three drifted apart by round 8", site.what, site.sig, site.file)
		}
	}

	// The verdict's own words. Short enough to survive rewording of the
	// surrounding prose, specific enough that no unrelated line matches.
	// The renderer's own body is the one legal home; the budget is an
	// occurrence COUNT rather than an excision so no whitespace boundary
	// can make the carve-out silently swallow a real second spelling.
	const verdictNeedle = "(other address) entry is normally"
	allowedInRenderer := strings.Count(adviceBody, verdictNeedle)
	for path, raw := range sources {
		allowed := 0
		if path == rendererPath {
			allowed = allowedInRenderer
		}
		// Comment-stripped: a doc comment that merely NAMES the verdict
		// is documentation, not a second rendering.
		if got := strings.Count(srctest.StripGoComments(raw), verdictNeedle); got > allowed {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("%s: spells the other-serve verdict inline (%d occurrences, %d allowed) — compose sight.Advice() instead (SR-17: one shared renderer; a second spelling is how the round-8 drift happened)", rel, got, allowed)
		}
	}
}

// Round-10 finding 2 (L9/L12): the fast-path WARN is the ONE verdict
// site where serve CONTINUES. startupMigrateRefusal returns an error
// that exits, and checkDeployReadiness runs before serve has started at
// all — so "stop this serve" is true here and false at the other two,
// which is exactly why it belongs in this caller's TAIL and not in the
// shared renderer (Advice's own godoc: callers own the tail).
//
// The word "stop" lived at this site alone before round 9 — the
// pre-round-9 text read "(stop and use `aveloxis scancode-worker`)" —
// and consolidating the three sites onto the shared renderer dropped
// it, leaving a WARN that names a mistake and no way to back out of it.
func TestFastPathWarnSaysTheServeIsStillStarting(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func RunMigrations("))

	const msgStart = `"another aveloxis-serve is connected to this database`
	i := strings.Index(body, msgStart)
	if i < 0 {
		t.Fatalf("the fast-path WARN is gone from RunMigrations — it is the only place a second same-version serve is named at all (round-4 finding 3)")
	}
	j := strings.Index(body[i:], "sight.Describe())")
	if j < 0 {
		t.Fatalf("could not delimit the fast-path WARN call; it must still report the address via sight.Describe()")
	}
	warn := body[i : i+j]

	for _, want := range []string{"sight.Advice()", "STARTING anyway", "aveloxis stop serve"} {
		if !strings.Contains(warn, want) {
			t.Errorf("the fast-path WARN must carry %q: serve is about to start ANYWAY, so this is the one verdict site where the operator can still back out, and `aveloxis stop serve` finds exactly this process (runServe writes the pidfile before store.Migrate). Got:\n%s", want, warn)
		}
	}

	// Counter-pin: the tail must stay in the caller. "stop this serve"
	// is FALSE at the refusal (it exits) and at the deploy gate (serve
	// has not started), so hoisting it into the shared renderer would
	// put a lie in front of two of the three sites.
	for _, c := range []struct{ name, got string }{
		{"tagged", OtherServe{Connected: true, From: []string{"10.0.0.5 (other address)"}}.Advice()},
		{"tagless", OtherServe{Connected: true, ListErr: errors.New("listing failed")}.Advice()},
		// Round-11 finding 1 added a third branch; the tail must stay in
		// the caller for it too.
		{"all this-host", OtherServe{Connected: true, From: []string{"::1 (this host)"}}.Advice()},
	} {
		if strings.Contains(c.got, "aveloxis stop serve") {
			t.Errorf("OtherServe.Advice (%s branch) must not tell the operator to `aveloxis stop serve` — that is true only where serve is still starting, which is the fast-path WARN's tail, not the shared verdict; got %q", c.name, c.got)
		}
	}
}

// Round-10 finding 8: the code and docs/guide/commands.md render the
// same operator verdict, and the docs' hedged inference chain is the
// authority — the code knows only that a backend connects from a
// DIFFERENT client address (sameClientHostSQL), never which host holds
// the primary, so the consequent must stay inside the hedge.
//
// Scoped to commands.md on purpose: docs/guide/dedicated-scancode-host.md
// states the same reading in its own voice (scare-quoted "wrong command
// on this host"), and pinning the code's exact chain there would fire on
// prose that is not wrong.
func TestOtherServeVerdictMatchesTheDocs(t *testing.T) {
	// The whole inference chain, hedge and consequent and command form
	// together — the three halves that drifted apart across rounds 8-10.
	const chain = "entry is normally the primary, so this host is running the wrong command (`aveloxis start scancode-worker` is the alternative)"

	rendered := OtherServe{Connected: true, From: []string{"10.0.0.5 (other address)"}}.Advice()
	if !srctest.ContainsNormalized(rendered, chain) {
		t.Errorf("OtherServe.Advice must render the docs' inference chain %q; got %q", chain, rendered)
	}
	// Whitespace-normalized: the doc is hard-wrapped, so the chain spans
	// line breaks there and would never match byte-for-byte.
	if !srctest.ContainsNormalized(srctest.Read(t, "docs/guide/commands.md"), chain) {
		t.Errorf("docs/guide/commands.md must state the same verdict as OtherServe.Advice, chain %q — code and docs describing one operator decision differently is the drift SR-17 exists to prevent", chain)
	}
}

// Round-11 finding 8: the confirmation loop runs ~1.75 s on a positive.
// Holding a TRANSACTION across it sits idle-in-transaction pinning xmin
// on every affected serve start, and a cancellation leaves
// Rollback(dead ctx) failing so pgx destroys the pooled connection. The
// probe needs one SESSION, not a transaction — Acquire gives that.
func TestOtherServeProbeAcquiresRatherThanBegins(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func (s *PostgresStore) otherServeConnected("))
	if !strings.Contains(body, "s.pool.Acquire(ctx)") || !strings.Contains(body, "conn.Release()") {
		t.Error("otherServeConnected must Acquire a connection (and Release it) for the confirmation loop")
	}
	if strings.Contains(body, "s.pool.Begin(") || strings.Contains(body, "tx.Rollback(") {
		t.Error("otherServeConnected must NOT hold a transaction across the ~1.75s confirmation loop (round-11 finding 8: idle-in-transaction pins xmin on every positive start)")
	}
}

// TestAllowSecondServeIsWiredAndNarrow pins the 2026-09-11 (F3)
// override's plumbing and — more importantly — its NARROWNESS.
//
// The refusal it overrides is "another scheduler is already running
// here". The refusal it must NOT reach is startupMigrateRefusal: a serve
// whose binary missed the stamp would run a FULL migration beside a live
// fleet, and that is the base-DDL deadlock of the 2026-09-09 incident
// itself. Widening the override to cover both would re-enable the
// incident through the front door, which is why the two conditions carry
// separate sentinels.
func TestAllowSecondServeIsWiredAndNarrow(t *testing.T) {
	pg := srctest.Read(t, "internal/db/postgres.go")
	if !strings.Contains(pg, "func (s *PostgresStore) SetAllowSecondServe(") {
		t.Error("PostgresStore must expose SetAllowSecondServe (the SetMatviewSkip / SetMigrateFastPath pattern)")
	}

	main := srctest.Read(t, "cmd/aveloxis/main.go")
	serveBody := srctest.FuncBody(t, main, "func runServe(")
	if !strings.Contains(serveBody, "SetAllowSecondServe(allowSecondServe)") {
		t.Error("runServe must pass the --allow-second-serve flag through to the store, or the flag " +
			"is inert and an operator who genuinely wants two serves cannot start one")
	}
	if !strings.Contains(main, `"allow-second-serve"`) {
		t.Error("serve must register the --allow-second-serve flag")
	}
	// A config key would persist silently and make the second serve
	// invisible again — exactly the condition the refusal exists to
	// surface. The override has to be per-invocation.
	cfg := srctest.Read(t, "internal/config/config.go")
	if strings.Contains(cfg, "allow_second_serve") {
		t.Error("--allow-second-serve must NOT become a config key: a persisted override restores " +
			"the silent-second-serve condition this refusal exists to prevent. Keep it per-invocation.")
	}

	// The narrowness pin: the override is consulted at the second-serve
	// arm only, never on the stamp-mismatch path.
	mig := srctest.StripGoComments(srctest.Read(t, "internal/db/migrate.go"))
	uses := strings.Count(mig, "allowSecondServe")
	if uses != 1 {
		t.Errorf("allowSecondServe is read %d times in migrate.go, want exactly 1 (the second-serve "+
			"arm). A second read means the override has spread — most dangerously onto "+
			"startupMigrateRefusal, which guards a FULL migration beside a live fleet.", uses)
	}
	refusal := srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func startupMigrateRefusal(")
	if strings.Contains(refusal, "allowSecondServe") || strings.Contains(refusal, "ErrSecondServeRefused") {
		t.Error("startupMigrateRefusal must not know about the second-serve override or its sentinel. " +
			"It refuses a serve that would run a full migration beside a live fleet — the 2026-09-09 " +
			"deadlock — and no override may reach it.")
	}
}
