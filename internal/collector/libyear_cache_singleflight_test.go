// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 (Copilot review round 1 on PR #210) — the 24-hour answer cache
// releases its lock before calling the resolver, so every concurrent miss on
// the same key resolves independently. Analysis workers start together and
// share dependencies, so the same crate is looked up N times at once — and on
// a host paced to one request per second (crates.io's published limit) each
// duplicate reserves another second, building a queue that buys nothing.
//
// The cache's job is to answer a key once; in flight is still "once".

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestResolveLibyearCachedCoalescesConcurrentMisses(t *testing.T) {
	cache := newTTLCache[*db.LibyearRow](time.Minute)

	var calls int32
	entered := make(chan struct{})
	release := make(chan struct{})
	resolve := func(_ context.Context, dep libyearDep) (*db.LibyearRow, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
		}
		<-release // hold the resolver open so the others must coalesce
		// Faithful to the real resolvers, which all stamp the dep's own
		// Requirement and Type onto the row they build. That is what makes
		// the per-caller copy below load-bearing: a coalesced answer was
		// built for ONE caller's requirement text.
		return &db.LibyearRow{Name: dep.Name, CurrentVersion: dep.Version, LatestVersion: "9.9.9",
			Requirement: dep.Requirement, Type: dep.Type}, nil
	}

	dep := func(i int) libyearDep {
		return libyearDep{Name: "serde", Version: "1.0", Manager: "cargo",
			Requirement: fmt.Sprintf("req-%d", i), Type: map[bool]string{true: "runtime", false: "dev"}[i%2 == 0]}
	}

	const n = 8
	rows := make([]*db.LibyearRow, n)
	errs := make([]error, n)
	var wg sync.WaitGroup

	// The first caller enters the resolver and registers the key as
	// in flight before any other starts, so the remaining seven must find it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rows[0], errs[0] = resolveLibyearCached(context.Background(), cache, dep(0), resolve)
	}()
	<-entered

	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rows[i], errs[i] = resolveLibyearCached(context.Background(), cache, dep(i), resolve)
		}(i)
	}
	// Give the waiters time to reach the cache before the resolver returns;
	// without coalescing they each start their own lookup in this window.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("resolver called %d times for one key, want 1 — concurrent misses are not coalesced", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if rows[i] == nil || rows[i].LatestVersion != "9.9.9" {
			t.Fatalf("caller %d got %+v, want the shared answer", i, rows[i])
		}
		// Each caller still gets ITS OWN requirement text and scope — the
		// same contract a cache hit already honours.
		if want := fmt.Sprintf("req-%d", i); rows[i].Requirement != want {
			t.Errorf("caller %d Requirement = %q, want %q", i, rows[i].Requirement, want)
		}
		if want := dep(i).Type; rows[i].Type != want {
			t.Errorf("caller %d Type = %q, want %q", i, rows[i].Type, want)
		}
	}
	// Callers must not share one row: mutating one must not touch another.
	for i := 1; i < n; i++ {
		if rows[i] == rows[0] {
			t.Fatalf("caller %d shares the row pointer with caller 0 — the per-caller fields would alias", i)
		}
	}
}

// TestResolveLibyearCachedReleasesTheKeyOnPanic — v0.29.57 (review of the
// v0.29.57 delta). The in-flight entry was cleared only by REACHING the end
// of the function, so a panic in a resolver left the key registered with an
// unclosed channel. Collection jobs run under `defer safego.Recover` — a
// panic is deliberately survivable and costs one job — so nothing crashes;
// instead every later caller for that key blocks on the dead channel,
// holding its worker slot until `stop serve`. Before the coalescing change a
// panic cost one job; after it, one panic could wedge the fleet on that
// dependency.
func TestResolveLibyearCachedReleasesTheKeyOnPanic(t *testing.T) {
	cache := newTTLCache[*db.LibyearRow](time.Minute)
	dep := libyearDep{Name: "serde", Version: "1.0", Manager: "cargo"}

	panicking := func(context.Context, libyearDep) (*db.LibyearRow, error) { panic("resolver blew up") }
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic must propagate to the caller's recover, not be swallowed")
			}
		}()
		_, _ = resolveLibyearCached(context.Background(), cache, dep, panicking)
	}()

	// A later caller for the SAME key must not inherit the dead call.
	done := make(chan struct{})
	go func() {
		defer close(done)
		row, err := resolveLibyearCached(context.Background(), cache, dep,
			func(_ context.Context, d libyearDep) (*db.LibyearRow, error) {
				return &db.LibyearRow{Name: d.Name, LatestVersion: "2.0", Requirement: d.Requirement, Type: d.Type}, nil
			})
		if err != nil || row == nil || row.LatestVersion != "2.0" {
			t.Errorf("second caller got (%+v, %v), want the fresh answer", row, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a caller after a panicking resolve blocked forever — the in-flight key was never released, so every worker that needs this dependency wedges until stop serve")
	}
}

// TestGithubModuleLicenseCoalescesConcurrentMisses — v0.29.57. The sibling
// cache in the same file was not swept when the libyear cache learned to
// coalesce, so six concurrent callers made six GitHub calls for one module —
// each spending a lease from the one shared key-pool budget (SR-20).
func TestGithubModuleLicenseCoalescesConcurrentMisses(t *testing.T) {
	cache := newTTLCache[string](time.Minute)
	var calls int32
	entered := make(chan struct{})
	release := make(chan struct{})
	gh := &fakeGitHubAPI{answers: map[string]string{
		"/repos/spf13/cobra/license": `{"license":{"spdx_id":"Apache-2.0"}}`,
	}, before: func() {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
		}
		<-release
	}}

	const n = 6
	got := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got[0], errs[0] = githubModuleLicense(context.Background(), gh, cache, "github.com/spf13/cobra")
	}()
	<-entered
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = githubModuleLicense(context.Background(), gh, cache, "github.com/spf13/cobra")
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Errorf("%d GitHub calls for one module, want 1 — each duplicate spends a key-pool lease", c)
	}
	for i := range got {
		if errs[i] != nil || got[i] != "Apache-2.0" {
			t.Errorf("caller %d got (%q, %v), want Apache-2.0", i, got[i], errs[i])
		}
	}
}

// TestGithubModuleLicenseReleasesTheKeyOnPanic — v0.29.57 round 2. The
// sibling cache got "the same deferred release" but no test for it, so the
// deferred shape was free to be unwound back into per-return calls. Same
// failure as the libyear half: one panic inside the GitHub call strands a
// popular module's key and every later analysis worker blocks on the dead
// channel, holding a slot until `stop serve`.
func TestGithubModuleLicenseReleasesTheKeyOnPanic(t *testing.T) {
	cache := newTTLCache[string](time.Minute)
	gh := &fakeGitHubAPI{
		answers: map[string]string{"/repos/spf13/cobra/license": `{"license":{"spdx_id":"Apache-2.0"}}`},
		before:  func() { panic("github call blew up") },
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic must reach the caller's recover")
			}
		}()
		_, _ = githubModuleLicense(context.Background(), gh, cache, "github.com/spf13/cobra")
	}()

	gh.before = nil // the next caller succeeds
	done := make(chan struct{})
	go func() {
		defer close(done)
		lic, err := githubModuleLicense(context.Background(), gh, cache, "github.com/spf13/cobra")
		if err != nil || lic != "Apache-2.0" {
			t.Errorf("second caller got (%q, %v), want Apache-2.0", lic, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a caller after a panicking license lookup blocked forever — the module key was never released")
	}
}
