// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.4 (the 2026-09-09 scancode-runner incident): `start`/`stop`
// knew only serve|web|api, which is how a "scancode-only" host ended up
// running `aveloxis start serve` against production. The worker is now
// a manageable component — and never part of `all`, because a primary
// that still runs its in-serve pool would double up.
func TestResolveComponents(t *testing.T) {
	cases := []struct {
		target string
		want   []string
		err    bool
	}{
		{"all", []string{"serve", "web", "api"}, false},
		{"ALL", []string{"serve", "web", "api"}, false},
		{"serve", []string{"serve"}, false},
		{"web", []string{"web"}, false},
		{"api", []string{"api"}, false},
		{"scancode-worker", []string{"scancode-worker"}, false},
		{"Scancode-Worker", []string{"scancode-worker"}, false},
		{"scancode", nil, true},
		{"", nil, true},
		{"monitor", nil, true},
	}
	for _, c := range cases {
		got, err := resolveComponents(c.target)
		if c.err != (err != nil) {
			t.Errorf("resolveComponents(%q): err=%v, want error=%v", c.target, err, c.err)
			continue
		}
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("resolveComponents(%q) = %v, want %v", c.target, got, c.want)
		}
	}
	for _, c := range []string{"all", "ALL"} {
		got, _ := resolveComponents(c)
		for _, comp := range got {
			if comp == scancodeWorkerComponent {
				t.Fatalf("%q must never expand to the scancode worker", c)
			}
		}
	}
}

// The application_name `stop` verifies against is derived the same way
// the components tag their pools — serve through the shared db constant.
func TestComponentAppNameMatchesTheTags(t *testing.T) {
	if got := componentAppName("serve"); got != db.ServeApplicationName {
		t.Errorf("componentAppName(serve) = %q, want db.ServeApplicationName %q", got, db.ServeApplicationName)
	}
	if got := componentAppName(scancodeWorkerComponent); got != "aveloxis-scancode-worker" {
		t.Errorf("the worker tag the docs and pg_stat_activity readers name is aveloxis-scancode-worker, got %q", got)
	}
	// Round-3 finding 7: web and api tag through the same derivation
	// `stop web|api` verifies against — a literal there can drift and
	// turn the post-stop check into an instant all-clear.
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	for _, comp := range []string{"web", "api"} {
		if !strings.Contains(src, `ConnectionStringWithAppName(componentAppName("`+comp+`"))`) {
			t.Errorf("run%s must tag its pool via componentAppName(%q)", strings.ToUpper(comp[:1])+comp[1:], comp)
		}
		if strings.Contains(src, `ConnectionStringWithAppName("aveloxis-`+comp+`")`) {
			t.Errorf("no literal aveloxis-%s tag in main.go — derive it", comp)
		}
	}
}

// Wiring: both operator commands resolve their targets through the one
// helper (so `all` cannot drift between them), the worker command tags
// its pool through the same derivation `stop` verifies against, and the
// old hand list is gone.
func TestStartStopRouteThroughResolveComponents(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	for _, fn := range []string{"func startCmd(", "func stopCmd("} {
		body := srctest.StripGoComments(srctest.FuncBody(t, src, fn))
		if n := strings.Count(body, "resolveComponents("); n != 1 {
			t.Errorf("%s must resolve its target through resolveComponents exactly once, found %d", fn, n)
		}
	}
	if strings.Contains(srctest.StripGoComments(src), "validComponents") {
		t.Error("the hand-listed validComponents must be gone — resolveComponents owns the component set")
	}
	stop := srctest.StripGoComments(srctest.FuncBody(t, src, "func stopCmd("))
	if !strings.Contains(stop, "verifyBackendsDisconnected(os.Stdout, *cfgPath, componentAppName(comp))") {
		t.Error("stop must verify against componentAppName(comp), the derivation the components tag with")
	}
	if !strings.Contains(stop, "stopAllHint(") {
		t.Error("`stop all` must tell the operator about a running scancode worker it did not stop")
	}
	worker := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/scancode_worker_cmd.go"))
	if !strings.Contains(worker, "ConnectionStringWithAppName(componentAppName(scancodeWorkerComponent))") {
		t.Error("runScancodeWorker must tag its pool via componentAppName(scancodeWorkerComponent)")
	}
	if !strings.Contains(worker, "pidfile.Path(scancodeWorkerComponent)") {
		t.Error("runScancodeWorker must write its pidfile under the component name start/stop manage")
	}
}

// The verdict `stop` prints after its poll: terminate recipes ONLY for
// this host's backends; other hosts' backends are counted, named as
// such, and never offered for termination. The incident printed 64
// recipes for the primary's pool.
func TestPrintBackendVerdictNeverTargetsOtherHosts(t *testing.T) {
	var out bytes.Buffer
	printBackendVerdict(&out, "aveloxis-serve", 110*time.Second, db.AppNameBackends{ThisHost: []int{11, 12}, OtherHosts: 3})
	s := out.String()
	if n := strings.Count(s, "pg_terminate_backend("); n != 2 {
		t.Errorf("two local PIDs → two recipes, got %d:\n%s", n, s)
	}
	for _, want := range []string{"pg_terminate_backend(11);", "pg_terminate_backend(12);", "WARNING: 2 aveloxis-serve backend(s) from this host", "3 aveloxis-serve backend(s) are connected from other client addresses"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}

	out.Reset()
	printBackendVerdict(&out, "aveloxis-serve", 110*time.Second, db.AppNameBackends{OtherHosts: 64})
	s = out.String()
	if strings.Contains(s, "pg_terminate_backend(") || strings.Contains(s, "WARNING") || strings.Contains(s, "orphan") {
		t.Errorf("64 other-host backends and none local: no warning, no recipe, no orphan talk:\n%s", s)
	}
	if !strings.Contains(s, "64 aveloxis-serve backend(s) are connected from other client addresses") || !strings.Contains(s, "not this host's") {
		t.Errorf("the other-address backends must be reported as not this host's:\n%s", s)
	}

	// Round-7 finding 1: backends of ANOTHER database role — their client
	// address is hidden from this role, so there is no verdict: named,
	// with the way to one, never a recipe.
	out.Reset()
	printBackendVerdict(&out, "aveloxis-serve", 110*time.Second, db.AppNameBackends{Hidden: 64})
	s = out.String()
	if strings.Contains(s, "pg_terminate_backend(") || strings.Contains(s, "WARNING") || strings.Contains(s, "orphan") {
		t.Errorf("64 hidden backends and none local: no warning, no recipe, no orphan talk:\n%s", s)
	}
	for _, want := range []string{"64 aveloxis-serve backend(s)", "not visible", "pg_read_all_stats"} {
		if !strings.Contains(s, want) {
			t.Errorf("the hidden backends must be reported with the way to a verdict (%q):\n%s", want, s)
		}
	}
	out.Reset()
	printBackendVerdict(&out, "aveloxis-serve", 110*time.Second, db.AppNameBackends{ThisHost: []int{11}, Hidden: 2})
	s = out.String()
	if strings.Count(s, "pg_terminate_backend(") != 1 || !strings.Contains(s, "2 aveloxis-serve backend(s)") || !strings.Contains(s, "not visible") {
		t.Errorf("one local recipe beside the hidden note:\n%s", s)
	}

	out.Reset()
	printBackendVerdict(&out, "aveloxis-serve", 110*time.Second, db.AppNameBackends{})
	if out.Len() != 0 {
		t.Errorf("nothing lingering anywhere → silence, got:\n%s", out.String())
	}
}

// Wiring: the verifier polls the host-scoped probe and renders through
// printBackendVerdict; the host-blind probe is gone from the file.
func TestVerifierRoutesThroughHostScopedProbe(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func verifyBackendsDisconnected("))
	if n := strings.Count(body, "store.BackendsByAppName(ctx, appName)"); n != 1 {
		t.Errorf("the verifier must poll store.BackendsByAppName exactly once, found %d", n)
	}
	if n := strings.Count(body, "printBackendVerdict("); n != 1 {
		t.Errorf("the verifier must render its verdict through printBackendVerdict exactly once, found %d", n)
	}
	if strings.Contains(body, "pg_terminate_backend") {
		t.Error("the terminate recipe is rendered only by printBackendVerdict (one place to keep host-scoped)")
	}
	if strings.Contains(srctest.StripGoComments(src), "PidsByAppName(") {
		t.Error("the host-blind PidsByAppName must not be called from main.go")
	}
	// The poll itself is the pure pollBackends (behaviorally pinned
	// above); the verifier only wires the real probe, clock and sleep.
	if n := strings.Count(body, "pollBackends("); n != 1 {
		t.Errorf("the verifier must run the poll through pollBackends exactly once, found %d", n)
	}
	if strings.Contains(body, "for ") {
		t.Error("no loop in the verifier — the poll lives in pollBackends so its exit rule is testable")
	}
}

// `stop all` stops serve/web/api only; on a host that also runs the
// scancode worker it says so instead of silently leaving it running.
func TestStopAllHintsRunningScancodeWorker(t *testing.T) {
	var out bytes.Buffer
	stopAllHint(&out, func(c string) bool { return c == scancodeWorkerComponent })
	if !strings.Contains(out.String(), "aveloxis stop scancode-worker") {
		t.Errorf("a running worker must be named with the command that stops it, got:\n%s", out.String())
	}
	out.Reset()
	stopAllHint(&out, func(string) bool { return false })
	if out.Len() != 0 {
		t.Errorf("no worker running → no hint, got:\n%s", out.String())
	}
}

// The cobra verb and the managed component name are one spelling.
func TestScancodeWorkerUseIsTheComponentName(t *testing.T) {
	cfg := "aveloxis.json"
	if got := scancodeWorkerCmd(&cfg).Use; got != scancodeWorkerComponent {
		t.Errorf("scancode-worker Use = %q, want %q", got, scancodeWorkerComponent)
	}
}

// The poll behind `stop`: it ends the moment THIS host's backends are
// gone — other hosts' backends never keep a local stop waiting (the
// incident waited the full 1m50s budget on the primary's pool). A
// failed poll is neither "all clear" nor the orphan verdict (pass 41):
// it aborts with the hedge and hands back nothing to render.
func TestPollBackendsEndsOnThisHostOnly(t *testing.T) {
	probes, sleeps, checks := 0, 0, 0
	// A fake clock: each check advances one second; the budget is 6 s,
	// so a poll that never sees its exit runs exactly five probes.
	elapsed := func() time.Duration { checks++; return time.Duration(checks) * time.Second }
	const budget = 6 * time.Second
	sleep := func() { sleeps++ }
	var out bytes.Buffer

	// Other hosts' backends only: one probe, no sleep, verdict rendered.
	probes = 0
	last, render := pollBackends(func() (db.AppNameBackends, error) {
		probes++
		return db.AppNameBackends{OtherHosts: 64}, nil
	}, budget, elapsed, sleep, &out, "aveloxis-serve")
	if probes != 1 || sleeps != 0 || !render || last.OtherHosts != 64 {
		t.Errorf("64 other-host backends must end the poll after one probe: probes=%d sleeps=%d render=%v last=%+v", probes, sleeps, render, last)
	}

	// Hidden backends only (another role's, address unseen): nothing to
	// wait for — this stop cannot verify them; one probe, verdict rendered.
	probes, sleeps, checks = 0, 0, 0
	last, render = pollBackends(func() (db.AppNameBackends, error) {
		probes++
		return db.AppNameBackends{Hidden: 3}, nil
	}, budget, elapsed, sleep, &out, "aveloxis-serve")
	if probes != 1 || sleeps != 0 || !render || last.Hidden != 3 {
		t.Errorf("hidden backends must end the poll after one probe: probes=%d sleeps=%d render=%v last=%+v", probes, sleeps, render, last)
	}

	// This host's backend persists: polled until the budget, last carries it.
	probes, sleeps, checks = 0, 0, 0
	last, render = pollBackends(func() (db.AppNameBackends, error) {
		probes++
		return db.AppNameBackends{ThisHost: []int{7}, OtherHosts: 2}, nil
	}, budget, elapsed, sleep, &out, "aveloxis-serve")
	if probes < 2 || sleeps < 1 || !render || len(last.ThisHost) != 1 || last.ThisHost[0] != 7 {
		t.Errorf("a persistent local backend must be polled to the budget and reported: probes=%d sleeps=%d render=%v last=%+v", probes, sleeps, render, last)
	}

	// Local backends gone on the second probe: ends there.
	probes, sleeps, checks = 0, 0, 0
	last, render = pollBackends(func() (db.AppNameBackends, error) {
		probes++
		if probes == 1 {
			return db.AppNameBackends{ThisHost: []int{7}}, nil
		}
		return db.AppNameBackends{}, nil
	}, budget, elapsed, sleep, &out, "aveloxis-serve")
	if probes != 2 || sleeps != 1 || !render || len(last.ThisHost) != 0 {
		t.Errorf("the poll must stop the probe after the local backends disconnect: probes=%d sleeps=%d render=%v last=%+v", probes, sleeps, render, last)
	}

	// A failed probe after a local sighting: abort with the hedge, no verdict.
	probes, sleeps, checks = 0, 0, 0
	out.Reset()
	_, render = pollBackends(func() (db.AppNameBackends, error) {
		probes++
		if probes == 1 {
			return db.AppNameBackends{ThisHost: []int{7}}, nil
		}
		return db.AppNameBackends{}, errors.New("boom")
	}, budget, elapsed, sleep, &out, "aveloxis-serve")
	if render {
		t.Error("a failed poll must not render the orphan verdict")
	}
	for _, want := range []string{"aborted", "boom", "[7]", "re-check pg_stat_activity"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the abort hedge must carry %q:\n%s", want, out.String())
		}
	}
}

// Round-2 finding 9: with no loadable config (the dedicated-host doc's
// `stop scancode-worker` without -c), the verifier used to return in
// silence — the "watches pg_stat_activity" promise quietly not kept.
func TestVerifyBackendsDisconnectedSaysWhenConfigIsMissing(t *testing.T) {
	var out bytes.Buffer
	verifyBackendsDisconnected(&out, "/nonexistent/aveloxis.json", "aveloxis-serve")
	if !strings.Contains(out.String(), "backend verification skipped") || !strings.Contains(out.String(), "/nonexistent/aveloxis.json") {
		t.Errorf("a config that cannot be loaded must be said out loud, got:\n%s", out.String())
	}
}
