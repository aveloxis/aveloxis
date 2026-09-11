// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pidfile"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round 16 finding 1, the loop contract: every component is attempted,
// every failure is in the returned error, a component that started is
// not in it, and an all-success run returns nil (exit 0). Driven
// through the injected starter so nothing is spawned.
func TestStartComponentsAttemptsEveryComponentAndReportsEachFailure(t *testing.T) {
	var attempted []string
	starter := func(fail ...string) func(component, cfgPath string) error {
		return func(component, cfgPath string) error {
			attempted = append(attempted, component)
			if cfgPath != "/etc/aveloxis.json" {
				t.Errorf("cfgPath must reach the starter unchanged, got %q", cfgPath)
			}
			for _, f := range fail {
				if f == component {
					return errors.New("refusing to start " + component + ": unreadable pidfile")
				}
			}
			return nil
		}
	}

	attempted = nil
	err := startComponents([]string{"serve", "web", "api"}, "/etc/aveloxis.json", starter("serve"))
	if err == nil {
		t.Fatal("a refused serve must surface as an error")
	}
	if got := strings.Join(attempted, ","); got != "serve,web,api" {
		t.Errorf("every component must still be attempted after a refusal (start all starts web and api beside an already-up serve), got %s", got)
	}
	if !strings.Contains(err.Error(), "serve") || strings.Contains(err.Error(), "web") || strings.Contains(err.Error(), "api") {
		t.Errorf("the error must name the failed component and only it, got: %v", err)
	}

	attempted = nil
	err = startComponents([]string{"serve", "web", "api"}, "/etc/aveloxis.json", starter("serve", "api"))
	if err == nil || !strings.Contains(err.Error(), "serve") || !strings.Contains(err.Error(), "api") {
		t.Errorf("two refusals must both be named, got: %v", err)
	}
	if len(attempted) != 3 {
		t.Errorf("attempted %v, want all three", attempted)
	}

	if err := startComponents([]string{"serve", "web", "api"}, "/etc/aveloxis.json", starter()); err != nil {
		t.Errorf("an all-success start must exit 0, got: %v", err)
	}
}

// The L11 sibling: stop's loop keeps going past a failed signal,
// reports every failure, verifies only the components that actually
// went down, and returns exit 0 for the idempotent nothing-to-stop
// case.
func TestStopComponentsAttemptsEveryComponentAndReportsSignalFailures(t *testing.T) {
	var attempted, verified []string
	stopper := func(outcome map[string]error, down map[string]bool) func(string) (bool, error) {
		return func(component string) (bool, error) {
			attempted = append(attempted, component)
			return down[component], outcome[component]
		}
	}
	onStopped := func(component string) { verified = append(verified, component) }

	attempted, verified = nil, nil
	n, err := stopComponents([]string{"serve", "web", "api"},
		stopper(map[string]error{"serve": errors.New("signaling serve (PID 7): operation not permitted")}, map[string]bool{"web": true}),
		onStopped)
	if err == nil || !strings.Contains(err.Error(), "serve") {
		t.Errorf("a failed signal must reach the exit status naming the component, got: %v", err)
	}
	if got := strings.Join(attempted, ","); got != "serve,web,api" {
		t.Errorf("every component must still be attempted after a failure, got %s", got)
	}
	if n != 1 || strings.Join(verified, ",") != "web" {
		t.Errorf("only the component that went down is counted and verified: n=%d verified=%v", n, verified)
	}

	attempted, verified = nil, nil
	n, err = stopComponents([]string{"serve", "web", "api"}, stopper(nil, nil), onStopped)
	if err != nil || n != 0 || len(verified) != 0 {
		t.Errorf("nothing running is not a failure (stop is idempotent): n=%d err=%v verified=%v", n, err, verified)
	}
}

// The real signal path: a pidfile naming PID 1 is a LIVE process this
// user cannot signal (init/launchd; EPERM). Before round 16 that
// printed "Failed to stop" and returned false — indistinguishable from
// "nothing running", and `aveloxis stop` exited 0 saying exactly that.
// The component name is one nothing on this machine can carry, so the
// pgrep fallback matches nothing and no real process is touched.
func TestStopComponentReportsASignalFailureAsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: PID 1 would be signaled for real")
	}
	t.Setenv("HOME", t.TempDir())
	const comp = "avtest-eperm-probe"
	pidPath := pidfile.Path(comp)
	if err := pidfile.Write(pidPath, 1); err != nil {
		t.Fatal(err)
	}
	stopped, err := stopComponent(comp)
	if stopped {
		t.Fatal("stopComponent reported PID 1 as stopped")
	}
	if err == nil {
		t.Fatal("a SIGTERM the kernel refused must be an error, not a silent false")
	}
	if !strings.Contains(err.Error(), "PID 1") {
		t.Errorf("the error must name the PID it could not signal, got: %v", err)
	}
	if _, statErr := os.Stat(pidPath); statErr != nil {
		t.Error("the pidfile of a process that is still running must not be removed")
	}
}

// Wiring: both commands return the loop helper's error — the exit
// status is the contract, and a RunE that prints and returns nil is
// the exact shape round 16 removed.
func TestStartAndStopReturnTheirLoopsError(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	start := srctest.StripGoComments(srctest.FuncBody(t, src, "func startCmd("))
	if !strings.Contains(start, "return startComponents(components, *cfgPath, startComponent)") {
		t.Error("startCmd must return startComponents' error (round 16 finding 1)")
	}
	stop := srctest.StripGoComments(srctest.FuncBody(t, src, "func stopCmd("))
	if !strings.Contains(stop, "stopComponents(components, stopComponent,") || !strings.Contains(stop, "return stopErr") {
		t.Error("stopCmd must route through stopComponents and return its error (round 16, L11 sweep)")
	}
	for _, fn := range []string{"func startCmd(", "func stopCmd("} {
		body := srctest.StripGoComments(srctest.FuncBody(t, src, fn))
		if strings.Contains(body, "Failed to start") || strings.Contains(body, "Failed to stop") {
			t.Errorf("%s must not print a failure and carry on — the failure is the return value", fn)
		}
	}
}
