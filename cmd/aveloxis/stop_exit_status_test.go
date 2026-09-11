// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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

// Round 17 (Copilot round 7, finding 1): the round-16 version of this
// test wrote a pidfile naming PID 1 and signaled it FOR REAL, skipping
// only when geteuid() == 0. In a rootless container PID 1 is the
// container's own init running as the SAME unprivileged uid, so the
// SIGTERM is delivered and the test kills the environment it runs in.
// The kernel's answer is now injected through the sendSignal seam; the
// pidfile names THIS process (live, so the stale-cleanup arm cannot
// swallow it) and the seam refuses with EPERM. The seam's production
// default is pinned separately (the pass-50 lesson: a seam shipped at
// its test value passed the whole suite).
func TestStopComponentReportsASignalFailureAsAnError(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	const comp = "avtest-eperm-probe"
	self := os.Getpid()
	pidPath := pidfile.Path(comp)
	if err := pidfile.Write(pidPath, self); err != nil {
		t.Fatal(err)
	}
	var gotPID int
	var gotSig syscall.Signal
	restore := sendSignal
	sendSignal = func(pid int, sig syscall.Signal) error {
		gotPID, gotSig = pid, sig
		return syscall.EPERM
	}
	t.Cleanup(func() { sendSignal = restore })

	stopped, err := stopComponent(comp)
	if stopped {
		t.Fatal("stopComponent reported a process the kernel refused to signal as stopped")
	}
	if err == nil {
		t.Fatal("a SIGTERM the kernel refused must be an error, not a silent false")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("PID %d", self)) {
		t.Errorf("the error must name the PID it could not signal, got: %v", err)
	}
	if gotPID != self || gotSig != syscall.SIGTERM {
		t.Errorf("stopComponent must SIGTERM the pidfile's PID through the seam, got pid=%d sig=%v", gotPID, gotSig)
	}
	if _, statErr := os.Stat(pidPath); statErr != nil {
		t.Error("the pidfile of a process that is still running must not be removed")
	}
}

// TestSendSignalProductionDefaultDeliversRealSignals pins the seam's
// default: it must actually reach the kernel. Signal 0 to this process
// is delivered (nil), and SIGTERM to a live `sleep` child is delivered
// and read back from the child's wait status — both through the real
// os.Process path. An earlier reaped-child arm was dropped in round 17
// L10: PID reuse made it a flake by construction, and a default that
// special-cased signal 0 passed it anyway.
func TestSendSignalProductionDefaultDeliversRealSignals(t *testing.T) {
	// Source pins first: they fail in milliseconds, before the runtime
	// arm below spends up to 30 s on a seam that does not deliver.
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func signalProcess("))
	if !strings.Contains(body, "sendSignal(pid, syscall.SIGTERM)") {
		t.Error("signalProcess must route SIGTERM through the sendSignal seam")
	}
	for _, direct := range []string{"os.FindProcess(", "proc.Signal("} {
		if strings.Contains(body, direct) {
			t.Errorf("signalProcess must not call %s directly — that is the path the seam exists to replace", direct)
		}
	}
	// The assignment count runs over EVERY non-test file in the package
	// (round 17 L10 pass 2): a `func init() { sendSignal = … }` in a
	// sibling file would otherwise pass a main.go-only count, and the
	// runtime arm above would catch it only after a 30 s wait.
	if !strings.Contains(src, "var sendSignal = ") {
		t.Fatal("the sendSignal seam must be declared in main.go")
	}
	files, err := filepath.Glob(filepath.Join(srctest.Root(t), "cmd", "aveloxis", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	assignments := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		assignments += strings.Count(srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/"+filepath.Base(f))), "sendSignal = ")
	}
	if assignments != 1 {
		t.Fatalf("sendSignal must be assigned exactly once in production (its declaration), got %d — "+
			"a second assignment ships the seam at a test value", assignments)
	}
	if err := sendSignal(os.Getpid(), syscall.Signal(0)); err != nil {
		t.Fatalf("the default seam must deliver a real signal to a live process, got: %v", err)
	}
	// Round 17 L10 finding 4: a default that special-cases signal 0
	// passed the self arm; the pin now delivers the signal `stop`
	// actually sends to a child and reads it back from the wait status.
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	if err := sendSignal(child.Process.Pid, syscall.SIGTERM); err != nil {
		child.Process.Kill()
		t.Fatalf("the default seam must deliver SIGTERM to a live child, got: %v", err)
	}
	werr := child.Wait()
	var exitErr *exec.ExitError
	if !errors.As(werr, &exitErr) {
		t.Fatalf("the child must exit by signal, got: %v", werr)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Fatalf("the child must have died of SIGTERM through the default seam, got: %v", werr)
	}

}

// Round 17 (Copilot round 7, finding 2 — SR-5, the round-11 rule at its
// sibling site): stopComponent's pidfile read was `if pid, err :=
// pidfile.Read(p); err == nil { ... }`, so EACCES, EIO and a corrupt
// file were silently dropped. The pgrep fallback then usually FOUND the
// component (startComponent execs `<binary> serve --config …`, which
// `pgrep -f "aveloxis serve"` matches) and stopped it — but the corrupt
// pidfile was never reported and was left behind, and the next `start`
// refused on it (round-11's fail-closed guard) with the operator none
// the wiser. A corrupt pidfile is neither stale nor live, so it is NOT
// removed: it is reported, and the operator decides.
//
// Round 17 L10 finding 1 folded the non-positive pids in: "-1", "0" and
// "-<pgrp>" must classify as corrupt too — kill(2) reads a negative pid
// as a process group, and only the stubbed seam makes that last case
// safe to drive.
func TestStopComponentDoesNotReadACorruptPidfileAsAbsent(t *testing.T) {
	for _, content := range []string{"not-a-pid\n", "-1", "0", "-" + strconv.Itoa(syscall.Getpgrp())} {
		t.Run(strings.TrimSpace(content), func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			const comp = "avtest-corrupt-probe"
			pidPath := pidfile.Path(comp)
			if err := os.MkdirAll(filepath.Dir(pidPath), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(pidPath, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			called := false
			restore := sendSignal
			sendSignal = func(int, syscall.Signal) error { called = true; return nil }
			t.Cleanup(func() { sendSignal = restore })

			stopped, err := stopComponent(comp)
			if stopped {
				t.Fatal("nothing was signaled, so nothing can have stopped")
			}
			if err == nil {
				t.Fatal("a corrupt pidfile is not evidence that the component is stopped (SR-5) — must be an error")
			}
			if !strings.Contains(err.Error(), pidPath) || !strings.Contains(err.Error(), "left in place") {
				t.Errorf("the error must name the pidfile and say it was left for the operator, got: %v", err)
			}
			if got := stopComponentsError(t, comp); strings.Contains(got, "failed to stop") {
				t.Errorf("nothing was found to stop, so \"failed to stop\" is the wrong verb (L10 pass 2): %s", got)
			}
			if called {
				t.Error("no PID was read, so nothing may be signaled — a negative pid would be a GROUP signal")
			}
			if _, statErr := os.Stat(pidPath); statErr != nil {
				t.Error("a corrupt pidfile is not stale — it must be left for the operator, not removed")
			}
		})
	}
}

// Round 17 L10 finding 2: when the pidfile is corrupt but pgrep finds
// and stops the process, stopComponent returns (true, err). Wrapping
// that as "failed to stop" would contradict the "Stopped serve" line
// printed one moment earlier; the exit is still nonzero (the next
// start refuses on the same file), but the words must be true.
func TestStopComponentsReportsAStopWithErrorsHonestly(t *testing.T) {
	stop := func(comp string) (bool, error) {
		return true, errors.New("pidfile left in place")
	}
	var verified []string
	n, err := stopComponents([]string{"serve"}, stop, func(c string) { verified = append(verified, c) })
	if n != 1 || len(verified) != 1 {
		t.Fatalf("a component that went down is counted and verified: n=%d verified=%v", n, verified)
	}
	if err == nil || !strings.Contains(err.Error(), "serve stopped, but") {
		t.Errorf("a stop that succeeded with errors must say so, got: %v", err)
	}
	if strings.Contains(err.Error(), "failed to stop") {
		t.Errorf("a component that WAS stopped must not be reported as failed: %v", err)
	}
}

// Round 17 L10 (pre-existing noise): on EPERM the pidfile arm fails and
// pgrep finds the SAME pid — it must not be signaled and reported
// twice. A child is spawned with the argv pgrep -f matches (bash's
// `exec -a` sets argv[0]); the seam refuses, so nothing is really sent.
func TestStopComponentDoesNotSignalOnePIDTwice(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	const comp = "avtest-twice-probe"
	child := exec.Command("bash", "-c", `exec -a "aveloxis `+comp+`" sleep 30`)
	if err := child.Start(); err != nil {
		t.Skipf("cannot spawn an argv-spoofed child: %v", err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, _ := exec.Command("pgrep", "-f", "aveloxis "+comp).Output()
		if strings.Contains(string(out), strconv.Itoa(child.Process.Pid)) {
			break
		}
		if time.Now().After(deadline) {
			t.Skip("pgrep never saw the spoofed argv on this host")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := pidfile.Write(pidfile.Path(comp), child.Process.Pid); err != nil {
		t.Fatal(err)
	}
	var pids []int
	restore := sendSignal
	sendSignal = func(pid int, _ syscall.Signal) error { pids = append(pids, pid); return syscall.EPERM }
	t.Cleanup(func() { sendSignal = restore })
	if _, err := stopComponent(comp); err == nil {
		t.Fatal("EPERM must surface")
	}
	if len(pids) != 1 || pids[0] != child.Process.Pid {
		t.Errorf("the pidfile's pid was signaled through %v; the pgrep fallback must skip a pid already attempted", pids)
	}
}

// SR-17: the ENOENT-is-the-only-definitive-absent rule has ONE
// spelling. componentAlreadyRunning and stopComponent both read the
// pidfile; both must classify the read through readComponentPID, and
// the fs.ErrNotExist test must live only there.
func TestPidfileReadClassificationHasOneSpelling(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	stripped := srctest.StripGoComments(src)
	for _, fn := range []string{"func componentAlreadyRunning(", "func stopComponent("} {
		body := srctest.StripGoComments(srctest.FuncBody(t, src, fn))
		if !strings.Contains(body, "readComponentPID(") {
			t.Errorf("%s must read the pidfile through readComponentPID (SR-17)", fn)
		}
		if strings.Contains(body, "pidfile.Read(") {
			t.Errorf("%s must not re-spell the pidfile read inline (SR-17)", fn)
		}
	}
	if n := strings.Count(stripped, "fs.ErrNotExist"); n != 1 {
		t.Errorf("fs.ErrNotExist must be tested exactly once in main.go (inside readComponentPID), got %d", n)
	}
	classifier := srctest.StripGoComments(srctest.FuncBody(t, src, "func readComponentPID("))
	if !strings.Contains(classifier, "fs.ErrNotExist") {
		t.Error("readComponentPID must be the site that treats ENOENT as the definitive absent")
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
	// The "No running aveloxis processes found." line is gated by the
	// ONE verdict predicate, never an inline comparison (round 17 L10
	// pass 3 — `stopped == 0` alone stayed green and reprinted the
	// round-16 line over a refused signal).
	if !strings.Contains(stop, "if nothingRunning(stopped, stopErr) {") {
		t.Error("stopCmd must gate the no-running line on nothingRunning(stopped, stopErr)")
	}
	if strings.Contains(stop, "stopped == 0") {
		t.Error("stopCmd must not re-spell the no-running verdict inline")
	}
	for _, fn := range []string{"func startCmd(", "func stopCmd("} {
		body := srctest.StripGoComments(srctest.FuncBody(t, src, fn))
		if strings.Contains(body, "Failed to start") || strings.Contains(body, "Failed to stop") {
			t.Errorf("%s must not print a failure and carry on — the failure is the return value", fn)
		}
	}
}

// stopComponentsError runs the real stopComponent for one component
// through stopComponents and returns the joined error text the
// operator would read.
func stopComponentsError(t *testing.T, comp string) string {
	t.Helper()
	_, err := stopComponents([]string{comp}, stopComponent, func(string) {})
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestNothingRunningVerdict pins the predicate behind "No running
// aveloxis processes found.": only a clean, empty stop earns it.
func TestNothingRunningVerdict(t *testing.T) {
	if !nothingRunning(0, nil) {
		t.Error("a clean stop that found nothing is exactly the no-running case")
	}
	if nothingRunning(1, nil) {
		t.Error("something was stopped — not nothing running")
	}
	if nothingRunning(0, errors.New("signaling serve (PID 7): operation not permitted")) {
		t.Error("a refused signal is a process still running — printing no-running over it is the round-16 incident")
	}
	if nothingRunning(0, errors.New("pidfile left in place")) {
		t.Error("an unreadable pidfile is UNKNOWN, not nothing running (SR-5)")
	}
}

// TestStopCommandNeverPrintsNothingRunningOverAFailure drives the REAL
// stop command in a CHILD process and reads everything the child wrote
// to file descriptors 1 and 2, plus its exit code.
//
// Why a child (round 17 L10 pass 6): three earlier drafts captured
// output by swapping the os.Stdout/os.Stderr Go variables, and each was
// escaped by a print through a channel that swap cannot see — first
// cobra's cmd.Println to stderr (pass 5), then the standard log
// logger, slog's default handler and println (pass 6), all of which
// hold the original descriptor. Duplicating descriptors in-process
// would work on darwin and linux/amd64 but not linux/arm64 (no
// syscall.Dup2), which CI would never catch. A child's stdout and
// stderr ARE descriptors 1 and 2, so every channel is captured by
// construction and there is nothing left to escape through.
//
// The contract per arm: the stop FAILS (exit code 3 from the helper,
// never 0) and prints NOTHING — on a failure the error is the return
// value, so any line at all, the round-16 "No running aveloxis
// processes found." or a rewording of it, is a finding.
//
// Arms: a refused signal, an unreadable pidfile, the no-args (all)
// target over a refused signal, and a missing pgrep (PATH emptied in
// the child — the SR-5 arm pass 6 found no test forced).
func TestStopCommandNeverPrintsNothingRunningOverAFailure(t *testing.T) {
	for _, arm := range []string{"web-refused", "web-corrupt", "all-refused", "all-pgrep-missing"} {
		t.Run(arm, func(t *testing.T) {
			env := append(os.Environ(),
				stopHelperEnv+"="+arm,
				"HOME="+t.TempDir(),
			)
			if arm == "all-pgrep-missing" {
				env = append(env, "PATH="+t.TempDir())
			}
			child := exec.Command(os.Args[0], "-test.run=^TestStopHelperProcess$")
			child.Env = env
			out, err := child.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("the stop must exit nonzero over a failure; child err=%v output:\n%s", err, out)
			}
			if code := exitErr.ExitCode(); code != stopHelperFailedExit {
				t.Fatalf("the child must exit %d (RunE returned an error), got %d — a setup failure or panic, not the contract; output:\n%s",
					stopHelperFailedExit, code, out)
			}
			if len(out) != 0 {
				t.Errorf("a failed stop that stopped nothing must print nothing to stdout or stderr — the failure is the return value; got:\n%s", out)
			}
		})
	}
}

const (
	stopHelperEnv        = "AVELOXIS_STOP_HELPER_ARM"
	stopHelperFailedExit = 3
	stopHelperSetupExit  = 4
)

// TestStopHelperProcess is not a test: it is the child body of
// TestStopCommandNeverPrintsNothingRunningOverAFailure, and returns
// immediately unless that test launched it. It stubs the signal seam
// ITSELF — a Go variable cannot be stubbed across a process boundary —
// and every pidfile it writes names its OWN pid, so nothing real is
// ever signaled. It exits before returning so the testing framework
// never prints its own PASS line into the captured output.
func TestStopHelperProcess(t *testing.T) {
	arm := os.Getenv(stopHelperEnv)
	if arm == "" {
		return
	}
	setupFail := func(msg string, err error) {
		fmt.Fprintf(os.Stderr, "stop helper setup (%s): %s: %v\n", arm, msg, err)
		os.Exit(stopHelperSetupExit)
	}
	sendSignal = func(int, syscall.Signal) error { return syscall.EPERM }

	pidPath := pidfile.Path("web")
	if err := os.MkdirAll(filepath.Dir(pidPath), 0o755); err != nil {
		setupFail("mkdir", err)
	}
	var args []string
	switch arm {
	case "web-refused":
		args = []string{"web"}
		if err := pidfile.Write(pidPath, os.Getpid()); err != nil {
			setupFail("pidfile", err)
		}
	case "web-corrupt":
		args = []string{"web"}
		if err := os.WriteFile(pidPath, []byte("not-a-pid"), 0o644); err != nil {
			setupFail("pidfile", err)
		}
	case "all-refused":
		if err := pidfile.Write(pidPath, os.Getpid()); err != nil {
			setupFail("pidfile", err)
		}
	case "all-pgrep-missing":
		// no pidfile: only pgrep can answer, and it is not on PATH
	default:
		setupFail("unknown arm", nil)
	}

	cfg := filepath.Join(os.Getenv("HOME"), "absent.json")
	cmd := stopCmd(&cfg)
	if err := cmd.RunE(cmd, args); err != nil {
		os.Exit(stopHelperFailedExit)
	}
	os.Exit(0)
}
