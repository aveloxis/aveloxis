// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The analysis's side of the child that reads a repository's manifests
// (0.29.213; manifest_child.go says what the child is and why).

// ManifestLimits are the child's limits: collection.manifest_read_cpu_seconds
// and collection.manifest_read_memory_mb, as the config's accessors give
// them. The zero value is the defaults.
type ManifestLimits struct {
	CPU       time.Duration
	HeapBytes int64
}

// effective is the limits used: the configured ones, or the defaults of the
// one default layer (config) for a collector that was given none (a test, a
// one-shot command).
func (l ManifestLimits) effective() ManifestLimits {
	var defaults config.CollectionConfig
	if l.CPU <= 0 {
		l.CPU = defaults.ManifestReadCPULimit()
	}
	if l.HeapBytes <= 0 {
		l.HeapBytes = defaults.ManifestReadMemoryLimit()
	}
	return l
}

// The parent's own bound on the child, by the clock. The child ends itself
// when it has USED its CPU time; this is for a child that does not (its
// watchdog never ran, it is stopped, it is blocked in a system call), and
// for a host so busy that the child is not given the CPU to reach its own
// limit.
//
// PROVISIONAL (summary/71, section 8). manifestWallFactor: the child is
// allowed four times its CPU limit on the clock, not counting the time the
// parent itself spent answering its requests for poms (that is the
// network's, and is bounded where it is spent: the registry client's
// timeout and declaredFetchBudget). Four is for a child that had a quarter
// of a processor while it worked: a host on which every collection worker
// is reading manifests at once with four workers to the core. Past that the
// host is starved and the analysis is given up, to be tried next cycle.
// manifestWallFloor is what starting a process and reading a cold clone's
// directories can take with no CPU used at all.
const (
	manifestWallFactor = 4
	manifestWallFloor  = time.Minute
)

// manifestWallAllowance is how long the child may run by the clock, beyond
// the time the parent spent answering it. A variable so that a test of the
// stop need not wait a minute; TestTheParentsClockAllowance pins it.
var manifestWallAllowance = func(cpu time.Duration) time.Duration {
	return manifestWallFloor + manifestWallFactor*cpu
}

// manifestAnswering is the time an analysis has spent answering its child's
// requests for poms, the answer in hand included: the clock that bounds the
// child does not run while the child waits for this process. (Until
// 0.29.214 an answer's time was counted only once it had been given, so a
// single answer slower than the allowance had the waiting child stopped by
// the clock; found by the test written for review round 7's mutant.)
type manifestAnswering struct {
	done  atomic.Int64 // nanoseconds of answers given
	since atomic.Int64 // when the answer in hand began (Unix nanoseconds); 0: none is
}

func (a *manifestAnswering) begin() { a.since.Store(time.Now().UnixNano()) }

// end: the time is added before the answer in hand is cleared, so that a
// look between the two counts it twice and never not at all.
func (a *manifestAnswering) end() {
	if since := a.since.Load(); since != 0 {
		a.done.Add(time.Now().UnixNano() - since)
		a.since.Store(0)
	}
}

func (a *manifestAnswering) spent() time.Duration {
	d := a.done.Load()
	if since := a.since.Load(); since != 0 {
		d += time.Now().UnixNano() - since
	}
	return time.Duration(d)
}

// manifestWallCheckEvery is how often the parent compares the clock with
// the allowance.
var manifestWallCheckEvery = time.Second

// manifestStderrLimit is how much of a crashed child's standard error is
// kept for the log: a Go panic's first lines.
const manifestStderrLimit = 16 << 10

// errManifestChild is an analysis whose manifests were not read. Attempt is
// what the coverage record says (a model.InventoryAttempt* value).
type errManifestChild struct {
	Attempt string
	detail  string
	// Stale: the program on disk is not this process's version. It is the
	// same failure for every analysis until serve is restarted, so it is
	// logged at Error once (runManifestChild) and the phases say their own
	// part at Debug (manifestFailureLevel).
	Stale bool
}

func (e *errManifestChild) Error() string { return e.detail }

func manifestChildFailed(format string, a ...any) *errManifestChild {
	return &errManifestChild{Attempt: model.InventoryAttemptReadFailed, detail: fmt.Sprintf(format, a...)}
}

// manifestParentVersion is the version an analysis says it is when it asks
// the child: this program's. A variable so that a test can ask as another
// version; TestTheManifestChildIsThisProgram pins it.
var manifestParentVersion = db.ToolVersion

// manifestStaleSaid: the Error line about a binary of another version has
// been logged by this process.
var manifestStaleSaid atomic.Bool

// manifestFailureLevel is the level at which a phase logs that its part of
// the clone was not read: level, or Debug when the reason is a binary of
// another version, which one Error line has already said for the process.
func manifestFailureLevel(err error, level slog.Level) slog.Level {
	var child *errManifestChild
	if errors.As(err, &child) && child.Stale {
		return slog.LevelDebug
	}
	return level
}

// manifestChildCommand is the command that starts the child: this program
// again, with the one argument. A variable so that a test can start a
// child that misbehaves; TestTheManifestChildIsThisProgram pins the value.
var manifestChildCommand = func() (string, []string, error) {
	exe, err := manifestChildProgram(runtime.GOOS, os.Executable)
	if err != nil {
		return "", nil, err
	}
	return exe, []string{ManifestChildArg}, nil
}

// procSelfExe is, on Linux, the program this process is RUNNING: a link the
// kernel keeps to the file the process was started from, which stays that
// file when its name is given to another (rename(2), which is how `go
// install` and a package upgrade replace a binary; the link then reads
// ".../aveloxis (deleted)" and still opens the running program) and cannot
// be written over while the process runs (ETXTBSY).
const procSelfExe = "/proc/self/exe"

// manifestChildProgram is the file the child is started from (0.29.214).
// The child must be the program that asks it, and until 0.29.214 it was the
// file at this program's PATH on disk, which is another program after an
// upgrade under a serve that was not stopped: every documented upgrade
// installs the new binary first and stops serve afterwards, and from the
// install to the stop no manifest was read (review round 7). On Linux the
// running program itself is started. Elsewhere there is no such link, the
// path is used, and the version check in the child (ManifestChildMain) is
// what catches a replaced file.
func manifestChildProgram(goos string, executable func() (string, error)) (string, error) {
	if goos == "linux" {
		return procSelfExe, nil
	}
	return executable()
}

// manifestChildFallback is the file started when the one manifestChildCommand
// names could not be: this program's path, when what failed was the running
// program's link, and nothing otherwise. A variable for a test.
var manifestChildFallback = func(failed string) (string, error) {
	if failed != procSelfExe {
		return "", nil
	}
	return os.Executable()
}

var manifestFallbackSaid atomic.Bool

// programBuild identifies the FILE a program is: its length and the time it
// was last written. Two builds of one version are two files (0.29.215: the
// handshake compared db.ToolVersion alone, so a `go install` of the same
// version under a running serve was started as the reader although it is
// another program). Not a digest of the contents: the child takes it at
// every start, of a file of tens of megabytes. "" when the file cannot be
// examined, and then nothing is compared.
func programBuild(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%d.%d", info.Size(), info.ModTime().UnixNano())
}

// manifestParentBuild is this process's build, taken ONCE, the first time
// it is asked (the self-test at serve's start asks): of the running
// program's link on Linux, which is the running program whatever has been
// done to its name since, and elsewhere of the file at this program's path
// as it then is. A variable so that a test can ask as another build.
var manifestParentBuild = sync.OnceValue(func() string {
	exe, err := manifestChildProgram(runtime.GOOS, os.Executable)
	if err != nil {
		return ""
	}
	return programBuild(exe)
})

// manifestChildEnv is the child's whole environment: nothing. It reads no
// variable, and what the parent's environment holds (a database password,
// forge tokens in a container's variables) is not the child's to see.
func manifestChildEnv() []string { return []string{} }

// manifestRun is one child and what the parent learned from it.
type manifestRun struct {
	names manifestNames
	decl  *manifestDeclared
	locks *lockfileSet
	// finished are the passes whose done frame arrived: their results are
	// whole. rootErr: the clone's root could not be read in that pass, and
	// it has no result.
	finished map[string]bool
	rootErr  map[string]error
	// passFail: the pass ran to its end in the child and its result could
	// not be KEPT by this process (the lockfile pass's, when the temporary
	// file it is kept in could not be made or written). It is that pass's
	// failure alone: the passes that finished stand (0.29.215; it was
	// treated as the child breaking the protocol, which discards them all
	// and blames the reader).
	passFail map[string]error
	// cpu, heapPeak and maxRSS are what the child says the passes cost it.
	cpu      time.Duration
	heapPeak int64
	maxRSS   int64
	// present are the lockfiles the clone holds, as the name pass listed
	// them; presentKnown: it did.
	present      map[string]bool
	presentKnown bool
	// logDropped is how many of the child's log records were not logged
	// (manifestLogRecords, manifestLogBytes).
	logDropped int
}

// close lets go of what the run holds outside memory: the lockfile pass's
// result on disk.
func (r *manifestRun) close() {
	if r != nil && r.locks != nil {
		r.locks.spool.close()
	}
}

// passErr says whether a pass's result may be used: nil when the pass
// finished; an *errManifestRoot when it could not read the clone's root;
// otherwise why the child did not get that far (runErr, what
// runManifestChild returned with this run).
func (r *manifestRun) passErr(pass string, runErr error) error {
	if r != nil && r.passFail[pass] != nil {
		return r.passFail[pass]
	}
	if r != nil && r.finished[pass] {
		if err := r.rootErr[pass]; err != nil {
			return &errManifestRoot{err}
		}
		return nil
	}
	if runErr != nil {
		return runErr
	}
	return manifestChildFailed("the %s were not asked of the reader", pass)
}

// runManifestChild runs the passes of req in one child. src answers the
// child's requests for poms (nil: the child asks for none).
//
// The run holds every pass that finished, and is returned WITH the error
// when the child was stopped or failed part-way: a pass it had finished is
// whole, and stands (passErr). The error is an *errManifestChild. When the
// child sent something this protocol does not send, nothing it sent is kept.
// A shutdown returns no run and the context's error.
func runManifestChild(ctx context.Context, req manifestRequest, limits ManifestLimits, src manifest.PomSource, logger *slog.Logger) (*manifestRun, error) {
	limits = limits.effective()
	req.CPULimitNanos, req.HeapLimitBytes = int64(limits.CPU), limits.HeapBytes
	req.PomSource = src != nil
	req.ParentPID = os.Getpid()
	req.Version = manifestParentVersion
	req.Build = manifestParentBuild()

	// The child's working directory is not this process's (below), so the
	// clone is named to it in full: a relative clone path (a relative
	// TMPDIR when serve started) named nothing to the child (review round
	// 7).
	abs, err := filepath.Abs(req.Dir)
	if err != nil {
		return nil, manifestChildFailed("the manifest reader could not be started: the clone's path %q cannot be made absolute: %v", req.Dir, err)
	}
	req.Dir = abs

	exe, args, err := manifestChildCommand()
	if err != nil {
		return nil, manifestChildFailed("the manifest reader could not be started: this program's own path is not known: %v", err)
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, manifestChildFailed("the manifest reader could not be started: %v", err)
	}
	defer inW.Close()
	stderr := &cappedBuffer{limit: manifestStderrLimit}
	command := func(exe string) *exec.Cmd {
		cmd := exec.CommandContext(childCtx, exe, args...)
		cmd.Env = manifestChildEnv()
		cmd.Dir = string(filepath.Separator) // not serve's working directory: the child reads no relative path
		cmd.Stdin = inR
		cmd.Stderr = stderr
		return cmd
	}
	cmd := command(exe)
	// Standard error is copied by a goroutine of os/exec, and Wait waits
	// for that copy to end. A process the child left behind in another
	// session, holding the descriptor, keeps this function for at most
	// sweptWaitDelay past the child's own end: startSweptCommand sets
	// cmd.WaitDelay (groupKilled), as for git and scc, and Wait then
	// answers exec.ErrWaitDelay, which is not the child's failure (below).
	// (Review round 7 measured 6 s past a 2 s clock for a `sleep 6` left
	// behind: inside that bound, which the comment here did not state.)
	started := time.Now()
	swept, err := startSweptCommand(cmd)
	if err != nil && ctx.Err() == nil {
		// The running program's link could not be started (a host without
		// procfs, a container that hides it, an emulator that does not
		// answer for it): the file at this program's path is started
		// instead, and the build check in the child is then what catches a
		// file replaced under a running serve (0.29.215).
		if alt, altErr := manifestChildFallback(exe); altErr == nil && alt != "" {
			first := err
			cmd = command(alt)
			if swept, err = startSweptCommand(cmd); err == nil {
				if manifestFallbackSaid.CompareAndSwap(false, true) {
					logger.Warn("the manifest reader could not be started from the running program's link and is started from this program's path instead (said once)",
						"link", exe, "error", first, "program", alt,
						"consequence", "a binary replaced under a running serve is another program; the reader refuses it and says so until serve is restarted")
				}
				exe = alt
			} else {
				err = fmt.Errorf("%v; and from %s: %v", first, alt, err)
			}
		}
	}
	_ = inR.Close()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, manifestChildFailed("the manifest reader could not be started (%s): %v", exe, err)
	}
	defer swept.Close()

	// The clock. answering is the time spent in src, which is not the
	// child's.
	var answering manifestAnswering
	var timedOut atomic.Bool
	allowance := manifestWallAllowance(limits.CPU)
	watchDone := make(chan struct{})
	var watch sync.WaitGroup
	watch.Add(1)
	go func() {
		defer watch.Done()
		tick := time.NewTicker(manifestWallCheckEvery)
		defer tick.Stop()
		for {
			select {
			case <-watchDone:
				return
			case <-childCtx.Done():
				return
			case <-tick.C:
				if time.Since(started)-answering.spent() > allowance {
					timedOut.Store(true)
					cancel() // kills the child's process group (groupKilled)
					return
				}
			}
		}
	}()
	defer func() {
		close(watchDone)
		watch.Wait()
	}()

	run, complete, protoErr := manifestConverse(req, limits, src, logger, bufio.NewReader(swept.Stdout), inW, &answering)
	if protoErr != nil {
		// What the child sent is not to be trusted further, nor what it
		// sent before: end it now.
		cancel()
		run.close()
		run = &manifestRun{}
	}
	if run != nil && run.logDropped > 0 {
		logger.Warn("the manifest reader logged more than one analysis writes to the log — the rest of its records were dropped",
			"repo_id", req.RepoID, "dropped", run.logDropped, "kept_records_limit", manifestLogRecords, "kept_bytes_limit", manifestLogBytes)
	}
	_ = inW.Close()
	waitErr := swept.Wait()
	if errors.Is(waitErr, exec.ErrWaitDelay) {
		waitErr = nil // the child itself ended; only something else held its standard error
	}

	switch {
	case ctx.Err() != nil:
		run.close()
		return nil, ctx.Err() // a shutdown, not a failure
	case timedOut.Load():
		return run, &errManifestChild{Attempt: model.InventoryAttemptReadTimedOut,
			detail: fmt.Sprintf("the manifest reader was stopped: it had run for %s by the clock, not counting %s the analysis spent answering it, and may run for %s (%d times collection.manifest_read_cpu_seconds, which is %s, and %s)",
				time.Since(started).Round(time.Second), answering.spent().Round(time.Second), allowance, manifestWallFactor, limits.CPU, manifestWallFloor)}
	}
	switch code := exitStatus(waitErr); {
	case code == manifestExitVersion:
		// The file this process started is not this process's version.
		onDisk := stderr.text()
		// A child of this version and another build says its build too;
		// this process's is then said beside it.
		running := manifestParentVersion
		if strings.Contains(onDisk, " build ") {
			running += " build " + manifestParentBuild()
		}
		err := &errManifestChild{Attempt: model.InventoryAttemptReadFailed, Stale: true,
			detail: fmt.Sprintf("the aveloxis binary on disk (%s) is version %s, this process is version %s: restart serve", exe, onDisk, running)}
		level := slog.LevelDebug
		if manifestStaleSaid.CompareAndSwap(false, true) {
			level = slog.LevelError
		}
		logger.Log(ctx, level, "the manifest reader is another version of aveloxis — no repository's manifests or lockfiles are read until serve is restarted (said once at this level)",
			"binary", exe, "binary_version", onDisk, "running_version", running, "to_do", "restart serve (aveloxis stop all, then start), so that the running program is the one on disk")
		run.close()
		return &manifestRun{}, err
	case code == manifestExitCPU:
		return run, &errManifestChild{Attempt: model.InventoryAttemptReadCPULimit,
			detail: fmt.Sprintf("the manifest reader stopped itself: reading this repository's manifests and lockfiles used more CPU time than collection.manifest_read_cpu_seconds allows (%s)", limits.CPU)}
	case code == manifestExitMemory:
		return run, &errManifestChild{Attempt: model.InventoryAttemptReadMemoryLimit,
			detail: fmt.Sprintf("the manifest reader stopped itself: reading this repository's manifests and lockfiles held more memory than collection.manifest_read_memory_mb allows (%d MiB)", limits.HeapBytes>>20)}
	case protoErr != nil:
		return run, manifestChildFailed("the manifest reader's output was refused: %v%s", protoErr, stderr.tail())
	case waitErr != nil:
		return run, manifestChildFailed("the manifest reader failed: %v%s", waitErr, stderr.tail())
	case !complete:
		return run, manifestChildFailed("the manifest reader ended without a result%s", stderr.tail())
	}
	logger.Debug("manifest reader finished", "repo_id", req.RepoID, "passes", strings.Join(req.Passes, ","), "cpu", run.cpu.Round(time.Millisecond).String(), "heap_peak_mb", run.heapPeak>>20, "rss_peak_mb", run.maxRSS>>20,
		"took", time.Since(started).Round(time.Millisecond).String(), "answering", answering.spent().Round(time.Millisecond).String())
	return run, nil
}

// exitStatus is a finished child's exit status; -1 when it did not exit
// (it was killed) or err is not an exit.
func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Exited() {
			return ws.ExitStatus()
		}
	}
	return -1
}

// manifestLogRecords and manifestLogBytes are how much of the child's log
// one analysis writes to this process's log. PROVISIONAL (summary/71,
// section 8). The passes log a line for a manifest or a lockfile that could
// not be read and a handful about the whole; a thousand records and a
// mebibyte of text are a repository with a thousand broken manifests. The
// rest is counted and said in one line (runManifestChild). Until 0.29.214 a
// child could write as much log as its memory limit (review round 7).
const (
	manifestLogRecords = 1000
	manifestLogBytes   = 1 << 20
)

// manifestConverse is the exchange with a started child: the request, then
// its frames until every pass asked for is done (complete). Not complete
// with a nil error: the child's output ended first (its exit status says
// why); the run holds the passes that finished. An error: the child sent
// something this protocol does not send; nothing it sent is used.
//
// What this process holds of what the child sends is one frame, the names
// and the declarations (which are no larger than the manifests they were
// read from), and the lockfile pass's heads; that pass's entries and edges
// go to disk as they arrive (lockfile_spool.go).
//
// The names and the declarations are held to the child's own memory limit:
// the child builds each of those results whole before it sends it, so one
// larger than its limit is not one it built. The lockfile pass is not:
// until 0.29.214 every byte received was counted against that limit, and
// the lockfile pass, which the child reads and sends one lockfile at a
// time, was refused for an honest repository of many lockfiles (review
// round 7). It is held instead to the size of the lockfiles in the clone
// (manifestCheck.lockPart), and every pass to the clock.
func manifestConverse(req manifestRequest, limits ManifestLimits, src manifest.PomSource, logger *slog.Logger, out *bufio.Reader, in *os.File, answering *manifestAnswering) (run *manifestRun, complete bool, err error) {
	run = &manifestRun{finished: map[string]bool{}, rootErr: map[string]error{}, passFail: map[string]error{}}
	if len(req.Passes) == 0 {
		return run, false, errors.New("no pass was asked for")
	}
	if err := writeManifestFrame(in, &manifestFrame{T: frameRequest, Request: &req}); err != nil {
		// The child is gone already (it could not even be told): its exit
		// status is the story.
		return run, false, nil
	}
	check := newManifestCheck(req.Dir)
	// cur is the pass the child is on; pending is that pass's result, which
	// becomes the run's only when its done frame arrives.
	cur := 0
	pending := newManifestPending(req.Passes[cur])
	built := run
	defer func() {
		pending.discard()
		if err != nil {
			built.close()
		}
	}()
	asked := 0
	var held int64 // bytes of the names' and the declarations' pieces
	logged, loggedBytes := 0, 0
	for {
		body, err := readManifestFrameBytes(out)
		if err != nil {
			if errors.Is(err, errManifestFrameShort) || errors.Is(err, io.EOF) {
				return run, false, nil // the child ended: its exit status says why
			}
			return nil, false, err
		}
		f, _, err := decodeManifestFrame(body)
		if err != nil {
			return nil, false, err
		}
		pass := req.Passes[cur]
		switch {
		case f.T == frameLog && f.Log != nil:
			if n := manifestLogSize(f.Log); logged >= manifestLogRecords || loggedBytes+n > manifestLogBytes {
				run.logDropped++
			} else {
				logged, loggedBytes = logged+1, loggedBytes+n
				logManifestRecord(logger, f.Log)
			}
		case f.T == framePom && f.Pom != nil && src != nil && pass == manifestPassDeclarations:
			if asked++; asked > req.RemotePomLimit {
				return nil, false, fmt.Errorf("it asked for more than the %d poms one analysis may ask for", req.RemotePomLimit)
			}
			answering.begin()
			pom, outcome := src.RemotePom(f.Pom.Group, f.Pom.Artifact, f.Pom.Version, f.Pom.Declared)
			answering.end()
			ans := &manifestPomAnswer{Outcome: int(outcome), Pom: pom}
			if pom != nil {
				ans.ServedBy = pom.ServedBy
			}
			if err := writeManifestFrame(in, &manifestFrame{T: framePomAnswer, PomAnswer: ans}); err != nil {
				if errors.Is(err, errManifestFrameTooLarge) {
					// A stored pom too large to hand over: not obtained.
					err = writeManifestFrame(in, &manifestFrame{T: framePomAnswer, PomAnswer: &manifestPomAnswer{Outcome: int(manifest.RemoteFailed)}})
				}
				if err != nil {
					return run, false, nil // the child is gone
				}
			}
		case f.T == framePart && f.Part != nil:
			if pass != manifestPassLockfiles {
				if held += int64(len(body)); held > limits.HeapBytes {
					return nil, false, fmt.Errorf("its names and declarations are more than %d bytes, its own memory limit", limits.HeapBytes)
				}
			}
			if err := check.part(pass, f.Part, pending); err != nil {
				return nil, false, err
			}
			if err := pending.add(f.Part, body); err != nil {
				return nil, false, err
			}
		case f.T == frameDone && f.Done != nil:
			if f.Done.Pass != pass {
				return nil, false, fmt.Errorf("it ended the %q pass while on the %q pass", f.Done.Pass, pass)
			}
			run.cpu, run.heapPeak, run.maxRSS = time.Duration(f.Done.CPUNanos), f.Done.HeapPeakBytes, f.Done.MaxRSSBytes
			switch {
			case f.Done.RootError != "":
				run.rootErr[pass] = errors.New(f.Done.RootError)
				run.finished[pass] = true
			case pending.spoolLost:
				if err := check.done(pass, f.Done, pending); err != nil {
					return nil, false, err
				}
				fail := manifestChildFailed("the lockfiles were read and could not be kept on this host: the temporary file for them (in %s) could not be made or written: %v", os.TempDir(), pending.spoolErr)
				run.passFail[pass] = fail
				pending.discard() // nothing of it is handed to the run
			default:
				if err := check.done(pass, f.Done, pending); err != nil {
					return nil, false, err
				}
				pending.into(run, f.Done)
				run.finished[pass] = true
			}
			if cur++; cur == len(req.Passes) {
				pending = nil
				return run, true, nil
			}
			pending.discard()
			pending = newManifestPending(req.Passes[cur])
		default:
			return nil, false, fmt.Errorf("it sent a %q frame, which a child does not send here", f.T)
		}
	}
}

// manifestLogSize is the text of one of the child's log records.
func manifestLogSize(l *manifestLog) int {
	n := len(l.Msg)
	for _, a := range l.Attrs {
		n += len(a.Key) + len(a.Value)
	}
	return n
}

// manifestPending is the result of the pass in hand.
type manifestPending struct {
	names manifestNames
	decl  *manifestDeclared
	locks *lockfileSet
	// present are the lockfiles the name pass has listed.
	present map[string]bool
	// The lockfile pass. heads are the lockfiles begun, with the entries
	// and edges each says it has; gotEntries and gotEdges, how many of each
	// have arrived; lockPaths, every lockfile path seen, parsed or failed;
	// lockBytes, the size on disk of the lockfiles begun, as this process
	// asked the file system.
	heads      []manifestWireLockfile
	gotEntries []int
	gotEdges   []int
	lockPaths  map[string]bool
	lockBytes  int64
	spoolErr   error
	spoolLost  bool // something of the lockfile pass could not be kept
}

// discard lets go of a pass's result that will not be used.
func (r *manifestPending) discard() {
	if r != nil && r.locks != nil {
		r.locks.spool.close()
	}
}

// into hands a finished pass's result to the run.
func (r *manifestPending) into(run *manifestRun, done *manifestDone) {
	switch {
	case r.names != nil:
		run.names = r.names
		run.present, run.presentKnown = r.present, done.LockfilesListed
	case r.decl != nil:
		run.decl = r.decl
	case r.locks != nil:
		r.locks.spool.heads = r.heads
		run.locks = r.locks
		r.locks = nil // the run's now: not this pass's to let go
	}
}

func newManifestPending(pass string) *manifestPending {
	switch pass {
	case manifestPassNames:
		return &manifestPending{names: manifestNames{}, present: map[string]bool{}}
	case manifestPassDeclarations:
		return &manifestPending{decl: &manifestDeclared{}}
	case manifestPassLockfiles:
		r := &manifestPending{locks: &lockfileSet{}, lockPaths: map[string]bool{}}
		r.locks.spool, r.spoolErr = newLockfileSpool()
		if r.spoolErr != nil {
			r.locks.spool = &lockfileSpool{}
		}
		return r
	}
	return &manifestPending{}
}

// add takes a checked piece. body is the frame it arrived in.
func (r *manifestPending) add(p *manifestPart, body []byte) error {
	switch {
	case r.names != nil:
		for _, n := range p.Names {
			if r.names[n.Language] == nil {
				r.names[n.Language] = map[string]int{}
			}
			r.names[n.Language][n.Name] += n.Count
		}
		for _, l := range p.Present {
			r.present[l.Path] = true
		}
	case r.decl != nil:
		d := r.decl
		d.Deps = append(d.Deps, p.Deps...)
		d.Unused = append(d.Unused, p.Unused...)
		for _, f := range p.Failed {
			f.ManifestFailure.NoCarry, f.ManifestFailure.TooLarge = f.NoCarry, f.TooLarge
			d.Failed = append(d.Failed, f.ManifestFailure)
		}
		d.TreeHosts = append(d.TreeHosts, p.TreeHosts...)
		d.NotWhole = append(d.NotWhole, p.NotWhole...)
		d.Repositories = append(d.Repositories, p.Repositories...)
	case r.locks != nil:
		// check.lockPart has held every entry and edge to a lockfile
		// begun, and counted them. What is kept here is the heads and the
		// failures; the piece itself goes to disk.
		for _, h := range p.Lockfiles {
			r.lockPaths[h.Path] = true
			r.heads = append(r.heads, h)
			r.gotEntries, r.gotEdges = append(r.gotEntries, 0), append(r.gotEdges, 0)
		}
		for _, e := range p.Entries {
			r.gotEntries[e.L]++
		}
		for _, e := range p.Edges {
			r.gotEdges[e.L]++
		}
		for _, f := range p.Failed {
			f.ManifestFailure.NoCarry, f.ManifestFailure.TooLarge = f.NoCarry, f.TooLarge
			r.locks.Failed = append(r.locks.Failed, f.ManifestFailure)
			r.lockPaths[f.Path] = true
		}
		if len(p.Lockfiles)+len(p.Entries)+len(p.Edges) > 0 {
			// A spool that cannot be made or written is this HOST's
			// trouble, not the reader's: the pass goes on being checked,
			// nothing more of it is kept, and it ends as its own failure
			// (passFail), with the other passes standing.
			if r.spoolErr == nil {
				r.spoolErr = r.locks.spool.add(body)
			}
			if r.spoolErr != nil {
				r.spoolLost = true
			}
		}
	}
	return nil
}

// manifestCheck is what the parent holds a child's result to. The child is
// this program, but what it read was written by the repository's owner, and
// the parent takes nothing from it on trust that it can check: a path is
// one inside the clone, and a kind, a language, a package manager and a
// reason are ones this program has. What a field SAYS (a name, a version, a
// requirement) is checked where it always was, by the code this result is
// handed to (the storable-text rule, the name grammar, the purl).
type manifestCheck struct {
	kinds, languages, managers map[string]bool
	// dir is the clone; lockfileSize asks the file system how large a
	// lockfile in it is (a variable for a test).
	dir          string
	lockfileSize func(dir, rel string) (int64, error)
}

func newManifestCheck(dir string) *manifestCheck {
	c := &manifestCheck{kinds: map[string]bool{}, languages: map[string]bool{}, managers: map[string]bool{}, dir: dir, lockfileSize: manifestLockfileSize}
	for _, k := range manifest.Roster() {
		c.kinds[k.Name] = true
		if k.Language != "" {
			c.languages[k.Language] = true
		}
		if k.Ecosystem != "" {
			c.managers[k.Ecosystem] = true
		}
	}
	return c
}

// manifestLockfileSize is cloneLockfileSize; a variable so that a test of a
// child that misbehaves need not make the files it lies about.
var manifestLockfileSize = cloneLockfileSize

// cloneLockfileSize is the size of a file of the clone that the child says
// is a lockfile it read: asked of the file system, which is the one thing
// about the clone this process learns for itself (no content is read). A
// path that is not a regular file there is not a lockfile the pass read.
func cloneLockfileSize(dir, rel string) (int64, error) {
	info, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(rel)))
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, errors.New("not a regular file")
	}
	return info.Size(), nil
}

// cloneRelative reports whether p is a path inside the clone, as the pass
// writes one (wirePathOK): relative, '/'-clean, no step out, and text that
// is stored as it is. Until 0.29.214 "a/./x", "a//x" and "." passed, and so
// did a path with an escape character in it (review round 7).
func cloneRelative(p string) bool { return wirePathOK(p) }

// addCount adds two counts and refuses a sum that is not a count.
func addCount(a, b int) (int, bool) {
	if a < 0 || b < 0 || a > math.MaxInt-b {
		return 0, false
	}
	return a + b, true
}

func (c *manifestCheck) part(pass string, p *manifestPart, pending *manifestPending) error {
	if pass == manifestPassLockfiles {
		return c.lockPart(p, pending)
	}
	if len(p.Lockfiles)+len(p.Entries)+len(p.Edges) > 0 {
		return errors.New("a manifest pass sent lockfiles")
	}
	if pass == manifestPassNames {
		if len(p.Deps)+len(p.Unused)+len(p.Failed)+len(p.TreeHosts)+len(p.NotWhole)+len(p.Repositories) > 0 {
			return errors.New("the name pass sent declarations")
		}
		for _, n := range p.Names {
			if !c.languages[n.Language] || n.Count <= 0 {
				return fmt.Errorf("a name count is not one the roster gives (language %q, count %d)", n.Language, n.Count)
			}
			if _, ok := addCount(pending.names[n.Language][n.Name], n.Count); !ok {
				return fmt.Errorf("a name's counts do not add up to a count (language %q)", n.Language)
			}
		}
		seen := map[string]bool{}
		for _, l := range p.Present {
			if _, ok := lockfileKinds[l.Kind]; !ok || !cloneRelative(l.Path) || path.Base(l.Path) != l.Kind || pending.present[l.Path] || seen[l.Path] {
				return fmt.Errorf("a lockfile listed is not one the roster has, or not once (path %q, kind %q)", l.Path, l.Kind)
			}
			seen[l.Path] = true
		}
		return nil
	}
	if len(p.Names)+len(p.Present) > 0 {
		return errors.New("the declarations pass sent name counts")
	}
	for _, list := range [][]model.LibyearDep{p.Deps, p.Unused} {
		for _, d := range list {
			if !cloneRelative(d.ManifestPath) {
				return fmt.Errorf("a declaration names a manifest that is not a path inside the clone: %q", d.ManifestPath)
			}
			if !c.managers[d.Manager] {
				return fmt.Errorf("a declaration is of a package manager the roster does not have: %q", d.Manager)
			}
			if d.VersionReason != "" && !slices.Contains(model.VersionReasons(), d.VersionReason) {
				return fmt.Errorf("a declaration gives a reason this program does not have: %q", d.VersionReason)
			}
		}
	}
	for _, f := range p.Failed {
		// A failure's path may be a sanitized spelling (NoCarry): still a
		// relative path with no step out.
		if !cloneRelative(f.Path) || !c.kinds[f.Kind] || (f.Reason != model.ManifestFailedRead && f.Reason != model.ManifestFailedParse) || f.RowsCarried != 0 {
			return fmt.Errorf("a failed manifest is not one the pass records (path %q, kind %q, reason %q)", f.Path, f.Kind, f.Reason)
		}
	}
	for _, h := range p.TreeHosts {
		if !cloneRelative(h.Root) {
			return fmt.Errorf("a Maven tree's root is not a path inside the clone: %q", h.Root)
		}
	}
	for _, g := range p.NotWhole {
		if !cloneRelative(g.Root) {
			return fmt.Errorf("a Maven tree's root is not a path inside the clone: %q", g.Root)
		}
	}
	for _, r := range p.Repositories {
		if !cloneRelative(r.ManifestPath) || (r.Kind != manifest.RepositoryForDependencies && r.Kind != manifest.RepositoryForPlugins) {
			return fmt.Errorf("a declared repository is not one the pass records (manifest %q, kind %q)", r.ManifestPath, r.Kind)
		}
	}
	return nil
}

// lockPart holds a piece of the lockfile pass to what that pass sends: a
// lockfile is a roster kind at a path inside the clone whose file name is
// that kind, once, and a regular file there; an entry or an edge belongs to
// the lockfile the child is on (the last begun before this piece, or one
// begun in it), which has said how many it has; an entry's own ecosystem
// and scope are ones a parser gives; and all the pieces together are no
// more than lockfileWireFactor times the lockfiles they are of, and a
// frame.
func (c *manifestCheck) lockPart(p *manifestPart, pending *manifestPending) error {
	if len(p.Deps)+len(p.Unused)+len(p.TreeHosts)+len(p.NotWhole)+len(p.Repositories)+len(p.Names)+len(p.Present) > 0 {
		return errors.New("the lockfile pass sent manifest results")
	}
	before := len(pending.heads)
	heads := append(pending.heads[:before:before], p.Lockfiles...)
	seen := map[string]bool{}
	lockPath := func(at, kind string) bool {
		return cloneRelative(at) && path.Base(at) == kind && !pending.lockPaths[at] && !seen[at]
	}
	for _, h := range p.Lockfiles {
		spec, ok := lockfileKinds[h.Kind]
		if !ok || !lockPath(h.Path, h.Kind) || h.Ecosystem != spec.ecosystem {
			return fmt.Errorf("a lockfile is not one the roster has, or not once (path %q, kind %q, ecosystem %q)", h.Path, h.Kind, h.Ecosystem)
		}
		seen[h.Path] = true
		if h.Entries < 0 || h.Edges < 0 || h.Aliased < 0 || h.NotRegistry < 0 || h.NotAVersion < 0 || (spec.binaryOnly && h.Entries+h.Edges > 0) {
			return fmt.Errorf("a lockfile's counts are not counts (path %q)", h.Path)
		}
		size, err := c.lockfileSize(c.dir, h.Path)
		if err != nil {
			return fmt.Errorf("a lockfile it says it read is not a file of the clone (path %q): %v", h.Path, err)
		}
		// A binary lockfile is never read by the child and sends nothing
		// but its name: it earns the spool no room.
		if !spec.binaryOnly {
			pending.lockBytes += size
		}
	}
	entries, edges := map[int]int{}, map[int]int{}
	// The child reads one lockfile at a time and sends it whole before the
	// next: an entry or an edge of an earlier lockfile is not its.
	current := func(l int) bool { return l >= 0 && l < len(heads) && l >= before-1 }
	for _, e := range p.Entries {
		if !current(e.L) {
			return errors.New("a lockfile entry belongs to no lockfile sent, or to one the child had finished")
		}
		switch {
		case e.Ecosystem == "":
		case e.Ecosystem == bioconductorEcosystem && heads[e.L].Kind == renvLockKind:
		default:
			return fmt.Errorf("a lockfile entry is of an ecosystem its lockfile does not hold: %q in %q", e.Ecosystem, heads[e.L].Kind)
		}
		if e.Scope != "" && e.Scope != lockfileScopeDev {
			return fmt.Errorf("a lockfile entry has a scope no parser gives: %q", e.Scope)
		}
		entries[e.L]++
	}
	for _, e := range p.Edges {
		if !current(e.L) {
			return errors.New("a lockfile edge belongs to no lockfile sent, or to one the child had finished")
		}
		edges[e.L]++
	}
	had := func(l int) (int, int) {
		if l < before {
			return pending.gotEntries[l], pending.gotEdges[l]
		}
		return 0, 0
	}
	for l, n := range entries {
		if e, _ := had(l); e+n > heads[l].Entries {
			return fmt.Errorf("a lockfile sent more entries than it said it has (%q)", heads[l].Path)
		}
	}
	for l, n := range edges {
		if _, e := had(l); e+n > heads[l].Edges {
			return fmt.Errorf("a lockfile sent more edges than it said it has (%q)", heads[l].Path)
		}
	}
	for _, f := range p.Failed {
		// NoCarry: a lockfile at a path that cannot be stored, failed under
		// its sanitized path (0.29.215). TooLarge: one not read for its
		// size; a file that was not read failed to be READ.
		if _, ok := lockfileKinds[f.Kind]; !ok || !lockPath(f.Path, f.Kind) || f.RowsCarried != 0 ||
			(f.Reason != model.ManifestFailedRead && f.Reason != model.ManifestFailedParse) ||
			((f.NoCarry || f.TooLarge) && f.Reason != model.ManifestFailedRead) {
			return fmt.Errorf("a failed lockfile is not one the pass records (path %q, kind %q, reason %q)", f.Path, f.Kind, f.Reason)
		}
		seen[f.Path] = true
	}
	if spool := pending.locks.spool; spool != nil && len(p.Lockfiles)+len(p.Entries)+len(p.Edges) > 0 {
		if allowed := lockfileWireFactor*pending.lockBytes + manifestFrameLimit; spool.written > allowed {
			return fmt.Errorf("the lockfile pass sent more than %d times the %d bytes of the lockfiles it is of", lockfileWireFactor, pending.lockBytes)
		}
	}
	return nil
}

// done holds a finished pass to its own counts.
func (c *manifestCheck) done(pass string, d *manifestDone, pending *manifestPending) error {
	switch pass {
	case manifestPassDeclarations:
		decl := pending.decl
		decl.Found, decl.Parsed, decl.RemoteNotAsked = d.Found, d.Parsed, d.RemoteNotAsked
		sum, ok := addCount(d.Parsed, len(decl.Failed))
		if d.Found < 0 || d.Parsed < 0 || d.RemoteNotAsked < 0 || !ok || d.Found != sum {
			return fmt.Errorf("its counts do not add up: %d manifests found, %d read, %d failed", d.Found, d.Parsed, len(decl.Failed))
		}
	case manifestPassLockfiles:
		if d.Lockfiles != len(pending.heads) {
			return fmt.Errorf("its counts do not add up: %d lockfiles parsed, %d sent", d.Lockfiles, len(pending.heads))
		}
		for l, h := range pending.heads {
			if pending.gotEntries[l] != h.Entries || pending.gotEdges[l] != h.Edges {
				return fmt.Errorf("a lockfile sent fewer entries or edges than it said it has (%q)", h.Path)
			}
		}
	}
	return nil
}

// logManifestRecord logs one of the child's records with the analysis's own
// logger.
func logManifestRecord(logger *slog.Logger, l *manifestLog) {
	attrs := make([]any, 0, len(l.Attrs))
	for _, a := range l.Attrs {
		switch a.Kind {
		case "i":
			if n, err := strconv.ParseInt(a.Value, 10, 64); err == nil {
				attrs = append(attrs, slog.Int64(a.Key, n))
				continue
			}
		case "b":
			if b, err := strconv.ParseBool(a.Value); err == nil {
				attrs = append(attrs, slog.Bool(a.Key, b))
				continue
			}
		}
		attrs = append(attrs, slog.String(a.Key, a.Value))
	}
	logger.Log(context.Background(), slog.Level(l.Level), l.Msg, attrs...)
}

// cappedBuffer keeps the first limit bytes written to it.
type cappedBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := b.limit - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

// text is what the child wrote to its standard error.
func (b *cappedBuffer) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.buf))
}

// tail is what the child wrote to its standard error, for a log line.
func (b *cappedBuffer) tail() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := strings.TrimSpace(string(b.buf)); s != "" {
		return "; it wrote: " + s
	}
	return ""
}
