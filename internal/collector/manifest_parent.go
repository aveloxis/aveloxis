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
	"os"
	"os/exec"
	"path"
	"path/filepath"
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
	exe, err := os.Executable()
	if err != nil {
		return "", nil, err
	}
	return exe, []string{ManifestChildArg}, nil
}

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
	// cpu, heapPeak and maxRSS are what the child says the passes cost it.
	cpu      time.Duration
	heapPeak int64
	maxRSS   int64
}

// passErr says whether a pass's result may be used: nil when the pass
// finished; an *errManifestRoot when it could not read the clone's root;
// otherwise why the child did not get that far (runErr, what
// runManifestChild returned with this run).
func (r *manifestRun) passErr(pass string, runErr error) error {
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

	exe, args, err := manifestChildCommand()
	if err != nil {
		return nil, manifestChildFailed("the manifest reader could not be started: this program's own path is not known: %v", err)
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(childCtx, exe, args...)
	cmd.Env = manifestChildEnv()
	cmd.Dir = string(filepath.Separator) // not serve's working directory: the child reads no relative path
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, manifestChildFailed("the manifest reader could not be started: %v", err)
	}
	defer inW.Close()
	cmd.Stdin = inR
	stderr := &cappedBuffer{limit: manifestStderrLimit}
	cmd.Stderr = stderr
	started := time.Now()
	swept, err := startSweptCommand(cmd)
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
	var answering atomic.Int64
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
				if time.Since(started)-time.Duration(answering.Load()) > allowance {
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
		run = &manifestRun{}
	}
	_ = inW.Close()
	waitErr := swept.Wait()

	switch {
	case ctx.Err() != nil:
		return nil, ctx.Err() // a shutdown, not a failure
	case timedOut.Load():
		return run, &errManifestChild{Attempt: model.InventoryAttemptReadTimedOut,
			detail: fmt.Sprintf("the manifest reader was stopped: it had run for %s by the clock, not counting %s the analysis spent answering it, and may run for %s (%d times collection.manifest_read_cpu_seconds, which is %s, and %s)",
				time.Since(started).Round(time.Second), time.Duration(answering.Load()).Round(time.Second), allowance, manifestWallFactor, limits.CPU, manifestWallFloor)}
	}
	switch code := exitStatus(waitErr); {
	case code == manifestExitVersion:
		// The file this process started is not this process's version.
		onDisk := stderr.text()
		err := &errManifestChild{Attempt: model.InventoryAttemptReadFailed, Stale: true,
			detail: fmt.Sprintf("the aveloxis binary on disk (%s) is version %s, this process is version %s: restart serve", exe, onDisk, manifestParentVersion)}
		level := slog.LevelDebug
		if manifestStaleSaid.CompareAndSwap(false, true) {
			level = slog.LevelError
		}
		logger.Log(ctx, level, "the manifest reader is another version of aveloxis — no repository's manifests or lockfiles are read until serve is restarted (said once at this level)",
			"binary", exe, "binary_version", onDisk, "running_version", manifestParentVersion, "to_do", "restart serve (aveloxis stop all, then start), so that the running program is the one on disk")
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
		"took", time.Since(started).Round(time.Millisecond).String(), "answering", time.Duration(answering.Load()).Round(time.Millisecond).String())
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

// manifestConverse is the exchange with a started child: the request, then
// its frames until every pass asked for is done (complete). Not complete
// with a nil error: the child's output ended first (its exit status says
// why); the run holds the passes that finished. An error: the child sent
// something this protocol does not send, or more than it may; nothing it
// sent is used.
func manifestConverse(req manifestRequest, limits ManifestLimits, src manifest.PomSource, logger *slog.Logger, out *bufio.Reader, in *os.File, answering *atomic.Int64) (run *manifestRun, complete bool, err error) {
	run = &manifestRun{finished: map[string]bool{}, rootErr: map[string]error{}}
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
	var received int64
	asked := 0
	for {
		f, size, err := readManifestFrame(out)
		if err != nil {
			if errors.Is(err, errManifestFrameShort) || errors.Is(err, io.EOF) {
				return run, false, nil // the child ended: its exit status says why
			}
			return nil, false, err
		}
		// Everything the child sends is counted against what it could have
		// held: a result larger than its heap limit is not one it built.
		if received += int64(size); received > limits.HeapBytes {
			return nil, false, fmt.Errorf("it sent more than %d bytes, its own memory limit", limits.HeapBytes)
		}
		pass := req.Passes[cur]
		switch {
		case f.T == frameLog && f.Log != nil:
			logManifestRecord(logger, f.Log)
		case f.T == framePom && f.Pom != nil && src != nil && pass == manifestPassDeclarations:
			if asked++; asked > req.RemotePomLimit {
				return nil, false, fmt.Errorf("it asked for more than the %d poms one analysis may ask for", req.RemotePomLimit)
			}
			began := time.Now()
			pom, outcome := src.RemotePom(f.Pom.Group, f.Pom.Artifact, f.Pom.Version, f.Pom.Declared)
			answering.Add(int64(time.Since(began)))
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
			if err := check.part(pass, f.Part, pending); err != nil {
				return nil, false, err
			}
			pending.add(f.Part)
		case f.T == frameDone && f.Done != nil:
			if f.Done.Pass != pass {
				return nil, false, fmt.Errorf("it ended the %q pass while on the %q pass", f.Done.Pass, pass)
			}
			run.cpu, run.heapPeak, run.maxRSS = time.Duration(f.Done.CPUNanos), f.Done.HeapPeakBytes, f.Done.MaxRSSBytes
			if f.Done.RootError != "" {
				run.rootErr[pass] = errors.New(f.Done.RootError)
			} else {
				if err := check.done(pass, f.Done, pending); err != nil {
					return nil, false, err
				}
				pending.into(run)
			}
			run.finished[pass] = true
			if cur++; cur == len(req.Passes) {
				return run, true, nil
			}
			pending = newManifestPending(req.Passes[cur])
		default:
			return nil, false, fmt.Errorf("it sent a %q frame, which a child does not send here", f.T)
		}
	}
}

// manifestPending is the result of the pass in hand.
type manifestPending struct {
	names manifestNames
	decl  *manifestDeclared
	locks *lockfileSet
	// heads are the lockfiles begun, with the entries and edges each says
	// it has; lockPaths, every lockfile path seen, parsed or failed.
	heads     []manifestWireLockfile
	lockPaths map[string]bool
}

// into hands a finished pass's result to the run.
func (r *manifestPending) into(run *manifestRun) {
	switch {
	case r.names != nil:
		run.names = r.names
	case r.decl != nil:
		run.decl = r.decl
	case r.locks != nil:
		run.locks = r.locks
	}
}

func newManifestPending(pass string) *manifestPending {
	switch pass {
	case manifestPassNames:
		return &manifestPending{names: manifestNames{}}
	case manifestPassDeclarations:
		return &manifestPending{decl: &manifestDeclared{}}
	case manifestPassLockfiles:
		return &manifestPending{locks: &lockfileSet{}, lockPaths: map[string]bool{}}
	}
	return &manifestPending{}
}

func (r *manifestPending) add(p *manifestPart) {
	switch {
	case r.names != nil:
		for _, n := range p.Names {
			if r.names[n.Language] == nil {
				r.names[n.Language] = map[string]int{}
			}
			r.names[n.Language][n.Name] += n.Count
		}
	case r.decl != nil:
		d := r.decl
		d.Deps = append(d.Deps, p.Deps...)
		d.Unused = append(d.Unused, p.Unused...)
		for _, f := range p.Failed {
			f.ManifestFailure.NoCarry = f.NoCarry
			d.Failed = append(d.Failed, f.ManifestFailure)
		}
		d.TreeHosts = append(d.TreeHosts, p.TreeHosts...)
		d.NotWhole = append(d.NotWhole, p.NotWhole...)
		d.Repositories = append(d.Repositories, p.Repositories...)
	case r.locks != nil:
		// check.part has held every entry and edge to a lockfile begun.
		l := r.locks
		for _, h := range p.Lockfiles {
			res := &LockfileResult{Kind: h.Kind, Ecosystem: h.Ecosystem, DirectKnown: h.DirectKnown,
				Aliased: h.Aliased, NotRegistry: h.NotRegistry, NotAVersion: h.NotAVersion}
			l.Parsed = append(l.Parsed, parsedLockfile{Path: h.Path, Result: res})
			r.lockPaths[h.Path] = true
			r.heads = append(r.heads, h)
		}
		for _, e := range p.Entries {
			res := l.Parsed[e.L].Result
			res.Entries = append(res.Entries, LockfileEntry{Name: e.Name, Version: e.Version, Direct: e.Direct,
				Namespace: e.Namespace, Ecosystem: e.Ecosystem, Scope: e.Scope})
		}
		for _, e := range p.Edges {
			res := l.Parsed[e.L].Result
			res.Edges = append(res.Edges, LockfileEdge{ParentName: e.ParentName, ParentVersion: e.ParentVersion,
				ChildName: e.ChildName, Constraint: e.Constraint})
		}
		for _, f := range p.Failed {
			l.Failed = append(l.Failed, f.ManifestFailure)
			r.lockPaths[f.Path] = true
		}
	}
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
}

func newManifestCheck(string) *manifestCheck {
	c := &manifestCheck{kinds: map[string]bool{}, languages: map[string]bool{}, managers: map[string]bool{}}
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

// cloneRelative reports whether p is a path inside the clone, as the pass
// writes one: relative, forward slashes, no step out.
func cloneRelative(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") || filepath.IsAbs(p) {
		return false
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." {
			return false
		}
	}
	return true
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
		}
		return nil
	}
	if len(p.Names) > 0 {
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
			switch d.VersionReason {
			case "", model.VersionReasonInheritedNotFound, model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedRepositoryRefused:
			default:
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
// that kind, once; an entry or an edge belongs to a lockfile already begun,
// which has said how many it has; an entry's own ecosystem and scope are
// ones a parser gives.
func (c *manifestCheck) lockPart(p *manifestPart, pending *manifestPending) error {
	if len(p.Deps)+len(p.Unused)+len(p.TreeHosts)+len(p.NotWhole)+len(p.Repositories)+len(p.Names) > 0 {
		return errors.New("the lockfile pass sent manifest results")
	}
	heads := append(pending.heads[:len(pending.heads):len(pending.heads)], p.Lockfiles...)
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
	}
	entries, edges := map[int]int{}, map[int]int{}
	for _, e := range p.Entries {
		if e.L < 0 || e.L >= len(heads) {
			return errors.New("a lockfile entry belongs to no lockfile sent")
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
		if e.L < 0 || e.L >= len(heads) {
			return errors.New("a lockfile edge belongs to no lockfile sent")
		}
		edges[e.L]++
	}
	had := func(l int) (int, int) {
		if l < len(pending.locks.Parsed) {
			return len(pending.locks.Parsed[l].Result.Entries), len(pending.locks.Parsed[l].Result.Edges)
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
		if _, ok := lockfileKinds[f.Kind]; !ok || !lockPath(f.Path, f.Kind) || f.NoCarry || f.RowsCarried != 0 ||
			(f.Reason != model.ManifestFailedRead && f.Reason != model.ManifestFailedParse) {
			return fmt.Errorf("a failed lockfile is not one the pass records (path %q, kind %q, reason %q)", f.Path, f.Kind, f.Reason)
		}
		seen[f.Path] = true
	}
	return nil
}

// done holds a finished pass to its own counts.
func (c *manifestCheck) done(pass string, d *manifestDone, pending *manifestPending) error {
	switch pass {
	case manifestPassDeclarations:
		decl := pending.decl
		decl.Found, decl.Parsed, decl.RemoteNotAsked = d.Found, d.Parsed, d.RemoteNotAsked
		if d.Found < 0 || d.Parsed < 0 || d.RemoteNotAsked < 0 || d.Found != d.Parsed+len(decl.Failed) {
			return fmt.Errorf("its counts do not add up: %d manifests found, %d read, %d failed", d.Found, d.Parsed, len(decl.Failed))
		}
	case manifestPassLockfiles:
		if d.Lockfiles != len(pending.heads) {
			return fmt.Errorf("its counts do not add up: %d lockfiles parsed, %d sent", d.Lockfiles, len(pending.heads))
		}
		for l, h := range pending.heads {
			res := pending.locks.Parsed[l].Result
			if len(res.Entries) != h.Entries || len(res.Edges) != h.Edges {
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
