// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The child that reads one repository's clone (0.29.213): its dependency
// manifests, and its lockfiles.
//
// It is this same program, started again with one argument
// (ManifestChildArg), and it holds nothing: it opens no database and no
// socket, reads no configuration, and is started with an empty environment.
// It is told a clone's directory on its standard input, reads that clone's
// manifests (manifest_pass.go) and lockfiles (lockfile_scan.go), asks its
// parent for the poms that are not in the clone, and writes what it read to
// its standard output, a pass at a time.
//
// It exists so that the text of a repository's build files and lockfiles
// cannot hold a collection worker, or the memory of the host, for longer or for more than
// the operator has said: the child watches its own CPU time and heap and
// ends itself when either passes its limit, with an exit status that says
// which, and its parent ends it when it has been running too long by the
// clock (manifest_parent.go).

// ManifestChildArg is the argument that makes this program the child. The
// command line is the same for the real binary (a hidden subcommand of
// cmd/aveloxis) and for a test binary (RunManifestChildIfAsked in its
// TestMain), so the tests start exactly what production starts.
const ManifestChildArg = "manifest-read"

// The child's exit statuses. Any other status is a crash.
const (
	manifestExitOK       = 0
	manifestExitProtocol = 64 // it could not read its request, or write its result
	manifestExitVersion  = 65 // the request is another version's: this is not the program that asked
	manifestExitCPU      = 70 // it used its CPU time
	manifestExitMemory   = 71 // its heap passed its limit
	manifestExitOrphan   = 72 // the analysis that started it is gone
)

// manifestWatchEvery is how often the child looks at its own CPU time and
// heap. PROVISIONAL (summary/71, section 8). It is how far past a limit the
// child can go: at most this much CPU time on every thread, and what its
// readers allocate in this long while they parse, which measured about a
// gigabyte a second for the densest, so a tenth of a gibibyte at the
// default. NOT the size of a file: no file is read whose size alone is more
// than the limit leaves (cloneFileTooLarge; until 0.29.214 a file was read
// whole between two looks, and a 6 GiB file took the child 2 GiB past any
// limit). A look costs about two microseconds (a getrusage call and one
// runtime metric), a fifty-thousandth of the interval.
const manifestWatchEvery = 100 * time.Millisecond

// manifestSoftHeapShare is where the child sets the Go runtime's own soft
// memory limit, as a share of its heap limit: the collector then works
// harder as the heap nears the limit, so that what the watchdog measures
// near the limit is what is really held and not garbage awaiting a
// collection. Nine tenths leaves the collector a tenth to work in.
const (
	manifestSoftHeapNum = 9
	manifestSoftHeapDen = 10
)

// RunManifestChildIfAsked makes the calling program the child when it was
// started as one, and never returns in that case. For the top of a test
// binary's TestMain; cmd/aveloxis has the subcommand.
func RunManifestChildIfAsked() {
	if len(os.Args) == 2 && os.Args[1] == ManifestChildArg {
		os.Exit(ManifestChildMain(os.Stdin, os.Stdout))
	}
}

// ManifestChildMain is the child: it serves one request and returns its
// exit status.
func ManifestChildMain(in io.Reader, out io.Writer) int {
	c := &manifestChild{in: bufio.NewReader(in), out: bufio.NewWriter(out)}
	body, err := readManifestFrameBytes(c.in)
	if err != nil {
		return manifestExitProtocol
	}
	// Whose request is it? Asked before the request is read as a request,
	// and leniently: a program of another version may send fields this one
	// does not know, and "not my version" is the answer that says what to
	// do about it. The child is this program's file on disk, which is not
	// the running program after a `go install` or an upgrade under a serve
	// that was not stopped.
	var asked struct {
		Request struct {
			Version string `json:"version"`
		} `json:"request"`
	}
	if json.Unmarshal(body, &asked) == nil && asked.Request.Version != db.ToolVersion {
		// Its own version, for the analysis that asked to log.
		fmt.Fprintln(os.Stderr, db.ToolVersion)
		return manifestExitVersion
	}
	first, _, err := decodeManifestFrame(body)
	if err != nil || first.T != frameRequest || first.Request == nil {
		return manifestExitProtocol
	}
	req := first.Request
	if req.CPULimitNanos <= 0 || req.HeapLimitBytes <= 0 {
		// There is no child without limits.
		return manifestExitProtocol
	}
	debug.SetMemoryLimit(req.HeapLimitBytes / manifestSoftHeapDen * manifestSoftHeapNum)
	// No file is read whose size alone is more than the limit leaves
	// (cloneFileTooLarge).
	cloneReadHeapLimit.Store(req.HeapLimitBytes)
	defer cloneReadHeapLimit.Store(0) // for a test that runs the child inside its own process
	// Nor is one read past it by a reader that opens a file for itself, or
	// whose file grew after that check (0.29.214): every read the manifest
	// package makes is held to the room the limit leaves.
	defer manifest.SetReadBound(cloneReadRoom)()
	stop := make(chan struct{})
	defer close(stop)
	orphaned := func() bool { return req.ParentPID != 0 && os.Getppid() != req.ParentPID }
	go manifestWatchdog(time.Duration(req.CPULimitNanos), req.HeapLimitBytes, stop, os.Exit, &c.heapPeak, orphaned)

	if err := c.serve(req); err != nil {
		return manifestExitProtocol
	}
	return manifestExitOK
}

// manifestWatchdog ends the process when it has used its CPU time, when its
// heap has passed its limit, and when the process that started it is gone
// (orphaned: its parent is another process now). The last is for a serve
// that was killed outright: nothing is left to read what this process would
// write, and a subprocess does not outlive the one it works for. exit is
// os.Exit.
func manifestWatchdog(cpuLimit time.Duration, heapLimit int64, stop <-chan struct{}, exit func(int), peak *atomic.Int64, orphaned func() bool) {
	tick := time.NewTicker(manifestWatchEvery)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		if orphaned() {
			exit(manifestExitOrphan)
			return
		}
		if processCPUTime() > cpuLimit {
			exit(manifestExitCPU)
			return
		}
		heap := processHeapBytes()
		if heap > peak.Load() {
			peak.Store(heap)
		}
		if heap > heapLimit {
			exit(manifestExitMemory)
			return
		}
	}
}

// processCPUTime is the CPU time this process has used, on all its threads,
// in user and system mode: getrusage(RUSAGE_SELF), which Linux and macOS
// both have. Time spent waiting (for the parent's answer to a pom, for a
// disk) is not CPU time, which is why the limit is on this and not on the
// clock.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// processMaxRSS is the most memory this process has had resident:
// getrusage's ru_maxrss, which macOS counts in bytes and Linux in kibibytes.
// It is reported, never judged: the limit is on the heap, which the child
// can read at any moment, and this is the system's own high-water mark of
// what that heap cost.
func processMaxRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return int64(ru.Maxrss)
	}
	return int64(ru.Maxrss) << 10
}

// processHeapBytes is the memory the Go heap's objects occupy, live or not
// yet collected.
func processHeapBytes() int64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return int64(sample[0].Value.Uint64())
}

type manifestChild struct {
	in  *bufio.Reader
	out *bufio.Writer
	mu  sync.Mutex
	// failed: a frame could not be written or read. The pass goes on to
	// its end (it cannot be interrupted) and the child then exits with the
	// protocol status.
	failed error
	// heapPeak is the largest heap the watchdog has seen.
	heapPeak atomic.Int64
}

func (c *manifestChild) send(f *manifestFrame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return
	}
	if err := writeManifestFrame(c.out, f); err != nil {
		c.failed = err
		return
	}
	if err := c.out.Flush(); err != nil {
		c.failed = err
	}
}

func (c *manifestChild) serve(req *manifestRequest) error {
	if len(req.Passes) == 0 {
		return errors.New("no pass was asked for")
	}
	logger := slog.New(&manifestLogHandler{child: c})
	for _, pass := range req.Passes {
		done := &manifestDone{Pass: pass}
		parts := &manifestPartWriter{child: c}
		switch pass {
		case manifestPassNames:
			c.serveNames(req, done, parts)
		case manifestPassDeclarations:
			c.serveDeclarations(req, done, parts, logger)
		case manifestPassLockfiles:
			c.serveLockfiles(req, done, parts, logger)
		default:
			return fmt.Errorf("no such pass: %q", pass)
		}
		parts.flush()
		done.CPUNanos, done.HeapPeakBytes, done.MaxRSSBytes = int64(processCPUTime()), max(c.heapPeak.Load(), processHeapBytes()), processMaxRSS()
		c.send(&manifestFrame{T: frameDone, Done: done})
		if c.failed != nil {
			break // nothing is reading: the passes left would be read for no one
		}
	}
	return c.failed
}

func (c *manifestChild) serveNames(req *manifestRequest, done *manifestDone, parts *manifestPartWriter) {
	names, err := readManifestNames(req.Dir)
	if err != nil {
		done.RootError = err.Error()
		return
	}
	for lang, counts := range names {
		for name, n := range counts {
			if len(name) > manifestItemLimit {
				continue // text nothing stores as a name (manifestItemLimit)
			}
			parts.add(func(p *manifestPart) { p.Names = append(p.Names, manifestNameCount{lang, name, n}) }, len(lang)+len(name))
		}
	}
	// The lockfiles the clone holds, for the stored rows to be held to if
	// the lockfile pass does not finish.
	present, err := presentLockfiles(req.Dir)
	if err != nil {
		done.RootError = err.Error()
		return
	}
	done.LockfilesListed = true
	for _, l := range present {
		parts.add(func(p *manifestPart) { p.Present = append(p.Present, l) }, len(l.Path)+len(l.Kind))
	}
}

func (c *manifestChild) serveDeclarations(req *manifestRequest, done *manifestDone, parts *manifestPartWriter, logger *slog.Logger) {
	var src manifest.PomSource
	if req.PomSource {
		src = &manifestChildPomSource{child: c}
	}
	got, err := readManifestDeclarations(req.Dir, manifest.Options{DevBuildDeps: req.DevBuildDeps, GitHubActionsDeps: req.GitHubActionsDeps},
		src, req.RemotePomLimit, logger, req.RepoID)
	if err != nil {
		done.RootError = err.Error()
		return
	}
	done.Found, done.Parsed, done.RemoteNotAsked = got.Found, got.Parsed, got.RemoteNotAsked
	for _, d := range got.Deps {
		parts.add(func(p *manifestPart) { p.Deps = append(p.Deps, d) }, libyearDepBytes(d))
	}
	for _, d := range got.Unused {
		parts.add(func(p *manifestPart) { p.Unused = append(p.Unused, d) }, libyearDepBytes(d))
	}
	for _, f := range got.Failed {
		parts.add(func(p *manifestPart) {
			p.Failed = append(p.Failed, manifestWireFailure{ManifestFailure: f, NoCarry: f.NoCarry})
		}, len(f.Path)+len(f.Kind)+len(f.Reason))
	}
	for _, h := range got.TreeHosts {
		n := len(h.Root)
		for _, host := range h.Hosts {
			n += len(host)
		}
		parts.add(func(p *manifestPart) { p.TreeHosts = append(p.TreeHosts, h) }, n)
	}
	for _, g := range got.NotWhole {
		parts.add(func(p *manifestPart) { p.NotWhole = append(p.NotWhole, g) }, len(g.Root)+len(g.Reason))
	}
	for _, r := range got.Repositories {
		parts.add(func(p *manifestPart) { p.Repositories = append(p.Repositories, r) }, len(r.ManifestPath)+len(r.URL)+len(r.Kind)+len(r.NotFetched))
	}
}

// serveLockfiles is the lockfile pass. Each lockfile is sent as it is
// parsed and then let go, so the child holds one at a time: the file's
// text, what its parser made of it, and one piece on its way out.
func (c *manifestChild) serveLockfiles(req *manifestRequest, done *manifestDone, parts *manifestPartWriter, logger *slog.Logger) {
	sent := 0
	failed, err := walkLockfiles(req.Dir, logger, func(pl parsedLockfile) {
		l, res := sent, pl.Result
		sent++
		parts.add(func(p *manifestPart) {
			p.Lockfiles = append(p.Lockfiles, manifestWireLockfile{Path: pl.Path, Kind: res.Kind, Ecosystem: res.Ecosystem,
				DirectKnown: res.DirectKnown, Aliased: res.Aliased, NotRegistry: res.NotRegistry, NotAVersion: res.NotAVersion,
				Entries: len(res.Entries), Edges: len(res.Edges)})
		}, len(pl.Path)+len(res.Kind)+len(res.Ecosystem))
		for _, e := range res.Entries {
			parts.addSmall(func(p *manifestPart) {
				p.Entries = append(p.Entries, manifestWireEntry{L: l, Name: e.Name, Version: e.Version, Direct: e.Direct,
					Namespace: e.Namespace, Ecosystem: e.Ecosystem, Scope: e.Scope})
			}, len(e.Name)+len(e.Version)+len(e.Namespace)+len(e.Ecosystem)+len(e.Scope))
		}
		for _, e := range res.Edges {
			parts.addSmall(func(p *manifestPart) {
				p.Edges = append(p.Edges, manifestWireEdge{L: l, ParentName: e.ParentName, ParentVersion: e.ParentVersion,
					ChildName: e.ChildName, Constraint: e.Constraint})
			}, len(e.ParentName)+len(e.ParentVersion)+len(e.ChildName)+len(e.Constraint))
		}
	})
	if err != nil {
		done.RootError = err.Error()
		return
	}
	done.Lockfiles = sent
	for _, f := range failed {
		parts.add(func(p *manifestPart) {
			p.Failed = append(p.Failed, manifestWireFailure{ManifestFailure: f})
		}, len(f.Path)+len(f.Kind)+len(f.Reason))
	}
}

func libyearDepBytes(d model.LibyearDep) int {
	n := len(d.Name) + len(d.Version) + len(d.Requirement) + len(d.Type) + len(d.Manager) + len(d.ManifestPath) + len(d.VersionSource) + len(d.VersionReason)
	for _, h := range d.VersionHosts {
		n += len(h)
	}
	return n
}

// manifestPartWriter cuts a result into pieces of about manifestPartTarget
// bytes.
type manifestPartWriter struct {
	child *manifestChild
	part  manifestPart
	size  int
	any   bool
}

// manifestItemOverhead stands for what an item costs on the wire beyond its
// strings: its field names and punctuation.
const manifestItemOverhead = 256

func (w *manifestPartWriter) add(put func(*manifestPart), bytes int) {
	put(&w.part)
	w.any = true
	// A string may travel as base64, four bytes for three.
	if w.size += bytes*4/3 + manifestItemOverhead; w.size >= int(manifestPartTarget) {
		w.flush()
	}
}

// manifestSmallItemOverhead is the same for a lockfile's entry or edge,
// whose field names are a letter or two: seven fields' punctuation.
const manifestSmallItemOverhead = 64

// addSmall is add for a lockfile's entry or edge.
func (w *manifestPartWriter) addSmall(put func(*manifestPart), bytes int) {
	put(&w.part)
	w.any = true
	// Escaped as JSON a byte can become six (\u0000), and base64 is less.
	if w.size += bytes*6 + manifestSmallItemOverhead; w.size >= int(manifestPartTarget) {
		w.flush()
	}
}

func (w *manifestPartWriter) flush() {
	if !w.any {
		return
	}
	w.child.send(&manifestFrame{T: framePart, Part: &w.part})
	w.part, w.size, w.any = manifestPart{}, 0, false
}

// manifestChildPomSource asks the parent.
type manifestChildPomSource struct{ child *manifestChild }

func (s *manifestChildPomSource) RemotePom(group, artifact, version string, declared []string) (*manifest.RemotePom, manifest.RemoteOutcome) {
	c := s.child
	c.send(&manifestFrame{T: framePom, Pom: &manifestPomAsk{Group: group, Artifact: artifact, Version: version, Declared: declared}})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return nil, manifest.RemoteFailed
	}
	f, _, err := readManifestFrame(c.in)
	if err != nil || f.T != framePomAnswer || f.PomAnswer == nil {
		if err == nil {
			err = errors.New("the answer to a pom was not an answer")
		}
		c.failed = err
		return nil, manifest.RemoteFailed
	}
	a := f.PomAnswer
	switch out := manifest.RemoteOutcome(a.Outcome); out {
	case manifest.RemoteFound:
		if a.Pom == nil {
			return nil, manifest.RemoteFailed
		}
		a.Pom.ServedBy = a.ServedBy
		return a.Pom, manifest.RemoteFound
	case manifest.RemoteNotFound, manifest.RemoteRefused, manifest.RemoteNotAsked:
		// Each is its own answer and must arrive as itself: "not asked"
		// withholds a later pom's version, "not found" does not.
		return nil, out
	}
	// RemoteFailed, and any value this program does not know.
	return nil, manifest.RemoteFailed
}

// manifestLogHandler sends the pass's log records to the parent, which logs
// them with its own logger.
type manifestLogHandler struct {
	child *manifestChild
	attrs []slog.Attr
}

func (h *manifestLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *manifestLogHandler) Handle(_ context.Context, r slog.Record) error {
	l := &manifestLog{Level: int(r.Level), Msg: r.Message}
	put := func(a slog.Attr) bool {
		l.Attrs = append(l.Attrs, manifestLogAttrOf(a))
		return true
	}
	for _, a := range h.attrs {
		put(a)
	}
	r.Attrs(put)
	h.child.send(&manifestFrame{T: frameLog, Log: l})
	return nil
}

func (h *manifestLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &manifestLogHandler{child: h.child, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}

func (h *manifestLogHandler) WithGroup(string) slog.Handler { return h }

func manifestLogAttrOf(a slog.Attr) manifestLogAttr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindInt64:
		return manifestLogAttr{Key: a.Key, Value: strconv.FormatInt(v.Int64(), 10), Kind: "i"}
	case slog.KindBool:
		return manifestLogAttr{Key: a.Key, Value: strconv.FormatBool(v.Bool()), Kind: "b"}
	}
	// An error, a duration, text: as the text handler would print it.
	return manifestLogAttr{Key: a.Key, Value: v.String()}
}
