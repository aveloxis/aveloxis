// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// What a subprocess writes is, for most of the subprocesses a collection
// worker starts, decided by the repository being collected: `go mod graph`
// prints one line per requirement edge, a tool's report one record per
// file. The worker is the long-lived process every repository shares, so
// it never takes such output in whole (0.29.215, review round 8: two
// go.mod files of 228 KB made `go mod graph` print nine million edges,
// which cmd.Output() held at 2.75 GB). It takes it as a stream, a line at
// a time, under a stated bound; past the bound the subprocess is stopped
// and the caller is told, by errOutputPastBound.

// errOutputPastBound: the subprocess wrote more than its caller allows.
var errOutputPastBound = errors.New("the subprocess wrote more than is taken from it")

// boundedLineBytes is the longest single line taken. No tool read this way
// writes a line near it (a module path and version, a hash, a ref); a
// longer one is output this program does not understand.
const boundedLineBytes = 1 << 20

// boundedLines runs cmd and hands each line of its standard output to
// each, in order, without holding more than one line. It stops the
// subprocess (its whole process group) and returns errOutputPastBound when
// the output passes maxBytes, or a line passes boundedLineBytes, or each
// returns an error (which is returned). Standard error is kept only as a
// bounded head (stderrCapture) and returned for the caller's log line.
//
// The context that stops cmd on shutdown or at a deadline is the one cmd
// was made with (exec.CommandContext): this function adds no clock.
func boundedLines(cmd *exec.Cmd, maxBytes int64, each func(line string) error) (stderr string, err error) {
	capture := &stderrCapture{}
	cmd.Stderr = capture
	swept, err := startSweptCommand(cmd)
	if err != nil {
		return "", err
	}
	defer swept.Close()
	stop := func() {
		if cmd.Cancel != nil {
			_ = cmd.Cancel()
		}
	}
	sc := bufio.NewScanner(swept.Stdout)
	sc.Buffer(make([]byte, 0, 64<<10), boundedLineBytes)
	var taken int64
	var stopped error
	for sc.Scan() {
		taken += int64(len(sc.Bytes())) + 1
		if taken > maxBytes {
			stopped = fmt.Errorf("%w: more than %d bytes", errOutputPastBound, maxBytes)
			break
		}
		if eerr := each(sc.Text()); eerr != nil {
			stopped = eerr
			break
		}
	}
	if stopped == nil && errors.Is(sc.Err(), bufio.ErrTooLong) {
		stopped = fmt.Errorf("%w: a line of more than %d bytes", errOutputPastBound, boundedLineBytes)
	}
	if stopped != nil {
		stop()
		_ = swept.Wait()
		return capture.String(), stopped
	}
	if rerr := sc.Err(); rerr != nil {
		stop()
		_ = swept.Wait()
		return capture.String(), rerr
	}
	return capture.String(), swept.Wait()
}

// boundedCapture is an io.Writer for a subprocess's standard error or its
// short standard output where the caller wants the text for a message: it
// keeps the first headBytes and the last tailBytes and forgets the middle,
// so that what is held does not depend on how much the subprocess wrote.
// (git's "the following paths have collided" lists one line per file of
// the repository; a bytes.Buffer held all of them.) Safe for the two
// goroutines os/exec uses when one writer is given to both streams.
type boundedCapture struct {
	mu  sync.Mutex
	buf headTailBuffer
}

// boundedCaptureBytes is what a boundedCapture keeps of each end: enough
// for the remote's notice at the head of a failed clone and for the final
// error at its tail.
const boundedCaptureBytes = 16 << 10

func newBoundedCapture() *boundedCapture {
	return &boundedCapture{buf: headTailBuffer{headCap: boundedCaptureBytes, tailCap: boundedCaptureBytes}}
}

func (c *boundedCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// String is what was kept: all of it when it fit, else the head, a marker
// and the tail.
func (c *boundedCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.TrimSpace(string(c.buf.Bytes()))
}

// cappedOutput is an io.Writer for a subprocess's standard output that is
// one document the caller will decode (a JSON report): it keeps at most
// max bytes and records that more arrived, so that the caller can refuse
// the document instead of decoding a truncated one or holding an unbounded
// one.
type cappedOutput struct {
	mu   sync.Mutex
	max  int
	buf  []byte
	over bool
}

func (c *cappedOutput) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := c.max - len(c.buf); room >= len(p) {
		c.buf = append(c.buf, p...)
	} else {
		if room > 0 {
			c.buf = append(c.buf, p[:room]...)
		}
		c.over = true
	}
	return len(p), nil // never fail the child's write: it would hide its real exit
}

// Bytes is what was kept; Over says the subprocess wrote more than max.
func (c *cappedOutput) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf
}

func (c *cappedOutput) Over() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.over
}

func (c *cappedOutput) String() string { return string(c.Bytes()) }

func (c *cappedOutput) Len() int { return len(c.Bytes()) }

// readToolOutputWithin reads a tool's report file whole when it is no
// longer than max, into one buffer of its known size, and refuses it with
// errOutputPastBound otherwise. A report's size follows the repository the
// tool was pointed at (one record per file), so it is not taken unbounded.
func readToolOutputWithin(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	if info.Size() > max {
		return nil, fmt.Errorf("%w: %s is %d bytes, more than the %d taken", errOutputPastBound, path, info.Size(), max)
	}
	return readMeasured(f, path, info.Size())
}

// readMeasured reads what r holds into one buffer of size bytes, the length
// its file had when it was measured, and refuses a file that has grown past
// that since (a tool still writing its report): one byte more than the
// size is asked for, and getting it is the refusal.
func readMeasured(r io.Reader, path string, size int64) ([]byte, error) {
	buf := make([]byte, size+1)
	n, err := io.ReadFull(r, buf)
	if err == nil {
		// It grew past the size it had when measured.
		return nil, fmt.Errorf("%w: %s grew past %d bytes while it was read", errOutputPastBound, path, size)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

// errWalkPastBound: a clone has more entries than a worker walks.
var errWalkPastBound = errors.New("the directory tree has more entries than are walked")

// cloneWalkEntries is the most entries a worker visits when it walks a
// clone itself (it does so only to find go.mod files; everything else is
// walked by the reader's child process). PROVISIONAL: the largest trees
// collected hold some hundreds of thousands of files (chromium about half
// a million); five million is ten times that.
const cloneWalkEntries = 5_000_000

// walkChunk is how many entries of one directory are held at a time.
const walkChunk = 1024

// walkDepth is how deep a worker's walk of a clone goes: one open directory
// for each level. PROVISIONAL: no repository has been measured against it;
// 256 levels is several times the deepest tree a build tool lays out, and a
// walk that passes it ends as one that failed (its caller's "not known").
const walkDepth = 256

// walkBounded visits every entry under root, as filepath.Walk does but
// with two differences that make what it holds independent of the tree: a
// directory is read walkChunk entries at a time and never sorted or held
// whole (filepath.Walk reads all of a directory's names first: five
// million entries in one directory are five million strings), and the
// walk ends with errWalkPastBound after maxEntries entries. visit may
// return filepath.SkipDir for a directory. Symbolic links are not
// followed. Any error reading a directory ends the walk with that error:
// the caller treats a walk that did not finish as not having an answer.
func walkBounded(root string, maxEntries int, visit func(path string, entry fs.DirEntry) error) error {
	seen := 0
	var walk func(dir string, depth int) error
	walk = func(dir string, depth int) error {
		// One directory is held open for each level being walked, so the
		// depth is bounded too: a clone nested as deep as its file system
		// allows would otherwise hold thousands of the process's
		// descriptors at once, which every other goroutine opens files
		// from.
		if depth > walkDepth {
			return fmt.Errorf("%w: directories nested more than %d deep under %s", errWalkPastBound, walkDepth, root)
		}
		f, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer f.Close()
		for {
			entries, rerr := f.ReadDir(walkChunk)
			for _, e := range entries {
				if seen++; seen > maxEntries {
					return fmt.Errorf("%w: more than %d under %s", errWalkPastBound, maxEntries, root)
				}
				path := filepath.Join(dir, e.Name())
				verr := visit(path, e)
				if e.IsDir() {
					if errors.Is(verr, filepath.SkipDir) {
						continue
					}
					if verr != nil {
						return verr
					}
					if werr := walk(path, depth+1); werr != nil {
						return werr
					}
					continue
				}
				if verr != nil && !errors.Is(verr, filepath.SkipDir) {
					return verr
				}
			}
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			if rerr != nil {
				return rerr
			}
		}
	}
	return walk(root, 0)
}
