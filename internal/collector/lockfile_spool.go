// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// The lockfile pass's result, as the analysis keeps it while the child runs
// (0.29.214): on disk, not in memory.
//
// A repository's lockfiles have no largest honest size, and until 0.29.214
// the worker collected every entry and edge the child sent as Go values:
// some three times the bytes on the wire, 673 MiB for fifteen ordinary
// yarn.lock files, for as long as the analysis ran (review round 7). The
// child holds one lockfile at a time; so, now, does the worker. What the
// child sends for the lockfile pass is checked as it arrives
// (manifestCheck.lockPart) and written, as it arrived, to an unnamed
// temporary file. The lockfile phase reads it back one lockfile at a time
// (lockfileSet.each).
//
// What it holds is bounded by what the clone holds: the pieces written may
// not pass lockfileWireFactor times the size on disk of the lockfiles they
// are of, which the worker asks the file system (manifestCheck.lockfileSize),
// plus one frame.
type lockfileSpool struct {
	f *os.File
	w *bufio.Writer
	// written is the bytes of pieces written.
	written int64
	// heads are the lockfiles the pieces are of, in order.
	heads []manifestWireLockfile
}

// lockfileWireFactor is how many bytes the lockfile pass's pieces may be for
// each byte of the lockfiles they are of. PROVISIONAL (summary/71, section
// 8). An entry or an edge on the wire is its strings and some punctuation,
// and its strings are text of the lockfile; an edge repeats its parent's
// name and version, which a lockfile writes once for all of a package's
// dependencies, and that is where the wire is the larger. Measured through
// the child for an honest file of each of ten formats (0.29.214, round 7):
// from 0.14 times the file (package-lock.json version 1, stack.yaml.lock)
// to 2.4 times (Gemfile.lock, whose every dependency line is an edge).
// Sixteen is the power of two past four times the largest;
// TestTheLockfilePassIsNoLargerThanItsLockfilesTimesTheFactor holds three
// formats, the largest among them, to a quarter of it.
const lockfileWireFactor = 16

func newLockfileSpool() (*lockfileSpool, error) {
	f, err := os.CreateTemp("", "aveloxis-lockfiles-*")
	if err != nil {
		return nil, err
	}
	// No name: it is gone when it is closed, however this process ends.
	_ = os.Remove(f.Name())
	return &lockfileSpool{f: f, w: bufio.NewWriter(f)}, nil
}

// add writes one checked piece: the frame as it arrived.
func (s *lockfileSpool) add(body []byte) error {
	var head [4]byte
	head[0], head[1], head[2], head[3] = byte(len(body)>>24), byte(len(body)>>16), byte(len(body)>>8), byte(len(body))
	if _, err := s.w.Write(head[:]); err != nil {
		return err
	}
	_, err := s.w.Write(body)
	s.written += int64(len(body))
	return err
}

func (s *lockfileSpool) close() {
	if s != nil && s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
}

// each hands fn every lockfile the spool holds, in the order the child read
// them, one at a time. The pieces were checked when they arrived; a spool
// that does not read back as it was written is a failed disk, and an error.
func (s *lockfileSpool) each(fn func(parsedLockfile) error) error {
	if s.f == nil {
		return errors.New("the lockfile pass's result was already let go")
	}
	if err := s.w.Flush(); err != nil {
		return fmt.Errorf("the lockfile pass's result could not be kept: %w", err)
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.f)
	open := map[int]*LockfileResult{}
	begun, emitted := 0, 0
	// emit hands over the lockfiles that can have nothing more to come:
	// all but the last begun (lockPart holds the child to that).
	emit := func(upTo int) error {
		for ; emitted < upTo; emitted++ {
			res := open[emitted]
			delete(open, emitted)
			if err := fn(parsedLockfile{Path: s.heads[emitted].Path, Result: res}); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		f, _, err := readManifestFrame(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || f.Part == nil {
			return fmt.Errorf("the lockfile pass's result did not read back as it was written: %v", err)
		}
		p := f.Part
		for _, h := range p.Lockfiles {
			if begun >= len(s.heads) {
				return errors.New("the lockfile pass's result did not read back as it was written: more lockfiles than were begun")
			}
			open[begun] = &LockfileResult{Kind: h.Kind, Ecosystem: h.Ecosystem, DirectKnown: h.DirectKnown,
				Aliased: h.Aliased, NotRegistry: h.NotRegistry, NotAVersion: h.NotAVersion}
			begun++
		}
		for _, e := range p.Entries {
			res := open[e.L]
			if res == nil {
				return errors.New("the lockfile pass's result did not read back as it was written: an entry of no open lockfile")
			}
			res.Entries = append(res.Entries, LockfileEntry{Name: e.Name, Version: e.Version, Direct: e.Direct,
				Namespace: e.Namespace, Ecosystem: e.Ecosystem, Scope: e.Scope})
		}
		for _, e := range p.Edges {
			res := open[e.L]
			if res == nil {
				return errors.New("the lockfile pass's result did not read back as it was written: an edge of no open lockfile")
			}
			res.Edges = append(res.Edges, LockfileEdge{ParentName: e.ParentName, ParentVersion: e.ParentVersion,
				ChildName: e.ChildName, Constraint: e.Constraint})
		}
		if err := emit(begun - 1); err != nil {
			return err
		}
	}
	return emit(begun)
}
