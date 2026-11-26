// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// ReadManifestWithin refuses a file longer than its bound from the file's
// length, before reading it; reads one at the bound whole; and is
// ReadManifest when it is given no bound.
func TestReadManifestWithinRefusesBeforeItReads(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	at := write("at", bytes.Repeat([]byte("a"), 1000))
	if got, err := ReadManifestWithin(at, 1000); err != nil || len(got) != 1000 {
		t.Errorf("a file of exactly the bound: %d bytes, %v", len(got), err)
	}
	over := write("over", bytes.Repeat([]byte("a"), 1001))
	_, err := ReadManifestWithin(over, 1000)
	var tooLarge *ManifestTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Size != 1001 || tooLarge.Max != 1000 || tooLarge.Path != over {
		t.Errorf("a file one byte over: %v (%+v)", err, tooLarge)
	}
	// A sparse file of a gibibyte is refused from its length: nothing of it
	// is read (it would take a second and a gibibyte to find out otherwise).
	sparse := filepath.Join(dir, "sparse")
	f, err := os.Create(sparse)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 30); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if allocs := testing.AllocsPerRun(1, func() { ReadManifestWithin(sparse, 1<<20) }); allocs > 50 {
		t.Errorf("refusing a file by its length took %v allocations: it was read", allocs)
	}
	if _, err := ReadManifestWithin(sparse, 1<<20); !errors.As(err, &tooLarge) || tooLarge.Size != 1<<30 {
		t.Errorf("a sparse gibibyte under a mebibyte bound: %v", err)
	}
	// No bound: ReadManifest, encoding and all.
	utf16 := write("utf16", []byte{0xff, 0xfe, 'a', 0, 'b', 0})
	for _, max := range []int64{0, -1, 6, 100} {
		if got, err := ReadManifestWithin(utf16, max); err != nil || string(got) != "ab" {
			t.Errorf("a UTF-16 file within %d: %q, %v", max, got, err)
		}
	}
	if _, err := ReadManifestWithin(utf16, 5); !errors.As(err, &tooLarge) {
		t.Errorf("the bound is on the file's bytes, not its text: %v", err)
	}
	whole, err1 := ReadManifest(over)
	within, err2 := ReadManifestWithin(over, 0)
	if err1 != nil || err2 != nil || !bytes.Equal(whole, within) || len(whole) != 1001 {
		t.Errorf("ReadManifest is ReadManifestWithin with no bound: %d, %d, %v, %v", len(whole), len(within), err1, err2)
	}
	if _, err := ReadManifestWithin(filepath.Join(dir, "none"), 10); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a file that is not there: %v", err)
	}
}

// A file whose length is not known up front is read no further than the
// bound.
func TestReadManifestWithinStopsAtTheBoundOfAFileItCannotMeasure(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	go func() {
		w.Write(bytes.Repeat([]byte("a"), 4096))
		w.Close()
	}()
	// /dev/fd names the pipe as a path on the systems the suite runs on.
	path := "/dev/fd/" + strconv.Itoa(int(r.Fd()))
	if _, err := os.Stat(path); err != nil {
		t.Logf("no /dev/fd on this system (%v): the length check is what this build has", err)
		return
	}
	_, err = ReadManifestWithin(path, 1000)
	var tooLarge *ManifestTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Size != -1 {
		t.Errorf("a pipe of 4096 bytes under a bound of 1000: %v", err)
	}
}
