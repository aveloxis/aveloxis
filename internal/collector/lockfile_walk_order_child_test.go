// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// honestYarnLock is a yarn.lock (v1) of about size bytes, entries of the
// length real ones have.
func honestYarnLock(size int) string {
	return fill(size, "# yarn lockfile v1\n\n", "", func(i int) string {
		return fmt.Sprintf("\"@scope%d/pkg-%d@^%d.%d.0\":\n  version \"%d.%d.%d\"\n  resolved \"https://registry.yarnpkg.com/@scope%d/pkg-%d/-/pkg-%d-%d.%d.%d.tgz#%040d\"\n  integrity sha512-%064d%022d==\n  dependencies:\n    pkg-%d \"^1.2.3\"\n\n",
			i%500, i, i%9, i%20, i%9, i%20, i%30, i%500, i, i, i%9, i%20, i%30, i, i, i, i+1)
	})
}

// What a lockfile cost its reader is let go before the next is read (review
// round 9, B9-7: a pnpm-lock.yaml well inside the limit was refused for its
// size when the walk had read a large yarn.lock before it, and read when
// the walk met it first). The yarn.lock reader's entries are slices of the
// file's text, and the last few of them stayed in the piece not yet sent
// to the parent, so the whole text stayed with them.
func TestALockfileIsNotRefusedForTheLockfileReadBeforeIt(t *testing.T) {
	if manifestRaceEnabled {
		return // the race detector's own memory is in the heap that is limited
	}
	const limit = 256 << 20
	// Alone, each is inside the limit by the reader's own prices
	// (readCostFactor): the yarn.lock 14 times its 16 MiB, the
	// pnpm-lock.yaml 46 times its 5.3 MiB (244 MiB of the 252 or so the
	// limit leaves). With the yarn.lock's 16 MiB of text still held, the
	// second no longer fits.
	dir := writeClone(t, map[string]string{
		"a/yarn.lock":      honestYarnLock(16 << 20),
		"b/pnpm-lock.yaml": honestLockfile("pnpm-lock.yaml", 53<<20/10),
	})
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassLockfiles}, Dir: dir},
		ManifestLimits{CPU: testLimits().CPU, HeapBytes: limit}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("the child did not finish: %v", err)
	}
	defer run.close()
	if lerr := run.passErr(manifestPassLockfiles, nil); lerr != nil {
		t.Fatal(lerr)
	}
	if len(run.locks.Failed) != 0 || run.locks.parsedCount() != 2 {
		t.Errorf("%d lockfiles read, failed %+v; want both read (heap peak %d MiB under %d MiB)", run.locks.parsedCount(), run.locks.Failed, run.heapPeak>>20, limit>>20)
	}
}
