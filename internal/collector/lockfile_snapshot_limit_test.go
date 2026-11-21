// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A snapshot the store refuses for its size (db.LockfileSnapshotRows) leaves
// the previous rows standing, so they are older than the analysis: that is
// the lockfile caution's case, and it is recorded. Any other failure of the
// replace records nothing (the rows may be current; the error is logged by
// the analysis).
func TestASnapshotRefusedForItsSizeRaisesTheLockfileCaution(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		err  error
		want []LockfileReading
	}{
		{fmt.Errorf("%w: more than %d", db.ErrLockfileSnapshotTooLarge, db.LockfileSnapshotRows),
			[]LockfileReading{{NotCompleted: model.LockfileUnreadTooLarge, Lockfiles: true}}},
		{errors.New("the connection went"), nil},
		{context.Canceled, nil},
	} {
		rec := &recordedReadings{}
		ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), LockfileReadings: rec}
		ac.lockfileSnapshotNotReplaced(ctx, 7, c.err)
		if !reflect.DeepEqual(rec.got, c.want) {
			t.Errorf("after %v: recorded %+v, want %+v", c.err, rec.got, c.want)
		}
	}
	// And the scan tells it of the replace's error, before returning it.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) scanLockfiles("))
	i := strings.Index(body, "ReplaceRepoLockfileSnapshotFrom(ctx, repoID, feed, failed, inTx); err != nil {")
	if i < 0 {
		t.Fatal("scanLockfiles does not replace the snapshot the way this test reads it")
	}
	if rest := strings.Join(strings.Fields(body[i:]), " "); !strings.Contains(rest, "err != nil { ac.lockfileSnapshotNotReplaced(ctx, repoID, err) return err }") {
		t.Error("scanLockfiles does not tell lockfileSnapshotNotReplaced of a replace that failed")
	}
}

// What one handing-over of a snapshot's rows held is counted for THAT
// handing-over (the store asks again when it retries its transaction, and a
// count carried over would say twice the lockfiles).
func TestTheRowsHandedOverAreCountedPerHandingOver(t *testing.T) {
	rows := db.LockfileRows{
		Inventory: []*db.RepoLockfileInfo{{}, {}},
		Packages:  []*model.RepoLockfilePackage{{Direct: true}, {}, {}},
		Edges:     []*db.RepoLockfileEdge{{}, {}, {}, {}},
	}
	var c lockfileFeedCounts
	c.take(rows)
	c.take(db.LockfileRows{Packages: []*model.RepoLockfilePackage{{Direct: true}}})
	if c != (lockfileFeedCounts{lockfiles: 2, direct: 2, transitive: 2, edges: 4}) {
		t.Errorf("counted %+v", c)
	}
	// Each call of the feed starts its own count and the last one stands.
	body := strings.Join(strings.Fields(srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) scanLockfiles("))), " ")
	for _, want := range []string{
		"feed := func(write func(db.LockfileRows) error) error { var now lockfileFeedCounts",
		"if err := hand(extra); err != nil { return err } fed = now return nil }",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scanLockfiles: the feed does not count each handing-over on its own (%q)", want)
		}
	}
}

// A lockfile the read bound refuses (it grew after the walk measured it, or
// a reader's own rule let it by) is the same failure as one refused for its
// size up front: too large, by name.
func TestALockfileTheReadBoundRefusesIsRecordedAsTooLarge(t *testing.T) {
	dir := writeClone(t, map[string]string{
		"big/yarn.lock":  "# yarn lockfile v1\n\nleft-pad@1.0.0:\n  version \"1.0.0\"\n",
		"ok/Cargo.lock":  "[[package]]\nname = \"serde\"\nversion = \"1.0.200\"\n",
		"bad/yarn.lock2": "",
	})
	// No limit of this process's (so the walk's own check lets every file
	// by), and a read bound that leaves a yarn.lock ten bytes.
	restore := manifest.SetReadBound(func(path string, _ int64) int64 {
		if filepath.Base(path) == "yarn.lock" {
			return 10
		}
		return 1 << 30
	})
	defer restore()
	read := 0
	failed, err := walkLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), func(parsedLockfile) { read++ })
	if err != nil {
		t.Fatal(err)
	}
	want := []model.ManifestFailure{{Path: "big/yarn.lock", Kind: "yarn.lock", Reason: model.ManifestFailedRead, TooLarge: true}}
	if read != 1 || !reflect.DeepEqual(failed, want) {
		t.Errorf("%d lockfiles read, failed %+v; want one read and %+v", read, failed, want)
	}
}
