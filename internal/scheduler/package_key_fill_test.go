// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

type fakeKeyFill struct {
	left   map[db.PackageKeyFillTable]int // steps before a table reports done
	calls  []string
	failAt int
	cancel context.CancelFunc
}

func (f *fakeKeyFill) FillPackageKeysStep(_ context.Context, table db.PackageKeyFillTable, batch int) (bool, error) {
	f.calls = append(f.calls, fmt.Sprintf("%s/%d", table, batch))
	if f.failAt > 0 && len(f.calls) == f.failAt {
		return false, errors.New("connection reset")
	}
	if f.cancel != nil && len(f.calls) == 2 {
		f.cancel()
		return false, context.Canceled
	}
	if f.left[table] == 0 {
		return true, nil
	}
	f.left[table]--
	return false, nil
}

func (f *fakeKeyFill) GetPackageKeyFillProgress(context.Context, db.PackageKeyFillTable) (db.PackageKeyFillProgress, error) {
	return db.PackageKeyFillProgress{Complete: true, Filled: 5, Unkeyable: 1}, nil
}

type recLog struct{ lines []string }

func (l *recLog) Info(msg string, _ ...any) { l.lines = append(l.lines, "INFO "+msg) }
func (l *recLog) Warn(msg string, _ ...any) { l.lines = append(l.lines, "WARN "+msg) }
func (l *recLog) has(s string) bool         { return strings.Contains(strings.Join(l.lines, "\n"), s) }

// The loop walks each table to done, in order, with the configured step, and
// reports each table it did work on.
func TestPackageKeyFillLoopWalksBothTablesToDone(t *testing.T) {
	f := &fakeKeyFill{left: map[db.PackageKeyFillTable]int{db.PackageKeyFillInventory: 2, db.PackageKeyFillLockfile: 0}}
	log := &recLog{}
	runPackageKeyFill(context.Background(), f, log, 250, time.Millisecond)
	want := []string{"repo_deps_libyear/250", "repo_deps_libyear/250", "repo_deps_libyear/250", "repo_lockfile_packages/250"}
	if strings.Join(f.calls, ",") != strings.Join(want, ",") {
		t.Errorf("steps = %v, want %v", f.calls, want)
	}
	if n := strings.Count(strings.Join(log.lines, "\n"), "package-key fill complete"); n != 1 {
		t.Errorf("%d completion lines, want 1: the table that was already complete says nothing (%v)", n, log.lines)
	}
}

// 0 turns it off: no step is taken.
func TestPackageKeyFillLoopOff(t *testing.T) {
	f := &fakeKeyFill{left: map[db.PackageKeyFillTable]int{}}
	log := &recLog{}
	runPackageKeyFill(context.Background(), f, log, 0, time.Millisecond)
	if len(f.calls) != 0 || !log.has("is off") {
		t.Errorf("calls %v, log %v; want no step and the 'off' line", f.calls, log.lines)
	}
}

// A failed step ends the run with one WARN; a shutdown ends it with none.
func TestPackageKeyFillLoopStopsOnFailureAndShutdown(t *testing.T) {
	f := &fakeKeyFill{left: map[db.PackageKeyFillTable]int{db.PackageKeyFillInventory: 5}, failAt: 2}
	log := &recLog{}
	runPackageKeyFill(context.Background(), f, log, 10, time.Millisecond)
	if len(f.calls) != 2 || !log.has("WARN package-key fill: a step failed") {
		t.Errorf("after a failed step: calls %v, log %v; want the run to end there with one WARN", f.calls, log.lines)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f = &fakeKeyFill{left: map[db.PackageKeyFillTable]int{db.PackageKeyFillInventory: 5}, cancel: cancel}
	log = &recLog{}
	runPackageKeyFill(ctx, f, log, 10, time.Millisecond)
	if len(f.calls) != 2 || len(log.lines) != 0 {
		t.Errorf("on shutdown: calls %v, log %v; want the run to end with no line (shutdown is not a failure)", f.calls, log.lines)
	}
}

// The knob, end to end: the value in aveloxis.json is the step the loop
// hands the store, and an explicit 0 turns the fill off (SR-10).
func TestPackageKeyFillRowsFromTheConfigFile(t *testing.T) {
	run := func(collection string) (*fakeKeyFill, *recLog) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection": {`+collection+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		f, log := &fakeKeyFill{left: map[db.PackageKeyFillTable]int{}}, &recLog{}
		runPackageKeyFill(context.Background(), f, log, cfg.Collection.PackageKeyFillRowsOrDefault(), time.Millisecond)
		return f, log
	}
	if f, _ := run(`"package_key_fill_rows": 37`); len(f.calls) != 2 || f.calls[0] != "repo_deps_libyear/37" {
		t.Errorf("37 rows: steps %v", f.calls)
	}
	if f, _ := run(``); len(f.calls) != 2 || f.calls[0] != fmt.Sprintf("repo_deps_libyear/%d", config.DefaultPackageKeyFillRows) {
		t.Errorf("the default: steps %v", f.calls)
	}
	if f, log := run(`"package_key_fill_rows": 0`); len(f.calls) != 0 || !log.has("is off") {
		t.Errorf("an explicit 0 must turn the fill off, not mean the default: steps %v", f.calls)
	}
	for _, bad := range []string{`"package_key_fill_rows": -1`, `"package_key_fill_rows": 50001`} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		_ = os.WriteFile(path, []byte(`{"collection": {`+bad+`}}`), 0o600)
		if _, err := config.Load(path); err == nil {
			t.Errorf("%s must be refused at load", bad)
		}
	}
}
