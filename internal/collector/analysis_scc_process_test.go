// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeSCC puts a shell script named "scc" at the front of PATH so scanSCC's
// exec.LookPath finds it. The subprocess half of scanSCC (drain, reap,
// classify) is only reachable through a real process — these are the paths
// the 2026-09-15 rewrite changed and that decoder-level tests cannot see.
func fakeSCC(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scc")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func quietCollector() *AnalysisCollector {
	return &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

const sccMinimalReport = `[{"Name":"Go","Files":[{"Location":"/w/a.go","Lines":1,"Code":1}]}]`

// errStoreReached marks a run that got all the way through parsing,
// draining and reaping to the snapshot write. These tests cover the
// subprocess half of scanSCC, so the collector has no *db.PostgresStore
// (a concrete type with no interface seam) and the store call panics on
// its nil pool. Reaching it is the PASS condition for the success paths:
// it means nothing before it rejected the report.
var errStoreReached = errors.New("reached ReplaceRepoLaborSnapshot")

// runScanSCC calls scanSCC with a hard deadline, so a wedge fails the test
// instead of hanging the package for 10 minutes.
func runScanSCC(t *testing.T, ctx context.Context, budget time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- errStoreReached
			}
		}()
		done <- quietCollector().scanSCC(ctx, 1, t.TempDir(), &AnalysisResult{})
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(budget):
		t.Fatalf("scanSCC did not return within %s — the scc subprocess is wedged and this collection worker is gone", budget)
		return nil
	}
}

// TestScanSCCDrainsTrailingOutput is the regression test for the wedge the
// streaming rewrite introduced. streamSCCLabor stops at the top-level "]",
// so anything scc writes after that stays in the pipe; once it exceeds the
// OS pipe buffer (64 KiB) scc blocks in write() and cmd.Wait() blocks
// FOREVER. Reproduced at exactly that boundary: 60,000 trailing bytes
// returned, 100,000 hung. The pre-rewrite code could not hit this because
// cmd.Stdout drained to EOF.
func TestScanSCCDrainsTrailingOutput(t *testing.T) {
	fakeSCC(t, `printf '`+sccMinimalReport+`'
head -c 100000 /dev/zero | tr '\0' 'x'`)

	err := runScanSCC(t, context.Background(), 20*time.Second)
	if err == nil {
		t.Fatal("trailing garbage after the top-level array must be an error — json.Unmarshal rejected it before the rewrite")
	}
	if !strings.Contains(err.Error(), "trailing") {
		t.Errorf("error = %v, want it to name the trailing data", err)
	}
}

// TestScanSCCAllowsTrailingWhitespace: scc 3.7.0 emits nothing after "]",
// but a future version adding a newline is not a corrupt report, and
// json.Unmarshal tolerated trailing whitespace too.
func TestScanSCCAllowsTrailingWhitespace(t *testing.T) {
	fakeSCC(t, `printf '`+sccMinimalReport+`\n\n  \n'`)

	if err := runScanSCC(t, context.Background(), 20*time.Second); !errors.Is(err, errStoreReached) {
		t.Errorf("scanSCC = %v; want it to parse, drain and reap cleanly and reach the snapshot write — trailing whitespace is not a corrupt report", err)
	}
}

// TestScanSCCAcceptsAWellFormedReport is the positive control for the three
// rejection tests above: without it, a scanSCC that rejected EVERYTHING
// would pass them all.
func TestScanSCCAcceptsAWellFormedReport(t *testing.T) {
	fakeSCC(t, `printf '`+sccMinimalReport+`'`)

	if err := runScanSCC(t, context.Background(), 20*time.Second); !errors.Is(err, errStoreReached) {
		t.Errorf("scanSCC = %v; want a clean report to reach the snapshot write", err)
	}
}

// TestScanSCCReportsSccExitStatus: when scc fails without producing a
// parseable report, the error must name SCC's failure. Reporting only our
// decoder's symptom ("expected [, got EOF") sends the reader to the wrong
// layer — the CLAUDE.md "log the effective cause" rule.
func TestScanSCCReportsSccExitStatus(t *testing.T) {
	fakeSCC(t, `exit 2`)

	err := runScanSCC(t, context.Background(), 20*time.Second)
	if err == nil {
		t.Fatal("scc exiting 2 with no output must be an error")
	}
	if !strings.Contains(err.Error(), "exit status 2") {
		t.Errorf("error = %v, want it to carry scc's exit status 2 rather than only the decode symptom", err)
	}
}

// TestScanSCCClassifiesShutdownAsCancellation: execErr exists (v0.28.18
// pass 35) precisely so a `stop serve` landing mid-scc is not counted as a
// failure — SubprocessKill reports "signal: killed", never context.Canceled.
// The decode-error path must route through it like the exit-status path.
func TestScanSCCClassifiesShutdownAsCancellation(t *testing.T) {
	// Emit a partial report, then hang: the decoder blocks mid-document
	// exactly as it would when a shutdown lands in a long scan.
	// `exec sleep` replaces the shell, so the fake is ONE childless
	// process like the real scc binary — otherwise the test would be
	// measuring a shell straggler holding the pipe, not scanSCC.
	fakeSCC(t, `printf '[{"Name":"Go","Files":[{"Location":"/w/a.go","Lines":1'
exec sleep 30`)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	err := runScanSCC(t, ctx, 20*time.Second)
	if err == nil {
		t.Fatal("a cancelled scan must return an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v; want errors.Is(err, context.Canceled) so the scheduler counts a shutdown as a shutdown, not a scc failure", err)
	}
}

// TestScanSCCKillsSccOnDecodeError: a malformed report must not leave the
// process running while we drain a report that may have minutes left.
func TestScanSCCKillsSccOnDecodeError(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "still-alive")
	fakeSCC(t, `printf 'not json at all'
sleep 5
touch `+marker)

	start := time.Now()
	if err := runScanSCC(t, context.Background(), 20*time.Second); err == nil {
		t.Fatal("malformed output must be an error")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("scanSCC took %s — it waited out the doomed scan instead of killing it", elapsed)
	}
	time.Sleep(6 * time.Second)
	if _, err := os.Stat(marker); err == nil {
		t.Error("scc survived the decode error and ran to completion — the process was not killed")
	}
}

// TestSCCRowStreamedSeamDefaultsToNil pins the seam's production default.
// A hook left installed would run on every labor row of every repo.
func TestSCCRowStreamedSeamDefaultsToNil(t *testing.T) {
	if sccRowStreamed != nil {
		t.Error("sccRowStreamed must be nil in production — it is a test seam, not a feature")
	}
}

// TestScanSCCStreamsRowsWhileSccIsStillWriting is the WIRING proof that
// TestScanSCCDoesNotBufferWholeReport cannot give. That pin greps scanSCC
// for banned spellings, and a fresh-context review escaped it with a
// one-line helper that buffered the pipe and returned a bytes.Reader —
// every test stayed green while the exact allocation shape behind the
// 2026-09-15 OOM was back.
//
// Here the fake scc emits a complete language object and then STAYS ALIVE
// holding the array open. Rows must already exist while it is still
// writing. Any implementation that reads the report to EOF before decoding
// — under any spelling — sees no rows until the fake exits, and fails.
func TestScanSCCStreamsRowsWhileSccIsStillWriting(t *testing.T) {
	fakeSCC(t, `printf '[{"Name":"Go","Files":[{"Location":"/w/a.go","Lines":1,"Code":1},{"Location":"/w/b.go","Lines":2,"Code":2}]}'
sleep 3
printf ']'`)

	rows := make(chan struct{}, 64)
	sccRowStreamed = func() {
		select {
		case rows <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { sccRowStreamed = nil })

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }() // the success path reaches the nil store
		_ = quietCollector().scanSCC(context.Background(), 1, t.TempDir(), &AnalysisResult{})
	}()

	// The fake holds the array open for 3s. Both rows must arrive well
	// before that, because they are decoded as the file objects close.
	for got := 0; got < 2; got++ {
		select {
		case <-rows:
		case <-done:
			t.Fatal("scanSCC finished before emitting 2 rows — it consumed the whole report before decoding, so the buffering is back")
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d rows emitted while scc was still writing, want 2 — scanSCC is not streaming the pipe into the decoder", got)
		}
	}

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("scanSCC did not return after the fake scc exited")
	}
}
