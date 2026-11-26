// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/linking"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A scan stopped partway (a shutdown) is not a completed scan (review round
// 1). The closing and the marking each swallow a cancellation, rightly: it
// is not a failure to warn about. But the scan then logged "vulnerability
// scan complete" and returned nil, and heal-vulnerabilities counted the
// repository healed, with nothing closed and nothing marked.
func TestAScanCutShortIsNotComplete(t *testing.T) {
	live := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		what string
		ctx  context.Context
		out  linking.Outcome
		cut  bool
	}{
		{"finished", live, linking.Outcome{Marked: true}, false},
		{"a closing that failed for another reason is still a finished scan", live, linking.Outcome{CloseErr: errors.New("deadlock")}, false},
		{"the context is done", done, linking.Outcome{Marked: true}, true},
		{"the closing was cancelled", live, linking.Outcome{CloseErr: fmt.Errorf("closing: %w", context.Canceled)}, true},
		{"the marking was cancelled", live, linking.Outcome{MarkErr: context.Canceled}, true},
		{"the marking ran out of time", live, linking.Outcome{MarkErr: context.DeadlineExceeded}, true},
	} {
		err := scanCutShort(c.ctx, c.out)
		if (err != nil) != c.cut {
			t.Errorf("%s: %v, want cut short %v", c.what, err, c.cut)
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s: the error %v is not the context's", c.what, err)
		}
	}
	// The wiring: the check stands between each linking exit and the lines
	// that call the scan done.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ScanVulnerabilities("))
	complete := strings.Index(body, `"vulnerability scan complete"`)
	if complete < 0 {
		t.Fatal("ScanVulnerabilities no longer logs \"vulnerability scan complete\": this pin reads nothing")
	}
	if strings.LastIndex(body[:complete], "scanCutShort(ctx, out)") < 0 {
		t.Fatal("ScanVulnerabilities does not check for a scan cut short before it says the scan is complete")
	}
	// And the pass after a re-ask is told it is the last, so it marks the
	// repository whatever it reports (linking's test drives the rule; a
	// second refusal needs another scan deciding at that instant).
	reask := strings.Index(body, "answers.replace(out.Reask, fresh)")
	if reask < 0 || !strings.Contains(body[reask:strings.Index(body[reask:], "link()")+reask], "universe.FinalPass = true") {
		t.Error("the pass after a re-ask is not marked as the final one before it links")
	}
	if n := strings.Count(body, "scanCutShort(ctx, out)"); n != 2 {
		t.Errorf("scanCutShort(ctx, out) is called %d times; want once on the exits with no answers and once on the exit with answers", n)
	}
}

// The same, driven (review round 2: the wiring above pins tokens, and a
// check that is present and can never fire passed it). A scan is stopped
// while it marks the repository scanned: another session holds the
// repository's row, the mark waits on it, and the context is cancelled.
// The mark swallows the cancellation; the scan must not then call itself
// complete. Both linking exits: with answers, and with nothing to scan.
func TestAScanStoppedWhileItMarksReturnsTheCancellation(t *testing.T) {
	for _, withDeps := range []bool{true, false} {
		name := "cut-nothing-to-scan"
		if withDeps {
			name = "cut-with-answers"
		}
		t.Run(name, func(t *testing.T) {
			f, _ := scanFixture(t, name)
			stubOSV(t, nil)
			bg := context.Background()
			if withDeps {
				f.manifest(t, map[string]string{uniq(t, "cut-a"): "1.0.0"})
			}
			if err := f.ac.scanLibyear(bg, f.repoID, f.dir, f.ac.readClone(bg, f.repoID, f.dir), nil, &AnalysisResult{}); err != nil {
				t.Fatal(err)
			}
			var logged bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logged, nil))

			// Hold the repository's row the way an UPDATE of it would. A
			// lock that leaves the row's key free, so the scan's inserts
			// (which take a key-share on it) are not held up: only the
			// mark is.
			holder, err := f.store.Pool().Begin(bg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = holder.Rollback(bg) }()
			if _, err := holder.Exec(bg, `SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1 FOR NO KEY UPDATE`, f.repoID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(bg)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := ScanVulnerabilities(ctx, f.store, f.repoID, logger, nil, true)
				done <- err
			}()
			// Cancel once the mark is the statement that is waiting.
			waiting := false
			for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline) && !waiting; time.Sleep(20 * time.Millisecond) {
				if err := f.store.Pool().QueryRow(bg, `
					SELECT EXISTS (SELECT 1 FROM pg_stat_activity
					                WHERE datname = current_database() AND wait_event_type = 'Lock'
					                  AND query LIKE 'UPDATE aveloxis_data.repos SET vuln_scan_last_run%')`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					t.Fatalf("the scan returned (%v) before its mark was seen waiting: the premise did not hold", err)
				default:
				}
			}
			if !waiting {
				t.Fatal("the scan's mark was never seen waiting on the held row")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("a scan stopped while marking returned %v, want the cancellation", err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("the scan did not return after its context was cancelled")
			}
			if strings.Contains(logged.String(), "vulnerability scan complete") || strings.Contains(logged.String(), "skipping vulnerability scan") {
				t.Errorf("a scan that was stopped said it finished:\n%s", logged.String())
			}
			// Whether the mark lands is not asserted: the statement was
			// already at the server when the client gave up on it, and it
			// may still complete once the row is free. What is asserted is
			// what this process says and returns.
		})
	}
}
