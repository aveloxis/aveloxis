// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestFleetWalkingCommandsAreInterruptible pins worklist §4: the commands
// that walk the fleet build their context from signal.NotifyContext (so
// Ctrl-C and a SIGTERM — `kill <pid>`; `aveloxis stop` signals only the
// four components — end the walk between items instead of killing the
// process with a statement in flight — the orphaned-backend class) AND
// classify context.Canceled in their report, so the operator
// learns what landed and that a rerun resumes. TestEveryNotifyContextHandlesSIGTERM
// checks only that a NotifyContext call registers SIGTERM; nothing required a
// command to have one, which is how these drifted.
func TestFleetWalkingCommandsAreInterruptible(t *testing.T) {
	for cmd, files := range map[string][]string{
		"heal-collection-gaps":             {"heal_collection_gaps.go", "heal_collection_gaps_run.go"},
		"heal-vulnerabilities":             {"heal_vulnerabilities.go"},
		"backfill-mailing-list-projection": {"backfill_mailing_list_projection.go"},
		"mark-gone-repos":                  {"mark_gone_repos.go"},
		"heal-libyear":                     {"heal_libyear.go"},
		"reconcile-repos":                  {"reconcile_repos.go"},
		"install-tools":                    {"install_tools_cmd.go"},
		"upgrade-tools":                    {"upgrade_tools_cmd.go"},
	} {
		var src strings.Builder
		for _, f := range files {
			src.WriteString(srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/"+f)))
		}
		code := src.String()
		if !strings.Contains(code, "signal.NotifyContext(") {
			t.Errorf("%s: a fleet-walking command must build its context with signal.NotifyContext", cmd)
		}
		// The report classifies the interrupt (errors.Is(err, context.Canceled)
		// or ctx.Err()) and says so to the operator.
		if !(strings.Contains(code, "context.Canceled") || strings.Contains(code, "ctx.Err()")) || !strings.Contains(code, "interrupted") {
			t.Errorf("%s: a fleet-walking command must classify the interrupt and report it as interrupted (what landed; a rerun resumes)", cmd)
		}
	}
}

func TestHealVulnerabilitiesReport(t *testing.T) {
	msg, err := healVulnerabilitiesReport(7, 1, 20, context.Canceled)
	if msg != "" || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "8 of 20") || !strings.Contains(err.Error(), "rerun") {
		t.Errorf("interrupted = (%q, %v); want an error naming 8 of 20 and the rerun", msg, err)
	}
	if msg, err := healVulnerabilitiesReport(3, 0, 3, nil); err != nil || !strings.Contains(msg, "healed=3 failed=0") {
		t.Errorf("clean run = (%q, %v)", msg, err)
	}
	if msg, err := healVulnerabilitiesReport(2, 1, 3, nil); err == nil || !strings.Contains(msg, "failed=1") {
		t.Errorf("a failed repository = (%q, %v); want the summary and a non-nil error", msg, err)
	}
	if err := backfillProjectionReport(10, 2, 0, context.Canceled); !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "10 keyed") {
		t.Errorf("backfill interrupted = %v; want the counts and the cancellation", err)
	}
	other := errors.New("boom")
	if err := backfillProjectionReport(1, 1, 1, other); !errors.Is(err, other) || strings.Contains(err.Error(), "interrupted") {
		t.Errorf("backfill other error = %v; want it unchanged", err)
	}
}

// TestToolCommandsReportFailures (batch 4a review round 14): install-tools
// exits non-zero when a tool failed, like upgrade-tools; a per-tool timeout
// is named as one in both CLIs instead of "context deadline exceeded" (with
// upgrade-tools' pipx hint, wrong for a timeout).
func TestToolCommandsReportFailures(t *testing.T) {
	if err := installToolsReport(0, 0, 3); err != nil {
		t.Errorf("no failures = %v; want nil", err)
	}
	if err := installToolsReport(1, 0, 3); err == nil || !strings.Contains(err.Error(), "1 of 3 tools failed") {
		t.Errorf("one failure = %v; want an error naming 1 of 3", err)
	}
	if err := installToolsReport(0, 1, 3); err == nil || !strings.Contains(err.Error(), "not on PATH") {
		t.Errorf("one off PATH = %v; want an error — serve's LookPath fails the same way (round 15)", err)
	}
	if err := installToolsReport(1, 1, 3); err == nil || !strings.Contains(err.Error(), "failed") || !strings.Contains(err.Error(), "off PATH") {
		t.Errorf("both = %v; want both causes named", err)
	}
	wrapped := fmt.Errorf("pipx upgrade x: %w (if scancode was installed via pip ...)", context.DeadlineExceeded)
	if got := toolFailureText(wrapped); !strings.Contains(got, "timed out after "+collector.ToolInstallBound().String()) || strings.Contains(got, "pipx") {
		t.Errorf("a wrapped deadline = %q; want the per-tool bound named and the hint dropped", got)
	}
	if got := toolFailureText(errors.New("exit status 1")); got != "exit status 1" {
		t.Errorf("another error = %q; want it unchanged", got)
	}
	for _, f := range []string{"install_tools_cmd.go", "upgrade_tools_cmd.go"} {
		code := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/"+f))
		if !strings.Contains(code, "toolFailureText(") {
			t.Errorf("%s prints a tool failure without toolFailureText: a timeout reads as \"context deadline exceeded\"", f)
		}
	}
	if code := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/install_tools_cmd.go"), "func runInstallTools(")); !strings.Contains(code, "return installToolsReport(failed, offPath, len(tools))") || !strings.Contains(code, "offPath++") {
		t.Error("runInstallTools must count the off-PATH outcome and end with installToolsReport(failed, offPath, len(tools)) so neither exits 0")
	}
}
