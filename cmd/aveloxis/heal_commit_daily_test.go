// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// summary/49: `aveloxis heal-commit-daily` fills repo_commit_daily for
// repositories collected before the facade wrote it (one commits scan each,
// largest first — the cost today's first page view pays, paid once in the
// background). Dry run by default; --apply writes; an interrupt reports
// what landed (each repository is its own transaction) and says to rerun.
func TestHealCommitDailyReport(t *testing.T) {
	cases := map[string]struct {
		apply           bool
		pending, filled int64
		err             error
		want            string // substring of the line, or of the error
		isErr           bool
	}{
		"dry run":          {false, 12, 0, nil, "12 repositories", false},
		"dry run, nothing": {false, 0, 0, nil, "0 repositories", false},
		"applied":          {true, 12, 12, nil, "12 of 12", false},
		"applied, partial": {true, 12, 7, nil, "7 of 12", false},
		"interrupted":      {true, 12, 7, context.Canceled, "rerun", true},
		"interrupted dry":  {false, 12, 0, context.Canceled, "nothing was written", true},
		"store error":      {true, 12, 3, errors.New("connection refused"), "connection refused", true},
	}
	for name, tc := range cases {
		line, err := healCommitDailyReport(tc.apply, tc.pending, tc.filled, tc.err)
		if tc.isErr {
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: want an error containing %q, got line=%q err=%v", name, tc.want, line, err)
			}
			continue
		}
		if err != nil || !strings.Contains(line, tc.want) {
			t.Errorf("%s: want a line containing %q, got %q err=%v", name, tc.want, line, err)
		}
	}
}

// Review round 1 F7: a dry run counts every pending repository; --limit
// bounds only what --apply fills.
func TestHealCommitDailyDryRunIgnoresTheLimit(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/heal_commit_daily.go"))
	body := srctest.FuncBody(t, src, "func healCommitDailyCmd(")
	if !strings.Contains(body, "listLimit := 1 << 30\n\t\t\tif apply && limit > 0 {\n\t\t\t\tlistLimit = limit\n\t\t\t}") {
		t.Error("the list is capped by --limit only when --apply fills it; a dry run reports the whole count")
	}
}
