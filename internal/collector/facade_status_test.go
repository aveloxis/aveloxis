// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Item 83 sibling (review round 1 F8): the one-shot `aveloxis collect` path
// recorded the facade as Success on a failed clone, because CollectRepo
// returns a non-nil empty result alongside its error.
func TestFacadeStatusFor(t *testing.T) {
	for name, tc := range map[string]struct {
		failed bool
		res    *FacadeResult
		want   string
	}{
		"clone failed, empty result alongside": {true, &FacadeResult{}, string(StatusError)},
		"no result":                            {false, nil, string(StatusError)},
		"result with errors":                   {false, &FacadeResult{Errors: []error{errors.New("git log: boom")}}, string(StatusError)},
		"clean":                                {false, &FacadeResult{Commits: 3}, string(StatusSuccess)},
	} {
		if got := facadeStatusFor(tc.failed, tc.res); got != tc.want {
			t.Errorf("%s: got %s want %s", name, got, tc.want)
		}
	}
}

// The call site (review round 3: `facadeFailed := false` survived the helper
// test alone): CollectRepo derives the flag from CollectRepo's error, right
// after the call, and hands it to the helper.
func TestCollectRepoRecordsTheFacadeStatusFromTheError(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/collector.go"), "func (c *Collector) CollectRepo("))
	if !strings.Contains(body, "facadeResult, err := c.facade.CollectRepo(ctx, repoID, gitURL)\n\tfacadeFailed := err != nil") {
		t.Error("CollectRepo must derive facadeFailed from the facade's error on the statement after the call")
	}
	if !strings.Contains(body, "facadeStatusFor(facadeFailed, facadeResult)") {
		t.Error("CollectRepo must record the facade status through facadeStatusFor(facadeFailed, facadeResult)")
	}
}
