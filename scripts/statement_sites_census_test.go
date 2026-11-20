// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The closing suite checks the write-policy judge's coverage (0.29.213):
// it has every DB-tier package record the store's library calls it reached,
// and after a green suite requires their union to cover every such call
// that is not on the baseline. Driven here with a recorded green suite and
// a folder of records that is EMPTY: no package wrote one, so the census
// must fail and say why. (The store's own test,
// TestTheSuiteReachesEveryStatementSite, checks nothing without the
// variable this script sets for it; this test is what shows the script
// sets it.)
func TestTheClosingSuiteChecksStatementSites(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Fatalf("the closing suite is a bash script, and bash is not installed: %v", err)
	}
	root := srctest.Root(t)
	run := func(sites string) (string, error) {
		cmd := exec.Command("bash", filepath.Join(root, "scripts", "skip-census.sh"))
		cmd.Env = append(os.Environ(),
			"AVELOXIS_TEST_DB=postgres://unused/the-suite-is-recorded",
			"SKIP_CENSUS_SUITE_JSON="+filepath.Join(root, "scripts", "testdata", "skip_census_passed_suite.jsonl"),
			"SKIP_CENSUS_OUT="+t.TempDir(),
		)
		if sites != "" {
			cmd.Env = append(cmd.Env, "SKIP_CENSUS_SITES="+sites)
		}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	report, err := run(t.TempDir())
	if err == nil {
		t.Errorf("a green suite whose packages recorded nothing passed the census:\n%s", report)
	}
	for _, want := range []string{"the write-policy judge's coverage", "holds no record of what internal/db reached", "sites-check.txt"} {
		if !strings.Contains(report, want) {
			t.Errorf("the census report lacks %q:\n%s", want, report)
		}
	}
	// The suite itself is run with the variable that makes each package
	// write its record, into the folder the check then reads.
	script := srctest.Read(t, "scripts/skip-census.sh")
	for _, want := range []string{
		`AVELOXIS_STATEMENT_SITES_DIR="$SITES" go test ${SKIP_CENSUS_GOFLAGS:--race -shuffle=on} ./... -count=1 -json`,
		`AVELOXIS_STATEMENT_SITES_CHECK="$SITES"`,
		`go test ./internal/db/ -run '^TestTheSuiteReachesEveryStatementSite$' -count=1`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("scripts/skip-census.sh lacks %q", want)
		}
	}
}
