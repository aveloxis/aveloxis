// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestSnapshotRunOrderAlternates(t *testing.T) {
	for reps, want := range map[int]string{
		1: "A B",
		2: "A B B A",
		3: "A B B A A B",
	} {
		if got := strings.Join(snapshotRunOrder(reps), " "); got != want {
			t.Errorf("%d pairs: got %q, want %q (neither side always first)", reps, got, want)
		}
	}
	if got := snapshotRunOrder(0); len(got) != 0 {
		t.Errorf("no pairs, no runs: %v", got)
	}
}

func TestSummarizeSnapshotTimings(t *testing.T) {
	timing := func(phase string, s float64) snapshotTiming { return snapshotTiming{Phase: phase, Seconds: s} }
	// No-change runs 10 and 11 s: the band is ln(1.1).
	base := []snapshotTiming{timing("no-change", 10), timing("no-change", 11), timing("upgrade", 30)}

	within := summarizeSnapshotTimings(append(base, timing("rerun", 10.5)), 0)
	if math.Abs(within.NoiseBand-math.Log(1.1)) > 1e-9 || within.RerunVerdict != "PASS" || within.WindowVerdict != "n/a" {
		t.Errorf("a rerun inside the band passes; no window, no verdict: %+v", within)
	}
	outside := summarizeSnapshotTimings(append(base, timing("rerun", 20)), 0)
	if outside.RerunVerdict != "FLAG" {
		t.Errorf("a rerun twice the no-change time is outside a 10%% band: %+v", outside)
	}
	if faster := summarizeSnapshotTimings(append(base, timing("rerun", 5)), 0); faster.RerunVerdict != "PASS" {
		t.Errorf("a rerun at half the no-change time costs the deploy nothing: one-sided, PASS: %+v", faster)
	}
	single := summarizeSnapshotTimings([]snapshotTiming{timing("no-change", 10), timing("rerun", 10)}, 0)
	if !math.IsNaN(single.NoiseBand) || single.RerunVerdict != "n/a" {
		t.Errorf("one no-change run measures no band: %+v", single)
	}
	if s := summarizeSnapshotTimings(base, 25*time.Second); s.WindowVerdict != "FAIL" {
		t.Errorf("a 30 s upgrade overruns a 25 s window: %+v", s)
	}
	if s := summarizeSnapshotTimings(base, 30*time.Second); s.WindowVerdict != "PASS" {
		t.Errorf("an upgrade equal to the window is within it: %+v", s)
	}
}

func TestLoadSnapshotSettings(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	got, err := loadSnapshotSettings(write("ok.json", `{"work_mem": "128MB", "jit": "off", "TimeZone": "UTC", "pg_stat_statements.track": "all"}`))
	if err != nil || got["work_mem"] != "128MB" || len(got) != 4 {
		t.Fatalf("a flat object of settings loads: %v %v", got, err)
	}
	if got, err := loadSnapshotSettings(""); err != nil || len(got) != 0 {
		t.Errorf("no file, no settings: %v %v", got, err)
	}
	for name, body := range map[string]string{
		"injection.json": `{"work_mem = '1GB'; DROP DATABASE x; --": "1"}`,
		"nested.json":    `{"work_mem": {"value": "128MB"}}`,
		"broken.json":    `{"work_mem": `,
	} {
		if _, err := loadSnapshotSettings(write(name, body)); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	if _, err := loadSnapshotSettings(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("a missing file is an error")
	}
}

func TestSQLStringLiteralEscapesQuotes(t *testing.T) {
	if got := sqlStringLiteral(`it's`); got != `'it''s'` {
		t.Errorf("got %s", got)
	}
}

// The release's shipped inputs load: a rule file that does not parse would
// fail the run only after an hour of migrates.
func TestShippedSnapshotInputsLoad(t *testing.T) {
	raw, err := os.ReadFile("testdata/expect-0.29.141.json")
	if err != nil {
		t.Fatal(err)
	}
	e, err := db.LoadContentExpectations(raw)
	if err != nil {
		t.Fatal(err)
	}
	must := 0
	for _, r := range e.Rules {
		if r.Mode == "must-differ" {
			must++
		}
	}
	if must < 5 {
		t.Errorf("the 0.29.141 rules require the release's five changes (ledger, gone history, visibility column, earlier era, cache stamp): %d must-differ rules", must)
	}
	for _, shape := range []string{"testdata/shape-provisional.json", "testdata/shape-kate-20261108.json"} {
		if _, err := loadSeedShape(shape); err != nil {
			t.Fatalf("%s: %v", shape, err)
		}
	}
	if settings, err := loadSnapshotSettings("testdata/settings-kate-20261108.json"); err != nil || settings["work_mem"] != "128MB" {
		t.Fatalf("kate's planner settings load: %v %v", settings, err)
	}
}

// The source is never a database the run replaces (the first clone drops its
// target before copying, so --from-db aveloxis_released would destroy the
// source); an already seeded template may be reused, but not re-seeded onto
// itself.
func TestSnapshotSourceIsNeverADatabaseTheRunReplaces(t *testing.T) {
	for _, name := range []string{dataTestSides[dataTestReleased].dbname, dataTestSides[dataTestNew].dbname,
		snapshotDatabases.base, snapshotDatabases.beforeRerun} {
		if err := checkSnapshotSource(snapshotOptions{fromDB: name}); err == nil {
			t.Errorf("--from-db %s must be refused: the run replaces it", name)
		}
	}
	if err := checkSnapshotSource(snapshotOptions{fromDB: seedTemplateDB, seedShape: "shape.json"}); err == nil {
		t.Error("seeding the template from itself must be refused")
	}
	if err := checkSnapshotSource(snapshotOptions{fromDB: seedTemplateDB, rewindSQL: "rewind.sql"}); err == nil {
		t.Error("rewinding the template onto itself must be refused")
	}
	if err := checkSnapshotSource(snapshotOptions{fromDB: seedTemplateDB}); err != nil {
		t.Errorf("reusing a seeded template is allowed: %v", err)
	}
	if err := checkSnapshotSource(snapshotOptions{fromDB: "aveloxis"}); err != nil {
		t.Errorf("an operator database is a source: %v", err)
	}
	if err := checkSnapshotSource(snapshotOptions{fromDB: ""}); err == nil {
		t.Error("an empty source is refused")
	}
}

func TestCheckSnapshotFlags(t *testing.T) {
	changed := func(names ...string) func(string) bool {
		return func(n string) bool { return slices.Contains(names, n) }
	}
	for _, name := range snapshotOnlyFlags {
		if err := checkSnapshotFlags(false, false, changed(name)); err == nil {
			t.Errorf("--%s without --from-db must be refused (it would be ignored and a collection run would start)", name)
		}
	}
	if err := checkSnapshotFlags(true, false, changed("repo")); err == nil {
		t.Error("--repo with --from-db must be refused")
	}
	if err := checkSnapshotFlags(true, true, changed()); err == nil {
		t.Error("--diff-only with --from-db must be refused")
	}
	if err := checkSnapshotFlags(true, false, changed("expect", "reps")); err != nil {
		t.Errorf("snapshot flags with --from-db are fine: %v", err)
	}
	if err := checkSnapshotFlags(false, false, changed("repo", "keep-dbs")); err != nil {
		t.Errorf("collection flags without --from-db are fine: %v", err)
	}
}

func TestNewVerifyFailuresIgnoreTheSourcesOwn(t *testing.T) {
	baseline := []db.VerifyResult{{Check: "case-duplicate repos", Severity: "FAIL"}, {Check: "stranded repos", Severity: "WARN"}}
	local := []db.VerifyResult{{Check: "case-duplicate repos", Severity: "FAIL"}, {Check: "stranded repos", Severity: "FAIL"},
		{Check: "gone episodes match the stamps", Severity: "FAIL"}, {Check: "invalid indexes", Severity: "OK"}}
	got := newVerifyFailures(baseline, local)
	if len(got) != 2 || got[0].Check != "stranded repos" || got[1].Check != "gone episodes match the stamps" {
		t.Errorf("a FAIL the released side already has is the source's; the others are the release's: %+v", got)
	}
}

// The same check failing on both sides with a different detail is the
// release's: the repair that fixed some repositories and not others still
// FAILs the check the released side FAILs by design (the second L10 pass).
func TestNewVerifyFailuresSeeAChangedDetail(t *testing.T) {
	baseline := []db.VerifyResult{{Check: "earlier-era daily buckets", Severity: "FAIL", Detail: "120 buckets — e.g. repo_id {9,8,7}"}}
	same := []db.VerifyResult{{Check: "earlier-era daily buckets", Severity: "FAIL", Detail: "120 buckets — e.g. repo_id {9,8,7}"}}
	partial := []db.VerifyResult{{Check: "earlier-era daily buckets", Severity: "FAIL", Detail: "40 buckets — e.g. repo_id {9}"}}
	if got := newVerifyFailures(baseline, same); len(got) != 0 {
		t.Errorf("an identical FAIL is the source's: %+v", got)
	}
	if got := newVerifyFailures(baseline, partial); len(got) != 1 {
		t.Errorf("a FAIL with another detail is the release's: %+v", got)
	}
}

func TestVerifyNotOK(t *testing.T) {
	local := []db.VerifyResult{{Check: "a", Severity: "OK"}, {Check: "b", Severity: "WARN"}, {Check: "c", Severity: "FAIL"}}
	got := verifyNotOK([]string{"a", "b", "c", "missing"}, local)
	var names []string
	for _, v := range got {
		names = append(names, v.Check+":"+v.Severity)
	}
	if strings.Join(names, " ") != "b:WARN c:FAIL missing:FAIL" {
		t.Errorf("required checks must be OK; a missing one is a FAIL: %v", names)
	}
}

func TestLedgerLabelVersion(t *testing.T) {
	for label, want := range map[string]string{
		"v0.29.140 open a gone episode for every gone-stamped repository": "0.29.140",
		"v0.27.51 backfill dependency_scope":                              "0.27.51",
		"dedup commits":                                                   "",
		"v0.29 something":                                                 "",
		"vX.Y.Z text":                                                     "",
	} {
		got, ok := ledgerLabelVersion(label)
		if got != want || ok != (want != "") {
			t.Errorf("%q: got %q %v, want %q", label, got, ok, want)
		}
	}
}

func TestLoadSnapshotSettingsRefusesListValues(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.json")
	if err := os.WriteFile(p, []byte(`{"search_path": "aveloxis_data, public"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSnapshotSettings(p); err == nil {
		t.Error("a list value would set one element named \"aveloxis_data, public\"; it must be refused")
	}
}

// A sampled probe's detail differs run to run on the same data: it is
// compared by check only, never read as the release's new failure.
func TestNewVerifyFailuresCompareASampledProbeByCheckOnly(t *testing.T) {
	baseline := []db.VerifyResult{{Check: "cached counts vs actual", Severity: "FAIL", Detail: "2 of 30 sampled repos", Sampled: true}}
	local := []db.VerifyResult{{Check: "cached counts vs actual", Severity: "FAIL", Detail: "1 of 30 sampled repos", Sampled: true}}
	if got := newVerifyFailures(baseline, local); len(got) != 0 {
		t.Errorf("another random sample of the same FAIL is not new: %+v", got)
	}
}

func TestSnapshotVerifyUsesTheDeterministicSample(t *testing.T) {
	args := strings.Join(snapshotVerifyArgs("c.json"), " ")
	for _, want := range []string{"-c c.json", "data-verify", "--json", "--deterministic-sample"} {
		if !strings.Contains(args, want) {
			t.Errorf("both sides' data-verify carries %q: %s", want, args)
		}
	}
}
