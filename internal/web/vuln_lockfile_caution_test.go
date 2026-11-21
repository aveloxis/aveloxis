// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// The repository page shows the lockfile caution as a warning notice (the
// page's existing one) when the tile carries it, and nothing when it does
// not (0.29.214; operator, 2026-11-20).
func TestRepoDetailShowsTheLockfileCautionAndOtherwiseNothing(t *testing.T) {
	s := New(nil, config.WebConfig{}, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	render := func(tile vulnTile) string {
		t.Helper()
		data := map[string]any{}
		for k, v := range renderFixtures["repo_detail"].(map[string]any) {
			data[k] = v
		}
		data["Vuln"] = tile
		var b bytes.Buffer
		if err := s.tmpl.ExecuteTemplate(&b, "repo_detail", data); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	const caution = "Lockfiles could not be read (reading them took <more> memory than allowed). Transitive dependencies are not checked."
	out := render(vulnTile{Value: "0", State: "scanned 2026-11-01", LockfileCaution: caution})
	want := `<div class="notice notice-warn" style="font-size:13px">Lockfiles could not be read (reading them took &lt;more&gt; memory than allowed). Transitive dependencies are not checked.</div>`
	if !strings.Contains(out, want) {
		t.Errorf("the caution is not on the page as a warning notice (escaped):\n%s", out)
	}
	out = render(vulnTile{Value: "0", State: "scanned 2026-11-01"})
	if strings.Contains(out, "Lockfiles") || strings.Contains(out, "lockfile") {
		t.Error("a repository with no caution says something about lockfiles")
	}
}

// The tile's count is a true count of what was checked and does not change
// with the caution (operator, 2026-11-21: "Withdraw, keep the count"): the
// built-in page says no sentence of its own about a clean result, so the
// tile's value and state line are the same with the caution as without.
func TestTheVulnerabilityTileKeepsItsCountUnderTheLockfileCaution(t *testing.T) {
	at := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for _, outcome := range []string{model.ScanSent, model.ScanNothingToSend, model.ScanUnsendable, model.ScanFailed, ""} {
		tile := func(reason string) vulnTile {
			in := vulnclass.CoverageInputs{HasRow: true, ScanStamp: &at}
			in.Coverage.AnalysedAt = &at
			if outcome != "" {
				in.Coverage.ScanAttemptAt, in.Coverage.Scan.Outcome = &at, outcome
			}
			in.Coverage.LockfilesUnreadReason = reason
			v := vulnclass.Coverage(in)
			return vulnTileOf(&db.RepoStats{Coverage: &v})
		}
		without, with := tile(""), tile(model.InventoryAttemptReadFailed)
		if with.LockfileCaution == "" || without.LockfileCaution != "" {
			t.Fatalf("%q: caution with %q, without %q", outcome, with.LockfileCaution, without.LockfileCaution)
		}
		if with.Value != without.Value || with.State != without.State || with.SecondLine != without.SecondLine {
			t.Errorf("%q: the tile reads %q / %q with the caution and %q / %q without", outcome, with.Value, with.State, without.Value, without.State)
		}
	}
	in := vulnclass.CoverageInputs{HasRow: true, ScanStamp: &at}
	in.Coverage.AnalysedAt, in.Coverage.ScanAttemptAt, in.Coverage.Scan.Outcome = &at, &at, model.ScanSent
	in.Coverage.LockfilesUnreadReason = model.InventoryAttemptReadFailed
	v := vulnclass.Coverage(in)
	if got := vulnTileOf(&db.RepoStats{Coverage: &v}); got.Value != "0" || got.State != "scanned 2026-11-02" {
		t.Errorf("scanned, lockfiles unread: the tile reads %q / %q; want 0 / scanned 2026-11-02", got.Value, got.State)
	}
}
