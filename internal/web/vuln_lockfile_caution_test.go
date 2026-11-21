// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
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
