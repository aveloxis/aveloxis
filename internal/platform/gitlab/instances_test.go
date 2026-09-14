// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func mustClient(t *testing.T, id model.Platform, webBase, apiURL string) *Client {
	t.Helper()
	logger := quietLogger()
	c, err := New(id, webBase, apiURL, platform.NewKeyPool([]string{"k-" + webBase}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// v0.30.0: the router picks a repository's client by its platform_id AND
// its URL — the longest configured web base it lives under — and never
// hands a repository to another instance's client.
func TestInstancesForRepo(t *testing.T) {
	com := mustClient(t, model.PlatformGitLab, "https://gitlab.com", "https://gitlab.com/api/v4")
	edu := mustClient(t, 101, "https://code.example.invalid/gitlab", "https://api.example.invalid/api/v4")
	eduRoot := mustClient(t, 102, "https://code.example.invalid", "https://code.example.invalid/api/v4")
	inst, err := NewInstances([]*Instance{
		{ID: model.PlatformGitLab, WebBase: com.WebBase(), APIURL: "https://gitlab.com/api/v4", Client: com},
		{ID: 101, WebBase: edu.WebBase(), APIURL: "https://api.example.invalid/api/v4", Client: edu},
		{ID: 102, WebBase: eduRoot.WebBase(), APIURL: "https://code.example.invalid/api/v4", Client: eduRoot},
		{ID: 103, WebBase: "https://salsa.example.invalid", APIURL: "https://salsa.example.invalid/api/v4"}, // no keys
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		id      model.Platform
		url     string
		want    *Client
		wantErr error
	}{
		{"gitlab.com", model.PlatformGitLab, "https://gitlab.com/g/r", com, nil},
		{"sub-path instance", 101, "https://code.example.invalid/gitlab/g/r", edu, nil},
		{"same host, root instance", 102, "https://code.example.invalid/other/r", eduRoot, nil},
		{"keyless instance", 103, "https://salsa.example.invalid/g/r", nil, ErrInstanceNotConfigured},
		{"unregistered id", 150, "https://unknown.example.invalid/g/r", nil, ErrInstanceNotConfigured},
		{"id says gitlab.com, URL says another instance", model.PlatformGitLab, "https://code.example.invalid/gitlab/g/r", nil, ErrInstanceMismatch},
		{"id says sub-path instance, URL is the root instance", 101, "https://code.example.invalid/other/r", nil, ErrInstanceMismatch},
		{"not a GitLab id", model.PlatformGitHub, "https://github.com/o/r", nil, ErrInstanceNotConfigured},
	}
	for _, tc := range cases {
		got, err := inst.ForRepo(tc.id, tc.url)
		if !errors.Is(err, tc.wantErr) || got != tc.want {
			t.Errorf("%s: ForRepo = (%p, %v), want (%p, %v)", tc.name, got, err, tc.want, tc.wantErr)
		}
	}

	if in, ok := inst.ForWebURL("https://code.example.invalid/gitlab/group"); !ok || in.ID != 101 {
		t.Errorf("ForWebURL(sub-path group) = (%v, %v), want instance 101", in, ok)
	}
	if _, ok := inst.ForWebURL("https://github.com/org"); ok {
		t.Error("ForWebURL matched a URL under no instance")
	}
	if got := len(inst.WebBases()); got != 4 {
		t.Errorf("WebBases has %d entries, want all 4 (keyless instances still classify)", got)
	}

	var nilInst *Instances
	if _, err := nilInst.ForRepo(model.PlatformGitLab, "https://gitlab.com/g/r"); !errors.Is(err, ErrInstanceNotConfigured) {
		t.Errorf("nil router ForRepo = %v, want ErrInstanceNotConfigured", err)
	}
	if _, ok := nilInst.ForWebURL("https://gitlab.com/g"); ok || nilInst.WebBases() != nil || nilInst.All() != nil {
		t.Error("a nil router must answer empty")
	}
}

func TestNewInstancesRefusesInconsistentInstances(t *testing.T) {
	com := mustClient(t, model.PlatformGitLab, "https://gitlab.com", "https://gitlab.com/api/v4")
	other := mustClient(t, 101, "https://other.example.invalid", "https://other.example.invalid/api/v4")
	cases := map[string][]*Instance{
		"duplicate id":           {{ID: 101, WebBase: "https://a.invalid"}, {ID: 101, WebBase: "https://b.invalid"}},
		"duplicate web base":     {{ID: 101, WebBase: "https://a.invalid"}, {ID: 102, WebBase: "https://A.invalid/"}},
		"non-GitLab id":          {{ID: model.PlatformGitHub, WebBase: "https://a.invalid"}},
		"client of another id":   {{ID: 101, WebBase: "https://other.example.invalid", Client: com}},
		"client of another base": {{ID: 101, WebBase: "https://a.invalid", Client: other}},
		"invalid web base":       {{ID: 101, WebBase: "not a url"}},
		"nil entry":              {nil},
	}
	for name, list := range cases {
		if _, err := NewInstances(list); err == nil {
			t.Errorf("%s: NewInstances accepted it", name)
		}
	}
}
