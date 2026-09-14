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
	emptyClient, err := New(104, "https://empty.example.invalid", "https://empty.example.invalid/api/v4", platform.NewKeyPool(nil, quietLogger()), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	inst, err := NewInstances([]*InstanceSpec{
		{ID: model.PlatformGitLab, WebBase: com.WebBase(), APIURL: "https://gitlab.com/api/v4", Client: com},
		{ID: 101, WebBase: edu.WebBase(), APIURL: "https://api.example.invalid/api/v4", Client: edu},
		{ID: 102, WebBase: eduRoot.WebBase(), APIURL: "https://code.example.invalid/api/v4", Client: eduRoot},
		{ID: 103, WebBase: "https://salsa.example.invalid", APIURL: "https://salsa.example.invalid/api/v4"},                      // no client at all
		{ID: 104, WebBase: "https://empty.example.invalid", APIURL: "https://empty.example.invalid/api/v4", Client: emptyClient}, // a client whose pool has no keys
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
		{"instance whose pool is empty", 104, "https://empty.example.invalid/g/r", nil, ErrInstanceNotConfigured},
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
	if got := len(inst.WebBases()); got != 5 {
		t.Errorf("WebBases has %d entries, want all 5 (keyless instances still classify)", got)
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
	cases := map[string][]*InstanceSpec{
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

// v0.30.0 Phase C (live key reload): every instance keeps its own pool for
// the life of the process, so a key added for a keyless instance makes it
// collectable and removing its last key makes it not configured again —
// without rebuilding the router. ReconcileKeys pairs each entry's OWN pool
// with the partition entry for its own web base; nothing outside this
// package can reach a pool.
func TestInstancesReconcileKeysPerInstance(t *testing.T) {
	logger := quietLogger()
	mk := func(id model.Platform, base string, toks []string) *InstanceSpec {
		c, err := New(id, base, base+"/api/v4", platform.NewKeyPool(toks, logger), logger)
		if err != nil {
			t.Fatal(err)
		}
		return &InstanceSpec{ID: id, WebBase: base, APIURL: base + "/api/v4", Client: c}
	}
	inst, err := NewInstances([]*InstanceSpec{
		mk(model.PlatformGitLab, "https://a.example.invalid", []string{"tok-a"}),
		mk(101, "https://b.example.invalid", nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inst.ForRepo(101, "https://b.example.invalid/g/r"); !errors.Is(err, ErrInstanceNotConfigured) {
		t.Fatalf("keyless B before reconcile: %v, want ErrInstanceNotConfigured", err)
	}

	res := inst.ReconcileKeys(map[string][]string{
		"https://a.example.invalid": {"tok-a"},
		"https://b.example.invalid": {"tok-b"},
	})
	if res["https://b.example.invalid"].Added != 1 || res["https://a.example.invalid"] != (platform.ReconcileResult{}) {
		t.Fatalf("ReconcileKeys = %+v, want B +1 and A unchanged", res)
	}
	c, err := inst.ForRepo(101, "https://b.example.invalid/g/r")
	if err != nil || c.Platform() != 101 {
		t.Fatalf("B after a key was added: (%v, %v), want its client", c, err)
	}
	got := map[string][]string{}
	for _, ik := range inst.KeySnapshots() {
		for _, k := range ik.Keys {
			got[ik.WebBase] = append(got[ik.WebBase], k.KeyID)
		}
	}
	if len(got["https://a.example.invalid"]) != 1 || got["https://a.example.invalid"][0] != platform.KeyID("tok-a") ||
		len(got["https://b.example.invalid"]) != 1 || got["https://b.example.invalid"][0] != platform.KeyID("tok-b") {
		t.Fatalf("key snapshots per instance = %v, want tok-a only on A and tok-b only on B", got)
	}

	// A partition without an entry for an instance empties that instance —
	// never leaves another instance's tokens in it.
	inst.ReconcileKeys(map[string][]string{"https://a.example.invalid": {"tok-a"}})
	if _, err := inst.ForRepo(101, "https://b.example.invalid/g/r"); !errors.Is(err, ErrInstanceNotConfigured) {
		t.Fatalf("B after its last key was removed: %v, want ErrInstanceNotConfigured", err)
	}
	if in, ok := inst.ByID(101); !ok {
		t.Fatal("ByID(101) missing")
	} else if _, keyed := in.KeyedClient(); keyed {
		t.Fatal("KeyedClient must report false for an instance with no active keys")
	}
}

func TestNewAcceptsEmptyPoolRefusesNil(t *testing.T) {
	logger := quietLogger()
	if _, err := New(101, "https://b.example.invalid", "https://b.example.invalid/api/v4", platform.NewKeyPool(nil, logger), logger); err != nil {
		t.Fatalf("New with an empty pool = %v, want nil (the router gates on keys at call time)", err)
	}
	if _, err := New(101, "https://b.example.invalid", "https://b.example.invalid/api/v4", nil, logger); err == nil {
		t.Fatal("New with a nil pool must be refused")
	}
}
