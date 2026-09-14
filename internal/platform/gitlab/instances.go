// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"errors"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// ErrInstanceNotConfigured means a repository's GitLab instance has no API
// client in this process: its platform_id is not a configured instance, or
// the instance has no keys. Git-based collection still runs; API phases do
// not, and the job records the reason.
var ErrInstanceNotConfigured = errors.New("GitLab instance not configured")

// ErrInstanceMismatch means a repository's platform_id and its URL name
// different GitLab instances — a stale platform_id after a URL rewrite. The
// repository is never collected with either instance's client.
var ErrInstanceMismatch = errors.New("repository URL is not on its GitLab instance")

// InstanceSpec is one configured GitLab instance as NewInstances takes it.
// Client may be nil (tests of classification-only routing); production
// builds a client for every registered instance, keys or not, so a key
// added at runtime can make it collectable (v0.30.0 Phase C).
type InstanceSpec struct {
	ID      model.Platform
	WebBase string
	APIURL  string
	Client  *Client
	// Primary marks the main instance (gitlab.base_url / gitlab.api_keys),
	// so a not-configured message names the right place to add keys.
	Primary bool
}

// Instance is one routed GitLab instance. Its client is reachable only
// through KeyedClient, which answers false while the instance has no active
// key — so no caller can reach an instance's API without keys (invariant 3,
// enforced here rather than at each call site; SR-18).
type Instance struct {
	ID      model.Platform
	WebBase string
	APIURL  string
	Primary bool
	client  *Client
}

// KeyedClient returns the instance's client when it has at least one active
// API key. A keyless instance's repositories still classify under ID; they
// are collected git-only and each job records why.
func (in *Instance) KeyedClient() (*Client, bool) {
	if in == nil || in.client == nil || in.client.keys.IsEmpty() {
		return nil, false
	}
	return in.client, true
}

// Instances routes repositories to their GitLab instance's client (v0.30.0).
// All methods are safe on a nil *Instances (no GitLab configured).
type Instances struct {
	list      []*Instance
	byID      map[model.Platform]*Instance
	webBases  []string
	byWebBase map[string]*Instance
}

// NewInstances validates and indexes the configured instances. It refuses a
// nil entry, a non-GitLab id, a web base that does not normalize, a
// duplicate id or web base, and a client whose own id or web base differs
// from its entry's.
func NewInstances(list []*InstanceSpec) (*Instances, error) {
	s := &Instances{byID: map[model.Platform]*Instance{}, byWebBase: map[string]*Instance{}}
	for i, in := range list {
		if in == nil {
			return nil, fmt.Errorf("gitlab instances: entry %d is nil", i)
		}
		if !in.ID.IsGitLab() {
			return nil, fmt.Errorf("gitlab instances: %s has non-GitLab platform_id %d", in.WebBase, in.ID)
		}
		base, err := model.NormalizeInstanceWebBase(in.WebBase)
		if err != nil {
			return nil, fmt.Errorf("gitlab instances: %w", err)
		}
		if _, dup := s.byID[in.ID]; dup {
			return nil, fmt.Errorf("gitlab instances: platform_id %d appears twice", in.ID)
		}
		if _, dup := s.byWebBase[base]; dup {
			return nil, fmt.Errorf("gitlab instances: web URL %s appears twice", base)
		}
		if in.Client != nil && (in.Client.Platform() != in.ID || in.Client.WebBase() != base) {
			return nil, fmt.Errorf("gitlab instances: the client for %s (platform_id %d) belongs to %s (platform_id %d)", base, in.ID, in.Client.WebBase(), in.Client.Platform())
		}
		entry := Instance{ID: in.ID, WebBase: base, APIURL: in.APIURL, Primary: in.Primary, client: in.Client}
		s.list = append(s.list, &entry)
		s.byID[in.ID] = &entry
		s.byWebBase[base] = &entry
		s.webBases = append(s.webBases, base)
	}
	return s, nil
}

// ForRepo returns the client for a repository with platform_id p and URL
// gitURL. The URL must live under the web base of the instance registered
// as p (longest match across all instances); otherwise ErrInstanceMismatch.
// An unknown id, a non-GitLab id and a keyless instance are
// ErrInstanceNotConfigured.
func (s *Instances) ForRepo(p model.Platform, gitURL string) (*Client, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: platform_id %d (no GitLab instances configured)", ErrInstanceNotConfigured, p)
	}
	in, ok := s.byID[p]
	if !ok {
		return nil, fmt.Errorf("%w: platform_id %d is not a configured GitLab instance", ErrInstanceNotConfigured, p)
	}
	if base, _, matched := model.MatchInstanceWebBase(gitURL, s.webBases); !matched || base != in.WebBase {
		return nil, fmt.Errorf("%w: %s has platform_id %d (%s) but its URL is under %q", ErrInstanceMismatch, gitURL, p, in.WebBase, base)
	}
	c, keyed := in.KeyedClient()
	if !keyed {
		return nil, fmt.Errorf("%w: GitLab instance %s (platform_id %d) has no API keys", ErrInstanceNotConfigured, in.WebBase, p)
	}
	return c, nil
}

// ForWebURL returns the instance a URL (a repository, group or project page)
// lives under, by longest web base.
func (s *Instances) ForWebURL(u string) (*Instance, bool) {
	if s == nil {
		return nil, false
	}
	base, _, ok := model.MatchInstanceWebBase(u, s.webBases)
	if !ok {
		return nil, false
	}
	return s.byWebBase[base], true
}

// ByID returns the instance registered as p.
func (s *Instances) ByID(p model.Platform) (*Instance, bool) {
	if s == nil {
		return nil, false
	}
	in, ok := s.byID[p]
	return in, ok
}

// WebBases returns every configured instance's web base — the URL parser's
// hints — including keyless instances.
func (s *Instances) WebBases() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.webBases...)
}

// All returns the configured instances in configuration order.
func (s *Instances) All() []*Instance {
	if s == nil {
		return nil
	}
	return append([]*Instance(nil), s.list...)
}

// ReconcileKeys makes each instance's pool hold exactly the tokens its own
// web base maps to in pools (v0.30.0 Phase C, live key reload; pools is a
// forgekeys.PartitionGitLabTokens result). The pairing happens HERE, entry
// by entry, so one instance's tokens can never be reconciled into another
// instance's pool; an instance absent from pools is emptied. It returns what
// changed per web base (instances without a client are skipped).
func (s *Instances) ReconcileKeys(pools map[string][]string) map[string]platform.ReconcileResult {
	out := map[string]platform.ReconcileResult{}
	if s == nil {
		return out
	}
	for _, in := range s.list {
		if in.client == nil {
			continue
		}
		out[in.WebBase] = in.client.keys.Reconcile(pools[in.WebBase])
	}
	return out
}

// InstanceKeys is one instance's key snapshot for the key report.
type InstanceKeys struct {
	ID         model.Platform
	WebBase    string
	APIURL     string
	Primary    bool
	HasClient  bool
	ActiveKeys int
	Keys       []platform.KeySnapshot
}

// KeySnapshots returns every instance's key state (active and draining keys;
// never a token), in configuration order.
func (s *Instances) KeySnapshots() []InstanceKeys {
	if s == nil {
		return nil
	}
	out := make([]InstanceKeys, 0, len(s.list))
	for _, in := range s.list {
		ik := InstanceKeys{ID: in.ID, WebBase: in.WebBase, APIURL: in.APIURL, Primary: in.Primary, HasClient: in.client != nil}
		if in.client != nil {
			ik.Keys, _ = in.client.keys.Snapshot()
			ik.ActiveKeys = in.client.keys.Len()
		}
		out = append(out, ik)
	}
	return out
}

// OnPermanentRedirect installs hook on every instance's client (keyed or
// not yet), so a rename observed after a key is added is still reported.
func (s *Instances) OnPermanentRedirect(hook func(from, to string)) {
	if s == nil {
		return
	}
	for _, in := range s.list {
		if in.client != nil {
			in.client.OnPermanentRedirect(hook)
		}
	}
}
