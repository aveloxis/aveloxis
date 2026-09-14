// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"errors"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
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

// Instance is one configured GitLab instance. Client is nil when the
// instance has no API keys: its repositories still classify under ID, but
// are not collectable over the API.
type Instance struct {
	ID      model.Platform
	WebBase string
	APIURL  string
	Client  *Client
	// Primary marks the main instance (gitlab.base_url / gitlab.api_keys),
	// so a not-configured message names the right place to add keys.
	Primary bool
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
func NewInstances(list []*Instance) (*Instances, error) {
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
		entry := *in
		entry.WebBase = base
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
	if in.Client == nil {
		return nil, fmt.Errorf("%w: GitLab instance %s (platform_id %d) has no API keys", ErrInstanceNotConfigured, in.WebBase, p)
	}
	return in.Client, nil
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
