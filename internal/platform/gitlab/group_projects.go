// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// GroupProject is one project in a GitLab group listing (v0.30.0's shape,
// ported in stage 1 of summary/59 so the rebase stays mechanical; the
// scheduler converts it with OwnerRepo).
//
// Name is the project's PATH (its slug: GitLab's `path`), never its `name`,
// which is a display name ("GitLab Runner" for gitlab-runner). Every later
// API call addresses the project as owner/name, so a display name stored
// as the name made the project uncollectable (0.29.106 whole-branch review
// P1). GitHub's `name` IS the slug, so the two listers agree on what Name
// holds.
type GroupProject struct {
	WebURL     string
	Owner      string    // namespace full path, e.g. "group/subgroup"
	Name       string    // the project's path (slug)
	ForgeID    string    // numeric project id (model.ForgeIDString), rename-proof
	CreatedAt  time.Time // when the forge created the project (v0.29.63's forge-ID recorder dates a change with it)
	Visibility string    // GitLab's `visibility`: public, internal or private ("" when the listing omitted it)
}

// OwnerRepo is the project as the one ingest item (platform.OwnerRepo).
// Private is every visibility but `public` — `internal` is visible to
// signed-in users only, which an unauthenticated probe is not — and an
// omitted visibility counts as not public (the safe side: no resurrection
// on a listing whose visibility the forge did not state).
func (p GroupProject) OwnerRepo() platform.OwnerRepo {
	return platform.OwnerRepo{URL: p.WebURL, Owner: p.Owner, Name: p.Name, ForgeID: p.ForgeID, CreatedAt: p.CreatedAt, Private: p.Visibility != "public"}
}

// ListGroupProjects lists every project in a group, subgroups included, on
// this client's instance (the scheduler's group refresh and the user-org
// pass no longer build a keyed HTTP client of their own). group is the
// group's full path. Only the group's own projects are listed
// (with_shared=false): a project shared into the group from another
// namespace is that namespace's, as GitHub's /orgs/{org}/repos lists an
// organization's own repositories (parity; documented in the README's data
// gaps). Pages are read until an empty one; a request or decode error is
// yielded once and ends the listing.
func (c *Client) ListGroupProjects(ctx context.Context, group string) iter.Seq2[GroupProject, error] {
	return func(yield func(GroupProject, error) bool) {
		encoded := url.PathEscape(group)
		for page := 1; ; page++ {
			path := fmt.Sprintf("/groups/%s/projects?per_page=100&include_subgroups=true&with_shared=false&page=%d", encoded, page)
			resp, err := c.http.Get(platform.WithoutETag(ctx), path)
			if err != nil {
				yield(GroupProject{}, fmt.Errorf("list group %q projects (page %d): %w", group, page, err))
				return
			}
			var items []struct {
				ID         int64     `json:"id"`
				WebURL     string    `json:"web_url"`
				Path       string    `json:"path"` // the slug; `name` is the display name
				CreatedAt  time.Time `json:"created_at"`
				Visibility string    `json:"visibility"`
				Namespace  struct {
					FullPath string `json:"full_path"`
				} `json:"namespace"`
			}
			err = json.NewDecoder(resp.Body).Decode(&items)
			resp.Body.Close()
			if err != nil {
				yield(GroupProject{}, fmt.Errorf("decode group %q projects (page %d): %w", group, page, err))
				return
			}
			if len(items) == 0 {
				return
			}
			for _, it := range items {
				if !yield(GroupProject{WebURL: it.WebURL, Owner: it.Namespace.FullPath, Name: it.Path, ForgeID: model.ForgeIDString(it.ID), CreatedAt: it.CreatedAt, Visibility: it.Visibility}, nil) {
					return
				}
			}
		}
	}
}
