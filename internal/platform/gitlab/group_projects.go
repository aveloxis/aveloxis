// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// GroupProject is one project in a GitLab group listing.
type GroupProject struct {
	WebURL  string
	Owner   string // namespace full path, e.g. "group/subgroup"
	Name    string
	ForgeID string // numeric project id (model.ForgeIDString), rename-proof
}

// ListGroupProjects lists every project in a group, subgroups included, on
// this client's instance (v0.30.0: the scheduler's legacy group refresh no
// longer builds a keyed HTTP client of its own). Pages are read until an
// empty one; a request or decode error is yielded once and ends the listing.
func (c *Client) ListGroupProjects(ctx context.Context, group string) iter.Seq2[GroupProject, error] {
	return func(yield func(GroupProject, error) bool) {
		encoded := url.PathEscape(group)
		for page := 1; ; page++ {
			path := fmt.Sprintf("/groups/%s/projects?per_page=100&include_subgroups=true&page=%d", encoded, page)
			resp, err := c.http.Get(platform.WithoutETag(ctx), path)
			if err != nil {
				yield(GroupProject{}, fmt.Errorf("list group %q projects (page %d): %w", group, page, err))
				return
			}
			var items []struct {
				ID        int64  `json:"id"`
				WebURL    string `json:"web_url"`
				Name      string `json:"name"`
				Namespace struct {
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
				if !yield(GroupProject{WebURL: it.WebURL, Owner: it.Namespace.FullPath, Name: it.Name, ForgeID: model.ForgeIDString(it.ID)}, nil) {
					return
				}
			}
		}
	}
}
