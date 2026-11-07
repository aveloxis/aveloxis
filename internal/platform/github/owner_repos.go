// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// ListOwnerRepos lists every repository of an organization on this
// client's host (stage 1 of summary/59, eval-3 target 1: the one lister
// behind the scheduler's org passes, which replaced four hand-written
// listing loops). With personalFallback, an owner that is not an
// organization — page 1 of /orgs/{owner}/repos answers a definitive 404
// (platform.ErrNotFound; N16: only that, never a transport or rate-limit
// failure, and only on the first page) — is listed as a user through
// /users/{owner}/repos. Pages are read until an empty one under
// WithoutETag; any other request or decode error is yielded once and ends
// the listing. The owner is path-escaped.
func (c *Client) ListOwnerRepos(ctx context.Context, owner string, personalFallback bool) iter.Seq2[platform.OwnerRepo, error] {
	return func(yield func(platform.OwnerRepo, error) bool) {
		encoded := url.PathEscape(owner)
		base := "/orgs/" + encoded + "/repos"
		for page := 1; ; page++ {
			path := fmt.Sprintf("%s?per_page=100&type=all&page=%d", base, page)
			resp, err := c.http.Get(platform.WithoutETag(ctx), path)
			if err != nil {
				if personalFallback && page == 1 && base != "/users/"+encoded+"/repos" && errors.Is(err, platform.ErrNotFound) {
					// The designed fallback, not a failure (review HF-1): Debug.
					c.logger.Debug("not an organization — listing the personal account", "owner", owner)
					base = "/users/" + encoded + "/repos"
					page = 0 // the loop's page++ makes it 1
					continue
				}
				yield(platform.OwnerRepo{}, fmt.Errorf("list %s (page %d): %w", base, page, err))
				return
			}
			var items []struct {
				ID        int64     `json:"id"`
				HTMLURL   string    `json:"html_url"`
				Name      string    `json:"name"`
				CreatedAt time.Time `json:"created_at"`
				Private   bool      `json:"private"`
				Owner     struct {
					Login string `json:"login"`
				} `json:"owner"`
			}
			err = json.NewDecoder(resp.Body).Decode(&items)
			resp.Body.Close()
			if err != nil {
				yield(platform.OwnerRepo{}, fmt.Errorf("decode %s (page %d): %w", base, page, err))
				return
			}
			if len(items) == 0 {
				return
			}
			for _, it := range items {
				if !yield(platform.OwnerRepo{URL: it.HTMLURL, Owner: it.Owner.Login, Name: it.Name, ForgeID: model.ForgeIDString(it.ID), CreatedAt: it.CreatedAt, Private: it.Private}, nil) {
					return
				}
			}
		}
	}
}
