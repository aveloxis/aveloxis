// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package github — search_commit.go implements
// platform.Client.SearchCommitByAuthorEmail using GitHub's GLOBAL commit
// search: GET /search/commits?q=author-email:{email}.
//
// Why this exists alongside SearchUserByEmail: SearchUserByEmail only matches
// a user's PUBLIC profile email, which most people keep private — a 2026-06-04
// bootstrap over real mailing-list senders resolved only ~8-11% that way.
// Global commit search finds the login from ANY commit the email authored,
// across all of GitHub, defeating the private-profile-email wall — ~35% on the
// same cohort. It is the load-bearing step of the shared email→identity
// resolver (summary/12 §5g).
//
// Rate-limit context: /search/commits runs on GitHub's SEARCH budget
// (30 requests/minute per token), the SAME pool as SearchUserByEmail — callers
// must pace/gate (the sender-resolve and search-resolve tickers gate by
// message count + cooldown). The cloak-preview media type is GA, so the
// HTTPClient's default Accept (application/json) works (verified 2026-06-04).
//
// Contract: identical to SearchUserByEmail — returns ("", 0, nil) on zero
// hits (NOT an error); returns an error only on transport / 5xx failures.

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// SearchCommitByAuthorEmail looks up a GitHub login by searching for any
// commit authored with the given email. See
// platform.Client.SearchCommitByAuthorEmail for the full contract.
func (c *Client) SearchCommitByAuthorEmail(ctx context.Context, email string) (string, int64, error) {
	email = strings.Trim(email, `"' `)
	if email == "" || !strings.Contains(email, "@") {
		return "", 0, nil
	}

	// author-email: is a search qualifier; the value itself is URL-escaped.
	path := fmt.Sprintf("/search/commits?q=%s&per_page=1",
		url.QueryEscape("author-email:"+email))
	// v0.28.17: ETag-free — a repeat search for the same email in one
	// process would otherwise get a 304 this body reader cannot use.
	resp, err := c.http.Get(platform.WithoutETag(ctx), path)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	var data struct {
		TotalCount int `json:"total_count"`
		Items      []struct {
			// items[].author is the resolved GitHub USER object (login/id),
			// null when GitHub can't map the commit to an account. (Distinct
			// from items[].commit.author, which is the raw git name/email.)
			Author *struct {
				Login string `json:"login"`
				ID    int64  `json:"id"`
			} `json:"author"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		// Match SearchUserByEmail: rate-limited search responses can be
		// non-JSON; treat as "no result" rather than propagating.
		return "", 0, nil
	}

	if data.TotalCount == 0 || len(data.Items) == 0 || data.Items[0].Author == nil {
		return "", 0, nil
	}
	return data.Items[0].Author.Login, data.Items[0].Author.ID, nil
}
