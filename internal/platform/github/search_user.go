// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package github — search_user.go implements platform.Client.SearchUserByEmail
// using GET /search/users?q={email}+in:email.
//
// Rate-limit context: GitHub's search API runs on a SEPARATE budget
// from the core 5000/hour limit — 30 requests/minute per token. The
// scheduler's v0.19.2 search-resolve background task uses this method
// at a controlled rate (default once per hour, batched) to backfill
// gh_user_id on contributors observed by email-only paths (commit
// author resolution, ghosted issue authors).
//
// Contract: returns ("", 0, nil) — no hit, not an error — when search
// returns zero results or the input is not an email address (no search is
// made). Every other outcome that is not a hit is an error: a transport or
// 5xx failure, a rate limit that outlasted the retries, a rejected query
// (400/422, ErrRequestRejected), or a body that did not decode. Callers
// record (stamp) only what platform.IsDefinitiveAnswer accepts.

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// SearchUserByEmail looks up a GitHub user by email address using the
// GitHub Search API. See platform.Client.SearchUserByEmail for the
// full contract.
func (c *Client) SearchUserByEmail(ctx context.Context, email string) (string, int64, error) {
	// Strip surrounding quotes that sometimes appear in git log
	// author emails (e.g., `"steve@example.com"`). GitHub rejects
	// quoted search queries with 400.
	email = strings.Trim(email, `"' `)
	if email == "" || !strings.Contains(email, "@") {
		return "", 0, nil
	}

	path := fmt.Sprintf("/search/users?q=%s+in:email&per_page=1", url.QueryEscape(email))
	// v0.28.17: ETag-free — see search_commit.go.
	resp, err := c.http.Get(platform.WithoutETag(ctx), path)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	var data struct {
		TotalCount int `json:"total_count"`
		Items      []struct {
			Login string `json:"login"`
			ID    int64  `json:"id"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		// A body that does not decode is not an answer (SR-5): returning
		// the no-hit ("", 0, nil) let a cut-off 200 stamp a cooldown or
		// create an email-only contributor (review round 2 on v0.29.55).
		// Rate-limit refusals never reach here — Get handles 403/429.
		return "", 0, fmt.Errorf("decode search/users response: %w", err)
	}

	if data.TotalCount == 0 || len(data.Items) == 0 {
		return "", 0, nil
	}
	return data.Items[0].Login, data.Items[0].ID, nil
}
