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
// Contract: returns ("", 0, nil) when search returns zero hits — not
// an error. Returns the actual error only on transport / 5xx
// failures the caller would want to retry.

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
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
	resp, err := c.http.Get(ctx, path)
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
		// Don't propagate decode errors — search responses can be
		// rate-limited with non-JSON bodies, and the caller treats
		// "no result" the same way.
		return "", 0, nil
	}

	if data.TotalCount == 0 || len(data.Items) == 0 {
		return "", 0, nil
	}
	return data.Items[0].Login, data.Items[0].ID, nil
}
