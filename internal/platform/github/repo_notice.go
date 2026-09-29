// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// FetchRepoNotice asks GitHub's REST API for a repository's block notice
// (worklist item 82). Prelim's probe is a web HEAD, so a 451 reaches the
// scheduler without GitHub's block object; this one keyed request fetches
// it. Three answers (SR-16): (notice, true, nil) when the answer carries
// the forge's block object; (zero, false, nil) when the repository
// answered normally; (zero, false, err) for anything else — a 404, a
// transport failure or a rate limit is not "no notice".
func (c *Client) FetchRepoNotice(ctx context.Context, owner, repo string) (platform.ForgeNotice, bool, error) {
	var ignored json.RawMessage
	err := c.http.GetJSON(ctx, fmt.Sprintf("/repos/%s/%s", owner, repo), &ignored)
	if err == nil {
		return platform.ForgeNotice{}, false, nil
	}
	if n, ok := platform.NoticeOf(err); ok {
		return n, true, nil
	}
	return platform.ForgeNotice{}, false, err
}
