// Package gitlab — search_user.go implements platform.Client.SearchUserByEmail
// for GitLab.
//
// GitLab's user search by email (`GET /users?search={email}`) is
// admin-only on gitlab.com (returns 403 for non-admin tokens), and
// most aveloxis deployments don't have admin tokens. Rather than
// burn search-quota tokens on requests that are guaranteed to fail
// — and risk silently degrading our 403 classification — we return
// ("", 0, nil) unconditionally. The caller (scheduler's
// search-resolve task) treats that as "no resolution available"
// and moves on.
//
// If a future deployment runs with admin tokens, this can be
// upgraded to call /users?search={email} and parse the response.

package gitlab

import "context"

// SearchUserByEmail returns no-result for GitLab — see file header for
// rationale.
func (c *Client) SearchUserByEmail(ctx context.Context, email string) (string, int64, error) {
	return "", 0, nil
}
