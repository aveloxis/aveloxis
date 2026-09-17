// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestSearchDecodeFailureIsAnError (review round 2 on v0.29.55): both search
// clients turned a body that would not decode into ("", 0, nil) — the
// documented NO-HIT answer — so a cut-off 200 reached the callers' no-hit arms
// and stamped a 30-day cooldown (search resolve) or created an email-only
// contributor (mailing-list sender resolve) for a person who may have a GitHub
// login. The old reason ("rate-limited search responses can be non-JSON") no
// longer holds: 403/429 never reach the decode. A body that cannot be read is
// not an answer (SR-5).
func TestSearchDecodeFailureIsAnError(t *testing.T) {
	cutOff := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":1,"items":[{"lo`))
	})
	for name, call := range map[string]func(*Client) (string, int64, error){
		"SearchUserByEmail": func(c *Client) (string, int64, error) {
			return c.SearchUserByEmail(context.Background(), "someone@example.com")
		},
		"SearchCommitByAuthorEmail": func(c *Client) (string, int64, error) {
			return c.SearchCommitByAuthorEmail(context.Background(), "someone@example.com")
		},
	} {
		t.Run(name, func(t *testing.T) {
			login, id, err := call(testGHClient(t, cutOff))
			if err == nil {
				t.Fatalf("got (%q, %d, nil) — a body that did not decode was reported as no hit", login, id)
			}
			if platform.IsDefinitiveAnswer(err) {
				t.Fatalf("err %v classifies as a definitive answer; callers would stamp it", err)
			}
		})
	}
}
