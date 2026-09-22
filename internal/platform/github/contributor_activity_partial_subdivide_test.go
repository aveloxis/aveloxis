// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.29.57 (Copilot review round 1 on PR #210, comment on
// contributor_activity.go:152) — v0.29.57 made a FAILED CHUNK report its
// logins as unfetched so the scheduler would not read their absence as
// "deleted". But a chunk subdivides, and the whole top-level batch was
// reported unfetched when any subchunk failed. A deleted account inside a
// subchunk that COMPLETED was therefore filed as unanswered and never
// mark-only stamped, so it stayed at the NULLS-FIRST claim head and was
// re-claimed every tick.
//
// That is the v0.20.17 claim-head lesson exactly, one level further down:
// the level that ANSWERED is the level whose absences are trustworthy.

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestPartiallyCompletedChunkReportsOnlyTheUnansweredLogins(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Four logins: the left half answers (with "deleted-acct" omitted the
	// way GitHub omits a removed user), the right half fails hard.
	logins := []string{"alive", "deleted-acct", "right-one", "right-two"}
	inRight := func(s string) bool { return strings.HasPrefix(s, "right-") }

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)

		var left, right [][]string
		for _, m := range aliases {
			if inRight(m[2]) {
				right = append(right, m)
			} else {
				left = append(left, m)
			}
		}
		switch {
		case len(left) > 0 && len(right) > 0:
			// The whole batch: transient, so the fetcher subdivides.
			fmt.Fprint(w, rleResponse(aliases))
		case len(right) > 0:
			// The right subchunk: a hard, non-transient failure.
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"message":"Bad credentials"}`)
		default:
			// The left subchunk ANSWERS. "deleted-acct" comes back as a
			// null node, which is how GitHub reports a removed user — it
			// is an answer, not a gap.
			var kept [][]string
			for _, m := range left {
				if m[2] != "deleted-acct" {
					kept = append(kept, m)
				}
			}
			fmt.Fprint(w, okResponse(kept))
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	out, unfetched, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("a hard failure in one subchunk must still fail the fetch")
	}
	if _, ok := out["alive"]; !ok {
		t.Errorf("the answered subchunk's data must survive: out = %v", out)
	}

	sort.Strings(unfetched)
	want := []string{"right-one", "right-two"}
	if strings.Join(unfetched, ",") != strings.Join(want, ",") {
		t.Errorf("unfetched = %v, want %v", unfetched, want)
	}
	// The load-bearing half: "deleted-acct" was ANSWERED by a subchunk that
	// completed. Reporting it unfetched means the scheduler never mark-only
	// stamps it, so it pins the claim head forever.
	for _, l := range unfetched {
		if l == "deleted-acct" {
			t.Error("a deleted account from a COMPLETED subchunk was reported unanswered — it will never retire from the claim head (v0.20.17)")
		}
	}
}

// TestAccountSkippedAloneIsAnsweredNotUnanswered — v0.29.57. An account that
// fails at batch size 1 has been ANSWERED: GitHub resolved the query and
// said it cannot serve that account. It must therefore be absent from
// `unfetched`, so the scheduler mark-only stamps it and it leaves the
// NULLS-FIRST claim head. Reporting it unanswered is safe for retirement but
// revives the v0.27.81 wedge the surrounding code cites twice — the account
// is re-claimed every tick, forever, which is the shape that cost three days
// of classification.
func TestAccountSkippedAloneIsAnsweredNotUnanswered(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const poison = "ghost-monster"

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			if m[2] == poison {
				// Transient for the batch, and still transient alone: the
				// subdivision bottoms out and skips it.
				fmt.Fprint(w, rleResponse(aliases))
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	logins := []string{"alpha", poison, "gamma", "delta"}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	out, unfetched, err := client.FetchContributorActivity(t.Context(), logins)
	if err != nil {
		t.Fatalf("one unresolvable account must not fail the chunk: %v", err)
	}
	if _, ok := out[poison]; ok {
		t.Errorf("%s resolved, so this fixture no longer exercises the skip", poison)
	}
	for _, l := range unfetched {
		if l == poison {
			t.Error("an account skipped at size 1 was reported UNANSWERED — the scheduler then never mark-only stamps it, so it re-pins the claim head every tick (v0.27.81)")
		}
	}
	for _, l := range []string{"alpha", "gamma", "delta"} {
		if _, ok := out[l]; !ok {
			t.Errorf("%s must still be delivered alongside the skipped account", l)
		}
	}
}

// TestSkippedAloneStaysAnsweredWhenASiblingFails — v0.29.57 round 2. The
// case above cannot see the defect on its own: when nothing fails, `flush`
// discards the subdivision's unanswered list entirely, so reporting a
// size-1 skip as unanswered is invisible. It becomes visible the moment a
// SIBLING subtree fails hard — the parent then returns both halves' lists
// WITH an error, and the skipped login rides out into `unfetched`.
//
// (An earlier draft of this release called that mutation "equivalent". It
// is not; it is observable exactly here, and the consequence is the
// v0.27.81 claim-head wedge.)
func TestSkippedAloneStaysAnsweredWhenASiblingFails(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const poison = "ghost-monster"
	logins := []string{"alpha", poison, "right-one", "right-two"}
	inRight := func(s string) bool { return strings.HasPrefix(s, "right-") }

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		var left, right [][]string
		for _, m := range aliases {
			if inRight(m[2]) {
				right = append(right, m)
			} else {
				left = append(left, m)
			}
		}
		switch {
		case len(left) > 0 && len(right) > 0:
			fmt.Fprint(w, rleResponse(aliases)) // transient: subdivide
		case len(right) > 0:
			w.WriteHeader(http.StatusUnauthorized) // hard failure
			fmt.Fprint(w, `{"message":"Bad credentials"}`)
		default:
			for _, m := range left {
				if m[2] == poison {
					// Still transient alone: the left half bottoms out and skips it.
					fmt.Fprint(w, rleResponse(left))
					return
				}
			}
			fmt.Fprint(w, okResponse(left))
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, unfetched, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("the hard failure in the right half must still fail the fetch")
	}
	for _, l := range unfetched {
		if l == poison {
			t.Error("an account skipped at size 1 rode out as UNANSWERED because a SIBLING failed — it is never mark-only stamped, so it re-pins the claim head every tick (v0.27.81)")
		}
	}
	sort.Strings(unfetched)
	if want := "right-one,right-two"; strings.Join(unfetched, ",") != want {
		t.Errorf("unfetched = %v, want [%s]", unfetched, want)
	}
}
