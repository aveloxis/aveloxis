// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ResetAgreementKey identifies a reset-agreement count: the rate-limit
// bucket and the endpoint template (Phase 0, worklist items 27/81).
type ResetAgreementKey struct {
	Bucket   string // "core" or "graphql"
	Endpoint string // endpointTemplate of the request path
}

// ResetAgreementCounts classifies responses by how their rate-limit reset
// compares with the window the pool tracks for that bucket, BEFORE the
// window guard updates it: Earlier/Equal/Later than the tracked reset, or
// Untracked (no window tracked yet). Whether GitHub's resets agree with one
// tracked window per key and bucket is what item 28 decides the tracking
// model from.
type ResetAgreementCounts struct {
	Earlier, Equal, Later, Untracked int
}

// RecordResetAgreement turns the counting on for this pool. The caller that
// drains the counts (the scheduler's key-pool summary, for the GitHub pool)
// opts in; a pool nobody drains never records.
func (kp *KeyPool) RecordResetAgreement() {
	kp.mu.Lock()
	kp.recordResetAgreement = true
	kp.mu.Unlock()
}

// noteResetAgreement counts one response. kp.mu is held (UpdateFromResponse).
func (kp *KeyPool) noteResetAgreement(bucket string, tracked time.Time, resp *http.Response, reset string) {
	if !kp.recordResetAgreement {
		return
	}
	epoch, err := strconv.ParseInt(reset, 10, 64)
	if reset == "" || err != nil {
		return // no reset: nothing to compare
	}
	path := ""
	if resp.Request != nil && resp.Request.URL != nil {
		path = resp.Request.URL.Path
	}
	k := ResetAgreementKey{Bucket: bucket, Endpoint: endpointTemplate(path)}
	if kp.resetAgreement == nil {
		kp.resetAgreement = make(map[ResetAgreementKey]ResetAgreementCounts)
	}
	c := kp.resetAgreement[k]
	got := time.Unix(epoch, 0)
	switch {
	case tracked.IsZero():
		c.Untracked++
	case got.Before(tracked):
		c.Earlier++
	case got.After(tracked):
		c.Later++
	default:
		c.Equal++
	}
	kp.resetAgreement[k] = c
}

// DrainResetAgreement returns the counts since the last drain and resets
// them (the 5-minute key pool summary reads them).
func (kp *KeyPool) DrainResetAgreement() map[ResetAgreementKey]ResetAgreementCounts {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := kp.resetAgreement
	kp.resetAgreement = nil
	return out
}

// endpointTemplate reduces a request path to a bounded-cardinality template:
// owner, repo, login, org and project ids become placeholders, and only the
// resource segment after them is kept ("…" marks a deeper path).
func endpointTemplate(path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	if len(segs) == 1 && segs[0] == "" {
		return "/"
	}
	var prefix, rest []string
	switch {
	case segs[0] == "repos" && len(segs) >= 3:
		prefix, rest = []string{"repos", "{owner}", "{repo}"}, segs[3:]
	case (segs[0] == "users" || segs[0] == "user") && len(segs) >= 2:
		prefix, rest = []string{segs[0], "{login}"}, segs[2:]
	case segs[0] == "orgs" && len(segs) >= 2:
		prefix, rest = []string{"orgs", "{org}"}, segs[2:]
	case len(segs) >= 4 && segs[0] == "api" && segs[2] == "projects":
		prefix, rest = []string{"api", segs[1], "projects", "{id}"}, segs[4:]
	default:
		prefix, rest = segs[:1], segs[1:]
	}
	out := "/" + strings.Join(prefix, "/")
	if len(rest) > 0 {
		out += "/" + rest[0]
		if len(rest) > 1 {
			out += "/…"
		}
	}
	return out
}

// resetHeaderTime is a response's rate-limit reset as a time, for a log
// (Phase 0: the GraphQL refusal lines); zero when absent or unparseable.
func resetHeaderTime(resp *http.Response) time.Time {
	if resp == nil {
		return time.Time{}
	}
	epoch, err := strconv.ParseInt(firstHeader(resp, "X-RateLimit-Reset", "RateLimit-Reset"), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(epoch, 0)
}
