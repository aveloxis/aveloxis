// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"sync"
)

// Response-size high-water marks (operator, 2026-09-30: data responses get
// no size limit — O5's data half — but the largest are logged, so a limit
// can be chosen from measurements if one is ever wanted). Process-wide, per
// source; observation only.
var (
	responseSizeMu  sync.Mutex
	responseSizeMax = map[string]int64{}
)

// NoteResponseSize records a data response's size in bytes and logs one INFO
// line, "response size high-water mark", when it is the largest seen from
// source since the process started: at most one line per request, and only
// when a new maximum is set — about ln(n) lines for n requests in random
// order, one per request only if a source's sizes keep rising (whole-branch
// review F4). A nil logger logs to the default.
func NoteResponseSize(logger *slog.Logger, source, url string, n int64) {
	responseSizeMu.Lock()
	prev, seen := responseSizeMax[source]
	if seen && n <= prev {
		responseSizeMu.Unlock()
		return
	}
	responseSizeMax[source] = n
	responseSizeMu.Unlock()
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("response size high-water mark", "source", source, "bytes", n, "previous_max", prev, "url", RedactURLUserinfo(url))
}

// countingBody reports the bytes read from a response body when it is
// closed (bodies are streamed into decoders, so the size is known only then).
type countingBody struct {
	rc     io.ReadCloser
	n      int64
	closed bool
	report func(int64)
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	b.n += int64(n)
	return n, err
}

func (b *countingBody) Close() error {
	if !b.closed {
		b.closed = true
		b.report(b.n)
	}
	return b.rc.Close()
}

// CountResponseBody wraps a data response's body so its size is noted
// (NoteResponseSize) when the caller closes it. A body the caller abandons
// unread reports what was read.
func CountResponseBody(body io.ReadCloser, logger *slog.Logger, source, url string) io.ReadCloser {
	if body == nil {
		return nil
	}
	return &countingBody{rc: body, report: func(n int64) { NoteResponseSize(logger, source, url, n) }}
}

// sizeSource names this client's responses for the high-water marks:
// "<forge>-<api>" (github-rest, gitlab-graphql, …), one spelling for REST
// and GraphQL, so each forge keeps its own maximum (whole-branch review F2:
// GitLab GraphQL was logged as github-graphql).
func (c *HTTPClient) sizeSource(api string) string {
	if c.authStyle == AuthGitHub {
		return "github-" + api
	}
	return "gitlab-" + api
}
