// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "io"

// ErrorBodyLimit bounds how much of an ERROR response's body is read (O5,
// v0.29.71). Derived from what its readers use: the error text takes at
// most the first 200 bytes (truncateBody), and the parsed shapes — GitHub's
// {message, documentation_url}, the block notice (a message and a URL), a
// GraphQL error list, a rate-limit phrase — are a few KB at most. 1 MiB is
// about three orders of magnitude above that and far below anything that
// hurts a worker; before, a broken or hostile upstream could answer an
// error with gigabytes and every byte was held in memory.
const ErrorBodyLimit int64 = 1 << 20

// ReadErrorBody is how the forge clients (HTTP, GraphQL) and OSV read an
// error response's body: at most ErrorBodyLimit bytes, the rest left unread (closing the body
// drops the connection instead of draining it). The error is the read's own
// (nil at the limit): a caller that decides on the body — the 403 rate-limit
// check — must not decide on a body that failed mid-read (SR-5). Success
// bodies are data and are not read through this.
func ReadErrorBody(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	return io.ReadAll(io.LimitReader(r, ErrorBodyLimit))
}
