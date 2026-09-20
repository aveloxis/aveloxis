// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"net/url"
	"strings"
)

// ErrURLUserinfo is returned for a URL that carries userinfo — credentials
// before the host, as in https://user:token@host/owner/name. A repository
// URL is stored, logged, shown in the GUI and handed to subprocesses
// (git, scorecard) on their command line, so credentials in it leak by
// every one of those paths. Refused at the URL parser, the web validator,
// the store's write boundary and the scorecard boundary (v0.29.57, Copilot
// review 5261384568: `aveloxis run-scorecard` forwarded a stored URL to
// `scorecard --repo` verbatim).
var ErrURLUserinfo = errors.New("URL carries credentials (userinfo before the host)")

// RefuseURLUserinfo is the ONE check (SR-17) behind ErrURLUserinfo: nil for
// a URL without userinfo, ErrURLUserinfo otherwise. A URL that does not
// parse is read textually — the authority between "://" and the next
// delimiter — so a malformed URL cannot smuggle credentials past the check
// (fail closed); its parse error is the caller's own to report. An "@" in
// the path, query or fragment is not userinfo.
func RefuseURLUserinfo(raw string) error {
	raw = strings.TrimSpace(raw)
	if u, err := url.Parse(raw); err == nil {
		if u.User != nil {
			return ErrURLUserinfo
		}
		return nil
	}
	i := strings.Index(raw, "://")
	if i < 0 {
		return nil
	}
	authority := raw[i+3:]
	if j := strings.IndexAny(authority, "/?#"); j >= 0 {
		authority = authority[:j]
	}
	if strings.Contains(authority, "@") {
		return ErrURLUserinfo
	}
	return nil
}
